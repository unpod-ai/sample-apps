"""One script: attach a number, make a call through the SBC, show every webhook.

    # 1. public https URL for the webhooks (another terminal):
    cloudflared tunnel --url http://localhost:8098
    # 2. the call:
    python call_test.py --to +91XXXXXXXXXX --public-url https://<tunnel>.trycloudflare.com --rotate

Steps (all through the unpod SDK, client.telephony):
  1. numbers.overview          show the caller ID's state; --attach-agent / --attach-type to attach it
  2. webhook_secret            --secret <s>, or --rotate (org owner/admin; the full secret is shown once)
  3. receiver on --port        every webhook verified with parse_call_event and printed live
  4. calls.create              dial --to (leg 1); with --customer, add_participant once leg 1 answers
  5. wait                      until leg 1 hangs up, --max-seconds, or Ctrl-C; then calls.end
  6. recording                 RecordingCompleted webhook + calls.get_recording + download (./recordings)

Target and auth come from .env (UNPOD_BASE_URL, UNPOD_PLATFORM_TOKEN, UNPOD_ORG_HANDLE).
Against anything but localhost this places REAL calls: you are asked to confirm (or pass --yes).
Without --public-url no webhook can arrive; the script then polls calls.get instead.
Needs the org's call API switched on (SbcCallApiSettings.call_api_enabled) and --from to be
one of the org's numbers.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit

from dotenv import load_dotenv

HERE = Path(__file__).resolve().parent
load_dotenv(HERE / ".env", override=True)
sys.path.insert(0, str(HERE))

import httpx
from unpod import AsyncClient
from unpod.telephony import TelephonyAPIError, parse_call_event

from webhook_receiver import WebhookReceiver


def say(tag: str, msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {tag:9} {msg}", flush=True)


def api_error(exc: TelephonyAPIError) -> str:
    hints = {
        "call_api_disabled": "switch the call API on for this org (SbcCallApiSettings.call_api_enabled)",
        "from_not_owned": "--from must be one of the org's numbers (see the overview above)",
        "invalid_url": "--public-url must be a public https URL",
        "organization_required": "check UNPOD_ORG_HANDLE / that the token's user is a member",
    }
    if exc.code == "forbidden" and "number available" in (exc.message or ""):
        return (f"{exc.status_code} {exc.code} {exc.message}  -> the SBC itself refused it: this org is not "
                "provisioned on the SBC with that caller ID (DID / org sync on the SBC side)")
    hint = hints.get(exc.code or "", "")
    return f"{exc.status_code} {exc.code or ''} {exc.message}" + (f"  -> {hint}" if hint else "")


async def step_numbers(tel, args) -> None:
    try:
        overview = await tel.overview()
    except Exception as exc:  # noqa: BLE001 - informational only
        say("numbers", f"overview failed: {exc}")
        overview = []
    mine = [o for o in overview if getattr(o, "number", None) == args.from_]
    if mine:
        o = mine[0]
        say("numbers", f"{o.number}: agent={o.agent_id} state={o.connection_state} sync={o.sync_state}")
    else:
        say("numbers", f"{args.from_} is not in this org's overview")
    if args.attach_agent or args.attach_type:
        res = await tel.numbers.attach(args.from_, agent_id=args.attach_agent, attach_type=args.attach_type)
        say("attach", str(res.model_dump(exclude_none=True) if hasattr(res, "model_dump") else res))


async def step_secret(tel, args) -> str | None:
    if args.secret:
        return args.secret
    if os.environ.get("SBC_WEBHOOK_SECRET"):
        return os.environ["SBC_WEBHOOK_SECRET"]
    if args.rotate:
        try:
            r = await tel.webhook_secret.rotate()
        except TelephonyAPIError as exc:
            if exc.code == "forbidden":
                sys.exit(f"rotate: {exc.status_code} forbidden -> needs an org owner/admin; pass --secret instead")
            raise
        say("secret", f"rotated: version {r.version}, pushed_to_sbc={r.pushed_to_sbc}")
        if not r.pushed_to_sbc:
            say("secret", "WARNING: not pushed to the SBC (org not provisioned there, or it refused the field): "
                "webhooks will fail verification until the SBC has this secret")
        return r.secret
    return None


async def wait_until(pred, timeout: float, poll: float = 0.5) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        await asyncio.sleep(poll)
    return pred()


async def download(url: str, out_dir: Path, name: str, auth_headers: dict | None = None) -> None:
    try:
        # The proxy's /recording URL needs the same auth as the SDK; other hosts get none.
        base = os.environ.get("UNPOD_BASE_URL", "").rstrip("/")
        same_host = base and urlsplit(url).netloc == urlsplit(base).netloc
        headers = auth_headers if (same_host and auth_headers) else {}
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as http:
            resp = await http.get(url, headers=headers)
        resp.raise_for_status()
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"{name}.wav"
        path.write_bytes(resp.content)
        say("recording", f"saved {path} ({len(resp.content)} bytes, {resp.headers.get('content-type')})")
    except httpx.HTTPError as exc:
        say("recording", f"download failed ({exc}); the URL may need auth on the real SBC")


async def run(args, client: AsyncClient) -> int:
    tel, calls = client.telephony, client.telephony.calls
    await step_numbers(tel, args)
    secret = await step_secret(tel, args)

    receiver = None
    if args.public_url:
        if not secret:
            sys.exit("webhooks need the org's secret: pass --secret, set SBC_WEBHOOK_SECRET, or --rotate")
        receiver = WebhookReceiver(secret, verbose=False)
        receiver.start(port=args.port)
        say("webhooks", f"listening on :{args.port}  (public: {args.public_url})")

        # print every webhook as it arrives
        async def printer():
            seen = 0
            while True:
                for r in receiver.received[seen:]:
                    say("webhook", r.summary())
                    if args.show_raw and not r.verified:
                        show_raw(r)
                seen = len(receiver.received)
                await asyncio.sleep(0.2)

        printer_task = asyncio.create_task(printer())
    else:
        say("webhooks", "no --public-url: webhooks cannot reach you, polling calls.get instead")

    hooks = args.public_url.rstrip("/") if args.public_url else None
    options = {"ring_timeout": args.ring_timeout, "caller_name": "unpod SBC test"}
    if hooks:
        options.update(status_callback_method="POST", recording_callback_url=f"{hooks}/recording",
                       record=True, record_participant_track=True)

    try:
        call = await calls.create(args.from_, args.to, role="agent",
                                  metadata={"test": "call_test"},
                                  **({"status_callback_url": f"{hooks}/call-status?leg=1"} if hooks else {}),
                                  **options)
    except TelephonyAPIError as exc:
        say("create", api_error(exc))
        return 1
    cid, leg1 = call.call_id, call.legs[0].leg_id if call.legs else None
    say("create", f"call {cid} leg {leg1} -> {args.to} ({call.state})  ringing your phone...")

    def events():
        return tracked_events(receiver, args.accept_unsigned) if receiver else []

    def leg1_event(name):
        return receiver and any(e.leg_id == leg1 and e.event == name for e in events())

    ended_by_us = False
    tune_task = None
    t0 = time.time()
    try:
        answered = False
        if args.customer or args.play or args.speak:
            if receiver:
                answered = await wait_until(lambda: leg1_event("Answered") or leg1_event("Hangup"), args.max_seconds)
                answered = answered and leg1_event("Answered")
            else:
                answered = await wait_until_polled(calls, cid, "answered", args.max_seconds)
            say("answer", "leg 1 answered" if answered else "leg 1 not answered (no Answered event / state)")
        if answered and (args.play or args.speak):
            await play_stage(args, calls, cid, leg1, receiver, hooks, events)
        if args.customer:
            if answered:
                tune = await start_wait_tune(args, calls, cid, leg1, hooks) if args.wait_tune else None
                try:
                    music = {} if args.dial_music == "default" else {"dial_music": args.dial_music}
                    leg = await calls.add_participant(
                        cid, args.from_, args.customer, role="customer", **music,
                        idempotency_key=f"{cid}:customer",
                        **({"status_callback_url": f"{hooks}/call-status?leg=2"} if hooks else {}), **options)
                    say("customer", f"leg {leg.leg_id} -> {args.customer}  ringing...  dial_music={args.dial_music}")
                    if tune:
                        tune_task = asyncio.create_task(
                            stop_tune_on_answer(args, calls, cid, leg1, leg.leg_id, tune, receiver, events))
                    if args.bridge_test:
                        await bridge_test(args, calls, cid, leg1, leg.leg_id, receiver, hooks, events)
                except TelephonyAPIError as exc:
                    say("customer", api_error(exc))

                if args.customer_2:
                    try:
                        music = {} if args.dial_music == "default" else {"dial_music": args.dial_music}
                        leg = await calls.add_participant(
                            cid, args.from_, args.customer_2, role="customer_2", **music,
                            idempotency_key=f"{cid}:customer_2",
                            **({"status_callback_url": f"{hooks}/call-status?leg=3"} if hooks else {}), **options)
                        say("customer_2", f"leg {leg.leg_id} -> {args.customer_2}  ringing...  dial_music={args.dial_music}")
                        if args.bridge_test:
                            await bridge_test(args, calls, cid, leg1, leg.leg_id, receiver, hooks, events)
                    except TelephonyAPIError as exc:
                        say("customer_2", api_error(exc))
            else:
                say("customer", "leg 1 never answered; not dialling the customer")

        if receiver:
            await wait_until(lambda: leg1_event("Hangup"), args.max_seconds)
        else:
            await wait_until_polled(calls, cid, "ended", args.max_seconds)
    except (KeyboardInterrupt, asyncio.CancelledError):
        say("stop", "interrupted")
    finally:
        if tune_task:
            tune_task.cancel()
        try:
            await calls.end(cid)
            ended_by_us = True
            say("end", f"calls.end({cid})")
        except TelephonyAPIError as exc:
            say("end", "already ended" if exc.status_code == 404 else api_error(exc))

    if receiver:
        await wait_until(lambda: any(e.event in ("RecordingCompleted", "MPCRecordingCompleted")
                                     for e in receiver.events()), args.recording_wait)
        printer_task.cancel()
    rec_url = None
    try:
        rec = await calls.get_recording(cid)
        say("recording", f"available={rec.available} channels={rec.channels} url={rec.url}")
        rec_url = rec.url
    except TelephonyAPIError as exc:
        say("recording", api_error(exc))
    if receiver:
        rec_url = next((e.recording_url for e in receiver.events() if e.recording_url), rec_url)
    if rec_url:
        auth = getattr(client, "_auth", None)
        await download(rec_url, HERE / "recordings", cid, auth.headers() if auth else None)

    if receiver and (args.play or args.speak):
        print_play_timeline(t0, receiver, args.accept_unsigned)
    if receiver and args.dump:
        dump_webhooks(receiver, Path(args.dump))
    if receiver:
        receiver.stop()
        verified = [r for r in receiver.received if r.verified]
        rejected = [r for r in receiver.received if not r.verified]
        print("\n-- webhooks")
        for r in verified:
            e = r.event
            print(f"  {e.event:22} leg={e.leg_id or '-':14} role={e.role or '-':9} "
                  f"status={e.call_status or '-':10} cause={e.hangup_cause or '-'}")
        print(f"  {len(verified)} verified, {len(rejected)} rejected"
              + ("  <- signed with a different secret?" if rejected else ""))
        if not receiver.received:
            print("  none arrived: is the tunnel pointing at --port, and the URL public https?")
    print(f"\ncall {cid}: {'ended by script' if ended_by_us else 'ended by the network'}")
    return 0


def tracked_events(receiver, accept_unsigned: bool) -> list:
    """Verified events, plus (with --accept-unsigned) rejected ones parsed WITHOUT verification, deduped."""
    out, seen = [], set()
    for r in receiver.received:
        ev = r.event if r.verified else None
        if ev is None and accept_unsigned:
            try:
                ev = parse_call_event(r.raw_body, r.headers, None, verify=False)
            except Exception:  # noqa: BLE001 - debugging aid only
                ev = None
        if ev is None:
            continue
        key = ev.event_id or (ev.event, ev.leg_id, ev.sequence, ev.play_id)
        if key in seen:
            continue
        seen.add(key)
        out.append(ev)
    return out


async def play_stage(args, calls, cid, leg1, receiver, hooks, events) -> None:
    """Play audio on leg 1, track PlayStarted / the SBC fetching the file / PlayStopped, then stop it."""
    if args.play:
        if args.play == "tone" and not hooks:
            say("play", "--play tone needs --public-url (the SBC fetches the tone from it)")
        else:
            url = f"{hooks}/audio/tone.wav" if args.play == "tone" else args.play
            fetches_before = len(receiver.audio_fetches) if receiver else 0
            try:
                p = await calls.play_audio(cid, url=url, leg_id=leg1, loop=args.play_loop)
            except TelephonyAPIError as exc:
                say("play", api_error(exc))
                p = None
            if p:
                say("play", f"play_audio play_id={p.play_id} loop={args.play_loop} url={url}  -> listen for beeps")
                if receiver:
                    got = await wait_until(lambda: any(e.event == "PlayStarted" and e.play_id == p.play_id
                                                       for e in events()), 10)
                    say("play", f"PlayStarted event {'received' if got else 'NOT received (10 s)'}")
                await asyncio.sleep(args.play_seconds)
                if receiver and args.play == "tone":
                    n = len(receiver.audio_fetches) - fetches_before
                    say("play", f"SBC fetched tone.wav {n}x" + ("" if n else "  <- the SBC never downloaded it"))
                try:
                    await calls.stop_audio(cid, leg_id=leg1, play_id=p.play_id)
                    say("play", f"stop_audio(leg_id={leg1}, play_id={p.play_id})")
                except TelephonyAPIError as exc:
                    say("play", f"stop_audio: {api_error(exc)}")
                if receiver:
                    got = await wait_until(lambda: any(e.event == "PlayStopped" and e.play_id == p.play_id
                                                       for e in events()), 10)
                    say("play", f"PlayStopped event {'received' if got else 'NOT received (10 s)'}")
    if args.speak:
        try:
            sp = await calls.speak(cid, args.speak, leg_id=leg1)
            say("speak", f"speak play_id={sp.play_id}  -> listen for the voice")
            await asyncio.sleep(args.play_seconds)
        except TelephonyAPIError as exc:
            say("speak", f"{api_error(exc)}  (the contract says the SBC may answer not_supported: TTS not wired)")


async def bridge_test(args, calls, cid, leg1, leg2, receiver, hooks, events) -> None:
    """Both phones answered: beep to phone 1 only, phone 2 only, then both; dump calls.get between."""
    def answered2():
        return any(e.leg_id == leg2 and e.event in ("Answered", "Hangup") for e in events())

    if not (receiver and hooks):
        say("bridge", "--bridge-test needs --public-url (webhooks + the hosted tone)")
        return
    if not await wait_until(answered2, args.max_seconds) or not any(
            e.leg_id == leg2 and e.event == "Answered" for e in events()):
        say("bridge", "phone 2 did not answer; skipping")
        return
    await show_call(calls, cid)
    say("bridge", f"TALK NOW for {args.play_seconds:.0f} s: can each phone hear the other?")
    await asyncio.sleep(args.play_seconds)
    url = f"{hooks}/audio/tone.wav"
    for label, leg in (("phone 1 ONLY", leg1), ("phone 2 ONLY", leg2), ("BOTH phones", None)):
        try:
            p = await calls.play_audio(cid, url=url, leg_id=leg, loop=True)
        except TelephonyAPIError as exc:
            say("bridge", f"play to {label}: {api_error(exc)}")
            continue
        say("bridge", f"beeps -> {label} (leg_id={leg or 'all'}) play_id={p.play_id}  for {args.play_seconds:.0f} s")
        await asyncio.sleep(args.play_seconds)
        try:
            await calls.stop_audio(cid, leg_id=leg, play_id=p.play_id)
            say("bridge", f"stopped beeps on {label}")
        except TelephonyAPIError as exc:
            say("bridge", f"stop_audio {label}: {api_error(exc)}")
        await asyncio.sleep(2)
    await show_call(calls, cid)
    say("bridge", "done. note per step which phone beeped; hang up when finished")


async def start_wait_tune(args, calls, cid, leg1, hooks):
    """Loop --wait-tune on leg 1 while the customer is being dialled (Plivo playMusic loop:true)."""
    url = f"{hooks}/audio/tone.wav" if args.wait_tune == "tone" and hooks else args.wait_tune
    try:
        p = await calls.play_audio(cid, url=url, leg_id=leg1, loop=True)
        say("tune", f"wait tune looping on leg 1  play_id={p.play_id}  url={url}")
        return p
    except TelephonyAPIError as exc:
        say("tune", f"play_audio: {api_error(exc)}")
        return None


async def stop_tune_on_answer(args, calls, cid, leg1, leg2, tune, receiver, events) -> None:
    """Stop the wait tune only when the customer leg picks up (Plivo stopPlayingMusic)."""
    if receiver:
        await wait_until(lambda: any(e.leg_id == leg2 and e.event == "Answered" for e in events()),
                         args.max_seconds)
        picked = any(e.leg_id == leg2 and e.event == "Answered" for e in events())
    else:
        picked = await wait_until_polled_leg(calls, cid, leg2, args.max_seconds)
    if not picked:
        say("tune", "customer never picked up; tune keeps looping until the call ends")
        return
    try:
        await calls.stop_audio(cid, leg_id=leg1, play_id=tune.play_id)
        say("tune", "customer picked up -> stop_audio on leg 1")
    except TelephonyAPIError as exc:
        say("tune", f"stop_audio: {api_error(exc)}")


async def wait_until_polled_leg(calls, cid, leg_id, timeout: float) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        try:
            c = await calls.get(cid)
            if any(l.leg_id == leg_id and l.state == "answered" for l in (c.legs or [])):
                return True
        except TelephonyAPIError:
            pass
        await asyncio.sleep(1)
    return False


async def show_call(calls, cid) -> None:
    try:
        c = await calls.get(cid)
        legs = ", ".join(f"{l.leg_id}={l.state}/{getattr(l, 'role', None)}" for l in (c.legs or []))
        say("state", f"calls.get: state={c.state} legs=[{legs}]")
    except TelephonyAPIError as exc:
        say("state", f"calls.get: {api_error(exc)}")


def print_play_timeline(t0: float, receiver, accept_unsigned: bool) -> None:
    """Arrival time of each call / play event at our receiver, plus the SBC's audio fetches."""
    rows, seen = [], set()
    for r in receiver.received:
        ev = r.event
        if ev is None and accept_unsigned:
            try:
                ev = parse_call_event(r.raw_body, r.headers, None, verify=False)
            except Exception:  # noqa: BLE001 - debugging aid only
                ev = None
        if ev is None or not (ev.event in ("Answered", "PlayStarted", "PlayStopped", "Hangup") or ev.play_id):
            continue
        key = ev.event_id or (ev.event, ev.leg_id, ev.sequence, ev.play_id)
        if key in seen:
            continue
        seen.add(key)
        mark = "" if r.verified else "  (UNVERIFIED)"
        rows.append((r.at - t0, ev.event, f"leg={ev.leg_id} play_id={ev.play_id or '-'}{mark}"))
    for f in receiver.audio_fetches:
        rows.append((f.at - t0, "audio fetched", f"{f.path} {f.bytes_sent} bytes UA={f.user_agent[:40]!r}"))
    print("\n-- play timeline (seconds after create, as received here)")
    for at, name, detail in sorted(rows):
        print(f"  +{at:6.1f}s  {name:15} {detail}")


def dump_webhooks(receiver, path: Path) -> None:
    """Every webhook exactly as the SBC sent it: one JSON object per line."""
    import json
    from urllib.parse import parse_qsl

    with path.open("w") as f:
        for r in receiver.received:
            text = r.raw_body.decode("utf-8", "replace")
            ctype = next((v for k, v in r.headers.items() if k.lower() == "content-type"), "")
            try:
                fields = json.loads(text) if "json" in ctype or text.lstrip().startswith("{") \
                    else dict(parse_qsl(text, keep_blank_values=True))
            except ValueError:
                fields = None
            f.write(json.dumps({"at": time.strftime("%H:%M:%S", time.localtime(r.at)), "path": r.path,
                                "query": r.query, "verified": r.verified, "error": r.error,
                                "headers": r.headers, "fields": fields, "body": text}) + "\n")
    say("dump", f"{len(receiver.received)} webhooks -> {path}")


def show_raw(r) -> None:
    """Print a rejected webhook as received: signature header and the decoded fields (unverified)."""
    sig = next((v for k, v in r.headers.items() if k.lower() == "x-webhook-signature"), None)
    sig_headers = {k: v for k, v in r.headers.items() if "sign" in k.lower() or "plivo" in k.lower()}
    ctype = next((v for k, v in r.headers.items() if k.lower() == "content-type"), "")
    print(f"            content-type: {ctype}")
    print(f"            X-Webhook-Signature: {sig or 'MISSING'}")
    if sig_headers and not sig:
        print(f"            other signature headers: {sig_headers}")
    try:
        ev = parse_call_event(r.raw_body, r.headers, None, verify=False)
        fields = ev.model_dump(by_alias=True, exclude={"raw"}, exclude_none=True)
        fields.pop("metadata", None) if not fields.get("metadata") else None
        print(f"            parsed (UNVERIFIED): {fields}")
    except Exception as exc:  # noqa: BLE001 - debugging output only
        print(f"            body: {r.raw_body[:400]!r} ({exc})")


async def wait_until_polled(calls, cid: str, want: str, timeout: float) -> bool:
    """Without webhooks: poll calls.get. want = 'answered' (leg 1) or 'ended'."""
    end = time.time() + timeout
    last = None
    while time.time() < end:
        try:
            c = await calls.get(cid)
        except TelephonyAPIError as exc:
            if exc.status_code == 404:
                return want == "ended"
            raise
        states = [lg.state for lg in c.legs]
        if (c.state, states) != last:
            say("poll", f"call={c.state} legs={states}")
            last = (c.state, states)
        if want == "answered" and states and states[0] == "answered":
            return True
        if want == "ended" and (c.state == "ended" or (states and states[0] == "hungup")):
            return True
        await asyncio.sleep(2)
    return False


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--to", required=True, help="your number (leg 1)")
    ap.add_argument("--customer", help="optional 2nd number, dialled when leg 1 answers")
    ap.add_argument("--customer_2", help="optional 3rd number (the SBC allows two participants, so expect 409 participant_limit)")
    ap.add_argument("--from", dest="from_", default=os.environ.get("SBC_FROM_NUMBER"),
                    help="caller ID: one of the org's numbers (or SBC_FROM_NUMBER)")
    ap.add_argument("--public-url", help="public https URL that reaches --port (cloudflared/ngrok)")
    ap.add_argument("--port", type=int, default=8098)
    ap.add_argument("--secret", help="org call-event secret (or SBC_WEBHOOK_SECRET)")
    ap.add_argument("--rotate", action="store_true", help="rotate the secret to get it (org owner/admin)")
    ap.add_argument("--attach-agent", help="attach --from to this agent_id first")
    ap.add_argument("--attach-type", help="attach type (agent | pipeline | human_call)")
    ap.add_argument("--ring-timeout", type=int, default=30)
    ap.add_argument("--max-seconds", type=float, default=120)
    ap.add_argument("--recording-wait", type=float, default=30)
    ap.add_argument("--play", help="after leg 1 answers, play audio on it: 'tone' (hosted at "
                    "<public-url>/audio/tone.wav) or a public https MP3/WAV URL")
    ap.add_argument("--play-seconds", type=float, default=8, help="how long to let it play before stop_audio")
    ap.add_argument("--wait-tune", metavar="URL",
                    help="with --customer: loop this audio (https URL, or 'tone') on leg 1 while the customer "
                         "is dialled; stopped only when the customer picks up")
    ap.add_argument("--play-loop", action="store_true", help="loop the audio")
    ap.add_argument("--bridge-test", action="store_true",
                    help="with --customer: after phone 2 answers, beep phone 1 only, phone 2 only, then both")
    ap.add_argument("--dial-music", default="none",
                    help="add_participant dial_music: none (default), an https URL, or 'default' to omit it")
    ap.add_argument("--dump", metavar="FILE",
                    help="write every webhook as received (headers + body + parsed fields) to FILE, JSON lines")
    ap.add_argument("--speak", help="after the audio, try speak(text) on leg 1 (TTS may be not_supported)")
    ap.add_argument("--accept-unsigned", action="store_true",
                    help="DEBUG: let unsigned/rejected webhooks drive the flow (printed as UNVERIFIED)")
    ap.add_argument("--show-raw", action="store_true",
                    help="print rejected webhooks as received (signature header + unverified fields)")
    ap.add_argument("--yes", action="store_true", help="do not ask before placing a real call")
    return ap


def main() -> None:
    ap = build_parser()
    args = ap.parse_args()
    if not args.from_:
        ap.error("--from (or SBC_FROM_NUMBER) is required: one of the org's numbers")

    base =os.environ.get("UNPOD_BASE_URL", "")
    local = urlsplit(base).hostname in ("localhost", "127.0.0.1")
    print(f"proxy {base or '(unset)'} | org {os.environ.get('UNPOD_ORG_HANDLE')} | from {args.from_} -> {args.to}"
          + (f" then {args.customer}" if args.customer else ""))
    if not local and not args.yes:
        print("This places a REAL call through the SBC.")
        if input("Type YES to dial: ").strip() != "YES":
            sys.exit("aborted")

    async def go():
        async with AsyncClient() as client:
            return await run(args, client)

    sys.exit(asyncio.run(go()))


if __name__ == "__main__":
    main()
