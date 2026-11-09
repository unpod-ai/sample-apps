"""Test the answer URL: point a DID's inbound calls at this script, call the DID, see what the SBC does.

Docs: https://docs.unpod.ai/telephony/apis/incoming-calls
Run:  uv run inbound_test.py --did <org number> --public-url https://<tunnel> --rotate --mode speak

    # 1. tunnel (another terminal):
    cloudflared tunnel --protocol http2 --url http://localhost:8099
    # 2. this script, then call the DID from your phone:
    python inbound_test.py --did +91XXXXXXXXXX --public-url https://<tunnel> --rotate --mode speak
    python inbound_test.py --did +91XXXXXXXXXX --public-url https://<tunnel> --rotate --mode dial --agent +91XXXXXXXXXX

What happens (all through the unpod SDK, client.telephony):
  1. inbound.get_did(did)      save the DID's current inbound config (./did-backup-<did>.json)
  2. webhook_secret            --secret <s> or --rotate (the SBC signs answer-URL requests with it)
  3. inbound.set_did(did, ...) answer_url = <public-url>/incoming (status callbacks to the same URL)
  4. you call the DID          the SBC POSTs StartApp to /incoming; we reply with XML:
                                 --mode speak : <Speak>..</Speak><Hangup/>       (no second phone needed)
                                 --mode dial  : <Record/><Dial callerId=did><Number>agent</Number></Dial>
                                 --mode empty : empty reply = the SBC's static routing (the attached agent)
                               then Hangup and RecordStop arrive at /incoming; every request is printed
  5. restore                   set_did(saved config) and show get_did, unless --keep

If restoring cannot remove the answer_url, the SBC falls back to static routing once the
tunnel is gone (answer_fallback=static_routing, after answer_timeout_ms), so the attached
agent still answers, a few seconds later.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path
from xml.sax.saxutils import escape, quoteattr

from dotenv import load_dotenv

HERE = Path(__file__).resolve().parent
load_dotenv(HERE / ".env", override=True)
sys.path.insert(0, str(HERE))

from unpod import AsyncClient
from unpod.telephony import InboundConfig, TelephonyAPIError

from call_test import api_error, dump_webhooks, say, show_raw, step_secret
from webhook_receiver import WebhookReceiver


def make_answer(args):
    """The XML /incoming returns for StartApp. Hangup / RecordStop get an empty <Response/>."""

    def answer(ev) -> str:
        if ev is None or ev.event != "StartApp":
            return '<?xml version="1.0" encoding="UTF-8"?><Response></Response>'
        if args.mode == "empty":
            return ""
        if args.mode == "speak":
            body = f"<Speak>{escape(args.text)}</Speak><Hangup/>"
        else:
            caller_id = args.caller_id or args.did
            body = (f"<Record/><Dial callerId={quoteattr(caller_id)}>"
                    f"<Number>{escape(args.agent)}</Number></Dial>")
        say("answer", f"StartApp from={ev.from_} status={ev.call_status} -> {args.mode}")
        return f'<?xml version="1.0" encoding="UTF-8"?><Response>{body}</Response>'

    return answer


async def run(args, client: AsyncClient) -> int:
    inbound = client.telephony.inbound
    backup_file = HERE / f"did-backup-{args.did.lstrip('+')}.json"

    try:
        before_d = (await inbound.get_did(args.did)).model_dump(exclude_none=True)
    except TelephonyAPIError as exc:
        say("get_did", api_error(exc))
        if exc.status_code == 404:
            say("get_did", f"the SBC does not know {args.did}: it must be provisioned on the SBC "
                "(org's sbc_target) before an answer URL can be set. Nothing was changed.")
        return 1
    if not backup_file.exists():  # never overwrite the first backup with our own test config
        backup_file.write_text(json.dumps(before_d, indent=2))
    say("get_did", f"current: {before_d or '{} (no override: static routing)'}  saved -> {backup_file.name}")

    secret = await step_secret(client.telephony, args)
    if not secret:
        sys.exit("needs the webhook secret: --secret, SBC_WEBHOOK_SECRET, or --rotate")
    receiver = WebhookReceiver(secret, verbose=False, answer=make_answer(args), answer_path="/incoming")
    receiver.start(port=args.port)
    hook = args.public_url.rstrip("/") + "/incoming"
    say("listen", f":{args.port}  answer_url={hook}")

    try:
        cfg = await inbound.set_did(args.did, InboundConfig(
            answer_url=hook, answer_method="POST", status_callback_url=hook,
            status_callback_format="form", record=args.mode == "dial"))
        say("set_did", f"{cfg.model_dump(exclude_none=True)}")
    except TelephonyAPIError as exc:
        say("set_did", api_error(exc))
        receiver.stop()
        return 1

    say("CALL NOW", f"from your phone, dial {args.did}  (waiting up to {args.max_seconds:.0f} s, Ctrl-C to stop)")
    seen, t0 = 0, time.time()
    try:
        while time.time() - t0 < args.max_seconds:
            while seen < len(receiver.received):
                r = receiver.received[seen]
                seen += 1
                say("webhook", r.summary() if r.verified else f"REJECTED {r.path}: {r.error}")
                if args.show_raw:
                    show_raw(r)
            evs = receiver.events()
            if any(e.event == "Hangup" for e in evs) and (
                    args.mode != "dial" or any(e.event == "RecordStop" for e in evs)
                    or time.time() - max(r.at for r in receiver.received) > 20):
                break
            await asyncio.sleep(0.3)
    except (KeyboardInterrupt, asyncio.CancelledError):
        say("stop", "interrupted")
    finally:
        if args.dump:
            dump_webhooks(receiver, Path(args.dump))
        receiver.stop()
        if args.keep:
            say("restore", f"--keep: answer_url stays {hook}; restore later from {backup_file.name}")
        else:
            await restore(inbound, args.did, json.loads(backup_file.read_text()), hook)

    got = [r for r in receiver.received]
    print(f"\n-- {len(got)} requests to /incoming: "
          + ", ".join(f"{r.event.event if r.event else 'REJECTED'}" for r in got))
    if not got:
        print("   none arrived: did the call reach the SBC? is the DID routed to this org (state PENDING_VERIFY?),"
              " is the tunnel up on --port?")
    return 0


async def restore(inbound, did: str, saved: dict, hook: str) -> None:
    try:
        await inbound.set_did(did, InboundConfig(**saved))
        try:
            now = (await inbound.get_did(did)).model_dump(exclude_none=True)
        except TelephonyAPIError as exc:
            if exc.status_code != 404:
                raise
            now = {}
        say("restore", f"get_did now: {now or '{}'}")
        if now.get("answer_url") == hook:
            say("restore", "answer_url is still ours (the SBC merges the override; the SDK cannot send null). "
                "Calls fall back to static routing after answer_timeout_ms once the tunnel is down; "
                "ask the SBC team to clear the DID override.")
    except TelephonyAPIError as exc:
        say("restore", f"{api_error(exc)}  -> restore by hand from the backup file")


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Test a DID's answer URL with a real inbound call")
    ap.add_argument("--did", required=True, help="the org's number to call (E.164)")
    ap.add_argument("--public-url", required=True, help="public https URL reaching --port")
    ap.add_argument("--port", type=int, default=8099)
    ap.add_argument("--mode", choices=["speak", "dial", "empty"], default="speak")
    ap.add_argument("--text", default="Hello. This is UNPOD how can we help you today ??")
    ap.add_argument("--agent", help="--mode dial: the number to ring (yours)")
    ap.add_argument("--caller-id", help="--mode dial: Dial callerId (default: --did)")
    ap.add_argument("--secret")
    ap.add_argument("--rotate", action="store_true")
    ap.add_argument("--max-seconds", type=float, default=180)
    ap.add_argument("--show-raw", action="store_true", help="print each request's headers and fields")
    ap.add_argument("--dump", metavar="FILE", help="write every request as received to FILE (JSON lines)")
    ap.add_argument("--keep", action="store_true", help="do not restore the DID config at the end")
    return ap


def main() -> None:
    ap = build_parser()
    args = ap.parse_args()
    if args.mode == "dial" and not args.agent:
        ap.error("--mode dial needs --agent")
    print(f"proxy {os.environ.get('UNPOD_BASE_URL')} | org {os.environ.get('UNPOD_ORG_HANDLE')} | DID {args.did}")
    print("This changes where calls to the DID go until the script restores it.")

    async def go() -> int:
        async with AsyncClient() as client:
            return await run(args, client)

    sys.exit(asyncio.run(go()))


if __name__ == "__main__":
    main()
