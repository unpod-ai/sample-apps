"""Blind transfer on QA: call A, then transfer(call_id, B, type="blind", leg="callee") once A answers.

Falls back to an attended transfer if no leg for B appears (attended answers 501 today).
--answer-q picks the answer XML: the default holds A with <Wait>; 'ct=mulaw&cb=1' runs an
echo <Stream> first, so you can check that the transfer stops it.
Docs: https://docs.unpod.ai/telephony/apis/call-control#transfer
Run:  python -u run_transfer2.py NAME --to +91A --also +91B [--answer-q 'ct=mulaw&cb=1']
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.parse as up
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from unpod import AsyncClient

from run_case import free_port, open_tunnel, read_events, wait_public

T0 = time.time()


def say(m):
    print(f"{time.strftime('%H:%M:%S')} (+{time.time() - T0:5.1f}s)  {m}", flush=True)


async def main(a):
    out_dir = Path("/tmp/streaming-e2e") / a.name
    shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True)
    events_path = out_dir / "events.jsonl"
    if os.environ.get("STREAM_TEST_QA_HOST", "qa.") not in (
        os.getenv("UNPOD_BASE_URL") or ""
    ):
        sys.exit("refusing: not QA")
    secret = os.getenv("UNPOD_WEBHOOK_SECRET") or ""
    from_ = os.getenv("UNPOD_FROM_NUMBER") or os.environ["SBC_FROM_NUMBER"]
    steps = []

    async def step(label, coro):
        t = round(time.time() - T0, 1)
        try:
            r = await coro
            steps.append(
                {
                    "t": t,
                    "step": label,
                    "ok": True,
                    "result": (r.model_dump() if hasattr(r, "model_dump") else r),
                }
            )
            say(f"[ok]  {label}")
        except Exception as e:
            steps.append(
                {
                    "t": t,
                    "step": label,
                    "ok": False,
                    "status": getattr(e, "status_code", None),
                    "error": str(e)[:200],
                }
            )
            say(f"[err] {label}: {getattr(e, 'status_code', None)} {str(e)[:160]}")

    async with AsyncClient() as client:
        calls = client.telephony.calls
        port = free_port()
        tunnel, url = await open_tunnel(port)
        wss = url.replace("https://", "wss://")
        env = {
            **os.environ,
            "BOT_SECRET": secret,
            "PUBLIC_BASE": wss,
            "PUBLIC_HTTP_BASE": url,
            "EVENTS": str(events_path),
        }
        bot = subprocess.Popen(
            [
                sys.executable,
                "-I",
                "-m",
                "uvicorn",
                "bot:app",
                "--app-dir",
                str(HERE),
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--log-level",
                "warning",
            ],
            env=env,
            stdout=open(out_dir / "bot.log", "w"),
            stderr=subprocess.STDOUT,
        )
        cid = None
        try:
            await wait_public(url)
            call = await calls.create(
                from_,
                a.to,
                answer_url=f"{url}/answer?{a.answer_q}",
                status_callback_url=f"{url}/cb/call",
                status_callback_method="POST",
            )
            cid = call.call_id
            say(f"-> call {cid} placed to A ({a.to}). ANSWER PHONE A.")

            def got(ev, role=None):
                return [
                    e
                    for e in read_events(events_path)
                    if e["kind"] == "callback"
                    and e["name"] == "call"
                    and f"Event={ev}" in e.get("body", "")
                ]

            end = time.monotonic() + 60
            while time.monotonic() < end and not got("Answered"):
                await asyncio.sleep(1)
            if not got("Answered"):
                say("!! A did not answer in 60s")
                return 1
            say("A answered. Waiting 3 s.")
            await asyncio.sleep(3)

            def new_legs():
                out = []
                for e in read_events(events_path):
                    if (
                        e["kind"] == "callback"
                        and e["name"] == "call"
                        and "Event=Initiated" in e.get("body", "")
                    ):
                        u_ = dict(up.parse_qsl(e["body"])).get("CallUUID", "")
                        if u_ and u_ != f"{cid}-callee" and u_ not in out:
                            out.append(u_)
                return out

            say(
                f">>> 1) BLIND transfer to B ({a.also}), leg=callee: does PHONE B ring? (answer it if so)"
            )
            await step(
                "1 transfer blind leg=callee -> B",
                calls.transfer(cid, a.also, type="blind", leg="callee"),
            )
            for _ in range(15):
                await asyncio.sleep(1)
                if new_legs():
                    break
            blind_worked = bool(new_legs())
            say(f"blind transfer produced a new leg for B: {blind_worked}")
            if not blind_worked:
                say(">>> 2) ATTENDED transfer to B: PHONE B SHOULD RING NOW, ANSWER IT")
                await step(
                    "2 transfer attended leg=callee -> B",
                    calls.transfer(cid, a.also, type="attended", leg="callee"),
                )
                end = time.monotonic() + 40
                while time.monotonic() < end and not new_legs():
                    await asyncio.sleep(1)
            legs = new_legs()
            say(f"new legs seen: {legs}")
            if legs:
                end = time.monotonic() + 40
                while time.monotonic() < end and not any(
                    f"CallUUID={legs[0]}" in e.get("body", "")
                    and "Event=Answered" in e.get("body", "")
                    for e in read_events(events_path)
                    if e["kind"] == "callback"
                ):
                    await asyncio.sleep(1)
                say(
                    "B answered (or timeout). Speak to B from A for 5 s, then I complete the transfer"
                )
                await asyncio.sleep(5)
                if not blind_worked:
                    await step(
                        "3 transfer_complete(consult leg)",
                        calls.transfer_complete(cid, legs[0]),
                    )
                say(
                    f"watching up to {a.max_wait} s: A and B should be connected; "
                    "hang up either phone to see what happens to the other"
                )
                end = time.monotonic() + a.max_wait
                while time.monotonic() < end and not any(
                    "Event=MPCEnd" in e.get("body", "")
                    for e in read_events(events_path)
                    if e["kind"] == "callback"
                ):
                    await asyncio.sleep(1)
            else:
                say("no leg for B appeared at all")
            say("done watching; hanging up")
        finally:
            if cid:
                with contextlib.suppress(Exception):
                    await calls.hangup(cid)
            await asyncio.sleep(6)
            bot.terminate()
            tunnel.terminate()
            with contextlib.suppress(Exception):
                bot.wait(5)

    ev = read_events(events_path)
    ws_start = next((e["ts"] for e in ev if e["kind"] == "ws_start"), None)
    tl = []
    t0 = next(
        (
            e["ts"]
            for e in ev
            if e["kind"] == "callback" and "Event=Answered" in e.get("body", "")
        ),
        ev[0]["ts"] if ev else 0,
    )
    for e in ev:
        if e["kind"] == "callback" and e["name"] == "call":
            q = dict(up.parse_qsl(e.get("body", "")))
            tl.append(
                {
                    "t": round(e["ts"] - t0, 1),
                    **{
                        k: q.get(k)
                        for k in (
                            "Event",
                            "Role",
                            "ParticipantRole",
                            "CallUUID",
                            "HangupCause",
                            "HangupBy",
                            "Duration",
                            "MPCEndReason",
                        )
                        if q.get(k)
                    },
                }
            )
    summ = {
        "timeline": tl,
        "call_id": cid,
        "steps": steps,
        "call_dtmf_events": [
            (
                round(e["ts"] - T0, 1),
                dict(up.parse_qsl(e.get("body", ""))).get("Digit"),
                dict(up.parse_qsl(e.get("body", ""))).get("Role"),
            )
            for e in ev
            if e["kind"] == "callback"
            and e["name"] == "call"
            and "Event=DTMF" in e.get("body", "")
        ],
        "ws_dtmf_in": [
            e["msg"]["dtmf"].get("digit")
            for e in ev
            if e["kind"] == "ws_in" and e.get("event") == "dtmf"
        ],
    }
    (out_dir / "summary.json").write_text(json.dumps(summ, indent=2, default=str))
    say(f"files: {out_dir}/")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("name")
    p.add_argument("--to", required=True)
    p.add_argument("--also", required=True)
    p.add_argument("--type", default="blind", choices=["blind", "attended"])
    p.add_argument(
        "--answer-q",
        default="plain=1&wait=900",
        help="bot.py /answer query; e.g. 'ct=mulaw&cb=1' to transfer while a <Stream> is active",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=0,
        help="time_limit seconds on create AND add_participant; 0 = omit (default 14400)",
    )
    p.add_argument(
        "--max-wait",
        type=int,
        default=120,
        help="hang up myself after this many seconds of the call being up",
    )
    sys.exit(asyncio.run(main(p.parse_args())))
