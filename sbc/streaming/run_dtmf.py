"""10.5 retest on QA: which send_dtmf variants actually deliver keypad tones?
A default leg, no stream | B leg_id=<call>-callee, no stream | C leg_id + stream active | D default leg + stream.
 $PY run_dtmf.py NAME --to +91...
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
                answer_url=f"{url}/answer?plain=1&wait=120",
                status_callback_url=f"{url}/cb/call",
                status_callback_method="POST",
            )
            cid = call.call_id
            say(f"-> call {cid} placed. ANSWER YOUR PHONE. DO NOT PRESS ANY KEYS.")
            end = time.monotonic() + 60
            while time.monotonic() < end:
                if any(
                    e["kind"] == "callback" and "Event=Answered" in e.get("body", "")
                    for e in read_events(events_path)
                ):
                    break
                await asyncio.sleep(1)
            else:
                say("!! not answered")
                return 1
            leg = f"{cid}-callee"
            await asyncio.sleep(3)
            say(">>> A: no leg, no stream: '1 2 3 #'")
            await step(
                "A send_dtmf 123# (no leg, no stream)", calls.send_dtmf(cid, "123#")
            )
            await asyncio.sleep(6)
            say(">>> B: leg=callee, no stream: '4 5 6'")
            await step(
                "B send_dtmf 456 (leg=callee, no stream)",
                calls.send_dtmf(cid, "456", leg="callee"),
            )
            await asyncio.sleep(6)
            say(">>> stream starts (echo). C: leg=callee: '7 8 9'")
            await step(
                "start_stream",
                calls.start_stream(
                    cid,
                    f"{wss}/ws?token=abc&mode=echo",
                    bidirectional=True,
                    content_type="audio/x-mulaw;rate=8000",
                    status_callback_url=f"{url}/cb/stream",
                ),
            )
            await asyncio.sleep(4)
            await step(
                "C send_dtmf 789 (leg=callee, stream active)",
                calls.send_dtmf(cid, "789", leg="callee"),
            )
            await asyncio.sleep(7)
            say(">>> D: no leg, stream active: '0 * #'")
            await step(
                "D send_dtmf 0*# (default leg, stream active)",
                calls.send_dtmf(cid, "0*#"),
            )
            await asyncio.sleep(6)
            await step("stop_stream", calls.stop_stream(cid))
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
    summ = {
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
    sys.exit(asyncio.run(main(p.parse_args())))
