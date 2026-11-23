"""Conference + stream + mix on QA. A = --to (callee of the REST call), B = --also (added participant).
Flow: call A -> A answers -> add_participant B -> B answers (bridged) -> start bidirectional stream (echo bot)
-> play_audio mix=True beep -> stop stream -> hang up. Prints prompts for both testers. QA only."""

import argparse
import asyncio
import contextlib
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")
from unpod import AsyncClient

from run_case import free_port, open_tunnel, read_events, wait_public

T0 = time.time()
HERE = Path(__file__).resolve().parent


def say(m):
    print(f"{time.strftime('%H:%M:%S')} (+{time.time() - T0:5.1f}s)  {m}", flush=True)


async def main(a):
    out = Path("/tmp/streaming-e2e") / a.name
    shutil.rmtree(out, ignore_errors=True)
    out.mkdir(parents=True)
    ep = out / "events.jsonl"
    if os.environ.get("STREAM_TEST_QA_HOST", "qa.") not in (
        os.getenv("UNPOD_BASE_URL") or ""
    ):
        sys.exit("refusing: not QA")
    secret = os.getenv("UNPOD_WEBHOOK_SECRET") or ""
    from_ = os.getenv("UNPOD_FROM_NUMBER") or os.environ["SBC_FROM_NUMBER"]
    steps = []

    async def step(label, coro):
        try:
            r = await coro
            steps.append(
                {
                    "step": label,
                    "ok": True,
                    "result": (r.model_dump() if hasattr(r, "model_dump") else r),
                }
            )
            say(f"[ok]  {label}")
            return r
        except Exception as e:
            steps.append(
                {
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
            "EVENTS": str(ep),
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
            stdout=open(out / "bot.log", "w"),
            stderr=subprocess.STDOUT,
        )
        cid = None

        def got(ev):
            return [
                e
                for e in read_events(ep)
                if e["kind"] == "callback"
                and e["name"] == "call"
                and f"Event={ev}" in e.get("body", "")
            ]

        try:
            await wait_public(url)
            call = await calls.create(
                from_,
                a.to,
                answer_url=f"{url}/answer?plain=1&wait=900",
                status_callback_url=f"{url}/cb/call",
                status_callback_method="POST",
            )
            cid = call.call_id
            say(f"-> call {cid} placed to A ({a.to}). A: ANSWER NOW.")
            end = time.monotonic() + 60
            while time.monotonic() < end and not got("Answered"):
                await asyncio.sleep(1)
            if not got("Answered"):
                say("!! A did not answer in 60s")
                return 1
            say("A answered. Dialing B in now: B: ANSWER NOW.")
            await asyncio.sleep(2)
            leg_b = await calls.add_participant(
                cid,
                from_,
                a.also,
                status_callback_url=f"{url}/cb/call",
                status_callback_method="POST",
                answer_url=f"{url}/answer?plain=1&wait=900",
            )
            say(f"B leg: {getattr(leg_b, 'leg_id', leg_b)}")
            end = time.monotonic() + 60
            while time.monotonic() < end and len(got("Answered")) < 2:
                await asyncio.sleep(1)
            if len(got("Answered")) < 2:
                say("!! B did not answer in 60s")
                return 1
            say("BRIDGED. A and B: TALK TO EACH OTHER for 8 s (no bot yet).")
            await asyncio.sleep(8)
            await step(
                "start_stream bidirectional echo (A hears bot echo + B?)",
                calls.start_stream(
                    cid,
                    f"{wss}/ws?mode=echo",
                    bidirectional=True,
                    content_type="audio/x-mulaw;rate=8000",
                    status_callback_url=f"{url}/cb/stream",
                ),
            )
            say(
                "STREAM ACTIVE 15 s. A: do you hear B AND your own voice echoed? B: do you hear A only, no echo? KEEP TALKING."
            )
            await asyncio.sleep(15)
            await step(
                "play_audio mix=True beep on callee leg (A should hear beep over B)",
                calls.play_audio(cid, url=f"{url}/beep.wav", leg_id="callee", mix=True),
            )
            say("BEEP 3 s: A: is B still audible under the beep? KEEP TALKING.")
            await asyncio.sleep(8)
            await step("stop_stream", calls.stop_stream(cid))
            say("stream stopped. TALK 5 s: bridge should continue normally.")
            await asyncio.sleep(5)
            say("hanging up")
        finally:
            if cid:
                with contextlib.suppress(Exception):
                    await calls.hangup(cid)
            await asyncio.sleep(6)
            bot.terminate()
            tunnel.terminate()
            with contextlib.suppress(Exception):
                bot.wait(5)
    ev = read_events(ep)
    (out / "summary.json").write_text(
        json.dumps(
            {"call_id": cid, "steps": steps, "events": len(ev)}, indent=2, default=str
        )
    )
    say(f"files: {out}/")


p = argparse.ArgumentParser()
p.add_argument("name")
p.add_argument("--to", required=True)
p.add_argument("--also", required=True)
asyncio.run(main(p.parse_args()))
