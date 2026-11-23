"""Live call-control during a stream on QA (checklist 10.3, 10.4, 10.5): one REST-created call,
echo stream started via REST, then hold (expect 400), play_audio mix=True / mix=False, send_dtmf.

  PY=python   # a venv with unpod>=0.7.0 (see README)
  $PY run_ctrl.py NAME --to +91XXXXXXXXXX
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

DEFAULT_FROM = os.environ.get("SBC_FROM_NUMBER", "")
T0 = time.time()


def say(m: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} (+{time.time() - T0:5.1f}s)  {m}", flush=True)


async def main(a) -> int:
    out_dir = Path("/tmp/streaming-e2e") / a.name
    shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True)
    events_path = out_dir / "events.jsonl"
    if os.environ.get("STREAM_TEST_QA_HOST", "qa.") not in (
        os.getenv("UNPOD_BASE_URL") or ""
    ):
        sys.exit("refusing: UNPOD_BASE_URL is not QA")
    secret = os.getenv("UNPOD_WEBHOOK_SECRET") or ""
    from_ = os.getenv("UNPOD_FROM_NUMBER") or DEFAULT_FROM
    steps: list[dict] = []

    async def step(name, coro, expect):
        t = round(time.time() - T0, 1)
        try:
            r = await coro
            val = r.model_dump() if hasattr(r, "model_dump") else r
            steps.append(
                {"t": t, "step": name, "expect": expect, "ok": True, "result": val}
            )
            say(f"[ok]  {name}: {json.dumps(val, default=str)[:200]}")
            return r
        except Exception as e:
            code = getattr(e, "status_code", None)
            steps.append(
                {
                    "t": t,
                    "step": name,
                    "expect": expect,
                    "ok": False,
                    "status": code,
                    "error": f"{type(e).__name__}: {str(e)[:240]}",
                }
            )
            say(f"[err] {name}: status={code} {type(e).__name__}: {str(e)[:200]}")

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
        call_id = None
        try:
            await wait_public(url)
            call = await calls.create(
                from_,
                a.to,
                answer_url=f"{url}/answer?plain=1&wait=120",
                status_callback_url=f"{url}/cb/call",
                status_callback_method="POST",
            )
            call_id = call.call_id
            say(f"-> call {call_id} placed. ANSWER YOUR PHONE")
            end = time.monotonic() + 60
            while time.monotonic() < end:
                if any(
                    e["kind"] == "callback" and "Event=Answered" in e.get("body", "")
                    for e in read_events(events_path)
                ):
                    break
                await asyncio.sleep(1)
            else:
                say("!! not answered in 60s")
                return 1
            await asyncio.sleep(2)
            say(">>> stream starts: you hear your echo")
            await step(
                "start_stream (echo)",
                calls.start_stream(
                    call_id,
                    f"{wss}/ws?token=abc&mode=echo",
                    bidirectional=True,
                    content_type="audio/x-mulaw;rate=8000",
                    status_callback_url=f"{url}/cb/stream",
                ),
                "202",
            )
            await asyncio.sleep(4)
            await asyncio.sleep(3)
            say(">>> mix=True 5 s: tone ON TOP of your echo")
            await step(
                "play_audio mix=True length=5",
                calls.play_audio(call_id, url=f"{url}/tone.wav", mix=True, length=5),
                "play_id",
            )
            await asyncio.sleep(9)
            say(
                ">>> mix=False 5 s: you should hear ONLY the tone (echo/bot audio silenced while it plays)"
            )
            await step(
                "play_audio mix=False length=5",
                calls.play_audio(call_id, url=f"{url}/tone.wav", mix=False, length=5),
                "play_id",
            )
            await asyncio.sleep(9)
            await step("stop_stream", calls.stop_stream(call_id), "204")
        finally:
            if call_id:
                with contextlib.suppress(Exception):
                    await calls.hangup(call_id)
            await asyncio.sleep(6)
            bot.terminate()
            tunnel.terminate()
            with contextlib.suppress(Exception):
                bot.wait(5)

    ev = read_events(events_path)
    summ = {
        "call_id": call_id,
        "steps": steps,
        "tone_fetches": sum(1 for e in ev if e["kind"] == "tone_fetch"),
        "ws_ends": [
            {k: v for k, v in e.items() if k in ("after", "media_in", "play_out")}
            for e in ev
            if e["kind"] == "ws_end"
        ],
        "ws_dtmf_in": [
            (e.get("msg") or {}).get("dtmf")
            for e in ev
            if e["kind"] == "ws_in" and e.get("event") == "dtmf"
        ],
        "call_events": [
            dict(up.parse_qsl(e.get("body", ""))).get("Event")
            for e in ev
            if e["kind"] == "callback" and e["name"] == "call"
        ],
        "stream_callbacks": [
            {
                k: v
                for k, v in dict(up.parse_qsl(e.get("body", ""))).items()
                if k in ("Event", "StatusReason", "Duration")
            }
            for e in ev
            if e["kind"] == "callback" and e["name"] == "stream"
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
