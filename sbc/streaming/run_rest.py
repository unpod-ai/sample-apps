"""Live REST stream API test on QA (checklist 9.x, 11.2): one call, no <Stream> in the answer,
streams are started/listed/stopped mid-call through the SDK (unpod proxy).

  PY=python   # a venv with unpod>=0.7.0 (see README)
  $PY run_rest.py NAME --to +91XXXXXXXXXX
Never prints the secret. Hangs the call up at the end; stops bot + tunnel.
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
BAD = "00000000-0000-0000-0000-000000000000"


def say(m: str) -> None:
    print(f"{time.strftime('%H:%M:%S')}  {m}", flush=True)


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

    async def step(name: str, coro, expect: str):
        try:
            r = await coro
            val = (
                r.model_dump()
                if hasattr(r, "model_dump")
                else ([x.model_dump() for x in r] if isinstance(r, list) else r)
            )
            steps.append({"step": name, "expect": expect, "ok": True, "result": val})
            say(f"[ok]  {name}: {json.dumps(val, default=str)[:260]}")
            return r
        except Exception as e:
            code = getattr(e, "status_code", None) or getattr(
                getattr(e, "response", None), "status_code", None
            )
            msg = str(e)[:260]
            steps.append(
                {
                    "step": name,
                    "expect": expect,
                    "ok": False,
                    "status": code,
                    "error": f"{type(e).__name__}: {msg}",
                }
            )
            say(f"[err] {name}: status={code} {type(e).__name__}: {msg}")
            return None

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
        ws_url = f"{wss}/ws?token=abc&mode=echo"
        try:
            await wait_public(url)
            say(f"bot reachable at {url} (signatures verified: {bool(secret)})")
            call = await calls.create(
                from_,
                a.to,
                answer_url=f"{url}/answer?plain=1&wait=90",
                status_callback_url=f"{url}/cb/call",
                status_callback_method="POST",
            )
            call_id = call.call_id
            say(
                f"-> call {call_id} placed. ANSWER YOUR PHONE (plain call, no stream yet)"
            )
            end = time.monotonic() + 60
            while time.monotonic() < end:
                if any(
                    e["kind"] == "callback" and "Event=Answered" in e.get("body", "")
                    for e in read_events(events_path)
                ):
                    break
                await asyncio.sleep(1)
            else:
                say("!! call not answered in 60s")
                return 1
            await asyncio.sleep(2)

            await step(
                "9.x invalid url wss://127.0.0.1 -> unpod 400",
                calls.start_stream(call_id, "wss://127.0.0.1/ws"),
                "400 invalid_url",
            )
            say(">>> starting stream 1: you should hear your echo")
            s1 = await step(
                "9.1 start_stream",
                calls.start_stream(
                    call_id,
                    ws_url,
                    bidirectional=True,
                    content_type="audio/x-mulaw;rate=8000",
                    status_callback_url=f"{url}/cb/stream",
                    extra_headers="src=rest",
                ),
                "202 + stream_id",
            )
            await asyncio.sleep(3)
            await step(
                "2.8 second start while active",
                calls.start_stream(call_id, ws_url),
                "400 already active",
            )
            await step(
                "9.2 list_streams", calls.list_streams(call_id), "1 stream in-progress"
            )
            if s1:
                await step(
                    "9.3 get_stream (active)",
                    calls.get_stream(call_id, s1.stream_id),
                    "in-progress + fields",
                )
            await asyncio.sleep(4)
            if s1:
                await step(
                    "9.4 stop_stream(id)",
                    calls.stop_stream(call_id, s1.stream_id),
                    "204",
                )
                await asyncio.sleep(1)
                await step(
                    "9.4 get_stream (after stop)",
                    calls.get_stream(call_id, s1.stream_id),
                    "stopped / api_request",
                )
            await step("9.4 call still up", calls.get(call_id), "state talking")
            say(">>> stream 1 stopped (echo should end, call stays). Starting stream 2")
            s2 = await step(
                "9.5 start_stream #2",
                calls.start_stream(
                    call_id, ws_url, status_callback_url=f"{url}/cb/stream"
                ),
                "202",
            )
            await asyncio.sleep(4)
            await step(
                "9.5 stop_stream() no id (stop active)",
                calls.stop_stream(call_id),
                "204",
            )
            await asyncio.sleep(1)
            await step(
                "9.5 list_streams (both ended)",
                calls.list_streams(call_id),
                "2 streams stopped",
            )
            await step(
                "9.x stop_stream() with none active",
                calls.stop_stream(call_id),
                "400 no active stream",
            )
            await step(
                "9.6 get_stream unknown id", calls.get_stream(call_id, BAD), "404"
            )
            await step("9.6 list_streams unknown call", calls.list_streams(BAD), "404")
            await step(
                "9.6 start_stream unknown call", calls.start_stream(BAD, ws_url), "404"
            )
        finally:
            if call_id:
                with contextlib.suppress(Exception):
                    await calls.hangup(call_id)
            await asyncio.sleep(6)  # late callbacks
            bot.terminate()
            tunnel.terminate()
            with contextlib.suppress(Exception):
                bot.wait(5)

    ev = read_events(events_path)
    summ = {"call_id": call_id, "steps": steps, "signature_checked": bool(secret)}
    summ["ws_upgrades"] = [
        {"verified": e.get("verified")} for e in ev if e["kind"] == "ws_signature"
    ]
    summ["ws_starts"] = [
        {k: e.get(k) for k in ("call_id", "stream_id", "fmt", "extra_headers")}
        for e in ev
        if e["kind"] == "ws_start"
    ]
    summ["ws_ends"] = [
        {k: v for k, v in e.items() if k in ("after", "media_in", "play_out")}
        for e in ev
        if e["kind"] == "ws_end"
    ]
    summ["stream_callbacks"] = []
    for e in ev:
        if e["kind"] == "callback" and e["name"] == "stream":
            q = dict(up.parse_qsl(e.get("body", "")))
            summ["stream_callbacks"].append(
                {
                    "verified": e.get("verified"),
                    **{
                        k: q.get(k)
                        for k in (
                            "Event",
                            "StreamID",
                            "StatusReason",
                            "Duration",
                            "ExtraHeaders",
                            "Error",
                        )
                        if q.get(k)
                    },
                }
            )
    (out_dir / "summary.json").write_text(json.dumps(summ, indent=2, default=str))
    say(f"files: {out_dir}/")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("name")
    p.add_argument("--to", required=True)
    sys.exit(asyncio.run(main(p.parse_args())))
