"""7.10 start_failed on QA: REST start before the call is answered must answer 400
AND send one failed/start_failed callback when status_callback_url is set.
  $PY run_prestart.py NAME --to +91...
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
            return r
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
                answer_url=f"{url}/answer?plain=1&wait=60",
                status_callback_url=f"{url}/cb/call",
                status_callback_method="POST",
            )
            cid = call.call_id
            say(f"-> call {cid} placed. ANSWER YOUR PHONE (stay on the line ~20s)")
            ws_url = f"{wss}/ws?token=abc&mode=echo"
            say(
                ">>> start_stream IMMEDIATELY (call not answered yet), with callback url"
            )
            await step(
                "7.10 start_stream before answer (cb url)",
                calls.start_stream(
                    cid,
                    ws_url,
                    bidirectional=True,
                    content_type="audio/x-mulaw;rate=8000",
                    status_callback_url=f"{url}/cb/stream",
                    extra_headers="pre=1",
                ),
            )
            say(
                "waiting 25s for callbacks (answer or ignore the ringing phone, either is fine)"
            )
            await asyncio.sleep(25)
        finally:
            if cid:
                with contextlib.suppress(Exception):
                    await calls.hangup(cid)
            await asyncio.sleep(7)
            bot.terminate()
            tunnel.terminate()
            with contextlib.suppress(Exception):
                bot.wait(5)

    ev = read_events(events_path)
    cbs = []
    for e in ev:
        if e["kind"] == "callback" and e["name"] in ("stream", "stream2"):
            q = dict(up.parse_qsl(e.get("body", "")))
            cbs.append(
                {
                    "endpoint": e["name"],
                    "verified": e.get("verified"),
                    **{
                        k: q.get(k)
                        for k in (
                            "Event",
                            "StreamID",
                            "StatusReason",
                            "Duration",
                            "Error",
                            "ExtraHeaders",
                        )
                        if q.get(k)
                    },
                }
            )
    summ = {
        "call_id": cid,
        "steps": steps,
        "stream_callbacks": cbs,
        "ws_starts": sum(1 for e in ev if e["kind"] == "ws_start"),
    }
    (out_dir / "summary.json").write_text(json.dumps(summ, indent=2, default=str))
    say(f"files: {out_dir}/")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("name")
    p.add_argument("--to", required=True)
    sys.exit(asyncio.run(main(p.parse_args())))
