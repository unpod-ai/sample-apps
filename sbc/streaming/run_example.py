"""11.7 live half on QA: run examples/pipecat_streaming/server.py AS SHIPPED (Deepgram -> OpenAI -> Cartesia)
behind a tunnel, then POST its own /start (X-Api-Key) to dial the tester. Secrets are loaded in-process and
passed only to the server's environment; nothing is printed or written.

  $PY run_example.py NAME --to +91XXXXXXXXXX [--openai-from PATH_TO_ENV]
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import secrets
import shutil
import subprocess
import sys
import time
from pathlib import Path

from dotenv import dotenv_values

HERE = Path(__file__).resolve().parent
WORK = HERE.parent
sys.path.insert(0, str(HERE))
import httpx
from unpod import AsyncClient

from run_case import free_port, open_tunnel

EXAMPLE = (
    WORK
    / "supervoice"
    / "SDK"
    / "unpod-python-sdk-stream"
    / "examples"
    / "pipecat_streaming"
)
T0 = time.time()


def say(m):
    print(f"{time.strftime('%H:%M:%S')} (+{time.time() - T0:5.1f}s)  {m}", flush=True)


async def main(a):
    out = Path("/tmp/streaming-e2e") / a.name
    shutil.rmtree(out, ignore_errors=True)
    out.mkdir(parents=True)
    qa = dotenv_values(HERE.parent / ".env")
    ai = dotenv_values(HERE.parent.parent / ".env")
    oa = dotenv_values(Path(a.openai_from).expanduser()) if a.openai_from else {}
    if os.environ.get("STREAM_TEST_QA_HOST", "qa.") not in (
        qa.get("UNPOD_BASE_URL") or ""
    ):
        sys.exit("refusing: not QA")
    missing = [
        k
        for k, d in (
            ("OPENAI_API_KEY", oa),
            ("DEEPGRAM_API_KEY", ai),
            ("CARTESIA_API_KEY", ai),
            ("UNPOD_WEBHOOK_SECRET", qa),
        )
        if not d.get(k)
    ]
    if missing:
        sys.exit(f"missing keys: {missing}")
    start_key = secrets.token_urlsafe(24)
    from_ = qa.get("UNPOD_FROM_NUMBER") or qa.get("SBC_FROM_NUMBER", "")
    port = free_port()
    tunnel, url = await open_tunnel(port)
    wss = url.replace("https://", "wss://")
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("UNPOD_", "OPENAI_", "DEEPGRAM_", "CARTESIA_"))
    }
    env.update(
        {
            k: qa[k]
            for k in (
                "UNPOD_BASE_URL",
                "UNPOD_SERVICE_BASE_URL",
                "UNPOD_PLATFORM_TOKEN",
                "UNPOD_ORG_HANDLE",
                "UNPOD_WEBHOOK_SECRET",
            )
            if qa.get(k)
        }
    )
    env.update(
        UNPOD_FROM_NUMBER=from_,
        START_API_KEY=start_key,
        PUBLIC_WS_BASE=wss,
        OPENAI_API_KEY=oa["OPENAI_API_KEY"],
        DEEPGRAM_API_KEY=ai["DEEPGRAM_API_KEY"],
        CARTESIA_API_KEY=ai["CARTESIA_API_KEY"],
    )
    log = open(out / "server.log", "w")
    srv = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "server:app",
            "--app-dir",
            str(EXAMPLE),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "info",
        ],
        env=env,
        cwd=str(EXAMPLE),
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    call_id = None
    result = {"name": a.name}
    try:
        # reachability: /answer is open on the example
        end = time.monotonic() + 90
        while time.monotonic() < end:
            with contextlib.suppress(Exception):
                async with httpx.AsyncClient(timeout=5) as c:
                    if (await c.get(f"{url}/answer")).status_code == 200:
                        break
            await asyncio.sleep(3)
        else:
            sys.exit("example server not reachable via tunnel")
        say(f"example server up at {url}")
        async with httpx.AsyncClient(timeout=30) as c:
            r0 = await c.post(f"{url}/start", json={"phone_number": a.to})
            result["start_without_key"] = r0.status_code
            say(f"POST /start without key -> {r0.status_code} (expect 401)")
            r = await c.post(
                f"{url}/start",
                json={"phone_number": a.to},
                headers={"X-Api-Key": start_key},
            )
            result["start_with_key"] = r.status_code
            say(
                f"POST /start with key -> {r.status_code}  {r.text[:120] if r.status_code != 200 else ''}"
            )
            if r.status_code != 200:
                return 1
            call_id = r.json()["call_id"]
        say(
            f"-> call {call_id} placed by the example's /start. ANSWER; the bot waits for you to speak first. Say something, ask a question."
        )
        async with AsyncClient() as client:
            calls = client.telephony.calls
            t_end = time.monotonic() + a.seconds
            ended = False
            while time.monotonic() < t_end:
                await asyncio.sleep(3)
                with contextlib.suppress(Exception):
                    st = (await calls.get(call_id)).state
                    if st == "ended":
                        ended = True
                        break
            result["ended_by_itself"] = ended
            if not ended:
                with contextlib.suppress(Exception):
                    await calls.hangup(call_id)
                say(f"hung up after {a.seconds}s")
            await asyncio.sleep(5)
    finally:
        srv.terminate()
        tunnel.terminate()
        with contextlib.suppress(Exception):
            srv.wait(8)
    txt = (out / "server.log").read_text(errors="replace")
    lines = txt.splitlines()
    result["log_lines"] = len(lines)
    result["ws_accepted"] = any(
        "WebSocket /ws" in l and "accepted" in l for l in lines
    ) or any('"WebSocket /ws' in l for l in lines)
    result["errors"] = [
        l[:200]
        for l in lines
        if any(w in l for w in ("ERROR", "Traceback", "Exception"))
    ][:8]
    result["markers"] = [
        l[:160]
        for l in lines
        if any(
            w in l
            for w in (
                "hung up",
                "Unpod call",
                "transcript",
                "Bot",
                "Deepgram",
                "connected",
            )
        )
    ][:10]
    (out / "summary.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))
    say(f"files: {out}/ (server.log has no secrets; keys are not logged)")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("name")
    p.add_argument("--to", required=True)
    p.add_argument("--openai-from", default="")
    p.add_argument("--seconds", type=int, default=75)
    sys.exit(asyncio.run(main(p.parse_args())))
