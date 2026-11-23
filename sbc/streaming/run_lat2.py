"""Two-phone telephony latency (no echo cancellation in the path).
  $PY run_lat2.py NAME --a +91..(plays beeps on speaker) --b +91..(mic next to A's speaker) [--rounds 10 --gap 4]
Beeps alternate A->B and B->A: t_send (bot) -> first inbound frame carrying the beep on the OTHER call.
Each result = out-leg + in-leg of two different calls (+ ~3 ms air). QA only; secret never printed."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

from unpod import AsyncClient

from run_case import (
    DEFAULT_FROM,
    HERE,
    free_port,
    open_tunnel,
    read_events,
    wait_public,
)


async def main(a) -> int:
    out = Path("/tmp/streaming-e2e") / a.name
    shutil.rmtree(out, ignore_errors=True)
    out.mkdir(parents=True)
    ep = out / "events.jsonl"
    if os.environ.get("STREAM_TEST_QA_HOST", "qa.") not in (
        os.getenv("UNPOD_BASE_URL") or ""
    ):
        sys.exit("refusing: UNPOD_BASE_URL is not QA")
    from_ = os.getenv("UNPOD_FROM_NUMBER") or DEFAULT_FROM
    secret = os.getenv("UNPOD_WEBHOOK_SECRET") or ""
    calls = {}
    async with AsyncClient() as client:
        port = free_port()
        tunnel, url = await open_tunnel(port)
        env = {
            **os.environ,
            "BOT_SECRET": secret,
            "PUBLIC_BASE": url.replace("https://", "wss://"),
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
        try:
            await wait_public(url)
            print(f"bot reachable (signatures verified: {bool(secret)})")
            for role, to in (("A", a.a), ("B", a.b)):
                ans = f"{url}/answer?ct={a.ct}&mode=latency2&role={role}&rounds={a.rounds}&gap={a.gap}&cb=1"
                c = await client.telephony.calls.create(
                    from_,
                    to,
                    answer_url=ans,
                    answer_method="POST",
                    status_callback_url=f"{url}/cb/call",
                    status_callback_method="POST",
                )
                calls[role] = c.call_id
                print(f"-> {role} {to}: call {c.call_id}")
            print(
                "ANSWER BOTH. A: speakerphone, full volume. B: mic next to A's speaker, B's own speaker low/earpiece. Stay silent."
            )
            end = time.monotonic() + a.timeout
            done = None
            while time.monotonic() < end:
                await asyncio.sleep(1)
                ev = read_events(ep)
                n = len([e for e in ev if e["kind"] == "lat2_beep"])
                if n >= a.rounds * 2 and done is None:
                    done = time.monotonic()
                if done and time.monotonic() - done > 6:
                    break
        finally:
            for cid in calls.values():
                with contextlib.suppress(Exception):
                    await client.telephony.calls.hangup(cid)
            bot.terminate()
            tunnel.terminate()
            with contextlib.suppress(Exception):
                bot.wait(5)
    ev = read_events(ep)
    print(
        "ws_start:",
        [(e.get("call_id"), e.get("stream_id")) for e in ev if e["kind"] == "ws_start"],
    )
    print(
        "beeps sent:", len([e for e in ev if e["kind"] == "lat2_beep"]), "calls:", calls
    )
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("name")
    p.add_argument("--a", required=True)
    p.add_argument("--b", required=True)
    p.add_argument("--rounds", type=int, default=10)
    p.add_argument("--gap", type=float, default=4)
    p.add_argument("--timeout", type=int, default=200)
    p.add_argument("--ct", default="l16_16")
    sys.exit(asyncio.run(main(p.parse_args())))
