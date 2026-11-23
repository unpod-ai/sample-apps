"""Concurrent calls on QA through ONE bot + tunnel: isolation / light-concurrency check.

  PY=<scratchpad>/sdkvenv07/bin/python   # unpod 0.7.0 from PyPI
  $PY run_multi.py NAME 'ct=l16_16&cb=1' --to +91XXXXXXXXXX --to +91YYYYYYYYYY [--hangup-first 30] [--timeout 90]

Places len(--to) calls at the same moment (asyncio.gather), each answered by the same bot (<Stream> echo).
Optional --hangup-first N: hang up the FIRST call N s after it was answered; the others must keep streaming.
Per call it reports: call id, stream id, place->WS-start latency, frames in/out, close code, callbacks seen.
QA only. Never prints the secret. Always hangs up everything and stops bot + tunnel at the end.
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
from pathlib import Path

from unpod import AsyncClient

from run_case import (
    DEFAULT_FROM,
    HERE,
    free_port,
    open_tunnel,
    read_events,
    wait_public,
)  # loads .env


async def main(a) -> int:
    out_dir = Path("/tmp/streaming-e2e") / a.name
    shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True)
    events_path = out_dir / "events.jsonl"
    from_ = a.from_ or os.getenv("UNPOD_FROM_NUMBER") or DEFAULT_FROM
    if os.environ.get("STREAM_TEST_QA_HOST", "qa.") not in (
        os.getenv("UNPOD_BASE_URL") or ""
    ):
        sys.exit("refusing: UNPOD_BASE_URL is not QA")
    print(
        f"QA org {os.getenv('UNPOD_ORG_HANDLE')}  {from_} -> {a.to}  case={a.name} query={a.query!r}"
    )
    secret = os.getenv("UNPOD_WEBHOOK_SECRET") or ""
    calls: list[dict] = []
    async with AsyncClient() as client:
        port = free_port()
        tunnel, url = await open_tunnel(port)
        env = {
            **os.environ,
            "BOT_SECRET": secret,
            "PUBLIC_BASE": url.replace("https://", "wss://"),
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
        try:
            await wait_public(url)
            print(f"bot reachable at {url} (signatures verified: {bool(secret)})")
            answer = f"{url}/answer" + (f"?{a.query}" if a.query else "")

            async def place(to: str) -> dict:
                t = time.time()
                c = await client.telephony.calls.create(
                    from_,
                    to,
                    answer_url=answer,
                    answer_method="POST",
                    status_callback_url=f"{url}/cb/call",
                    status_callback_method="POST",
                )
                return {
                    "to": to,
                    "call_id": c.call_id,
                    "placed_at": t,
                    "api_ms": round((time.time() - t) * 1000),
                }

            calls = list(await asyncio.gather(*(place(t) for t in a.to)))
            for c in calls:
                print(f"-> call {c['call_id']} to {c['to']} placed in {c['api_ms']} ms")
            print("ANSWER BOTH PHONES NOW. " + a.hint)
            end = time.monotonic() + a.timeout
            hung_first = False
            all_done_at = None
            while time.monotonic() < end:
                await asyncio.sleep(1)
                ev = read_events(events_path)
                starts = [e for e in ev if e["kind"] == "ws_start"]
                ends = [e for e in ev if e["kind"] == "ws_end"]
                if a.hangup_first and not hung_first and starts:
                    first = next(
                        (
                            e
                            for e in starts
                            if str(e.get("call_id")).removesuffix("-callee")
                            == calls[0]["call_id"]
                        ),
                        None,
                    )
                    if first and time.time() - first["ts"] >= a.hangup_first:
                        with contextlib.suppress(Exception):
                            await client.telephony.calls.hangup(calls[0]["call_id"])
                        hung_first = True
                        print(
                            f"hung up first call {calls[0]['call_id']} (others must stay up)"
                        )
                if (
                    len(starts) >= len(calls)
                    and len(ends) >= len(calls)
                    and all_done_at is None
                ):
                    all_done_at = time.monotonic()
                if all_done_at and time.monotonic() - all_done_at > a.settle:
                    break
        finally:
            for c in calls:
                with contextlib.suppress(Exception):
                    await client.telephony.calls.hangup(c["call_id"])
            bot.terminate()
            tunnel.terminate()
            with contextlib.suppress(Exception):
                bot.wait(5)

    ev = read_events(events_path)
    starts = {
        str(e.get("call_id")).removesuffix("-callee"): e
        for e in ev
        if e["kind"] == "ws_start"
    }
    ends = {e.get("sid"): e for e in ev if e["kind"] == "ws_end"}
    discs = {e.get("sid"): e for e in ev if e["kind"] == "ws_disconnect"}
    cbs = [e for e in ev if e["kind"] == "callback"]
    rows = []
    for c in calls:
        s = starts.get(c["call_id"])
        sid = s.get("stream_id") if s else None
        e = ends.get(sid, {})
        mine = [
            x
            for x in cbs
            if c["call_id"] in json.dumps(x, default=str)
            or (sid and sid in json.dumps(x, default=str))
        ]
        kinds = sorted(
            {
                str(
                    (x.get("event") or {}).get("event")
                    or (x.get("event") or {}).get("status")
                    or x.get("name")
                )
                for x in mine
            }
        )
        rows.append(
            {
                "to": c["to"],
                "call_id": c["call_id"],
                "stream_id": sid,
                "ws_start_after_place_s": round(s["ts"] - c["placed_at"], 2)
                if s
                else None,
                "media_in": e.get("media_in"),
                "peak_in": e.get("peak_in"),
                "play_out": e.get("play_out"),
                "stream_seconds": e.get("after"),
                "close_code": (discs.get(sid) or {}).get("code"),
                "callbacks": len(mine),
                "callback_kinds": kinds,
                "callbacks_verified": [x.get("verified") for x in mine],
            }
        )
    ups = [e for e in ev if e["kind"] == "ws_signature"]
    summ = {
        "case": a.name,
        "calls": rows,
        "ws_signatures_verified": [e.get("verified") for e in ups],
        "distinct_stream_ids": len({r["stream_id"] for r in rows if r["stream_id"]}),
        "bot_errors": [e for e in ev if e["kind"] == "ws_error"],
    }
    (out_dir / "summary.json").write_text(json.dumps(summ, indent=2, default=str))
    print(json.dumps(summ, indent=2, default=str))
    print(f"\nfiles: {out_dir}/ (events.jsonl, bot.log, summary.json)")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("name")
    p.add_argument("query", nargs="?", default="")
    p.add_argument("--to", action="append", required=True)
    p.add_argument("--from", dest="from_")
    p.add_argument("--timeout", type=int, default=90)
    p.add_argument("--settle", type=int, default=6)
    p.add_argument(
        "--hangup-first",
        type=int,
        default=0,
        help="hang up first call N s after its stream started",
    )
    p.add_argument("--hint", default="")
    sys.exit(asyncio.run(main(p.parse_args())))
