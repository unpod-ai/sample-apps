"""N concurrent two-party conferences on QA: for each pair, call A, then add_participant B when A answers.
  $PY run_conf_multi.py NAME --pair +91A,+91B --pair +91C,+91D ... [--limit 120] [--max-wait 140]
QA only, one bot+tunnel, secret never printed; hangs everything up at the end."""

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

from run_case import free_port, open_tunnel, read_events, wait_public

T0 = time.time()


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
    lim = {"time_limit": a.limit} if a.limit else {}
    res = {}
    async with AsyncClient() as client:
        calls = client.telephony.calls
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
                str(Path(__file__).parent),
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

        def got(cid, ev):
            return [
                e
                for e in read_events(ep)
                if e["kind"] == "callback"
                and e["name"] == "call"
                and (e.get("event") or {}).get("event") == ev
                and (e.get("event") or {}).get("call_id") == cid
            ]

        async def pair(idx, A, B):
            r = res[idx] = {
                "A": A,
                "B": B,
                "call_id": None,
                "a_answered": None,
                "b_leg": None,
                "b_answered": None,
                "ended": None,
                "cause": None,
                "notes": [],
            }
            ans = f"{url}/answer?plain=1&wait=900"
            try:
                c = await calls.create(
                    from_,
                    A,
                    answer_url=ans,
                    status_callback_url=f"{url}/cb/call",
                    status_callback_method="POST",
                    **lim,
                )
            except Exception as e:
                r["notes"].append(
                    f"create A failed: {getattr(e, 'status_code', None)} {str(e)[:150]}"
                )
                return
            cid = r["call_id"] = c.call_id
            say(f"pair {idx}: call {cid} -> A {A}")
            end = time.monotonic() + 60
            while time.monotonic() < end and not got(cid, "Answered"):
                await asyncio.sleep(1)
            if not got(cid, "Answered"):
                r["notes"].append("A did not answer in 60 s")
                with contextlib.suppress(Exception):
                    await calls.hangup(cid)
                return
            t_a = time.time()
            r["a_answered"] = round(t_a - T0, 1)
            say(f"pair {idx}: A {A} answered")
            await asyncio.sleep(2)
            try:
                lb = await calls.add_participant(
                    cid,
                    from_,
                    B,
                    status_callback_url=f"{url}/cb/call",
                    status_callback_method="POST",
                    answer_url=ans,
                    **lim,
                )
                r["b_leg"] = lb.model_dump() if hasattr(lb, "model_dump") else str(lb)
            except Exception as e:
                r["notes"].append(
                    f"add_participant failed: {getattr(e, 'status_code', None)} {str(e)[:150]}"
                )
                return
            say(f"pair {idx}: dialing B {B}")
            end = time.monotonic() + 60
            while time.monotonic() < end and len(got(cid, "Answered")) < 2:
                await asyncio.sleep(1)
            if len(got(cid, "Answered")) >= 2:
                r["b_answered"] = round(time.time() - T0, 1)
                say(f"pair {idx}: B {B} answered -> bridged")
            else:
                r["notes"].append("B did not answer in 60 s")
            while time.time() - t_a < a.max_wait and not got(cid, "MPCEnd"):
                await asyncio.sleep(1)
            m = got(cid, "MPCEnd")
            if m:
                r["ended"] = round(m[0]["ts"] - T0, 1)
                r["cause"] = m[0]["event"].get("hangup_cause")
                r["duration"] = m[0]["event"].get("duration")
            else:
                r["notes"].append("still up at max-wait: hung up by script")
            with contextlib.suppress(Exception):
                await calls.hangup(cid)

        try:
            await wait_public(url)
            pairs = [tuple(p.split(",")) for p in a.pair]
            say(
                f"{len(pairs)} pairs, all A legs dialed now. ANSWER; when your pair's second phone rings, answer it and talk to each other."
            )
            await asyncio.gather(*(pair(i + 1, A, B) for i, (A, B) in enumerate(pairs)))
        finally:
            for r in res.values():
                if r.get("call_id"):
                    with contextlib.suppress(Exception):
                        await calls.hangup(r["call_id"])
            await asyncio.sleep(5)
            bot.terminate()
            tunnel.terminate()
            with contextlib.suppress(Exception):
                bot.wait(5)
    ev = read_events(ep)
    bad = [e for e in ev if e["kind"] == "callback" and e.get("verified") is False]
    summ = {"pairs": res, "unverified_callbacks": len(bad), "limit": a.limit}
    (out / "summary.json").write_text(json.dumps(summ, indent=2, default=str))
    print(
        json.dumps(
            {
                k: {
                    x: v.get(x)
                    for x in (
                        "A",
                        "B",
                        "a_answered",
                        "b_answered",
                        "ended",
                        "duration",
                        "cause",
                        "notes",
                    )
                }
                for k, v in res.items()
            },
            indent=1,
        )
    )
    print("unverified callbacks:", len(bad))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("name")
    p.add_argument("--pair", action="append", required=True)
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--max-wait", type=int, default=1800)
    sys.exit(asyncio.run(main(p.parse_args())))
