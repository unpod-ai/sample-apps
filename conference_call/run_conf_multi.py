"""Human-to-human e2e: N concurrent two-party calls. Dial A, add B when A answers.

Docs: https://docs.unpod.ai/telephony/apis/multiple-participants
Run:  uv run run_conf_multi.py NAME --pair +91A,+91B [--pair +91C,+91D ...]
          [--from +91...] [--limit 120] [--max-wait 1800]

Each pair: calls.create(A) -> A answers -> calls.add_participant(B) -> both talk.
Neither leg gets an answer_url, so Unpod bridges them. B leaving ends the call
(end_mpc_on_exit). bot.py behind a tunnel only collects the status callbacks.
--limit sets time_limit on each leg. Every call is hung up at the end; results
go to logs/NAME/summary.json. Targets UNPOD_BASE_URL from .env.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import sys
import time
from typing import Any

from unpod import AsyncClient

from run_case import (
    caller_id,
    case_dir,
    free_port,
    open_tunnel,
    read_events,
    start_bot,
    wait_public,
    webhook_secret,
)

T0 = time.time()
ANSWER_WAIT = 60  # seconds each person gets to pick up


def say(msg: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} (+{time.time() - T0:5.1f}s)  {msg}", flush=True)


async def main(a: argparse.Namespace) -> int:
    out = case_dir(a.name)
    events = out / "events.jsonl"
    from_ = caller_id(a.from_)
    pairs = [tuple(p.split(",")) for p in a.pair]
    if any(len(p) != 2 for p in pairs):
        sys.exit("--pair takes two numbers: +91A,+91B")
    results: dict[int, dict[str, Any]] = {}
    async with AsyncClient() as client:
        secret = await webhook_secret(client)
        if not secret:
            sys.exit("H2H needs the webhook secret: Answered callbacks are verified")
        await client.telephony.numbers.attach(from_, attach_type="human_call")
        port = free_port()
        tunnel, url = await open_tunnel(port)
        bot = start_bot(port, url, out, secret)
        calls = client.telephony.calls
        legs: dict[str, Any] = {
            "status_callback_url": f"{url}/cb/call",
            "status_callback_method": "POST",
            **({"time_limit": a.limit} if a.limit else {}),
        }

        def got(call_id: str, event: str) -> list[dict]:
            """Verified call callbacks of one kind for one call (both legs)."""
            return [
                e
                for e in read_events(events)
                if e["kind"] == "callback"
                and e["name"] == "call"
                and (e.get("event") or {}).get("event") == event
                and (e.get("event") or {}).get("call_id") == call_id
            ]

        async def wait_for(call_id: str, event: str, count: int, secs: float) -> bool:
            end = time.monotonic() + secs
            while time.monotonic() < end and len(got(call_id, event)) < count:
                await asyncio.sleep(1)
            return len(got(call_id, event)) >= count

        async def run_pair(idx: int, first: str, second: str) -> None:
            r: dict[str, Any] = {"A": first, "B": second, "call_id": None, "notes": []}
            results[idx] = r
            try:
                call = await calls.create(from_, first, role="A", **legs)
            except Exception as e:  # noqa: BLE001 - record it and move on to the next pair
                r["notes"].append(
                    f"create A failed: {getattr(e, 'status_code', None)} {e}"
                )
                return
            call_id = r["call_id"] = call.call_id
            say(f"pair {idx}: call {call_id} -> A {first}")
            if not await wait_for(call_id, "Answered", 1, ANSWER_WAIT):
                r["notes"].append(f"A did not answer in {ANSWER_WAIT} s")
                return
            t_a = time.time()
            r["a_answered"] = round(t_a - T0, 1)
            say(f"pair {idx}: A {first} answered, dialing B {second}")
            await asyncio.sleep(2)  # let A's leg settle before adding B
            try:
                leg = await calls.add_participant(
                    call_id,
                    from_,
                    second,
                    role="B",
                    end_mpc_on_exit=True,
                    idempotency_key=f"{call_id}:B",
                    **legs,
                )
                r["b_leg"] = leg.model_dump()
            except Exception as e:  # noqa: BLE001 - record it, the finally hangs A up
                r["notes"].append(
                    f"add_participant failed: {getattr(e, 'status_code', None)} {e}"
                )
                return
            if await wait_for(call_id, "Answered", 2, ANSWER_WAIT):
                r["b_answered"] = round(time.time() - T0, 1)
                say(f"pair {idx}: B {second} answered -> bridged, talk to each other")
            else:
                r["notes"].append(f"B did not answer in {ANSWER_WAIT} s")
                return
            if await wait_for(call_id, "MPCEnd", 1, a.max_wait - (time.time() - t_a)):
                end = got(call_id, "MPCEnd")[0]
                r["ended"] = round(end["ts"] - T0, 1)
                r["cause"] = end["event"].get("hangup_cause")
                r["duration"] = end["event"].get("duration")
            else:
                r["notes"].append("still up at max-wait: hung up by script")

        try:
            await wait_public(url)
            say(
                f"{len(pairs)} pair(s), dialing every A now. Answer; when your pair's "
                "second phone rings, answer it and talk to each other."
            )
            await asyncio.gather(
                *(run_pair(i + 1, A, B) for i, (A, B) in enumerate(pairs))
            )
        finally:
            for r in results.values():
                if r.get("call_id"):
                    with contextlib.suppress(Exception):
                        await calls.hangup(r["call_id"])
            await asyncio.sleep(5)  # late Hangup/MPCEnd callbacks
            bot.terminate()
            tunnel.terminate()
            with contextlib.suppress(Exception):
                bot.wait(5)
    ev = read_events(events)
    unverified = sum(
        1 for e in ev if e["kind"] == "callback" and e.get("verified") is False
    )
    summary = {"pairs": results, "unverified_callbacks": unverified, "limit": a.limit}
    (out / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
    shown = (
        "A",
        "B",
        "a_answered",
        "b_answered",
        "ended",
        "duration",
        "cause",
        "notes",
    )
    print(
        json.dumps(
            {k: {x: v.get(x) for x in shown} for k, v in results.items()}, indent=1
        )
    )
    print("unverified callbacks:", unverified)
    ok = all(r.get("b_answered") for r in results.values()) and not unverified
    return 0 if ok else 1


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("name")
    p.add_argument("--pair", action="append", required=True, help="+91A,+91B")
    p.add_argument("--from", dest="from_")
    p.add_argument("--limit", type=int, default=0, help="time_limit seconds per leg")
    p.add_argument("--max-wait", type=int, default=1800)
    sys.exit(asyncio.run(main(p.parse_args())))
