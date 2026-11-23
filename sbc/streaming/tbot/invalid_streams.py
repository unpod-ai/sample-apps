import asyncio
import json
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")
import unpod

URL = open(sys.argv[1]).read().strip()
EV = sys.argv[2]
CASES = sys.argv[3].split(",")


def events():
    out = []
    for l in open(EV):
        try:
            out.append(json.loads(l))
        except ValueError:
            pass
    return out


async def main():
    async with unpod.AsyncClient() as c:
        for case in CASES:
            scn = f"bad-{case}"
            call = await c.telephony.calls.create(
                os.environ["SBC_FROM_NUMBER"],
                os.environ["STREAM_TEST_TO"],
                answer_url=f"{URL}/answer?scn={scn}&bad={case}&after=5",
            )
            print(
                f"{time.strftime('%H:%M:%S')} {case}: placed {call.call_id}", flush=True
            )
            start = hang = None
            forced = False
            for _ in range(80):
                await asyncio.sleep(0.5)
                for e in events():
                    if (
                        e.get("scn") == scn
                        and e["kind"] == "answer"
                        and e.get("event") == "StartApp"
                        and start is None
                    ):
                        start = e["t"]
                    if (
                        e["kind"] == "answer"
                        and e.get("event") == "Hangup"
                        and start
                        and e["t"] > start
                        and hang is None
                        and e["params"].get("CallUUID", "").startswith(call.call_id)
                    ):
                        hang = e["t"]
                if hang:
                    break
                if start and time.time() - start > 20:
                    try:
                        await c.telephony.calls.hangup(call.call_id)
                    except Exception as ex:
                        print("  hangup err", ex)
                    forced = True
                    break
            ws = any(e["kind"] == "ws_open" and e.get("scn") == scn for e in events())
            cbs = [
                e["body"].get("Event")
                for e in events()
                if e["kind"] == "cb" and e.get("query", {}).get("scn") == scn
            ]
            res = (
                "never answered"
                if not start
                else (
                    f"ended by SBC after {hang - start:.1f}s"
                    if hang
                    else "STAYED UP >20s, forced hangup"
                    if forced
                    else "?"
                )
            )
            print(
                f"{time.strftime('%H:%M:%S')} {case}: {res}; ws_open={ws}; callbacks={cbs}",
                flush=True,
            )
            await asyncio.sleep(4)


asyncio.run(main())
