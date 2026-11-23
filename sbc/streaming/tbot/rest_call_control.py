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
ts = lambda: time.strftime("%H:%M:%S")


async def step(label, coro):
    try:
        r = await coro
        d = r.model_dump() if hasattr(r, "model_dump") else r
        print(f"{ts()} OK   {label}: {json.dumps(d, default=str)[:260]}", flush=True)
        return r
    except Exception as e:
        print(f"{ts()} ERR  {label}: {type(e).__name__} {str(e)[:200]}", flush=True)


async def main():
    async with unpod.AsyncClient() as c:
        k = c.telephony.calls
        call = await k.create(
            os.environ["SBC_FROM_NUMBER"],
            os.environ["STREAM_TEST_TO"],
            answer_url=f"{URL}/answer?scn=r23&mode=echo&kca=true&bidi=true",
        )
        cid = call.call_id
        print(ts(), "CALL", cid, flush=True)
        for _ in range(60):
            try:
                if await k.list_streams(cid):
                    break
            except Exception:
                pass
            await asyncio.sleep(1)
        else:
            print("never answered")
            return
        print(ts(), ">>> STREAM ACTIVE, keep talking: echo 5 s", flush=True)
        await asyncio.sleep(5)
        await step("10.3 hold on POST /calls call (expect 400)", k.hold(cid))
        await asyncio.sleep(2)
        print(ts(), ">>> 1) MUTE: echo should stop", flush=True)
        await step("10.2 mute callee", k.mute(cid, "callee"))
        await asyncio.sleep(5)
        print(ts(), ">>> 2) UNMUTE: echo should return", flush=True)
        await step("10.2 unmute callee", k.unmute(cid, "callee"))
        await asyncio.sleep(5)
        print(
            ts(),
            ">>> 3) BEEP mix=True (3 s): is your echo still audible under it?",
            flush=True,
        )
        await step(
            "10.4 play_audio mix=True",
            k.play_audio(cid, url=f"{URL}/beep.wav", leg_id="callee", mix=True),
        )
        await asyncio.sleep(7)
        print(
            ts(),
            ">>> 4) BEEP mix=False (3 s): is your echo audible under it?",
            flush=True,
        )
        await step(
            "10.4 play_audio mix=False",
            k.play_audio(cid, url=f"{URL}/beep.wav", leg_id="callee", mix=False),
        )
        await asyncio.sleep(7)
        print(ts(), ">>> 5) DTMF 123# (default leg): any keypad tones?", flush=True)
        await step("10.5 send_dtmf 123# default leg", k.send_dtmf(cid, "123#"))
        await asyncio.sleep(5)
        print(ts(), ">>> 6) DTMF 456 (leg callee): any keypad tones?", flush=True)
        await step(
            "10.5 send_dtmf 456 leg callee", k.send_dtmf(cid, "456", leg_id="callee")
        )
        await asyncio.sleep(5)
        print(ts(), ">>> 7) hanging up through the API", flush=True)
        await step("10.6 hangup", k.hangup(cid))
        print("DONE", flush=True)


asyncio.run(main())
