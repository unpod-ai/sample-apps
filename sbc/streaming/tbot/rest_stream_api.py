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
WSS = URL.replace("https://", "wss://")
ts = lambda: time.strftime("%H:%M:%S")


async def step(label, coro):
    try:
        r = await coro
        d = r.model_dump() if hasattr(r, "model_dump") else r
        print(f"{ts()} OK   {label}: {json.dumps(d, default=str)[:330]}", flush=True)
        return r
    except Exception as e:
        print(f"{ts()} ERR  {label}: {type(e).__name__} {str(e)[:200]}", flush=True)


async def main():
    async with unpod.AsyncClient() as c:
        k = c.telephony.calls
        call = await k.create(
            os.environ["SBC_FROM_NUMBER"],
            os.environ["STREAM_TEST_TO"],
            answer_url=f"{URL}/answer?scn=r22&plain=1&wait=90",
        )
        cid = call.call_id
        print(ts(), "CALL", cid, flush=True)
        kw = dict(
            bidirectional=True,
            content_type="audio/x-mulaw;rate=8000",
            status_callback_url=f"{URL}/cb/stream?scn=r22",
        )
        r1 = await step(
            "9.1 start_stream (waits for the answer)",
            k.start_stream(cid, f"{WSS}/ws?scn=r22a&mode=echo", **kw),
        )
        if not r1:
            return
        print(ts(), ">>> STREAM 1 ACTIVE: talk, you should hear your echo", flush=True)
        sid = r1.stream_id
        await asyncio.sleep(3)
        await step("9.2 list_streams", k.list_streams(cid))
        await step("9.3 get_stream", k.get_stream(cid, sid))
        await step(
            "2.8 second start while one is active (expect 400)",
            k.start_stream(cid, f"{WSS}/ws?scn=r22b&mode=echo", **kw),
        )
        await asyncio.sleep(3)
        await step("9.4 stop_stream one (expect 204)", k.stop_stream(cid, sid))
        await asyncio.sleep(1)
        await step(
            "list after stop (status stopped / api_request)", k.list_streams(cid)
        )
        print(ts(), ">>> echo should have stopped, call still up", flush=True)
        await asyncio.sleep(4)
        r2 = await step(
            "start second stream (call still up -> 202)",
            k.start_stream(cid, f"{WSS}/ws?scn=r22c&mode=echo", **kw),
        )
        print(ts(), ">>> STREAM 2 ACTIVE: echo again", flush=True)
        await asyncio.sleep(5)
        await step("9.5 stop_stream all (expect 204)", k.stop_stream(cid))
        await asyncio.sleep(3)
        await step("hangup", k.hangup(cid))
        print("DONE", flush=True)


asyncio.run(main())
