import asyncio
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")
import unpod

URL = open(sys.argv[1]).read().strip()
OUT = Path(sys.argv[2])


def ts():
    return time.strftime("%H:%M:%S")


async def main():
    async with unpod.AsyncClient() as c:
        calls = c.telephony.calls
        call = await calls.create(
            os.environ["SBC_FROM_NUMBER"],
            os.environ["STREAM_TEST_TO"],
            answer_url=f"{URL}/answer?scn=s15&mode=echo&kca=true&bidi=true",
        )
        cid = call.call_id
        print(ts(), "CALL", cid, flush=True)
        for _ in range(60):
            try:
                if await calls.list_streams(cid):
                    break
            except Exception:
                pass
            await asyncio.sleep(1)
        else:
            print("never answered")
            return
        print(
            ts(),
            "STREAM ACTIVE; TALK NOW: count aloud steadily 'one two three ...'",
            flush=True,
        )
        await asyncio.sleep(1)
        try:
            await calls.record(cid)
            print(ts(), "recording started", flush=True)
        except Exception as e:
            print(ts(), "record err", e, flush=True)
        await asyncio.sleep(6)
        print(ts(), "BEEP (mix=True) starts", flush=True)
        r = await calls.play_audio(
            cid, url=f"{URL}/beep.wav", leg_id="callee", mix=True
        )
        print(ts(), "play_id", r.play_id, flush=True)
        await asyncio.sleep(9)
        try:
            await calls.stop_recording(cid)
        except Exception as e:
            print(ts(), "stop_recording err", e, flush=True)
        await calls.hangup(cid)
        print(ts(), "hangup sent", flush=True)
        await asyncio.sleep(4)
        for _ in range(10):
            try:
                p = await calls.download_recording(cid, OUT)
                print(ts(), "recording saved", p, p.stat().st_size, flush=True)
                break
            except Exception as e:
                print(ts(), "download retry:", str(e)[:100], flush=True)
                await asyncio.sleep(3)


asyncio.run(main())
