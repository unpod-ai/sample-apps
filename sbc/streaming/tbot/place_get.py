import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")
import unpod


async def main():
    async with unpod.AsyncClient() as c:
        call = await c.telephony.calls.create(
            os.environ["SBC_FROM_NUMBER"],
            os.environ["STREAM_TEST_TO"],
            answer_url=f"{sys.argv[1]}/answer?scn=s12&plain=1&wait=3",
            answer_method="GET",
        )
        print("call", call.call_id, call.state)


asyncio.run(main())
