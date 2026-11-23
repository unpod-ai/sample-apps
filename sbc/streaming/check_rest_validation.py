import asyncio
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")
import unpod

GHOST = "00000000-0000-4000-8000-000000000000"


async def t(label, coro):
    try:
        r = await coro
        print(f"OK   {label}: {str(r)[:120]}", flush=True)
    except Exception as e:
        print(f"ERR  {label}: {type(e).__name__} {str(e)[:150]}", flush=True)


async def main():
    async with unpod.AsyncClient() as c:
        k = c.telephony.calls
        await t("9.6 list_streams unknown id", k.list_streams(GHOST))
        await t("9.6 get_stream unknown id", k.get_stream(GHOST, "nope"))
        await t("9.6 stop_stream unknown id", k.stop_stream(GHOST))
        await t(
            "start_stream unknown id, valid wss URL",
            k.start_stream(GHOST, "wss://example.com/ws"),
        )
        await t(
            "start_stream unknown id, wss://127.0.0.1",
            k.start_stream(GHOST, "wss://127.0.0.1/ws"),
        )
        await t(
            "start_stream unknown id, http:// URL",
            k.start_stream(GHOST, "http://example.com/ws"),
        )
        await t(
            "start_stream unknown id, 2100-char URL",
            k.start_stream(GHOST, "wss://example.com/" + "a" * 2100),
        )
        await t("start_stream unknown id, empty URL", k.start_stream(GHOST, ""))


asyncio.run(main())
