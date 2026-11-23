"""Fake SBC: plays the SBC's side of a <Stream> WebSocket against bot.py, to prove the
harness locally before any real call. Signs the upgrade like SuperSBC does.

  python fake_stream_sbc.py --bot ws://127.0.0.1:28090 --public wss://bot.example.com \
      --secret S --path '/ws?token=abc&mode=echo' --enc mulaw --seconds 3 [--dtmf 12#]
Prints one JSON summary line.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import math
import secrets
import struct
import time

import websockets
from unpod.pipecat.g711 import alaw_to_pcm, pcm_to_alaw, pcm_to_ulaw, ulaw_to_pcm  # noqa: F401
from unpod.telephony import stream as st

CT = {
    "mulaw": (st.MULAW, 8000),
    "alaw": (st.ALAW, 8000),
    "l16_8": (st.L16, 8000),
    "l16_16": (st.L16, 16000),
    "l16_24": (st.L16, 24000),
}


def tone(enc: str, rate: int, k: int) -> bytes:
    pcm = b"".join(
        struct.pack(
            "<h", int(10000 * math.sin(2 * math.pi * 440 * (k * rate // 50 + i) / rate))
        )
        for i in range(rate // 50)
    )
    return (
        pcm_to_ulaw(pcm)
        if enc == st.MULAW
        else pcm_to_alaw(pcm)
        if enc == st.ALAW
        else pcm
    )


async def run(a) -> dict:
    enc, rate = CT[a.enc]
    t, n = int(time.time()) + a.skew, secrets.token_hex(8)
    signed_url = a.public + a.path
    sig = st.stream_signature(a.secret, signed_url, t, n)
    headers = {"X-Unpod-Signature": f"t={t},n={n},v1={sig}"}
    if a.bad_sig:
        headers["X-Unpod-Signature"] = f"t={t},n={n},v1={'0' * 64}"
    if a.no_sig:
        headers = {}
    out = {"upgrade": None, "sent": 0, "play_bytes": 0, "events": [], "close": None}
    try:
        async with websockets.connect(
            a.bot + a.path, additional_headers=headers, max_size=16 * 1024 * 1024
        ) as ws:
            out["upgrade"] = "ok"
            sid = "fake-stream-1"
            start = {
                "event": "start",
                "sequenceNumber": 1,
                "streamId": sid,
                "extra_headers": a.extra,
                "start": {
                    "callId": "fakecall-caller",
                    "streamId": sid,
                    "accountId": "qa_org",
                    "tracks": ["inbound"],
                    "mediaFormat": {"encoding": enc, "sampleRate": rate},
                },
            }
            await ws.send(json.dumps(start))
            seq = 1

            async def reader():
                try:
                    async for raw in ws:
                        m = json.loads(raw)
                        ev = m.get("event")
                        if ev == "playAudio":
                            out["play_bytes"] += len(
                                base64.b64decode(m["media"]["payload"])
                            )
                            out["events"].append("playAudio") if "playAudio" not in out[
                                "events"
                            ] else None
                        else:
                            out["events"].append(ev)
                            if ev == "checkpoint":
                                await ws.send(
                                    json.dumps(
                                        {
                                            "event": "playedStream",
                                            "streamId": sid,
                                            "name": m["name"],
                                        }
                                    )
                                )
                            if ev == "clearAudio":
                                await ws.send(
                                    json.dumps(
                                        {"event": "clearedAudio", "streamId": sid}
                                    )
                                )
                except websockets.ConnectionClosed:
                    pass

            rt = asyncio.create_task(reader())
            if a.dtmf:
                for d in a.dtmf:
                    seq += 1
                    await ws.send(
                        json.dumps(
                            {
                                "event": "dtmf",
                                "sequenceNumber": seq,
                                "streamId": sid,
                                "dtmf": {"digit": d},
                            }
                        )
                    )
            t0 = time.time()
            k = 0
            while time.time() - t0 < a.seconds and not rt.done():
                seq += 1
                k += 1
                await ws.send(
                    json.dumps(
                        {
                            "event": "media",
                            "sequenceNumber": seq,
                            "streamId": sid,
                            "extra_headers": a.extra,
                            "media": {
                                "track": "inbound",
                                "timestamp": str(k * 20),
                                "chunk": str(k),
                                "payload": base64.b64encode(
                                    tone(enc, rate, k)
                                ).decode(),
                            },
                        }
                    )
                )
                out["sent"] += 1
                await asyncio.sleep(0.02)
            if not rt.done():
                await ws.close(code=1000)
            await asyncio.wait_for(rt, 3)
            out["close"] = ws.close_code
    except websockets.InvalidStatus as e:
        out["upgrade"] = f"refused HTTP {e.response.status_code}"
    except Exception as e:
        out["upgrade"] = out["upgrade"] or f"error {type(e).__name__}: {e}"
        out["close"] = out["close"] or "n/a"
    out["events"] = sorted(set(out["events"]))
    return out


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bot", default="ws://127.0.0.1:28090")
    p.add_argument("--public", default="wss://bot.example.com")
    p.add_argument("--secret", required=True)
    p.add_argument("--path", default="/ws?token=abc&mode=echo")
    p.add_argument("--enc", default="mulaw", choices=list(CT))
    p.add_argument("--seconds", type=float, default=3)
    p.add_argument("--dtmf", default="")
    p.add_argument("--extra", default="")
    p.add_argument("--skew", type=int, default=0)
    p.add_argument("--bad-sig", action="store_true")
    p.add_argument("--no-sig", action="store_true")
    print(json.dumps(asyncio.run(run(p.parse_args()))))
