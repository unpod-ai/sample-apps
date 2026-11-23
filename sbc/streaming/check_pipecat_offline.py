"""Test Unpod audio streaming for Pipecat bots (SDK branch feat/pipecat-audio-streaming).

    PY=../SDK/unpod-python-sdk-stream/.venv/bin/python

    $PY 16-pipecat-streaming.py                  # offline self-test (default)
    $PY 16-pipecat-streaming.py -v               # same, printing every message
    $PY 16-pipecat-streaming.py --serve          # run the bot server for a live test
    $PY 16-pipecat-streaming.py --call +91XXXXXXXXXX --public-url https://<ngrok-host>

Run it with the venv of the SDK *worktree* (../SDK/unpod-python-sdk-stream):
that is where the branch lives and where pipecat-ai is installed. The
unpod-test venv and the main SDK checkout don't have these modules.

OFFLINE SELF-TEST (no SBC, no phone, no API keys)
--------------------------------------------------
Starts a real Pipecat bot server on 127.0.0.1 (FastAPI + Pipecat's own
FastAPIWebsocketTransport + UnpodFrameSerializer), then plays the part of
Unpod's SBC against it, exactly as the SBC will once its `stream` action ships:

  1. answer XML      GET /answer           -> <Stream ...>wss://.../ws</Stream>
  2. answer JSON     GET /answer?fmt=json  -> {"actions":[{"type":"stream",...}]}
  3. no signature    WS without X-Unpod-Signature    -> refused (close 1008)
  4. bad signature   WS signed with the wrong secret -> refused (close 1008)
  5. audio round trip  signed WS, `start` (mu-law 8k), 1 s of a 440 Hz tone in
                     20 ms `media` frames -> the echo bot plays it back as
                     `playAudio`; the returned audio must carry the tone
  6. DTMF            `dtmf` 5 -> the bot reports it saw digit 5
  7. barge-in        `dtmf` * -> the bot interrupts itself -> `clearAudio`
  8. hangup          `dtmf` # -> the bot ends -> UnpodFrameSerializer hangs up
                     the call (call id from `start`) and the socket closes

The bot is an echo bot (no STT/LLM/TTS), so the test needs no AI keys and
checks only the transport layer: the part this branch changed.

LIVE TEST (needs SuperSBC's `stream` action on the target SBC)
--------------------------------------------------------------
--serve   runs the same echo bot on --port (default 7860) with real hangup
          through the Unpod call API. Expose it (ngrok http 7860), then point a
          number at it once:  inbound.set_did(<number>, answer_url=https://<host>/answer)
          and call the number: you hear yourself echoed; press # to hang up.
          Needs UNPOD_WEBHOOK_SECRET (the org's webhook secret) in .env, or
          --no-verify to accept unsigned streams while testing.
--serve --local [--secret S]
          the same echo bot for a SuperSBC built and run on this machine
          (supersbc branch local/stream-verb); hangup is only logged.
          The SBC's e2e test drives it:
            cd <supersbc checkout>
            STREAM_E2E_BOT_URL=ws://127.0.0.1:7860/ws cargo test --profile deploy \
              --lib test_stream_verb_with_external_bot -- --nocapture
--call    places an outbound call whose answer_url is <public-url>/answer
          (start --serve first, in another terminal).

Until the SBC ships `stream`, a live call is rejected by the SBC with an
unknown-action error: that's expected, not a bug in this branch.

Target and auth come from .env (UNPOD_BASE_URL, UNPOD_PLATFORM_TOKEN,
UNPOD_ORG_HANDLE), used only by --serve/--call.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import math
import os
import socket
import struct
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

HERE = Path(__file__).resolve().parent
load_dotenv(HERE / ".env")

try:
    import uvicorn
    import websockets
    from fastapi import FastAPI, Request, WebSocket
    from fastapi.responses import JSONResponse, Response
    from pipecat.frames.frames import (
        EndFrame,
        InputAudioRawFrame,
        InputDTMFFrame,
        InterruptionFrame,
        OutputAudioRawFrame,
        OutputTransportMessageUrgentFrame,
    )
    from pipecat.pipeline.pipeline import Pipeline
    from pipecat.pipeline.worker import PipelineParams, PipelineWorker
    from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
    from pipecat.transports.websocket.fastapi import (
        FastAPIWebsocketParams,
        FastAPIWebsocketTransport,
    )
    from pipecat.workers.runner import WorkerRunner
    from unpod.pipecat import (
        UnpodFrameSerializer,
        parse_unpod_websocket,
        verify_stream_request,
    )
    from unpod.pipecat.g711 import pcm_to_ulaw, ulaw_to_pcm
    from unpod.telephony.answer import Response as Answer
except ImportError as exc:  # wrong interpreter
    sys.exit(
        f"import failed ({exc}).\nRun with the SDK worktree venv:\n"
        "  ../SDK/unpod-python-sdk-stream/.venv/bin/python 16-pipecat-streaming.py"
    )

TEST_SECRET = "whsec_local_selftest"
CALL_ID = "call-selftest-1"
STREAM_ID = "stream-selftest-1"
RATE = 8000
CHUNK = 160  # 20 ms of mu-law at 8 kHz
VERBOSE = False


def log(*a) -> None:
    if VERBOSE:
        print("   ", *a)


# ── the bot ─────────────────────────────────────────────────────────────────


class EchoBot(FrameProcessor):
    """Plays the caller's audio back. DTMF: # ends the call, * interrupts the
    bot (barge-in), any other digit is reported back as a message."""

    def __init__(self) -> None:
        super().__init__()
        self.worker: PipelineWorker | None = None

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, InputAudioRawFrame):
            await self.push_frame(
                OutputAudioRawFrame(
                    audio=frame.audio, sample_rate=frame.sample_rate, num_channels=1
                )
            )
        elif isinstance(frame, InputDTMFFrame):
            digit = frame.button.value
            if digit == "#" and self.worker:
                await self.worker.queue_frame(EndFrame())
            elif digit == "*":
                await self.push_frame(InterruptionFrame())
            else:
                await self.push_frame(
                    OutputTransportMessageUrgentFrame(
                        message={"event": "botSawDtmf", "digit": digit}
                    )
                )
        else:
            await self.push_frame(frame, direction)


def build_app(*, secret: str | None, hangup) -> FastAPI:
    """The customer's server: an answer endpoint and the stream endpoint."""
    app = FastAPI()
    app.state.hangups = []

    @app.api_route("/answer", methods=["GET", "POST"])
    async def answer(request: Request, fmt: str = "xml"):
        resp = Answer().stream(f"wss://{request.headers['host']}/ws")
        if fmt == "json":
            return JSONResponse(resp.to_json())
        return Response(resp.to_xml(), media_type="application/xml")

    @app.websocket("/ws")
    async def ws(websocket: WebSocket):
        if secret is not None and not verify_stream_request(
            websocket.headers, secret, url=str(websocket.url)
        ):
            await websocket.close(code=1008)
            return
        await websocket.accept()
        start = await parse_unpod_websocket(websocket)
        print(
            f"   stream from SBC: call={start.call_id} stream={start.stream_id} "
            f"format={start.media_format.encoding}@{start.media_format.sample_rate}"
        )

        async def record_and_hang_up(call_id: str) -> None:
            app.state.hangups.append(call_id)
            await hangup(call_id)

        transport = FastAPIWebsocketTransport(
            websocket=websocket,
            params=FastAPIWebsocketParams(
                audio_in_enabled=True,
                audio_out_enabled=True,
                add_wav_header=False,
                serializer=UnpodFrameSerializer.from_start(
                    start, hangup=record_and_hang_up
                ),
            ),
        )
        bot = EchoBot()
        worker = PipelineWorker(
            Pipeline([transport.input(), bot, transport.output()]),
            params=PipelineParams(
                audio_in_sample_rate=RATE, audio_out_sample_rate=RATE
            ),
        )
        bot.worker = worker
        runner = WorkerRunner(handle_sigint=False)
        await runner.add_workers(worker)

        @transport.event_handler("on_client_disconnected")
        async def _gone(transport, client):
            await runner.cancel()

        await runner.run()

    return app


# ── the fake SBC ────────────────────────────────────────────────────────────


def sign(secret: str, url: str) -> dict[str, str]:
    """The X-Unpod-Signature header SuperSBC puts on the stream upgrade."""
    import secrets

    from unpod.telephony.stream import SIGNATURE_HEADER, stream_signature

    ts, nonce = int(time.time()), secrets.token_hex(8)
    return {
        SIGNATURE_HEADER: f"t={ts},n={nonce},v1={stream_signature(secret, url, ts, nonce)}"
    }


def tone_ulaw(seconds: float, freq: float = 440.0) -> bytes:
    n = int(RATE * seconds)
    pcm = b"".join(
        struct.pack("<h", int(12000 * math.sin(2 * math.pi * freq * i / RATE)))
        for i in range(n)
    )
    return pcm_to_ulaw(pcm)


def rms(ulaw: bytes) -> float:
    pcm = ulaw_to_pcm(ulaw)
    samples = struct.unpack(f"<{len(pcm) // 2}h", pcm)
    return math.sqrt(sum(s * s for s in samples) / max(1, len(samples)))


class Checks:
    def __init__(self) -> None:
        self.rows: list[tuple[str, bool, str]] = []

    def add(self, name: str, ok: bool, detail: str = "") -> None:
        self.rows.append((name, ok, detail))
        print(
            f"  [{'PASS' if ok else 'FAIL'}] {name}"
            + (f" — {detail}" if detail else "")
        )

    @property
    def ok(self) -> bool:
        return all(ok for _, ok, _ in self.rows)


async def recv_until(ws, want, timeout: float = 5.0) -> dict | None:
    """Read messages until `want(msg)` is true; None on timeout or close."""
    deadline = time.monotonic() + timeout
    while (left := deadline - time.monotonic()) > 0:
        try:
            raw = await asyncio.wait_for(ws.recv(), left)
        except (TimeoutError, websockets.ConnectionClosed):
            return None
        msg = json.loads(raw)
        log("<-", msg["event"], {k: v for k, v in msg.items() if k != "media"})
        if want(msg):
            return msg
    return None


async def refused(url: str, headers: dict) -> tuple[bool, str]:
    try:
        async with websockets.connect(url, additional_headers=headers) as ws:
            await asyncio.wait_for(ws.recv(), 3)
            return False, "connection stayed open"
    except websockets.ConnectionClosed as e:
        code = e.rcvd.code if e.rcvd else None
        return code == 1008, f"closed with {code}"
    except websockets.InvalidStatus as e:  # refused before the upgrade
        return True, f"HTTP {e.response.status_code}"


async def self_test() -> bool:
    import httpx

    hangup_calls: list[str] = []

    async def fake_hangup(call_id: str) -> None:  # stands in for calls.hangup
        hangup_calls.append(call_id)

    app = build_app(secret=TEST_SECRET, hangup=fake_hangup)
    port = free_port()
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    )
    serving = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.05)
    base, ws_url = f"http://127.0.0.1:{port}", f"ws://127.0.0.1:{port}/ws"
    checks = Checks()
    print(f"bot server on {base}\n")

    try:
        async with httpx.AsyncClient() as http:
            xml = (await http.get(f"{base}/answer")).text
            checks.add(
                "1 answer XML",
                "<Stream" in xml
                and "/ws</Stream>" in xml
                and 'contentType="audio/x-mulaw;rate=8000"' in xml,
                xml.splitlines()[-1][:90],
            )
            js = (await http.get(f"{base}/answer", params={"fmt": "json"})).json()
            act = js["actions"][0]
            checks.add(
                "2 answer JSON",
                act["type"] == "stream" and act["url"].endswith("/ws"),
                json.dumps(act)[:90],
            )

        ok, why = await refused(ws_url, {})
        checks.add("3 unsigned stream refused", ok, why)
        ok, why = await refused(ws_url, sign("wrong-secret", ws_url))
        checks.add("4 wrongly signed stream refused", ok, why)

        async with websockets.connect(
            ws_url, additional_headers=sign(TEST_SECRET, ws_url)
        ) as sbc:
            seq = 1
            await sbc.send(
                json.dumps(
                    {
                        "event": "start",
                        "sequenceNumber": seq,
                        "start": {
                            "callId": CALL_ID,
                            "streamId": STREAM_ID,
                            "accountId": "org-selftest",
                            "tracks": ["inbound"],
                            "mediaFormat": {
                                "encoding": "audio/x-mulaw",
                                "sampleRate": RATE,
                            },
                        },
                    }
                )
            )

            # 5. stream 1 s of tone in real time, collect what the bot plays back
            played: list[dict] = []

            async def collect():
                async for raw in sbc:
                    msg = json.loads(raw)
                    if msg.get("event") == "playAudio":
                        played.append(msg)
                    if len(played) >= 25:
                        return

            collector = asyncio.create_task(collect())
            audio = tone_ulaw(1.0)
            for i in range(0, len(audio), CHUNK):
                seq += 1
                await sbc.send(
                    json.dumps(
                        {
                            "event": "media",
                            "sequenceNumber": seq,
                            "streamId": STREAM_ID,
                            "media": {
                                "track": "inbound",
                                "chunk": seq,
                                "timestamp": str(int(time.time() * 1000)),
                                "payload": base64.b64encode(
                                    audio[i : i + CHUNK]
                                ).decode(),
                            },
                        }
                    )
                )
                await asyncio.sleep(0.02)
            try:
                await asyncio.wait_for(collector, 5)
            except TimeoutError:
                collector.cancel()
            back = b"".join(base64.b64decode(m["media"]["payload"]) for m in played)
            shape_ok = bool(played) and all(
                m["streamId"] == STREAM_ID
                and m["media"]["contentType"] == "audio/x-mulaw"
                and m["media"]["sampleRate"] == RATE
                for m in played
            )
            level = rms(back) if back else 0.0
            checks.add(
                "5 audio round trip (tone echoed as playAudio)",
                shape_ok and level > 2000,
                f"{len(played)} playAudio frames, {len(back)} bytes, level {level:.0f}",
            )

            async def dtmf(digit: str):
                nonlocal seq
                seq += 1
                await sbc.send(
                    json.dumps(
                        {
                            "event": "dtmf",
                            "sequenceNumber": seq,
                            "streamId": STREAM_ID,
                            "dtmf": {"track": "inbound", "digit": digit},
                        }
                    )
                )

            await dtmf("5")
            msg = await recv_until(sbc, lambda m: m.get("event") == "botSawDtmf")
            checks.add(
                "6 DTMF reaches the bot",
                bool(msg and msg["digit"] == "5"),
                f"bot saw {msg['digit']}" if msg else "no reply",
            )

            await dtmf("*")
            msg = await recv_until(sbc, lambda m: m.get("event") == "clearAudio")
            checks.add(
                "7 barge-in sends clearAudio",
                bool(msg and msg["streamId"] == STREAM_ID),
                json.dumps(msg) if msg else "no clearAudio",
            )

            await dtmf("#")
            closed = await recv_until(sbc, lambda m: False, timeout=5) is None
            await asyncio.sleep(0.2)
            checks.add(
                "8 bot ends -> call hung up via call API",
                hangup_calls == [CALL_ID] and closed,
                f"hangup({hangup_calls}), socket closed={closed}",
            )
    finally:
        server.should_exit = True
        await serving

    passed = sum(ok for _, ok, _ in checks.rows)
    print(f"\n{passed}/{len(checks.rows)} checks passed")
    return checks.ok


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ── live modes ──────────────────────────────────────────────────────────────


async def serve_local(port: int, secret: str | None) -> None:
    """--serve --local: the echo bot for a locally built SBC. Hangup is a
    log line (nothing is sent to the Unpod API); the SBC ends the call itself
    when the bot closes the stream."""

    async def hangup(call_id: str) -> None:
        print(f"   bot ended -> hangup({call_id}) [local: not sent to the Unpod API]")

    app = build_app(secret=secret, hangup=hangup)
    print(
        f"local echo bot on ws://127.0.0.1:{port}/ws  (signature check: {'on' if secret else 'off'})"
    )
    await uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    ).serve()


async def serve(port: int, verify: bool) -> None:
    from unpod import AsyncClient

    secret = os.getenv("UNPOD_WEBHOOK_SECRET")
    if verify and not secret:
        sys.exit(
            "set UNPOD_WEBHOOK_SECRET in .env (the org's webhook secret), or pass --no-verify"
        )
    async with AsyncClient() as client:

        async def hangup(call_id: str) -> None:
            await client.telephony.calls.hangup(call_id)

        app = build_app(secret=secret if verify else None, hangup=hangup)
        print(
            f"echo bot on 0.0.0.0:{port}  (answer_url = https://<public-host>/answer)"
        )
        print("call the number: you hear yourself; press 5 / * / # (# hangs up)")
        await uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=port)).serve()


async def call(to: str, public_url: str, from_: str | None) -> None:
    from unpod import AsyncClient

    from_ = from_ or os.getenv("UNPOD_FROM_NUMBER")
    if not from_:
        sys.exit("pass --from or set UNPOD_FROM_NUMBER")
    async with AsyncClient() as client:
        c = await client.telephony.calls.create(
            from_, to, answer_url=public_url.rstrip("/") + "/answer"
        )
        print(f"call {c.call_id} {c.state}: answer it, you should hear yourself echoed")


def main() -> None:
    global VERBOSE
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument(
        "--serve", action="store_true", help="run the echo bot for a live test"
    )
    ap.add_argument("--port", type=int, default=7860)
    ap.add_argument(
        "--no-verify", action="store_true", help="--serve: accept unsigned streams"
    )
    ap.add_argument(
        "--local",
        action="store_true",
        help="--serve: for a locally built SBC; no Unpod API calls",
    )
    ap.add_argument("--secret", help="--serve --local: verify streams with this secret")
    ap.add_argument("--call", metavar="TO", help="place an outbound call to TO (E.164)")
    ap.add_argument(
        "--public-url", help="--call: public https URL of the --serve server"
    )
    ap.add_argument(
        "--from", dest="from_", help="--call: caller id (else UNPOD_FROM_NUMBER)"
    )
    args = ap.parse_args()
    VERBOSE = args.verbose

    if args.serve and args.local:
        asyncio.run(serve_local(args.port, args.secret))
    elif args.serve:
        asyncio.run(serve(args.port, verify=not args.no_verify))
    elif args.call:
        if not args.public_url:
            ap.error("--call needs --public-url")
        asyncio.run(call(args.call, args.public_url, args.from_))
    else:
        sys.exit(0 if asyncio.run(self_test()) else 1)


if __name__ == "__main__":
    main()
