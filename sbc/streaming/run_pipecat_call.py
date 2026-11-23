"""Real phone call on QA through Unpod streaming -> Pipecat bot (UnpodFrameSerializer).

One command does the whole live test:

    python 17-qa-stream-call.py +91XXXXXXXXXX           # echo bot: you hear yourself
    python 17-qa-stream-call.py +91XXXXXXXXXX --ai      # AI agent: Deepgram -> Claude -> Cartesia

Any python works: the script re-runs itself with the SDK worktree venv
(../SDK/unpod-python-sdk-stream/.venv), where Pipecat and the branch live.

What it does:
  1. starts the bot server on 127.0.0.1:<port> (answer endpoint + stream WebSocket)
  2. opens a public cloudflared tunnel to it (the SBC needs a public wss:// URL)
  3. places a call on QA:  calls.create(from, to, answer_url=<tunnel>/answer)
  4. QA's SBC asks /answer -> gets <Stream keepCallAlive="true" ...>wss://<tunnel>/ws</Stream>
     -> opens the WebSocket -> UnpodFrameSerializer turns it into Pipecat frames
  5. when the call ends, prints a PASS/FAIL checklist of every step

On the call (echo bot): speak and hear yourself; 5 = bot logs the digit,
* = bot interrupts itself (clearAudio), # = bot ends the call via the Unpod API.
AI bot: talk to it; # ends the call.

Config (unpod-test/.env): UNPOD_BASE_URL, UNPOD_PLATFORM_TOKEN, UNPOD_ORG_HANDLE,
optional UNPOD_FROM_NUMBER (else --from, else SBC_FROM_NUMBER) and
UNPOD_WEBHOOK_SECRET (to check the SBC's X-Unpod-Signature on the stream).
AI keys (--ai) come from supervoice/.env: DEEPGRAM_API_KEY, ANTHROPIC_API_KEY, CARTESIA_API_KEY.

Needs the SBC with streaming (supersbc feat/unpo-487-streaming, on QA) and
`audio_stream_enabled` on for the org there; without it the stream fails at once.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import contextlib
import json
import math
import os
import re
import shutil
import struct
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parents[1]  # sbc/


from dotenv import load_dotenv

load_dotenv(HERE / ".env")
load_dotenv(HERE.parent / ".env")  # AI keys only; .env above wins

try:
    import httpx
    import uvicorn
    from fastapi import FastAPI, Request, WebSocket
    from fastapi.responses import PlainTextResponse, Response
    from pipecat.frames.frames import (
        EndFrame,
        InputAudioRawFrame,
        InputDTMFFrame,
        InterruptionFrame,
        OutputAudioRawFrame,
    )
    from pipecat.pipeline.pipeline import Pipeline
    from pipecat.pipeline.worker import PipelineParams, PipelineWorker
    from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
    from pipecat.transports.websocket.fastapi import (
        FastAPIWebsocketParams,
        FastAPIWebsocketTransport,
    )
    from pipecat.workers.runner import WorkerRunner
    from unpod import AsyncClient
    from unpod.pipecat import (
        UnpodFrameSerializer,
        parse_unpod_websocket,
        verify_stream_request,
    )
    from unpod.pipecat.g711 import ulaw_to_pcm
    from unpod.telephony.answer import Response as Answer
    from unpod.telephony.stream import decode_message
except ImportError as exc:
    sys.exit(f"import failed ({exc}) in {sys.executable}")

RATE = 8000
DEFAULT_FROM = os.environ.get("SBC_FROM_NUMBER", "")


async def request_params(request) -> dict:
    """Query + form/JSON body, without needing python-multipart."""
    from urllib.parse import parse_qsl

    params = dict(request.query_params)
    body = await request.body()
    if body:
        try:
            data = json.loads(body)
            if isinstance(data, dict):
                params.update({k: str(v) for k, v in data.items()})
        except ValueError:
            params.update(dict(parse_qsl(body.decode(errors="replace"))))
    return params


def now() -> str:
    return time.strftime("%H:%M:%S")


def say(msg: str) -> None:
    print(f"{now()}  {msg}", flush=True)


# ── what happened on the call (filled in as it goes, printed at the end) ────


class Report:
    def __init__(self) -> None:
        self.tunnel = ""
        self.call_id = ""
        self.call_error = ""
        self.answer_hits: list[str] = []
        self.status_events: list[str] = []
        self.ws_opened = False
        self.signature = "not checked (no UNPOD_WEBHOOK_SECRET)"
        self.signature_ok: bool | None = None
        self.start: dict = {}
        self.media_in = 0
        self.media_out_track = 0
        self.loud_frames = 0
        self.play_audio = 0
        self.clear_audio = 0
        self.dtmf: list[str] = []
        self.unknown_events: dict[str, int] = {}
        self.bot_hangup: list[str] = []
        self.ended_by = ""
        self.t_open = 0.0
        self.t_close = 0.0
        self.done = asyncio.Event()

    def print(self) -> bool:
        rows: list[tuple[str, bool | None, str]] = [
            ("tunnel up", bool(self.tunnel), self.tunnel),
            ("call placed on QA", bool(self.call_id), self.call_id or self.call_error),
            (
                "SBC fetched answer_url",
                bool(self.answer_hits),
                "; ".join(self.answer_hits[:2]),
            ),
            ("SBC opened the stream WebSocket", self.ws_opened, ""),
            ("stream signature", self.signature_ok, self.signature),
            (
                "start event",
                bool(self.start),
                ", ".join(f"{k}={v}" for k, v in self.start.items()),
            ),
            (
                "caller audio reached the bot",
                self.media_in > 0,
                f"{self.media_in} media frames ({self.media_in * 20 / 1000:.1f}s), "
                f"{self.loud_frames} with voice",
            ),
            (
                "bot audio sent back (playAudio)",
                self.play_audio > 0,
                f"{self.play_audio} frames",
            ),
            (
                "DTMF",
                None if not self.dtmf else True,
                " ".join(self.dtmf) or "none pressed",
            ),
            (
                "barge-in (clearAudio)",
                None if not self.clear_audio else True,
                f"{self.clear_audio} sent",
            ),
            (
                "bot hangup via Unpod API",
                None if not self.bot_hangup else True,
                ", ".join(self.bot_hangup) or "not used",
            ),
            ("call ended", bool(self.ended_by), self.ended_by),
        ]
        if self.status_events:
            rows.append(
                ("stream status callbacks", True, ", ".join(self.status_events))
            )
        if self.media_out_track:
            rows.append(
                ("outbound-track frames ignored", True, str(self.media_out_track))
            )
        if self.unknown_events:
            rows.append(("other SBC events", True, str(self.unknown_events)))
        print("\n" + "=" * 72 + "\nRESULT\n" + "=" * 72)
        for name, ok, detail in rows:
            tag = "INFO" if ok is None else ("PASS" if ok else "FAIL")
            print(f"  [{tag}] {name}" + (f" — {detail}" if detail else ""))
        required = [ok for _, ok, _ in rows[:8] if ok is not None]
        passed = all(required)
        print(
            "\n"
            + (
                "PASS: the call streamed through UnpodFrameSerializer"
                if passed
                else "FAIL: see the first FAIL above (hints below)"
            )
        )
        if not passed:
            print(hints(self))
        return passed


def hints(r: Report) -> str:
    if not r.call_id:
        return "  - calls.create failed: check token/org in .env and the --from number."
    if not r.answer_hits:
        return (
            "  - the SBC never asked /answer: the call may not have been answered, or the\n"
            "    backend did not pass answer_url to the SBC."
        )
    if not r.ws_opened:
        return (
            "  - /answer was fetched but no WebSocket came: streaming is probably off for the\n"
            "    org on QA (audio_stream_enabled) — ask Anuj; or the SBC refused the URL."
        )
    if not r.media_in:
        return "  - stream opened but no audio: check the SBC logs for this call."
    return "  - bot audio missing: check the bot log above."


# ── serializer that also counts what goes over the wire ─────────────────────


class CountingSerializer(UnpodFrameSerializer):
    report: Report

    async def deserialize(self, data):
        msg = decode_message(data)
        ev = msg.get("event")
        if ev == "media":
            media = msg.get("media") or {}
            if media.get("track", "inbound") != "inbound":
                self.report.media_out_track += 1
            else:
                self.report.media_in += 1
                with contextlib.suppress(Exception):
                    pcm = ulaw_to_pcm(base64.b64decode(media.get("payload", "")))
                    n = len(pcm) // 2
                    if n:
                        s = struct.unpack(f"<{n}h", pcm)
                        if math.sqrt(sum(x * x for x in s) / n) > 500:
                            self.report.loud_frames += 1
                if self.report.media_in == 1:
                    say("<- first caller audio frame")
        elif ev == "dtmf":
            d = (msg.get("dtmf") or {}).get("digit", "?")
            self.report.dtmf.append(str(d))
            say(f"<- DTMF {d}")
        elif ev:
            self.report.unknown_events[ev] = self.report.unknown_events.get(ev, 0) + 1
        return await super().deserialize(data)

    async def serialize(self, frame):
        out = await super().serialize(frame)
        if isinstance(out, str):
            if '"playAudio"' in out:
                self.report.play_audio += 1
                if self.report.play_audio == 1:
                    say("-> first bot audio frame (playAudio)")
            elif '"clearAudio"' in out:
                self.report.clear_audio += 1
                say("-> clearAudio (barge-in)")
        return out


# ── bots ────────────────────────────────────────────────────────────────────


class EchoBot(FrameProcessor):
    """Plays the caller back. 5 -> logged, * -> interrupt, # -> end the call."""

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
                say("   # -> bot ends the call")
                await self.worker.queue_frame(EndFrame())
            elif digit == "*":
                await self.push_frame(InterruptionFrame())
        else:
            await self.push_frame(frame, direction)


def ai_pipeline(transport, on_hash):
    """Deepgram -> Claude -> Cartesia, like local-sbc/ai_bot.py."""
    from pipecat.audio.vad.silero import SileroVADAnalyzer
    from pipecat.frames.frames import (
        LLMFullResponseEndFrame,
        LLMTextFrame,
        TranscriptionFrame,
    )
    from pipecat.processors.aggregators.llm_context import LLMContext
    from pipecat.processors.aggregators.llm_response_universal import (
        LLMContextAggregatorPair,
        LLMUserAggregatorParams,
    )
    from pipecat.services.anthropic.llm import AnthropicLLMService
    from pipecat.services.cartesia.tts import CartesiaTTSService
    from pipecat.services.deepgram.stt import DeepgramSTTService

    class Transcript(FrameProcessor):
        def __init__(self) -> None:
            super().__init__()
            self.buf: list[str] = []

        async def process_frame(self, frame, direction):
            await super().process_frame(frame, direction)
            if isinstance(frame, TranscriptionFrame) and frame.text.strip():
                say(f"   caller: {frame.text}")
            elif isinstance(frame, LLMTextFrame):
                self.buf.append(frame.text)
            elif isinstance(frame, LLMFullResponseEndFrame) and self.buf:
                say(f"   agent:  {''.join(self.buf).strip()}")
                self.buf = []
            await self.push_frame(frame, direction)

    prompt = (
        "You are Unpod's friendly test assistant on a phone call. Answer in one or two "
        "short spoken sentences, no lists or symbols. To end the call the caller presses hash."
    )
    stt = DeepgramSTTService(
        api_key=os.environ["DEEPGRAM_API_KEY"],
        settings=DeepgramSTTService.Settings(language="en"),
    )
    llm = AnthropicLLMService(
        api_key=os.environ["ANTHROPIC_API_KEY"],
        settings=AnthropicLLMService.Settings(
            model="claude-haiku-5-5", system_instruction=prompt
        ),
    )
    tts = CartesiaTTSService(
        api_key=os.environ["CARTESIA_API_KEY"],
        settings=CartesiaTTSService.Settings(
            voice="71a7ad14-091c-4e8e-a314-022ece01c121", language="en"
        ),
    )
    context = LLMContext()
    user_agg, asst_agg = LLMContextAggregatorPair(
        context, user_params=LLMUserAggregatorParams(vad_analyzer=SileroVADAnalyzer())
    )
    pipeline = Pipeline(
        [
            transport.input(),
            on_hash,
            stt,
            Transcript(),
            user_agg,
            llm,
            Transcript(),
            tts,
            transport.output(),
            asst_agg,
        ]
    )
    return pipeline, context


class HangupOnHash(FrameProcessor):
    def __init__(self) -> None:
        super().__init__()
        self.worker: PipelineWorker | None = None

    async def process_frame(self, frame, direction):
        await super().process_frame(frame, direction)
        if (
            isinstance(frame, InputDTMFFrame)
            and frame.button.value == "#"
            and self.worker
        ):
            say("   # -> bot ends the call")
            await self.worker.queue_frame(EndFrame())
            return
        await self.push_frame(frame, direction)


# ── the customer's server ───────────────────────────────────────────────────


def build_app(
    report: Report, client: AsyncClient, *, ai: bool, secret: str | None, strict: bool
) -> FastAPI:
    app = FastAPI()

    @app.get("/health")
    async def health():
        return PlainTextResponse("ok")

    @app.api_route("/answer", methods=["GET", "POST"])
    async def answer(request: Request):
        params = await request_params(request)
        keys = {
            k: params[k]
            for k in ("CallUUID", "Direction", "From", "To", "Event", "CallStatus")
            if k in params
        }
        report.answer_hits.append(f"{request.method} {keys}")
        say(f"<- SBC {request.method} /answer {keys}")
        say(f"   full params: {dict(params)}")
        if params.get("Event", "StartApp") != "StartApp":  # Hangup etc.: acknowledge
            return PlainTextResponse("ok")
        host = request.headers.get("x-forwarded-host") or request.headers["host"]
        if os.getenv("LEGACY_XML"):
            xml = (
                '<?xml version="1.0" encoding="UTF-8"?><Response><Stream bidirectional="true" '
                'contentType="audio/x-mulaw;rate=8000" '
                f'statusCallbackUrl="https://{host}/status">wss://{host}/ws</Stream></Response>'
            )
            say(f"   LEGACY_XML -> {xml}")
            return Response(xml, media_type="application/xml")
        resp = Answer().stream(
            f"wss://{host}/ws", status_callback_url=f"https://{host}/status"
        )
        return Response(resp.to_xml(), media_type="application/xml")

    @app.api_route("/status", methods=["GET", "POST"])
    async def status(request: Request):
        params = await request_params(request)
        ev = (
            params.get("Event")
            or params.get("event")
            or params.get("StreamStatus")
            or "?"
        )
        detail = (
            params.get("Status")
            or params.get("StreamStatus")
            or params.get("Reason")
            or ""
        )
        report.status_events.append(f"{ev}{'/' + detail if detail else ''}")
        say(f"<- stream status callback: {ev} {detail}")
        say(f"   full params: {dict(params)}")
        return PlainTextResponse("ok")

    @app.websocket("/ws")
    async def ws(websocket: WebSocket):
        report.ws_opened = True
        report.t_open = time.monotonic()
        has_v3 = "x-unpod-signature" in {k.lower() for k in websocket.headers}
        if secret:
            ok = verify_stream_request(
                websocket.headers,
                secret,
                url=str(websocket.url),
                public_base=report.tunnel.replace("https://", "wss://"),
            )
            report.signature_ok = ok
            report.signature = (
                "valid X-Unpod-Signature"
                if ok
                else "INVALID"
                if has_v3
                else "no signature headers"
            )
            if not ok and strict:
                say("!! stream refused: bad signature")
                await websocket.close(code=1008)
                return
        else:
            report.signature = (
                "X-Unpod-Signature present (not checked: no UNPOD_WEBHOOK_SECRET)"
                if has_v3
                else "no signature headers (org has no webhook secret?)"
            )
        await websocket.accept()
        say(f"<- SBC opened the stream ({report.signature})")
        start = await parse_unpod_websocket(websocket)
        fmt = start.media_format
        report.start = {
            "callId": start.call_id,
            "streamId": start.stream_id[:8] + "…",
            "accountId": start.account_id,
            "format": f"{fmt.encoding}@{fmt.sample_rate}",
            "tracks": "/".join(start.tracks),
        }
        say(f"<- start {report.start}")

        async def hangup(call_id: str) -> None:
            report.bot_hangup.append(call_id)
            say(f"-> hangup({call_id}) via Unpod API")
            await client.telephony.calls.hangup(call_id)

        ser = CountingSerializer.from_start(start, hangup=hangup)
        ser.report = report
        transport = FastAPIWebsocketTransport(
            websocket=websocket,
            params=FastAPIWebsocketParams(
                audio_in_enabled=True,
                audio_out_enabled=True,
                add_wav_header=False,
                serializer=ser,
            ),
        )
        if ai:
            hash_key = HangupOnHash()
            pipeline, context = ai_pipeline(transport, hash_key)
        else:
            hash_key = EchoBot()
            pipeline, context = (
                Pipeline([transport.input(), hash_key, transport.output()]),
                None,
            )
        worker = PipelineWorker(
            pipeline,
            params=PipelineParams(
                audio_in_sample_rate=RATE, audio_out_sample_rate=RATE
            ),
        )
        hash_key.worker = worker
        runner = WorkerRunner(handle_sigint=False)
        await runner.add_workers(worker)

        if context is not None:
            from pipecat.frames.frames import LLMRunFrame

            @transport.event_handler("on_client_connected")
            async def _hello(transport, client_):
                context.add_message(
                    {
                        "role": "user",
                        "content": "(The call just connected. Greet the caller in one short sentence.)",
                    }
                )
                await worker.queue_frames([LLMRunFrame()])

        @transport.event_handler("on_client_disconnected")
        async def _gone(transport, client_):
            await runner.cancel()

        try:
            await runner.run()
        finally:
            report.t_close = time.monotonic()
            report.ended_by = report.ended_by or (
                "bot (#) -> hangup via API"
                if report.bot_hangup
                else "SBC closed the stream (caller hung up)"
            )
            say(
                f"<- stream closed after {report.t_close - report.t_open:.1f}s: {report.ended_by}"
            )
            report.done.set()

    return app


# ── tunnel + call ───────────────────────────────────────────────────────────


async def open_tunnel(port: int) -> tuple[asyncio.subprocess.Process, str]:
    if not shutil.which("cloudflared"):
        sys.exit("cloudflared not found (sudo apt install cloudflared, or use ngrok)")
    proc = await asyncio.create_subprocess_exec(
        "cloudflared",
        "tunnel",
        "--no-autoupdate",
        "--url",
        f"http://127.0.0.1:{port}",
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        line = await asyncio.wait_for(proc.stderr.readline(), 40)
        if m := re.search(rb"https://[a-z0-9-]+\.trycloudflare\.com", line):
            url = m.group(0).decode()
            # keep draining stderr so cloudflared never blocks on a full pipe
            asyncio.create_task(_drain(proc.stderr))
            return proc, url
    sys.exit("cloudflared gave no URL in 40s")


async def _drain(stream) -> None:
    while await stream.readline():
        pass


async def wait_public(url: str) -> None:
    say(f"waiting for {url} to resolve (DNS can take ~30s)...")
    async with httpx.AsyncClient(timeout=5) as http:
        for _ in range(45):
            with contextlib.suppress(Exception):
                if (await http.get(f"{url}/health")).status_code == 200:
                    return
            # Local/router DNS can lag on fresh quick-tunnel hosts while public
            # resolvers already answer; probe via 1.1.1.1 so the tunnel is still
            # verified (the SBC resolves it from its own network anyway).
            with contextlib.suppress(Exception):
                host = url.split("://", 1)[1]
                dig = await asyncio.create_subprocess_exec(
                    "dig",
                    "+short",
                    "@1.1.1.1",
                    host,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                out, _ = await dig.communicate()
                ip = out.decode().split()[0] if out.strip() else ""
                if ip:
                    curl = await asyncio.create_subprocess_exec(
                        "curl",
                        "-s",
                        "-m",
                        "5",
                        "-o",
                        "/dev/null",
                        "-w",
                        "%{http_code}",
                        "--resolve",
                        f"{host}:443:{ip}",
                        f"{url}/health",
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.DEVNULL,
                    )
                    code, _ = await curl.communicate()
                    if code.decode().strip() == "200":
                        say("   (local DNS not resolving yet; verified via 1.1.1.1)")
                        return
            await asyncio.sleep(2)
    sys.exit(f"{url} not reachable after 90s")


async def main(args) -> int:
    report = Report()
    secret = os.getenv("UNPOD_WEBHOOK_SECRET") or None
    if args.ai:
        missing = [
            k
            for k in ("DEEPGRAM_API_KEY", "ANTHROPIC_API_KEY", "CARTESIA_API_KEY")
            if not os.getenv(k)
        ]
        if missing:
            sys.exit(f"--ai needs {', '.join(missing)} in supervoice/.env")
    from_ = args.from_ or os.getenv("UNPOD_FROM_NUMBER") or DEFAULT_FROM
    print(
        f"QA: {os.getenv('UNPOD_BASE_URL')}  org: {os.getenv('UNPOD_ORG_HANDLE')}  "
        f"bot: {'AI' if args.ai else 'echo'}  from {from_} -> {args.to}"
    )

    async with AsyncClient() as client:
        app = build_app(report, client, ai=args.ai, secret=secret, strict=args.strict)
        server = uvicorn.Server(
            uvicorn.Config(app, host="127.0.0.1", port=args.port, log_level="warning")
        )
        server_task = asyncio.create_task(server.serve())
        tunnel, url = await open_tunnel(args.port)
        report.tunnel = url
        try:
            await wait_public(url)
            say(f"bot reachable at {url}")
            try:
                call = await client.telephony.calls.create(
                    from_, args.to, answer_url=f"{url}/answer"
                )
                report.call_id = call.call_id
                say(f"-> call {call.call_id} placed ({call.state}). ANSWER YOUR PHONE.")
                if args.ai:
                    say("   talk to the agent; press # to end")
                else:
                    say(
                        "   speak: you hear yourself. 5 = logged, * = barge-in, # = bot hangs up"
                    )
            except Exception as exc:
                report.call_error = f"{type(exc).__name__}: {exc}"
                say(f"!! calls.create failed: {report.call_error}")
                return 0 if report.print() else 1
            try:
                await asyncio.wait_for(report.done.wait(), args.timeout)
            except TimeoutError:
                report.ended_by = report.ended_by or f"timeout ({args.timeout}s)"
                say(f"!! no finished stream within {args.timeout}s")
            await asyncio.sleep(3)  # late status callbacks
        except KeyboardInterrupt:
            report.ended_by = report.ended_by or "stopped (Ctrl+C)"
        finally:
            if report.call_id and not report.t_close and not report.bot_hangup:
                with contextlib.suppress(Exception):
                    await client.telephony.calls.hangup(report.call_id)
            tunnel.terminate()
            server.should_exit = True
            with contextlib.suppress(Exception):
                await asyncio.wait_for(server_task, 5)
    return 0 if report.print() else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("to", help="your phone, E.164 (+91...)")
    ap.add_argument(
        "--ai", action="store_true", help="AI agent instead of the echo bot"
    )
    ap.add_argument(
        "--from",
        dest="from_",
        help=f"caller id (default UNPOD_FROM_NUMBER or {DEFAULT_FROM})",
    )
    ap.add_argument("--port", type=int, default=7861)
    ap.add_argument(
        "--timeout", type=int, default=300, help="seconds to wait for the call to end"
    )
    ap.add_argument(
        "--strict",
        action="store_true",
        help="refuse the stream when the signature is not valid (needs UNPOD_WEBHOOK_SECRET)",
    )
    a = ap.parse_args()
    if not re.fullmatch(r"\+\d{8,15}", a.to):
        ap.error("phone must be E.164, e.g. +91XXXXXXXXXX")
    with contextlib.suppress(KeyboardInterrupt):
        sys.exit(asyncio.run(main(a)))
