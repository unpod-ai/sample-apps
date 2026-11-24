"""Human-to-agent call with handoff: an AI agent answers, then passes the caller to a person.

Docs: https://docs.unpod.ai/telephony/integrations/websockets
Run:  uv run uvicorn agent_handoff:app --port 8000    # served at https://$PUBLIC_HOST
      uv run agent_handoff.py <from> <to>             # dial <to>; the agent answers

The answer XML streams the call to the agent and puts a <Dial> after the stream.
When the caller asks for a person (or presses 0), the agent closes the stream and
Unpod dials HUMAN_NUMBER. The transfer API is not live yet; this is the documented
handoff. Env: UNPOD_WEBHOOK_SECRET, PUBLIC_HOST, HUMAN_NUMBER, DEEPGRAM_API_KEY,
ANTHROPIC_API_KEY, CARTESIA_API_KEY (plus the UNPOD_* API credentials for the CLI).
"""

import os
import sys

from fastapi import FastAPI, Request, Response, WebSocket
from pipecat.adapters.llm.schemas.function_schema import FunctionSchema
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import (
    EndWorkerFrame,
    Frame,
    FunctionCallResultProperties,
    InputDTMFFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineParams, PipelineWorker
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams,
)
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.services.anthropic.llm import AnthropicLLMService
from pipecat.services.cartesia.tts import CartesiaTTSService
from pipecat.services.deepgram.stt import DeepgramSTTService
from pipecat.services.llm_service import FunctionCallParams
from pipecat.transports.websocket.fastapi import (
    FastAPIWebsocketParams,
    FastAPIWebsocketTransport,
)
from pipecat.workers.runner import WorkerRunner
from unpod import Client
from unpod.pipecat import (
    UnpodFrameSerializer,
    parse_unpod_websocket,
    verify_stream_request,
)
from unpod.telephony import WebhookSignatureError, parse_call_event
from unpod.telephony.answer import Response as Answer
from unpod.telephony.stream import NonceCache

RATE = 8000
GREETING = "Hi, this is the Unpod assistant. Press 0 at any time to reach a person."
HANDOFF_LINE = "Connecting you to a person now."
PROMPT = (
    "You are a friendly phone assistant. The caller already heard a greeting. "
    "Answer in one or two short spoken sentences, with no lists or symbols. "
    "If the caller asks for a human, a person or an agent, or you cannot help, "
    "call transfer_to_human."
)
VOICE = "71a7ad14-091c-4e8e-a314-022ece01c121"
NONCES = NonceCache()  # ponytail: per process; share it if you run several workers

app = FastAPI()


def answer_xml() -> str:
    """Greet, stream to the agent, then dial the human once the agent lets go."""
    return (
        Answer()
        .speak(GREETING)
        .stream(f"wss://{os.environ['PUBLIC_HOST']}/ws")
        .speak(HANDOFF_LINE)
        .dial(os.environ["HUMAN_NUMBER"])
        .to_xml()
    )


@app.post("/answer")
async def handle_answer(request: Request) -> Response:
    """Signed StartApp gets the call plan; other events (Hangup) just get a 200."""
    secret = os.environ.get("UNPOD_WEBHOOK_SECRET")
    try:
        ev = parse_call_event(await request.body(), request.headers, secret)
    except WebhookSignatureError:
        return Response(status_code=401)
    if ev.event != "StartApp":
        return Response()
    return Response(answer_xml(), media_type="application/xml")


@app.websocket("/ws")
async def handle_stream(websocket: WebSocket) -> None:
    """Run the agent on the call audio. Closing the socket hands off to the human."""
    ok = verify_stream_request(
        websocket.headers,
        os.environ.get("UNPOD_WEBHOOK_SECRET", ""),
        url=str(websocket.url),
        public_base=f"wss://{os.environ['PUBLIC_HOST']}",
        nonce_cache=NONCES,
    )
    if not ok:
        await websocket.close(code=1008)
        return
    await websocket.accept()
    start = await parse_unpod_websocket(websocket)
    # No auto hang-up: the agent ending closes the stream but keeps the call,
    # so Unpod moves on to the <Dial> after <Stream>.
    serializer = UnpodFrameSerializer.from_start(
        start, params=UnpodFrameSerializer.InputParams(auto_hang_up=False)
    )
    transport = FastAPIWebsocketTransport(
        websocket=websocket,
        params=FastAPIWebsocketParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            add_wav_header=False,
            audio_out_end_silence_secs=0,  # no dead air before the handoff
            serializer=serializer,
        ),
    )
    worker = PipelineWorker(
        build_pipeline(transport),
        params=PipelineParams(audio_in_sample_rate=RATE, audio_out_sample_rate=RATE),
    )
    runner = WorkerRunner(handle_sigint=False)
    await runner.add_workers(worker)

    @transport.event_handler("on_client_disconnected")
    async def handle_caller_gone(*_: object) -> None:
        await runner.cancel()

    await runner.run()


def build_pipeline(transport: FastAPIWebsocketTransport) -> Pipeline:
    """Caller audio -> Deepgram -> Claude (with the handoff tool) -> Cartesia."""
    stt = DeepgramSTTService(
        api_key=os.environ["DEEPGRAM_API_KEY"],
        settings=DeepgramSTTService.Settings(language="en"),
    )
    llm = AnthropicLLMService(
        api_key=os.environ["ANTHROPIC_API_KEY"],
        settings=AnthropicLLMService.Settings(
            model="claude-haiku-5-5", system_instruction=PROMPT
        ),
    )
    tts = CartesiaTTSService(
        api_key=os.environ["CARTESIA_API_KEY"],
        settings=CartesiaTTSService.Settings(voice=VOICE, language="en"),
    )
    user, assistant = LLMContextAggregatorPair(
        LLMContext(tools=[HANDOFF_TOOL]),
        user_params=LLMUserAggregatorParams(vad_analyzer=SileroVADAnalyzer()),
    )
    return Pipeline(
        [
            transport.input(),
            HandoffOnZero(),
            stt,
            user,
            llm,
            tts,
            transport.output(),
            assistant,
        ]
    )


class HandoffOnZero(FrameProcessor):
    """Pressing 0 ends the agent, which hands the caller to the human."""

    async def process_frame(self, frame: Frame, direction: FrameDirection) -> None:
        await super().process_frame(frame, direction)
        if isinstance(frame, InputDTMFFrame) and frame.button.value == "0":
            await self.push_frame(EndWorkerFrame(reason="handoff"))
            return
        await self.push_frame(frame, direction)


async def transfer_to_human(params: FunctionCallParams) -> None:
    """LLM tool: end the agent so Unpod dials the caller through to a person."""
    await params.result_callback(
        {"status": "transferring"},
        properties=FunctionCallResultProperties(run_llm=False),
    )
    await params.llm.push_frame(EndWorkerFrame(reason="handoff"))


HANDOFF_TOOL = FunctionSchema(
    name="transfer_to_human",
    description="Hand the caller over to a human agent.",
    properties={},
    required=[],
    handler=transfer_to_human,
)


def place_call(from_: str, to: str) -> None:
    """Dial `to`; when they answer, Unpod fetches /answer and the agent picks up."""
    with Client() as client:
        tel = client.telephony
        tel.numbers.attach(from_, attach_type="human_call")
        call = tel.calls.create(
            from_,
            to,
            answer_url=f"https://{os.environ['PUBLIC_HOST']}/answer",
            answer_method="POST",
        )
        print(
            f"{call.call_id}: {call.state}. Answer, then ask for a person or press 0."
        )


if __name__ == "__main__":
    match sys.argv[1:]:
        case [from_, to]:
            place_call(from_, to)
        case _:
            sys.exit(__doc__)
