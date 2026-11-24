"""Checks for agent_handoff.py: answer XML, signature gates, and the handoff."""

import hashlib
import hmac
import json
import secrets
import time
from types import SimpleNamespace
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient
from pipecat.frames.frames import EndWorkerFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.services.llm_service import FunctionCallParams
from starlette.websockets import WebSocketDisconnect
from unpod.telephony.stream import SIGNATURE_HEADER, stream_signature

import agent_handoff
from agent_handoff import HandoffOnZero, app, transfer_to_human

SECRET = "whsec_test"
HOST = "bot.example.com"
HUMAN = "+919000000003"
FORM = {"Content-Type": "application/x-www-form-urlencoded"}
START_APP = b"Event=StartApp&MPCUUID=call-1&CallUUID=leg-1&Direction=inbound"
HANGUP = b"Event=Hangup&MPCUUID=call-1&CallUUID=leg-1&CallStatus=completed"
STREAM_START = {
    "event": "start",
    "sequenceNumber": 1,
    "start": {
        "callId": "call-1",
        "streamId": "stream-1",
        "accountId": "org-1",
        "tracks": ["inbound"],
        "mediaFormat": {"encoding": "audio/x-mulaw", "sampleRate": 8000},
    },
}

client = TestClient(app)


@pytest.fixture(autouse=True)
def env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UNPOD_WEBHOOK_SECRET", SECRET)
    monkeypatch.setenv("PUBLIC_HOST", HOST)
    monkeypatch.setenv("HUMAN_NUMBER", HUMAN)
    # No AI services offline: the agent is just the handoff key.
    monkeypatch.setattr(
        agent_handoff,
        "build_pipeline",
        lambda t: Pipeline([t.input(), HandoffOnZero(), t.output()]),
    )


def signed_post(body: bytes, secret: str = SECRET, ts: int | None = None) -> dict:
    """Headers with an Unpod `t=..,v1=..` webhook signature for `body`."""
    t = int(time.time()) if ts is None else ts
    mac = hmac.new(secret.encode(), f"{t}.".encode() + body, hashlib.sha256)
    return {**FORM, "X-Webhook-Signature": f"t={t},v1={mac.hexdigest()}"}


def signed_ws(secret: str = SECRET) -> dict:
    """The X-Unpod-Signature Unpod puts on the stream upgrade to wss://HOST/ws."""
    ts, nonce = int(time.time()), secrets.token_hex(8)
    sig = stream_signature(secret, f"wss://{HOST}/ws", ts, nonce)
    return {SIGNATURE_HEADER: f"t={ts},n={nonce},v1={sig}"}


def hand_off(headers: dict) -> int:
    """Open a stream, press 0, and return the close code the bot sends."""
    with client.websocket_connect("/ws", headers=headers) as ws:
        ws.send_text(json.dumps(STREAM_START))
        dtmf = {"event": "dtmf", "streamId": "stream-1", "dtmf": {"digit": "0"}}
        ws.send_text(json.dumps(dtmf))
        for _ in range(50):  # skip any audio still in flight
            msg = ws.receive()
            if msg["type"] == "websocket.close":
                return msg["code"]
    raise AssertionError("bot did not close the stream")


def test_start_app_streams_to_agent_then_dials_human() -> None:
    r = client.post("/answer", content=START_APP, headers=signed_post(START_APP))
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/xml")
    xml = r.text
    stream = 'keepCallAlive="true" audioTrack="inbound"'
    assert stream in xml and f">wss://{HOST}/ws</Stream>" in xml
    assert f"<Number>{HUMAN}</Number>" in xml
    order = [
        xml.index(tag) for tag in ("<Speak>", "<Stream", "</Stream><Speak>", "<Dial")
    ]
    assert order == sorted(order)


def test_hangup_event_gets_empty_200() -> None:
    r = client.post("/answer", content=HANGUP, headers=signed_post(HANGUP))
    assert (r.status_code, r.text) == (200, "")


@pytest.mark.parametrize(
    "headers",
    [
        FORM,
        signed_post(START_APP, secret="wrong"),
        signed_post(START_APP, ts=int(time.time()) - 3600),
    ],
    ids=["missing", "wrong-secret", "stale"],
)
def test_badly_signed_answer_is_rejected(headers: dict) -> None:
    assert client.post("/answer", content=START_APP, headers=headers).status_code == 401


@pytest.mark.parametrize(
    "headers", [{}, signed_ws("wrong")], ids=["unsigned", "wrong-secret"]
)
def test_unverified_stream_is_refused(headers: dict) -> None:
    with (
        pytest.raises(WebSocketDisconnect) as refused,
        client.websocket_connect("/ws", headers=headers),
    ):
        pass
    assert refused.value.code == 1008


def test_pressing_zero_closes_stream_so_unpod_dials_human() -> None:
    assert hand_off(signed_ws()) == 1000


def test_replayed_stream_signature_is_refused() -> None:
    headers = signed_ws()
    hand_off(headers)
    with (
        pytest.raises(WebSocketDisconnect) as refused,
        client.websocket_connect("/ws", headers=headers),
    ):
        pass
    assert refused.value.code == 1008


@pytest.mark.anyio
async def test_transfer_tool_ends_agent_without_another_llm_turn() -> None:
    results: list[tuple[Any, Any]] = []
    pushed: list[Any] = []

    async def result_callback(result: Any, *, properties: Any = None) -> None:
        results.append((result, properties))

    async def push_frame(frame: Any) -> None:
        pushed.append(frame)

    params = SimpleNamespace(
        result_callback=result_callback, llm=SimpleNamespace(push_frame=push_frame)
    )
    await transfer_to_human(cast(FunctionCallParams, params))
    [(result, props)] = results
    assert result == {"status": "transferring"} and props.run_llm is False
    [frame] = pushed
    assert isinstance(frame, EndWorkerFrame)
