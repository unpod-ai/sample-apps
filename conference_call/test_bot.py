"""Checks for bot.py, the streaming e2e harness: handoff XML, echo, signature gate."""

import base64
import json
import secrets
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect
from unpod.pipecat.g711 import pcm_to_ulaw
from unpod.telephony.stream import SIGNATURE_HEADER, stream_signature

import bot

SECRET = "whsec_test"
BASE = "wss://bot.example.com"
START = {
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

client = TestClient(bot.app)


@pytest.fixture(autouse=True)
def harness(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(bot, "SECRET", SECRET)
    monkeypatch.setattr(bot, "PUBLIC_BASE", BASE)
    monkeypatch.setattr(bot, "EVENTS", str(tmp_path / "events.jsonl"))


def signed_ws(path: str, secret: str = SECRET) -> dict:
    ts, nonce = int(time.time()), secrets.token_hex(8)
    sig = stream_signature(secret, f"{BASE}{path}", ts, nonce)
    return {SIGNATURE_HEADER: f"t={ts},n={nonce},v1={sig}"}


def test_dial_option_puts_handoff_after_the_stream() -> None:
    q = {"dial": "+919000000003", "mode": "close", "close_after": "15"}
    xml = client.get("/answer", params=q).text
    assert "mode=close&amp;close_after=15</Stream>" in xml
    assert xml.index("</Stream>") < xml.index("<Dial><Number>+919000000003</Number>")


def test_echo_mode_plays_caller_audio_back() -> None:
    audio = pcm_to_ulaw(b"\x00\x10" * 160)  # one 20 ms mu-law frame
    payload = base64.b64encode(audio).decode()
    media = {"track": "inbound", "chunk": 1, "timestamp": "0", "payload": payload}
    with client.websocket_connect(
        "/ws?mode=echo", headers=signed_ws("/ws?mode=echo")
    ) as ws:
        ws.send_text(json.dumps(START))
        ws.send_text(
            json.dumps({"event": "media", "streamId": "stream-1", "media": media})
        )
        played = json.loads(ws.receive_text())
    assert played["event"] == "playAudio" and played["streamId"] == "stream-1"
    assert played["media"]["payload"] == payload


def test_wrongly_signed_stream_is_refused() -> None:
    headers = signed_ws("/ws", secret="wrong")
    with (
        pytest.raises(WebSocketDisconnect) as refused,
        client.websocket_connect("/ws", headers=headers),
    ):
        pass
    assert refused.value.code == 1008
