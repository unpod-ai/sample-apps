"""Inbound call checks for inbound_test.py, against the in-memory FakeSBC (no real call)."""

from __future__ import annotations

import asyncio
import json
import socket
from pathlib import Path

import pytest

import inbound_test
from fake_sbc import FakeSBC

DID, CALLER, AGENT = "+918000000000", "+919000000001", "+919000000002"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(autouse=True)
def backups_in_tmp(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(inbound_test, "HERE", tmp_path)


def inbound_call(mode: str, *extra: str) -> tuple[FakeSBC, int]:
    port = free_port()
    sbc = FakeSBC(port)
    args = inbound_test.build_parser().parse_args([
        "--did", DID, "--public-url", "https://hooks.example.com", "--port", str(port),
        "--rotate", "--mode", mode, "--max-seconds", "5", *extra,
    ])

    async def go() -> int:
        script = asyncio.create_task(inbound_test.run(args, sbc))
        while ("set_did", DID) not in [r[:2] for r in sbc.requests]:
            await asyncio.sleep(0.05)
        await sbc.ring_did(DID, CALLER)
        return await script

    return sbc, asyncio.run(go())


def test_speak_mode_answers_start_app_with_speak_and_hangup() -> None:
    sbc, rc = inbound_call("speak", "--text", "Hello from the test")
    assert rc == 0
    status, xml = sbc.answers[0]
    assert status == 200
    assert "<Speak>Hello from the test</Speak><Hangup/>" in xml


def test_dial_mode_records_and_dials_the_agent_with_the_did_as_caller_id() -> None:
    sbc, rc = inbound_call("dial", "--agent", AGENT)
    assert rc == 0
    _, xml = sbc.answers[0]
    assert f'<Record/><Dial callerId="{DID}"><Number>{AGENT}</Number></Dial>' in xml
    set_did = next(r for r in sbc.requests if r[0] == "set_did")
    assert set_did[2]["record"] is True


def test_empty_mode_hands_the_call_back_to_static_routing() -> None:
    sbc, _ = inbound_call("empty")
    assert sbc.answers[0] == (200, "")


def test_answer_url_is_set_then_the_saved_config_is_restored(tmp_path: Path) -> None:
    sbc, _ = inbound_call("speak")
    sets = [r[2] for r in sbc.requests if r[0] == "set_did"]
    assert sets[0]["answer_url"] == "https://hooks.example.com/incoming"
    assert sets[-1] == {}                                       # restored to "no override"
    assert json.loads((tmp_path / f"did-backup-{DID.lstrip('+')}.json").read_text()) == {}


def test_unsigned_request_is_rejected(tmp_path: Path) -> None:
    import urllib.error
    import urllib.request

    from webhook_receiver import WebhookReceiver

    receiver = WebhookReceiver("whsec_fake", answer=inbound_test.make_answer(
        inbound_test.build_parser().parse_args(["--did", DID, "--public-url", "https://x"])))
    url = receiver.start(port=0)
    try:
        req = urllib.request.Request(f"{url}/incoming", data=b"Event=StartApp", method="POST")
        with pytest.raises(urllib.error.HTTPError) as err:
            urllib.request.urlopen(req, timeout=5)
        assert err.value.code == 401
    finally:
        receiver.stop()
