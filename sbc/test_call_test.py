"""Outbound call checks for call_test.py, against the in-memory FakeSBC (no real call)."""

from __future__ import annotations

import asyncio
import socket

import pytest

import call_test
from fake_sbc import FakeSBC

TO, CUSTOMER, THIRD, FROM = "+919000000001", "+919000000002", "+919000000003", "+918000000000"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def args_for(port: int, *extra: str):
    return call_test.build_parser().parse_args([
        "--to", TO, "--from", FROM, "--public-url", "https://hooks.example.com",
        "--port", str(port), "--rotate", "--max-seconds", "2", "--recording-wait", "0", *extra,
    ])


def run(sbc: FakeSBC, args) -> int:
    return asyncio.run(call_test.run(args, sbc))


def test_outbound_call_is_created_and_every_webhook_verifies(capsys) -> None:
    port = free_port()
    sbc = FakeSBC(port)
    assert run(sbc, args_for(port)) == 0

    assert ("create", FROM, TO, "agent") in sbc.requests
    assert sbc.requests[-1][0] == "end"
    out = capsys.readouterr().out
    assert "Answered" in out and "Hangup" in out
    assert "0 rejected" in out


def test_customer_is_added_once_leg_one_answers() -> None:
    port = free_port()
    sbc = FakeSBC(port)
    assert run(sbc, args_for(port, "--customer", CUSTOMER)) == 0

    adds = [r for r in sbc.requests if r[0] == "add_participant"]
    assert adds == [("add_participant", CUSTOMER, "customer", "none")]


def test_wait_tune_loops_on_leg_one_and_stops_only_when_customer_answers() -> None:
    port = free_port()
    sbc = FakeSBC(port, answer_after=0.3)
    url = "https://cdn.example.com/hold.mp3"
    assert run(sbc, args_for(port, "--customer", CUSTOMER, "--wait-tune", url)) == 0

    names = [r[0] for r in sbc.requests]
    play = next(r for r in sbc.requests if r[0] == "play_audio")
    assert play[1:] == ("api-1-leg1", url, True)                 # leg 1, looping
    stop = next(r for r in sbc.requests if r[0] == "stop_audio")
    assert stop[1] == "api-1-leg1"
    # stopped after the customer was dialled, and the customer's leg had answered by then
    assert names.index("add_participant") < names.index("stop_audio")
    assert any(w["Event"] == "Answered" and w["CallUUID"] == "api-1-leg2" for w in sbc.webhooks)


def test_third_participant_gets_participant_limit(capsys) -> None:
    port = free_port()
    sbc = FakeSBC(port)
    assert run(sbc, args_for(port, "--customer", CUSTOMER, "--customer_2", THIRD)) == 0

    assert "409 participant_limit" in capsys.readouterr().out


def test_from_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SBC_FROM_NUMBER", raising=False)
    monkeypatch.setattr("sys.argv", ["call_test.py", "--to", TO])
    with pytest.raises(SystemExit):
        call_test.main()
