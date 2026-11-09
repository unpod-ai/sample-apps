"""Checks for step7_app.py (the docs' answer server): signature gate and XML reply."""

import hashlib
import hmac
import time

import pytest
from fastapi.testclient import TestClient

SECRET = "whsec_test"
FORM = {"Content-Type": "application/x-www-form-urlencoded"}
START = b"Event=StartApp&MPCUUID=call-1&CallUUID=leg-1&From=%2B919000000001&CallStatus=ringing"
HANGUP = b"Event=Hangup&MPCUUID=call-1&CallUUID=leg-1&CallStatus=completed"


def signed(body: bytes, secret: str = SECRET) -> dict:
    t = int(time.time())
    mac = hmac.new(secret.encode(), f"{t}.".encode() + body, hashlib.sha256)
    return {**FORM, "X-Webhook-Signature": f"t={t},v1={mac.hexdigest()}"}


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    import step7_app

    monkeypatch.setattr(step7_app, "SECRET", SECRET)
    return TestClient(step7_app.app)


def test_signed_start_app_gets_speak_and_hangup(client: TestClient) -> None:
    r = client.post("/answer", content=START, headers=signed(START))
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/xml")
    assert "<Speak>Hello! Thanks for calling.</Speak><Hangup/>" in r.text


def test_signed_hangup_gets_empty_200(client: TestClient) -> None:
    r = client.post("/answer", content=HANGUP, headers=signed(HANGUP))
    assert r.status_code == 200
    assert r.text == ""


def test_unsigned_request_is_401(client: TestClient) -> None:
    assert client.post("/answer", content=START, headers=FORM).status_code == 401


def test_wrong_secret_is_401(client: TestClient) -> None:
    assert client.post("/answer", content=START, headers=signed(START, "other")).status_code == 401
