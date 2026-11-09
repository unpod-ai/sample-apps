"""Checks for answer_url.py: signature gate and XML reply."""

import hashlib
import hmac
import time

import pytest
from fastapi.testclient import TestClient

from answer_url import GREETING_XML, app

SECRET = "whsec_test"
FORM = {"Content-Type": "application/x-www-form-urlencoded"}
START = b"Event=StartApp&MPCUUID=call-1&CallUUID=leg-1&Direction=inbound"
HANGUP = b"Event=Hangup&MPCUUID=call-1&CallUUID=leg-1&call_status=completed"

client = TestClient(app)


def signed(body: bytes, secret: str = SECRET, ts: int | None = None) -> dict:
    """Headers carrying a SuperSBC-style `t=..,v1=..` signature for `body`."""
    t = int(time.time()) if ts is None else ts
    mac = hmac.new(secret.encode(), f"{t}.".encode() + body, hashlib.sha256)
    return {**FORM, "X-Webhook-Signature": f"t={t},v1={mac.hexdigest()}"}


@pytest.fixture(autouse=True)
def webhook_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UNPOD_WEBHOOK_SECRET", SECRET)


def test_signed_start_app_gets_xml() -> None:
    r = client.post("/answer", content=START, headers=signed(START))
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/xml")
    assert r.text == GREETING_XML


def test_signed_hangup_is_acknowledged_with_empty_200() -> None:
    r = client.post("/answer", content=HANGUP, headers=signed(HANGUP))
    assert (r.status_code, r.text) == (200, "")


@pytest.mark.parametrize(
    "headers",
    [
        FORM,  # no signature header
        signed(START, secret="wrong"),
        signed(START, ts=int(time.time()) - 3600),  # replayed, outside tolerance
    ],
    ids=["missing", "wrong-secret", "stale"],
)
def test_bad_signature_is_rejected(headers: dict) -> None:
    assert client.post("/answer", content=START, headers=headers).status_code == 401


def test_inbound_rejected_when_secret_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("UNPOD_WEBHOOK_SECRET")
    r = client.post("/answer", content=START, headers=signed(START))
    assert r.status_code == 401


def test_outbound_unsigned_start_app_gets_xml() -> None:
    r = client.post("/outbound", content=START, headers=FORM)
    assert (r.status_code, r.text) == (200, GREETING_XML)
