"""In-memory stand-in for ``client.telephony`` and the SBC behind it, for the offline tests.

It answers the SDK calls ``call_test.py`` and ``inbound_test.py`` make, and it sends
signed webhooks to the scripts' local receiver the way SuperSBC does: form-encoded
Plivo fields, ``X-Webhook-Signature: t=<ts>,v1=<hex HMAC-SHA256(secret, "<ts>.<body>")>``.

It follows the behaviour seen on the real SBC:
  * every leg answers (no ``Ringing`` event, like mobile legs on the real network);
  * at most two participants per call, the third gets 409 ``participant_limit``;
  * ``end`` on an ended call is a 404;
  * inbound: ``StartApp`` goes to the DID's ``answer_url``, then ``Hangup``.

Nothing here touches the network beyond 127.0.0.1.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import itertools
import time
import urllib.request
from dataclasses import dataclass, field
from types import SimpleNamespace
from urllib.parse import urlencode, urlsplit

from unpod.telephony import InboundConfig, TelephonyAPIError


def sign(secret: str, body: bytes, ts: int | None = None) -> str:
    t = int(time.time()) if ts is None else ts
    mac = hmac.new(secret.encode(), f"{t}.".encode() + body, hashlib.sha256)
    return f"t={t},v1={mac.hexdigest()}"


def post_signed(port: int, url: str, fields: dict, secret: str) -> tuple[int, str]:
    """POST ``fields`` form-encoded to the local receiver at ``url``'s path and query."""
    parts = urlsplit(url)
    target = f"http://127.0.0.1:{port}{parts.path}" + (f"?{parts.query}" if parts.query else "")
    body = urlencode(fields).encode()
    req = urllib.request.Request(target, data=body, method="POST", headers={
        "Content-Type": "application/x-www-form-urlencoded",
        "X-Webhook-Signature": sign(secret, body),
    })
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()


@dataclass
class FakeSBC:
    port: int                                   # where the script's receiver listens
    secret: str = "whsec_fake"
    answer_after: float = 0.2                   # seconds before a leg answers
    requests: list[tuple] = field(default_factory=list)       # every SDK call, in order
    webhooks: list[dict] = field(default_factory=list)        # every webhook sent
    answers: list[tuple[int, str]] = field(default_factory=list)  # answer-URL replies
    dids: dict[str, dict] = field(default_factory=dict)
    _calls: dict[str, dict] = field(default_factory=dict)
    _ids: itertools.count = field(default_factory=lambda: itertools.count(1))
    _seq: itertools.count = field(default_factory=lambda: itertools.count(1))
    _tasks: list[asyncio.Task] = field(default_factory=list)

    # ── client.telephony shape ────────────────────────────────────────────
    @property
    def telephony(self):
        return SimpleNamespace(
            overview=self.overview,
            numbers=SimpleNamespace(attach=self.attach),
            webhook_secret=SimpleNamespace(rotate=self.rotate),
            calls=SimpleNamespace(
                create=self.create, add_participant=self.add_participant,
                play_audio=self.play_audio, stop_audio=self.stop_audio,
                get=self.get, end=self.end, get_recording=self.get_recording),
            inbound=SimpleNamespace(get_did=self.get_did, set_did=self.set_did),
        )

    async def overview(self):
        return []

    async def attach(self, number, **kw):
        self.requests.append(("attach", number, kw))
        return SimpleNamespace(numbers=[SimpleNamespace(number=number, ok=True)])

    async def rotate(self):
        self.requests.append(("rotate",))
        return SimpleNamespace(secret=self.secret, version=1, pushed_to_sbc=True)

    # ── calls ─────────────────────────────────────────────────────────────
    async def create(self, from_, to, *, role=None, status_callback_url=None, **kw):
        self.requests.append(("create", from_, to, role))
        cid = f"api-{next(self._ids)}"
        self._calls[cid] = {"state": "initiated", "legs": []}
        leg = self._new_leg(cid, to, role, status_callback_url)
        return SimpleNamespace(call_id=cid, state="initiated", legs=[SimpleNamespace(leg_id=leg)])

    async def add_participant(self, cid, from_, to, *, role=None, status_callback_url=None, **kw):
        self.requests.append(("add_participant", to, role, kw.get("dial_music")))
        if len(self._calls[cid]["legs"]) >= 2:
            raise TelephonyAPIError(409, "participant_limit", "call already has two participants")
        leg = self._new_leg(cid, to, role, status_callback_url)
        return SimpleNamespace(leg_id=leg, state="initiated")

    async def play_audio(self, cid, *, url, leg_id=None, loop=False):
        play_id = f"play-{next(self._ids)}"
        self.requests.append(("play_audio", leg_id, url, loop))
        for lg in self._target_legs(cid, leg_id):
            self._event(cid, lg, "PlayStarted", PlayId=play_id)
        return SimpleNamespace(play_id=play_id)

    async def stop_audio(self, cid, *, leg_id=None, play_id=None):
        self.requests.append(("stop_audio", leg_id, play_id))
        for lg in self._target_legs(cid, leg_id):
            self._event(cid, lg, "PlayStopped", PlayId=play_id or "")

    async def get(self, cid):
        call = self._calls.get(cid)
        if call is None:
            raise TelephonyAPIError(404, "not_found", f"call {cid} not found")
        legs = [SimpleNamespace(leg_id=lg["id"], state=lg["state"], role=lg["role"]) for lg in call["legs"]]
        return SimpleNamespace(state=call["state"], legs=legs)

    async def end(self, cid):
        self.requests.append(("end", cid))
        call = self._calls[cid]
        if call["state"] == "ended":
            raise TelephonyAPIError(404, "not_found", "call already ended")
        for t in self._tasks:
            t.cancel()
        for lg in call["legs"]:
            lg["state"] = "hungup"
            self._event(cid, lg, "Hangup", CallStatus="completed", HangupCause="Normal Hangup")
        call["state"] = "ended"

    async def get_recording(self, cid):
        raise TelephonyAPIError(404, "not_found", "no recording")

    # ── inbound ───────────────────────────────────────────────────────────
    async def get_did(self, did):
        return InboundConfig(**self.dids.get(did, {}))

    async def set_did(self, did, cfg: InboundConfig):
        self.requests.append(("set_did", did, cfg.model_dump(exclude_none=True)))
        self.dids[did] = cfg.model_dump(exclude_none=True)   # PUT replaces, like the SBC
        return InboundConfig(**self.dids[did])

    async def ring_did(self, did: str, caller: str) -> None:
        """A caller dials ``did``: StartApp to its answer_url, then Hangup to its status callback."""
        cfg = self.dids.get(did, {})
        answer_url = cfg.get("answer_url")
        assert answer_url, f"{did} has no answer_url"
        base = {"MPCUUID": "inbound-1", "CallUUID": "inbound-1-leg", "From": caller, "To": did,
                "Direction": "inbound"}
        self.answers.append(await asyncio.to_thread(
            post_signed, self.port, answer_url, {**base, "Event": "StartApp", "CallStatus": "ringing"}, self.secret))
        await asyncio.sleep(0.1)
        await asyncio.to_thread(post_signed, self.port, cfg.get("status_callback_url") or answer_url,
                                {**base, "Event": "Hangup", "CallStatus": "completed", "Duration": "8"},
                                self.secret)

    # ── internals ─────────────────────────────────────────────────────────
    def _new_leg(self, cid, to, role, callback) -> str:
        call = self._calls[cid]
        leg = {"id": f"{cid}-leg{len(call['legs']) + 1}", "to": to, "role": role,
               "state": "ringing", "callback": callback}
        call["legs"].append(leg)
        self._event(cid, leg, "Initiated")
        self._tasks.append(asyncio.create_task(self._answer(cid, leg)))
        return leg["id"]

    async def _answer(self, cid, leg) -> None:
        await asyncio.sleep(self.answer_after)
        leg["state"] = "answered"
        self._calls[cid]["state"] = "talking"
        self._event(cid, leg, "Answered", CallStatus="in-progress")

    def _target_legs(self, cid, leg_id):
        return [lg for lg in self._calls[cid]["legs"] if leg_id is None or lg["id"] == leg_id]

    def _event(self, cid, leg, name, **extra) -> None:
        fields = {"Event": name, "MPCUUID": cid, "CallUUID": leg["id"], "Role": leg["role"] or "",
                  "Sequence": str(next(self._seq)), "EventId": f"ev-{next(self._ids)}", **extra}
        self.webhooks.append(fields)
        if leg["callback"]:
            post_signed(self.port, leg["callback"], fields, self.secret)
