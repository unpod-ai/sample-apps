"""Local webhook receiver: verifies SBC call / recording events with the unpod SDK.

What your app's ``status_callback_url`` / ``recording_callback_url`` routes do,
in ~30 lines: read the raw body, call ``parse_call_event`` with the org's
webhook secret, answer 200, or 401 on a bad signature.

    python webhook_receiver.py --port 8098 --secret <secret from webhook_secret.rotate()>

Pair it with ``mock_sbc.py --deliver-to http://127.0.0.1:8098``. Import
``WebhookReceiver`` to use it from a script (``live_flow.py``, ``sdk_driver.py``).

``IncomingCallHandler`` is a reference Incoming Call (answer URL) handler: it
answers ``StartApp`` + ringing with Record + Dial XML for a free agent, or
Speak + Hangup when none is free; on ``Hangup`` it stores the call details and on
``RecordStop`` it downloads ``RecordUrl``. Mount it with
``WebhookReceiver(secret, answer=IncomingCallHandler([...]), answer_path="/incoming")``.
"""

from __future__ import annotations

import argparse
import io
import math
import struct
import threading
import time
import urllib.request
import wave
from collections.abc import Callable
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit
from xml.sax.saxutils import escape, quoteattr

from unpod.telephony import CallEvent, WebhookSignatureError, parse_call_event


@dataclass
class Received:
    path: str
    query: dict[str, str]
    verified: bool
    event: CallEvent | None = None
    error: str | None = None
    raw_body: bytes = b""                               # as received (debugging / --dump)
    headers: dict[str, str] = field(default_factory=dict)
    at: float = field(default_factory=time.time)        # when it arrived

    def summary(self) -> str:
        if not self.verified:
            return f"REJECTED {self.path}: {self.error}"
        e = self.event
        bits = [e.event, f"call={e.call_id}"]
        if e.leg_id:
            bits.append(f"leg={e.leg_id}")
        if e.role:
            bits.append(f"role={e.role}")
        if e.hangup_cause:
            bits.append(f"cause={e.hangup_cause!r} sip={e.sip_code}")
        if e.call_status:
            bits.append(f"status={e.call_status}")
        if e.play_id:
            bits.append(f"play_id={e.play_id}")
        if e.recording_url:
            bits.append(f"recording={e.recording_url}")
        if self.query:
            bits.append(f"query={self.query}")
        return f"{self.path} " + " ".join(bits)


def tone_wav(seconds: float = 6.0, freq: float = 440.0, rate: int = 8000) -> bytes:
    """A telephony-friendly test sound: 0.4 s beeps of ``freq`` Hz with 0.4 s gaps, 8 kHz mono PCM16."""
    frames = bytearray()
    for i in range(int(seconds * rate)):
        t = i / rate
        on = (t % 0.8) < 0.4
        frames += struct.pack("<h", int(12000 * math.sin(2 * math.pi * freq * t)) if on else 0)
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(rate)
        w.writeframes(bytes(frames))
    return buf.getvalue()


@dataclass
class AudioFetch:
    path: str
    at: float
    user_agent: str
    bytes_sent: int


@dataclass
class IncomingCallHandler:
    """Answer-URL handler: route the caller to a free agent, record, store the recording."""

    agents: list[str]
    action_url: str = "https://93.184.216.34/incoming"
    busy_message: str = "All our agents are busy. Please call back later."
    calls: dict[str, dict[str, Any]] = field(default_factory=dict)       # CallUUID -> details
    recordings: dict[str, bytes] = field(default_factory=dict)          # CallUUID -> WAV bytes

    def __call__(self, ev: CallEvent) -> str:
        if ev.event == "StartApp" and ev.call_status == "ringing":
            agent = self.agents.pop(0) if self.agents else None
            self.calls[ev.leg_id] = {"from": ev.from_, "to": ev.to, "agent": agent,
                                     "session_start": ev.raw.get("SessionStart")}
            if agent is None:
                return f"<Response><Speak>{escape(self.busy_message)}</Speak><Hangup/></Response>"
            return (f"<Response><Record action={quoteattr(self.action_url)} recordSession=\"true\" "
                    f"redirect=\"false\"/><Dial><Number>{escape(agent)}</Number></Dial></Response>")
        details = self.calls.setdefault(ev.leg_id or "", {})
        if ev.event == "Hangup":
            details.update(status=ev.call_status, duration=ev.duration,
                           start_time=ev.raw.get("StartTime"), end_time=ev.raw.get("EndTime"))
        elif ev.event == "RecordStop" and ev.recording_url:
            with urllib.request.urlopen(ev.recording_url, timeout=10) as resp:
                self.recordings[ev.leg_id] = resp.read()
            details.update(recording_url=ev.recording_url, recording_duration=ev.recording_duration)
        return "<Response/>"


@dataclass
class WebhookReceiver:
    secret: str
    verbose: bool = False
    answer: Callable[[CallEvent], str] | None = None      # Incoming Call handler (returns XML)
    answer_path: str = "/incoming"
    received: list[Received] = field(default_factory=list)
    audio_fetches: list[AudioFetch] = field(default_factory=list)   # GET /audio/tone.wav hits (the SBC)
    _server: ThreadingHTTPServer | None = None
    _tone: bytes = field(default_factory=tone_wav)

    def start(self, host: str = "127.0.0.1", port: int = 0) -> str:
        receiver = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args: Any) -> None:
                pass

            def do_GET(self) -> None:
                """Serve the test sound the SBC plays (``<public url>/audio/tone.wav``)."""
                path = urlsplit(self.path).path
                if path != "/audio/tone.wav":
                    self.send_response(404)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                data = receiver._tone
                receiver.audio_fetches.append(
                    AudioFetch(path, time.time(), self.headers.get("User-Agent", ""), len(data)))
                if receiver.verbose:
                    print(f"[GET] {path} fetched ({len(data)} bytes, UA={self.headers.get('User-Agent', '')!r})")
                self.send_response(200)
                self.send_header("Content-Type", "audio/wav")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_POST(self) -> None:
                parts = urlsplit(self.path)
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                query = {k: v[-1] for k, v in parse_qs(parts.query).items()}
                try:
                    event = parse_call_event(body, dict(self.headers), receiver.secret)
                    item, status = Received(parts.path, query, True, event,
                                            raw_body=body, headers=dict(self.headers)), 200
                except WebhookSignatureError as exc:
                    item, status = Received(parts.path, query, False, error=str(exc),
                                            raw_body=body, headers=dict(self.headers)), 401
                reply = b""
                if status == 200 and receiver.answer and parts.path == receiver.answer_path:
                    reply = receiver.answer(item.event).encode()
                receiver.received.append(item)
                if receiver.verbose:
                    print(f"[{status}] {item.summary()}" + (f"\n      -> {reply.decode()}" if reply else ""))
                self.send_response(status)
                if reply:
                    self.send_header("Content-Type", "application/xml")
                self.send_header("Content-Length", str(len(reply)))
                self.end_headers()
                self.wfile.write(reply)

        self._server = ThreadingHTTPServer((host, port), Handler)
        threading.Thread(target=self._server.serve_forever, daemon=True).start()
        return f"http://{host}:{self._server.server_address[1]}"

    def stop(self) -> None:
        if self._server:
            self._server.shutdown()
            self._server.server_close()

    def events(self) -> list[CallEvent]:
        return [r.event for r in self.received if r.verified]

    def wait_for(self, predicate, timeout: float = 10.0) -> bool:
        """Wait until some verified event matches ``predicate``."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if any(predicate(e) for e in self.events()):
                return True
            time.sleep(0.05)
        return False


def main() -> None:
    ap = argparse.ArgumentParser(description="Verify SBC webhooks with unpod.telephony.parse_call_event")
    ap.add_argument("--port", type=int, default=8098)
    ap.add_argument("--secret", required=True, help="org webhook secret (webhook_secret.rotate())")
    args = ap.parse_args()
    url = WebhookReceiver(args.secret, verbose=True).start(port=args.port)
    print(f"webhook receiver on {url}  Ctrl-C to stop")
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
