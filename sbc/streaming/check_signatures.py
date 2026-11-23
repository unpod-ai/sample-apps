import glob
import hashlib
import hmac
import json
import os
import time
from pathlib import Path
from urllib.parse import urlencode

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")
from unpod.telephony import stream as S
from unpod.telephony.events import STREAM_EVENTS, parse_call_event

SECRET = (
    os.environ.get("UNPOD_WEBHOOK_SECRET") or "test-secret-not-real"
)  # offline: any secret works; never printed
URL = "wss://bot.example.com/ws?token=abc&x=1"
now = int(time.time())


def hdr(t, n, *sigs):
    return {"X-Unpod-Signature": f"t={t},n={n}," + ",".join(f"v1={s}" for s in sigs)}


def manual(url, t, n, key=SECRET):
    return hmac.new(
        key.encode(), f"ws.{t}.{n}.{url}".encode(), hashlib.sha256
    ).hexdigest()


R = []


def chk(name, ok, note=""):
    R.append((name, bool(ok), note))
    print(("PASS" if ok else "FAIL"), name, note, flush=True)


n = "0123456789abcdef"
# 4.2 / 4.3
good = hdr(now, n, S.stream_signature(SECRET, URL, now, n))
chk("4.2 SDK signs and verifies", S.verify_stream_request(good, SECRET, url=URL))
chk(
    "4.3 manual HMAC equals SDK signature",
    manual(URL, now, n) == S.stream_signature(SECRET, URL, now, n),
)
chk(
    "4.3 header built only from manual HMAC verifies",
    S.verify_stream_request(hdr(now, n, manual(URL, now, n)), SECRET, url=URL),
)
# 4.4
chk(
    "4.4 one query char changed fails",
    not S.verify_stream_request(good, SECRET, url=URL.replace("abc", "abd")),
)
# 4.5
old = now - 301
chk(
    "4.5 timestamp 301 s old rejected",
    not S.verify_stream_request(hdr(old, n, manual(URL, old, n)), SECRET, url=URL),
)
ok299 = now - 299
chk(
    "4.5 timestamp 299 s old accepted",
    S.verify_stream_request(hdr(ok299, n, manual(URL, ok299, n)), SECRET, url=URL),
)
# 4.6
nc = S.NonceCache()
a = S.verify_stream_request(good, SECRET, url=URL, nonce_cache=nc)
b = S.verify_stream_request(good, SECRET, url=URL, nonce_cache=nc)
chk("4.6 replayed nonce rejected the second time", a and not b, f"first={a} second={b}")
# 4.7
spoof = dict(good)
spoof["Host"] = "evil.example"
spoof["X-Forwarded-Host"] = "evil.example"
chk(
    "4.7 spoofed Host/X-Forwarded-Host headers ignored, URL param decides",
    S.verify_stream_request(spoof, SECRET, url=URL),
)
chk(
    "4.7 URL built from a spoofed host fails",
    not S.verify_stream_request(
        good, SECRET, url="wss://evil.example/ws?token=abc&x=1"
    ),
)
chk(
    "4.7 public_base maps a tunnel URL back",
    S.verify_stream_request(
        hdr(now, n, manual(URL, now, n)),
        SECRET,
        url="ws://127.0.0.1:9/ws?token=abc&x=1",
        public_base="wss://bot.example.com",
    ),
)
# 4.8
body = b'{"x":1}'
webhook_v1 = hmac.new(
    SECRET.encode(), f"{now}.".encode() + body, hashlib.sha256
).hexdigest()
chk(
    "4.8 X-Webhook-Signature value used as stream signature fails",
    not S.verify_stream_request(
        {"X-Unpod-Signature": f"t={now},v1={webhook_v1}"}, SECRET, url=URL
    ),
)
chk(
    "4.8 webhook-style digest with a nonce fails (ws. prefix)",
    not S.verify_stream_request(hdr(now, n, webhook_v1), SECRET, url=URL),
)
# 4.9 (header semantics only, not a live rotation)
newkey = SECRET + "-rotated"
two = hdr(now, n, manual(URL, now, n, newkey), manual(URL, now, n, SECRET))
chk(
    "4.9 header with two v1 values: old secret verifies",
    S.verify_stream_request(two, SECRET, url=URL),
)
chk(
    "4.9 header with two v1 values: new secret verifies",
    S.verify_stream_request(two, newkey, url=URL),
)
chk(
    "4.9 header with only the new secret's v1 fails the old secret",
    not S.verify_stream_request(
        hdr(now, n, manual(URL, now, n, newkey)), SECRET, url=URL
    ),
)
# 8.4 parse stream callbacks: captured ones from run_case.py results when present, else built-in samples
SAMPLES = [
    "CallUUID=c-callee&StreamID=s1&Event=started&Timestamp=2026-11-22T05%3A18%3A35Z&From=%2B910000000000&To=%2B910000000001&Direction=outbound&ServiceURL=wss%3A%2F%2Fbot.example.com%2Fws&ExtraHeaders=a%3D1",
    "CallUUID=c-callee&StreamID=s1&Event=stopped&Timestamp=2026-11-22T05%3A18%3A41Z&ServiceURL=wss%3A%2F%2Fbot.example.com%2Fws&StatusReason=completed&Duration=6",
    "CallUUID=c-callee&StreamID=s2&Event=failed&Timestamp=2026-11-22T05%3A19%3A00Z&ServiceURL=wss%3A%2F%2Fbot.example.com%2Fws&StatusReason=connection_failed&Duration=0&Error=resolve+failed",
]
parsed = bad = 0
reasons = set()
bodies = []
for f in sorted(glob.glob("/tmp/streaming-e2e/r*/events.jsonl")):
    for l in open(f):
        d = json.loads(l)
        if d.get("kind") == "callback" and d.get("name") == "stream":
            bodies.append(d.get("body") or urlencode(d.get("query") or {}))
bodies = bodies or SAMPLES
for body in bodies:
    try:
        ev = parse_call_event(body, {}, None, verify=False)
        good_ev = (
            ev.event in STREAM_EVENTS
            and ev.stream_id
            and ev.service_url
            and ev.event_time
        )
        if ev.event in ("stopped", "failed"):
            good_ev = good_ev and ev.stream_reason
        reasons.add(ev.stream_reason)
        parsed += 1 if good_ev else 0
        bad += 0 if good_ev else 1
    except Exception as e:
        bad += 1
        print("parse error", repr(e)[:80])
chk(
    "8.4 parse_call_event on captured stream callbacks",
    parsed > 0 and bad == 0,
    f"{parsed} parsed, {bad} bad, reasons={sorted(r for r in reasons if r)}",
)
print("SUMMARY", sum(1 for r in R if r[1]), "/", len(R), "pass")
