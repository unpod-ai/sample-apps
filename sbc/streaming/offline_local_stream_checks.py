"""Local (offline) streaming checks against the installed SDK (GitHub branch).
Covers checklist sections 2/3 (builder validation), 4 (signature), 5/6 (frames),
11 (SDK + Pipecat serializer). No network, no calls."""

import asyncio
import base64
import hashlib
import hmac
import json
import math
import struct
import sys
import time

import unpod
from pipecat.audio.dtmf.types import KeypadEntry
from pipecat.frames.frames import (
    CancelFrame,
    EndFrame,
    InputAudioRawFrame,
    InputDTMFFrame,
    InputTransportMessageFrame,
    InterruptionFrame,
    OutputAudioRawFrame,
    OutputDTMFFrame,
)
from unpod.pipecat.handshake import parse_unpod_websocket
from unpod.pipecat.serializer import UnpodFrameSerializer
from unpod.telephony import answer as ans
from unpod.telephony import stream as st

RESULTS = []


def check(name, cond, detail=""):
    RESULTS.append((name, bool(cond), detail))
    print(
        ("PASS " if cond else "FAIL ")
        + name
        + (f"  [{detail}]" if detail and not cond else "")
    )


def raises(fn, *a, **k):
    try:
        fn(*a, **k)
    except ValueError:
        return True
    except Exception as e:  # wrong exception type
        print("   unexpected", type(e).__name__, e)
        return False
    return False


SECRET = "test-secret-not-real"
URL = "wss://bot.example.com/ws?token=abc&x=1"
now = int(time.time())


def hdr(t=None, n="a1b2c3d4e5f60718", url=URL, secret=SECRET, extra=()):
    t = now if t is None else t
    sigs = [st.stream_signature(secret, url, t, n)] + [
        st.stream_signature(s, url, t, n) for s in extra
    ]
    return {"X-Unpod-Signature": f"t={t},n={n}," + ",".join(f"v1={s}" for s in sigs)}


# ---- 4. signature -------------------------------------------------------
check(
    "4.2 SDK verify ok",
    st.verify_stream_request(hdr(), SECRET, url=URL, nonce_cache=st.NonceCache()),
)
raw = hmac.new(
    SECRET.encode(), f"ws.{now}.a1b2c3d4e5f60718.{URL}".encode(), hashlib.sha256
).hexdigest()
check("4.3 independent HMAC == v1", hdr()["X-Unpod-Signature"].endswith("v1=" + raw))
check(
    "4.4 tampered URL fails",
    not st.verify_stream_request(hdr(), SECRET, url=URL.replace("x=1", "x=2")),
)
check("4.4b wrong secret fails", not st.verify_stream_request(hdr(), "other", url=URL))
check(
    "4.5 old timestamp fails",
    not st.verify_stream_request(hdr(t=now - 301), SECRET, url=URL),
)
check(
    "4.5b within tolerance ok",
    st.verify_stream_request(hdr(t=now - 299), SECRET, url=URL),
)
nc = st.NonceCache()
check(
    "4.6 first use ok", st.verify_stream_request(hdr(), SECRET, url=URL, nonce_cache=nc)
)
check(
    "4.6 replay rejected",
    not st.verify_stream_request(hdr(), SECRET, url=URL, nonce_cache=nc),
)
check(
    "4.7 behind tunnel needs public_base",
    not st.verify_stream_request(
        hdr(), SECRET, url="ws://127.0.0.1:28090/ws?token=abc&x=1"
    )
    and st.verify_stream_request(
        hdr(),
        SECRET,
        url="ws://127.0.0.1:28090/ws?token=abc&x=1",
        public_base="wss://bot.example.com",
    ),
)
check(
    "4.7b spoofed host rejected",
    not st.verify_stream_request(
        hdr(),
        SECRET,
        url="ws://127.0.0.1:28090/ws?token=abc&x=1",
        public_base="wss://evil.example.org",
    ),
)
check(
    "4.8 X-Webhook-Signature not accepted",
    not st.verify_stream_request(
        {"X-Webhook-Signature": hdr()["X-Unpod-Signature"]}, SECRET, url=URL
    ),
)
check(
    "4.8b callback-style sig (no ws. prefix) fails",
    not st.verify_stream_request(
        {
            "X-Unpod-Signature": f"t={now},n=aaaaaaaaaaaaaaaa,v1="
            + hmac.new(
                SECRET.encode(),
                f"{now}.aaaaaaaaaaaaaaaa.{URL}".encode(),
                hashlib.sha256,
            ).hexdigest()
        },
        SECRET,
        url=URL,
    ),
)
check(
    "4.9 rotation: 2 v1 values, either secret passes",
    st.verify_stream_request(hdr(extra=["new-secret"]), SECRET, url=URL)
    and st.verify_stream_request(hdr(extra=["new-secret"]), "new-secret", url=URL),
)
check(
    "4.x empty secret / missing header / junk fail",
    not st.verify_stream_request(hdr(), "", url=URL)
    and not st.verify_stream_request({}, SECRET, url=URL)
    and not st.verify_stream_request({"X-Unpod-Signature": "garbage"}, SECRET, url=URL)
    and not st.verify_stream_request(
        {"X-Unpod-Signature": "t=abc,n=x,v1=y"}, SECRET, url=URL
    ),
)
check(
    "4.x case-insensitive header name",
    st.verify_stream_request(
        {"x-unpod-signature": hdr()["X-Unpod-Signature"]}, SECRET, url=URL
    ),
)

# ---- 2/3 builder + option validation ------------------------------------
check("2.7 http:// refused", raises(st.stream_options, "http://x/ws"))
check("2.7 empty url refused", raises(st.stream_options, ""))
check(
    "3.2 bidirectional + both refused",
    raises(st.stream_options, "wss://x", bidirectional=True, audio_track="both"),
)
check(
    "3.3 audioTrack inbound/outbound/both ok (non-bidi)",
    all(
        st.stream_options("wss://x", bidirectional=False, audio_track=t)["audio_track"]
        == t
        for t in ("inbound", "outbound", "both")
    ),
)
check(
    "3.3b bad audioTrack refused",
    raises(st.stream_options, "wss://x", audio_track="left"),
)
check(
    "3.6 streamTimeout 10 ok, 0 refused",
    st.stream_options("wss://x", stream_timeout=10)["stream_timeout"] == 10
    and raises(st.stream_options, "wss://x", stream_timeout=0),
)
check(
    "3.7 extraHeaders dict/str ok",
    st.extra_headers_str({"a": "1", "b-2": "x_y"}) in ("a=1,b-2=x_y",)
    and st.extra_headers_str("a=1,b-2=x_y") == "a=1,b-2=x_y",
)
check(
    "3.7 extraHeaders bad chars refused",
    raises(st.extra_headers_str, "a b=1") and raises(st.extra_headers_str, {"a;": "1"}),
)
check("3.7 extraHeaders >512B refused", raises(st.extra_headers_str, {"k": "v" * 600}))
check(
    "3.8 callback method GET/POST ok, PUT refused",
    st.stream_options("wss://x", status_callback_method="GET")["status_callback_method"]
    == "GET"
    and raises(st.stream_options, "wss://x", status_callback_method="PUT"),
)
check(
    "3.10 noise level 60/100 ok, 59/101 refused",
    all(st.stream_options("wss://x", noise_cancellation_level=l) for l in (60, 100))
    and raises(st.stream_options, "wss://x", noise_cancellation_level=59)
    and raises(st.stream_options, "wss://x", noise_cancellation_level=101),
)
check(
    "2.4 L16 rates 8/16/24k ok, 12k refused",
    all(
        st.MediaFormat(encoding=st.L16, sampleRate=r).validate_supported()
        for r in (8000, 16000, 24000)
    )
    and raises(st.MediaFormat(encoding=st.L16, sampleRate=12000).validate_supported),
)
check(
    "2.x G.711 8k only",
    raises(st.MediaFormat(encoding=st.MULAW, sampleRate=16000).validate_supported)
    and raises(st.MediaFormat(encoding=st.ALAW, sampleRate=16000).validate_supported),
)
check(
    "2.x unsupported encoding refused",
    raises(st.MediaFormat(encoding="audio/x-opus").validate_supported),
)
check(
    "2.5 default content type (no attr) = L16 8k",
    st.MediaFormat.parse(None).encoding == st.L16
    and st.MediaFormat.parse(None).sample_rate == 8000,
)

r = ans.Response().stream(
    "wss://bot.example.com/ws?token=abc",
    content_type=st.ALAW,
    status_callback_url="https://cb.example.com/cb",
    status_callback_method="POST",
    stream_timeout=30,
    extra_headers={"a": "1"},
    noise_cancellation=True,
    noise_cancellation_level=80,
    keep_call_alive=False,
)
x = r.to_xml()
check("11.1 builder emits <Stream> XML", "<Stream" in x, x[:200])
print("   XML:", x)
print("   JSON:", json.dumps(r.actions))
check(
    "11.1 JSON action carries all stream fields",
    r.actions[0]["type"] == "stream"
    and r.actions[0]["keep_call_alive"] is False
    and r.actions[0]["stream_timeout"] == 30
    and r.actions[0]["extra_headers"] == "a=1"
    and r.actions[0]["content_type"] == "audio/x-alaw;rate=8000",
)
check(
    "2.6 URL kept exactly (query+token)",
    r.actions[0]["url"] == "wss://bot.example.com/ws?token=abc",
)
check("11.1 builder rejects bad input", raises(ans.Response().stream, "http://x"))
check(
    "11.1 record/get_digits/get_input exist",
    all(hasattr(ans.Response, m) for m in ("record", "get_digits", "get_input")),
)

# ---- 5/6 frame helpers --------------------------------------------------
check(
    "6.3 checkpoint 256 ok / 257 refused / empty refused",
    json.loads(st.checkpoint_message("s", "a" * 256))["event"] == "checkpoint"
    and raises(st.checkpoint_message, "s", "a" * 257)
    and raises(st.checkpoint_message, "s", ""),
)
check(
    "6.5 sendDTMF 12# ok, lowercase upper-cased, bad chars/33 refused",
    json.loads(st.send_dtmf_message("s", "12#"))["dtmf"] == "12#"
    and json.loads(st.send_dtmf_message("s", "ab"))["dtmf"] == "AB"
    and raises(st.send_dtmf_message, "s", "1x")
    and raises(st.send_dtmf_message, "s", "1" * 33)
    and raises(st.send_dtmf_message, "s", ""),
)
START = {
    "event": "start",
    "sequenceNumber": 1,
    "streamId": "sid-1",
    "extra_headers": "a=1",
    "start": {
        "callId": "abc123-caller",
        "streamId": "sid-1",
        "accountId": "qa_org",
        "tracks": ["inbound"],
        "mediaFormat": {"encoding": "audio/x-alaw", "sampleRate": 8000},
    },
}
ps = st.parse_start(START)
check(
    "11.4 parse_start round-trip",
    ps.call_id == "abc123-caller"
    and ps.stream_id == "sid-1"
    and ps.media_format.encoding == st.ALAW
    and ps.extra_headers == "a=1"
    and ps.account_id == "qa_org",
)
check("11.4 parse_start non-start refused", raises(st.parse_start, {"event": "media"}))
check(
    "11.4 parse_start missing ids refused",
    raises(st.parse_start, {"event": "start", "start": {}}),
)
check(
    "11.4 parse_start accepts JSON str",
    st.parse_start(json.dumps(START)).stream_id == "sid-1",
)


class FakeWS:
    def __init__(self, msgs):
        self.msgs = list(msgs)

    async def receive_text(self):
        return self.msgs.pop(0)


async def main():
    # handshake
    s = await parse_unpod_websocket(
        FakeWS([json.dumps({"event": "connected"}), json.dumps(START)])
    )
    check("11.5 handshake skips leading frame", s.stream_id == "sid-1")
    check(
        "11.5 handshake gives up w/o start",
        await _araises(parse_unpod_websocket(FakeWS(["{}", "{}", "{}"]))),
    )

    hung = []

    async def hangup(cid):
        hung.append(cid)

    for enc, rate in (
        (st.MULAW, 8000),
        (st.ALAW, 8000),
        (st.L16, 8000),
        (st.L16, 16000),
        (st.L16, 24000),
    ):
        fmt = st.MediaFormat(encoding=enc, sampleRate=rate)
        ser = UnpodFrameSerializer(
            "sid", "call-caller", media_format=fmt, hangup=hangup
        )
        ser._pipeline_rate = 16000  # what setup() records from the pipeline
        # 440 Hz tone, 100 ms at 16k PCM16 -> serialize -> playAudio -> loop back as media -> deserialize
        pcm = b"".join(
            struct.pack("<h", int(12000 * math.sin(2 * math.pi * 440 * i / 16000)))
            for i in range(1600)
        )
        out = await ser.serialize(
            OutputAudioRawFrame(audio=pcm, sample_rate=16000, num_channels=1)
        )
        m = json.loads(out)
        ok = (
            m["event"] == "playAudio"
            and m["streamId"] == "sid"
            and m["media"]["contentType"] == enc
            and m["media"]["sampleRate"] == rate
        )
        payload = base64.b64decode(m["media"]["payload"])
        per_s = 2 if enc == st.L16 else 1
        ok = (
            ok and abs(len(payload) / per_s - rate * 0.1) <= rate * 0.02
        )  # ~100ms of audio
        check(f"11.5/6.1 serialize {enc} {rate}", ok, f"len={len(payload)}")
        # 1 s in 10 chunks (a streaming resampler needs more than one chunk to settle)
        got = b""
        for k in range(10):
            pcm_k = b"".join(
                struct.pack(
                    "<h",
                    int(12000 * math.sin(2 * math.pi * 440 * (k * 1600 + i) / 16000)),
                )
                for i in range(1600)
            )
            mk = json.loads(
                await ser.serialize(
                    OutputAudioRawFrame(audio=pcm_k, sample_rate=16000, num_channels=1)
                )
            )
            back = await ser.deserialize(
                json.dumps(
                    {
                        "event": "media",
                        "streamId": "sid",
                        "media": {
                            "track": "inbound",
                            "timestamp": str(k * 100),
                            "chunk": str(k),
                            "payload": mk["media"]["payload"],
                        },
                    }
                )
            )
            assert isinstance(back, InputAudioRawFrame) and back.sample_rate == 16000
            got += back.audio
        a = struct.unpack(f"<{len(got) // 2}h", got)
        zc = sum(1 for i in range(1, len(a)) if (a[i - 1] < 0) != (a[i] < 0))
        good = (
            len(a) == 16000 and 850 <= zc <= 950 and 11000 <= max(map(abs, a)) <= 13000
        )
        check(
            f"11.5 round-trip {enc} {rate} keeps 440Hz tone (1 s)",
            good,
            f"n={len(a)} zc={zc} peak={max(map(abs, a))}",
        )

    fmt = st.MediaFormat(encoding=st.MULAW, sampleRate=8000)
    ser = UnpodFrameSerializer.from_start(st.parse_start(START), hangup=hangup)
    ser._pipeline_rate = 16000
    check(
        "11.5 from_start uses start mediaFormat (alaw)",
        ser.input_format.encoding == st.ALAW,
    )
    d = await ser.deserialize(
        json.dumps({"event": "dtmf", "streamId": "sid-1", "dtmf": {"digit": "5"}})
    )
    check(
        "5.4/11.5 dtmf -> InputDTMFFrame",
        isinstance(d, InputDTMFFrame) and d.button == KeypadEntry.FIVE,
    )
    d = await ser.deserialize(json.dumps({"event": "dtmf", "dtmf": {"digit": "#"}}))
    check(
        "5.4 dtmf # -> InputDTMFFrame",
        isinstance(d, InputDTMFFrame) and d.button == KeypadEntry.POUND,
    )
    check(
        "5.4 bad dtmf digit ignored",
        await ser.deserialize(json.dumps({"event": "dtmf", "dtmf": {"digit": "Z"}}))
        is None,
    )
    o = await ser.serialize(OutputDTMFFrame(button=KeypadEntry.ONE))
    check(
        "11.5 OutputDTMFFrame -> sendDTMF",
        json.loads(o) == {"event": "sendDTMF", "streamId": "sid-1", "dtmf": "1"},
    )
    o = await ser.serialize(InterruptionFrame())
    check(
        "6.4/11.5 InterruptionFrame -> clearAudio",
        json.loads(o) == {"event": "clearAudio", "streamId": "sid-1"},
    )
    for ev in ("playedStream", "clearedAudio"):
        f = await ser.deserialize(
            json.dumps({"event": ev, "streamId": "sid-1", "name": "cp1"})
        )
        check(
            f"5.5/5.6 {ev} -> InputTransportMessageFrame",
            isinstance(f, InputTransportMessageFrame) and f.message["event"] == ev,
        )
    check(
        "11.5 outbound-track media ignored (audioTrack=both)",
        await ser.deserialize(
            json.dumps(
                {"event": "media", "media": {"track": "outbound", "payload": "AAAA"}}
            )
        )
        is None,
    )
    check(
        "11.5 bad base64 / empty payload / unknown event ignored",
        await ser.deserialize(
            json.dumps({"event": "media", "media": {"payload": "!!!"}})
        )
        is None
        and await ser.deserialize(json.dumps({"event": "media", "media": {}})) is None
        and await ser.deserialize(json.dumps({"event": "weird"})) is None
        and await ser.deserialize("not json") is None,
    )

    # hangup: EndFrame waits for queued audio (capped), CancelFrame immediate

    p = UnpodFrameSerializer.InputParams(hangup_drain_secs=0.3)
    ser2 = UnpodFrameSerializer(
        "sid", "callA", media_format=fmt, hangup=hangup, params=p
    )
    ser2._pipeline_rate = 8000
    pcm8 = b"\x00\x10" * 8000  # 1 s queued at the SBC
    await ser2.serialize(
        OutputAudioRawFrame(audio=pcm8, sample_rate=8000, num_channels=1)
    )
    hung.clear()
    t0 = time.monotonic()
    await ser2.serialize(EndFrame())
    dt = time.monotonic() - t0
    check(
        "11.6 EndFrame drains then hangs up (capped by hangup_drain_secs)",
        hung == ["callA"] and 0.25 <= dt < 0.8,
        f"dt={dt:.2f} hung={hung}",
    )
    await ser2.serialize(EndFrame())
    check("11.6 hangup only once", hung == ["callA"])
    ser3 = UnpodFrameSerializer("sid", "callB", media_format=fmt, hangup=hangup)
    hung.clear()
    t0 = time.monotonic()
    await ser3.serialize(CancelFrame())
    check(
        "11.6 CancelFrame hangs up at once",
        hung == ["callB"] and time.monotonic() - t0 < 0.2,
    )
    check(
        "11.6 auto_hang_up needs client/hangup", raises(UnpodFrameSerializer, "s", "c")
    )
    ser4 = UnpodFrameSerializer(
        "s", "c", params=UnpodFrameSerializer.InputParams(auto_hang_up=False)
    )
    check(
        "11.6 auto_hang_up=False builds w/o client",
        ser4 is not None and await ser4.serialize(EndFrame()) is None,
    )

    class E(Exception):
        status_code = 404

    async def gone(cid):
        raise E()

    ser5 = UnpodFrameSerializer("s", "c", hangup=gone)
    try:
        await ser5.serialize(CancelFrame())
        ok = True
    except Exception:
        ok = False
    check("11.6 hangup 404 (already ended) swallowed", ok)
    check(
        "11.5 unsupported format refused at build",
        raises(
            UnpodFrameSerializer,
            "s",
            "c",
            media_format=st.MediaFormat(encoding="audio/x-opus"),
            hangup=hangup,
        ),
    )
    check(
        "11.5 empty ids refused", raises(UnpodFrameSerializer, "", "c", hangup=hangup)
    )


async def _araises(coro):
    try:
        await coro
    except ValueError:
        return True
    except Exception:
        return False
    return False


asyncio.run(main())
bad = [n for n, ok, _ in RESULTS if not ok]
print(
    f"\n{len(RESULTS) - len(bad)}/{len(RESULTS)} passed; SDK {unpod.__version__ if hasattr(unpod, '__version__') else ''}"
)
if bad:
    print("FAILED:", *bad, sep="\n  ")
    sys.exit(1)
