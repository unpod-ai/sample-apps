"""Streaming e2e harness: answer URL + signature-verifying WebSocket bot + callback sink.

Run (from this dir, with the SDK venv that has unpod from the GitHub branch):
    BOT_SECRET=<org webhook secret> PUBLIC_BASE=wss://<tunnel-host> \
      uvicorn bot:app --host 127.0.0.1 --port 28090

/answer query options (all optional):
  ct=mulaw|alaw|l16_8|l16_16|l16_24   audio format          kca=0|1  keepCallAlive
  cb=1 [cbm=GET|POST]                  statusCallbackUrl     wait=N   <Wait> after <Stream>
  at=inbound|outbound|both             audioTrack            bidi=0|1 bidirectional
  to=N streamTimeout   ext=a=1,b=2 extraHeaders   nc=60..100 noiseCancellationLevel
  plain=1                              no <Stream>, just <Wait wait or 60>
  rec=1                                add <Record recordSession> before the stream
  mode=<ws bot behaviour>              see MODES below
  close_after=N                        bot closes WS after N seconds (mode=close implied)

MODES (ws bot): echo (default) | close | checkpoint | clear | dtmf | wrongfmt |
  bigmsg | drop | noread | reject | badstart
Every event goes to $EVENTS (default /tmp/streaming-e2e/bot_events.jsonl).
Never prints the secret.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import time
from urllib.parse import urlencode

from fastapi import (
    FastAPI,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi import (
    Response as HttpResponse,
)
from unpod.telephony import answer as ans
from unpod.telephony import stream as st
from unpod.telephony.events import (
    WebhookSignatureError,
    parse_call_event,
    verify_signature,
)

SECRET = os.environ.get("BOT_SECRET", "")
PUBLIC_BASE = os.environ.get("PUBLIC_BASE", "ws://127.0.0.1:28090").rstrip("/")
HTTP_BASE = os.environ.get("PUBLIC_HTTP_BASE") or PUBLIC_BASE.replace(
    "wss://", "https://"
).replace("ws://", "http://")
EVENTS = os.environ.get("EVENTS", "/tmp/streaming-e2e/bot_events.jsonl")
os.makedirs(os.path.dirname(EVENTS), exist_ok=True)

app = FastAPI()
NONCES = st.NonceCache()
CT = {
    "mulaw": (st.MULAW, 8000),
    "alaw": (st.ALAW, 8000),
    "l16_8": (st.L16, 8000),
    "l16_16": (st.L16, 16000),
    "l16_24": (st.L16, 24000),
}


def log(kind: str, **data) -> None:
    rec = {"ts": round(time.time(), 3), "kind": kind, **data}
    with open(EVENTS, "a") as f:
        f.write(json.dumps(rec, default=str) + "\n")
    print(
        kind,
        {
            k: (v if len(str(v)) < 160 else str(v)[:160] + "...")
            for k, v in data.items()
        },
        flush=True,
    )


@app.get("/health")
def health():
    return {"ok": True, "secret_set": bool(SECRET), "public_base": PUBLIC_BASE}


@app.api_route("/answer", methods=["GET", "POST"])
async def answer(request: Request):
    q = dict(request.query_params)
    body = await request.body()
    sig = request.headers.get("x-webhook-signature")
    verified = None
    if request.method == "POST":  # 12.1 POST signed, 12.2 GET unsigned
        verified = bool(SECRET) and verify_signature(body, sig, SECRET)
    log(
        "answer",
        method=request.method,
        query=q,
        signed=bool(sig),
        verified=verified,
        form=body.decode(errors="replace")[:500],
    )
    r = ans.Response()
    if q.get("rec") == "1":
        r.record(
            record_session=True,
            callback_url=f"{HTTP_BASE}/cb/recording",
            callback_method="POST",
        )
    if q.get("plain") != "1":
        enc, rate = CT.get(q.get("ct", "mulaw"), CT["mulaw"])
        mode = q.get("mode") or ("close" if q.get("close_after") else "echo")
        wsq = {"token": "abc", "mode": mode}
        for k in (
            "close_after",
            "drop_after",
            "noread_for",
            "big_kb",
            "rounds",
            "gap",
            "role",
        ):
            if k in q:
                wsq[k] = q[k]
        kw: dict = dict(
            bidirectional=q.get("bidi", "1") == "1",
            content_type=enc,
            sample_rate=rate,
            keep_call_alive=q.get("kca", "1") == "1",
            audio_track=q.get("at", "inbound"),
        )
        if q.get("cb") == "1":
            kw["status_callback_url"] = f"{HTTP_BASE}/cb/stream"
            if q.get("cbm"):
                kw["status_callback_method"] = q["cbm"]
        if q.get("to"):
            kw["stream_timeout"] = int(q["to"])
        if q.get("ext"):
            kw["extra_headers"] = q["ext"]
        if q.get("nc"):
            kw["noise_cancellation"] = True
            kw["noise_cancellation_level"] = int(q["nc"])
        url = q.get("wsurl") or f"{PUBLIC_BASE}/ws?{urlencode(wsq)}"
        r.stream(url, **kw)
    if q.get("wait") or q.get("plain") == "1":
        r.wait(int(q.get("wait") or 60))
    if q.get("fmt") == "json":  # 2.2 JSON answer form
        body = json.dumps(r.to_json())
        log("answer_xml", xml=body, fmt="json")
        return HttpResponse(body, media_type="application/json")
    xml = r.to_xml()
    log("answer_xml", xml=xml)
    return HttpResponse(xml, media_type="application/xml")


@app.get("/tone.wav")
def tone_wav(secs: int = 6, hz: int = 880):
    """Public test tone for play_audio (8 kHz mono PCM16 WAV)."""
    import io
    import math
    import struct
    import wave

    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(8000)
        w.writeframes(
            b"".join(
                struct.pack("<h", int(9000 * math.sin(2 * math.pi * hz * i / 8000)))
                for i in range(8000 * secs)
            )
        )
    log("tone_fetch", secs=secs, hz=hz)
    return HttpResponse(buf.getvalue(), media_type="audio/wav")


@app.api_route("/cb/{name}", methods=["GET", "POST"])
async def cb(name: str, request: Request):
    body = await request.body()
    headers = dict(request.headers)
    info = {
        "name": name,
        "method": request.method,
        "query": dict(request.query_params),
        "content_type": headers.get("content-type"),
        "sig_header": "x-webhook-signature" in headers,
    }
    if request.method == "POST" and SECRET:
        try:
            ev = parse_call_event(body, headers, SECRET)
            info["verified"] = True
            info["event"] = (
                {
                    k: v
                    for k, v in ev.model_dump().items()
                    if v not in (None, "", {}, []) and k != "raw"
                }
                if hasattr(ev, "model_dump")
                else str(ev)
            )
        except WebhookSignatureError as e:
            info["verified"] = False
            info["error"] = str(e)
        except Exception as e:  # parse problem, keep going
            info["verified"] = "parse_error"
            info["error"] = f"{type(e).__name__}: {e}"
    if request.method == "GET" and SECRET and headers.get("x-webhook-signature"):
        raw_q = request.url.query.encode()
        cands = {
            "empty_body": b"",
            "raw_query": raw_q,
            "path_query": f"{request.url.path}?{request.url.query}".encode(),
            "full_public_url": f"{HTTP_BASE}{request.url.path}?{request.url.query}".encode(),
        }
        info["get_sig_matches"] = [
            n
            for n, b in cands.items()
            if verify_signature(b, headers["x-webhook-signature"], SECRET)
        ]
        info["verified"] = bool(info["get_sig_matches"])
    info["body"] = body.decode(errors="replace")[:1500]
    log("callback", **info)
    return {"ok": True}


def _decode_pcm(raw: bytes, fmt) -> list[int]:
    import array as _arr
    import audioop as _ao

    if fmt.encoding == st.L16:
        pcm = raw[: len(raw) // 2 * 2]
    elif fmt.encoding == st.MULAW:
        pcm = _ao.ulaw2lin(raw, 2)
    elif fmt.encoding == st.ALAW:
        pcm = _ao.alaw2lin(raw, 2)
    else:
        pcm = b""
    a = _arr.array("h")
    a.frombytes(pcm)
    return list(a)


def _goertzel(x: list[int], rate: int, hz: float) -> float:
    import math

    if not x:
        return 0.0
    k = 2 * math.cos(2 * math.pi * hz / rate)
    s1 = s2 = 0.0
    for v in x:
        s0 = v + k * s1 - s2
        s2, s1 = s1, s0
    return round((s1 * s1 + s2 * s2 - k * s1 * s2) / (len(x) ** 2), 1)


def _rms(x: list[int]) -> float:
    return round((sum(v * v for v in x) / len(x)) ** 0.5, 1) if x else 0.0


def _beep(fmt, ms: int = 300, hz: float = 1000.0) -> bytes:
    import array as _arr
    import audioop as _ao
    import math

    n = int(fmt.sample_rate * ms / 1000)
    a = _arr.array(
        "h",
        (
            int(
                11000
                * (
                    math.sin(2 * math.pi * hz * i / fmt.sample_rate)
                    + math.sin(2 * math.pi * 1500.0 * i / fmt.sample_rate)
                )
            )
            for i in range(n)
        ),
    )
    pcm = a.tobytes()
    if fmt.encoding == st.MULAW:
        return _ao.lin2ulaw(pcm, 2)
    if fmt.encoding == st.ALAW:
        return _ao.lin2alaw(pcm, 2)
    return pcm


SESS: dict = {}


async def _latency2_probe(rounds: int, gap: float) -> None:
    """Two phones side by side: beep goes out on one call, the other call's mic picks it up.
    Directions alternate A->B, B->A. t_send is taken right before playAudio."""
    await asyncio.sleep(6)
    for i in range(rounds * 2):
        tx, rx = ("A", "B") if i % 2 == 0 else ("B", "A")
        s = SESS[tx]
        t_send = time.time()
        await s["ws"].send_text(
            st.play_audio_message(s["sid"], _beep(s["fmt"], 300), s["fmt"])
        )
        t_after = time.time()
        await s["ws"].send_text(st.checkpoint_message(s["sid"], f"b{i}"))
        for _ in range(300):
            if f"b{i}" in s["lat"]["marks"]:
                break
            await asyncio.sleep(0.01)
        tp = s["lat"]["marks"].get(f"b{i}")
        log(
            "lat2_beep",
            i=i,
            tx=tx,
            rx=rx,
            t_send=t_send,
            send_call_ms=round((t_after - t_send) * 1000, 1),
            played_minus_beep_ms=round((tp - t_send) * 1000 - 300, 1) if tp else None,
        )
        await asyncio.sleep(gap)


async def _latency_probe(websocket, sid, fmt, lat) -> None:
    """Per round: (1) bare checkpoint = WS round trip to the SBC; (2) 300 ms 1 kHz beep + checkpoint.
    t_send is taken right before the playAudio send; playedStream arrival marks SBC playout end."""
    await asyncio.sleep(4)
    ms = 300
    for i in range(lat["n"]):
        t_cp = time.time()
        await websocket.send_text(st.checkpoint_message(sid, f"rtt{i}"))
        for _ in range(100):
            if f"rtt{i}" in lat["marks"]:
                break
            await asyncio.sleep(0.01)
        log(
            "lat_rtt",
            i=i,
            t_sent=t_cp,
            t_back=lat["marks"].get(f"rtt{i}"),
            rtt_ms=round((lat["marks"][f"rtt{i}"] - t_cp) * 1000, 1)
            if f"rtt{i}" in lat["marks"]
            else None,
        )
        t_send = time.time()
        await websocket.send_text(st.play_audio_message(sid, _beep(fmt, ms), fmt))
        t_after = time.time()
        await websocket.send_text(st.checkpoint_message(sid, f"bp{i}"))
        for _ in range(300):
            if f"bp{i}" in lat["marks"]:
                break
            await asyncio.sleep(0.01)
        tp = lat["marks"].get(f"bp{i}")
        log(
            "lat_beep",
            i=i,
            t_send=t_send,
            send_call_ms=round((t_after - t_send) * 1000, 1),
            t_played=tp,
            beep_ms=ms,
            played_minus_beep_ms=round((tp - t_send) * 1000 - ms, 1) if tp else None,
        )
        await asyncio.sleep(lat["gap"])


@app.websocket("/ws")
async def ws(websocket: WebSocket):
    q = dict(websocket.query_params)
    mode = q.get("mode", "echo")
    url = str(websocket.url)
    hdrs = dict(websocket.headers)
    sig = hdrs.get("x-unpod-signature")
    log(
        "ws_upgrade",
        url=url,
        mode=mode,
        has_sig=bool(sig),
        has_plivo_v3="x-plivo-signature-v3" in hdrs,
        sig_shape=(sig or "")[:12] + "…" if sig else None,
    )
    if SECRET:
        ok = st.verify_stream_request(
            hdrs, SECRET, url=url, public_base=PUBLIC_BASE, nonce_cache=NONCES
        )
        log("ws_signature", verified=ok)
        if not ok:
            await websocket.close(code=1008)
            return
    else:
        log("ws_signature", verified=None, note="BOT_SECRET unset: not verified")
    if mode == "reject":
        await websocket.close(code=1008)  # pre-accept close => HTTP 403 on upgrade
        return
    await websocket.accept()
    t0 = time.time()
    frames = {"media_in": 0, "play_out": 0}
    start = None
    sid = None
    try:
        first = json.loads(await websocket.receive_text())
        log(
            "ws_in",
            event=first.get("event"),
            seq=first.get("sequenceNumber"),
            msg=first,
        )
        if first.get("event") == "start":
            start = st.parse_start(first)
        sid = (start.stream_id if start else first.get("streamId")) or "unknown"
        fmt = start.media_format if start else st.MediaFormat()
        log(
            "ws_start",
            call_id=getattr(start, "call_id", None),
            stream_id=sid,
            fmt=fmt.model_dump(),
            extra_headers=getattr(start, "extra_headers", None),
            account=getattr(start, "account_id", None),
        )
        if mode == "dtmf":
            await websocket.send_text(st.send_dtmf_message(sid, "12#"))
        if mode == "badstart":
            pass
        closer = None
        if mode == "close" or q.get("close_after"):
            n = float(q.get("close_after", 5))

            async def _close():
                await asyncio.sleep(n)
                log("ws_bot_close", after=n)
                await websocket.close(code=1000)

            closer = asyncio.create_task(_close())
        if mode == "drop":
            await asyncio.sleep(float(q.get("drop_after", 3)))
            raise RuntimeError("mode=drop: abrupt end")  # server aborts the connection
        if mode == "noread":
            log("ws_noread", note="not reading frames")
            await asyncio.sleep(float(q.get("noread_for", 120)))
            return
        if mode == "bigmsg":
            await websocket.send_text(
                json.dumps(
                    {
                        "event": "playAudio",
                        "streamId": sid,
                        "media": {
                            "contentType": fmt.encoding,
                            "sampleRate": fmt.sample_rate,
                            "payload": base64.b64encode(
                                b"\x00" * (int(q.get("big_kb", 8192)) * 1024)
                            ).decode(),
                        },
                    }
                )
            )
        lat = None
        if mode == "latency2":
            role = q.get("role", "A")
            lat = {"fmt": fmt, "marks": {}, "role": role}
            SESS[role] = {"ws": websocket, "sid": sid, "fmt": fmt, "lat": lat}
            if len(SESS) == 2 and not SESS.get("_started"):
                SESS["_started"] = True
                asyncio.create_task(
                    _latency2_probe(int(q.get("rounds", 10)), float(q.get("gap", 4)))
                )
        if mode == "latency":
            lat = {
                "fmt": fmt,
                "marks": {},
                "n": int(q.get("rounds", 6)),
                "gap": float(q.get("gap", 5)),
            }
            asyncio.create_task(_latency_probe(websocket, sid, fmt, lat))
        while True:
            m = json.loads(await websocket.receive_text())
            ev = m.get("event")
            if lat is not None and ev == "playedStream":
                lat["marks"][m.get("name")] = time.time()
            if ev == "media":
                frames["media_in"] += 1
                if (
                    lat is not None
                    and (m.get("media") or {}).get("track", "inbound") == "inbound"
                ):
                    _p = (m.get("media") or {}).get("payload")
                    if _p:
                        _pcm = _decode_pcm(base64.b64decode(_p), fmt)
                        _l = {
                            "kind": "lat_frame",
                            "t": time.time(),
                            "mts": (m.get("media") or {}).get("timestamp"),
                            "role": (lat or {}).get("role"),
                            "chunk": (m.get("media") or {}).get("chunk"),
                            "g1k": _goertzel(_pcm, fmt.sample_rate, 1000.0),
                            "g15": _goertzel(_pcm, fmt.sample_rate, 1500.0),
                            "rms": _rms(_pcm),
                        }
                        with open(EVENTS.replace(".jsonl", ".lat.jsonl"), "a") as _f:
                            _f.write(json.dumps(_l) + "\n")
                if (m.get("media") or {}).get("track") == "outbound" and (
                    m.get("media") or {}
                ).get("payload"):
                    with open(
                        os.path.join(os.path.dirname(EVENTS), "outbound.raw"), "ab"
                    ) as _f:
                        _f.write(base64.b64decode(m["media"]["payload"]))
                _trk = "trk_" + str((m.get("media") or {}).get("track", "inbound"))
                frames[_trk] = frames.get(_trk, 0) + 1
                if frames["media_in"] <= 3 or frames["media_in"] % 100 == 0:
                    log(
                        "ws_in",
                        event="media",
                        n=frames["media_in"],
                        seq=m.get("sequenceNumber"),
                        track=(m.get("media") or {}).get("track"),
                        ts=(m.get("media") or {}).get("timestamp"),
                        chunk=(m.get("media") or {}).get("chunk"),
                        plen=len((m.get("media") or {}).get("payload", "")),
                        extra=m.get("extra_headers"),
                    )
                payload = (m.get("media") or {}).get("payload")
                if (
                    payload
                    and (m.get("media") or {}).get("track", "inbound") == "inbound"
                ):
                    import array as _arr
                    import audioop as _ao

                    _raw = base64.b64decode(payload)
                    if fmt.encoding == st.L16:
                        _pcm = _raw[: len(_raw) // 2 * 2]
                    elif fmt.encoding == st.MULAW:
                        _pcm = _ao.ulaw2lin(_raw, 2)
                    elif fmt.encoding == st.ALAW:
                        _pcm = _ao.alaw2lin(_raw, 2)
                    else:
                        _pcm = b""
                    _a = _arr.array("h")
                    _a.frombytes(_pcm)
                    frames["peak_in"] = max(
                        frames.get("peak_in", 0), max((abs(x) for x in _a), default=0)
                    )
                if (
                    (m.get("media") or {}).get("track", "inbound") == "inbound"
                    and payload
                    and mode not in ("dtmf", "latency", "latency2")
                ):
                    out_fmt = fmt
                    if (
                        mode == "wrongfmt" and frames["media_in"] == 5
                    ):  # 6.2 wrong format, stream must survive
                        out_fmt = st.MediaFormat(
                            encoding=st.L16 if fmt.encoding != st.L16 else st.MULAW,
                            sampleRate=8000,
                        )
                    await websocket.send_text(
                        st.play_audio_message(sid, base64.b64decode(payload), out_fmt)
                    )
                    frames["play_out"] += 1
                if mode == "checkpoint" and frames["media_in"] == 50:
                    await websocket.send_text(st.checkpoint_message(sid, "cp1"))
                    log("ws_out", event="checkpoint", name="cp1")
                if (
                    mode == "frames"
                ):  # 5.5 checkpoint, 6.5 sendDTMF, 6.2 wrong-format playAudio
                    n_in = frames["media_in"]
                    if n_in == 50:
                        await websocket.send_text(st.checkpoint_message(sid, "cp1"))
                        log("ws_out", event="checkpoint", name="cp1")
                    if n_in == 100:
                        await websocket.send_text(st.send_dtmf_message(sid, "12#"))
                        log("ws_out", event="sendDTMF", dtmf="12#")
                    if n_in == 150:
                        bad = st.MediaFormat(
                            encoding=st.L16 if fmt.encoding != st.L16 else st.MULAW,
                            sampleRate=8000,
                        )
                        await websocket.send_text(
                            st.play_audio_message(sid, b"\x00\x10" * 160, bad)
                        )
                        log(
                            "ws_out",
                            event="playAudio_wrong_format",
                            fmt=bad.model_dump(),
                        )
                if mode == "clear" and frames["media_in"] == 50:
                    await websocket.send_text(st.clear_audio_message(sid))
                    log("ws_out", event="clearAudio")
            else:
                log("ws_in", event=ev, msg=m)
    except WebSocketDisconnect as e:
        log(
            "ws_disconnect",
            sid=sid,
            code=e.code,
            after=round(time.time() - t0, 2),
            **frames,
        )
    except Exception as e:
        log(
            "ws_error",
            sid=sid,
            err=f"{type(e).__name__}: {e}",
            after=round(time.time() - t0, 2),
            **frames,
        )
        raise
    finally:
        log("ws_end", sid=sid, after=round(time.time() - t0, 2), **frames)


@app.get("/beep.wav")
async def _beep():
    import io
    import math
    import struct
    import wave

    buf = io.BytesIO()
    pcm = b"".join(
        struct.pack("<h", int(9000 * math.sin(2 * math.pi * 880 * i / 8000)))
        for i in range(8000 * 3)
    )
    with wave.open(buf, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(8000)
        w.writeframes(pcm)
    return HttpResponse(content=buf.getvalue(), media_type="audio/wav")
