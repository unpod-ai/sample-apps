"""Streaming e2e harness: answer URL + signature-checking WebSocket bot + callback sink.

Docs: https://docs.unpod.ai/telephony/apis/audio-streaming
Run:  BOT_SECRET=<webhook secret> PUBLIC_BASE=wss://<tunnel-host> \
        uv run uvicorn bot:app --port 28090
      (run_case.py and run_conf_multi.py start it for you, behind a tunnel)

/answer query options (all optional):
  ct=mulaw|alaw|l16_8|l16_16|l16_24   audio format          kca=0|1  keepCallAlive
  cb=1 [cbm=GET|POST]                  statusCallbackUrl     wait=N   <Wait> after <Stream>
  at=inbound|outbound|both             audioTrack            bidi=0|1 bidirectional
  to=N streamTimeout   ext=a=1,b=2 extraHeaders   nc=60..100 noiseCancellationLevel
  dial=+E164                           <Dial> after <Stream>: the handoff case
  plain=1                              no <Stream>, just <Wait wait or 60>
  rec=1                                add <Record recordSession> before the stream
  fmt=json                             JSON answer instead of XML
  mode=<ws bot behaviour>              see MODES below
  close_after=N                        bot closes WS after N seconds (mode=close implied)

Handoff: `dial=+E164&mode=close&close_after=15` echoes for 15 s, then the bot closes
the stream and Unpod dials the number.

MODES (ws bot): echo (default) | close | checkpoint | clear | dtmf | frames |
  wrongfmt | bigmsg | drop | noread | reject | latency | latency2
Every event goes to $EVENTS (default logs/bot_events.jsonl). Never prints the secret.
"""

from __future__ import annotations

import array
import asyncio
import base64
import io
import json
import math
import os
import struct
import time
import wave
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi import Response as HttpResponse
from unpod.pipecat.g711 import alaw_to_pcm, pcm_to_alaw, pcm_to_ulaw, ulaw_to_pcm
from unpod.telephony import answer as ans
from unpod.telephony import stream as st
from unpod.telephony.events import (
    WebhookSignatureError,
    parse_call_event,
    verify_signature,
)

HERE = Path(__file__).resolve().parent
SECRET = os.environ.get("BOT_SECRET", "")
PUBLIC_BASE = os.environ.get("PUBLIC_BASE", "ws://127.0.0.1:28090").rstrip("/")
HTTP_BASE = os.environ.get("PUBLIC_HTTP_BASE") or PUBLIC_BASE.replace(
    "wss://", "https://"
).replace("ws://", "http://")
EVENTS = os.environ.get("EVENTS", str(HERE / "logs" / "bot_events.jsonl"))
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
WS_PARAMS = (
    "close_after",
    "drop_after",
    "noread_for",
    "big_kb",
    "rounds",
    "gap",
    "role",
)
SESS: dict[str, Any] = {}


def log(kind: str, **data: Any) -> None:
    """Append one event to $EVENTS and echo a trimmed copy to stdout."""
    rec = {"ts": round(time.time(), 3), "kind": kind, **data}
    with open(EVENTS, "a") as f:
        f.write(json.dumps(rec, default=str) + "\n")
    short = {
        k: v if len(str(v)) < 160 else str(v)[:160] + "..." for k, v in data.items()
    }
    print(kind, short, flush=True)


def append(path: str, data: bytes) -> None:
    """Append to a log file. ponytail: blocking, fine for a few KB per frame."""
    with open(path, "ab") as f:
        f.write(data)


@app.get("/health")
def health() -> dict:
    return {"ok": True, "secret_set": bool(SECRET), "public_base": PUBLIC_BASE}


@app.api_route("/answer", methods=["GET", "POST"])
async def answer(request: Request) -> HttpResponse:
    """Build the answer XML/JSON from the query options in the module docstring."""
    q = dict(request.query_params)
    body = await request.body()
    sig = request.headers.get("x-webhook-signature")
    verified = None
    if request.method == "POST":  # POST is signed, GET is not
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
        url = q.get("wsurl") or f"{PUBLIC_BASE}/ws?{urlencode(ws_query(q))}"
        r.stream(url, **stream_options(q))
    if q.get("dial"):
        r.dial(q["dial"])
    if q.get("wait") or q.get("plain") == "1":
        r.wait(int(q.get("wait") or 60))
    if q.get("fmt") == "json":
        text = json.dumps(r.to_json())
        log("answer_xml", xml=text, fmt="json")
        return HttpResponse(text, media_type="application/json")
    xml = r.to_xml()
    log("answer_xml", xml=xml)
    return HttpResponse(xml, media_type="application/xml")


def ws_query(q: dict[str, str]) -> dict[str, str]:
    """The /ws query: bot mode plus the per-mode knobs passed through from /answer."""
    mode = q.get("mode") or ("close" if q.get("close_after") else "echo")
    return {"token": "abc", "mode": mode, **{k: q[k] for k in WS_PARAMS if k in q}}


def stream_options(q: dict[str, str]) -> dict[str, Any]:
    """`Response.stream()` keyword options from the /answer query."""
    enc, rate = CT.get(q.get("ct", "mulaw"), CT["mulaw"])
    kw: dict[str, Any] = {
        "bidirectional": q.get("bidi", "1") == "1",
        "content_type": enc,
        "sample_rate": rate,
        "keep_call_alive": q.get("kca", "1") == "1",
        "audio_track": q.get("at", "inbound"),
    }
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
    return kw


@app.get("/tone.wav")
def tone_wav(secs: int = 6, hz: int = 880) -> HttpResponse:
    """Public test tone for play_audio (8 kHz mono PCM16 WAV)."""
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
async def cb(name: str, request: Request) -> dict:
    """Log every status callback, with whether its signature checked out."""
    body = await request.body()
    headers = dict(request.headers)
    info: dict[str, Any] = {
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
            info["event"] = {
                k: v
                for k, v in ev.model_dump().items()
                if v not in (None, "", {}, []) and k != "raw"
            }
        except WebhookSignatureError as e:
            info["verified"] = False
            info["error"] = str(e)
        except Exception as e:  # noqa: BLE001 - a parse problem is a finding, keep going
            info["verified"] = "parse_error"
            info["error"] = f"{type(e).__name__}: {e}"
    if request.method == "GET" and SECRET and headers.get("x-webhook-signature"):
        query = request.url.query
        cands = {
            "empty_body": b"",
            "raw_query": query.encode(),
            "path_query": f"{request.url.path}?{query}".encode(),
            "full_public_url": f"{HTTP_BASE}{request.url.path}?{query}".encode(),
        }
        sig = headers["x-webhook-signature"]
        matches = [n for n, b in cands.items() if verify_signature(b, sig, SECRET)]
        info["get_sig_matches"] = matches
        info["verified"] = bool(matches)
    info["body"] = body.decode(errors="replace")[:1500]
    log("callback", **info)
    return {"ok": True}


def decode_pcm(raw: bytes, fmt: st.MediaFormat) -> list[int]:
    """Stream payload -> 16-bit samples."""
    if fmt.encoding == st.MULAW:
        pcm = ulaw_to_pcm(raw)
    elif fmt.encoding == st.ALAW:
        pcm = alaw_to_pcm(raw)
    else:
        pcm = raw[: len(raw) // 2 * 2]
    samples = array.array("h")
    samples.frombytes(pcm)
    return list(samples)


def goertzel(x: list[int], rate: int, hz: float) -> float:
    """Power of one frequency in `x` (detects the probe beeps)."""
    if not x:
        return 0.0
    k = 2 * math.cos(2 * math.pi * hz / rate)
    s1 = s2 = 0.0
    for v in x:
        s0 = v + k * s1 - s2
        s2, s1 = s1, s0
    return round((s1 * s1 + s2 * s2 - k * s1 * s2) / (len(x) ** 2), 1)


def rms(x: list[int]) -> float:
    return round((sum(v * v for v in x) / len(x)) ** 0.5, 1) if x else 0.0


def beep(fmt: st.MediaFormat, ms: int = 300, hz: float = 1000.0) -> bytes:
    """A `hz` + 1.5 kHz beep in the stream's encoding."""
    rate = fmt.sample_rate
    pcm = array.array(
        "h",
        (
            int(
                11000
                * (
                    math.sin(2 * math.pi * hz * i / rate)
                    + math.sin(2 * math.pi * 1500.0 * i / rate)
                )
            )
            for i in range(int(rate * ms / 1000))
        ),
    ).tobytes()
    if fmt.encoding == st.MULAW:
        return pcm_to_ulaw(pcm)
    if fmt.encoding == st.ALAW:
        return pcm_to_alaw(pcm)
    return pcm


async def wait_mark(marks: dict[str, float], name: str, tries: int) -> float | None:
    """Wait up to `tries` x 10 ms for the `playedStream` of checkpoint `name`."""
    for _ in range(tries):
        if name in marks:
            break
        await asyncio.sleep(0.01)
    return marks.get(name)


async def latency2_probe(rounds: int, gap: float) -> None:
    """Two phones side by side: a beep goes out on one call and the other call's
    mic picks it up. Directions alternate A->B, B->A. t_send is taken right
    before playAudio."""
    await asyncio.sleep(6)
    for i in range(rounds * 2):
        tx, rx = ("A", "B") if i % 2 == 0 else ("B", "A")
        s = SESS[tx]
        t_send = time.time()
        await s["ws"].send_text(
            st.play_audio_message(s["sid"], beep(s["fmt"]), s["fmt"])
        )
        t_after = time.time()
        await s["ws"].send_text(st.checkpoint_message(s["sid"], f"b{i}"))
        tp = await wait_mark(s["lat"]["marks"], f"b{i}", 300)
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


async def latency_probe(
    websocket: WebSocket, sid: str, fmt: st.MediaFormat, lat: dict[str, Any]
) -> None:
    """Per round: (1) bare checkpoint = WS round trip to the SBC; (2) 300 ms
    beep + checkpoint. playedStream arrival marks the SBC's playout end."""
    await asyncio.sleep(4)
    ms = 300
    for i in range(lat["n"]):
        t_cp = time.time()
        await websocket.send_text(st.checkpoint_message(sid, f"rtt{i}"))
        t_back = await wait_mark(lat["marks"], f"rtt{i}", 100)
        log(
            "lat_rtt",
            i=i,
            t_sent=t_cp,
            t_back=t_back,
            rtt_ms=round((t_back - t_cp) * 1000, 1) if t_back else None,
        )
        t_send = time.time()
        await websocket.send_text(st.play_audio_message(sid, beep(fmt, ms), fmt))
        t_after = time.time()
        await websocket.send_text(st.checkpoint_message(sid, f"bp{i}"))
        tp = await wait_mark(lat["marks"], f"bp{i}", 300)
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


def other_format(fmt: st.MediaFormat) -> st.MediaFormat:
    """A format the stream did not negotiate (the wrong-format cases)."""
    enc = st.L16 if fmt.encoding != st.L16 else st.MULAW
    return st.MediaFormat(encoding=enc, sampleRate=8000)


@app.websocket("/ws")
async def ws(websocket: WebSocket) -> None:
    """The stream bot: verify, read `start`, then act out `mode` on the media."""
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
    frames: dict[str, int] = {"media_in": 0, "play_out": 0}
    tasks: list[asyncio.Task] = []  # keep background probes referenced
    sid = None
    try:
        first = json.loads(await websocket.receive_text())
        log(
            "ws_in",
            event=first.get("event"),
            seq=first.get("sequenceNumber"),
            msg=first,
        )
        start = st.parse_start(first) if first.get("event") == "start" else None
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
        if mode == "close" or q.get("close_after"):
            n = float(q.get("close_after", 5))

            async def close_later() -> None:
                await asyncio.sleep(n)
                log("ws_bot_close", after=n)
                await websocket.close(code=1000)

            tasks.append(asyncio.create_task(close_later()))
        if mode == "drop":
            await asyncio.sleep(float(q.get("drop_after", 3)))
            raise RuntimeError("mode=drop: abrupt end")  # server aborts the connection
        if mode == "noread":
            log("ws_noread", note="not reading frames")
            await asyncio.sleep(float(q.get("noread_for", 120)))
            return
        if mode == "bigmsg":
            size = int(q.get("big_kb", 8192)) * 1024
            payload = base64.b64encode(b"\x00" * size).decode()
            media = {
                "contentType": fmt.encoding,
                "sampleRate": fmt.sample_rate,
                "payload": payload,
            }
            await websocket.send_text(
                json.dumps({"event": "playAudio", "streamId": sid, "media": media})
            )
        lat: dict[str, Any] | None = None
        if mode == "latency2":
            role = q.get("role", "A")
            lat = {"fmt": fmt, "marks": {}, "role": role}
            SESS[role] = {"ws": websocket, "sid": sid, "fmt": fmt, "lat": lat}
            if "A" in SESS and "B" in SESS and not SESS.get("_started"):
                SESS["_started"] = True
                rounds, gap = int(q.get("rounds", 10)), float(q.get("gap", 4))
                tasks.append(asyncio.create_task(latency2_probe(rounds, gap)))
        if mode == "latency":
            lat = {
                "fmt": fmt,
                "marks": {},
                "n": int(q.get("rounds", 6)),
                "gap": float(q.get("gap", 5)),
            }
            tasks.append(asyncio.create_task(latency_probe(websocket, sid, fmt, lat)))
        while True:
            m = json.loads(await websocket.receive_text())
            ev = m.get("event")
            if lat is not None and ev == "playedStream":
                lat["marks"][m.get("name")] = time.time()
            if ev != "media":
                log("ws_in", event=ev, msg=m)
                continue
            await on_media(websocket, m, sid, fmt, mode, frames, lat)
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


async def on_media(
    websocket: WebSocket,
    m: dict[str, Any],
    sid: str,
    fmt: st.MediaFormat,
    mode: str,
    frames: dict[str, int],
    lat: dict[str, Any] | None,
) -> None:
    """One `media` frame: count it, measure it, and echo or probe per `mode`."""
    media = m.get("media") or {}
    track = media.get("track", "inbound")
    payload = media.get("payload")
    frames["media_in"] += 1
    frames[f"trk_{track}"] = frames.get(f"trk_{track}", 0) + 1
    n_in = frames["media_in"]
    if n_in <= 3 or n_in % 100 == 0:
        log(
            "ws_in",
            event="media",
            n=n_in,
            seq=m.get("sequenceNumber"),
            track=track,
            ts=media.get("timestamp"),
            chunk=media.get("chunk"),
            plen=len(payload or ""),
            extra=m.get("extra_headers"),
        )
    raw = base64.b64decode(payload) if payload else b""
    if raw and track == "outbound":
        append(os.path.join(os.path.dirname(EVENTS), "outbound.raw"), raw)
    if raw and track == "inbound":
        await on_caller_audio(websocket, raw, media, sid, fmt, mode, frames, lat)
    await on_nth_frame(websocket, sid, fmt, mode, n_in)


async def on_caller_audio(
    websocket: WebSocket,
    raw: bytes,
    media: dict[str, Any],
    sid: str,
    fmt: st.MediaFormat,
    mode: str,
    frames: dict[str, int],
    lat: dict[str, Any] | None,
) -> None:
    """Caller audio: track the peak, record probe levels, and echo it back."""
    n_in = frames["media_in"]
    pcm = decode_pcm(raw, fmt)
    frames["peak_in"] = max(frames.get("peak_in", 0), max(map(abs, pcm), default=0))
    if lat is not None:
        rate = fmt.sample_rate
        rec = {
            "kind": "lat_frame",
            "t": time.time(),
            "mts": media.get("timestamp"),
            "role": lat.get("role"),
            "chunk": media.get("chunk"),
            "g1k": goertzel(pcm, rate, 1000.0),
            "g15": goertzel(pcm, rate, 1500.0),
            "rms": rms(pcm),
        }
        append(
            EVENTS.replace(".jsonl", ".lat.jsonl"), (json.dumps(rec) + "\n").encode()
        )
    if mode not in ("dtmf", "latency", "latency2"):
        # wrongfmt: one frame in a format the stream did not ask for; it must survive
        out_fmt = other_format(fmt) if mode == "wrongfmt" and n_in == 5 else fmt
        await websocket.send_text(st.play_audio_message(sid, raw, out_fmt))
        frames["play_out"] += 1


async def on_nth_frame(
    websocket: WebSocket, sid: str, fmt: st.MediaFormat, mode: str, n_in: int
) -> None:
    """Bot-to-SBC frames sent at fixed media-frame counts (any track)."""
    if mode in ("checkpoint", "frames") and n_in == 50:
        await websocket.send_text(st.checkpoint_message(sid, "cp1"))
        log("ws_out", event="checkpoint", name="cp1")
    if mode == "frames" and n_in == 100:
        await websocket.send_text(st.send_dtmf_message(sid, "12#"))
        log("ws_out", event="sendDTMF", dtmf="12#")
    if mode == "frames" and n_in == 150:
        bad = other_format(fmt)
        await websocket.send_text(st.play_audio_message(sid, b"\x00\x10" * 160, bad))
        log("ws_out", event="playAudio_wrong_format", fmt=bad.model_dump())
    if mode == "clear" and n_in == 50:
        await websocket.send_text(st.clear_audio_message(sid))
        log("ws_out", event="clearAudio")
