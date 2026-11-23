"""Raw-WebSocket test bot for the UNPO-647 streaming checklist (no Pipecat).

GET|POST /answer?scn=<name>&... builds the <Stream> answer from query switches.
/ws?scn=..&mode=.. logs every frame to events.jsonl and (mode=suite) runs a scripted check.
/cb/<name> records status callbacks.  Scratch tool: not part of any repo.
"""

from __future__ import annotations

import asyncio
import audioop
import base64
import json
import math
import os
import struct
import time
from pathlib import Path
from urllib.parse import parse_qs

import uvicorn
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import Response

OUT = Path(os.environ.get("TBOT_OUT", ".")) / "events.jsonl"
SECRET = os.environ.get("UNPOD_WEBHOOK_SECRET")
app = FastAPI()

CT = {
    "mulaw": "audio/x-mulaw;rate=8000",
    "alaw": "audio/x-alaw;rate=8000",
    "l16_8": "audio/x-l16;rate=8000",
    "l16_16": "audio/x-l16;rate=16000",
    "l16_24": "audio/x-l16;rate=24000",
}


def rec(kind: str, **kw) -> None:
    with OUT.open("a") as f:
        f.write(
            json.dumps({"t": round(time.time(), 3), "kind": kind, **kw}, default=str)
            + "\n"
        )


def xh(headers) -> dict:
    return {k: v for k, v in headers.items() if k.lower().startswith("x-")}


def tone_frames(
    encoding: str, rate: int, secs: float, hz: float = 440.0
) -> list[bytes]:
    n = int(rate * 0.02)
    frames, phase = [], 0
    for _ in range(int(secs * 50)):
        pcm = b"".join(
            struct.pack(
                "<h", int(8000 * math.sin(2 * math.pi * hz * (phase + i) / rate))
            )
            for i in range(n)
        )
        phase += n
        if encoding == "audio/x-mulaw":
            frames.append(audioop.lin2ulaw(pcm, 2))
        elif encoding == "audio/x-alaw":
            frames.append(audioop.lin2alaw(pcm, 2))
        else:
            frames.append(pcm)
    return frames


@app.api_route("/answer", methods=["GET", "POST"])
async def answer(request: Request):
    q = dict(request.query_params)
    body = (await request.body()).decode("utf-8", "replace")
    form = {k: v[0] for k, v in parse_qs(body).items()}
    rec(
        "answer",
        method=request.method,
        scn=q.get("scn"),
        event=form.get("Event") or q.get("Event"),
        xheaders=xh(request.headers),
        params=form,
    )
    if (form.get("Event") or "StartApp") != "StartApp":
        return Response("<Response></Response>", media_type="application/xml")
    host = request.headers.get("x-forwarded-host") or request.headers["host"]
    scn = q.get("scn", "x")
    mode = q.get("mode", "echo")
    wsq = f"scn={scn}&mode={mode}"
    if q.get("wsq"):
        wsq += "&" + q["wsq"]
    if q.get("sdk"):
        wsq += "&sdk=1"
    url = (
        "wss://does-not-exist.invalid/ws"
        if q.get("badurl")
        else f"wss://{host}/ws?{wsq}"
    )
    attrs = []
    if q.get("ct") in CT:
        attrs.append(f'contentType="{CT[q["ct"]]}"')
    for key, attr in (
        ("bidi", "bidirectional"),
        ("track", "audioTrack"),
        ("kca", "keepCallAlive"),
        ("timeout", "streamTimeout"),
        ("hdrs", "extraHeaders"),
        ("cbm", "statusCallbackMethod"),
        ("noise", "noiseCancellationLevel"),
    ):
        if q.get(key):
            attrs.append(f'{attr}="{q[key]}"')
    if not q.get("nocb"):
        attrs.append(f'statusCallbackUrl="https://{host}/cb/stream?scn={scn}"')
    tail = f'<Wait length="{q["after"]}"/>' if q.get("after") else ""
    xml = f"<Response><Stream {' '.join(attrs)}>{url.replace('&', '&amp;')}</Stream>{tail}</Response>"
    if q.get("chain"):
        parts = []
        for c in q["chain"].split(","):
            cb = f'statusCallbackUrl="https://{host}/cb/stream?scn={scn}-{c}"'
            if c == "conn":
                u = "wss://does-not-exist.invalid/ws"
            else:
                u = f"wss://{host}/ws?scn={scn}-{c}&amp;mode={ {'auth': 'auth403', 'drop': 'drop', 'big': 'big'}[c] }"
            parts.append(
                f'<Stream bidirectional="true" keepCallAlive="true" {cb}>{u}</Stream>'
            )
        xml = f'<Response>{"".join(parts)}<Wait length="3"/></Response>'
        rec("answer_xml", scn=scn, xml=xml)
        return Response(xml, media_type="application/xml")
    if q.get("sdk") and not q.get("bad"):
        from unpod.telephony.answer import Response as SdkAnswer

        resp = SdkAnswer().stream(
            url,
            bidirectional=True,
            keep_call_alive=True,
            content_type="audio/x-mulaw",
            sample_rate=8000,
            status_callback_url=f"https://{host}/cb/stream?scn={scn}",
            extra_headers={"sdk": "1"},
        )
        xml = resp.to_xml()
        rec("answer_xml", scn=scn, xml=xml, via="sdk Answer().stream()")
        return Response(xml, media_type="application/xml")
    if q.get("bad"):
        good = f"wss://{host}/ws?scn={scn}&amp;mode=echo"
        cases = {
            "bidi_both": f'<Stream bidirectional="true" audioTrack="both">{good}</Stream>',
            "noise59": f'<Stream noiseCancellationLevel="59">{good}</Stream>',
            "timeout0": f'<Stream streamTimeout="0">{good}</Stream>',
            "http": "<Stream>http://example.com/ws</Stream>",
            "empty": "<Stream></Stream>",
            "long": f"<Stream>wss://{host}/ws?x={'a' * 2100}</Stream>",
        }
        body = "".join(cases[c] for c in q["bad"].split(",") if c in cases)
        xml = f'<Response>{body}<Wait length="{q.get("after", 5)}"/></Response>'
    if q.get("plain"):
        xml = f'<Response><Wait length="{q.get("wait", 60)}"/></Response>'
    rec("answer_xml", scn=scn, xml=xml)
    return Response(xml, media_type="application/xml")


@app.get("/beep.wav")
async def beep():
    import io
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
    rec("beep_fetch")
    return Response(buf.getvalue(), media_type="audio/wav")


@app.api_route("/cb/{name}", methods=["GET", "POST"])
async def cb(name: str, request: Request):
    body = (await request.body()).decode("utf-8", "replace")
    parsed = {k: v[0] for k, v in parse_qs(body).items()} or body[:500]
    rec(
        "cb",
        name=name,
        method=request.method,
        query=dict(request.query_params),
        body=parsed,
        xheaders=xh(request.headers),
    )
    return Response("ok")


@app.websocket("/ws")
async def ws(websocket: WebSocket):
    q = dict(websocket.query_params)
    mode, scn = q.get("mode", "echo"), q.get("scn", "x")
    hdrs = xh(websocket.headers)
    sig_ok = None
    if SECRET:
        try:
            from unpod.telephony.stream import verify_stream_request

            sig_ok = bool(
                verify_stream_request(websocket.headers, SECRET, url=str(websocket.url))
            )
        except Exception as e:
            sig_ok = f"error: {e}"
    rec(
        "ws_open",
        scn=scn,
        mode=mode,
        url=str(websocket.url),
        xheaders=hdrs,
        signature_verified=sig_ok,
    )
    if mode == "auth403":
        await websocket.close(code=1008)
        rec("ws_refused", scn=scn)
        return
    await websocket.accept()
    st: dict = {
        "sid": None,
        "enc": "audio/x-mulaw",
        "rate": 8000,
        "seq": [],
        "bad": [],
        "played": {},
        "cleared": 0,
        "dtmf": [],
        "media": 0,
        "first_ts": None,
        "extra": q.get("hdrs_expect"),
    }
    results: list[tuple[str, bool, str]] = []

    async def send(obj: dict) -> None:
        await websocket.send_text(json.dumps(obj))
        rec(
            "tx",
            scn=scn,
            ev=obj.get("event"),
            name=obj.get("name"),
            dtmf=obj.get("dtmf"),
        )

    async def play(
        frames: list[bytes],
        pace: float = 0.0,
        enc: str | None = None,
        rate: int | None = None,
    ) -> None:
        for fr in frames:
            if sdk:
                await rawsend(
                    S.play_audio_message(
                        st["sid"],
                        fr,
                        S.MediaFormat(
                            encoding=enc or st["enc"], sampleRate=rate or st["rate"]
                        ),
                    ),
                    "playAudio",
                )
            else:
                await send(
                    {
                        "event": "playAudio",
                        "streamId": st["sid"],
                        "media": {
                            "contentType": enc or st["enc"],
                            "sampleRate": rate or st["rate"],
                            "payload": base64.b64encode(fr).decode(),
                        },
                    }
                )
            if pace:
                await asyncio.sleep(pace)

    sdk = bool(q.get("sdk"))
    if sdk:
        from unpod.telephony import stream as S

    async def rawsend(text: str, ev: str, **kw) -> None:
        await websocket.send_text(text)
        rec("tx", scn=scn, ev=ev, via="sdk" if sdk else "raw", **kw)

    async def waitfor(pred, timeout: float) -> bool:
        end = time.time() + timeout
        while time.time() < end:
            if pred():
                return True
            await asyncio.sleep(0.05)
        return pred()

    async def suite() -> None:
        await waitfor(lambda: st["sid"], 5)
        sid = st["sid"]
        await asyncio.sleep(1)
        # 5.5 / 6.3: checkpoint after 1 s of audio -> playedStream
        await play(tone_frames(st["enc"], st["rate"], 1.0), 0.02)
        await (
            rawsend(S.checkpoint_message(sid, "cp1"), "checkpoint", name="cp1")
            if sdk
            else send({"event": "checkpoint", "streamId": sid, "name": "cp1"})
        )
        results.append(
            (
                "5.5/6.3 playedStream cp1",
                await waitfor(lambda: "cp1" in st["played"], 5),
                "",
            )
        )
        # 6.2: wrong format dropped, stream stays up
        bad = struct.pack("<160h", *([1000] * 160))
        enc_wrong = "audio/x-l16" if st["enc"] != "audio/x-l16" else "audio/x-mulaw"
        await play([bad], enc=enc_wrong, rate=8000)
        await asyncio.sleep(0.5)
        await play(tone_frames(st["enc"], st["rate"], 0.5), 0.02)
        await (
            rawsend(
                S.checkpoint_message(sid, "cp_after_wrong"),
                "checkpoint",
                name="cp_after_wrong",
            )
            if sdk
            else send(
                {"event": "checkpoint", "streamId": sid, "name": "cp_after_wrong"}
            )
        )
        results.append(
            (
                "6.2 wrong format dropped, stream alive",
                await waitfor(lambda: "cp_after_wrong" in st["played"], 5),
                "",
            )
        )
        # 6.3: 300-char checkpoint name refused (no playedStream)
        long_name = "n" * 300
        if sdk:
            try:
                S.checkpoint_message(sid, long_name)
                results.append(
                    (
                        "11.4 SDK refuses 300-char checkpoint name client-side",
                        False,
                        "no error",
                    )
                )
            except ValueError as e:
                results.append(
                    (
                        "11.4 SDK refuses 300-char checkpoint name client-side",
                        True,
                        str(e)[:60],
                    )
                )
        await send({"event": "checkpoint", "streamId": sid, "name": long_name})
        await asyncio.sleep(2)
        results.append(
            (
                "6.3 300-char checkpoint refused by SBC",
                long_name not in st["played"],
                "",
            )
        )
        # 6.5: sendDTMF good then bad characters (stream must stay up)
        await (
            rawsend(S.send_dtmf_message(sid, "12#"), "sendDTMF", dtmf="12#")
            if sdk
            else send({"event": "sendDTMF", "streamId": sid, "dtmf": "12#"})
        )
        await asyncio.sleep(1.5)
        if sdk:
            try:
                S.send_dtmf_message(sid, "1x")
                results.append(
                    (
                        "11.4 SDK refuses bad DTMF characters client-side",
                        False,
                        "no error",
                    )
                )
            except ValueError as e:
                results.append(
                    (
                        "11.4 SDK refuses bad DTMF characters client-side",
                        True,
                        str(e)[:60],
                    )
                )
        await send({"event": "sendDTMF", "streamId": sid, "dtmf": "1x"})
        await asyncio.sleep(1)
        # 6.4: long audio, clearAudio after 1 s -> clearedAudio, pending checkpoint not played
        await play(tone_frames(st["enc"], st["rate"], 6.0))
        await (
            rawsend(S.checkpoint_message(sid, "cp_long"), "checkpoint", name="cp_long")
            if sdk
            else send({"event": "checkpoint", "streamId": sid, "name": "cp_long"})
        )
        await asyncio.sleep(1.0)
        c0 = st["cleared"]
        await (
            rawsend(S.clear_audio_message(sid), "clearAudio")
            if sdk
            else send({"event": "clearAudio", "streamId": sid})
        )
        ok_c = await waitfor(lambda: st["cleared"] > c0, 3)
        await asyncio.sleep(3)
        results.append(("6.4 clearedAudio received", ok_c, ""))
        results.append(
            ("6.4 pending checkpoint not played", "cp_long" not in st["played"], "")
        )
        await asyncio.sleep(1)
        await websocket.close(code=1000)

    async def slow() -> None:
        await asyncio.sleep(90)

    async def guard(coro) -> None:
        try:
            await coro
        except Exception as e:
            import traceback

            rec(
                "driver_error", scn=scn, error=repr(e), tb=traceback.format_exc()[-700:]
            )
            try:
                await websocket.close(code=1011)
            except Exception:
                pass

    driver = None
    if mode == "suite":
        driver = asyncio.create_task(guard(suite()))
    elif mode == "slow":
        # stop reading: SBC backpressure -> degraded slow_connection_*, then failed
        rec("slow_mode", scn=scn)
        try:
            await slow()
        finally:
            rec("slow_end", scn=scn)
        return
    elif mode == "tone":

        async def tone() -> None:
            await waitfor(lambda: st["sid"], 5)
            await play(tone_frames(st["enc"], st["rate"], 4.0), 0.02)
            await asyncio.sleep(2)
            await websocket.close(code=1000)

        driver = asyncio.create_task(tone())
    elif mode == "drop":
        await asyncio.sleep(2.5)
        rec("drop_mode", scn=scn)
        raise RuntimeError("test bot drops the connection")
    elif mode == "big":
        await asyncio.sleep(1.0)
        rec("big_mode", scn=scn)
        await websocket.send_text(
            json.dumps(
                {
                    "event": "playAudio",
                    "streamId": "x",
                    "media": {"payload": "A" * 140000},
                }
            )
        )
        await asyncio.sleep(8)
        return
    elif mode.startswith("close"):
        secs = float(q.get("after", 6))

        async def closer() -> None:
            await asyncio.sleep(secs)
            await websocket.close(code=1000)

        driver = asyncio.create_task(closer())

    try:
        while True:
            raw = await websocket.receive_text()
            try:
                m = json.loads(raw)
            except ValueError:
                continue
            ev = m.get("event")
            if "sequenceNumber" in m:
                st["seq"].append(m["sequenceNumber"])
            if not m.get("streamId") and ev != "start":
                st["bad"].append((ev, "no streamId"))
            if ev == "start":
                s = m.get("start", {})
                st["sid"] = s.get("streamId") or m.get("streamId")
                mf = s.get("mediaFormat") or {}
                st["enc"] = mf.get("encoding", st["enc"])
                st["rate"] = int(mf.get("sampleRate", st["rate"]))
                rec("rx_start", scn=scn, msg=m)
                if sdk:
                    try:
                        ps = S.parse_start(m)
                        results.append(
                            (
                                "11.4 SDK parse_start on the real start frame",
                                True,
                                f"call_id={ps.call_id[-12:]} fmt={ps.media_format.encoding}@{ps.media_format.sample_rate} tracks={ps.tracks} extra={ps.extra_headers!r}",
                            )
                        )
                    except Exception as e:
                        results.append(
                            (
                                "11.4 SDK parse_start on the real start frame",
                                False,
                                str(e)[:80],
                            )
                        )
            elif ev == "media":
                st["media"] += 1
                md = m.get("media", {})
                raw_len = len(base64.b64decode(md.get("payload", "")))
                if st["media"] <= 3 or st["media"] % 100 == 0:
                    rec(
                        "rx_media",
                        scn=scn,
                        n=st["media"],
                        track=md.get("track"),
                        ts=md.get("timestamp"),
                        ts_type=type(md.get("timestamp")).__name__,
                        chunk=md.get("chunk"),
                        bytes=raw_len,
                        seq=m.get("sequenceNumber"),
                        extra_headers=m.get("extra_headers"),
                        streamId=m.get("streamId"),
                    )
                if mode == "echo" and st["sid"]:
                    try:
                        await send(
                            {
                                "event": "playAudio",
                                "streamId": st["sid"],
                                "media": {
                                    "contentType": st["enc"],
                                    "sampleRate": st["rate"],
                                    "payload": md.get("payload", ""),
                                },
                            }
                        )
                    except Exception:
                        pass
            elif ev == "playedStream":
                st["played"][m.get("name")] = time.time()
                rec("rx_playedStream", scn=scn, msg={k: v for k, v in m.items()})
            elif ev == "clearedAudio":
                st["cleared"] += 1
                rec("rx_clearedAudio", scn=scn, msg=m)
            elif ev == "dtmf":
                st["dtmf"].append(m.get("dtmf", {}).get("digit"))
                rec("rx_dtmf", scn=scn, msg=m)
            else:
                rec("rx_other", scn=scn, msg=m)
    except Exception as e:
        rec(
            "ws_end",
            scn=scn,
            why=type(e).__name__,
            code=getattr(e, "code", None),
            detail=str(e)[:120],
        )
    finally:
        if driver:
            driver.cancel()
        mono = all(b > a for a, b in zip(st["seq"], st["seq"][1:]))
        results.append(
            (
                "5.3 sequenceNumber strictly rising",
                mono and len(st["seq"]) > 0,
                f"{len(st['seq'])} frames",
            )
        )
        results.append(
            ("5.3 streamId on every frame", not st["bad"], str(st["bad"][:3]))
        )
        rec(
            "summary",
            scn=scn,
            mode=mode,
            media=st["media"],
            dtmf=st["dtmf"],
            cleared=st["cleared"],
            played=list(st["played"]),
            results=results,
        )


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="127.0.0.1",
        port=int(os.environ.get("TBOT_PORT", "7870")),
        log_level="warning",
        ws_ping_interval=None,
        ws_ping_timeout=None,
    )
