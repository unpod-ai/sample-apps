"""Run one live streaming case on QA: tunnel + bot.py + a real outbound call.

  PY=python   # a venv with unpod>=0.7.0 (see README)
  $PY run_case.py NAME 'ct=mulaw&cb=1' --to +91XXXXXXXXXX [--secret-from env|reveal] [--timeout 90]

NAME            case id, results in /tmp/streaming-e2e/<NAME>/ (events.jsonl, summary.json)
QUERY           /answer query options (see bot.py docstring)
--secret-from   env    : UNPOD_WEBHOOK_SECRET from unpod-test/.env (default; none => signatures not verified)
                reveal : read-only GET webhook_secret.reveal() in-process, never printed
Reads unpod-test/.env (QA). Places ONE call, always hangs it up at the end, stops bot + tunnel.
No DID / secret changes. Never prints the secret.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

HERE = Path(__file__).resolve().parent
load_dotenv(HERE.parent / ".env")  # unpod-test/.env (QA)

from unpod import AsyncClient

DEFAULT_FROM = os.environ.get("SBC_FROM_NUMBER", "")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def open_tunnel(port: int):
    proc = await asyncio.create_subprocess_exec(
        "cloudflared",
        "tunnel",
        "--no-autoupdate",
        "--url",
        f"http://127.0.0.1:{port}",
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        line = await asyncio.wait_for(proc.stderr.readline(), 40)
        if m := re.search(rb"https://[a-z0-9-]+\.trycloudflare\.com", line):

            async def drain():
                while await proc.stderr.readline():
                    pass

            asyncio.create_task(drain())
            return proc, m.group(0).decode()
    proc.terminate()
    sys.exit("cloudflared gave no URL in 40s")


async def wait_public(url: str, secs: int = 90) -> None:
    import httpx

    end = time.monotonic() + secs
    while time.monotonic() < end:
        for kw in (
            {},
            {"transport": httpx.AsyncHTTPTransport(local_address="0.0.0.0")},
        ):
            with contextlib.suppress(Exception):
                async with httpx.AsyncClient(timeout=5, **kw) as c:
                    if (await c.get(f"{url}/health")).status_code == 200:
                        return
        # local DNS can lag: resolve via 1.1.1.1 and pin
        with contextlib.suppress(Exception):
            host = url.split("//")[1]
            out = subprocess.run(
                ["dig", "+short", host, "@1.1.1.1"],
                capture_output=True,
                text=True,
                timeout=5,
            ).stdout.split()
            if out:
                async with httpx.AsyncClient(timeout=5, verify=True) as c:
                    r = await c.get(
                        f"https://{out[0]}/health",
                        headers={"Host": host},
                        extensions={"sni_hostname": host},
                    )
                    if r.status_code == 200:
                        return
        await asyncio.sleep(3)
    sys.exit(f"{url} not reachable after {secs}s")


def read_events(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(l) for l in path.read_text().splitlines() if l.strip()]


async def main(a) -> int:
    out_dir = Path("/tmp/streaming-e2e") / a.name
    shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True)
    events_path = out_dir / "events.jsonl"
    from_ = a.from_ or os.getenv("UNPOD_FROM_NUMBER") or DEFAULT_FROM
    print(
        f"QA {os.getenv('UNPOD_BASE_URL')} org {os.getenv('UNPOD_ORG_HANDLE')}  {from_} -> {a.to}  case={a.name} query={a.query!r}"
    )
    if os.environ.get("STREAM_TEST_QA_HOST", "qa.") not in (
        os.getenv("UNPOD_BASE_URL") or ""
    ):
        sys.exit("refusing: UNPOD_BASE_URL is not QA")

    secret = os.getenv("UNPOD_WEBHOOK_SECRET") or ""
    async with AsyncClient() as client:
        if a.secret_from == "reveal" and not secret:
            ws = await client.telephony.webhook_secret.reveal()
            secret = ws.secret or ""
            print(
                f"secret revealed in-process (version {ws.version}, {len(secret)} chars, not printed)"
            )
        port = free_port()
        tunnel, url = await open_tunnel(port)
        wss = url.replace("https://", "wss://")
        env = {
            **os.environ,
            "BOT_SECRET": secret,
            "PUBLIC_BASE": wss,
            "PUBLIC_HTTP_BASE": url,
            "EVENTS": str(events_path),
        }
        bot = subprocess.Popen(
            [
                sys.executable,
                "-I",
                "-m",
                "uvicorn",
                "bot:app",
                "--app-dir",
                str(HERE),
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--log-level",
                "warning",
                *os.environ.get("BOT_UVICORN_EXTRA", "").split(),
            ],
            env=env,
            stdout=open(out_dir / "bot.log", "w"),
            stderr=subprocess.STDOUT,
        )
        call_id = None
        try:
            await wait_public(url)
            print(f"bot reachable at {url}  (signatures verified: {bool(secret)})")
            answer = f"{url}/answer" + (f"?{a.query}" if a.query else "")
            call = await client.telephony.calls.create(
                from_,
                a.to,
                answer_url=answer,
                answer_method=a.answer_method,
                status_callback_url=f"{url}/cb/call",
                status_callback_method="POST",
            )
            call_id = call.call_id
            print(
                f"-> call {call_id} placed ({call.state}). ANSWER YOUR PHONE. {a.hint}"
            )
            end = time.monotonic() + a.timeout
            settled = None
            while time.monotonic() < end:
                await asyncio.sleep(1)
                ev = read_events(events_path)
                kinds = [e["kind"] for e in ev]
                done_ws = "ws_end" in kinds
                call_done = any(
                    e["kind"] == "callback"
                    and e.get("name") == "call"
                    and "completed"
                    in json.dumps(e.get("event", e.get("body", ""))).lower()
                    for e in ev
                )
                if (done_ws or call_done) and settled is None:
                    settled = time.monotonic()
                if settled and time.monotonic() - settled > a.settle:
                    break
        finally:
            if call_id:
                with contextlib.suppress(Exception):
                    await client.telephony.calls.hangup(call_id)
            bot.terminate()
            tunnel.terminate()
            with contextlib.suppress(Exception):
                bot.wait(5)

    ev = read_events(events_path)
    summ = {
        "case": a.name,
        "call_id": call_id,
        "query": a.query,
        "signature_checked": bool(secret),
        "events": {},
    }
    for e in ev:
        summ["events"][e["kind"]] = summ["events"].get(e["kind"], 0) + 1
    ws_up = next((e for e in ev if e["kind"] == "ws_upgrade"), {})
    summ["upgrade"] = {
        k: ws_up.get(k) for k in ("has_sig", "has_plivo_v3", "sig_shape", "mode")
    }
    summ["signature_verified"] = next(
        (e.get("verified") for e in ev if e["kind"] == "ws_signature"), None
    )
    summ["start"] = next(
        (
            {
                k: e.get(k)
                for k in ("call_id", "stream_id", "fmt", "extra_headers", "account")
            }
            for e in ev
            if e["kind"] == "ws_start"
        ),
        None,
    )
    end_ev = next((e for e in ev if e["kind"] == "ws_end"), {})
    summ["ws_end"] = {
        k: v
        for k, v in end_ev.items()
        if k in ("after", "media_in", "play_out") or k.startswith("trk_")
    }
    summ["ws_close_code"] = next(
        (e.get("code") for e in ev if e["kind"] == "ws_disconnect"), None
    )
    summ["callbacks"] = [
        {
            "name": e["name"],
            "method": e["method"],
            "verified": e.get("verified"),
            "get_sig_matches": e.get("get_sig_matches"),
            "query": e.get("query") if e["method"] == "GET" else None,
            "event": {
                k: v
                for k, v in (e.get("event") or {}).items()
                if k
                in (
                    "event",
                    "stream_reason",
                    "stream_id",
                    "service_url",
                    "error",
                    "duration",
                    "event_time",
                    "extra_headers",
                    "status",
                    "call_status",
                )
            },
            "body": e.get("body", "")[:1500],
        }
        for e in ev
        if e["kind"] == "callback"
    ]
    summ["answer"] = [
        {"method": e["method"], "signed": e["signed"], "verified": e["verified"]}
        for e in ev
        if e["kind"] == "answer"
    ]
    summ["answer_xml"] = next((e["xml"] for e in ev if e["kind"] == "answer_xml"), None)
    (out_dir / "summary.json").write_text(json.dumps(summ, indent=2, default=str))
    print(json.dumps(summ, indent=2, default=str))
    print(f"\nfiles: {out_dir}/ (events.jsonl, bot.log, summary.json)")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("name")
    p.add_argument("query", nargs="?", default="")
    p.add_argument("--to", required=True)
    p.add_argument("--from", dest="from_")
    p.add_argument("--secret-from", choices=("env", "reveal"), default="env")
    p.add_argument("--answer-method", choices=("POST", "GET"), default="POST")
    p.add_argument("--timeout", type=int, default=90)
    p.add_argument(
        "--settle",
        type=int,
        default=6,
        help="seconds to wait for late callbacks after the stream ends",
    )
    p.add_argument("--hint", default="")
    sys.exit(asyncio.run(main(p.parse_args())))
