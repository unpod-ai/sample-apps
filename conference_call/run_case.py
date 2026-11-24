"""Run one live streaming case: tunnel + bot.py + a real outbound call.

Docs: https://docs.unpod.ai/telephony/apis/audio-streaming
Run:  uv run run_case.py NAME [QUERY] --to +91XXXXXXXXXX [--from +91...] [--timeout 90]

NAME     case id; results go to logs/<NAME>/ (events.jsonl, bot.log, summary.json)
QUERY    /answer options for bot.py, e.g. 'ct=mulaw&cb=1' (see bot.py's docstring)
         handoff: 'mode=close&close_after=15&dial=+91HUMAN' echoes 15 s, then
         Unpod dials the human
--from   caller id (else UNPOD_FROM_NUMBER); attached with attach_type=human_call

Targets UNPOD_BASE_URL from .env (prod: https://unpod.ai). Places ONE call, always
hangs it up at the end, then stops bot + tunnel. Needs cloudflared on PATH. The
webhook secret is UNPOD_WEBHOOK_SECRET, else read in-process; never printed.
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
from typing import Any

import httpx
from unpod import AsyncClient
from unpod.telephony import TelephonyAPIError

HERE = Path(__file__).resolve().parent
LOGS = HERE / "logs"
_BACKGROUND: set[asyncio.Task] = set()


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def caller_id(from_: str | None) -> str:
    """--from, else UNPOD_FROM_NUMBER; there is no default number."""
    number = from_ or os.getenv("UNPOD_FROM_NUMBER")
    if not number:
        sys.exit("pass --from +E164 or set UNPOD_FROM_NUMBER (a number your org owns)")
    return number


def case_dir(name: str) -> Path:
    """A fresh logs/<name>/ directory for one run."""
    out = LOGS / name
    shutil.rmtree(out, ignore_errors=True)
    out.mkdir(parents=True)
    return out


async def webhook_secret(client: AsyncClient) -> str:
    """UNPOD_WEBHOOK_SECRET, else the org's secret read in-process (never printed).

    Empty when neither works (reveal needs an org admin): signatures go unchecked.
    """
    if secret := os.getenv("UNPOD_WEBHOOK_SECRET"):
        return secret
    with contextlib.suppress(TelephonyAPIError):
        return (await client.telephony.webhook_secret.reveal()).secret or ""
    print("warning: no webhook secret; signatures will not be verified")
    return ""


async def open_tunnel(port: int) -> tuple[asyncio.subprocess.Process, str]:
    """A cloudflared quick tunnel to 127.0.0.1:port and its https URL."""
    if not shutil.which("cloudflared"):
        sys.exit("cloudflared not found (brew install cloudflared)")
    proc = await asyncio.create_subprocess_exec(
        "cloudflared",
        "tunnel",
        "--no-autoupdate",
        "--url",
        f"http://127.0.0.1:{port}",
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    assert proc.stderr is not None
    stderr = proc.stderr
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        line = await asyncio.wait_for(stderr.readline(), 40)
        if m := re.search(rb"https://[a-z0-9-]+\.trycloudflare\.com", line):

            async def drain() -> None:  # a full pipe would block cloudflared
                while await stderr.readline():
                    pass

            task = asyncio.create_task(drain())
            _BACKGROUND.add(task)
            task.add_done_callback(_BACKGROUND.discard)
            return proc, m.group(0).decode()
    proc.terminate()
    sys.exit("cloudflared gave no URL in 40s")


async def wait_public(url: str, secs: int = 90) -> None:
    """Wait until url/health answers through the tunnel."""
    end = time.monotonic() + secs
    while time.monotonic() < end:
        for transport in (None, httpx.AsyncHTTPTransport(local_address="0.0.0.0")):
            with contextlib.suppress(Exception):
                async with httpx.AsyncClient(timeout=5, transport=transport) as c:
                    if (await c.get(f"{url}/health")).status_code == 200:
                        return
        # local DNS can lag on a fresh tunnel host: resolve via 1.1.1.1 and pin it
        with contextlib.suppress(Exception):
            host = url.split("//")[1]
            dig = await asyncio.create_subprocess_exec(
                "dig", "+short", host, "@1.1.1.1", stdout=asyncio.subprocess.PIPE
            )
            ips, _ = await asyncio.wait_for(dig.communicate(), 5)
            if ip := next(iter(ips.decode().split()), ""):
                async with httpx.AsyncClient(timeout=5) as c:
                    r = await c.get(
                        f"https://{ip}/health",
                        headers={"Host": host},
                        extensions={"sni_hostname": host},
                    )
                    if r.status_code == 200:
                        return
        await asyncio.sleep(3)
    sys.exit(f"{url} not reachable after {secs}s")


def start_bot(port: int, url: str, out: Path, secret: str) -> subprocess.Popen:
    """bot.py under uvicorn on 127.0.0.1:port, logging to out/."""
    env = {
        **os.environ,
        "BOT_SECRET": secret,
        "PUBLIC_BASE": url.replace("https://", "wss://"),
        "PUBLIC_HTTP_BASE": url,
        "EVENTS": str(out / "events.jsonl"),
    }
    cmd = [
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
    ]
    log = open(out / "bot.log", "w")  # noqa: SIM115 - lives as long as the bot
    return subprocess.Popen(cmd, env=env, stdout=log, stderr=subprocess.STDOUT)


def read_events(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


async def main(a: argparse.Namespace) -> int:
    out = case_dir(a.name)
    events_path = out / "events.jsonl"
    from_ = caller_id(a.from_)
    print(
        f"{os.getenv('UNPOD_BASE_URL')}  {from_} -> {a.to}  case={a.name} query={a.query!r}"
    )
    async with AsyncClient() as client:
        secret = await webhook_secret(client)
        await client.telephony.numbers.attach(from_, attach_type="human_call")
        port = free_port()
        tunnel, url = await open_tunnel(port)
        bot = start_bot(port, url, out, secret)
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
            await wait_settled(events_path, a.timeout, a.settle)
        finally:
            if call_id:
                with contextlib.suppress(Exception):
                    await client.telephony.calls.hangup(call_id)
            bot.terminate()
            tunnel.terminate()
            with contextlib.suppress(Exception):
                bot.wait(5)
    summ = summarize(a.name, call_id, a.query, secret, read_events(events_path))
    (out / "summary.json").write_text(json.dumps(summ, indent=2, default=str))
    print(json.dumps(summ, indent=2, default=str))
    print(f"\nfiles: {out}/ (events.jsonl, bot.log, summary.json)")
    return 0


async def wait_settled(events_path: Path, timeout: int, settle: int) -> None:
    """Until the stream or call ends (plus `settle` s for late callbacks), or timeout."""
    end = time.monotonic() + timeout
    settled = None
    while time.monotonic() < end:
        await asyncio.sleep(1)
        ev = read_events(events_path)
        done_ws = any(e["kind"] == "ws_end" for e in ev)
        call_done = any(
            e["kind"] == "callback"
            and e.get("name") == "call"
            and "completed" in json.dumps(e.get("event", e.get("body", ""))).lower()
            for e in ev
        )
        if (done_ws or call_done) and settled is None:
            settled = time.monotonic()
        if settled and time.monotonic() - settled > settle:
            return


def summarize(
    name: str, call_id: str | None, query: str, secret: str, ev: list[dict]
) -> dict[str, Any]:
    """What the bot saw, condensed from events.jsonl."""
    counts: dict[str, int] = {}
    for e in ev:
        counts[e["kind"]] = counts.get(e["kind"], 0) + 1
    up = next((e for e in ev if e["kind"] == "ws_upgrade"), {})
    end = next((e for e in ev if e["kind"] == "ws_end"), {})
    keep = ("event", "stream_reason", "stream_id", "service_url", "error", "duration")
    keep += ("event_time", "extra_headers", "status", "call_status")
    return {
        "case": name,
        "call_id": call_id,
        "query": query,
        "signature_checked": bool(secret),
        "events": counts,
        "upgrade": {
            k: up.get(k) for k in ("has_sig", "has_plivo_v3", "sig_shape", "mode")
        },
        "signature_verified": next(
            (e.get("verified") for e in ev if e["kind"] == "ws_signature"), None
        ),
        "start": next(
            (
                {
                    k: e.get(k)
                    for k in ("call_id", "stream_id", "fmt", "extra_headers", "account")
                }
                for e in ev
                if e["kind"] == "ws_start"
            ),
            None,
        ),
        "ws_end": {
            k: v
            for k, v in end.items()
            if k in ("after", "media_in", "play_out") or k.startswith("trk_")
        },
        "ws_close_code": next(
            (e.get("code") for e in ev if e["kind"] == "ws_disconnect"), None
        ),
        "callbacks": [
            {
                "name": e["name"],
                "method": e["method"],
                "verified": e.get("verified"),
                "get_sig_matches": e.get("get_sig_matches"),
                "query": e.get("query") if e["method"] == "GET" else None,
                "event": {k: v for k, v in (e.get("event") or {}).items() if k in keep},
                "body": e.get("body", "")[:1500],
            }
            for e in ev
            if e["kind"] == "callback"
        ],
        "answer": [
            {"method": e["method"], "signed": e["signed"], "verified": e["verified"]}
            for e in ev
            if e["kind"] == "answer"
        ],
        "answer_xml": next((e["xml"] for e in ev if e["kind"] == "answer_xml"), None),
    }


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("name")
    p.add_argument("query", nargs="?", default="")
    p.add_argument("--to", required=True)
    p.add_argument("--from", dest="from_")
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
