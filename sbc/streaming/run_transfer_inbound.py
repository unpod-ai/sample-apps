"""Blind transfer on an inbound call with a Stream bot: phone A calls the DID, the bot streams, then transfer to B.

Points the DID's answer URL at bot.py (through a tunnel), waits for your call, transfers the caller
to B once the stream is up, watches until the call ends, then restores the DID's inbound config.
Docs: https://docs.unpod.ai/telephony/apis/call-control#transfer
Run:  python -u run_transfer_inbound.py NAME --did +91DID --also +91B [--leg caller] [--answer-q 'ct=mulaw&cb=1&wait=60']
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.parse as up
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from unpod import AsyncClient
from unpod.telephony.calls import InboundConfig

from run_case import free_port, open_tunnel, read_events, wait_public

T0 = time.time()
FIELDS = list(InboundConfig.model_fields)


def say(m):
    print(f"{time.strftime('%H:%M:%S')} (+{time.time() - T0:5.1f}s)  {m}", flush=True)


def form(e: dict) -> dict:
    return dict(up.parse_qsl(e.get("body") or e.get("form") or ""))


async def restore(inbound, did: str, saved: dict, ours: str) -> dict:
    """Put the saved config back. A saved {} means "no override", so null every field."""
    path = f"/sbc/v1/dids/{did}/inbound"
    body = {k: saved.get(k) for k in FIELDS}
    await inbound._http.put(path, json=body)
    now = (await inbound.get_did(did)).model_dump(exclude_none=True)
    if now.get("answer_url") == ours:
        say("restore: PUT with nulls left our answer_url; trying DELETE")
        with contextlib.suppress(Exception):
            await inbound._http.delete(path)
        now = (await inbound.get_did(did)).model_dump(exclude_none=True)
    return now


async def main(a):
    out_dir = Path("/tmp/streaming-e2e") / a.name
    shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True)
    events_path = out_dir / "events.jsonl"
    if os.environ.get("STREAM_TEST_QA_HOST", "qa.") not in (
        os.getenv("UNPOD_BASE_URL") or ""
    ):
        sys.exit("refusing: not QA")
    secret = os.getenv("UNPOD_WEBHOOK_SECRET") or ""
    steps = []
    async with AsyncClient() as client:
        calls, inbound = client.telephony.calls, client.telephony.inbound
        saved = (await inbound.get_did(a.did)).model_dump(exclude_none=True)
        (out_dir / "did-backup.json").write_text(json.dumps(saved, indent=2))
        say(f"DID {a.did} config now: {saved or '{} (static routing)'}")
        port = free_port()
        tunnel, url = await open_tunnel(port)
        env = {
            **os.environ,
            "BOT_SECRET": secret,
            "PUBLIC_BASE": url.replace("https://", "wss://"),
            "PUBLIC_HTTP_BASE": url,
            "EVENTS": str(events_path),
        }
        bot = subprocess.Popen(
            [sys.executable, "-I", "-m", "uvicorn", "bot:app", "--app-dir", str(HERE),
             "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
            env=env, stdout=open(out_dir / "bot.log", "w"), stderr=subprocess.STDOUT,
        )  # fmt: skip
        ours = f"{url}/answer?{a.answer_q}"
        cid = None
        try:
            await wait_public(url)
            await inbound.set_did(
                a.did,
                InboundConfig(
                    answer_url=ours,
                    answer_method="POST",
                    status_callback_url=f"{url}/cb/call",
                    status_callback_format="form",
                ),
            )
            say(f">>> CALL {a.did} FROM PHONE A NOW (waiting {a.ring_wait} s)")
            end = time.monotonic() + a.ring_wait
            start = None
            while time.monotonic() < end and not start:
                start = next(
                    (e for e in read_events(events_path)
                     if e["kind"] == "answer" and form(e).get("Event") == "StartApp"),
                    None,
                )  # fmt: skip
                await asyncio.sleep(0.5)
            if not start:
                say("!! no call reached the answer URL")
                return 1
            f = form(start)
            cid = f.get("MPCUUID") or f.get("CallUUID", "").rsplit("-", 1)[0]
            say(
                f"call {cid} from {f.get('From')} (CallUUID {f.get('CallUUID')}). Waiting for the stream."
            )
            end = time.monotonic() + 15
            while time.monotonic() < end and not any(
                e["kind"] == "ws_start" for e in read_events(events_path)
            ):
                await asyncio.sleep(0.5)
            await asyncio.sleep(3)
            say(
                f">>> transfer(call, {a.also}, type=blind, leg={a.leg}): PHONE B SHOULD RING"
            )
            try:
                await calls.transfer(cid, a.also, type="blind", leg=a.leg)
                steps.append({"step": f"transfer leg={a.leg}", "ok": True})
                say("[ok] transfer")
            except Exception as e:
                steps.append(
                    {
                        "step": f"transfer leg={a.leg}",
                        "ok": False,
                        "error": str(e)[:200],
                    }
                )
                say(f"[err] transfer: {getattr(e, 'status_code', None)} {str(e)[:160]}")
            say(f"watching up to {a.max_wait} s; hang up either phone")
            end = time.monotonic() + a.max_wait
            while time.monotonic() < end and not any(
                form(e).get("Event") == "MPCEnd"
                for e in read_events(events_path)
                if e["kind"] == "callback"
            ):
                await asyncio.sleep(1)
        finally:
            if cid:
                with contextlib.suppress(Exception):
                    await calls.hangup(cid)
            await asyncio.sleep(4)
            try:
                now = await restore(inbound, a.did, saved, ours)
                say(f"restore: DID config now {now or '{} (static routing)'}")
            except Exception as e:
                say(
                    f"!! restore failed ({str(e)[:120]}); backup in {out_dir}/did-backup.json"
                )
            bot.terminate()
            tunnel.terminate()
            with contextlib.suppress(Exception):
                bot.wait(5)
    (out_dir / "summary.json").write_text(
        json.dumps({"call_id": cid, "steps": steps}, indent=2)
    )
    say(f"files: {out_dir}/")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("name")
    p.add_argument("--did", required=True, help="org number phone A calls (E.164)")
    p.add_argument("--also", required=True, help="transfer target B (E.164)")
    p.add_argument("--leg", default="caller", choices=["caller", "callee"])
    p.add_argument(
        "--answer-q", default="ct=mulaw&cb=1&wait=60", help="bot.py /answer query"
    )
    p.add_argument(
        "--ring-wait", type=int, default=120, help="seconds to wait for your call"
    )
    p.add_argument(
        "--max-wait", type=int, default=120, help="seconds to watch after the transfer"
    )
    sys.exit(asyncio.run(main(p.parse_args())))
