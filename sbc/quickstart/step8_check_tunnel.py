"""Quickstart "Receive your first call", step 3: expose it.

Docs: https://docs.unpod.ai/telephony/apis/quickstart
Run:  ngrok http 8000        (or: cloudflared tunnel --protocol http2 --url http://localhost:8000)
      uv run quickstart/step8_check_tunnel.py https://<your-tunnel>

Sends one UNSIGNED POST to <tunnel>/answer. A 401 proves the whole path works:
public https -> tunnel -> step 7's server -> signature check. Changes nothing.
"""

import sys

import httpx
from _common import fail, info, ok

if len(sys.argv) != 2 or not sys.argv[1].startswith("https://"):
    fail("usage: step8_check_tunnel.py https://<your-tunnel>", "Unpod only calls public https URLs")
url = sys.argv[1].rstrip("/") + "/answer"
try:
    r = httpx.post(url, content=b"Event=StartApp", timeout=15,
                   headers={"Content-Type": "application/x-www-form-urlencoded"})
except httpx.HTTPError as exc:
    fail(f"POST {url}: {exc}", "tunnel URL wrong or tunnel not running")
hints = {401: None, 530: "tunnel is down or the URL is old: restart it and copy the new URL",
         502: "tunnel is up but nothing listens on its port: start step7_app.py / match the port",
         404: "server reached but no /answer route: is step7_app.py what runs on that port?"}
if r.status_code == 401:
    ok(f"{url} -> 401: tunnel reaches the server and unsigned requests are rejected")
    info(f"use this as the answer URL in step 9: {url}")
else:
    fail(f"{url} -> {r.status_code}", hints.get(r.status_code) or "expected 401")
