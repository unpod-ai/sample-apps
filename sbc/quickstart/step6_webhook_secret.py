"""Quickstart "Receive your first call", step 1: get your webhook secret.

Docs: https://docs.unpod.ai/telephony/apis/quickstart
Run:  uv run quickstart/step6_webhook_secret.py           # shows version only, changes nothing
      uv run quickstart/step6_webhook_secret.py --rotate  # new secret, saved to quickstart/.env

--rotate invalidates the previous secret (any server verifying with it starts
answering 401). Needs an org owner or editor. The full secret is returned only
here; it is written to quickstart/.env as UNPOD_WEBHOOK_SECRET, never printed.
"""

import os
import sys

from _common import ENV_FILE, fail, info, need_env, ok

need_env()
from unpod import Client
from unpod.telephony import TelephonyAPIError

with Client() as client:
    ws = client.telephony.webhook_secret
    try:
        cur = ws.get()
        ok(f"webhook_secret.get: version={cur.version} secret={cur.secret}")
    except TelephonyAPIError as exc:
        info(f"webhook_secret.get: {exc}")
    if "--rotate" not in sys.argv:
        have = bool(os.environ.get("UNPOD_WEBHOOK_SECRET"))
        info(f"UNPOD_WEBHOOK_SECRET in env: {'yes' if have else 'no'}. Add --rotate to create a new one.")
        sys.exit(0)
    try:
        r = ws.rotate()
    except TelephonyAPIError as exc:
        fail(f"rotate: {exc}", "needs an org owner or editor; 403 call_api_disabled = Call API not enabled")

lines = [ln for ln in (ENV_FILE.read_text().splitlines() if ENV_FILE.exists() else [])
         if not ln.startswith("UNPOD_WEBHOOK_SECRET=")]
ENV_FILE.write_text("\n".join([*lines, f"UNPOD_WEBHOOK_SECRET={r.secret}"]) + "\n")
ok(f"rotated: version={r.version}  secret saved to {ENV_FILE} (ends ...{(r.secret or '')[-4:]})")
if r.pushed_to_sbc:
    ok("pushed_to_sbc=True: the SBC signs with the new secret now")
else:
    info("pushed_to_sbc=False: Unpod retries in the background; wait ~1 minute before the test call")
