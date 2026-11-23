"""Quickstart "Receive your first call", step 1: get your webhook secret.

Docs: https://docs.unpod.ai/telephony/apis/quickstart
Run:  uv run quickstart/step6_webhook_secret.py           # reveal() the current secret, save it
      uv run quickstart/step6_webhook_secret.py --rotate  # only if your org has no secret yet

reveal() reads the current secret in full (org admin) and changes nothing, so
every server already verifying with it keeps working. --rotate creates a new
secret and invalidates the previous one (any server verifying with it starts
answering 401); it needs an org owner or editor. Either way the secret is
written to quickstart/.env as UNPOD_WEBHOOK_SECRET and never printed.
"""

import sys

from _common import ENV_FILE, fail, info, need_env, ok

need_env()
from unpod import Client
from unpod.telephony import TelephonyAPIError


def save(secret: str) -> None:
    lines = [ln for ln in (ENV_FILE.read_text().splitlines() if ENV_FILE.exists() else [])
             if not ln.startswith("UNPOD_WEBHOOK_SECRET=")]
    ENV_FILE.write_text("\n".join([*lines, f"UNPOD_WEBHOOK_SECRET={secret}"]) + "\n")


with Client() as client:
    ws = client.telephony.webhook_secret
    if "--rotate" in sys.argv:
        try:
            s = ws.rotate()
        except TelephonyAPIError as exc:
            fail(f"rotate: {exc}", "needs an org owner or editor; 403 call_api_disabled = Call API not enabled")
        label = "rotated"
    else:
        try:
            s = ws.reveal()
        except TelephonyAPIError as exc:
            fail(f"reveal: {exc}",
                 "403 = not an org admin (or Call API not enabled); "
                 "if your org has no secret yet, an owner or editor runs this with --rotate")
        label = "revealed"

if not s.secret:
    fail(f"{label}: no secret in the response (version={s.version})",
         "your org has no secret yet: an org owner or editor runs this with --rotate")
save(s.secret)
ok(f"{label}: version={s.version}  secret saved to {ENV_FILE} (ends ...{s.secret[-4:]})")
if label == "rotated":
    if s.pushed_to_sbc:
        ok("pushed_to_sbc=True: the SBC signs with the new secret now")
    else:
        info("pushed_to_sbc=False: Unpod retries in the background; wait ~1 minute before the test call")
