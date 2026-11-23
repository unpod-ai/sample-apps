"""Quickstart "Make your first call", step 1: credentials.

Docs: https://docs.unpod.ai/telephony/apis/quickstart
Run:  uv run quickstart/step1_credentials.py

Checks the three env vars are set and that the token + org handle reach the
telephony API (GET /telephony/overview/). Read-only.
"""

import os

from _common import error_text, fail, info, need_env, ok

need_env()
ok(f"env set: UNPOD_BASE_URL={os.environ['UNPOD_BASE_URL']} UNPOD_ORG_HANDLE={os.environ['UNPOD_ORG_HANDLE']}")
if os.environ["UNPOD_PLATFORM_TOKEN"].startswith("sk_"):
    fail("UNPOD_PLATFORM_TOKEN is an sk_ key", "telephony needs the personal API key (Token auth), not sk_")

from unpod import Client

try:
    with Client() as client:
        overview = client.telephony.overview()
except Exception as exc:  # noqa: BLE001
    fail(f"overview request failed: {error_text(exc)}",
         "401 = wrong token or wrong environment (QA key on prod); empty = check UNPOD_ORG_HANDLE")
ok(f"token + org reach the telephony API ({len(overview)} numbers in the overview)")
for o in overview:
    info(f"{o.number}  state={o.connection_state}  agent={o.agent_id or '-'}")
if not overview:
    info("no numbers yet: that is fine, step 3 attaches one (or the SDK hid an 'Organization not found')")
