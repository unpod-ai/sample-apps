"""Quickstart "Make your first call", step 5: what you get back.

Docs: https://docs.unpod.ai/telephony/apis/quickstart
Run:  uv run quickstart/step5_call_result.py <CALL_ID>     # after the call has ended

After hangup, calls.get returns a recording summary instead of a state.
Checks that summary and get_recording(). Read-only.
"""

import sys

from _common import fail, info, need_env, ok

need_env()
from unpod import Client
from unpod.telephony import TelephonyAPIError

if len(sys.argv) != 2:
    fail("usage: step5_call_result.py <CALL_ID>  (printed by step 4)")
cid = sys.argv[1]

with Client() as client:
    calls = client.telephony.calls
    try:
        c = calls.get(cid)
    except TelephonyAPIError as exc:
        fail(f"calls.get: {exc}")
    data = c.model_dump(exclude_none=True)
    if c.state:
        fail(f"call still has state={c.state}", "hang up first, then run this again")
    ok("calls.get after hangup: no state, recording summary instead")
    for k in ("available", "url", "duration_secs", "format", "channels"):
        info(f"{k}: {data.get(k)}")
    try:
        rec = calls.get_recording(cid)
        ok(f"get_recording: available={rec.available} url={rec.url}")
    except TelephonyAPIError as exc:
        info(f"get_recording: {exc}  (recording may still be processing; try again in a minute)")
