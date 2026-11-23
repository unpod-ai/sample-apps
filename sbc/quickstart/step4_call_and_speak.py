"""Quickstart "Make your first call", step 4: call someone and speak.

Docs: https://docs.unpod.ai/telephony/apis/quickstart
Run:  uv run quickstart/step4_call_and_speak.py <FROM> <TO>   # E.164; places a REAL call

FROM is your Unpod number, TO a phone you can answer. Creates the call, polls
until it is talking, speaks a greeting, and prints the call_id for step 5.
"""

import sys
import time

from _common import fail, info, is_e164, need_env, ok

need_env()
from unpod import Client
from unpod.telephony import TelephonyAPIError

if len(sys.argv) != 3 or not all(is_e164(a) for a in sys.argv[1:]):
    fail("usage: step4_call_and_speak.py <FROM> <TO>  (both E.164, e.g. +919000000001)")
FROM, TO = sys.argv[1:]
GREETING = "Hello! This is your first call from Unpod."

with Client() as client:
    calls = client.telephony.calls
    try:
        call = calls.create(FROM, TO, ring_timeout=30)
    except TelephonyAPIError as exc:
        hints = {"call_api_disabled": "the Unpod team must enable the Call API for this org",
                 "from_not_owned": "FROM is not one of your org's numbers",
                 "forbidden": "FROM is not ready (NOT_LINKED?) - check step 3's state column"}
        fail(f"calls.create: {exc}", hints.get(exc.code or "", ""))
    ok(f"calls.create -> {call.call_id} state={call.state}")

    last, spoke, deadline = None, False, time.time() + 90
    while time.time() < deadline:
        try:
            state = calls.get(call.call_id).state
        except TelephonyAPIError as exc:
            info(f"calls.get: {exc}")
            break
        if state != last:
            info(f"state: {state}")
            last = state
        if state == "talking" and not spoke:
            try:
                p = calls.speak(call.call_id, GREETING)
                ok(f"speak -> play_id={getattr(p, 'play_id', None)}  (you should hear the greeting)")
                spoke = True
            except TelephonyAPIError as exc:
                fail(f"speak: {exc}")
        if state is None:          # ended: get returns the recording summary, no state
            break
        time.sleep(2)

    if not spoke:
        fail(f"call never reached talking (last state {last})",
             "check TO is E.164, the phone answered, FROM is not NOT_LINKED")
    ok("call placed and greeting spoken")
    print(f"\nnext: uv run quickstart/step5_call_result.py {call.call_id}   (after you hang up)")
