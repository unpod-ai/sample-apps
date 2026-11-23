"""Quickstart "Make your first call", step 3: get a number.

Docs: https://docs.unpod.ai/telephony/apis/quickstart
Run:  uv run quickstart/step3_get_number.py              # show numbers, change nothing
      uv run quickstart/step3_get_number.py --attach     # attach the first free pool number (docs code)
      uv run quickstart/step3_get_number.py --attach +91XXXXXXXXXX

Without --attach it only lists. With --attach it does what the docs do:
numbers.attach(number, attach_type="human_call") and prints the per-number result.
A 400/206 "Failed to attach number." or a channel-limit message means the org has
no free channel: free one or raise the plan's channel limit.
"""

import sys

import httpx
from _common import error_text, fail, info, is_e164, need_env, ok

need_env()
from unpod import Client

args = sys.argv[1:]
attach = "--attach" in args
wanted = next((a for a in args if a.startswith("+")), None)

with Client() as client:
    tel = client.telephony
    numbers = tel.numbers.list()
    states = {o.number: o for o in tel.overview()}
    ok(f"numbers.list(): {len(numbers)} numbers")
    for n in numbers:
        o = states.get(n.number)
        info(f"{n.number}  {n.status:13} state={getattr(o, 'connection_state', None) or '-':15}"
             f" agent={getattr(o, 'agent_id', None) or '-'}")
    usable = [n.number for n in numbers if n.status == "assigned"
              and getattr(states.get(n.number), "connection_state", None) not in (None, "NOT_LINKED")]
    if usable:
        ok(f"ready to dial from (assigned, not NOT_LINKED): {', '.join(usable)}")

    if not attach:
        info("list only. Add --attach to attach a free pool number as human_call.")
        sys.exit(0)

    free = [n.number for n in numbers if n.status == "not_assigned"]
    number = wanted or (free[0] if free else None)
    if not number:
        fail("no free pool number", "add one from the dashboard (Numbers page)")
    if not is_e164(number):
        fail(f"{number} is not E.164")
    if number not in free:
        fail(f"{number} is not a free pool number",
             "the attach endpoint only takes not_assigned numbers; an assigned number can already be `from`")
    try:
        result = tel.numbers.attach(number, attach_type="human_call")
    except httpx.HTTPStatusError as exc:
        fail(f"attach {number}: {error_text(exc)}",
             "'Failed to attach number.' on QA was the org's channel limit (e.g. 5 of 5 used)")
    r = result.numbers[0]
    if not r.ok:
        fail(f"attach {number}: {r.error}")
    ok(f"attached {r.number} as human_call  connection_state={r.connection_state}")
    info("the SBC learns a new number in about a minute (step 9's get_did returns 404 until then)")
