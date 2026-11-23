"""Quickstart "Receive your first call", step 4: point your number at it.

Docs: https://docs.unpod.ai/telephony/apis/quickstart
Run:  uv run quickstart/step9_point_number.py <DID>                       # show config only
      uv run quickstart/step9_point_number.py <DID> https://<tunnel>/answer  # set answer_url
      uv run quickstart/step9_point_number.py <DID> --clear                 # back to no override

set_did replaces the DID's whole inbound config. The config before the change is
saved to quickstart/did-backup-<DID>.json (first time only).
"""

import json
import sys

from _common import HERE, fail, info, is_e164, need_env, ok

need_env()
from unpod import Client
from unpod.telephony import InboundConfig, TelephonyAPIError

args = sys.argv[1:]
if not args or not is_e164(args[0]):
    fail("usage: step9_point_number.py <DID> [https://<tunnel>/answer | --clear]")
did, target = args[0], (args[1] if len(args) > 1 else None)

with Client() as client:
    inbound = client.telephony.inbound
    try:
        before = inbound.get_did(did).model_dump(exclude_none=True)
    except TelephonyAPIError as exc:
        fail(f"get_did {did}: {exc}",
             "404 = the SBC does not know this number yet (just attached? wait a minute) or it is not yours; "
             "403 call_api_disabled = Call API not enabled")
    ok(f"get_did {did}: {before or '{} (no override: calls go to the attached agent / static routing)'}")
    if target is None:
        sys.exit(0)

    backup = HERE / f"did-backup-{did.lstrip('+')}.json"
    if not backup.exists():
        backup.write_text(json.dumps(before, indent=2))
        info(f"saved current config -> {backup.name}")

    if target == "--clear":
        cfg = InboundConfig()
    elif target.startswith("https://"):
        cfg = InboundConfig(answer_url=target, answer_method="POST", status_callback_url=target)
    else:
        fail("answer URL must be public https (the URL step 8 printed)")
    try:
        inbound.set_did(did, cfg)
    except TelephonyAPIError as exc:
        fail(f"set_did: {exc}")
    after = inbound.get_did(did).model_dump(exclude_none=True)
    if target != "--clear" and after.get("answer_url") != target:
        fail(f"get_did after set: {after}", "answer_url did not stick")
    ok(f"set_did done, get_did now: {after or '{}'}")
    if target != "--clear":
        info(f"step 10: call {did} from any phone; step 7's terminal prints StartApp then Hangup")
        info(f"afterwards: uv run quickstart/step9_point_number.py {did} --clear")
