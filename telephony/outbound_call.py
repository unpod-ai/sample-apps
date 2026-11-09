"""Place an outbound call and speak once the callee answers.

Docs: https://docs.unpod.ai/telephony/apis/quickstart
Run:  uv run outbound_call.py                 # list your numbers
      uv run outbound_call.py <from> <to>     # E.164, e.g. +919000000001
"""

import sys
import time

from unpod import Client

DIALING = {"initiated", "ringing"}
GREETING = "Hello from Unpod. This is a test call."


def call_and_speak(from_: str, to: str) -> None:
    """Dial `to` from `from_`, wait for an answer, then speak GREETING."""
    with Client() as client:
        tel = client.telephony
        tel.numbers.attach(from_, attach_type="human_call")
        call = tel.calls.create(from_, to, ring_timeout=30)
        # ponytail: polling; production apps take status_callback_url events
        while call.state in DIALING:
            print(f"{call.call_id}: {call.state}")
            time.sleep(1)
            call = tel.calls.get(call.call_id)
        print(f"{call.call_id}: {call.state}")
        if call.state == "talking":
            tel.calls.speak(call.call_id, GREETING)


def list_numbers() -> None:
    """Print the org's numbers; `not_assigned` ones are free to attach."""
    with Client() as client:
        for n in client.telephony.numbers.list():
            print(f"{n.number}  {n.status}")


if __name__ == "__main__":
    match sys.argv[1:]:
        case []:
            list_numbers()
        case [from_, to]:
            call_and_speak(from_, to)
        case _:
            sys.exit(__doc__)
