# SBC call tests

End-to-end test scripts for the Unpod telephony call API (the SuperSBC behind
`client.telephony`). They place a **real outbound call** or take a **real inbound
call**, verify every signed webhook the SBC sends with `parse_call_event`, and print
what happened. The offline tests run the same scripts against an in-memory fake
SBC, so no call is made.

Docs: [Telephony APIs](https://docs.unpod.ai/telephony/introduction)

| File | What it does |
|---|---|
| `call_test.py` | Outbound: dial a number, then optionally add a second participant, play hold music until they pick up, play audio, and download the recording |
| `inbound_test.py` | Inbound: point a DID's answer URL at this machine, call it from a phone, answer with `<Speak>`, `<Dial>` or static routing, then restore the DID |
| `webhook_receiver.py` | Local receiver used by both scripts: verifies signatures, serves the answer XML and a test tone |
| `fake_sbc.py` | In-memory stand-in for `client.telephony` and the SBC, used by the tests |
| `test_call_test.py`, `test_inbound_test.py` | Offline tests: outbound flow, add participant, wait tune, participant limit, inbound speak/dial/empty, DID restore, unsigned request rejected |

## Setup

With uv:

```bash
cd sbc
uv sync
```

Or with pip:

```bash
cd sbc
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
```

Create `sbc/.env`:

```bash
UNPOD_BASE_URL=https://unpod.ai      # or your QA host
UNPOD_PLATFORM_TOKEN=...             # https://unpod.ai/settings/security/api-keys/
UNPOD_ORG_HANDLE=...                 # workspace Domain without the "@"
SBC_FROM_NUMBER=+91...               # optional default caller ID: one of the org's numbers
```

The org needs the call API switched on, and the numbers you use must be the org's.
Webhooks need a public https URL that reaches the script's port, for example:

```bash
cloudflared tunnel --protocol http2 --url http://localhost:8098   # call_test.py
cloudflared tunnel --protocol http2 --url http://localhost:8099   # inbound_test.py
```

The tunnel's port must match `--port`. If the URL returns 530 or 502, the tunnel
is down or on the wrong port and no webhook will arrive.

## Outbound call

Places a real call. Only dial numbers whose owners agreed to the test.

```bash
# dial one phone, print every webhook
uv run call_test.py --to +91XXXXXXXXXX --public-url https://<tunnel> --rotate

# two people: leg 1 hears hold music until the customer picks up
uv run call_test.py --to +91XXXXXXXXXX --customer +91YYYYYYYYYY \
  --public-url https://<tunnel> --rotate --wait-tune https://<host>/hold.mp3

# check audio between the two legs, and dump every webhook as received
uv run call_test.py --to +91XXXXXXXXXX --customer +91YYYYYYYYYY \
  --public-url https://<tunnel> --rotate --bridge-test --dump webhooks.jsonl
```

`--rotate` creates a new webhook secret (org owner or editor), which invalidates the
old one. Pass `--secret` instead to keep the current one. `uv run call_test.py -h`
lists every option.

## Inbound call

Changes where calls to the DID go until the script restores it at the end.

```bash
# the SBC speaks a message and hangs up
uv run inbound_test.py --did +91XXXXXXXXXX --public-url https://<tunnel> --rotate --mode speak

# record and forward the caller to your phone
uv run inbound_test.py --did +91XXXXXXXXXX --public-url https://<tunnel> --rotate \
  --mode dial --agent +91YYYYYYYYYY
```

When the script prints `CALL NOW`, call the DID. It prints each request to
`/incoming` (`StartApp`, then `Hangup`, plus `RecordStop` in dial mode).

If `get_did` returns 404, the SBC doesn't know the DID yet: attach it to the org
first (`numbers.attach(did, attach_type="human_call")`, which can take about a
minute to reach the SBC).

## Tests

```bash
uv run pytest -q
uv run ruff check .
```

The tests never call the Unpod API or dial anything. `fake_sbc.py` behaves like the
real SBC where that matters: signed form-encoded webhooks, no `Ringing` event, at
most two participants (409 `participant_limit`), and `set_did` replacing the whole
config.
