# Telephony samples

Sample apps for [Unpod Telephony](https://docs.unpod.ai/telephony/introduction):
place, receive, and control calls on Unpod numbers, or bring your own SIP carrier.

## Setup

```bash
cd telephony
uv sync
```

Create `telephony/.env` (the SDK loads it automatically):

```bash
UNPOD_BASE_URL=https://unpod.ai
UNPOD_PLATFORM_TOKEN=...      # https://unpod.ai/settings/security/api-keys/
UNPOD_ORG_HANDLE=...          # workspace Domain without the "@"
UNPOD_WEBHOOK_SECRET=...      # only for answer_url.py; from webhook_secret.rotate()
```

An `sk_...` Bearer key cannot reach the telephony APIs. Use the personal API key.

## Apps

| App | What it shows | Docs |
|---|---|---|
| `outbound_call.py` | List numbers, dial out, speak when answered | [Quick Start](https://docs.unpod.ai/telephony/apis/quickstart) |
| `answer_url.py` | Answer-URL server: verify signed callbacks, reply with XML | [Answer URL & XML](https://docs.unpod.ai/telephony/apis/answer-url) |

```bash
uv run outbound_call.py                        # list numbers
uv run outbound_call.py +919000000001 +919000000002
uv run uvicorn answer_url:app --port 8000      # expose over https, set as answer URL
uv run pytest -q
```

Planned, one per docs page: multiple participants, incoming calls, call blasting,
number masking, recording, call control, webhooks, SIP trunking.
