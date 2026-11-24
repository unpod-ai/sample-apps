# Conference call samples

End-to-end tests for live Unpod calls. They cover two people on one call
(human to human) and an AI agent that hands the caller to a person (human to
agent with handoff), both over [WebSocket audio
streaming](https://docs.unpod.ai/telephony/integrations/websockets).

## Setup

```bash
cd conference_call
uv sync
```

Create `conference_call/.env` (the SDK loads it automatically):

```bash
UNPOD_BASE_URL=https://unpod.ai
UNPOD_PLATFORM_TOKEN=...      # https://unpod.ai/settings/security/api-keys/
UNPOD_ORG_HANDLE=...          # workspace Domain without the "@"
UNPOD_WEBHOOK_SECRET=...      # webhook_secret.reveal(); the runners reveal it if unset
UNPOD_FROM_NUMBER=+91...      # an org number; attached as human_call on each run
# agent_handoff.py only
PUBLIC_HOST=abc.trycloudflare.com   # public host serving agent_handoff:app
HUMAN_NUMBER=+91...                 # who the agent hands off to
DEEPGRAM_API_KEY=...
ANTHROPIC_API_KEY=...
CARTESIA_API_KEY=...
```

Audio streaming is a preview feature that Unpod turns on per org, so ask the Unpod
team to enable it first. The runners need [`cloudflared`](https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/)
on `PATH`. Live runs place real calls, and the runners hang up every call they
place.

## Apps

| App | What it shows | Docs |
|---|---|---|
| `run_conf_multi.py` | Human to human: dial A, add B when A answers, N pairs at once | [Multiple Participants](https://docs.unpod.ai/telephony/apis/multiple-participants) |
| `agent_handoff.py` | Human to agent: Claude answers, then `<Dial>`s a person on request or `0` | [WebSockets](https://docs.unpod.ai/telephony/integrations/websockets) |
| `run_case.py` + `bot.py` | One streaming case (format, callbacks, barge-in, DTMF, handoff, latency) | [Audio Streaming](https://docs.unpod.ai/telephony/apis/audio-streaming) |

### Human to human

```bash
uv run run_conf_multi.py h2h --pair +91A,+91B          # add more --pair for load
```

A rings first. When A answers, B rings, and then the two of them can talk. If B
hangs up, the whole call ends. Results go to `logs/h2h/summary.json`, and the
exit code is 0 only when every B answered.

### Human to agent with handoff

The agent's answer XML is `<Speak>` greeting, `<Stream>` to the agent, `<Speak>`,
then `<Dial>HUMAN_NUMBER</Dial>`. When the caller asks for a person or presses
`0`, the agent closes the stream and Unpod runs the `<Dial>`. Any exit by the
agent goes to the human, so a caller is never dropped. Use this flow because
`calls.transfer` doesn't work yet.

```bash
cloudflared tunnel --url http://localhost:8000          # put its host in PUBLIC_HOST
uv run uvicorn agent_handoff:app --port 8000
uv run agent_handoff.py +91FROM +91YOU                  # answer, talk, then press 0
```

For inbound calls, point a number at the agent instead:
`client.telephony.inbound.set_did("+91...", InboundConfig(answer_url="https://<PUBLIC_HOST>/answer", answer_method="POST"))`.

### Streaming cases

```bash
uv run run_case.py echo --to +91YOU                                   # hear yourself
uv run run_case.py handoff 'mode=close&close_after=15&dial=+91HUMAN' --to +91YOU
uv run run_case.py fmt 'ct=l16_16&cb=1' --to +91YOU                   # L16 16k + stream events
```

`bot.py`'s docstring lists every `/answer` option and bot mode. Each run writes
`logs/<name>/` with `events.jsonl`, `bot.log` and `summary.json`. The `logs/`
folder is gitignored.

## Offline tests

```bash
uv run pytest -q
```

These need no account, phone or AI keys. They play Unpod's side with real
signatures and check the answer XML order, the signature gates (including
replays), that pressing `0` closes the stream without hanging up, and that
`bot.py` echoes μ-law.
