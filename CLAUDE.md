# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

Runnable sample apps for the Unpod AI voice-agent and telephony platform.
Docs: https://docs.unpod.ai (full page index: https://docs.unpod.ai/llms.txt).

Each top-level directory is a **module** that mirrors one docs section. Each app in
a module demonstrates one docs page.

| Module | Docs section | SDK surface |
|---|---|---|
| `telephony/` | https://docs.unpod.ai/telephony/introduction | `client.telephony.*`, `unpod.telephony` |
| `sbc/` | https://docs.unpod.ai/telephony/introduction (call API QA scripts) | `client.telephony.calls`, `.inbound`, `parse_call_event` |
| `voice-agents/` | https://docs.unpod.ai/speech-stack/introduction | `client.agents.*`, `AgentRunner`, `unpod.adapters` |
| `autonomous-agents/` | https://docs.unpod.ai/core-engine/flows-journeys | `unpod.flows`, `client.flows`, `client.journeys` |

`voice-agents/` and `autonomous-agents/` hold only a README of planned apps so far.
When you add a module, add a row here and in the root `README.md`.

## Layout conventions

- Every module is a standalone uv project (`pyproject.toml` + `uv.lock`), so a
  reader can copy one folder and run it. There is no root project. Run all
  commands from inside the module directory. A planned module becomes a project
  when its first app lands: `uv init --bare --python ">=3.12"`, then
  `uv add "unpod>=0.6.2"` and copy the dev group and `[tool.pyrefly]` from
  `telephony/pyproject.toml`.
- Apps are flat single files in the module (`outbound_call.py`), with tests beside
  them (`test_<app>.py`). Promote an app to a subdirectory only when it needs more
  than one file.
- Every app's module docstring has a one-line purpose, a `Docs:` link, and a
  `Run:` command. CLIs print `__doc__` as their usage text.
- The module `README.md` holds the app-to-docs-page table and the planned apps.
  Update it when you add an app.
- Tests never call the live Unpod API. They drive HTTP handlers through FastAPI's
  `TestClient`. Apps that only make API calls (e.g. `outbound_call.py`) have no
  tests because they need a real account.

## Commands (inside a module, e.g. `cd telephony`)

```bash
uv sync
uv run pytest -q
uv run pytest -q test_answer_url.py::test_bad_signature_is_rejected   # one test
uv run ruff format . && uv run ruff check .
uv run pyrefly check
uv run outbound_call.py                       # needs .env credentials
uv run uvicorn answer_url:app --port 8000
```

## Unpod SDK facts that aren't obvious

- Package `unpod` (>=0.6.2, Python 3.12+). `Client` is a blocking facade over
  `AsyncClient`. `unpod.client` calls `load_dotenv()` at import, so a `.env` in the
  module directory is picked up.
- When docs and SDK disagree, the installed source
  (`.venv/lib/python*/site-packages/unpod/`) is ground truth. Read it instead of
  guessing signatures. The sync wrappers return `Any`, so pyrefly will not catch a
  wrong attribute name such as `call.id` (it's `call.call_id`).
- Telephony is `client.telephony.{numbers,calls,inbound,trunks,webhook_secret}`
  on `/api/v2/platform/telephony/*`. It needs `UNPOD_PLATFORM_TOKEN` and
  `UNPOD_ORG_HANDLE` (Token auth plus `Org-Handle` header). An `sk_...` Bearer key
  (`UNPOD_API_KEY`) cannot reach it.
- Numbers are always strict E.164 strings, never ids. A number must be attached
  with `numbers.attach(n, attach_type="human_call")` before API call control.
- Call `state` goes `initiated` → `ringing` → `talking` → `ended`. `calls.speak`
  works only while `talking` and takes at most 1000 chars.
- Answer-URL and status callbacks use Plivo field names (`Event`,
  `MPCUUID` = call_id, `CallUUID` = leg_id) and are form-encoded by default. Parse
  them with `unpod.telephony.parse_call_event(raw_body, headers, secret)`. Pass the
  raw body, not parsed form data.
  - Inbound callbacks are signed:
    `X-Webhook-Signature: t=<ts>,v1=<hex HMAC-SHA256(secret, "<ts>.<body>")>`,
    with a 300 s tolerance. The secret comes from
    `client.telephony.webhook_secret.rotate()`, and rotating invalidates the old one.
  - Outbound answer-URL callbacks are not signed yet, so use `verify=False`. The
    outbound `CallUUID` ends in `-caller`, so match on `MPCUUID`.
- Answer-URL XML allows only `Speak`, `Play`, `Wait`, `Record`, `Dial` (with a
  `Number` child), and `Hangup`. Any other verb (`GetDigits`, `Conference`,
  `Redirect`) fails the whole reply. Limits are 64 KB, 32 verbs, and 4096 chars per
  `Speak`. The JSON form is `{"actions": [{"type": "speak", "text": "..."}]}`.
