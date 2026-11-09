# Unpod sample apps

Runnable examples for [Unpod AI](https://docs.unpod.ai), the platform for phone
calls, voice agents, and autonomous agents. Each folder is one module and maps to
a section of the docs. Each app in a module shows one docs page and links to it.

| Module | What it covers | Status |
|---|---|---|
| [`telephony/`](telephony/) | Place, receive, and control calls; answer URLs; SIP trunking | 2 apps |
| [`sbc/`](sbc/) | End-to-end outbound and inbound call tests for the telephony call API (SBC) | 2 scripts + offline tests |
| [`voice-agents/`](voice-agents/) | Agents that talk on calls: prompt, playbook, or your own brain | Planned |
| [`autonomous-agents/`](autonomous-agents/) | Flows and journeys that call, message, wait, and branch per contact | Planned |

## Prerequisites

- Python 3.12+ and [uv](https://docs.astral.sh/uv/)
- An Unpod account, with:
  - a personal API key from [API keys](https://unpod.ai/settings/security/api-keys/)
  - your org handle, which is the Domain on
    [workspace settings](https://unpod.ai/settings/workspace/general/) without the `@`

## Run a sample

Each module is its own uv project. Run commands from inside the module folder:

```bash
cd telephony
uv sync
cat > .env <<'EOF'
UNPOD_BASE_URL=https://unpod.ai
UNPOD_PLATFORM_TOKEN=<your API key>
UNPOD_ORG_HANDLE=<your org handle>
EOF
uv run outbound_call.py          # lists your numbers
```

The module's README lists its apps and any extra settings they need.

## Links

- Docs: https://docs.unpod.ai (full page index: https://docs.unpod.ai/llms.txt)
- Python SDK: https://github.com/unpod-ai/unpod-python-sdk
- Platform source: https://github.com/unpod-ai/unpod
