# Quickstart step checks

One script per step of the [Telephony APIs Quick Start](https://docs.unpod.ai/telephony/apis/quickstart),
so you can run the page step by step and see exactly which step fails. Each
script prints `PASS` lines, or one `FAIL` line with the likely cause.

Run everything from the `sbc/` folder after `uv sync` (or `pip install -r requirements.txt`).
Put `UNPOD_BASE_URL`, `UNPOD_PLATFORM_TOKEN` and `UNPOD_ORG_HANDLE` in `sbc/.env`
or `sbc/quickstart/.env`.

## Make your first call

| Docs step | Script | Changes anything? |
|---|---|---|
| 1. Credentials | `uv run quickstart/step1_credentials.py` | No |
| 2. Install | `uv run quickstart/step2_install.py` | No |
| 3. Get a number | `uv run quickstart/step3_get_number.py` (list) / `--attach [+91...]` | Only with `--attach`: takes a free pool number for the org |
| 4. Call someone and speak | `uv run quickstart/step4_call_and_speak.py <FROM> <TO>` | Places a real call |
| 5. What you get back | `uv run quickstart/step5_call_result.py <CALL_ID>` | No |

## Receive your first call

| Docs step | Script | Changes anything? |
|---|---|---|
| 1. Webhook secret | `uv run quickstart/step6_webhook_secret.py` / `--rotate` | Only with `--rotate`: new secret, old one stops working; saved to `quickstart/.env` |
| 2. Run a server | `uv run quickstart/step7_app.py` (port 8000) | No |
| 3. Expose it | `ngrok http 8000` or `cloudflared tunnel --protocol http2 --url http://localhost:8000`, then `uv run quickstart/step8_check_tunnel.py https://<tunnel>` | No |
| 4. Point your number at it | `uv run quickstart/step9_point_number.py <DID> https://<tunnel>/answer` | Yes: sets the DID's answer URL. Undo with `--clear` |
| 5. Call your number | Call the DID from a phone; step 7's terminal prints `PASS StartApp` then `PASS Hangup` | No |

## Common failures

| Step | Message | Meaning |
|---|---|---|
| 1 | `401` | Wrong token, or a key from another environment (QA key on unpod.ai) |
| 3 | `Failed to attach number.` / channel limit | The org's channels are all used: free a number or raise the plan's channel limit |
| 4 | `403 call_api_disabled` | The Call API is not enabled for the org yet |
| 4 | `403 forbidden ... not a number available` | `FROM` is `NOT_LINKED` or not ready |
| 8 | `530` / name not known | Tunnel is down or the URL is old |
| 8 | `502` | Tunnel is up but points at the wrong port |
| 9 | `404 DID not found` | The SBC does not know the number yet (wait a minute after attaching) or it is not yours |
| 10 | Nothing arrives at step 7 | Check step 8 again; the SBC falls back to the attached agent when the answer URL does not respond |

`uv run pytest -q` runs `test_step7_app.py` (the server's signature check and XML reply) with the other offline tests.
