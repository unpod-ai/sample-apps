# SBC streaming tests

QA test drivers for call-audio streaming: the `<Stream>` answer verb and the REST stream API
(`client.telephony.calls.start_stream` and friends), where the SBC opens a signed WebSocket to your bot and
speaks the Unpod stream protocol (`start`, `media`, `dtmf`, `playedStream`, `clearedAudio` to the bot;
`playAudio`, `checkpoint`, `clearAudio`, `sendDTMF` from the bot). They cover the streaming test checklist
(enablement, `<Stream>` attributes, the `X-Unpod-Signature` upgrade signature, frames both ways, every stop and
fail reason, status callbacks, the REST stream API, call control during a stream, conference, and the SDK's
Pipecat serializer).

Docs: [Telephony APIs](https://docs.unpod.ai/telephony/introduction)

Most scripts place **real calls on a QA org**. They refuse to run unless `UNPOD_BASE_URL` contains the QA host
marker (`STREAM_TEST_QA_HOST`, default `qa.`). The `check_*.py` and `offline_*.py` scripts make no calls and need
no credentials.

## Requirements

These scripts need the streaming SDK, which is newer than the `unpod` release pinned in this module's `uv.lock`
(the lockfile is left alone). Use a separate virtualenv:

```bash
python3.12 -m venv .venv-streaming && . .venv-streaming/bin/activate
pip install "git+https://github.com/unpod-ai/unpod-python-sdk@feat/pipecat-audio-streaming" \
    "pipecat-ai>=1.0" fastapi uvicorn httpx python-dotenv websockets
```

Live calls also need [`cloudflared`](https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/)
(each driver opens a quick tunnel so the SBC can reach the local bot) and `dig`. Put the settings in `sbc/.env`
(shared with `call_test.py`):

```bash
UNPOD_BASE_URL=https://qa.example.com   # a QA host
UNPOD_PLATFORM_TOKEN=...
UNPOD_ORG_HANDLE=...
SBC_FROM_NUMBER=+91...                  # an org number used as caller ID
UNPOD_WEBHOOK_SECRET=...                # org webhook secret: lets the bot verify signatures
# optional: STREAM_TEST_QA_HOST=qa.     # substring the base URL must contain
```

The `tbot/` scripts also read `STREAM_TEST_TO` (the phone to ring). Use your own numbers: never commit them.

## Run

All commands run from this folder (`cd sbc/streaming`) with the virtualenv active. Results go to
`/tmp/streaming-e2e/<NAME>/` (`events.jsonl`, `summary.json`, `bot.log`).

### Offline (no call, no credentials)

```bash
python check_signatures.py            # X-Unpod-Signature rules: HMAC, tamper, 300 s age, replay, Host spoofing, rotation; parse_call_event
python check_pipecat_offline.py       # real Pipecat bot against a stand-in SBC: signature, audio round trip, DTMF, barge-in, hangup
python offline_local_stream_checks.py # SDK rules: formats, attribute validation, message helpers, serializer (71 checks)
python offline_parse84.py             # parse_call_event over captured stream callbacks
python check_rest_validation.py       # needs .env: unknown call id and invalid service_url against QA, no call
python fake_stream_sbc.py --bot ws://127.0.0.1:28090 --public wss://bot.example.com --secret test-secret   # stand-in SBC for developing the bot without QA
```

### One real call per case

```bash
python -u run_case.py NAME 'ct=mulaw&cb=1&close_after=6' --to +91XXXXXXXXXX [--answer-method GET] [--timeout 70]
python summarize_case.py NAME         # short summary of /tmp/streaming-e2e/NAME/summary.json
```

`run_case.py` starts `bot.py` plus a tunnel, places one call, verifies the upgrade signature and every callback
against the org secret, hangs up and cleans up. The query selects the `<Stream>` options: `ct=mulaw|alaw|l16_8|l16_16|l16_24`,
`cb=1` and `cbm=GET`, `at=inbound|outbound|both`, `bidi=0|1`, `to=N` (streamTimeout), `ext=a=1,b=2`
(extraHeaders), `nc=60..100`, `kca=0|1`, `wait=N`, `close_after=N`, `fmt=json`, `plain=1`, `rec=1`, and
`mode=echo|close|checkpoint|clear|dtmf|wrongfmt|bigmsg|drop|noread|reject|badstart` for the bot's behaviour
(full list at the top of `bot.py`).

| Checklist area | Command |
|---|---|
| μ-law / A-law / L16 echo (formats) | `run_case.py NAME 'ct=mulaw&cb=1&close_after=15'`, `ct=alaw`, `ct=l16_8`, `ct=l16_16`, `ct=l16_24` |
| Default format, both tracks, not bidirectional | `run_case.py NAME 'at=both&bidi=0&cb=1&close_after=12'` |
| Outbound track, `streamTimeout` | `run_case.py NAME 'at=outbound&bidi=0&to=10&cb=1&ct=mulaw'` |
| Bot closes the stream | `run_case.py NAME 'ct=mulaw&cb=1&close_after=5'` |
| `keepCallAlive=false` with a `<Wait>` | `run_case.py NAME 'ct=mulaw&cb=1&kca=0&wait=10'` |
| `extraHeaders` and GET callbacks | `run_case.py NAME 'ct=mulaw&cb=1&cbm=GET&ext=a=1,b-2=x_y&close_after=6'` |
| No `statusCallbackUrl` | `run_case.py NAME 'ct=mulaw&close_after=6'` |
| Noise cancellation levels | `run_case.py NAME 'ct=mulaw&cb=1&nc=60&close_after=6'` (and `nc=100`) |
| JSON answer fetched with GET | `run_case.py NAME 'ct=mulaw&cb=1&fmt=json&close_after=8' --answer-method GET` |
| Caller hangs up mid-stream | `run_case.py NAME 'ct=mulaw&cb=1'`, hang up yourself |
| Frames: checkpoint, wrong format, barge-in, DTMF | `run_case.py NAME 'mode=checkpoint'`, `mode=wrongfmt`, `mode=clear`, `mode=dtmf` |
| Failures: refused upgrade, dropped connection, oversized frame | `run_case.py NAME 'mode=reject'`, `mode=drop`, `mode=bigmsg` |

### Other drivers (usage in the first lines of each file)

```bash
python -u run_rest.py NAME --to +91XXXXXXXXXX          # REST stream API: start, list, get, stop one, stop all
python -u run_ctrl.py NAME --to +91XXXXXXXXXX          # hold, mute, play_audio mix, send_dtmf, hangup during a stream
python -u run_mix.py NAME --to +91XXXXXXXXXX           # play_audio mix over the bot's audio
python -u run_conf.py NAME --to +91A --also +91B [--limit 60]       # conference with time_limit
python -u run_conf_stream_mix.py NAME --to +91A --also +91B         # conference, stream on the bridged call, mix beep
python -u run_flagoff.py NAME --to +91XXXXXXXXXX       # org flag off (untick it in the QA admin first, re-tick after)
python -u run_startfail.py NAME --to +91XXXXXXXXXX     # refused REST start and its callback
python -u run_prestart.py NAME --to +91XXXXXXXXXX      # REST start before the call is answered
python -u run_pipecat_call.py +91XXXXXXXXXX            # Pipecat echo bot through UnpodFrameSerializer; speak, press 5 * #
python -u run_example.py NAME --to +91XXXXXXXXXX       # examples/pipecat_streaming as shipped
python -u run_dtmf.py NAME --to +91XXXXXXXXXX          # send_dtmf variants (also run_dtmf2.py, run_dtmf3.py)
python -u run_transfer2.py NAME --to +91A --also +91B [--answer-q 'ct=mulaw&cb=1']   # blind transfer, without or with a stream
python -u run_transfer_inbound.py NAME --did +91DID --also +91B          # blind transfer of an inbound call with a Stream bot; restores the DID config
python -u run_multi.py NAME 'ct=l16_16&cb=1' --to +91A --to +91B     # concurrent calls
python -u run_lat2.py NAME --a +91A --b +91B            # echo latency between two phones
```

### `tbot/`: a scripted bot for chained and combined cases

```bash
export STREAM_TEST_TO=+91XXXXXXXXXX SBC_FROM_NUMBER=+91...
cd tbot
UNPOD_WEBHOOK_SECRET=... TBOT_OUT=. python tbot.py &                 # the bot, port 7870
cloudflared tunnel --no-autoupdate --url http://127.0.0.1:7870       # note the https URL
python place.py https://<tunnel> 'scn=x&mode=suite&sdk=1'           # checkpoint, wrong format, DTMF, barge-in, built with the SDK helpers
python place.py https://<tunnel> 'scn=x&chain=conn,auth,drop,big'   # four failing streams in one call
python place.py https://<tunnel> 'scn=x&bad=bidi_both,noise59,timeout0,http,empty,long&after=6'   # invalid <Stream> verbs
```

`rest_stream_api.py`, `rest_call_control.py` and `mix_recording.py` take a file containing the tunnel URL.

## Notes

- A failed `<Stream>` (unreachable URL, refused upgrade, dropped connection, oversized frame) lets the call move on
  to the next verb. An answer with an **invalid** `<Stream>` attribute is rejected as a whole.
- Tunnels (cloudflared quick tunnels) drop idle connections at about 40 to 60 s, so the slow-reader case
  (`slow_connection_*`) cannot be reached through one.
- Never commit `.env`, tunnel URLs, call ids or phone numbers.
