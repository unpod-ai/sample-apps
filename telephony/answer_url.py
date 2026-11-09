"""Answer-URL server: greet callers with Unpod XML, then hang up.

Docs: https://docs.unpod.ai/telephony/apis/answer-url
Run:  uv run uvicorn answer_url:app --port 8000
      Expose it over https (e.g. a tunnel) and set it as the number's answer URL.
"""

import os

from fastapi import FastAPI, Request, Response
from unpod.telephony import WebhookSignatureError, parse_call_event

GREETING_XML = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    "<Response><Speak>Hello from Unpod. Goodbye.</Speak><Hangup/></Response>"
)

app = FastAPI()


def reply(event: str | None) -> Response:
    """StartApp gets call instructions; Hangup/RecordStop just get a 200."""
    if event == "StartApp":
        return Response(GREETING_XML, media_type="application/xml")
    return Response()


@app.post("/answer")
async def handle_inbound(request: Request) -> Response:
    """Inbound calls: signed once the org has a webhook secret."""
    secret = os.environ.get("UNPOD_WEBHOOK_SECRET")
    try:
        ev = parse_call_event(await request.body(), request.headers, secret)
    except WebhookSignatureError:
        return Response(status_code=401)
    return reply(ev.event)


@app.post("/outbound")
async def handle_outbound(request: Request) -> Response:
    """Outbound calls (`answer_url` on calls.create): not signed yet."""
    body = await request.body()
    ev = parse_call_event(body, request.headers, None, verify=False)
    return reply(ev.event)
