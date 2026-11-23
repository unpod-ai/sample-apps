"""Quickstart "Receive your first call", step 2: run a server.

Docs: https://docs.unpod.ai/telephony/apis/quickstart
Run:  uv run quickstart/step7_app.py          # listens on 127.0.0.1:8000

The docs' app.py: POST /answer verifies the signature with parse_call_event,
answers StartApp with <Speak> + <Hangup/>, and 200s every other event. 401 on a
bad or missing signature. Every request is printed so you can see step 10 arrive.
"""

import os

import _common  # noqa: F401  (loads .env)
from fastapi import FastAPI, HTTPException, Request, Response
from unpod.telephony import WebhookSignatureError, parse_call_event

SECRET = os.environ.get("UNPOD_WEBHOOK_SECRET")
GREETING = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    "<Response><Speak>Hello! Thanks for calling.</Speak><Hangup/></Response>"
)

app = FastAPI()


@app.post("/answer")
async def answer(request: Request) -> Response:
    body = await request.body()
    try:
        event = parse_call_event(body, request.headers, secret=SECRET)
    except WebhookSignatureError as exc:
        print(f"REJECTED (401): {exc}")
        raise HTTPException(status_code=401) from exc
    print(f"PASS  {event.event}  call={event.call_id}  from={event.from_}  status={event.call_status}")
    if event.event == "StartApp":
        return Response(GREETING, media_type="application/xml")
    return Response()


if __name__ == "__main__":
    import uvicorn

    if not SECRET:
        raise SystemExit("FAIL  UNPOD_WEBHOOK_SECRET is not set -> run step6_webhook_secret.py --rotate")
    uvicorn.run(app, host="127.0.0.1", port=8000)
