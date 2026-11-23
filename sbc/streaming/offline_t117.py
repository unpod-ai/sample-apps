import os
import secrets
import sys
import time

sys.path.insert(
    0, os.environ["PIPECAT_EXAMPLE_DIR"]
)  # unpod-python-sdk/examples/pipecat_streaming
os.environ.update(
    UNPOD_WEBHOOK_SECRET="fake-secret",
    START_API_KEY="k-test",
    PUBLIC_WS_BASE="wss://bot.example.com",
    UNPOD_BASE_URL="http://127.0.0.1:9",
    UNPOD_PLATFORM_TOKEN="fake",
    UNPOD_ORG_HANDLE="fake.org",
    UNPOD_FROM_NUMBER="+910000000000",
)
try:
    import server
except Exception as e:
    print("IMPORT FAIL:", type(e).__name__, e)
    sys.exit(2)
from fastapi.testclient import TestClient
from unpod.telephony.stream import stream_signature

R = []


def chk(n, ok, d=""):
    R.append(ok)
    print(("PASS " if ok else "FAIL "), n, d)


with TestClient(server.app, raise_server_exceptions=False) as c:
    r = c.post("/start", json={"phone_number": "+910000000001"})
    chk("11.7 /start without X-Api-Key -> 401", r.status_code == 401, r.status_code)
    r = c.post(
        "/start", json={"phone_number": "+910000000001"}, headers={"X-Api-Key": "wrong"}
    )
    chk("11.7 /start wrong key -> 401", r.status_code == 401, r.status_code)
    r = c.post(
        "/start",
        json={"phone_number": "+910000000001"},
        headers={"X-Api-Key": "k-test"},
    )
    chk(
        "11.7 /start right key passes auth, then fails at the dead API (500, no call placed)",
        r.status_code == 500,
        f"{r.status_code} {r.text[:120]}",
    )
    for m in ("get", "post"):
        r = getattr(c, m)("/answer")
        chk(
            f"11.7 /answer {m.upper()} returns <Stream> XML",
            r.status_code == 200
            and "<Stream" in r.text
            and ">wss://testserver/ws?direction=inbound<" in r.text,
            r.text[:160],
        )

    def attempt(headers=None):
        """('accepted'|'refused', detail): refused = the server closed before accept."""
        state = {"accepted": False}
        try:
            with c.websocket_connect("/ws", headers=headers or {}) as ws:
                state["accepted"] = True
        except Exception as e:
            if state["accepted"]:
                return "accepted", f"(exit error ignored: {type(e).__name__})"
            return "refused", f"{type(e).__name__} {getattr(e, 'code', '')}"
        return "accepted", ""

    r1 = attempt()
    chk("11.7 /ws unsigned upgrade refused", r1[0] == "refused", r1)
    url = "wss://bot.example.com/ws"
    t, n = int(time.time()), secrets.token_hex(8)
    hdr = {
        "X-Unpod-Signature": f"t={t},n={n},v1={stream_signature('fake-secret', url, t, n)}"
    }
    server.PUBLIC_WS_BASE = None
    r2 = attempt(hdr)
    chk(
        "11.7 /ws signed for a public host is refused when PUBLIC_WS_BASE is unset (Host=testserver)",
        r2[0] == "refused",
        r2,
    )
    # correct config: public base set, same signature

    t, n = int(time.time()), secrets.token_hex(8)
    hdr = {
        "X-Unpod-Signature": f"t={t},n={n},v1={stream_signature('fake-secret', url, t, n)}"
    }
    server.PUBLIC_WS_BASE = "wss://bot.example.com"
    r3 = attempt(hdr)
    chk(
        "11.7 /ws correctly signed upgrade accepted (PUBLIC_WS_BASE set)",
        r3[0] == "accepted",
        r3,
    )
    r4 = attempt(hdr)
    chk("11.7 /ws replayed signature refused", r4[0] == "refused", r4)
print(f"\n{sum(R)}/{len(R)} passed")
