import json
import sys

d = json.load(open(f"/tmp/streaming-e2e/{sys.argv[1]}/summary.json"))
print(
    "call",
    d.get("call_id"),
    "| sig_checked",
    d.get("signature_checked"),
    "verified",
    d.get("signature_verified"),
    "| close",
    d.get("ws_close_code"),
)
print("start", d.get("start"))
print("ws_end", d.get("ws_end"))
for c in d.get("callbacks", []):
    if c["name"] == "stream":
        e = c["event"]
        print(
            "  stream cb",
            c["method"],
            e.get("event"),
            e.get("stream_reason"),
            "dur",
            e.get("duration"),
            "err",
            (e.get("error") or "")[:80],
            "verified",
            c["verified"],
        )
ev = [c["event"].get("event") for c in d.get("callbacks", []) if c["name"] == "call"]
print(
    "call cbs", ev, "| all verified", all(c["verified"] for c in d.get("callbacks", []))
)
print(
    "answers", [(a["method"], a["signed"], a["verified"]) for a in d.get("answer", [])]
)
for k in d:
    if k not in (
        "case",
        "call_id",
        "query",
        "signature_checked",
        "signature_verified",
        "events",
        "upgrade",
        "start",
        "ws_end",
        "ws_close_code",
        "callbacks",
        "answer",
        "answer_xml",
    ):
        print(k, json.dumps(d[k])[:200])
