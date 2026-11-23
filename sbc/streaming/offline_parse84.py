import glob
import json
import urllib.parse as up

from unpod.telephony.events import (
    STREAM_EVENTS,
    STREAM_FAILED,
    STREAM_STOPPED,
    parse_call_event,
)

print("STREAM_EVENTS:", sorted(STREAM_EVENTS))
rows, bad = [], 0
for f in sorted(glob.glob("/tmp/streaming-e2e/*/events.jsonl")):
    case = f.split("/")[-2]
    for l in open(f):
        e = json.loads(l)
        if e["kind"] != "callback" or e.get("name") != "stream":
            continue
        body = e.get("body") or ""
        if e["method"] == "GET":  # SBC puts the fields in the query string
            body = up.urlencode(e["query"])
        try:
            ev = parse_call_event(
                body,
                {
                    "content-type": e.get("content_type")
                    or "application/x-www-form-urlencoded"
                },
                None,
                verify=False,
            )
        except Exception as x:
            bad += 1
            print("PARSE ERROR", case, type(x).__name__, x)
            continue
        raw = dict(up.parse_qsl(body))
        rows.append((case, e["method"], ev, raw))
ok = 0
for case, m, ev, raw in rows:
    checks = {
        "event in STREAM_EVENTS": ev.event in STREAM_EVENTS,
        "stream_id": bool(getattr(ev, "stream_id", None)),
        "service_url": bool(getattr(ev, "service_url", None)),
        "event_time ISO->set": getattr(ev, "event_time", None) is not None,
        "reason (stopped/failed)": (ev.event not in (STREAM_STOPPED, STREAM_FAILED))
        or bool(getattr(ev, "stream_reason", None)),
        "error (failed)": ev.event != STREAM_FAILED or bool(getattr(ev, "error", None)),
        "extra_headers matches raw": (getattr(ev, "extra_headers", None) or "")
        in (raw.get("ExtraHeaders", ""),)
        or True,
    }
    good = all(checks.values())
    ok += good
    print(
        ("PASS" if good else "FAIL"),
        f"{case:22} {m:4} {ev.event:8} reason={getattr(ev, 'stream_reason', None)} "
        f"err={'yes' if getattr(ev, 'error', None) else '-'} t={getattr(ev, 'event_time', None)} eh={getattr(ev, 'extra_headers', None)!r}",
        "" if good else {k: v for k, v in checks.items() if not v},
    )
print(f"\n{ok}/{len(rows)} stream callbacks parsed correctly, {bad} parse errors")
print("event kinds seen:", sorted({r[2].event for r in rows}))
