"""Quickstart "Make your first call", step 2: install.

Docs: https://docs.unpod.ai/telephony/apis/quickstart
Run:  uv run quickstart/step2_install.py

Checks Python 3.12+ and unpod>=0.6.2 with the telephony module. No network.
"""

import sys
from importlib.metadata import version

from _common import fail, ok

if sys.version_info < (3, 12):  # noqa: UP036 - this check is the point of the step
    fail(f"Python {sys.version.split()[0]}", "unpod needs Python 3.12+")
ok(f"Python {sys.version.split()[0]}")

try:
    v = version("unpod")
except Exception:  # noqa: BLE001
    fail("unpod is not installed", 'pip install "unpod>=0.6.2"  (or uv sync)')
parts = tuple(int(p) for p in v.split(".")[:3] if p.isdigit())
if parts < (0, 6, 2):
    fail(f"unpod {v}", 'pip install -U "unpod>=0.6.2"')
ok(f"unpod {v}")

from unpod.telephony import InboundConfig, TelephonyAPIError, parse_call_event  # noqa: F401

ok("unpod.telephony imports (parse_call_event, InboundConfig, TelephonyAPIError)")
