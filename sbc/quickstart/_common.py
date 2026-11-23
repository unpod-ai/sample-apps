"""Shared helpers for the quickstart step scripts: load .env, print PASS / FAIL."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

HERE = Path(__file__).resolve().parent
ENV_FILE = HERE / ".env"
# quickstart/.env first, then sbc/.env
load_dotenv(ENV_FILE)
load_dotenv(HERE.parent / ".env")

REQUIRED = ("UNPOD_BASE_URL", "UNPOD_PLATFORM_TOKEN", "UNPOD_ORG_HANDLE")


def ok(msg: str) -> None:
    print(f"PASS  {msg}")


def info(msg: str) -> None:
    print(f"      {msg}")


def fail(msg: str, hint: str = "") -> None:
    print(f"FAIL  {msg}")
    if hint:
        print(f"      -> {hint}")
    sys.exit(1)


def need_env() -> None:
    missing = [k for k in REQUIRED if not os.environ.get(k)]
    if missing:
        fail(f"missing {', '.join(missing)}", f"put them in {ENV_FILE} or ../.env (see README)")


def is_e164(n: str) -> bool:
    return n.startswith("+") and n[1:].isdigit() and 8 <= len(n) <= 16


def error_text(exc: Exception) -> str:
    """Status + body of an httpx error, or the error itself."""
    resp = getattr(exc, "response", None)
    if resp is not None:
        return f"{resp.status_code} {resp.text[:400]}"
    return str(exc)
