# core/config.py
from __future__ import annotations
import os
from pathlib import Path

ROOT       = Path(__file__).resolve().parent.parent
DATA_DIR   = Path(os.environ.get("QS_DATA_DIR", str(ROOT / "data_store")))
TOKEN_FILE = DATA_DIR / "upstox_token.txt"
PORT       = int(os.environ.get("PORT", 5050))
DEBUG      = os.environ.get("QS_DEBUG", "false").lower() == "true"

UPSTOX_KEY      = os.environ.get("UPSTOX_API_KEY",    "")
UPSTOX_SECRET   = os.environ.get("UPSTOX_API_SECRET", "")
UPSTOX_REDIRECT = os.environ.get(
    "UPSTOX_REDIRECT",
    f"http://127.0.0.1:{PORT}/api/data/auth/callback"
)

# Try keys.py fallback for local dev only
if not UPSTOX_KEY:
    try:
        import keys as _k
        UPSTOX_KEY      = getattr(_k, "api_key",      "").strip()
        UPSTOX_SECRET   = getattr(_k, "secret",       "").strip()
        UPSTOX_REDIRECT = getattr(_k, "redirect_uri", UPSTOX_REDIRECT).strip()
    except ImportError:
        pass

DATA_DIR.mkdir(parents=True, exist_ok=True)