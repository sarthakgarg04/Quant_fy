"""
utils/logger.py  —  QuantScanner Logging Infrastructure
=========================================================
Rotating file logger with structured context, shared by backend and frontend.

Design decisions:
  • RotatingFileHandler: 2 MB per file, 5 backups → 10 MB max total on disk
  • Single log file: logs/app.log (backend + frontend events in one place)
  • Format:  [2025-01-15 14:32:01.123] [INFO ] [module_name     ] message | key=val key=val
  • Log level controlled by env var QS_LOG_LEVEL (default: INFO)
  • Thread-safe: Python's logging module handles locking internally
  • Frontend events arrive via POST /api/log and are written with [FE] tag

Usage:
    from utils.logger import get_logger
    log = get_logger(__name__)

    log.info("Scan complete", interval="1d", matched=23, elapsed_ms=4210)
    log.warning("No zones found", ticker="RELIANCE", strategy="atr")
    log.error("Data load failed", ticker="TCS", exc=str(e))
    log.debug("Pivot count", ticker="INFY", count=47)
"""

from __future__ import annotations

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

# ── Configuration ─────────────────────────────────────────────────────────────

# Project root = two levels up from this file (utils/logger.py → root)
_ROOT     = Path(__file__).resolve().parent.parent
_LOG_DIR  = _ROOT / "logs"
_LOG_FILE = _LOG_DIR / "app.log"

# Rotation: 2 MB per file, keep 5 backups → max 12 MB total including active file
_MAX_BYTES    = 2 * 1024 * 1024   # 2 MB
_BACKUP_COUNT = 5

# Log level from environment, default INFO
_LEVEL_NAME = os.environ.get("QS_LOG_LEVEL", "INFO").upper()
_LEVEL      = getattr(logging, _LEVEL_NAME, logging.INFO)

# ── Custom formatter ──────────────────────────────────────────────────────────

class _StructuredFormatter(logging.Formatter):
    """
    Formats log records as:
      [2025-01-15 14:32:01.123] [INFO ] [scanner_engine  ] message | key=val key=val

    Context kwargs are attached to the record via QSLogger.info/warning/etc.
    Module name is padded to 16 chars for easy column-scanning in the log file.
    """

    def format(self, record: logging.LogRecord) -> str:
        ts      = self.formatTime(record, "%Y-%m-%d %H:%M:%S")
        ms      = f"{record.msecs:03.0f}"
        level   = f"{record.levelname:<5}"          # 'INFO ' / 'WARN ' / 'ERROR'
        module  = f"{record.name:<16}"[:16]          # padded, truncated at 16
        message = record.getMessage()

        # Structured context appended as key=val pairs after ' | '
        ctx = getattr(record, "_ctx", {})
        if ctx:
            ctx_str = " ".join(f"{k}={v}" for k, v in ctx.items())
            line    = f"[{ts}.{ms}] [{level}] [{module}] {message} | {ctx_str}"
        else:
            line    = f"[{ts}.{ms}] [{level}] [{module}] {message}"

        # Append exception info if present
        if record.exc_info:
            line += "\n" + self.formatException(record.exc_info)

        return line


# ── Logger wrapper ────────────────────────────────────────────────────────────

class QSLogger:
    """
    Thin wrapper around stdlib Logger that supports structured context kwargs.

    Instead of:
        log.info(f"Scan done interval=1d matched=23 elapsed=4.2s")

    Write:
        log.info("Scan done", interval="1d", matched=23, elapsed_ms=4200)

    This keeps messages grep-friendly and machine-parseable.
    """

    def __init__(self, name: str):
        self._log = logging.getLogger(name)

    def _emit(self, level: int, msg: str, **ctx: Any) -> None:
        if self._log.isEnabledFor(level):
            record = self._log.makeRecord(
                name    = self._log.name,
                level   = level,
                fn      = "(unknown)",
                lno     = 0,
                msg     = msg,
                args    = (),
                exc_info= ctx.pop("exc_info", None),
            )
            record._ctx = ctx
            self._log.handle(record)

    def debug(self, msg: str, **ctx: Any)   -> None: self._emit(logging.DEBUG,   msg, **ctx)
    def info(self, msg: str, **ctx: Any)    -> None: self._emit(logging.INFO,    msg, **ctx)
    def warning(self, msg: str, **ctx: Any) -> None: self._emit(logging.WARNING, msg, **ctx)
    def error(self, msg: str, **ctx: Any)   -> None: self._emit(logging.ERROR,   msg, **ctx)
    def critical(self, msg: str, **ctx: Any)-> None: self._emit(logging.CRITICAL,msg, **ctx)


# ── Setup (runs once on first import) ────────────────────────────────────────

def _setup() -> None:
    """
    Configure root logger with:
      1. RotatingFileHandler  → logs/app.log
      2. StreamHandler        → stdout (for dev visibility in terminal)

    Called once at module import. Idempotent if called again.
    """
    root = logging.getLogger()

    # Already configured — skip
    if root.handlers:
        return

    root.setLevel(_LEVEL)

    # Ensure logs directory exists
    _LOG_DIR.mkdir(parents=True, exist_ok=True)

    fmt = _StructuredFormatter()

    # ── File handler (rotating) ───────────────────────────────────────────────
    fh = RotatingFileHandler(
        filename    = _LOG_FILE,
        maxBytes    = _MAX_BYTES,
        backupCount = _BACKUP_COUNT,
        encoding    = "utf-8",
        delay       = False,       # open file immediately (not on first write)
    )
    fh.setFormatter(fmt)
    fh.setLevel(_LEVEL)
    root.addHandler(fh)

    # ── Console handler (stdout) ──────────────────────────────────────────────
    # Keep console at INFO even if file is at DEBUG — less noise in terminal
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    ch.setLevel(max(_LEVEL, logging.INFO))
    root.addHandler(ch)

    # Suppress noisy third-party loggers
    for noisy in ("werkzeug", "urllib3", "requests", "botocore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


# Run setup on import
_setup()


# ── Public factory ────────────────────────────────────────────────────────────

def get_logger(name: str) -> QSLogger:
    """
    Get a structured logger for a module.

    Args:
        name: typically __name__ of the calling module

    Returns:
        QSLogger instance

    Example:
        log = get_logger(__name__)
        log.info("Server started", port=5050, level=_LEVEL_NAME)
    """
    return QSLogger(name)


# ── Log file helpers ──────────────────────────────────────────────────────────

def read_log_tail(n_lines: int = 200, level_filter: str = "") -> list[dict]:
    """
    Read last N lines from the active log file.
    Optionally filter to only lines containing level_filter (e.g. "ERROR").

    Returns list of dicts: [{line, level, module, message, timestamp}]
    Used by /api/logs endpoint.
    """
    if not _LOG_FILE.exists():
        return []

    try:
        with open(_LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except OSError:
        return []

    # Filter by level if requested
    if level_filter:
        lf = level_filter.upper()
        lines = [l for l in lines if f"[{lf}" in l or f"[{lf} " in l]

    # Take last n_lines
    lines = lines[-n_lines:]

    results = []
    for raw in lines:
        raw = raw.rstrip("\n")
        if not raw:
            continue
        # Try to parse structured format for richer response
        # Format: [TIMESTAMP.ms] [LEVEL] [MODULE] message
        entry = {"raw": raw}
        try:
            parts = raw.split("] [")
            if len(parts) >= 4:
                entry["timestamp"] = parts[0].lstrip("[")
                entry["level"]     = parts[1].strip()
                entry["module"]    = parts[2].strip()
                rest               = parts[3]
                bracket_end        = rest.index("]")
                entry["message"]   = rest[bracket_end + 2:]
        except (ValueError, IndexError):
            pass
        results.append(entry)

    return results


def log_stats() -> dict:
    """
    Return current log file stats: size, path, rotation info.
    Used by /api/logs/stats endpoint.
    """
    stats = {
        "log_file":     str(_LOG_FILE),
        "log_dir":      str(_LOG_DIR),
        "max_bytes":    _MAX_BYTES,
        "backup_count": _BACKUP_COUNT,
        "level":        _LEVEL_NAME,
        "files":        [],
    }

    # List all log files (active + rotations)
    for i in range(_BACKUP_COUNT + 1):
        path = _LOG_FILE if i == 0 else Path(f"{_LOG_FILE}.{i}")
        if path.exists():
            stats["files"].append({
                "path":  str(path),
                "size":  path.stat().st_size,
                "index": i,
            })

    return stats
