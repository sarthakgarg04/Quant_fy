# core/serializers.py
from __future__ import annotations
import numpy as np
import pandas as pd
from typing import Optional


def _safe(v):
    """Sanitise a single value for JSON output."""
    if isinstance(v, float) and (np.isnan(v) or np.isinf(v)): return None
    if isinstance(v, np.integer):  return int(v)
    if isinstance(v, np.floating): return float(v)
    if isinstance(v, bool):        return bool(v)
    return v


def safe_dict(d: dict) -> dict:
    """Sanitise all values in a dict for JSON output. Replaces _sr()."""
    return {k: _safe(v) for k, v in d.items()}


def candles(df: pd.DataFrame) -> list:
    """Convert OHLCV DataFrame → list of JSON-safe dicts for the chart."""
    out = []
    for ts, row in df.iterrows():
        out.append({
            "time":   int(pd.Timestamp(ts).timestamp()),
            "open":   round(float(row["Open"]),  2),
            "high":   round(float(row["High"]),  2),
            "low":    round(float(row["Low"]),   2),
            "close":  round(float(row["Close"]), 2),
            "volume": float(row.get("Volume", 0)),
        })
    return out


def serialise_zones(zones: Optional[list]) -> list:
    """Convert ZoneDict list → JSON-safe list."""
    out = []
    for z in (zones or []):
        try:
            out.append({
                "time_start":      int(pd.Timestamp(z["time_start"]).timestamp()),
                "time_end":        int(pd.Timestamp(z["time_end"]).timestamp()),
                "price_high":      z["price_high"],
                "price_low":       z["price_low"],
                "zone_type":       z.get("zone_type",       "rbr"),
                "status":          z.get("status",          "fresh"),
                "base_bars":       z.get("base_bars",        0),
                "legout_strength": z.get("legout_strength",  0.0),
                "vol_score":       z.get("vol_score",        0.0),
            })
        except Exception:
            pass
    return out


def parse_states(raw: str) -> list:
    """Parse a comma-separated structure state filter string → list."""
    return [s.strip() for s in raw.split(",") if s.strip()] if raw else []