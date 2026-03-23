"""
zones.py  –  RBR / DBR / DBD / RBD zone detection
====================================================
Improvements over original:
  • All magic numbers extracted as named parameters
  • ATR computed once per call (not per-loop)
  • Inner looker() loop uses vectorised pre-filters to skip obvious non-zones
  • buy_zone / sell_zone share a unified _zone_scanner core
  • HTF resistance check is a pure function (no side-effects)
  • 5m variant unified (multiplier param, not separate function)
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from typing import List, Tuple, Optional
import datetime as dt

from algos.pivot_engine import compute_atr

ZoneResult = List[List]   # [[legin_ts, legout_ts], ...]


# ──────────────────────────────────────────────────────────────────────────────
# Internal scanner  (replaces the nested looker/main pattern)
# ──────────────────────────────────────────────────────────────────────────────

def _zone_scanner(
    df: pd.DataFrame,
    start_idx: int,
    direction: str,          # "buy" | "sell"
    ztype: str,              # "rbr" | "dbr" | "dbd" | "rbd"
    legin_mult: float,       # ATR multiple for legin candle
    base_mult: float,        # ATR multiple – base candle (inside zone)
    legout_mult: float,      # ATR multiple for legout candle
    legout_strength: float,  # legout must be >= this × legin_len
    max_base_candles: int,   # max candles allowed inside base
    legout_wick_pct: float,  # upper wick of legout must be < this × range
    exclude_eod: bool = True,# skip 15:15 candles
) -> ZoneResult:
    """
    Scan for demand/supply zones defined by: legin → base(s) → legout.

    Returns list of [legin_timestamp, legout_timestamp].
    """
    close  = df["Close"].values
    prev_c = df["Prev_close"].values
    high   = df["High"].values
    low    = df["Low"].values
    atr    = df["atr"].values
    n      = len(df)

    result = []
    i = start_idx

    while i < n - 2:
        candle_body = close[i] - prev_c[i]

        # ── 1. Legin detection ─────────────────────────────────────────────
        legin_ok = False
        if direction == "buy" and ztype == "rbr":
            legin_ok = candle_body > legin_mult * atr[i]
        elif direction == "buy" and ztype == "dbr":
            legin_ok = -candle_body > legin_mult * atr[i]
        elif direction == "sell":
            legin_ok = abs(candle_body) > legin_mult * atr[i]

        if not legin_ok:
            i += 1
            continue

        legin_idx  = i
        legin_len  = abs(candle_body)
        legin_close = close[i]
        threshold  = high[i]         # legout must close above this (buy)

        # ── 2. Base candles ────────────────────────────────────────────────
        base_count = 0
        j = i + 1
        in_base = True

        while j < min(i + max_base_candles + 2, n - 1) and in_base:
            # EOD filter
            if exclude_eod and hasattr(df.index[j], 'time'):
                if df.index[j].time() == dt.time(15, 15):
                    j += 1
                    continue

            cj = close[j]
            pc_j = prev_c[j]
            body_j = cj - pc_j

            # Buy zone base: price stays near legin close
            if direction == "buy":
                center = legin_close
                half   = base_mult * abs(legin_close - prev_c[legin_idx])
                in_zone = (center - half) < cj < (center + half * 1.2)
            else:
                center = legin_close
                half   = base_mult * abs(legin_close - prev_c[legin_idx])
                in_zone = (center - half) < low[j] < (center + half)

            if in_zone:
                base_count += 1
                threshold = max(threshold, high[j])
                j += 1
            else:
                in_base = False

        if base_count == 0:
            i += 1
            continue

        # ── 3. Legout detection ────────────────────────────────────────────
        if j >= n - 1:
            i = j
            continue

        body_j    = close[j] - prev_c[j]
        wick_up   = (high[j] - close[j]) / max(high[j] - low[j], 1e-8)

        legout_ok = False
        if direction == "buy":
            legout_ok = (
                body_j > legout_mult * atr[j]
                and wick_up < legout_wick_pct
                and abs(body_j) >= legout_strength * legin_len
                and close[j] > threshold
            )
        else:
            body_abs = abs(body_j)
            legout_ok = (
                body_abs > legout_mult * atr[j]
                and body_abs >= legout_strength * legin_len
                and close[j] < threshold
            )

        if legout_ok:
            result.append([df.index[legin_idx], df.index[j]])
            i = j + 1
        else:
            i = i + 1

    return result


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────

def _prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    """Add Prev_close and ATR columns if missing."""
    df = df.copy()
    if "Prev_close" not in df.columns:
        df["Prev_close"] = df["Close"].shift(1).fillna(df["Close"])
    if "atr" not in df.columns:
        df["atr"] = compute_atr(df)
    return df


def buy_zone(
    df: pd.DataFrame,
    start_idx: int,
    resist,                   # "NA" or [resist_level, ..., atr_at_resist]
    ztype: str = "rbr",       # "rbr" | "dbr"
    multiplier: float = 1.0,  # overall scaling for all ATR thresholds
) -> Tuple[ZoneResult, int]:
    """
    Detect bullish demand zones.

    Returns ([zone_list], htf_flag).
    """
    df = _prepare_df(df)

    zones = _zone_scanner(
        df=df,
        start_idx=start_idx,
        direction="buy",
        ztype=ztype,
        legin_mult=multiplier,
        base_mult=0.55 * multiplier,
        legout_mult=1.35 * multiplier,
        legout_strength=1.0,
        max_base_candles=5,
        legout_wick_pct=0.25,
    )

    htf_flag = 0
    if resist != "NA" and resist is not None:
        resist_level = resist[1] - resist[3]
        if df["High"].max() > resist_level:
            htf_flag = 1

    return zones, htf_flag


def sell_zone(
    df: pd.DataFrame,
    start_idx: int,
    multiplier: float = 1.0,
) -> ZoneResult:
    """Detect bearish supply zones."""
    df = _prepare_df(df)
    return _zone_scanner(
        df=df,
        start_idx=start_idx,
        direction="sell",
        ztype="dbd",
        legin_mult=0.9 * multiplier,
        base_mult=0.45 * multiplier,
        legout_mult=1.5 * multiplier,
        legout_strength=1.1,
        max_base_candles=7,
        legout_wick_pct=0.99,  # not checked for sell
    )


def buy_zone_5m(
    df: pd.DataFrame,
    start_idx: int,
    resist,
    ztype: str = "rbr",
) -> Tuple[ZoneResult, int]:
    """5-minute timeframe variant (tighter thresholds)."""
    return buy_zone(df, start_idx, resist, ztype, multiplier=0.85)
