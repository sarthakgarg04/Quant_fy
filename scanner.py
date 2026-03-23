"""
scanner.py  –  Zone finder + trend filter
==========================================
Logic:
  1. Detect trend using pivot engine (configurable order)
  2. Filter: trend must match the selected trend label(s)
  3. Filter: at least one buy/sell zone must exist in the last
             `zone_lookback` bars
  4. Return matching symbols sorted by zone recency (most recent first)

No confluence scoring — results are purely structural.
"""

from __future__ import annotations

import traceback
from typing import List, Dict, Optional, Any, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd

from algos.pivot_engine import extrems, detect_trend, compute_atr
from algos.patterns     import pivot_trend_strength
from algos.zones        import buy_zone, sell_zone
from data_fetch.data_fetch import fetch_ohlcv

# All valid trend labels the UI can filter on
ALL_TRENDS: List[str] = [
    "strong_uptrend",
    "continuous_uptrend",
    "up",
    "bullish_mitigation",
    "semi-up",
    "weakening_uptrend",
    "consolidate",
    "flat_consolidation",
    "deep_consolidation",
    "sw",
    "expanding_structure",
    "weakening_downtrend",
    "down",
    "continuous_downtrend",
    "strong_downtrend",
]


# ──────────────────────────────────────────────────────────────────────────────
# Per-symbol analysis
# ──────────────────────────────────────────────────────────────────────────────

# REPLACE WITH:
def analyse_symbol(
    ticker: str,
    df: pd.DataFrame,
    order: int = 5,
    zone_lookback: int = 20,
    direction: str = "buy",
    trend_filter: Optional[Set[str]] = None,
    legout_mult: float = 1.35,
) -> Optional[Dict[str, Any]]:
    """
    Analyse one symbol.

    Returns a result dict if:
      • trend matches trend_filter (or filter is None)
      • at least one zone found in last zone_lookback bars

    Returns None otherwise.
    """
    try:
        if df is None or len(df) < max(50, order * 4):
            return None

        # ── Pivots + trend ────────────────────────────────────────────────
        pvts     = extrems(df, order=order)
        trend    = detect_trend(pvts)
        strength = pivot_trend_strength(pvts)

        # ── Trend filter ──────────────────────────────────────────────────
        if trend_filter and trend not in trend_filter:
            return None

        # ── ATR ───────────────────────────────────────────────────────────
        atr_series = compute_atr(df)
        atr_val    = float(atr_series.iloc[-1])
        close      = float(df["Close"].iloc[-1])
        atr_pct    = round(atr_val / close * 100, 2)

        # ── Zone detection in last zone_lookback bars ─────────────────────
        start_idx = max(0, len(df) - zone_lookback)

        if direction == "buy":
            zones, _ = buy_zone(df, start_idx, "NA", legout_strength_mult=legout_mult)
        else:
            zones = sell_zone(df, start_idx, legout_strength_mult=legout_mult)

        if not zones:
            return None

        # ── Zone details (most recent zone) ───────────────────────────────
        last_zone  = zones[-1]
        legin_ts   = pd.Timestamp(last_zone[0])
        legout_ts  = pd.Timestamp(last_zone[1])
        # Use actual base high/low (now returned by zone scanner)
        zone_high  = round(float(last_zone[2]), 2) if len(last_zone) > 2 else round(close, 2)
        zone_low   = round(float(last_zone[3]), 2) if len(last_zone) > 3 else round(close * 0.99, 2)

        # Zone recency: bars ago from end of df
        try:
            legin_bar_idx = df.index.get_indexer([legin_ts], method="nearest")[0]
            bars_ago      = len(df) - 1 - legin_bar_idx
        except Exception:
            bars_ago = zone_lookback

        return {
            "ticker":          ticker,
            "close":           round(close, 2),
            "trend":           trend,
            "trend_strength":  strength["classification"],
            "atr":             round(atr_val, 2),
            "atr_pct":         atr_pct,
            "zones_count":     len(zones),
            "zone_high":       zone_high,
            "zone_low":        zone_low,
            "zone_legin_ts":   int(legin_ts.timestamp()),
            "zone_legout_ts":  int(legout_ts.timestamp()),
            "bars_ago":        bars_ago,
            "pivot_count":     len(pvts),
            "direction":       direction,
        }

    except Exception:
        traceback.print_exc()
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Batch scanner
# ──────────────────────────────────────────────────────────────────────────────


def scan_watchlist(
    tickers: List[str],
    data_dict: Dict[str, pd.DataFrame],
    order: int = 5,
    zone_lookback: int = 20,
    direction: str = "buy",
    trend_filter: Optional[List[str]] = None,
    legout_mult: float = 1.35,
    max_workers: int = 6,
) -> pd.DataFrame:
    """
    Scan multiple symbols.

    Parameters
    ----------
    tickers       : list of ticker strings
    data_dict     : {ticker: df}
    order         : pivot look-left / look-right bars
    zone_lookback : only find zones in last N bars
    direction     : 'buy' | 'sell'
    trend_filter  : list of trend labels to include (None = all)
    """
    tf_set = set(trend_filter) if trend_filter else None
    results = []

    def _analyse(ticker: str):
        df = data_dict.get(ticker)
        return analyse_symbol(
            ticker, df,
            order=order,
            zone_lookback=zone_lookback,
            direction=direction,
            trend_filter=tf_set,
            legout_mult=legout_mult,
        )

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_analyse, t): t for t in tickers}
        for fut in as_completed(futures):
            try:
                res = fut.result()
                if res:
                    results.append(res)
            except Exception:
                pass

    if not results:
        return pd.DataFrame()

    df_out = pd.DataFrame(results)
    # Sort by zone recency: most recent zone first (lowest bars_ago)
    df_out.sort_values("bars_ago", ascending=True, inplace=True)
    df_out.reset_index(drop=True, inplace=True)
    return df_out
