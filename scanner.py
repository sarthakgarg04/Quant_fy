"""
scanner.py  –  Multi-stock zone + trend scanner
=================================================
Runs buy_zone, trend detection, and confluence scoring across
a watchlist and returns a ranked DataFrame of setups.
"""

from __future__ import annotations

import traceback
from typing import List, Dict, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd

from algos.pivot_engine import extrems, detect_trend, compute_atr, tag_signals
from algos.zones import buy_zone, sell_zone
from algos.confluence import confluence_score, HTFContext
from algos.patterns import detect_flag, detect_triangle, pivot_trend_strength
from data_fetch.data_fetch import fetch_ohlcv


# ──────────────────────────────────────────────────────────────────────────────
# Per-symbol analysis
# ──────────────────────────────────────────────────────────────────────────────

def analyse_symbol(
    ticker: str,
    df: pd.DataFrame,
    df_weekly: Optional[pd.DataFrame] = None,
    order: int = 5,
    zone_lookback_bars: int = 200,
    direction: str = "buy",
) -> Optional[Dict[str, Any]]:
    """
    Full analysis for one symbol.
    Returns None if analysis fails or no setup found.
    """
    try:
        if df is None or len(df) < 50:
            return None

        # ATR
        atr_series = compute_atr(df)
        atr_val    = float(atr_series.iloc[-1])
        close      = float(df["Close"].iloc[-1])
        atr_pct    = atr_val / close * 100

        # Pivots & trend
        pvts  = extrems(df, order=order, min_strength_atr_mult=0.3)
        trend = detect_trend(pvts)
        strength = pivot_trend_strength(pvts)

        # HTF context
        htf = None
        if df_weekly is not None and len(df_weekly) >= 20:
            try:
                htf = HTFContext(df_weekly, df)
            except Exception:
                pass

        # Confluence score
        cscore = confluence_score(df, order=order, htf=htf, direction=direction)

        # Zone detection (last zone_lookback_bars)
        start_idx = max(0, len(df) - zone_lookback_bars)
        if direction == "buy":
            zones, htf_flag = buy_zone(df, start_idx, "NA")
        else:
            zones = sell_zone(df, start_idx)
            htf_flag = 0

        if not zones:
            return None

        # Most recent zone
        last_zone = zones[-1]
        legin_ts  = str(last_zone[0])
        legout_ts = str(last_zone[1])

        # Pattern flags
        flag_pattern     = detect_flag(df, order=order)
        triangle_pattern = detect_triangle(df, order=order)

        return {
            "ticker":          ticker,
            "close":           round(close, 2),
            "trend":           trend,
            "trend_score":     cscore["trend_score"],
            "confluence":      cscore["score"],
            "zone_score":      cscore["zone_score"],
            "htf_score":       cscore["htf_score"],
            "vol_score":       cscore["vol_score"],
            "atr":             round(atr_val, 2),
            "atr_pct":         round(atr_pct, 2),
            "zones_count":     len(zones),
            "last_zone_legin": legin_ts,
            "last_zone_legout":legout_ts,
            "htf_flag":        htf_flag,
            "flag_pattern":    flag_pattern,
            "triangle":        triangle_pattern,
            "trend_strength":  strength["classification"],
            "strength_score":  round(strength["score"], 4),
            "pivot_count":     len(pvts),
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
    weekly_dict: Optional[Dict[str, pd.DataFrame]] = None,
    order: int = 5,
    direction: str = "buy",
    min_confluence: float = 0.45,
    max_workers: int = 6,
) -> pd.DataFrame:
    """
    Scan multiple symbols and return ranked results.

    Parameters
    ----------
    tickers         : list of ticker strings
    data_dict       : {ticker: daily_df}
    weekly_dict     : {ticker: weekly_df}  (optional, for HTF context)
    order           : pivot order
    direction       : "buy" | "sell"
    min_confluence  : filter out scores below this
    max_workers     : thread pool size
    """
    results = []

    def _analyse(ticker: str):
        df      = data_dict.get(ticker)
        df_wkly = weekly_dict.get(ticker) if weekly_dict else None
        return analyse_symbol(ticker, df, df_wkly, order=order, direction=direction)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_analyse, t): t for t in tickers}
        for fut in as_completed(futures):
            res = fut.result()
            if res and res["confluence"] >= min_confluence:
                results.append(res)

    if not results:
        return pd.DataFrame()

    df_out = pd.DataFrame(results)
    df_out.sort_values("confluence", ascending=False, inplace=True)
    df_out.reset_index(drop=True, inplace=True)
    return df_out


# ──────────────────────────────────────────────────────────────────────────────
# Live single-stock fetch + analyse
# ──────────────────────────────────────────────────────────────────────────────

def quick_scan(
    ticker: str,
    interval: str = "1d",
    days: int = 500,
    order: int = 5,
    direction: str = "buy",
) -> Optional[Dict[str, Any]]:
    """Fetch fresh data and analyse a single ticker."""
    df = fetch_ohlcv(ticker, interval=interval, days=days)
    if df.empty:
        return None
    df_wkly = fetch_ohlcv(ticker, interval="1wk", days=days * 2)
    return analyse_symbol(ticker, df, df_wkly if not df_wkly.empty else None,
                          order=order, direction=direction)
