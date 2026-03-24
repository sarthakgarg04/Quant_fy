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
from algos.pivot_engine import (
      extrems, detect_trend, compute_atr,
      detect_trend_multiorder, compute_pivot_velocity,
      compute_pivot_amplitude, compute_leg_ratio,
      _trend_matches,
  )

import numpy as np
import pandas as pd

from algos.patterns     import pivot_trend_strength
from algos.zones        import buy_zone, sell_zone, scan_zones
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

def analyse_symbol(
    ticker: str,
    df: pd.DataFrame,
    order: int = 5,
    zone_lookback: int = 20,
    direction: str = "buy",
    trend_filter: Optional[Set[str]] = None,
    legout_mult: float = 1.35,
    strategy: str = "atr",
    # Multi-order params
    multi_order: bool = False,
    order_low:   int = 5,
    order_mid:   int = 10,
    order_high:  int = 20,
    trend_low:   str = "any",
    trend_mid:   str = "any",
    trend_high:  str = "any",
) -> Optional[Dict[str, Any]]:
    try:
        if df is None or len(df) < max(50, order * 4):
            return None
 
        pvts     = extrems(df, order=order)
        trend    = detect_trend(pvts)
        strength = pivot_trend_strength(pvts)
 
        # ── Single-order trend filter (existing behaviour) ─────────────
        if trend_filter and trend not in trend_filter:
            return None
 
        # ── Multi-order analysis + filter ──────────────────────────────
        mo_data = None
        if multi_order:
            mo = detect_trend_multiorder(
                df,
                order_low=order_low,
                order_mid=order_mid,
                order_high=order_high,
            )
            # AND logic: ALL non-"any" filters must match
            if not _trend_matches(mo["low"]["trend"],  trend_low):  return None
            if not _trend_matches(mo["mid"]["trend"],  trend_mid):  return None
            if not _trend_matches(mo["high"]["trend"], trend_high): return None
 
            # Override displayed trend with combined trend
            trend   = mo["combined_trend"]
            mo_data = mo
 
        # ── ATR + close ────────────────────────────────────────────────
        atr_series = compute_atr(df)
        atr_val    = float(atr_series.iloc[-1])
        close      = float(df["Close"].iloc[-1])
        atr_pct    = round(atr_val / close * 100, 2)
 
        # ── Zone detection ─────────────────────────────────────────────
        start_idx      = max(0, len(df) - zone_lookback)
        include_atr    = strategy in ("atr", "both")
        include_consol = strategy in ("consolidation", "both")
 
        zones       = scan_zones(
            df,
            direction=direction,
            legout_mult=legout_mult,
            max_base_candles=8,
            start_idx=start_idx,
            include_atr=include_atr,
            include_consolidation=include_consol,
        )
        valid_zones = [z for z in zones if z.get("status") != "invalidated"]
        if not valid_zones:
            return None
 
        last_zone = valid_zones[-1]
        legin_ts  = pd.Timestamp(last_zone["time_start"])
        legout_ts = pd.Timestamp(last_zone["time_end"])
 
        try:
            legin_bar_idx = df.index.get_indexer([legin_ts], method="nearest")[0]
            bars_ago      = len(df) - 1 - legin_bar_idx
        except Exception:
            bars_ago = zone_lookback
 
        # ── Velocity / amplitude / leg_ratio on primary order ──────────
        vel = compute_pivot_velocity(pvts)
        amp = compute_pivot_amplitude(pvts)
        lr  = compute_leg_ratio(pvts)
 
        result = {
            "ticker":          ticker,
            "close":           round(close, 2),
            "trend":           trend,
            "trend_strength":  strength["classification"],
            "atr":             round(atr_val, 2),
            "atr_pct":         atr_pct,
            "zones_count":     len(valid_zones),
            "zone_high":       last_zone["price_high"],
            "zone_low":        last_zone["price_low"],
            "zone_legin_ts":   int(legin_ts.timestamp()),
            "zone_legout_ts":  int(legout_ts.timestamp()),
            "zone_type":       last_zone.get("zone_type",  "rbr"),
            "zone_status":     last_zone.get("status",     "fresh"),
            "vol_score":       last_zone.get("vol_score",  0.0),
            "bars_ago":        bars_ago,
            "pivot_count":     len(pvts),
            "direction":       direction,
            "strategy":        strategy,
            # Velocity
            "vel_current":     vel["current_vel"],
            "vel_avg":         vel["avg_velocity"],
            "vel_accel":       vel["acceleration"],
            "vel_label":       vel["accel_label"],
            # Amplitude
            "amp_avg":         amp["avg_amplitude"],
            "amp_recent":      amp["recent_avg"],
            "amp_regime":      amp["regime"],
            "amp_variance":    amp["variance_pct"],
            # Leg ratio
            "lr_ratio":        lr["ratio"],
            "lr_recent":       lr["recent_ratio"],
            "lr_bull":         lr["avg_bull"],
            "lr_bear":         lr["avg_bear"],
            "lr_label":        lr["label"],
        }
 
        # ── Multi-order fields for expanded row display ─────────────────
        if mo_data:
            result["mo_low_trend"]   = mo_data["low"]["trend"]
            result["mo_low_score"]   = mo_data["low"]["score"]
            result["mo_mid_trend"]   = mo_data["mid"]["trend"]
            result["mo_mid_score"]   = mo_data["mid"]["score"]
            result["mo_high_trend"]  = mo_data["high"]["trend"]
            result["mo_high_score"]  = mo_data["high"]["score"]
            result["mo_alignment"]   = mo_data["alignment"]
            result["mo_combined"]    = mo_data["combined_trend"]
            result["mo_order_low"]   = order_low
            result["mo_order_mid"]   = order_mid
            result["mo_order_high"]  = order_high
 
        return result
 
    except Exception:
        traceback.print_exc()
        return None

def scan_watchlist(
    tickers: List[str],
    data_dict: Dict[str, pd.DataFrame],
    order: int = 5,
    zone_lookback: int = 20,
    direction: str = "buy",
    trend_filter: Optional[List[str]] = None,
    legout_mult: float = 1.35,
    strategy: str = "atr",
    multi_order: bool = False,
    order_low:   int = 5,
    order_mid:   int = 10,
    order_high:  int = 20,
    trend_low:   str = "any",
    trend_mid:   str = "any",
    trend_high:  str = "any",
    max_workers: int = 6,
) -> pd.DataFrame:
    tf_set  = set(trend_filter) if trend_filter else None
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
            strategy=strategy,
            multi_order=multi_order,
            order_low=order_low,
            order_mid=order_mid,
            order_high=order_high,
            trend_low=trend_low,
            trend_mid=trend_mid,
            trend_high=trend_high,
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
    df_out.sort_values("bars_ago", ascending=True, inplace=True)
    df_out.reset_index(drop=True, inplace=True)
    return df_out
