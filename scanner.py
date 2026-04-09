"""
scanner.py  –  Zone finder + pivot structure filter  v3.3
==========================================================
Changes vs v3.2
---------------
• trend_low / trend_mid / trend_high REPLACED by structure_filter_low /
  structure_filter_mid / structure_filter_high using the HH/HL/LH/LL
  state system from trend_analysis.py.

• alignment_filter: "any" | "majority_bull" | "majority_bear" | "all_match"

• Structure state always computed and returned in result so the
  frontend can display it regardless of filter settings.

• strategy='trend_only' skips zone detection entirely.
"""

from __future__ import annotations

import traceback
from typing import List, Dict, Optional, Any, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

from algos.pivot_engine import (
    extrems, detect_trend, compute_atr,
    compute_pivot_velocity, compute_pivot_amplitude, compute_leg_ratio,
)
from algos.trend_analysis import (
    get_multiorder_structure,
    structure_matches_filter,
)
from algos.context import SymbolContext  

import pandas as pd

from algos.patterns  import pivot_trend_strength
from algos.zones     import scan_zones

from utils.logger import get_logger

log = get_logger(__name__)

_DEFAULT_ATR_TYPES = {
    "buy":  ["rbr", "dbr"],
    "sell": ["dbd", "rbd"],
}


# ─────────────────────────────────────────────────────────────────────────────
# Shared structure fields for result dicts
# ─────────────────────────────────────────────────────────────────────────────

def _structure_fields(mo: Dict[str, Any]) -> Dict[str, Any]:
    lo  = mo["low"]
    mid = mo["mid"]
    hi  = mo["high"]
    aln = mo["alignment"]
    return {
        "struct_low":        lo["state"],
        "struct_low_pair":   lo.get("curr_pair"),
        "struct_mid":        mid["state"],
        "struct_mid_pair":   mid.get("curr_pair"),
        "struct_high":       hi["state"],
        "struct_high_pair":  hi.get("curr_pair"),
        "struct_alignment":  aln["majority"],
        "struct_bull_count": aln["bull_count"],
        "struct_bear_count": aln["bear_count"],
        "mo_order_low":      lo["order"],
        "mo_order_mid":      mid["order"],
        "mo_order_high":     hi["order"],
    }

def _serialize_pivots(df: pd.DataFrame, pvts: list, n: int = 30) -> list:
    """
    Serialize the last `n` scan-time pivots to JSON-safe dicts.
    Format: {"d": "2024-11-15", "v": 1234.56, "t": "T"|"B"}
    Used purely for frontend debug display — does not affect scan logic.
    """
    out = []
    idx = df.index
    for bar_idx, val, typ in pvts[-n:]:
        try:
            date_str = str(idx[bar_idx].date())
        except Exception:
            date_str = str(bar_idx)
        out.append({"d": date_str, "v": round(float(val), 2), "t": typ})
    return out


def _debug_pivot_fields(df: pd.DataFrame, mo: dict, pvts: list,
                        multi_order: bool) -> dict:
    """
    Return the debug_pivots_* fields to embed in a scan result.
    Single-order → {"debug_pivots": [...]}
    Multi-order  → {"debug_pivots_H": [...], "debug_pivots_M": [...], "debug_pivots_L": [...]}
    """
    if multi_order:
        return {
            "debug_pivots_H": _serialize_pivots(df, mo["high"]["pivots"]),
            "debug_pivots_M": _serialize_pivots(df, mo["mid"]["pivots"]),
            "debug_pivots_L": _serialize_pivots(df, mo["low"]["pivots"]),
        }
    return {"debug_pivots": _serialize_pivots(df, pvts)}

# ─────────────────────────────────────────────────────────────────────────────
# Trend-only result
# ─────────────────────────────────────────────────────────────────────────────

def _trend_only_result(
    ticker: str,
    df: pd.DataFrame,
    pvts: list,
    trend: str,
    strength: dict,
    mo: Dict[str, Any],
    ctx: "SymbolContext",           # ← ADD
    order: int = 5,
    eff_low: int = 5,
    eff_mid: int = 10,
    eff_high: int = 20,
    multi_order: bool = False,
    zone_lookback: int = 20,
    legout_mult: float = 1.35,
    strategy: str = "trend_only",
    interval: str = "1d",
) -> Dict[str, Any]:
    atr_val = ctx.atr_val
    close   = ctx.close
    vel     = compute_pivot_velocity(pvts)
    amp     = compute_pivot_amplitude(pvts)
    lr      = compute_leg_ratio(pvts)

    result = {
        "ticker": ticker, "close": round(close, 2),
        "trend": trend, "trend_strength": strength["classification"],
        "atr": round(atr_val, 2), "atr_pct": round(atr_val / close * 100, 2),
        "zones_count": 0,
        "zone_high": None, "zone_low": None,
        "zone_legin_ts": None, "zone_legout_ts": None,
        "zone_type": "none", "zone_status": "none",
        "vol_score": None, "bars_ago": None,
        "pivot_count": len(pvts), "direction": "buy", "strategy": "trend_only",
        "vel_current": vel["current_vel"], "vel_avg": vel["avg_velocity"],
        "vel_accel": vel["acceleration"],  "vel_label": vel["accel_label"],
        "amp_avg": amp["avg_amplitude"],   "amp_recent": amp["recent_avg"],
        "amp_regime": amp["regime"],       "amp_variance": amp["variance_pct"],
        "lr_ratio": lr["ratio"],  "lr_recent": lr["recent_ratio"],
        "lr_bull": lr["avg_bull"], "lr_bear": lr["avg_bear"],
        "lr_label": lr["label"],
    }
    result.update(_structure_fields(mo))

    # ── DATA LINEAGE: stamp exact qualification params ──────────────
    result["scan_params"] = {
        "order":         order,
        "order_low":     eff_low,
        "order_mid":     eff_mid,
        "order_high":    eff_high,
        "multi_order":   multi_order,
        "zone_lookback": zone_lookback,
        "legout_mult":   legout_mult,
        "strategy":      strategy,
        "interval":      interval,
    }

    result.update(_debug_pivot_fields(df, mo, pvts, multi_order))
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Per-symbol analysis
# ─────────────────────────────────────────────────────────────────────────────

def analyse_symbol(
    ticker: str,
    df: pd.DataFrame,
    order: int = 5,
    zone_lookback: int = 20,
    direction: str = "buy",
    trend_filter: Optional[Set[str]] = None,
    legout_mult: float = 1.35,
    strategy: str = "atr",
    atr_zone_types: Optional[List[str]] = None,
    multi_order: bool = False,
    order_low:   int = 5,
    order_mid:   int = 10,
    order_high:  int = 20,
    structure_filter_low:  Optional[Set[str]] = None,
    structure_filter_mid:  Optional[Set[str]] = None,
    structure_filter_high: Optional[Set[str]] = None,
    alignment_filter: str = "any",
    interval: str = "1d",  
) -> Optional[Dict[str, Any]]:
    
    try:
        if df is None or len(df) < max(50, order * 4):
            return None

        # ── Build context once — ATR computed here, reused everywhere below ──
        ctx = SymbolContext.build(df)

        # Effective order levels
        eff_low  = order_low  if multi_order else order
        eff_mid  = order_mid  if multi_order else order
        eff_high = order_high if multi_order else order

        # get_multiorder_structure now returns pivots + trend per level
        # (after the change to trend_analysis.py above)
        mo = get_multiorder_structure(
            df, order_low=eff_low, order_mid=eff_mid, order_high=eff_high
        )

        # Reuse pivots at eff_low order — already computed inside mo
        pvts     = mo["low"]["pivots"]    # ← no extra extrems() call
        trend    = mo["low"]["trend"]     # ← no extra detect_trend() call
        strength = pivot_trend_strength(pvts)

        # Trend filter — reads directly from mo, zero extra computation
        if trend_filter and trend not in trend_filter:
            log.debug(
                "Trend filter drop",
                ticker=ticker,
                effective_trend=trend,
                filter=sorted(trend_filter),
            )
            return None  # use authoritative value in result row

        # Apply structure filter
        if multi_order:
            if not structure_matches_filter(
                mo,
                filter_low=structure_filter_low,
                filter_mid=structure_filter_mid,
                filter_high=structure_filter_high,
                alignment_filter=alignment_filter,
            ):
                return None
        else:
            # Single-order: only low filter applies
            if not structure_matches_filter(
                mo, filter_low=structure_filter_low,
                alignment_filter="any",
            ):
                return None

        # Trend only — skip zones
        if strategy == "trend_only":
            return _trend_only_result(
            ticker, df, pvts, trend, strength, mo,
            ctx=ctx,                # ← ADD
            order=order, eff_low=eff_low, eff_mid=eff_mid, eff_high=eff_high,
            multi_order=multi_order, zone_lookback=zone_lookback,
            legout_mult=legout_mult, strategy=strategy, interval=interval,
        )

        # Zone detection
        # Use pre-built context — no recomputation
        atr_val   = ctx.atr_val
        close     = ctx.close
        start_idx = max(0, len(df) - zone_lookback)

        zones = scan_zones(
            df, direction=direction, legout_mult=legout_mult,
            max_base_candles=8, start_idx=start_idx,
            include_atr=(strategy in ("atr", "both")),
            include_consolidation=(strategy in ("consolidation", "both")),
        )

        if atr_zone_types and strategy in ("atr", "both"):
            zones = [
                z for z in zones
                if z.get("zone_type") in atr_zone_types
                or z.get("zone_type") == "consolidation"
            ]

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

        vel = compute_pivot_velocity(pvts)
        amp = compute_pivot_amplitude(pvts)
        lr  = compute_leg_ratio(pvts)

        result = {
            "ticker": ticker, "close": round(close, 2),
            "trend": trend, "trend_strength": strength["classification"],
            "atr": round(atr_val, 2),
            "atr_pct": round(atr_val / close * 100, 2),
            "zones_count": len(valid_zones),
            "zone_high": last_zone["price_high"],
            "zone_low":  last_zone["price_low"],
            "zone_legin_ts":  int(legin_ts.timestamp()),
            "zone_legout_ts": int(legout_ts.timestamp()),
            "zone_type":   last_zone.get("zone_type",  "rbr"),
            "zone_status": last_zone.get("status",     "fresh"),
            "vol_score":   last_zone.get("vol_score",  0.0),
            "bars_ago":    bars_ago, "pivot_count": len(pvts),
            "direction": direction, "strategy": strategy,
            "vel_current": vel["current_vel"], "vel_avg": vel["avg_velocity"],
            "vel_accel": vel["acceleration"],  "vel_label": vel["accel_label"],
            "amp_avg": amp["avg_amplitude"],   "amp_recent": amp["recent_avg"],
            "amp_regime": amp["regime"],       "amp_variance": amp["variance_pct"],
            "lr_ratio": lr["ratio"],  "lr_recent": lr["recent_ratio"],
            "lr_bull": lr["avg_bull"], "lr_bear": lr["avg_bear"],
            "lr_label": lr["label"],
        }
        result.update(_structure_fields(mo))

        # ── DATA LINEAGE: stamp exact qualification params ──────────────
        result["scan_params"] = {
            "order":         order,
            "order_low":     eff_low,
            "order_mid":     eff_mid,
            "order_high":    eff_high,
            "multi_order":   multi_order,
            "zone_lookback": zone_lookback,
            "legout_mult":   legout_mult,
            "strategy":      strategy,
            "interval":      interval,   # ← df doesn't carry interval; see note below
        }

        result.update(_debug_pivot_fields(df, mo, pvts, multi_order))
        return result

    except Exception as e:
        log.error("analyse_symbol exception", ticker=ticker, error=str(e))
        traceback.print_exc()
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Watchlist scan
# ─────────────────────────────────────────────────────────────────────────────

def scan_watchlist(
    tickers: List[str],
    data_dict: Dict[str, pd.DataFrame],
    order: int = 5,
    zone_lookback: int = 20,
    direction: str = "buy",
    trend_filter: Optional[List[str]] = None,
    legout_mult: float = 1.35,
    strategy: str = "atr",
    atr_zone_types: Optional[List[str]] = None,
    multi_order: bool = False,
    order_low: int = 5, order_mid: int = 10, order_high: int = 20,
    structure_filter_low:  Optional[List[str]] = None,
    structure_filter_mid:  Optional[List[str]] = None,
    structure_filter_high: Optional[List[str]] = None,
    alignment_filter: str = "any",
    max_workers: int = 6,
    interval: str = "1d",   
) -> pd.DataFrame:

    tf_set  = set(trend_filter)          if trend_filter          else None
    sf_low  = set(structure_filter_low)  if structure_filter_low  else None
    sf_mid  = set(structure_filter_mid)  if structure_filter_mid  else None
    sf_high = set(structure_filter_high) if structure_filter_high else None

    import time as _time
    t0 = _time.perf_counter()
    log.info(
        "Scan started",
        symbols=len(tickers),
        direction=direction,
        strategy=strategy,
        multi_order=multi_order,
        trend_filter=sorted(trend_filter) if trend_filter else "any",
    )

    if not atr_zone_types:
        atr_zone_types = _DEFAULT_ATR_TYPES.get(direction, ["rbr", "dbr"])

    def _analyse(ticker):
        return analyse_symbol(
            ticker, data_dict.get(ticker),
            order=order, zone_lookback=zone_lookback,
            direction=direction, trend_filter=tf_set,
            legout_mult=legout_mult, strategy=strategy,
            atr_zone_types=atr_zone_types,
            multi_order=multi_order,
            order_low=order_low, order_mid=order_mid, order_high=order_high,
            structure_filter_low=sf_low,
            structure_filter_mid=sf_mid,
            structure_filter_high=sf_high,
            alignment_filter=alignment_filter,
            interval=interval,          
        )

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for fut in as_completed({ex.submit(_analyse, t): t for t in tickers}):
            try:
                res = fut.result()
                if res: results.append(res)
            except Exception:
                pass

    if not results:
        return pd.DataFrame()

    df_out = pd.DataFrame(results)
    sort_col = "atr_pct" if strategy == "trend_only" else "bars_ago"
    df_out.sort_values(sort_col, ascending=True, inplace=True)
    df_out.reset_index(drop=True, inplace=True)

    elapsed = round((_time.perf_counter() - t0) * 1000)
    log.info(
        "Scan complete",
        symbols=len(tickers),
        matched=len(results),
        elapsed_ms=elapsed,
    )

    return df_out