"""
confluence.py  –  Signal scoring, HTF context, position sizing
===============================================================
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Dict, Any, Optional, List, Tuple

from algos.pivot_engine import extrems, detect_trend, compute_atr, tag_signals
from algos.zones import buy_zone, sell_zone


# ──────────────────────────────────────────────────────────────────────────────
# HTF context
# ──────────────────────────────────────────────────────────────────────────────

class HTFContext:
    """
    Precompute weekly/daily pivot structure and trend for a symbol.
    Used to filter LTF entries.
    """
    def __init__(self, df_weekly: pd.DataFrame, df_daily: pd.DataFrame, order: int = 5):
        self.weekly_pivots = extrems(df_weekly, order=order)
        self.daily_pivots  = extrems(df_daily,  order=order)
        self.weekly_trend  = detect_trend(self.weekly_pivots)
        self.daily_trend   = detect_trend(self.daily_pivots)
        self.weekly_atr    = float(compute_atr(df_weekly).iloc[-1])
        self.daily_atr     = float(compute_atr(df_daily).iloc[-1])

    @property
    def is_bullish(self) -> bool:
        up = {"strong_uptrend", "continuous_uptrend", "up", "weakening_uptrend"}
        return self.weekly_trend in up or self.daily_trend in up

    @property
    def is_bearish(self) -> bool:
        dn = {"strong_downtrend", "continuous_downtrend", "down", "weakening_downtrend"}
        return self.weekly_trend in dn or self.daily_trend in dn

    def nearest_resistance(self) -> Optional[float]:
        tops = [(i, v) for i, v, t in self.weekly_pivots if t == "T"]
        if not tops:
            return None
        return tops[-1][1]

    def nearest_support(self) -> Optional[float]:
        bots = [(i, v) for i, v, t in self.weekly_pivots if t == "B"]
        if not bots:
            return None
        return bots[-1][1]


# ──────────────────────────────────────────────────────────────────────────────
# Confluence scorer
# ──────────────────────────────────────────────────────────────────────────────

_TREND_SCORE: Dict[str, float] = {
    "strong_uptrend":        1.0,
    "continuous_uptrend":    0.85,
    "up":                    0.70,
    "weakening_uptrend":     0.50,
    "consolidate":           0.40,
    "flat_consolidation":    0.40,
    "deep_consolidation":    0.35,
    "sw":                    0.30,
    "bullish_mitigation":    0.55,
    "semi-up":               0.55,
    "expanding_structure":   0.25,
    "weakening_downtrend":   0.20,
    "down":                  0.15,
    "continuous_downtrend":  0.10,
    "strong_downtrend":      0.05,
    "NA":                    0.30,
}


def confluence_score(
    df: pd.DataFrame,
    order: int = 5,
    htf: Optional[HTFContext] = None,
    direction: str = "buy",
) -> Dict[str, Any]:
    """
    Composite score [0..1] for entry quality.

    Components
    ----------
    trend_score     0.35  weight
    zone_quality    0.25
    htf_alignment   0.25
    vol_regime      0.15
    """
    pvts  = extrems(df, order=order)
    trend = detect_trend(pvts)
    ts    = _TREND_SCORE.get(trend, 0.3)

    # Zone quality: was a zone found in last 10% of bars?
    n = len(df)
    start = max(0, n - n // 5)
    zones, _ = buy_zone(df, start, "NA") if direction == "buy" else (sell_zone(df, start), 0)
    zone_score = min(len(zones) / 3.0, 1.0) * 0.8 + (0.2 if zones else 0.0)

    # HTF alignment
    htf_score = 0.5
    if htf is not None:
        if direction == "buy"  and htf.is_bullish: htf_score = 1.0
        elif direction == "sell" and htf.is_bearish: htf_score = 1.0
        elif direction == "buy"  and htf.is_bearish: htf_score = 0.1
        elif direction == "sell" and htf.is_bullish: htf_score = 0.1

    # Volatility regime: low ATR% = cleaner signals
    atr  = compute_atr(df)
    atr_pct = float(atr.iloc[-1] / df["Close"].iloc[-1])
    vol_score = 1.0 - min(atr_pct / 0.04, 1.0)  # 4% ATR/price = worst case

    score = (ts * 0.35 + zone_score * 0.25 + htf_score * 0.25 + vol_score * 0.15)

    return {
        "score":        round(score, 3),
        "trend":        trend,
        "trend_score":  round(ts, 3),
        "zone_score":   round(zone_score, 3),
        "htf_score":    round(htf_score, 3),
        "vol_score":    round(vol_score, 3),
        "atr_pct":      round(atr_pct * 100, 2),
        "zones_found":  len(zones),
    }


# ──────────────────────────────────────────────────────────────────────────────
# ATR-based position sizing
# ──────────────────────────────────────────────────────────────────────────────

def atr_position_size(
    capital: float,
    risk_pct: float,          # fraction of capital to risk per trade (e.g. 0.01 = 1%)
    entry: float,
    stop: float,              # stop-loss price
    atr: float,
    atr_stop_mult: float = 2.0,
    max_position_pct: float = 0.10,  # max 10% of capital in one trade
) -> Dict[str, float]:
    """
    Compute position size using ATR-based risk.

    If a stop price is provided, it overrides ATR stop.
    """
    risk_per_unit = abs(entry - stop) if stop else atr * atr_stop_mult
    risk_per_unit = max(risk_per_unit, 1e-8)

    risk_amount = capital * risk_pct
    units       = risk_amount / risk_per_unit
    position_val = units * entry

    # Cap at max_position_pct
    max_val  = capital * max_position_pct
    if position_val > max_val:
        units        = max_val / entry
        position_val = max_val

    return {
        "units":         round(units, 2),
        "position_value": round(position_val, 2),
        "risk_amount":    round(risk_amount, 2),
        "risk_per_unit":  round(risk_per_unit, 4),
        "capital_pct":    round(position_val / capital * 100, 2),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Edge measurement (walk-forward friendly)
# ──────────────────────────────────────────────────────────────────────────────

def measure_edge(
    tagged: pd.DataFrame,       # output of tag_signals()
    horizons: Tuple[int, ...] = (5, 10, 20),
) -> pd.DataFrame:
    """
    Summarise forward-return stats per pivot type.
    Returns a DataFrame with hit_rate, avg_R, payoff_ratio per horizon.
    """
    rows = []
    for typ in ("B", "T"):
        sub = tagged[tagged["type"] == typ]
        for h in horizons:
            col = f"fwd_{h}"
            if col not in sub.columns:
                continue
            s = sub[col].dropna()
            if len(s) == 0:
                continue
            wins = (s > 0).sum()
            rows.append({
                "type":        typ,
                "horizon":     h,
                "n":           len(s),
                "hit_rate":    round(wins / len(s), 3),
                "avg_R":       round(s.mean() * 100, 3),
                "avg_win":     round(s[s > 0].mean() * 100, 3) if wins else 0,
                "avg_loss":    round(s[s < 0].mean() * 100, 3) if (s <= 0).any() else 0,
                "payoff":      round(abs(s[s > 0].mean() / s[s < 0].mean()), 3)
                               if wins and (s <= 0).any() else np.nan,
            })
    return pd.DataFrame(rows)
