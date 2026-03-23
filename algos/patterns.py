"""
patterns.py  –  Chart pattern detection
=========================================
Consolidates: flag.py, triangle_pattern.py, pivot_strategies.py
  • detect_flag()
  • detect_triangle()
  • pivot_channel()
  • pivot_momentum_divergence()
  • equal_pivots_liquidity_sweep()
  • pivot_trend_strength()
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import linregress
from typing import List, Dict, Any, Tuple, Optional

from algos.pivot_engine import PivotList, extrems


# ──────────────────────────────────────────────────────────────────────────────
# Flag pattern
# ──────────────────────────────────────────────────────────────────────────────

def detect_flag(
    df: pd.DataFrame,
    order: int = 10,
    backcandles: int = 100,
    window: int = 1,
    k: int = 3,
    r2_min: float = 0.9,
) -> bool:
    """
    Returns True when a flag/pennant consolidation is detected in the
    last `backcandles` bars.  Uses wick-based extremes.
    """
    from algos.pivot_engine import extrems as _extrems

    pvts = _extrems(df, order=order)
    candle = len(df)

    highs_x, highs_y, lows_x, lows_y = [], [], [], []
    for idx, val, typ in pvts:
        if idx > candle - backcandles - window:
            if typ == "T":
                highs_x.append(idx); highs_y.append(val)
            else:
                lows_x.append(idx);  lows_y.append(val)

    if len(highs_x) < k or len(lows_x) < k:
        return False

    hx, hy = np.array(highs_x[-k:-1]), np.array(highs_y[-k:-1])
    lx, ly = np.array(lows_x[-k:-1]),  np.array(lows_y[-k:-1])

    if len(hx) < 2 or len(lx) < 2:
        return False

    slmax, _, rmax, _, _ = linregress(hx, hy)
    slmin, _, rmin, _, _ = linregress(lx, ly)

    return (
        rmax ** 2 >= r2_min
        and rmin ** 2 >= r2_min
        and slmin >= 0.0001
        and slmax <= -0.0001
    )


# ──────────────────────────────────────────────────────────────────────────────
# Triangle pattern
# ──────────────────────────────────────────────────────────────────────────────

def detect_triangle(
    df: pd.DataFrame,
    order: int = 5,
    window: int = 6,
    r2_min: float = 0.9,
    lower_slope: float = 0.0,
    upper_slope: float = 0.0,
) -> bool:
    """
    Symmetric / ascending / descending triangle detection.
    lower_slope > 0 = ascending; upper_slope < 0 = descending.
    """
    pvts = extrems(df, order=order)
    n = len(pvts)
    if n < window:
        return False

    for i in range(n - window):
        subset = pvts[i: i + window]
        hx = [p[0] for p in subset if p[2] == "T"]
        hy = [p[1] for p in subset if p[2] == "T"]
        lx = [p[0] for p in subset if p[2] == "B"]
        ly = [p[1] for p in subset if p[2] == "B"]

        if len(hx) < 2 or len(lx) < 2:
            continue

        slmax, _, rmax, _, _ = linregress(hx, hy)
        slmin, _, rmin, _, _ = linregress(lx, ly)

        if (
            rmax ** 2 >= r2_min
            and rmin ** 2 >= r2_min
            and slmin > lower_slope
            and slmax < upper_slope
        ):
            return True

    return False


# ──────────────────────────────────────────────────────────────────────────────
# Channel trading
# ──────────────────────────────────────────────────────────────────────────────

def pivot_channel(
    df: pd.DataFrame,
    pivots: PivotList,
    lookback_pairs: int = 2,
    parallel_tol: float = 0.20,
    touch_tol_pct: float = 0.005,
) -> List[Dict[str, Any]]:
    """
    Detect and classify price channels from pivot regression lines.
    """
    signals: List[Dict] = []
    if len(pivots) < 4:
        return signals

    highs = [(i, v) for i, v, t in pivots if t == "T"]
    lows  = [(i, v) for i, v, t in pivots if t == "B"]

    if len(highs) < lookback_pairs or len(lows) < lookback_pairs:
        return signals

    sel_h = highs[-lookback_pairs:]
    sel_l = lows[-lookback_pairs:]

    # Use integer bar index (not timestamp) for regression
    xh = np.array([i for i, _ in sel_h], dtype=float)
    yh = np.array([v for _, v in sel_h])
    xl = np.array([i for i, _ in sel_l], dtype=float)
    yl = np.array([v for _, v in sel_l])

    m_h, c_h = np.polyfit(xh, yh, 1)
    m_l, c_l = np.polyfit(xl, yl, 1)

    slope_diff = abs(m_h - m_l) / (abs(m_h) + 1e-8)
    is_parallel = slope_diff <= parallel_tol

    if m_h > 0 and m_l > 0:   channel_type = "uptrend"
    elif m_h < 0 and m_l < 0: channel_type = "downtrend"
    else:                      channel_type = "sideways"

    last_idx  = len(df) - 1
    last_close = df["Close"].iloc[-1]
    upper_now  = m_h * last_idx + c_h
    lower_now  = m_l * last_idx + c_l
    width      = max(upper_now - lower_now, 1e-8)
    tol        = touch_tol_pct * width

    tu = sum(abs(v - (m_h * i + c_h)) <= tol for i, v in highs)
    tl = sum(abs(v - (m_l * i + c_l)) <= tol for i, v in lows)
    total_touches = tu + tl
    quality = (1 - min(slope_diff, 1.0)) * 0.5 + min(total_touches, 6) / 6 * 0.5

    base = {
        "channel_type": channel_type, "is_parallel": is_parallel,
        "quality": round(quality, 3),
        "upper_slope": m_h, "upper_intercept": c_h,
        "lower_slope": m_l, "lower_intercept": c_l,
        "upper": upper_now, "lower": lower_now,
        "touches_upper": tu, "touches_lower": tl,
        "selected_highs": sel_h, "selected_lows": sel_l,
        "timestamp": df.index[-1], "price": last_close,
    }

    if abs(last_close - upper_now) <= tol:
        signals.append({**base, "type": "touch_upper"})
    if abs(last_close - lower_now) <= tol:
        signals.append({**base, "type": "touch_lower"})
    if last_close > upper_now + tol:
        signals.append({**base, "type": "breakout_upper"})
    if last_close < lower_now - tol:
        signals.append({**base, "type": "breakout_lower"})
    if total_touches >= 3:
        signals.append({**base, "type": "third_touch_confirm"})

    return signals


# ──────────────────────────────────────────────────────────────────────────────
# Momentum divergence
# ──────────────────────────────────────────────────────────────────────────────

def pivot_momentum_divergence(
    pivots: PivotList,
    min_cycles: int = 3,
) -> List[Dict[str, Any]]:
    """
    Pure-geometry divergence detection (no oscillator needed).
    """
    signals = []
    bottoms = [(i, v) for i, v, t in pivots if t == "B"]
    tops    = [(i, v) for i, v, t in pivots if t == "T"]

    if len(tops) < min_cycles or len(bottoms) < min_cycles:
        return signals

    t1, t2, t3 = tops[-3], tops[-2], tops[-1]
    b1, b2, b3 = bottoms[-3], bottoms[-2], bottoms[-1]

    d_prev, d_last = t2[1] - t1[1], t3[1] - t2[1]
    hl_prev, hl_last = b2[1] - b1[1], b3[1] - b2[1]

    if t3[1] > t2[1] > t1[1] and d_last < d_prev * 0.8:
        signals.append({"type": "bearish_hh_momentum_divergence",
                         "tops": [t1,t2,t3], "metric": {"d_prev":d_prev,"d_last":d_last}})
    if b3[1] > b2[1] > b1[1] and hl_last < hl_prev * 0.7:
        signals.append({"type": "bearish_hl_weakening",
                         "bottoms": [b1,b2,b3]})

    dp_d = b1[1] - b2[1]; dl_d = b2[1] - b3[1]
    if b3[1] < b2[1] < b1[1] and dl_d < dp_d * 0.8:
        signals.append({"type": "bullish_ll_momentum_divergence",
                         "bottoms": [b1,b2,b3]})
    lh_p = t1[1] - t2[1]; lh_l = t2[1] - t3[1]
    if t1[1] > t2[1] > t3[1] and lh_l < lh_p * 0.7:
        signals.append({"type": "bullish_lh_weakening",
                         "tops": [t1,t2,t3]})

    return signals


# ──────────────────────────────────────────────────────────────────────────────
# Liquidity sweep
# ──────────────────────────────────────────────────────────────────────────────

def equal_pivots_liquidity_sweep(
    pivots: PivotList,
    tol_pct: float = 0.002,
) -> List[Dict[str, Any]]:
    signals = []
    for typ in ("T", "B"):
        vals = [(i, v) for i, v, t in pivots if t == typ]
        if len(vals) < 2:
            continue
        cluster = [vals[0]]
        for cur in vals[1:]:
            prev = cluster[-1]
            if abs(cur[1] - prev[1]) / max(prev[1], 1e-9) <= tol_pct:
                cluster.append(cur)
            else:
                if len(cluster) >= 2:
                    c_min = min(v for _, v in cluster)
                    c_max = max(v for _, v in cluster)
                    lcp = cluster[-1][0]
                    after = [e for e in pivots if e[0] > lcp]
                    for j in range(len(after) - 1):
                        if typ == "T" and after[j][1] > c_max and after[j+1][1] <= c_max:
                            signals.append({"type":"liquidity_sweep_top","cluster":cluster,"breaker":after[j],"reentry":after[j+1]})
                            break
                        elif typ == "B" and after[j][1] < c_min and after[j+1][1] >= c_min:
                            signals.append({"type":"liquidity_sweep_bottom","cluster":cluster,"breaker":after[j],"reentry":after[j+1]})
                            break
                cluster = [cur]
    return signals


# ──────────────────────────────────────────────────────────────────────────────
# Trend strength score
# ──────────────────────────────────────────────────────────────────────────────

def pivot_trend_strength(
    pivots: PivotList,
    lookback_cycles: int = 3,
) -> Dict[str, Any]:
    need = lookback_cycles * 2
    if len(pivots) < need:
        return {"score": 0.0, "classification": "no-trend"}

    recent = pivots[-need:]
    amps  = [abs(recent[k+1][1] - recent[k][1]) for k in range(len(recent)-1)]
    times = [max(1, abs(recent[k+1][0] - recent[k][0])) for k in range(len(recent)-1)]

    avg_amp  = float(np.mean(amps))
    avg_time = float(np.mean(times))
    score    = avg_amp / avg_time
    median_a = float(np.median(amps))

    if score > 1.5 * median_a:   cls = "strong"
    elif score < 0.5 * median_a: cls = "weak"
    else:                         cls = "moderate"

    return {"score": score, "classification": cls,
            "avg_amp": avg_amp, "avg_time": avg_time}
