"""
pivot_engine.py  –  Pivot detection + multi-order trend engine
==============================================================

New in v2:
  • detect_trend()          — weighted scoring (fixes single-bounce misclassification)
  • compute_pivot_velocity()— velocity + acceleration per swing leg
  • compute_pivot_amplitude()— swing sizes, variance, regime detection
  • compute_leg_ratio()     — bull/bear leg symmetry (trend quality)
  • detect_trend_multiorder()— runs all three orders, combines with alignment score
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import linregress
from typing import List, Tuple, Optional, Dict, Any

PivotList = List[Tuple[int, float, str]]   # (bar_index, price, "T"|"B")


# ─────────────────────────────────────────────────────────────────────────────
# ATR
# ─────────────────────────────────────────────────────────────────────────────

def compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Wilder's ATR."""
    h, l, pc = df["High"], df["Low"], df["Close"].shift(1)
    tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1.0 / period, adjust=False).mean()


# ─────────────────────────────────────────────────────────────────────────────
# Vectorised pivot detection
# ─────────────────────────────────────────────────────────────────────────────

def _local_extremes_vectorised(
    highs: np.ndarray,
    lows: np.ndarray,
    order: int,
) -> PivotList:
    n   = len(highs)
    win = 2 * order + 1

    roll_max = pd.Series(highs).rolling(win, center=True, min_periods=win).max().values
    roll_min = pd.Series(lows).rolling(win,  center=True, min_periods=win).min().values

    raw: PivotList = []
    for i in range(order, n - order):
        is_top    = highs[i] == roll_max[i]
        is_bottom = lows[i]  == roll_min[i]
        if is_top and not is_bottom:
            raw.append((i, float(highs[i]), "T"))
        elif is_bottom and not is_top:
            raw.append((i, float(lows[i]),  "B"))
    return raw


def _enforce_alternation(raw: PivotList, min_strength: float = 0.0) -> PivotList:
    if not raw:
        return raw
    cleaned: PivotList = [raw[0]]
    for cur_idx, cur_val, cur_type in raw[1:]:
        last_idx, last_val, last_type = cleaned[-1]
        if cur_type == last_type:
            if cur_type == "T" and cur_val >= last_val:
                cleaned[-1] = (cur_idx, cur_val, cur_type)
            elif cur_type == "B" and cur_val <= last_val:
                cleaned[-1] = (cur_idx, cur_val, cur_type)
        else:
            if abs(cur_val - last_val) >= min_strength:
                cleaned.append((cur_idx, cur_val, cur_type))
    return cleaned


def extrems(
    df: pd.DataFrame,
    order: int = 5,
    min_strength: float = 0.0,
    atr_period: int = 14,
    min_strength_atr_mult: float = 0.0,
) -> PivotList:
    highs = df["High"].values
    lows  = df["Low"].values

    if min_strength_atr_mult > 0:
        atr          = compute_atr(df, atr_period)
        min_strength = float(atr.median()) * min_strength_atr_mult

    raw     = _local_extremes_vectorised(highs, lows, order)
    cleaned = _enforce_alternation(raw, min_strength)

    # Force-include tail pivot (last `order` bars can't be confirmed by rolling)
    n          = len(highs)
    tail_start = max(0, n - order * 2)
    tail_highs = highs[tail_start:]
    tail_lows  = lows[tail_start:]

    if len(cleaned) >= 2 and len(tail_highs) > 0:
        last_type = cleaned[-1][2]
        last_val  = cleaned[-1][1]

        tail_high_val = float(tail_highs.max())
        tail_low_val  = float(tail_lows.min())
        tail_high_idx = int(tail_start + tail_highs.argmax())
        tail_low_idx  = int(tail_start + tail_lows.argmin())

        if last_type == "T" and tail_low_val < last_val:
            cleaned.append((tail_low_idx, tail_low_val, "B"))
        elif last_type == "B" and tail_high_val > last_val:
            cleaned.append((tail_high_idx, tail_high_val, "T"))

    return cleaned


# ─────────────────────────────────────────────────────────────────────────────
# Trend detection  —  weighted scoring (v2)
# ─────────────────────────────────────────────────────────────────────────────

def _weighted_sequence_score(seq: list, n: int = 4) -> float:
    """
    Score last n pivots with exponential recency weighting.
    Returns normalised score in [-1.0, +1.0].
    +1 = all higher (uptrend), -1 = all lower (downtrend).
    """
    recent = seq[-(n + 1):]
    if len(recent) < 2:
        return 0.0

    total_w  = 0.0
    w_score  = 0.0
    for k in range(len(recent) - 1):
        weight    = 2 ** k          # older = lower weight
        direction = (1  if recent[k + 1][1] > recent[k][1] else
                    -1  if recent[k + 1][1] < recent[k][1] else 0)
        w_score  += direction * weight
        total_w  += weight

    return w_score / total_w if total_w > 0 else 0.0


def _consolidation_type(pivots: PivotList) -> str:
    """Classify the neutral zone into consolidation sub-type."""
    if len(pivots) < 4:
        return "consolidate"

    vals  = [v for _, v, _ in pivots[-6:]]
    rng   = max(vals) - min(vals)
    avg   = sum(vals) / len(vals)

    # Amplitude trend — is range expanding or contracting?
    amps = []
    tops    = [(i, v) for i, v, t in pivots if t == "T"]
    bottoms = [(i, v) for i, v, t in pivots if t == "B"]
    for k in range(min(len(tops), len(bottoms), 3) - 1):
        amps.append(abs(tops[-(k+1)][1] - bottoms[-(k+1)][1]))

    if len(amps) >= 2:
        if amps[-1] > amps[0] * 1.3:
            return "expanding_structure"
        if amps[-1] < amps[0] * 0.6:
            return "deep_consolidation"

    if avg > 0 and (rng / avg) < 0.02:
        return "flat_consolidation"

    return "consolidate"


def detect_trend(pivots: PivotList) -> str:
    """
    Classify market structure using weighted pivot scoring.

    Uses exponential recency weighting across last 4 tops + 4 bottoms
    so a single higher-low bounce doesn't override 3 prior lower-highs.
    """
    if len(pivots) < 4:
        return "NA"

    tops    = [(i, v) for i, v, t in pivots if t == "T"]
    bottoms = [(i, v) for i, v, t in pivots if t == "B"]

    if len(tops) < 2 or len(bottoms) < 2:
        return "sw"

    top_score    = _weighted_sequence_score(tops,    n=4)
    bottom_score = _weighted_sequence_score(bottoms, n=4)
    combined     = (top_score + bottom_score) / 2.0

    # Leg ratio for trend quality
    try:
        lr = compute_leg_ratio(pivots)
        ratio = lr["ratio"]
    except Exception:
        ratio = 1.0

    # ── Classification ────────────────────────────────────────────────
    if combined >= 0.70:
        return "strong_uptrend"      if ratio >= 1.8  else "continuous_uptrend"
    elif combined >= 0.40:
        return "continuous_uptrend"  if ratio >= 1.4  else "up"
    elif combined >= 0.15:
        return "up"                  if ratio >= 1.1  else "weakening_uptrend"
    elif combined >= 0.05:
        return "semi-up"             if ratio >= 1.0  else "weakening_uptrend"
    elif combined >= -0.05:
        return _consolidation_type(pivots)
    elif combined >= -0.15:
        return "weakening_downtrend" if ratio >= 0.9  else "down"
    elif combined >= -0.40:
        return "down"                if ratio <= 0.8  else "weakening_downtrend"
    elif combined >= -0.70:
        return "continuous_downtrend" if ratio <= 0.7 else "down"
    else:
        return "strong_downtrend"    if ratio <= 0.55 else "continuous_downtrend"


# ─────────────────────────────────────────────────────────────────────────────
# Velocity  —  rate of change of pivot structure
# ─────────────────────────────────────────────────────────────────────────────

def compute_pivot_velocity(pivots: PivotList) -> Dict[str, Any]:
    """
    Measure how fast price is moving between pivots.

    Returns
    -------
    {
      legs          : list of { from, to, direction, pts_per_bar, bars }
      avg_velocity  : float   average abs pts/bar across all legs
      current_vel   : float   most recent leg velocity
      prev_vel      : float   second most recent leg velocity
      acceleration  : float   current_vel - prev_vel (+ = speeding up)
      accel_label   : str     'accelerating' | 'decelerating' | 'steady'
      trend_vel     : float   net signed velocity of current trend direction
    }
    """
    if len(pivots) < 3:
        return {
            "legs": [], "avg_velocity": 0.0,
            "current_vel": 0.0, "prev_vel": 0.0,
            "acceleration": 0.0, "accel_label": "steady",
            "trend_vel": 0.0,
        }

    legs = []
    for k in range(len(pivots) - 1):
        i0, v0, t0 = pivots[k]
        i1, v1, t1 = pivots[k + 1]
        bars         = max(i1 - i0, 1)
        pts_per_bar  = (v1 - v0) / bars
        legs.append({
            "from":        k,
            "to":          k + 1,
            "direction":   "up" if v1 > v0 else "down",
            "pts_per_bar": round(pts_per_bar, 4),
            "abs_vel":     round(abs(pts_per_bar), 4),
            "bars":        bars,
            "amplitude":   round(abs(v1 - v0), 4),
        })

    abs_vels     = [l["abs_vel"] for l in legs]
    avg_velocity = float(np.mean(abs_vels)) if abs_vels else 0.0
    current_vel  = legs[-1]["abs_vel"]  if legs     else 0.0
    prev_vel     = legs[-2]["abs_vel"]  if len(legs) >= 2 else 0.0

    acceleration = current_vel - prev_vel
    if   acceleration >  avg_velocity * 0.15: accel_label = "accelerating"
    elif acceleration < -avg_velocity * 0.15: accel_label = "decelerating"
    else:                                      accel_label = "steady"

    # Net velocity in dominant trend direction (signed)
    trend_vel = float(np.mean([l["pts_per_bar"] for l in legs[-4:]]) if legs else 0.0)

    return {
        "legs":         legs,
        "avg_velocity": round(avg_velocity, 4),
        "current_vel":  round(current_vel,  4),
        "prev_vel":     round(prev_vel,     4),
        "acceleration": round(acceleration, 4),
        "accel_label":  accel_label,
        "trend_vel":    round(trend_vel,    4),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Amplitude  —  swing size analysis
# ─────────────────────────────────────────────────────────────────────────────

def compute_pivot_amplitude(pivots: PivotList) -> Dict[str, Any]:
    """
    Analyse swing amplitudes (absolute price distance between consecutive pivots).

    Returns
    -------
    {
      amplitudes      : list[float]  all swing sizes
      avg_amplitude   : float
      recent_avg      : float        last 4 swings avg
      amplitude_trend : float        slope of amplitude over time (+= expanding)
      regime          : str          'expanding' | 'contracting' | 'stable' | 'choppy'
      variance_pct    : float        coefficient of variation (consistency)
    }
    """
    if len(pivots) < 3:
        return {
            "amplitudes": [], "avg_amplitude": 0.0,
            "recent_avg": 0.0, "amplitude_trend": 0.0,
            "regime": "stable", "variance_pct": 0.0,
        }

    amps = [abs(pivots[k+1][1] - pivots[k][1]) for k in range(len(pivots) - 1)]

    avg_amplitude = float(np.mean(amps))
    recent_avg    = float(np.mean(amps[-4:])) if len(amps) >= 4 else float(np.mean(amps))
    variance_pct  = float(np.std(amps) / avg_amplitude * 100) if avg_amplitude > 0 else 0.0

    # Amplitude trend — linear slope over swing index
    if len(amps) >= 4:
        x = np.arange(len(amps), dtype=float)
        slope, _, r, _, _ = linregress(x, amps)
        amplitude_trend = round(float(slope), 4)
        norm_slope      = slope / avg_amplitude if avg_amplitude > 0 else 0
    else:
        amplitude_trend = 0.0
        norm_slope      = 0.0
        r               = 0.0

    # Regime classification
    if   variance_pct > 60:               regime = "choppy"
    elif norm_slope >  0.05 and r > 0.4:  regime = "expanding"
    elif norm_slope < -0.05 and r > 0.4:  regime = "contracting"
    else:                                  regime = "stable"

    return {
        "amplitudes":      [round(a, 2) for a in amps],
        "avg_amplitude":   round(avg_amplitude,  2),
        "recent_avg":      round(recent_avg,      2),
        "amplitude_trend": amplitude_trend,
        "regime":          regime,
        "variance_pct":    round(variance_pct,    1),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Leg Ratio  —  bull vs bear leg symmetry
# ─────────────────────────────────────────────────────────────────────────────

def compute_leg_ratio(pivots: PivotList) -> Dict[str, Any]:
    """
    Compare the magnitude of bullish legs (B→T) vs bearish legs (T→B).

    ratio > 1.0  →  bulls dominating (up moves > down moves)
    ratio < 1.0  →  bears dominating
    ratio ≈ 1.0  →  balanced / consolidation

    Returns
    -------
    {
      bull_legs     : list[float]   B→T amplitudes
      bear_legs     : list[float]   T→B amplitudes
      avg_bull      : float
      avg_bear      : float
      ratio         : float         avg_bull / avg_bear
      recent_ratio  : float         last 2 bull / last 2 bear
      label         : str           'bull_dominating' | 'bear_dominating' |
                                    'balanced' | 'bull_weakening' | 'bear_weakening'
    }
    """
    bull_legs = []
    bear_legs = []

    for k in range(len(pivots) - 1):
        _, v0, t0 = pivots[k]
        _, v1, t1 = pivots[k + 1]
        amp = abs(v1 - v0)
        if t0 == "B" and t1 == "T":
            bull_legs.append(amp)
        elif t0 == "T" and t1 == "B":
            bear_legs.append(amp)

    avg_bull = float(np.mean(bull_legs)) if bull_legs else 0.0
    avg_bear = float(np.mean(bear_legs)) if bear_legs else 0.0
    ratio    = avg_bull / avg_bear if avg_bear > 0 else 1.0

    # Recent ratio — last 2 of each
    r_bull   = float(np.mean(bull_legs[-2:])) if len(bull_legs) >= 2 else avg_bull
    r_bear   = float(np.mean(bear_legs[-2:])) if len(bear_legs) >= 2 else avg_bear
    r_ratio  = r_bull / r_bear if r_bear > 0 else 1.0

    # Label
    if   ratio >= 2.0:                        label = "bull_dominating"
    elif ratio >= 1.3:                        label = "bull_favoured"
    elif ratio >= 0.77:
        if   r_ratio > ratio * 1.2:           label = "bull_recovering"
        elif r_ratio < ratio * 0.8:           label = "bear_recovering"
        else:                                 label = "balanced"
    elif ratio >= 0.5:                        label = "bear_favoured"
    else:                                     label = "bear_dominating"

    # Weakening checks — recent ratio diverging from overall
    if label in ("bull_dominating", "bull_favoured") and r_ratio < ratio * 0.7:
        label = "bull_weakening"
    if label in ("bear_dominating", "bear_favoured") and r_ratio > ratio * 1.4:
        label = "bear_weakening"

    return {
        "bull_legs":    [round(x, 2) for x in bull_legs],
        "bear_legs":    [round(x, 2) for x in bear_legs],
        "avg_bull":     round(avg_bull,  2),
        "avg_bear":     round(avg_bear,  2),
        "ratio":        round(ratio,     3),
        "recent_ratio": round(r_ratio,   3),
        "label":        label,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Multi-Order Trend  —  core new function
# ─────────────────────────────────────────────────────────────────────────────

# Trend group mappings for filter matching
UPTREND_LABELS    = frozenset({
    "strong_uptrend", "continuous_uptrend", "up",
    "semi-up", "weakening_uptrend", "bullish_mitigation",
})
DOWNTREND_LABELS  = frozenset({
    "strong_downtrend", "continuous_downtrend", "down", "weakening_downtrend",
})
NEUTRAL_LABELS    = frozenset({
    "consolidate", "flat_consolidation", "deep_consolidation",
    "sw", "expanding_structure",
})

# Filter group aliases — what "any_uptrend" expands to
TREND_GROUPS = {
    "any":            None,             # no filter
    "any_uptrend":    UPTREND_LABELS,
    "any_downtrend":  DOWNTREND_LABELS,
    "any_neutral":    NEUTRAL_LABELS,
}


def _trend_matches(actual: str, filter_val: str) -> bool:
    """Check if actual trend satisfies the filter setting."""
    if not filter_val or filter_val == "any":
        return True
    if filter_val in TREND_GROUPS:
        group = TREND_GROUPS[filter_val]
        return group is None or actual in group
    # Exact label match
    return actual == filter_val


def _alignment_score(trends: List[str]) -> int:
    """
    Count how many of the given trend labels agree on direction.
    Returns 0-N where N = number of orders.
    """
    up   = sum(1 for t in trends if t in UPTREND_LABELS)
    dn   = sum(1 for t in trends if t in DOWNTREND_LABELS)
    return max(up, dn)


def _combined_trend_label(
    trends: List[str],
    alignment: int,
    n: int,
) -> str:
    """
    Derive a single combined trend label from multiple order trends.
    """
    if alignment == n:
        # Full agreement — use most conservative (weakest) common label
        if all(t == "strong_uptrend"      for t in trends): return "strong_uptrend"
        if all(t in UPTREND_LABELS        for t in trends): return "continuous_uptrend"
        if all(t in DOWNTREND_LABELS      for t in trends): return "continuous_downtrend"
        if all(t == "strong_downtrend"    for t in trends): return "strong_downtrend"
        return trends[-1]   # fallback: use lowest order (most current)

    if alignment == n - 1:
        # Majority agreement
        up = sum(1 for t in trends if t in UPTREND_LABELS)
        dn = sum(1 for t in trends if t in DOWNTREND_LABELS)
        if up > dn: return "up"
        if dn > up: return "down"

    return "transitioning"


def detect_trend_multiorder(
    df: pd.DataFrame,
    order_low:  int = 5,
    order_mid:  int = 10,
    order_high: int = 20,
    min_strength: float = 0.0,
) -> Dict[str, Any]:
    """
    Run pivot detection at three orders and combine into a multi-order
    trend assessment.

    Returns
    -------
    {
      low:  { order, trend, pivots, velocity, amplitude, leg_ratio, score }
      mid:  { ... }
      high: { ... }
      alignment:      int   (0-3, how many orders agree on direction)
      combined_trend: str
    }
    """
    result = {}

    for level, order in [("low", order_low), ("mid", order_mid), ("high", order_high)]:
        pvts = extrems(df, order=order, min_strength=min_strength)
        tr   = detect_trend(pvts)

        # Weighted score for display bar width
        tops    = [(i, v) for i, v, t in pvts if t == "T"]
        bottoms = [(i, v) for i, v, t in pvts if t == "B"]
        ts   = _weighted_sequence_score(tops,    n=4)
        bs   = _weighted_sequence_score(bottoms, n=4)
        score = (ts + bs) / 2.0   # -1 to +1

        vel  = compute_pivot_velocity(pvts)
        amp  = compute_pivot_amplitude(pvts)
        lr   = compute_leg_ratio(pvts)

        result[level] = {
            "order":     order,
            "trend":     tr,
            "score":     round(score, 3),   # -1.0 to +1.0
            "velocity":  vel,
            "amplitude": amp,
            "leg_ratio": lr,
            "pivot_count": len(pvts),
        }

    trends    = [result["low"]["trend"],
                 result["mid"]["trend"],
                 result["high"]["trend"]]
    alignment = _alignment_score(trends)
    combined  = _combined_trend_label(trends, alignment, 3)

    result["alignment"]      = alignment
    result["combined_trend"] = combined

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Helpers used elsewhere
# ─────────────────────────────────────────────────────────────────────────────

def _split(pivots: PivotList):
    bottoms = [(i, v) for i, v, t in pivots if t == "B"]
    tops    = [(i, v) for i, v, t in pivots if t == "T"]
    return bottoms, tops


def trend_analyser(df: pd.DataFrame, order: int = 5,
                   min_strength: float = 0.0) -> str:
    return detect_trend(extrems(df, order=order, min_strength=min_strength))


def bottom_turn(df: pd.DataFrame, order: int = 5) -> int:
    pvts    = extrems(df, order=order)
    if len(pvts) < 8:
        return 0
    ups     = {"continuous_uptrend", "strong_uptrend", "up"}
    current = detect_trend(pvts[-6:])
    prev    = detect_trend(pvts[:-2])
    return 1 if current in ups and prev not in ups else 0


def tag_signals(
    df: pd.DataFrame,
    pivots: PivotList,
    horizons=(5, 10, 20),
) -> pd.DataFrame:
    close = df["Close"].values
    rows  = []
    for idx, val, typ in pivots:
        row = {"bar_idx": idx, "price": val, "type": typ, "date": df.index[idx]}
        for h in horizons:
            fi = idx + h
            if fi < len(close):
                fwd = (close[fi] - close[idx]) / close[idx]
                row[f"fwd_{h}"] = fwd * (1 if typ == "B" else -1)
            else:
                row[f"fwd_{h}"] = np.nan
        rows.append(row)
    return pd.DataFrame(rows)
