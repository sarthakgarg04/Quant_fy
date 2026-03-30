"""
pivot_engine.py  –  Pivot detection + multi-order trend engine
==============================================================

v2.1 fixes (tail pivot section):
  • tail_start now correctly uses n - order (not n - order*2) so the
    unconfirmed window is exactly the bars the rolling window cannot see.
  • Both a Top AND a Bottom can now be appended from the tail — the old
    code could only ever append one pivot, missing the case where price
    makes a new high then crashes (or vice versa) all within order bars.
  • Alternation is enforced AFTER appending via _enforce_alternation on
    the merged tail candidates, preventing consecutive T→T or B→B.
  • Tail candidates are compared against the correct reference pivot
    (same-type predecessor) rather than just cleaned[-1].price.
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
    """
    Enforce strict T/B alternation. When two consecutive same-type pivots
    appear, keep the more extreme one (higher Top, lower Bottom).
    """
    if not raw:
        return raw
    cleaned: PivotList = [raw[0]]
    for cur_idx, cur_val, cur_type in raw[1:]:
        last_idx, last_val, last_type = cleaned[-1]
        if cur_type == last_type:
            # Same type — keep more extreme
            if cur_type == "T" and cur_val >= last_val:
                cleaned[-1] = (cur_idx, cur_val, cur_type)
            elif cur_type == "B" and cur_val <= last_val:
                cleaned[-1] = (cur_idx, cur_val, cur_type)
        else:
            if abs(cur_val - last_val) >= min_strength:
                cleaned.append((cur_idx, cur_val, cur_type))
    return cleaned


# ─────────────────────────────────────────────────────────────────────────────
# Tail pivot helper  (replaces the old single-append block)
# ─────────────────────────────────────────────────────────────────────────────

def _append_tail_pivots(
    cleaned: PivotList,
    highs: np.ndarray,
    lows: np.ndarray,
    order: int,
    min_strength: float,
) -> PivotList:
    """
    The rolling window with size (2*order+1) and center=True cannot confirm
    any pivot in the last `order` bars.  This function finds the structurally
    significant extremes in that unconfirmed tail and appends them while
    maintaining strict alternation.

    Design rules
    ────────────
    1. Search window: bars [n - order, n)  — exactly the unconfirmed tail.
       We also look back `order` bars before that for context when deciding
       whether an extreme is more extreme than the last confirmed pivot.

    2. Find the highest High and lowest Low in the tail window.

    3. Determine chronological order: which happened later?
       - If tail_high comes after tail_low  → sequence is ...Low then High
       - If tail_low  comes after tail_high → sequence is ...High then Low

    4. Build a candidate list in chronological order, then merge with
       `cleaned` using _enforce_alternation so no T→T or B→B ever results.

    5. At most 2 tail pivots are added (one T + one B is the maximum
       meaningful new structure within a single order-window).

    6. A tail pivot is only accepted if:
       - It would be more extreme than the last confirmed same-type pivot
         (i.e. a new structural high/low, not a noise wiggle), OR
       - It alternates from the last confirmed pivot AND exceeds min_strength.
    """
    if not cleaned:
        return cleaned

    n          = len(highs)
    tail_start = max(0, n - order)          # first unconfirmed bar

    # Nothing to do if the tail window is empty or trivially small
    if tail_start >= n - 1:
        return cleaned

    tail_highs = highs[tail_start:]
    tail_lows  = lows[tail_start:]

    # Absolute extremes in the tail
    tail_high_val = float(tail_highs.max())
    tail_low_val  = float(tail_lows.min())
    # Bar indices are relative to the full array
    tail_high_idx = int(tail_start + int(tail_highs.argmax()))
    tail_low_idx  = int(tail_start + int(tail_lows.argmin()))

    # Last confirmed pivots of each type — used for extremity check
    last_confirmed_top = next(
        ((i, v) for i, v, t in reversed(cleaned) if t == "T"), None
    )
    last_confirmed_bot = next(
        ((i, v) for i, v, t in reversed(cleaned) if t == "B"), None
    )

    # Decide whether each tail extreme qualifies as a new structural pivot
    # A Top qualifies if it is higher than the last confirmed Top
    # A Bottom qualifies if it is lower than the last confirmed Bottom
    # If there is no prior same-type pivot, it always qualifies.
    top_qualifies = (
        last_confirmed_top is None
        or tail_high_val > last_confirmed_top[1]
    )
    bot_qualifies = (
        last_confirmed_bot is None
        or tail_low_val < last_confirmed_bot[1]
    )

    # Also require the tail extreme to differ from the last pivot by min_strength
    last_pivot_val = cleaned[-1][1]
    top_qualifies  = top_qualifies and abs(tail_high_val - last_pivot_val) >= min_strength
    bot_qualifies  = bot_qualifies and abs(tail_low_val  - last_pivot_val) >= min_strength

    # Build candidate list in chronological order
    candidates: PivotList = []

    if top_qualifies and bot_qualifies:
        # Both qualify — order them chronologically
        if tail_high_idx <= tail_low_idx:
            # High came first (or same bar — treat as simultaneous, skip Top)
            if tail_high_idx < tail_low_idx:
                candidates.append((tail_high_idx, tail_high_val, "T"))
            candidates.append((tail_low_idx, tail_low_val, "B"))
        else:
            # Low came first
            candidates.append((tail_low_idx,  tail_low_val,  "B"))
            candidates.append((tail_high_idx, tail_high_val, "T"))

    elif top_qualifies:
        candidates.append((tail_high_idx, tail_high_val, "T"))

    elif bot_qualifies:
        candidates.append((tail_low_idx, tail_low_val, "B"))

    if not candidates:
        return cleaned

    # Merge candidates into cleaned via _enforce_alternation
    merged = _enforce_alternation(cleaned + candidates, min_strength)
    return merged


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def extrems(
    df: pd.DataFrame,
    order: int = 5,
    min_strength: float = 0.0,
    atr_period: int = 14,
    min_strength_atr_mult: float = 0.0,
) -> PivotList:
    """
    Detect pivot highs (T) and lows (B) in df using a rolling-window
    vectorised approach, then append unconfirmed tail pivots.

    Parameters
    ----------
    order               : look-left / look-right bars for pivot confirmation
    min_strength        : minimum price distance between consecutive pivots
    atr_period          : period for ATR used when min_strength_atr_mult > 0
    min_strength_atr_mult : set min_strength = median_ATR × this multiplier
    """
    highs = df["High"].values
    lows  = df["Low"].values

    if min_strength_atr_mult > 0:
        atr          = compute_atr(df, atr_period)
        min_strength = float(atr.median()) * min_strength_atr_mult

    raw     = _local_extremes_vectorised(highs, lows, order)
    cleaned = _enforce_alternation(raw, min_strength)

    # Append unconfirmed tail pivots with proper alternation
    cleaned = _append_tail_pivots(cleaned, highs, lows, order, min_strength)

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

    if len(amps) >= 4:
        x = np.arange(len(amps), dtype=float)
        slope, _, r, _, _ = linregress(x, amps)
        amplitude_trend = round(float(slope), 4)
        norm_slope      = slope / avg_amplitude if avg_amplitude > 0 else 0
    else:
        amplitude_trend = 0.0
        norm_slope      = 0.0
        r               = 0.0

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

    r_bull   = float(np.mean(bull_legs[-2:])) if len(bull_legs) >= 2 else avg_bull
    r_bear   = float(np.mean(bear_legs[-2:])) if len(bear_legs) >= 2 else avg_bear
    r_ratio  = r_bull / r_bear if r_bear > 0 else 1.0

    if   ratio >= 2.0:                        label = "bull_dominating"
    elif ratio >= 1.3:                        label = "bull_favoured"
    elif ratio >= 0.77:
        if   r_ratio > ratio * 1.2:           label = "bull_recovering"
        elif r_ratio < ratio * 0.8:           label = "bear_recovering"
        else:                                 label = "balanced"
    elif ratio >= 0.5:                        label = "bear_favoured"
    else:                                     label = "bear_dominating"

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
# Multi-Order Trend
# ─────────────────────────────────────────────────────────────────────────────

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

TREND_GROUPS = {
    "any":            None,
    "any_uptrend":    UPTREND_LABELS,
    "any_downtrend":  DOWNTREND_LABELS,
    "any_neutral":    NEUTRAL_LABELS,
}


def _trend_matches(actual: str, filter_val: str) -> bool:
    if not filter_val or filter_val == "any":
        return True
    if filter_val in TREND_GROUPS:
        group = TREND_GROUPS[filter_val]
        return group is None or actual in group
    return actual == filter_val


def _alignment_score(trends: List[str]) -> int:
    up   = sum(1 for t in trends if t in UPTREND_LABELS)
    dn   = sum(1 for t in trends if t in DOWNTREND_LABELS)
    return max(up, dn)


def _combined_trend_label(
    trends: List[str],
    alignment: int,
    n: int,
) -> str:
    if alignment == n:
        if all(t == "strong_uptrend"      for t in trends): return "strong_uptrend"
        if all(t in UPTREND_LABELS        for t in trends): return "continuous_uptrend"
        if all(t in DOWNTREND_LABELS      for t in trends): return "continuous_downtrend"
        if all(t == "strong_downtrend"    for t in trends): return "strong_downtrend"
        return trends[-1]

    if alignment == n - 1:
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
    """
    result = {}

    for level, order in [("low", order_low), ("mid", order_mid), ("high", order_high)]:
        pvts = extrems(df, order=order, min_strength=min_strength)
        tr   = detect_trend(pvts)

        tops    = [(i, v) for i, v, t in pvts if t == "T"]
        bottoms = [(i, v) for i, v, t in pvts if t == "B"]
        ts   = _weighted_sequence_score(tops,    n=4)
        bs   = _weighted_sequence_score(bottoms, n=4)
        score = (ts + bs) / 2.0

        vel  = compute_pivot_velocity(pvts)
        amp  = compute_pivot_amplitude(pvts)
        lr   = compute_leg_ratio(pvts)

        result[level] = {
            "order":     order,
            "trend":     tr,
            "score":     round(score, 3),
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
