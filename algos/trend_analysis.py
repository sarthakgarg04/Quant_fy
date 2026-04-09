"""
trend_analysis.py  –  Pivot structure state engine
====================================================
Pure pivot-based structural classification.
No scoring, no weights — raw structure only.

Public API
----------
get_pivot_structure(pivots)
    → StructureResult dict for one order level

get_multiorder_structure(df, order_low, order_mid, order_high)
    → dict with low / mid / high StructureResults + alignment

structure_matches_filter(mo_structure, filter_low, filter_mid,
                         filter_high, alignment_filter)
    → bool  used by scanner.py

Constants
---------
ALL_STATES        full list of all 22 state strings
BULLISH_STATES    set of bullish state strings
BEARISH_STATES    set of bearish state strings
NEUTRAL_STATES    set of neutral state strings
STATE_LABELS      human-readable label per state  (for UI)
STATE_GROUPS      grouped dict for dropdown rendering

Convention
----------
Two separate transition maps are used depending on which pivot type
is most recent in the pivot list:

  last pivot = Bottom  →  curr_pair = "LAST_TOP_LABEL+LAST_BOT_LABEL"
                           uses _TRANSITION_MAP_BOT_END

  last pivot = Top     →  curr_pair = "LAST_BOT_LABEL+LAST_TOP_LABEL"
                           uses _TRANSITION_MAP_TOP_END

This ensures curr_pair always describes the most recently completed
swing, not an artificially aligned index pairing.
"""

from __future__ import annotations

from typing import List, Tuple, Dict, Any, Optional, Set
import pandas as pd

from algos.pivot_engine import extrems, detect_trend, PivotList


# ─────────────────────────────────────────────────────────────────────────────
# State constants
# ─────────────────────────────────────────────────────────────────────────────

# Continuation
TRENDING_UP               = "trending_up"
TRENDING_DOWN             = "trending_down"

# From uptrend — deteriorating / reversing
TOP_COIL                  = "top_coil"                  # uptrend → tops rolling, base holding (pullback setup)
TOP_EXPANDING             = "top_expanding"             # uptrend → top held HH after base broke LL (expansion, bullish recovery)
TOP_EXPANDING_BREAKDOWN   = "top_expanding_breakdown"   # uptrend → base cracked LL (BOT_END) or full flip (TOP_END)
TOP_PULLBACK_BREAKDOWN    = "top_pullback_breakdown"    # uptrend → full flip, both sides bearish

# From downtrend — recovering / reversing
BOTTOM_COIL               = "bottom_coil"               # downtrend → base holding, tops still falling (early reversal)
BOTTOM_EXPANDING          = "bottom_expanding"          # downtrend → top broke earlier, base confirmed LL again
BOTTOM_EXPANDING_BREAKOUT = "bottom_expanding_breakout" # downtrend → top just broke HH (TOP_END) or full flip (BOT_END)
BOTTOM_PULLBACK_BREAKOUT  = "bottom_pullback_breakout"  # downtrend → full flip, both sides bullish

# From expanding
EXPANDING_TO_UP           = "expanding_to_up"
EXPANDING_TO_DOWN         = "expanding_to_down"
EXPANSION_COIL            = "expansion_coil"            # expanding tightened into a coil

# From coiling
COILING_TO_UP             = "coiling_to_up"
COILING_TO_DOWN           = "coiling_to_down"
COILING_EXPAND            = "coiling_expand"            # coil expanded instead of resolving directionally

# Fallback states  (curr_pair only — not enough pivots for transition)
STRUCTURE_UP              = "structure_up"
STRUCTURE_DOWN            = "structure_down"
STRUCTURE_EXPAND          = "structure_expanding"
STRUCTURE_COIL            = "structure_coiling"

# Not enough pivots
NO_STRUCTURE              = "no_structure"


ALL_STATES: List[str] = [
    # Continuation
    TRENDING_UP, TRENDING_DOWN,
    # From uptrend
    TOP_COIL, TOP_EXPANDING, TOP_EXPANDING_BREAKDOWN, TOP_PULLBACK_BREAKDOWN,
    # From downtrend
    BOTTOM_COIL, BOTTOM_EXPANDING, BOTTOM_EXPANDING_BREAKOUT, BOTTOM_PULLBACK_BREAKOUT,
    # Expanding
    EXPANDING_TO_UP, EXPANDING_TO_DOWN, EXPANSION_COIL,
    # Coiling
    COILING_TO_UP, COILING_TO_DOWN, COILING_EXPAND,
    # Fallback
    STRUCTURE_UP, STRUCTURE_DOWN, STRUCTURE_EXPAND, STRUCTURE_COIL,
]

BULLISH_STATES: Set[str] = {
    TRENDING_UP,
    TOP_COIL,               # pullback setup within uptrend
    TOP_EXPANDING,          # uptrend recovered — top held HH after base scare
    BOTTOM_EXPANDING_BREAKOUT,
    BOTTOM_PULLBACK_BREAKOUT,
    COILING_TO_UP,
    EXPANDING_TO_UP,
    STRUCTURE_UP,
}

BEARISH_STATES: Set[str] = {
    TRENDING_DOWN,
    TOP_EXPANDING_BREAKDOWN,
    TOP_PULLBACK_BREAKDOWN,
    BOTTOM_COIL,            # base forming in downtrend — still bearish context
    BOTTOM_EXPANDING,       # downtrend expanding — base confirmed LL again
    COILING_TO_DOWN,
    EXPANDING_TO_DOWN,
    STRUCTURE_DOWN,
}

NEUTRAL_STATES: Set[str] = {
    EXPANSION_COIL,
    COILING_EXPAND,
    STRUCTURE_EXPAND,
    STRUCTURE_COIL,
}

# Human-readable labels for the UI dropdowns
STATE_LABELS: Dict[str, str] = {
    TRENDING_UP:               "Trending Up",
    TRENDING_DOWN:             "Trending Down",
    TOP_COIL:                  "Top Coil",
    TOP_EXPANDING:             "Top Expanding",
    TOP_EXPANDING_BREAKDOWN:   "Top Expanding Breakdown",
    TOP_PULLBACK_BREAKDOWN:    "Top Pullback Breakdown",
    BOTTOM_COIL:               "Bottom Coil",
    BOTTOM_EXPANDING:          "Bottom Expanding",
    BOTTOM_EXPANDING_BREAKOUT: "Bottom Expanding Breakout",
    BOTTOM_PULLBACK_BREAKOUT:  "Bottom Pullback Breakout",
    EXPANDING_TO_UP:           "Expanding → Up",
    EXPANDING_TO_DOWN:         "Expanding → Down",
    EXPANSION_COIL:            "Expansion Coil",
    COILING_TO_UP:             "Coiling → Up",
    COILING_TO_DOWN:           "Coiling → Down",
    COILING_EXPAND:            "Coiling Expand",
    STRUCTURE_UP:              "Structure Up",
    STRUCTURE_DOWN:            "Structure Down",
    STRUCTURE_EXPAND:          "Expanding",
    STRUCTURE_COIL:            "Coiling",
    NO_STRUCTURE:              "No Structure",
}

# Grouped for UI rendering — group_name → [state, ...]
STATE_GROUPS: Dict[str, List[str]] = {
    "Bullish – Continuation": [TRENDING_UP],
    "Bullish – Reversal":     [BOTTOM_EXPANDING_BREAKOUT, BOTTOM_PULLBACK_BREAKOUT],
    "Bullish – Setup":        [TOP_COIL, TOP_EXPANDING, COILING_TO_UP, EXPANDING_TO_UP, STRUCTURE_UP],
    "Bearish – Continuation": [TRENDING_DOWN],
    "Bearish – Reversal":     [TOP_EXPANDING_BREAKDOWN, TOP_PULLBACK_BREAKDOWN],
    "Bearish – Setup":        [BOTTOM_COIL, BOTTOM_EXPANDING, COILING_TO_DOWN, EXPANDING_TO_DOWN, STRUCTURE_DOWN],
    "Neutral":                [EXPANSION_COIL, COILING_EXPAND, STRUCTURE_EXPAND, STRUCTURE_COIL],
}


# ─────────────────────────────────────────────────────────────────────────────
# Transition maps
# ─────────────────────────────────────────────────────────────────────────────

# When last pivot is Bottom: curr_pair = "LAST_TOP_LABEL+LAST_BOT_LABEL"
_TRANSITION_MAP_BOT_END: Dict[Tuple[str, str], str] = {

    # ── From uptrend HH+HL ─────────────────────────────────────────────────────
    ("HH+HL", "HH+HL"): TRENDING_UP,
    ("HH+HL", "LH+HL"): TOP_COIL,                  # tops rolling, bottom JUST held HL — pullback setup
    ("HH+HL", "HH+LL"): TOP_EXPANDING_BREAKDOWN,   # top still HH, bottom JUST broke LL — base cracking
    ("HH+HL", "LH+LL"): TOP_PULLBACK_BREAKDOWN,    # both flipped — full reversal from uptrend

    # ── From downtrend LH+LL ───────────────────────────────────────────────────
    ("LH+LL", "LH+LL"): TRENDING_DOWN,
    ("LH+LL", "LH+HL"): BOTTOM_COIL,               # top still LH, bottom JUST held HL — base forming
    ("LH+LL", "HH+LL"): BOTTOM_EXPANDING,          # top broke HH earlier, bottom JUST confirmed LL again
    ("LH+LL", "HH+HL"): BOTTOM_EXPANDING_BREAKOUT, # both flipped — full reversal from downtrend

    # ── From expanding HH+LL ───────────────────────────────────────────────────
    ("HH+LL", "HH+HL"): EXPANDING_TO_UP,           # bottom JUST held HL — bullish resolution
    ("HH+LL", "LH+LL"): EXPANDING_TO_DOWN,         # top rolled LH, bottom JUST confirmed LL — bearish resolution
    ("HH+LL", "HH+LL"): STRUCTURE_EXPAND,          # bottom JUST confirmed LL again — still expanding
    ("HH+LL", "LH+HL"): EXPANSION_COIL,            # both sides pulled back — expansion into coil

    # ── From coiling LH+HL ─────────────────────────────────────────────────────
    ("LH+HL", "HH+HL"): COILING_TO_UP,             # bottom JUST held HL, top already broke HH — bullish
    ("LH+HL", "LH+LL"): COILING_TO_DOWN,           # top still LH, bottom JUST broke LL — bearish resolution
    ("LH+HL", "LH+HL"): STRUCTURE_COIL,            # bottom JUST held HL again — coil continuing
    ("LH+HL", "HH+LL"): COILING_EXPAND,            # both sides broke out of coil — expanding
}

# When last pivot is Top: curr_pair = "LAST_BOT_LABEL+LAST_TOP_LABEL"
_TRANSITION_MAP_TOP_END: Dict[Tuple[str, str], str] = {

    # ── From uptrend HL+HH ─────────────────────────────────────────────────────
    ("HL+HH", "HL+HH"): TRENDING_UP,
    ("HL+HH", "HL+LH"): TOP_COIL,                  # bottom held HL, top JUST failed as LH — pullback setup
    ("HL+HH", "LL+HH"): TOP_EXPANDING,             # bottom broke LL earlier, top JUST confirmed HH again — bullish recovery
    ("HL+HH", "LL+LH"): TOP_EXPANDING_BREAKDOWN,   # bottom already LL, top JUST confirmed LH — full flip

    # ── From downtrend LL+LH ───────────────────────────────────────────────────
    ("LL+LH", "LL+LH"): TRENDING_DOWN,
    ("LL+LH", "HL+LH"): BOTTOM_COIL,               # bottom made HL earlier, top JUST confirmed LH still — base forming
    ("LL+LH", "LL+HH"): BOTTOM_EXPANDING_BREAKOUT, # bottom still LL, top JUST broke out as HH — first breakout signal
    ("LL+LH", "HL+HH"): BOTTOM_PULLBACK_BREAKOUT,  # bottom already HL, top JUST made HH — full flip

    # ── From expanding LL+HH ───────────────────────────────────────────────────
    ("LL+HH", "HL+HH"): EXPANDING_TO_UP,           # top JUST made HH, bottom already HL — bullish resolution
    ("LL+HH", "LL+LH"): EXPANDING_TO_DOWN,         # top JUST rolled to LH, bottom still LL — bearish resolution
    ("LL+HH", "LL+HH"): STRUCTURE_EXPAND,          # top JUST made HH again, bottom still LL — still expanding
    ("LL+HH", "HL+LH"): EXPANSION_COIL,            # top JUST rolled LH, bottom already HL — expansion into coil

    # ── From coiling HL+LH ─────────────────────────────────────────────────────
    ("HL+LH", "HL+HH"): COILING_TO_UP,             # top JUST broke out as HH, bottom already HL — bullish resolution
    ("HL+LH", "LL+LH"): COILING_TO_DOWN,           # top JUST confirmed LH, bottom already broke LL — bearish resolution
    ("HL+LH", "HL+LH"): STRUCTURE_COIL,            # top JUST confirmed LH again — coil continuing
    ("HL+LH", "LL+HH"): COILING_EXPAND,            # top JUST broke HH, bottom already LL — coil expanded
}


# Fallback maps — only one pair available (not enough pivots for transition)
_SINGLE_PAIR_MAP_BOT_END: Dict[str, str] = {
    "HH+HL": STRUCTURE_UP,
    "LH+LL": STRUCTURE_DOWN,
    "HH+LL": STRUCTURE_EXPAND,
    "LH+HL": STRUCTURE_COIL,
}

_SINGLE_PAIR_MAP_TOP_END: Dict[str, str] = {
    "HL+HH": STRUCTURE_UP,
    "LL+LH": STRUCTURE_DOWN,
    "LL+HH": STRUCTURE_EXPAND,
    "HL+LH": STRUCTURE_COIL,
}


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _tops(pivots: PivotList) -> List[Tuple[int, float]]:
    return [(i, v) for i, v, t in pivots if t == "T"]

def _bots(pivots: PivotList) -> List[Tuple[int, float]]:
    return [(i, v) for i, v, t in pivots if t == "B"]

def _label_seq(
    seq: List[Tuple[int, float]],
    bull_lbl: str,
    bear_lbl: str,
) -> List[str]:
    """
    Label each element relative to the previous one.
    Returns list of length len(seq)-1.
    Equal is treated as bear (not higher = not a HH/HL).
    """
    labels = []
    for k in range(1, len(seq)):
        labels.append(bull_lbl if seq[k][1] > seq[k-1][1] else bear_lbl)
    return labels


# ─────────────────────────────────────────────────────────────────────────────
# Core: single-order structure
# ─────────────────────────────────────────────────────────────────────────────

def get_pivot_structure(pivots: PivotList) -> Dict[str, Any]:
    """
    Derive structural state from a pivot list.

    Convention
    ----------
    curr_pair is always built to reflect which pivot type is most recent:

      last pivot = Bottom  →  curr_pair = f"{last_top_label}+{last_bot_label}"
                               uses _TRANSITION_MAP_BOT_END

      last pivot = Top     →  curr_pair = f"{last_bot_label}+{last_top_label}"
                               uses _TRANSITION_MAP_TOP_END

    prev_pair uses the same convention (second-to-last labels in the same order).

    Returns
    -------
    {
      state           : str    one of ALL_STATES or NO_STRUCTURE
      state_label     : str    human-readable
      is_transition   : bool   True if a full transition state was matched
      curr_pair       : str    e.g. "HH+HL"  (None if no_structure)
      prev_pair       : str    e.g. "LH+HL"  (None if no transition)
      last_pivot_type : str    "T" or "B"
      top_labels      : list[str]
      bot_labels      : list[str]
      n_tops          : int
      n_bots          : int
      pivot_count     : int
    }
    """
    tops     = _tops(pivots)
    bots     = _bots(pivots)
    n_tops   = len(tops)
    n_bots   = len(bots)
    n_pivots = len(pivots)

    _empty = {
        "state":           NO_STRUCTURE,
        "state_label":     STATE_LABELS[NO_STRUCTURE],
        "is_transition":   False,
        "curr_pair":       None,
        "prev_pair":       None,
        "last_pivot_type": None,
        "top_labels":      [],
        "bot_labels":      [],
        "n_tops":          n_tops,
        "n_bots":          n_bots,
        "pivot_count":     n_pivots,
    }

    if n_tops < 2 or n_bots < 2:
        return _empty

    top_labels = _label_seq(tops, "HH", "LH")   # length = n_tops - 1
    bot_labels = _label_seq(bots, "HL", "LL")   # length = n_bots - 1

    if not top_labels or not bot_labels:
        return _empty

    last_pivot_type = pivots[-1][2]   # "T" or "B"
    last_top = top_labels[-1]
    last_bot = bot_labels[-1]

    if last_pivot_type == "B":
        curr_pair       = f"{last_top}+{last_bot}"
        transition_map  = _TRANSITION_MAP_BOT_END
        single_pair_map = _SINGLE_PAIR_MAP_BOT_END
        prev_top = top_labels[-2] if len(top_labels) >= 2 else None
        prev_bot = bot_labels[-2] if len(bot_labels) >= 2 else None
        prev_pair = f"{prev_top}+{prev_bot}" if (prev_top and prev_bot) else None
    else:
        curr_pair       = f"{last_bot}+{last_top}"
        transition_map  = _TRANSITION_MAP_TOP_END
        single_pair_map = _SINGLE_PAIR_MAP_TOP_END
        prev_bot = bot_labels[-2] if len(bot_labels) >= 2 else None
        prev_top = top_labels[-2] if len(top_labels) >= 2 else None
        prev_pair = f"{prev_bot}+{prev_top}" if (prev_bot and prev_top) else None

    state         = NO_STRUCTURE
    is_transition = False

    if prev_pair:
        looked_up = transition_map.get((prev_pair, curr_pair))
        if looked_up is not None:
            state         = looked_up
            is_transition = True

    if not is_transition:
        state = single_pair_map.get(curr_pair, NO_STRUCTURE)

    return {
        "state":           state,
        "state_label":     STATE_LABELS.get(state, state),
        "is_transition":   is_transition,
        "curr_pair":       curr_pair,
        "prev_pair":       prev_pair,
        "last_pivot_type": last_pivot_type,
        "top_labels":      top_labels,
        "bot_labels":      bot_labels,
        "n_tops":          n_tops,
        "n_bots":          n_bots,
        "pivot_count":     n_pivots,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Multi-order structure
# ─────────────────────────────────────────────────────────────────────────────

def get_multiorder_structure(
    df: pd.DataFrame,
    order_low:    int   = 5,
    order_mid:    int   = 10,
    order_high:   int   = 20,
    min_strength: float = 0.0,
) -> Dict[str, Any]:
    """
    Run get_pivot_structure at three order levels and compute alignment.

    Returns
    -------
    {
      low  : StructureResult + {"order": int, "pivots": list, "trend": str}
      mid  : StructureResult + {"order": int, "pivots": list, "trend": str}
      high : StructureResult + {"order": int, "pivots": list, "trend": str}
      alignment: {
        states        : [low_state, mid_state, high_state]
        bull_count    : int
        bear_count    : int
        neutral_count : int
        all_match     : bool
        majority      : "bullish"|"bearish"|"neutral"|"mixed"
      }
    }
    """
    results: Dict[str, Any] = {}
    for level, order in [
        ("low",  order_low),
        ("mid",  order_mid),
        ("high", order_high),
    ]:
        pvts           = extrems(df, order=order, min_strength=min_strength)
        s              = get_pivot_structure(pvts)
        s["order"]     = order
        s["pivots"]    = pvts
        s["trend"]     = detect_trend(pvts)
        results[level] = s

    states = [
        results["low"]["state"],
        results["mid"]["state"],
        results["high"]["state"],
    ]

    bull_count    = sum(1 for s in states if s in BULLISH_STATES)
    bear_count    = sum(1 for s in states if s in BEARISH_STATES)
    neutral_count = sum(1 for s in states if s in NEUTRAL_STATES)
    all_match     = len(set(states)) == 1

    if   bull_count > bear_count:  majority = "bullish"
    elif bear_count > bull_count:  majority = "bearish"
    elif neutral_count == 3:       majority = "neutral"
    else:                          majority = "mixed"

    results["alignment"] = {
        "states":        states,
        "bull_count":    bull_count,
        "bear_count":    bear_count,
        "neutral_count": neutral_count,
        "all_match":     all_match,
        "majority":      majority,
    }

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Filter helper — used by scanner.py
# ─────────────────────────────────────────────────────────────────────────────

def structure_matches_filter(
    mo_structure:     Dict[str, Any],
    filter_low:       Optional[Set[str]] = None,
    filter_mid:       Optional[Set[str]] = None,
    filter_high:      Optional[Set[str]] = None,
    alignment_filter: str = "any",
) -> bool:
    """
    Returns True if the multi-order structure passes all active filters.

    Parameters
    ----------
    mo_structure        : output of get_multiorder_structure()
    filter_low/mid/high : set of allowed state strings.
                          None or empty set = no filter (pass all).
    alignment_filter    : "any"           — no alignment requirement
                          "majority_bull" — bull_count > bear_count
                          "majority_bear" — bear_count > bull_count
                          "all_match"     — all three orders same state

    All active filters are AND logic.
    """
    def _passes(level: str, filt: Optional[Set[str]]) -> bool:
        if not filt:
            return True
        return mo_structure[level]["state"] in filt

    if not _passes("low",  filter_low):  return False
    if not _passes("mid",  filter_mid):  return False
    if not _passes("high", filter_high): return False

    aln = mo_structure["alignment"]
    if alignment_filter == "majority_bull" and aln["majority"] != "bullish": return False
    if alignment_filter == "majority_bear" and aln["majority"] != "bearish": return False
    if alignment_filter == "all_match"     and not aln["all_match"]:         return False

    return True
