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
ALL_STATES        full list of all 16 state strings
BULLISH_STATES    set of bullish state strings
BEARISH_STATES    set of bearish state strings
NEUTRAL_STATES    set of neutral state strings
STATE_LABELS      human-readable label per state  (for UI)
STATE_GROUPS      grouped dict for dropdown rendering
"""

from __future__ import annotations

from typing import List, Tuple, Dict, Any, Optional, Set
import pandas as pd

from algos.pivot_engine import extrems, PivotList


# ─────────────────────────────────────────────────────────────────────────────
# State constants
# ─────────────────────────────────────────────────────────────────────────────

# Transition states  (prev_pair → curr_pair, need 3T + 3B = 6+ pivots)
TRENDING_UP       = "trending_up"
TOPPING           = "topping"
TOP_BREAKING      = "top_breaking"
BREAKDOWN         = "breakdown"

TRENDING_DOWN     = "trending_down"
BOTTOMING         = "bottoming"
BOTTOM_BREAKING   = "bottom_breaking"
BREAKOUT          = "breakout"

EXPANDING_TO_UP   = "expanding_to_up"
EXPANDING_TO_DOWN = "expanding_to_down"
COILING_TO_UP     = "coiling_to_up"
COILING_TO_DOWN   = "coiling_to_down"

# Fallback states  (curr_pair only, need 2T + 2B = 4+ pivots)
STRUCTURE_UP      = "structure_up"
STRUCTURE_DOWN    = "structure_down"
STRUCTURE_EXPAND  = "structure_expanding"
STRUCTURE_COIL    = "structure_coiling"

# Not enough pivots
NO_STRUCTURE      = "no_structure"

ALL_STATES: List[str] = [
    TRENDING_UP, BREAKOUT, COILING_TO_UP, EXPANDING_TO_UP,
    BOTTOMING, BOTTOM_BREAKING,
    TOPPING, TOP_BREAKING,
    TRENDING_DOWN, BREAKDOWN, COILING_TO_DOWN, EXPANDING_TO_DOWN,
    STRUCTURE_UP, STRUCTURE_DOWN, STRUCTURE_EXPAND, STRUCTURE_COIL,
]

BULLISH_STATES: Set[str] = {
    TRENDING_UP, BREAKOUT, COILING_TO_UP, EXPANDING_TO_UP,
    BOTTOMING, BOTTOM_BREAKING, STRUCTURE_UP,
}
BEARISH_STATES: Set[str] = {
    TRENDING_DOWN, BREAKDOWN, COILING_TO_DOWN, EXPANDING_TO_DOWN,
    TOPPING, TOP_BREAKING, STRUCTURE_DOWN,
}
NEUTRAL_STATES: Set[str] = {
    STRUCTURE_EXPAND, STRUCTURE_COIL,
}

# Human-readable labels for the UI dropdowns
STATE_LABELS: Dict[str, str] = {
    TRENDING_UP:       "Trending Up",
    TOPPING:           "Topping",
    TOP_BREAKING:      "Top Breaking",
    BREAKDOWN:         "Breakdown",
    TRENDING_DOWN:     "Trending Down",
    BOTTOMING:         "Bottoming",
    BOTTOM_BREAKING:   "Bottom Breaking",
    BREAKOUT:          "Breakout",
    EXPANDING_TO_UP:   "Expanding → Up",
    EXPANDING_TO_DOWN: "Expanding → Down",
    COILING_TO_UP:     "Coiling → Up",
    COILING_TO_DOWN:   "Coiling → Down",
    STRUCTURE_UP:      "Structure Up",
    STRUCTURE_DOWN:    "Structure Down",
    STRUCTURE_EXPAND:  "Expanding",
    STRUCTURE_COIL:    "Coiling",
    NO_STRUCTURE:      "No Structure",
}

# Grouped for UI rendering — group_name → [state, ...]
STATE_GROUPS: Dict[str, List[str]] = {
    "Bullish – Continuation": [TRENDING_UP],
    "Bullish – Reversal":     [BREAKOUT, BOTTOM_BREAKING, BOTTOMING],
    "Bullish – Setup":        [COILING_TO_UP, EXPANDING_TO_UP, STRUCTURE_UP],
    "Bearish – Continuation": [TRENDING_DOWN],
    "Bearish – Reversal":     [BREAKDOWN, TOP_BREAKING, TOPPING],
    "Bearish – Setup":        [COILING_TO_DOWN, EXPANDING_TO_DOWN, STRUCTURE_DOWN],
    "Neutral":                [STRUCTURE_EXPAND, STRUCTURE_COIL],
}

# Transition lookup: (prev_pair, curr_pair) → state
# Pairs are encoded as "TOP_LABEL+BOT_LABEL" e.g. "HH+HL"
_TRANSITION_MAP: Dict[Tuple[str, str], str] = {
    # From uptrend (HH+HL)
    ("HH+HL", "HH+HL"): TRENDING_UP,
    ("HH+HL", "HH+LL"): TOPPING,
    ("HH+HL", "LH+HL"): TOP_BREAKING,
    ("HH+HL", "LH+LL"): BREAKDOWN,

    # From downtrend (LH+LL)
    ("LH+LL", "LH+LL"): TRENDING_DOWN,
    ("LH+LL", "LH+HL"): BOTTOMING,
    ("LH+LL", "HH+LL"): BOTTOM_BREAKING,
    ("LH+LL", "HH+HL"): BREAKOUT,

    # From expanding (HH+LL)
    ("HH+LL", "HH+HL"): EXPANDING_TO_UP,
    ("HH+LL", "LH+LL"): EXPANDING_TO_DOWN,
    ("HH+LL", "HH+LL"): STRUCTURE_EXPAND,
    ("HH+LL", "LH+HL"): STRUCTURE_COIL,

    # From coiling (LH+HL)
    ("LH+HL", "HH+HL"): COILING_TO_UP,
    ("LH+HL", "LH+LL"): COILING_TO_DOWN,
    ("LH+HL", "LH+HL"): STRUCTURE_COIL,
    ("LH+HL", "HH+LL"): STRUCTURE_EXPAND,
}

# Single-pair fallback map: curr_pair → state
_SINGLE_PAIR_MAP: Dict[str, str] = {
    "HH+HL": STRUCTURE_UP,
    "LH+LL": STRUCTURE_DOWN,
    "HH+LL": STRUCTURE_EXPAND,
    "LH+HL": STRUCTURE_COIL,
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
    Derive the structural state for one pivot list.

    Logic
    -----
    1. Split pivots into Tops and Bottoms.
    2. Label each sequence: HH/LH for Tops, HL/LL for Bottoms.
    3. Pair labels by index (nth Top label paired with nth Bottom label).
    4. If 2+ label pairs exist: look up (prev_pair, curr_pair) in
       _TRANSITION_MAP → transition state (12 options).
    5. If only 1 label pair: look up curr_pair in _SINGLE_PAIR_MAP
       → fallback state (4 options).
    6. If < 2 Tops or < 2 Bottoms: NO_STRUCTURE.

    Returns
    -------
    {
      state          : str    one of ALL_STATES or NO_STRUCTURE
      state_label    : str    human-readable
      is_transition  : bool   True if a full transition state was matched
      curr_pair      : str    e.g. "HH+HL"  (None if no_structure)
      prev_pair      : str    e.g. "HH+LL"  (None if no transition)
      top_labels     : list[str]   all HH/LH labels oldest→newest
      bot_labels     : list[str]   all HL/LL labels oldest→newest
      last_top_label : str | None
      last_bot_label : str | None
      n_tops         : int
      n_bots         : int
      pivot_count    : int
    }
    """
    tops     = _tops(pivots)
    bots     = _bots(pivots)
    n_tops   = len(tops)
    n_bots   = len(bots)
    n_pivots = len(pivots)

    _empty = {
        "state":          NO_STRUCTURE,
        "state_label":    STATE_LABELS[NO_STRUCTURE],
        "is_transition":  False,
        "curr_pair":      None,
        "prev_pair":      None,
        "top_labels":     [],
        "bot_labels":     [],
        "last_top_label": None,
        "last_bot_label": None,
        "n_tops":         n_tops,
        "n_bots":         n_bots,
        "pivot_count":    n_pivots,
    }

    if n_tops < 2 or n_bots < 2:
        return _empty

    top_labels = _label_seq(tops, "HH", "LH")
    bot_labels = _label_seq(bots, "HL", "LL")

    # Align into pairs by index (Option A — deterministic)
    # n_pairs = number of complete pairs both sequences provide
    n_pairs = min(len(top_labels), len(bot_labels))
    if n_pairs < 1:
        return _empty

    last_top   = top_labels[-1]
    last_bot   = bot_labels[-1]
    curr_pair  = f"{last_top}+{last_bot}"

    state:        str  = NO_STRUCTURE
    prev_pair:    Optional[str] = None
    is_transition: bool = False

    # Attempt transition state
    if n_pairs >= 2:
        prev_top  = top_labels[-(n_pairs)]     if n_pairs <= len(top_labels) else top_labels[-2]
        prev_bot  = bot_labels[-(n_pairs)]     if n_pairs <= len(bot_labels) else bot_labels[-2]
        # Use the second-to-last aligned pair
        aligned_top_labels = top_labels[:n_pairs]
        aligned_bot_labels = bot_labels[:n_pairs]
        prev_top  = aligned_top_labels[-2]
        prev_bot  = aligned_bot_labels[-2]
        prev_pair = f"{prev_top}+{prev_bot}"
        looked_up = _TRANSITION_MAP.get((prev_pair, curr_pair))
        if looked_up is not None:
            state         = looked_up
            is_transition = True

    # Fallback to single-pair state
    if not is_transition:
        state = _SINGLE_PAIR_MAP.get(curr_pair, NO_STRUCTURE)

    return {
        "state":          state,
        "state_label":    STATE_LABELS.get(state, state),
        "is_transition":  is_transition,
        "curr_pair":      curr_pair,
        "prev_pair":      prev_pair,
        "top_labels":     top_labels,
        "bot_labels":     bot_labels,
        "last_top_label": last_top,
        "last_bot_label": last_bot,
        "n_tops":         n_tops,
        "n_bots":         n_bots,
        "pivot_count":    n_pivots,
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
      low  : StructureResult + {"order": int}
      mid  : StructureResult + {"order": int}
      high : StructureResult + {"order": int}
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
        pvts            = extrems(df, order=order, min_strength=min_strength)
        s               = get_pivot_structure(pvts)
        s["order"]      = order
        results[level]  = s

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
    mo_structure      : output of get_multiorder_structure()
    filter_low/mid/high : set of allowed state strings.
                          None or empty set = no filter (pass all).
    alignment_filter  : "any"           — no alignment requirement
                        "majority_bull" — bull_count > bear_count
                        "majority_bear" — bear_count > bull_count
                        "all_match"     — all three orders same state

    All active filters are AND logic.
    """
    def _passes(level: str, filt: Optional[Set[str]]) -> bool:
        if not filt:   # None or empty = no filter
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
