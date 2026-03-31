"""
zones.py  –  Zone detection engine v2
=======================================
Three complementary methods:

  1. _zone_scanner()          — ATR-body method (RBR + DBR)
                                Legin → base candles → legout
                                Fixed: base band is now relative to base candle
                                ranges, not legin body. Counts actual candles.

  2. detect_consolidation_zones() — Price-acceptance / volume method
                                Finds tight-range clusters with volume
                                accumulation followed by explosive breakout.
                                Catches zones the body method misses
                                (e.g. Christmas-week low-volume bases).

  3. tag_zone_status()        — Labels every zone as:
                                  fresh      → price never re-entered zone
                                  tested     → price entered but closed above/below
                                  invalidated→ price closed through the zone

Public API
----------
  scan_zones(df, direction, legout_mult, max_base_candles)
      → returns List[ZoneResult] with all zones from both methods, deduplicated

  tag_zone_status(df, zones)
      → adds 'status' and 'zone_type' to each zone dict

ZoneResult dict keys
--------------------
  time_start      : int  unix timestamp of legin bar
  time_end        : int  unix timestamp of legout bar
  price_high      : float  zone top (base candles only)
  price_low       : float  zone bottom (base candles only)
  zone_type       : str   'rbr' | 'dbr' | 'consolidation'
  status          : str   'fresh' | 'tested' | 'invalidated'
  base_bars       : int   number of base candles
  legout_strength : float legout body / ATR ratio
  vol_score       : float 0-1, relative volume during base vs lookback avg
"""

from __future__ import annotations

import datetime as dt
from typing import List, Dict, Any, Optional, Tuple

import numpy as np
import pandas as pd

from algos.pivot_engine import compute_atr

# Type alias for raw scanner output
ZoneDict = Dict[str, Any]


# ─────────────────────────────────────────────────────────────────────────────
# DataFrame preparation
# ─────────────────────────────────────────────────────────────────────────────

def _prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    """Add Prev_close and atr columns if missing. Never modifies caller's df."""
    df = df.copy()
    if "Prev_close" not in df.columns:
        df["Prev_close"] = df["Close"].shift(1).fillna(df["Close"])
    if "atr" not in df.columns:
        df["atr"] = compute_atr(df)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Method 1: ATR-body zone scanner  (RBR + DBR)
# ─────────────────────────────────────────────────────────────────────────────

def _zone_scanner(
    df: pd.DataFrame,
    start_idx: int,
    direction: str,           # "buy" | "sell"
    ztype: str,               # "rbr" | "dbr" | "dbd" | "rbd"
    legin_mult: float,        # ATR multiple for legin body
    legout_mult: float,       # ATR multiple for legout body
    legout_strength: float,   # legout body >= this × legin body
    max_base_candles: int,    # max actual candles in base (not days)
    legout_wick_pct: float,   # upper wick of legout < this × range (buy)
    exclude_eod: bool = True,
) -> List[ZoneDict]:
    """
    Core scanner: legin → base(s) → legout.

    Key fixes vs v1:
    - Base band is computed from EACH base candle's own range,
      not from the legin body — catches low-volatility bases
      (e.g. Christmas-week tight candles after a strong legin).
    - Counts actual candle indices, not calendar days.
    - Breaks on first out-of-base candle; legout check uses that candle.
    - On legout failure, advances by 1 only — no valid candidates skipped.
    - base_high / base_low exclude the legin candle entirely.
    """
    close  = df["Close"].values
    prev_c = df["Prev_close"].values
    high   = df["High"].values
    low    = df["Low"].values
    atr    = df["atr"].values
    n      = len(df)

    result: List[ZoneDict] = []
    i = start_idx

    while i < n - 2:
        body = close[i] - prev_c[i]

        # ── 1. Legin detection ─────────────────────────────────────────────
        legin_ok = False
        if ztype == "rbr":
            legin_ok = body > legin_mult * atr[i]
        elif ztype == "dbr":
            legin_ok = -body > legin_mult * atr[i]
        elif ztype == "dbd":
            legin_ok = -body > legin_mult * atr[i]
        elif ztype == "rbd":
            legin_ok = body > legin_mult * atr[i]

        if not legin_ok:
            i += 1
            continue

        legin_idx   = i
        legin_len   = abs(body)
        legin_close = close[i]

        # Threshold the legout must clear
        # For buy zones (rbr/dbr): legout close > highest base high
        # For sell zones (dbd/rbd): legout close < lowest base low
        threshold = high[i] if direction == "buy" else low[i]

        # ── 2. Base candles ────────────────────────────────────────────────
        # Use actual candle count, not calendar days.
        # Base band: each candle must stay within 1.5× its own ATR
        # of the legin close — wider than v1 to catch quiet bases.
        base_count = 0
        base_high  = None
        base_low   = None
        legout_idx = None

        for j in range(i + 1, min(i + max_base_candles + 2, n - 1)):
            # Skip EOD candle (15:15 IST) if intraday data
            if exclude_eod and hasattr(df.index[j], "time"):
                if df.index[j].time() == dt.time(15, 15):
                    continue

            cj  = close[j]
            hj  = high[j]
            lj  = low[j]
            # Use candle's own ATR for the base band — key fix
            band = max(atr[j], atr[legin_idx]) * 0.75

            if direction == "buy":
                # Base candle: close stays within band of legin close
                in_base = abs(cj - legin_close) <= band * 1.5
            else:
                in_base = abs(cj - legin_close) <= band * 1.5

            if in_base:
                base_count += 1
                threshold   = max(threshold, hj) if direction == "buy" \
                              else min(threshold, lj)
                base_high   = hj if base_high is None else max(base_high, hj)
                base_low    = lj if base_low  is None else min(base_low,  lj)
            else:
                # First candle outside base is the legout candidate
                legout_idx = j
                break
        else:
            # All candles were base candles — next one is legout candidate
            legout_idx = min(i + max_base_candles + 1, n - 2)

        if base_count == 0 or legout_idx is None or legout_idx >= n - 1:
            i += 1
            continue

        # ── 3. Legout detection ────────────────────────────────────────────
        lo_body    = close[legout_idx] - prev_c[legout_idx]
        lo_range   = max(high[legout_idx] - low[legout_idx], 1e-8)
        wick_up    = (high[legout_idx] - close[legout_idx]) / lo_range

        legout_ok = False
        if direction == "buy":
            legout_ok = (
                lo_body > legout_mult * atr[legout_idx]
                and wick_up < legout_wick_pct
                and abs(lo_body) >= legout_strength * legin_len
                and close[legout_idx] > threshold
            )
        else:
            lo_abs = abs(lo_body)
            legout_ok = (
                lo_abs > legout_mult * atr[legout_idx]
                and lo_abs >= legout_strength * legin_len
                and close[legout_idx] < threshold
            )

        if legout_ok:
            # Fallback zone bounds (should rarely trigger)
            if base_high is None:
                base_high = max(close[legin_idx], prev_c[legin_idx])
            if base_low is None:
                base_low  = min(close[legin_idx], prev_c[legin_idx])

            result.append({
                "time_start":      df.index[legin_idx],
                "time_end":        df.index[legout_idx],
                "price_high":      round(float(base_high), 4),
                "price_low":       round(float(base_low),  4),
                "zone_type":       ztype,
                "base_bars":       base_count,
                "legout_strength": round(abs(lo_body) / max(atr[legout_idx], 1e-8), 2),
                "vol_score":       0.0,   # filled by scan_zones
            })
            # Advance past legout so it can't be reused as next legin
            i = legout_idx + 1
        else:
            # Only +1: don't skip potential legins between here and legout_idx
            i += 1

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Method 2: Price-acceptance / volume consolidation zones
# ─────────────────────────────────────────────────────────────────────────────

def detect_consolidation_zones(
    df: pd.DataFrame,
    direction: str,
    min_tight_bars: int = 2,       # min consecutive tight candles
    max_tight_bars: int = 10,      # max consecutive tight candles
    tight_atr_mult: float = 0.6,   # candle range < mult × ATR = "tight"
    breakout_atr_mult: float = 1.2,# breakout candle body > mult × ATR
    breakout_bars: int = 3,        # check this many candles after cluster
    vol_lookback: int = 20,        # bars for avg volume comparison
    min_vol_ratio: float = 0.3,    # base avg vol >= this × lookback avg vol
    no_return_bars: int = 5,       # breakout bar +N must not re-enter zone
) -> List[ZoneDict]:
    """
    Volume-aware price-acceptance method.

    Logic:
    1. Find clusters of N consecutive "tight" candles
       (range < tight_atr_mult × ATR) — these are consolidation bases.
    2. Require a breakout candle immediately after (body > breakout_atr_mult × ATR).
    3. Require price does not immediately return into the zone
       (confirms the move was genuine, not a false breakout).
    4. Score by relative volume: low volume during base = accumulation signature.
    5. Zone boundary = high/low of tight cluster (base candles only).

    This catches zones the ATR-body method misses because:
    - Legin may not be a single large candle (could be gradual drift)
    - Base candles might be too quiet to pass the body filter
    - Particularly effective for holiday/low-liquidity consolidations
    """
    df = _prepare_df(df)

    close  = df["Close"].values
    prev_c = df["Prev_close"].values
    high   = df["High"].values
    low    = df["Low"].values
    atr    = df["atr"].values
    vol    = df["Volume"].values if "Volume" in df.columns else np.ones(len(df))
    n      = len(df)

    # Rolling average volume (exclude zeros)
    vol_s    = pd.Series(vol).replace(0, np.nan)
    avg_vol  = vol_s.rolling(vol_lookback, min_periods=5).mean().fillna(
                   vol_s.expanding().mean()).values

    result: List[ZoneDict] = []
    i = max(vol_lookback, 1)

    while i < n - breakout_bars - 1:
        # ── 1. Try to start a tight cluster at bar i ───────────────────────
        cluster_start = i
        cluster_bars  = 0
        c_high = None
        c_low  = None
        c_vol  = []

        for k in range(i, min(i + max_tight_bars, n - breakout_bars - 1)):
            candle_range = high[k] - low[k]
            if candle_range < tight_atr_mult * atr[k]:
                cluster_bars += 1
                c_high = high[k] if c_high is None else max(c_high, high[k])
                c_low  = low[k]  if c_low  is None else min(c_low,  low[k])
                c_vol.append(vol[k])
            else:
                break   # cluster ended

        if cluster_bars < min_tight_bars or c_high is None:
            i += 1
            continue

        cluster_end = i + cluster_bars  # index of candle after cluster

        # ── 2. Breakout candle check ───────────────────────────────────────
        # Look in the next breakout_bars candles for a qualifying breakout
        found_breakout = False
        breakout_idx   = None
        breakout_dir   = None

        for b in range(cluster_end, min(cluster_end + breakout_bars, n - 1)):
            body    = close[b] - prev_c[b]
            body_abs = abs(body)

            if body_abs < breakout_atr_mult * atr[b]:
                continue

            if direction == "buy" and body > 0 and close[b] > c_high:
                found_breakout = True
                breakout_idx   = b
                breakout_dir   = "up"
                break
            elif direction == "sell" and body < 0 and close[b] < c_low:
                found_breakout = True
                breakout_idx   = b
                breakout_dir   = "down"
                break

        if not found_breakout:
            i += cluster_bars  # skip whole cluster
            continue

        # ── 3. No-return confirmation ──────────────────────────────────────
        # After the breakout, price should not immediately close back inside
        re_entered = False
        check_end  = min(breakout_idx + no_return_bars + 1, n)
        for k in range(breakout_idx + 1, check_end):
            if direction == "buy"  and close[k] < c_high:
                re_entered = True; break
            if direction == "sell" and close[k] > c_low:
                re_entered = True; break

        if re_entered:
            i += cluster_bars
            continue

        # ── 4. Volume score ────────────────────────────────────────────────
        # Ideal: low volume during base (accumulation), high on breakout
        base_avg_vol = float(np.mean(c_vol)) if c_vol else 0.0
        ref_vol      = float(avg_vol[cluster_start])

        # Reject if base has basically zero volume
        if ref_vol > 0 and (base_avg_vol / ref_vol) < min_vol_ratio:
            i += cluster_bars
            continue

        # Score: how much quieter was the base vs context?
        # Low = accumulation signature (score near 1.0)
        # High = distribution (score near 0.0)
        if ref_vol > 0:
            vol_score = float(np.clip(1.0 - (base_avg_vol / ref_vol), 0.0, 1.0))
        else:
            vol_score = 0.5

        result.append({
            "time_start":      df.index[cluster_start],
            "time_end":        df.index[breakout_idx],
            "price_high":      round(float(c_high), 4),
            "price_low":       round(float(c_low),  4),
            "zone_type":       "consolidation",
            "base_bars":       cluster_bars,
            "legout_strength": round(
                abs(close[breakout_idx] - prev_c[breakout_idx])
                / max(atr[breakout_idx], 1e-8), 2
            ),
            "vol_score":       round(vol_score, 3),
        })

        # Advance past the entire cluster + breakout
        i = breakout_idx + 1

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Method 3: Zone status tagging
# ─────────────────────────────────────────────────────────────────────────────

def tag_zone_status(
    df: pd.DataFrame,
    zones: List[ZoneDict],
) -> List[ZoneDict]:
    """
    Tag each zone with a status based on subsequent price action.

    Rules (applied to candles AFTER the legout bar):
    ─────────────────────────────────────────────────
    fresh       Price has never re-entered the zone (low > zone_high for buy,
                high < zone_low for sell). Zone is untested.

    tested      Price entered the zone (low <= zone_high) but every close
                while inside was above zone_low (buy) or below zone_high (sell).
                The zone held — it's been tested and respected.

    invalidated Price closed below zone_low (buy) or above zone_high (sell).
                The zone is breached. Grey it out.

    Note: a single wick through the zone that closes back inside = tested,
    not invalidated. A candle that CLOSES through = invalidated.
    """
    close = df["Close"].values
    high  = df["High"].values
    low   = df["Low"].values
    n     = len(df)

    tagged = []
    for z in zones:
        # Find the bar index of legout (time_end)
        try:
            legout_loc = df.index.get_indexer(
                [pd.Timestamp(z["time_end"])], method="nearest"
            )[0]
        except Exception:
            z["status"] = "fresh"
            tagged.append(z)
            continue

        zone_high = z["price_high"]
        zone_low  = z["price_low"]
        ztype     = z.get("zone_type", "rbr")
        is_buy    = ztype in ("rbr", "dbr", "consolidation")

        status = "fresh"

        for k in range(legout_loc + 1, n):
            if is_buy:
                if low[k] <= zone_high:          # price entered zone
                    if close[k] < zone_low:       # closed through → invalidated
                        status = "invalidated"
                        break
                    else:                          # tested but held
                        status = "tested"
            else:
                if high[k] >= zone_low:           # price entered zone
                    if close[k] > zone_high:      # closed through → invalidated
                        status = "invalidated"
                        break
                    else:
                        status = "tested"

        z["status"] = status
        tagged.append(z)

    return tagged


# ─────────────────────────────────────────────────────────────────────────────
# Deduplication
# ─────────────────────────────────────────────────────────────────────────────

def _dedup_zones(zones: List[ZoneDict], price_tol_pct: float = 0.005) -> List[ZoneDict]:
    """
    Remove near-duplicate zones (same price area, close timestamps).

    Two zones are considered duplicates if:
    - Their price_high values are within price_tol_pct of each other
    - AND their price_low values are within price_tol_pct of each other

    Keep the one with the higher legout_strength (more conviction).
    """
    if not zones:
        return zones

    kept: List[ZoneDict] = []
    for z in sorted(zones, key=lambda x: x["legout_strength"], reverse=True):
        is_dup = False
        for k in kept:
            hi_close = abs(z["price_high"] - k["price_high"]) / max(k["price_high"], 1e-8)
            lo_close = abs(z["price_low"]  - k["price_low"])  / max(k["price_low"],  1e-8)
            if hi_close < price_tol_pct and lo_close < price_tol_pct:
                is_dup = True
                break
        if not is_dup:
            kept.append(z)

    # Restore chronological order
    kept.sort(key=lambda x: pd.Timestamp(x["time_start"]))
    return kept


# ─────────────────────────────────────────────────────────────────────────────
# Volume score enrichment for body-method zones
# ─────────────────────────────────────────────────────────────────────────────

def _enrich_vol_score(df: pd.DataFrame, zones: List[ZoneDict],
                      vol_lookback: int = 20) -> List[ZoneDict]:
    """
    Add volume score to body-method zones (rbr/dbr/dbd/rbd).
    Score = 1 - (avg_base_vol / avg_lookback_vol), clipped 0-1.
    Low volume base = high score = accumulation signature.
    """
    if "Volume" not in df.columns:
        return zones

    vol      = df["Volume"].replace(0, np.nan)
    avg_vol  = vol.rolling(vol_lookback, min_periods=5).mean().fillna(
                   vol.expanding().mean())

    for z in zones:
        try:
            t0 = pd.Timestamp(z["time_start"])
            t1 = pd.Timestamp(z["time_end"])
            mask = (df.index >= t0) & (df.index <= t1)
            base_vol = float(df.loc[mask, "Volume"].mean())
            ref_vol  = float(avg_vol.loc[t0])
            if ref_vol > 0:
                z["vol_score"] = round(
                    float(np.clip(1.0 - base_vol / ref_vol, 0.0, 1.0)), 3
                )
        except Exception:
            pass
    return zones


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def scan_zones(
    df: pd.DataFrame,
    direction: str = "buy",
    legout_mult: float = 1.35,
    max_base_candles: int = 8,
    start_idx: int = 0,
    include_atr: bool = True,
    include_consolidation: bool = False,
    _prepared: bool = False,         
) -> List[ZoneDict]:
    """
    Run zone detection methods based on include_* flags.

    Parameters
    ----------
    df                    : OHLCV DataFrame with DatetimeIndex
    direction             : 'buy' | 'sell'
    legout_mult           : ATR multiple for legout candle body
    max_base_candles      : max actual candles (not days) in base
    start_idx             : bar index to start scanning from
    include_atr           : run RBR+DBR (buy) or DBD+RBD (sell) body method
    include_consolidation : run volume/price-acceptance consolidation method

    Returns
    -------
    List of ZoneDict sorted by time_start, deduplicated, status-tagged.
    """
    if not _prepared:
        df = _prepare_df(df)
    all_zones: List[ZoneDict] = []

    if include_atr:
        if direction == "buy":
            # RBR — Rally-Base-Rally
            all_zones.extend(_zone_scanner(
                df, start_idx,
                direction="buy", ztype="rbr",
                legin_mult=1.0,
                legout_mult=legout_mult,
                legout_strength=0.8,
                max_base_candles=max_base_candles,
                legout_wick_pct=0.30,
            ))
            # DBR — Drop-Base-Rally
            all_zones.extend(_zone_scanner(
                df, start_idx,
                direction="buy", ztype="dbr",
                legin_mult=0.9,
                legout_mult=legout_mult,
                legout_strength=0.8,
                max_base_candles=max_base_candles,
                legout_wick_pct=0.35,
            ))
        else:
            # DBD — Drop-Base-Drop
            all_zones.extend(_zone_scanner(
                df, start_idx,
                direction="sell", ztype="dbd",
                legin_mult=0.9,
                legout_mult=legout_mult,
                legout_strength=0.9,
                max_base_candles=max_base_candles,
                legout_wick_pct=0.99,
            ))
            # RBD — Rally-Base-Drop
            all_zones.extend(_zone_scanner(
                df, start_idx,
                direction="sell", ztype="rbd",
                legin_mult=1.0,
                legout_mult=legout_mult,
                legout_strength=0.9,
                max_base_candles=max_base_candles,
                legout_wick_pct=0.99,
            ))

    if include_consolidation:
        all_zones.extend(detect_consolidation_zones(
            df,
            direction=direction,
            min_tight_bars=2,
            max_tight_bars=max_base_candles,
            tight_atr_mult=0.65,
            breakout_atr_mult=max(legout_mult * 0.8, 1.0),
            breakout_bars=3,
            vol_lookback=20,
            min_vol_ratio=0.2,
            no_return_bars=3,
        ))

    # Enrich body-method zones with volume scores
    all_zones = _enrich_vol_score(df, all_zones)

    # Deduplicate, tag status, sort
    all_zones = _dedup_zones(all_zones, price_tol_pct=0.005)
    all_zones = tag_zone_status(df, all_zones)
    all_zones.sort(key=lambda x: pd.Timestamp(x["time_start"]))

    return all_zones

# ─────────────────────────────────────────────────────────────────────────────
# Legacy shim — keeps scanner.py and app.py working without changes
# ─────────────────────────────────────────────────────────────────────────────

def buy_zone(
    df: pd.DataFrame,
    start_idx: int,
    resist,
    ztype: str = "rbr",
    multiplier: float = 1.0,
    legout_strength_mult: float = 1.35,
) -> Tuple[list, int]:
    """
    Legacy API shim. Returns (raw_zone_list, htf_flag).
    raw_zone_list items: [legin_ts, legout_ts, price_high, price_low]
    """
    df = _prepare_df(df)
    zones = _zone_scanner(
        df, start_idx,
        direction="buy", ztype=ztype,
        legin_mult=1.0 * multiplier,
        legout_mult=legout_strength_mult * multiplier,
        legout_strength=0.8,
        max_base_candles=8,
        legout_wick_pct=0.30,
    )
    raw = [
        [z["time_start"], z["time_end"], z["price_high"], z["price_low"]]
        for z in zones
    ]
    htf_flag = 0
    if resist != "NA" and resist is not None:
        try:
            resist_level = resist[1] - resist[3]
            if df["High"].max() > resist_level:
                htf_flag = 1
        except Exception:
            pass
    return raw, htf_flag


def sell_zone(
    df: pd.DataFrame,
    start_idx: int,
    multiplier: float = 1.0,
    legout_strength_mult: float = 1.35,
) -> list:
    """Legacy API shim. Returns raw_zone_list."""
    df = _prepare_df(df)
    zones = _zone_scanner(
        df, start_idx,
        direction="sell", ztype="dbd",
        legin_mult=0.9 * multiplier,
        legout_mult=legout_strength_mult * multiplier,
        legout_strength=0.9,
        max_base_candles=8,
        legout_wick_pct=0.99,
    )
    return [
        [z["time_start"], z["time_end"], z["price_high"], z["price_low"]]
        for z in zones
    ]


def buy_zone_5m(df, start_idx, resist, ztype="rbr"):
    """5-minute timeframe variant."""
    return buy_zone(df, start_idx, resist, ztype, multiplier=0.85)
