"""
algos/context.py  —  SymbolContext: single-build per-symbol analysis cache
===========================================================================
Compute ATR, close price, and atr_pct once per symbol per scan request.
Pass this object downstream to zones, confluence, and metrics to avoid
redundant recomputation across the call chain.
"""
from __future__ import annotations
from dataclasses import dataclass
import pandas as pd
from algos.pivot_engine import compute_atr


@dataclass(frozen=True)
class SymbolContext:
    """
    Immutable pre-computed values for one (symbol, df) pair.

    Build once at the top of analyse_symbol(); pass to every function
    that would otherwise recompute ATR or close internally.
    """
    df:      pd.DataFrame
    atr:     pd.Series   # full ATR series — length == len(df)
    atr_val: float       # atr.iloc[-1]
    close:   float       # df["Close"].iloc[-1]
    atr_pct: float       # (atr_val / close) * 100

    @classmethod
    def build(cls, df: pd.DataFrame, atr_period: int = 14) -> "SymbolContext":
        """Compute all fields in one pass. Call once per symbol."""
        atr     = compute_atr(df, atr_period)
        atr_val = float(atr.iloc[-1])
        close   = float(df["Close"].iloc[-1])
        return cls(
            df=df,
            atr=atr,
            atr_val=atr_val,
            close=close,
            atr_pct=(atr_val / close * 100) if close > 0 else 0.0,
        )