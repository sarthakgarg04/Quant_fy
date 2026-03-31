"""
data_fetch/crypto_fetch.py
==========================
Binance USDT-M Perpetual Futures data fetcher.

Architecture
------------
- ONE canonical source of truth per symbol: 1-minute OHLCV in parquet
- All higher timeframes (3m, 5m, 15m, 1h, 4h) are DERIVED by resampling 1m locally
- No API calls needed for higher TFs once 1m is stored
- Subsequent fetches are APPEND-ONLY: only the gap since last stored candle is pulled

Storage layout (under data_store/crypto/):
    raw_1m/
        BTCUSDT_PERP_1m.parquet     ← canonical, ~60MB per symbol per 2 years
        ETHUSDT_PERP_1m.parquet
        ...
    derived/
        BTCUSDT_PERP_3m.parquet     ← resampled on demand, cached here
        BTCUSDT_PERP_5m.parquet
        ...

API used: Binance USDT-M Futures  GET /fapi/v1/klines
  - No API key required for market data (public endpoint)
  - Max 1500 candles per request
  - Rate limit: 2400 weight/min; each 1500-candle call costs weight=5
  - We stay well under limit: ~0.5s sleep between calls = safe throughput

Trading Note on Crypto Perps
-----------------------------
Binance USDT-M perpetual data starts from Sept 2019 for BTCUSDT.
Most altcoin perps have data from 2020-2021 onwards.
Funding rate resets every 8h — not stored here (separate concern for backtesting).
24/7 trading means NO session gaps, so pd.resample() reconstructs exact candles.
"""

from __future__ import annotations

import time
import datetime as dt
from pathlib import Path
from typing import Dict, Generator, List, Optional, Tuple

import pandas as pd
import requests

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

# Symbols we track: base name only — we always append USDT for Binance perp
CRYPTO_SYMBOLS: List[str] = [
    "BTC", "ETH", "SOL", "BNB", "XRP",
    "LINK", "AVAX", "DOGE", "ADA", "DOT",
]

# How many years of 1m history to pull on first fetch
DEFAULT_LOOKBACK_DAYS: int = 730  # 2 years

# Binance USDT-M Futures klines endpoint (no auth required)
BINANCE_FAPI_BASE = "https://fapi.binance.com"
KLINES_ENDPOINT   = "/fapi/v1/klines"

# Max candles Binance returns per call (hard limit = 1500)
CANDLES_PER_REQUEST = 1500

# Sleep between paginated calls to stay under rate limits
# Weight budget: 2400/min. Each call (1500 candles) = weight 5.
# At 0.4s sleep → max ~150 calls/min → 750 weight/min. Very safe.
INTER_CALL_SLEEP = 0.4  # seconds

# Timeframes we derive from 1m (all resampled locally)
# Format: canonical label → pandas resample offset
DERIVED_INTERVALS: Dict[str, str] = {
    "3m":  "3min",
    "5m":  "5min",
    "15m": "15min",
    "1h":  "1h",
    "4h":  "4h",
    "1d":  "1D",    
    "1w":  "1W",
}

# ─────────────────────────────────────────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────────────────────────────────────────

_ROOT         = Path(__file__).resolve().parent.parent
_CRYPTO_DIR   = _ROOT / "data_store" / "crypto"
_RAW_1M_DIR   = _CRYPTO_DIR / "raw_1m"
_DERIVED_DIR  = _CRYPTO_DIR / "derived"

_RAW_1M_DIR.mkdir(parents=True, exist_ok=True)
_DERIVED_DIR.mkdir(parents=True, exist_ok=True)


def _perp_symbol(base: str) -> str:
    """BTC → BTCUSDT  (Binance perp naming convention)"""
    base = base.upper().replace("USDT", "")  # idempotent
    return f"{base}USDT"


def _file_key(base: str) -> str:
    """BTC → BTCUSDT_PERP  (our internal storage key, distinguishes perp from spot)"""
    return f"{_perp_symbol(base)}_PERP"


def _raw_path(base: str) -> Path:
    return _RAW_1M_DIR / f"{_file_key(base)}_1m.parquet"


def _derived_path(base: str, interval: str) -> Path:
    return _DERIVED_DIR / f"{_file_key(base)}_{interval}.parquet"


# ─────────────────────────────────────────────────────────────────────────────
# BINANCE API — single page fetch
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_klines_page(
    symbol: str,
    start_ms: int,
    end_ms: int,
) -> List[list]:
    """
    Fetch one page of 1m klines from Binance.

    Parameters
    ----------
    symbol   : Binance symbol, e.g. "BTCUSDT"
    start_ms : Start time in milliseconds (inclusive)
    end_ms   : End time in milliseconds (inclusive)

    Returns
    -------
    Raw list of kline arrays from Binance, or [] on failure.
    Each row: [open_time, open, high, low, close, volume,
               close_time, quote_vol, trades, taker_buy_base,
               taker_buy_quote, ignore]
    """
    params = {
        "symbol":    symbol,
        "interval":  "1m",
        "startTime": start_ms,
        "endTime":   end_ms,
        "limit":     CANDLES_PER_REQUEST,
    }
    try:
        resp = requests.get(
            BINANCE_FAPI_BASE + KLINES_ENDPOINT,
            params=params,
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        print(f"  ⚠  Binance API error [{symbol}]: {e}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# PARSE raw klines into DataFrame
# ─────────────────────────────────────────────────────────────────────────────

def _parse_klines(raw: List[list]) -> pd.DataFrame:
    """
    Convert Binance raw kline list to a clean OHLCV DataFrame.

    Index: DatetimeIndex (UTC, timezone-naive for parquet compatibility)
    Columns: open, high, low, close, volume, quote_volume, trades,
             taker_buy_base, taker_buy_quote
    """
    if not raw:
        return pd.DataFrame()

    df = pd.DataFrame(raw, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trades",
        "taker_buy_base", "taker_buy_quote", "_ignore",
    ])

    # Timestamps: Binance sends milliseconds
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df = df.set_index("open_time")
    df.index = df.index.tz_localize(None)  # strip tz → naive UTC for parquet

    # Cast price/volume columns to float
    float_cols = ["open", "high", "low", "close", "volume",
                  "quote_volume", "taker_buy_base", "taker_buy_quote"]
    df[float_cols] = df[float_cols].astype(float)
    df["trades"] = df["trades"].astype(int)

    return df.drop(columns=["close_time", "_ignore"])


# ─────────────────────────────────────────────────────────────────────────────
# CORE FETCH: paginated pull with append logic
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_range_iter(
    symbol: str,
    start_ms: int,
    end_ms: int,
) -> Generator[Tuple[pd.DataFrame, int, int], None, None]:
    """
    Paginate through Binance klines from start_ms to end_ms.

    Yields (df_page, candles_fetched_so_far, estimated_total) per page.
    This powers the progress logging in _run_fetch_task.

    The estimated_total is approximate (based on time range / 1 candle per minute).
    """
    estimated_total = max(1, (end_ms - start_ms) // 60_000)
    fetched = 0
    cursor  = start_ms

    while cursor < end_ms:
        raw = _fetch_klines_page(symbol, cursor, end_ms)
        if not raw:
            break  # API error or genuinely no data

        df_page = _parse_klines(raw)
        if df_page.empty:
            break

        fetched += len(df_page)
        yield df_page, fetched, estimated_total

        # Advance cursor past last returned candle
        # Binance open_time is in ms; our index is naive UTC datetime
        last_open_ms = int(df_page.index[-1].timestamp() * 1000)
        cursor = last_open_ms + 60_000  # +1 minute

        if len(raw) < CANDLES_PER_REQUEST:
            break  # Last page — we've reached the end

        time.sleep(INTER_CALL_SLEEP)


# ─────────────────────────────────────────────────────────────────────────────
# STORE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _load_raw(base: str) -> Optional[pd.DataFrame]:
    """Load the stored 1m parquet for a symbol. Returns None if not found."""
    p = _raw_path(base)
    if not p.exists():
        return None
    try:
        return pd.read_parquet(p)
    except Exception as e:
        print(f"  ⚠  Could not read {p.name}: {e}")
        return None


def _save_raw(base: str, df: pd.DataFrame) -> None:
    """Write 1m DataFrame to parquet. Always sorts and deduplicates first."""
    if df is None or df.empty:
        return
    df = df.sort_index()
    df = df[~df.index.duplicated(keep="last")]
    df.to_parquet(_raw_path(base))


def _get_last_stored_ts(base: str) -> Optional[dt.datetime]:
    """Return the last stored candle's open_time, or None if no data exists."""
    df = _load_raw(base)
    if df is None or df.empty:
        return None
    return df.index[-1].to_pydatetime()


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC: fetch one symbol — handles both initial load and append
# ─────────────────────────────────────────────────────────────────────────────

def fetch_crypto_1m(
    base: str,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    log_fn=None,
) -> Tuple[bool, str]:
    """
    Fetch 1m OHLCV for a single perpetual symbol and store to parquet.

    Logic:
        - If no data stored → full historical fetch (lookback_days)
        - If data exists   → APPEND only: fetch from last_stored_ts to now
        - Deduplicates on save (safe to call repeatedly)

    Parameters
    ----------
    base        : e.g. "BTC", "ETH", "SOL"
    lookback_days : Days of history for initial load (default 730 = 2 years)
    log_fn      : Optional callable(msg, kind) for progress logging
                  (same signature as _run_fetch_task's internal log fn)

    Returns
    -------
    (success: bool, message: str)
    """
    def _log(msg: str, kind: str = "inf"):
        print(f"  [{kind.upper()}] {msg}")
        if log_fn:
            log_fn(msg, kind)

    symbol     = _perp_symbol(base)        # e.g. "BTCUSDT"
    file_key   = _file_key(base)           # e.g. "BTCUSDT_PERP"
    now_utc    = dt.datetime.utcnow()
    end_ms     = int(now_utc.timestamp() * 1000) - 60_000  # exclude current incomplete candle

    # ── Determine start: append from last candle, or full historical load ──
    last_ts = _get_last_stored_ts(base)

    if last_ts is None:
        # No data exists → full historical fetch
        start_dt = now_utc - dt.timedelta(days=lookback_days)
        mode     = f"full historical ({lookback_days}d)"
    else:
        # Data exists → only fetch the gap
        # Add 1 minute to avoid re-fetching the last stored candle
        start_dt = last_ts + dt.timedelta(minutes=1)
        gap_mins = int((now_utc - last_ts).total_seconds() / 60)

        if gap_mins < 2:
            msg = f"{file_key}: already up to date (last: {last_ts.strftime('%Y-%m-%d %H:%M')} UTC)"
            _log(msg, "inf")
            return True, msg

        mode = f"append ({gap_mins} candles gap)"

    start_ms = int(start_dt.timestamp() * 1000)
    _log(f"{file_key}: starting {mode}", "inf")

    # ── Paginate and collect ──
    pages: List[pd.DataFrame] = []
    try:
        for df_page, fetched, estimated in _fetch_range_iter(symbol, start_ms, end_ms):
            pages.append(df_page)
            _log(
                f"{file_key}: fetched {fetched:,} / ~{estimated:,} candles",
                "inf",
            )
    except Exception as e:
        msg = f"{file_key}: fetch failed — {e}"
        _log(msg, "err")
        return False, msg

    if not pages:
        msg = f"{file_key}: no new data returned from Binance"
        _log(msg, "inf")
        return True, msg  # Not an error — data might be up to date

    new_df = pd.concat(pages)

    # ── Merge with existing (append mode) ──
    existing = _load_raw(base)
    if existing is not None and not existing.empty:
        combined = pd.concat([existing, new_df])
    else:
        combined = new_df

    _save_raw(base, combined)

    final_df = _load_raw(base)
    row_count = len(final_df) if final_df is not None else 0
    msg = (
        f"✓ {file_key}: saved {row_count:,} total rows "
        f"(+{len(new_df):,} new) — "
        f"from {combined.index[0].date()} to {combined.index[-1].date()}"
    )
    _log(msg, "ok")
    return True, msg


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC: batch fetch all configured symbols (generator for progress streaming)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_crypto_batch_iter(
    symbols: Optional[List[str]] = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> Generator[Tuple[int, int, str, bool], None, None]:
    """
    Fetch 1m data for each symbol sequentially.

    Yields (done, total, symbol_key, success) after each symbol.
    Same interface as fetch_upstox_batch_iter — plugs into existing
    _run_fetch_task progress tracking in app.py.

    Parameters
    ----------
    symbols      : List of base symbols e.g. ["BTC", "ETH"]. Defaults to CRYPTO_SYMBOLS.
    lookback_days: Days of history for initial load.
    """
    targets = symbols or CRYPTO_SYMBOLS
    total   = len(targets)

    for i, base in enumerate(targets, 1):
        ok, _ = fetch_crypto_1m(base, lookback_days=lookback_days)
        yield i, total, _file_key(base), ok


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC: resample 1m → higher timeframe
# ─────────────────────────────────────────────────────────────────────────────

# Correct OHLCV aggregation rules — these are not arbitrary
_RESAMPLE_AGG = {
    "open":            "first",
    "high":            "max",
    "low":             "min",
    "close":           "last",
    "volume":          "sum",
    "quote_volume":    "sum",
    "trades":          "sum",
    "taker_buy_base":  "sum",
    "taker_buy_quote": "sum",
}


def resample_crypto(
    base: str,
    interval: str,
    force: bool = False,
) -> Optional[pd.DataFrame]:
    """
    Load 1m data and resample to the requested interval.

    Returns the resampled DataFrame, also caching it in derived/.
    Reads from derived/ cache if available and not forced to rebuild.

    Parameters
    ----------
    base     : e.g. "BTC"
    interval : one of "3m", "5m", "15m", "1h", "4h"
    force    : if True, rebuild from 1m even if cache exists

    Returns
    -------
    pd.DataFrame with DatetimeIndex, or None if 1m data not available.

    Note on partial candle exclusion:
        The last row of a resample may represent an in-progress candle
        (e.g. a 4h candle that hasn't closed yet). We drop the last row
        to avoid feeding incomplete candles into the scanner/pivot engine.
        For live charting, callers can pass include_last=True.
    """
    if interval not in DERIVED_INTERVALS:
        raise ValueError(
            f"Interval '{interval}' not supported. "
            f"Supported: {list(DERIVED_INTERVALS.keys())}"
        )

    # Check derived cache first
    cache_path = _derived_path(base, interval)
    if not force and cache_path.exists():
        try:
            cached = pd.read_parquet(cache_path)
            # Cache is valid if it's less than 5 minutes stale for intraday
            if not cached.empty:
                age_mins = (dt.datetime.utcnow() - cached.index[-1].to_pydatetime()).total_seconds() / 60
                if age_mins < 5:
                    return cached
        except Exception:
            pass  # Fall through to rebuild

    # Load canonical 1m store
    df_1m = _load_raw(base)
    if df_1m is None or df_1m.empty:
        return None

    # Resample
    offset = DERIVED_INTERVALS[interval]
    agg    = {col: rule for col, rule in _RESAMPLE_AGG.items() if col in df_1m.columns}

    df_resampled = (
        df_1m
        .resample(offset, label="left", closed="left")
        .agg(agg)
        .dropna(subset=["close"])  # drop empty periods (no trades)
    )

    # Drop the last (potentially incomplete) candle
    # A candle is "complete" if its open_time + interval <= now
    if len(df_resampled) > 1:
        df_resampled = df_resampled.iloc[:-1]

    # Cache to disk
    df_resampled.to_parquet(cache_path)

    return df_resampled


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC: unified loader — used by scanner and chart API
# ─────────────────────────────────────────────────────────────────────────────

def load_crypto_ohlcv(base: str, interval: str) -> Optional[pd.DataFrame]:
    """
    Load OHLCV for a crypto perp symbol at any supported interval.

    Routing:
        interval == "1m"  → load raw_1m parquet directly
        interval in DERIVED_INTERVALS → resample from 1m (with caching)

    This is the single entry point the scanner and API should use.
    It abstracts away whether data comes from the raw store or derived cache.

    Returns
    -------
    pd.DataFrame with standard columns (open, high, low, close, volume)
    or None if data is not available.
    """
    if interval == "1m":
        return _load_raw(base)

    if interval in DERIVED_INTERVALS:
        return resample_crypto(base, interval)

    raise ValueError(
        f"Interval '{interval}' not supported for crypto. "
        f"Supported: 1m, {', '.join(DERIVED_INTERVALS.keys())}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC: storage info (feeds into existing /api/data/store endpoint)
# ─────────────────────────────────────────────────────────────────────────────

def list_crypto_stored() -> List[Dict]:
    """
    Return storage metadata for all stored crypto symbols.
    Format mirrors list_stored() in data_fetch.py for UI compatibility.
    """
    results = []
    for p in sorted(_RAW_1M_DIR.glob("*_1m.parquet")):
        try:
            df       = pd.read_parquet(p)
            file_key = p.stem.replace("_1m", "")  # e.g. "BTCUSDT_PERP"
            results.append({
                "symbol":    file_key,
                "interval":  "1m",
                "rows":      len(df),
                "first_date": str(df.index[0].date())  if len(df) else "–",
                "last_date":  str(df.index[-1].date()) if len(df) else "–",
                "size_kb":   round(p.stat().st_size / 1024, 1),
                "source":    "binance_perp",
            })
        except Exception:
            pass
    return results


def delete_crypto_stored(base: str) -> bool:
    """Delete raw 1m and all derived parquets for a symbol."""
    deleted = False
    raw = _raw_path(base)
    if raw.exists():
        raw.unlink()
        deleted = True
    for interval in DERIVED_INTERVALS:
        dp = _derived_path(base, interval)
        if dp.exists():
            dp.unlink()
    return deleted
