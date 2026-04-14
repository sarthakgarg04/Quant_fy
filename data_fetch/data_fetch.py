"""
data_fetch/data_fetch.py
========================
Upstox ONLY — no yfinance dependency.
Auto-resolves instrument keys from ticker symbols (e.g. RELIANCE.NS → NSE key).
Local parquet store so the scanner never re-fetches fresh data.

Local layout:
  quant_project/
    data_store/
      upstox_token.txt          – saved OAuth token
      instruments.parquet       – instrument master (24h cache)
      1d/  1wk/  1h/  5m/ ...  – one folder per timeframe
        RELIANCE_NS_1d.parquet  – OHLCV per symbol
"""

from __future__ import annotations

import os, gzip, json, time, warnings, datetime as dt
from io import BytesIO
from pathlib import Path
from typing import Generator, List, Optional, Dict, Tuple

import numpy as np
import pandas as pd
import requests

warnings.simplefilter("ignore", FutureWarning)
warnings.simplefilter("ignore", DeprecationWarning)

_INSTRUMENT_DF: Optional[pd.DataFrame] = None
_KEY_MAP:       Optional[Dict[str, str]] = None   # instrument_key → trading_symbol
_NSE_SYM_MAP:  Optional[Dict[str, str]] = None   # trading_symbol → instrument_key (NSE_EQ)
_BSE_SYM_MAP:  Optional[Dict[str, str]] = None   # trading_symbol → instrument_key (BSE_EQ)
_NSE_ISIN_MAP: Optional[Dict[str, str]] = None   # isin → instrument_key (NSE_EQ)


# ── Project paths ─────────────────────────────────────────────────────────────
_ROOT      = Path(__file__).resolve().parent.parent
DATA_DIR   = _ROOT / "data_store"
TOKEN_FILE = DATA_DIR / "upstox_token.txt"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ── Upstox constants ──────────────────────────────────────────────────────────
UPSTOX_PROFILE_URL = "https://api.upstox.com/v2/user/profile"
UPSTOX_INSTR_URL   = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
UPSTOX_AUTH_URL    = "https://api.upstox.com/v2/login/authorization/dialog"
UPSTOX_TOKEN_URL   = "https://api.upstox.com/v2/login/authorization/token"

UPSTOX_INTERVAL_MAP: Dict[str, Tuple[str, str]] = {
    "1m":  ("minutes", "1"),    # ← new
    "5m":  ("minutes", "5"),
    "15m": ("minutes", "15"),
    "1h":  ("minutes", "60"),
    "4h":  ("minutes", "240"),
    "1d":  ("days",    "1"),
    "1wk": ("weeks",   "1"),
}

DAYS_LOOKBACK: Dict[str, int] = {
    "1m": 7,    # ← new — Upstox only provides ~2 days of 1m data
    "5m": 30,
    "15m": 31,
    "1h": 31,
    "4h": 31,
    "1d": 1200,
    "1wk": 2000,
}

UPSTOX_MAX_DAYS_PER_CALL: Dict[str, int] = {
    "1m":  15,    
    "5m":  30,   # 30 days, within 31-day limit
    "15m": 31,   # 1 month max
    "1h":  31,
    "4h":  31,
    "1d":  1000,
    "1wk": 2000,
}

# ═══════════════════════════════════════════════════════════════════════════════
# Token helpers
# ═══════════════════════════════════════════════════════════════════════════════

def save_token(token: str) -> None:
    TOKEN_FILE.write_text(token.strip())


def load_token() -> Optional[str]:
    if TOKEN_FILE.exists():
        t = TOKEN_FILE.read_text().strip()
        return t or None
    return None


def is_token_valid(token: str) -> Tuple[bool, Optional[dict]]:
    import requests
    try:
        r = requests.get(
            UPSTOX_PROFILE_URL,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            timeout=8,
        )
        if r.status_code == 200:
            return True, r.json()
    except Exception:
        pass
    return False, None


# ═══════════════════════════════════════════════════════════════════════════════
# Instrument master
# ═══════════════════════════════════════════════════════════════════════════════

_INSTRUMENT_DF: Optional[pd.DataFrame] = None
_KEY_MAP: Optional[Dict[str, str]]     = None   # instrument_key → trading_symbol
_NSE_SYM_MAP: Optional[Dict[str, str]] = None   # trading_symbol → instrument_key (NSE)
_BSE_SYM_MAP: Optional[Dict[str, str]] = None   # trading_symbol → instrument_key (BSE)


def _build_maps(df: pd.DataFrame) -> None:
    """
    Build all lookup maps from the instrument master in one pass.
 
    Key insight: filter by `segment` not `exchange`.
      segment == "NSE_EQ"  →  NSE equities (3005 records)
      segment == "BSE_EQ"  →  BSE equities (4144 records)
    The `exchange` column says "NSE" for everything NSE-related
    (equities, F&O, currency) so it cannot be used to isolate equities.
    """
    global _KEY_MAP, _NSE_SYM_MAP, _BSE_SYM_MAP, _NSE_ISIN_MAP
 
    _KEY_MAP = dict(zip(df["instrument_key"], df["trading_symbol"]))
 
    # Filter strictly by segment
    nse = df[
        (df["segment"] == "NSE_EQ") &
        (df["instrument_type"].isin(["EQ", "BE", "SM"]))
    ]
    bse = df[
        (df["segment"] == "BSE_EQ") &
        (df["instrument_type"].isin(["EQ", "BE", "SM", "A", "B", "X", "XT"]))
    ]
 
    _NSE_SYM_MAP  = dict(zip(nse["trading_symbol"], nse["instrument_key"]))
    _BSE_SYM_MAP  = dict(zip(bse["trading_symbol"], bse["instrument_key"]))
 
    # ISIN map for NSE (useful for resolving .NS tickers)
    if "isin" in nse.columns:
        _NSE_ISIN_MAP = dict(zip(nse["isin"], nse["instrument_key"]))
    else:
        _NSE_ISIN_MAP = {}
 
    print(f"  [instruments] NSE_EQ equities: {len(_NSE_SYM_MAP)}")
    print(f"  [instruments] BSE_EQ equities: {len(_BSE_SYM_MAP)}")


def get_instrument_df(force_refresh: bool = False) -> pd.DataFrame:
    """Download and cache the Upstox instrument master (refreshes every 24h)."""
    global _INSTRUMENT_DF
    import requests
 
    cache = DATA_DIR / "instruments.parquet"
 
    if not force_refresh and _INSTRUMENT_DF is not None:
        return _INSTRUMENT_DF
 
    if not force_refresh and cache.exists():
        age_h = (dt.datetime.now().timestamp() - cache.stat().st_mtime) / 3600
        if age_h < 24:
            _INSTRUMENT_DF = pd.read_parquet(cache)
            _build_maps(_INSTRUMENT_DF)
            return _INSTRUMENT_DF
 
    print("  [instruments] Downloading instrument master from Upstox…")
    resp = requests.get(UPSTOX_INSTR_URL, timeout=30)
    resp.raise_for_status()
 
    with gzip.GzipFile(fileobj=BytesIO(resp.content)) as gz:
        data = json.loads(gz.read())
 
    _INSTRUMENT_DF = pd.DataFrame(data)
    _INSTRUMENT_DF.to_parquet(cache)
    _build_maps(_INSTRUMENT_DF)
 
    print(f"  [instruments] Total records: {len(_INSTRUMENT_DF)}")
    return _INSTRUMENT_DF

def get_eq_instruments(
    exchanges: Tuple[str, ...] = ("NSE_EQ",)
) -> pd.DataFrame:
    """
    Return equity instruments for the requested segments.
 
    Parameters
    ----------
    exchanges : tuple of segment names, e.g. ("NSE_EQ",) or ("NSE_EQ","BSE_EQ")
                Note: pass segment names ("NSE_EQ"), NOT exchange names ("NSE").
 
    Returns
    -------
    DataFrame with equity instrument records including instrument_key,
    trading_symbol, segment, instrument_type, isin (where available).
    """
    df = get_instrument_df()
 
    # Equity instrument types for NSE/BSE cash segment
    NSE_EQ_TYPES = {"EQ", "BE", "SM"}
    BSE_EQ_TYPES = {"EQ", "BE", "SM", "A", "B", "X", "XT"}
 
    frames = []
    for seg in exchanges:
        seg = seg.strip().upper()
        if seg == "NSE_EQ":
            mask = (df["segment"] == "NSE_EQ") & (df["instrument_type"].isin(NSE_EQ_TYPES))
        elif seg == "BSE_EQ":
            mask = (df["segment"] == "BSE_EQ") & (df["instrument_type"].isin(BSE_EQ_TYPES))
        else:
            # Fallback: match segment prefix
            mask = df["segment"].str.startswith(seg)
        frames.append(df[mask])
 
    if not frames:
        return pd.DataFrame()
 
    result = pd.concat(frames, ignore_index=True).drop_duplicates(
        subset=["instrument_key"]
    )
    print(f"  [get_eq_instruments] segments={exchanges} → {len(result)} instruments")
    return result


def instrument_key_to_symbol() -> Dict[str, str]:
    """instrument_key → trading_symbol"""
    global _KEY_MAP
    if _KEY_MAP is None:
        get_instrument_df()
    return _KEY_MAP or {}


def symbol_to_instrument_key(exchange: str = "NSE") -> Dict[str, str]:
    """
    trading_symbol → instrument_key.
 
    Parameters
    ----------
    exchange : "NSE" or "BSE" (maps to NSE_EQ / BSE_EQ segment respectively)
    """
    global _NSE_SYM_MAP, _BSE_SYM_MAP
    if _NSE_SYM_MAP is None:
        get_instrument_df()
    if exchange.upper() in ("NSE", "NSE_EQ"):
        return _NSE_SYM_MAP or {}
    return _BSE_SYM_MAP or {}
 
 
def resolve_instrument_key(ticker: str) -> Optional[str]:
    """
    Auto-resolve instrument_key from a ticker like 'RELIANCE.NS' or 'RELIANCE'.
 
    Resolution order:
    1. Strip .NS / .BO suffix to get base symbol
    2. Look up in NSE_EQ symbol map (exact match)
    3. Look up in BSE_EQ symbol map (exact match)
    4. Try common symbol variants (hyphen → space, etc.)
    """
    # Strip exchange suffixes
    sym = (ticker
           .replace(".NS", "")
           .replace(".BO", "")
           .replace(".BSE", "")
           .strip()
           .upper())
 
    if _NSE_SYM_MAP is None:
        get_instrument_df()
 
    # 1. Direct NSE lookup
    key = (_NSE_SYM_MAP or {}).get(sym)
    if key:
        return key
 
    # 2. Direct BSE lookup
    key = (_BSE_SYM_MAP or {}).get(sym)
    if key:
        return key
 
    # 3. Common variant: BAJAJ-AUTO → BAJAJ-AUTO, M&M → M&M
    # Some symbols use & which yfinance strips
    for variant in [sym.replace("-", " "), sym.replace("&", "AND")]:
        key = (_NSE_SYM_MAP or {}).get(variant)
        if key:
            return key
 
    print(f"  ⚠  No instrument_key found for '{ticker}' (sym='{sym}')")
    return None

# ═══════════════════════════════════════════════════════════════════════════════
# Local store helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _store_path(symbol: str, interval: str) -> Path:
    folder = DATA_DIR / interval
    folder.mkdir(parents=True, exist_ok=True)
    safe = symbol.replace(".", "_").replace("/", "_").replace("\\", "_")
    return folder / f"{safe}_{interval}.parquet"


def store_df(symbol: str, interval: str, df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return
    _store_path(symbol, interval).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(_store_path(symbol, interval))


# REPLACE WITH:
def load_stored_df(symbol: str, interval: str) -> Optional[pd.DataFrame]:
    # Try as-is first
    p = _store_path(symbol, interval)
    if p.exists():
        try:
            return pd.read_parquet(p)
        except Exception:
            pass
    # Strip exchange suffix (.NS, .BO) — files stored without suffix
    sym2 = symbol.replace(".NS", "").replace(".BO", "").replace(".BSE", "")
    if sym2 != symbol:
        p2 = _store_path(sym2, interval)
        if p2.exists():
            try:
                return pd.read_parquet(p2)
            except Exception:
                pass
    return None


import datetime as dt
import pytz

IST = pytz.timezone("Asia/Kolkata")

def _market_open_ist() -> bool:
    """True if NSE is currently in its trading session."""
    now = dt.datetime.now(IST)
    if now.weekday() >= 5:          # Sat=5, Sun=6
        return False
    open_t  = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    close_t = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return open_t <= now <= close_t


# Freshness thresholds per interval (in minutes)
# During market hours intraday data expires quickly
_FRESH_TTL_MINUTES: dict = {
    "1m":   2,    # 2 minutes during market hours
    "5m":   5,
    "15m":  15,
    "1h":   30,   # re-fetch if >30 min stale during market hours
    "4h":   60,
    "1d":   1440, # 1 day — doesn't need intraday refresh
    "1wk":  4320, # 3 days
}

def _is_fresh(cached: Optional[pd.DataFrame], interval: str) -> bool:
    """
    Returns True if cached data is recent enough to skip a re-fetch.

    Logic:
      - If market is open AND interval is intraday → use per-interval TTL in minutes
      - If market is closed OR interval is daily/weekly → generous TTL (age in days)
    """
    if cached is None or cached.empty:
        return False

    last_ts = cached.index[-1]
    now     = pd.Timestamp.now()
    age_min = (now - last_ts).total_seconds() / 60

    ttl = _FRESH_TTL_MINUTES.get(interval, 1440)

    if _market_open_ist() and interval not in ("1d", "1wk"):
        # During market hours: use minute-based TTL
        return age_min <= ttl
    else:
        # Outside market hours: daily/weekly tolerance is fine
        # For intraday outside market hours: data from today's session is fresh
        age_days = (now - last_ts).days
        if interval in ("1d", "1wk"):
            return age_days <= 2
        else:
            # Good if we have data from today's market session
            return age_days == 0

# REPLACE WITH:
def list_stored(interval: Optional[str] = None) -> List[Dict]:
    results = []
    dirs = [interval] if interval else [
        d.name for d in DATA_DIR.iterdir()
        if d.is_dir() and not d.name.startswith(".")
    ]
    for itv in dirs:
        folder = DATA_DIR / itv
        if not folder.exists():
            continue
        for f in sorted(folder.glob("*.parquet")):
            try:
                df  = pd.read_parquet(f)
                # Filename is e.g. RELIANCE_1d.parquet or BAJAJ_AUTO_1d.parquet
                # Strip the _interval suffix to get the symbol as stored
                sym = f.stem[: -(len(itv) + 1)]   # e.g. "RELIANCE" or "BAJAJ_AUTO"
                results.append({
                    "symbol":    sym,          # raw stored name, no .NS
                    "interval":  itv,
                    "rows":      len(df),
                    "last_date": str(df.index[-1].date()) if len(df) else "–",
                    "size_kb":   round(f.stat().st_size / 1024, 1),
                })
            except Exception:
                pass
    return results

def delete_stored(symbol: str, interval: str) -> bool:
    p = _store_path(symbol, interval)
    if p.exists():
        p.unlink()
        return True
    return False


def storage_summary() -> Dict:
    items    = list_stored()
    by_iv: Dict[str, int] = {}
    total_kb = 0.0
    for it in items:
        by_iv[it["interval"]] = by_iv.get(it["interval"], 0) + 1
        total_kb += it["size_kb"]
    return {
        "total_symbols": len(items),
        "total_size_mb": round(total_kb / 1024, 2),
        "by_interval":   by_iv,
        "items":         items,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Upstox API client + candle fetch
# ═══════════════════════════════════════════════════════════════════════════════

def get_upstox_api_client(token: str):
    try:
        import upstox_client
        cfg = upstox_client.Configuration()
        cfg.access_token = token
        return upstox_client.HistoryV3Api(upstox_client.ApiClient(cfg))
    except ImportError:
        return None


def fetch_candle_data(
    api_instance,
    instrument_key: str,
    start: str,
    end: str,
    interval: str,
    retries: int = 3,
    delay: float = 2.0,
) -> Optional[pd.DataFrame]:
    if instrument_key is None or api_instance is None:
        return None

    interval_unit, interval_value = UPSTOX_INTERVAL_MAP.get(interval, ("days", "1"))
    last_exc = None
    for attempt in range(retries):
        try:
            response = api_instance.get_historical_candle_data1(
                instrument_key, interval_unit, interval_value, end, start
            )
            candles = response.data.candles
            if not candles:
                return None
            df = pd.DataFrame(
                candles,
                columns=["Timestamp", "Open", "High", "Low", "Close", "Volume", "Open Interest"],
            )
            df["Timestamp"] = pd.to_datetime(df["Timestamp"])
            df.set_index("Timestamp", inplace=True)
            if df.index.tz is not None:
                df.index = df.index.tz_localize(None)
            df.sort_index(inplace=True)
            df = df[~df.index.duplicated(keep="last")]
            df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
            df = df[df["Volume"] > 0]
            return _add_prev_close(df)
        except Exception as e:
            last_exc = e
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                raise last_exc   # re-raise on final attempt so caller sees it
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# OHLCV helper
# ═══════════════════════════════════════════════════════════════════════════════

def _add_prev_close(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["Prev_close"] = df["Close"].shift(1).fillna(df["Close"])
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# Public: fetch_ohlcv  (local store → Upstox only)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_ohlcv(
    ticker: str,
    interval: str = "1d",
    days: Optional[int] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    force_refresh: bool = False,
    upstox_token: Optional[str] = None,
    instrument_key: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch OHLCV for one symbol via Upstox only.
    Priority: local store (if fresh) → Upstox API.
    instrument_key is auto-resolved from ticker symbol if not provided.
    Always writes result back to data_store/.
    """

    end_str = end or dt.datetime.now().strftime("%Y-%m-%d")

    # Cap lookback to what Upstox allows per call for this interval
    _max   = UPSTOX_MAX_DAYS_PER_CALL.get(interval, 700)
    _days  = min(days or DAYS_LOOKBACK.get(interval, 500), _max)

    start_str = start or (dt.datetime.now() - dt.timedelta(days=_days)).strftime("%Y-%m-%d")

    # 1. Local store
    if not force_refresh:
        cached = load_stored_df(ticker, interval)
        if _is_fresh(cached, interval):
            return cached

    # 2. Resolve token
    token = upstox_token or load_token()
    if not token:
        print(f"  ⚠  No Upstox token — cannot fetch {ticker}")
        return pd.DataFrame()

    # 3. Auto-resolve instrument key if not given
    if not instrument_key:
        instrument_key = resolve_instrument_key(ticker)
        if not instrument_key:
            print(f"  ⚠  Instrument key not found for {ticker}")
            return pd.DataFrame()

    # 4. Fetch from Upstox — historical + today's intraday stitched
    api = get_upstox_api_client(token)
    df  = fetch_candle_data_with_today(
        api, instrument_key, start_str, end_str, interval, token=token
    )
    if df is not None and not df.empty:
        store_df(ticker, interval, df)
        return df
    return pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════════════
# Public: fetch_batch  (parallel, Upstox only, crash-safe)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_batch(
    tickers: List[str],
    interval: str = "1d",
    days: int = 500,
    max_workers: int = 2,
    force_refresh: bool = False,
    upstox_token: Optional[str] = None,
    instr_key_map: Optional[Dict[str, str]] = None,
) -> Dict[str, pd.DataFrame]:
    """
    Parallel Upstox fetch for multiple tickers.
    - Serves from local store if fresh (no network call).
    - Builds the full instrument key map ONCE for the whole batch.
    - One bad ticker never crashes the rest.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    token = upstox_token or load_token()
    if not token:
        print("  ⚠  No Upstox token — cannot fetch batch.")
        return {}

    # Build instrument key map once for all tickers
    if instr_key_map:
        key_map = instr_key_map
    else:
        nse_map = symbol_to_instrument_key("NSE")
        bse_map = symbol_to_instrument_key("BSE")
        key_map = {}
        for t in tickers:
            sym = t.replace(".NS", "").replace(".BO", "").upper()
            key_map[t] = nse_map.get(sym) or bse_map.get(sym) or ""

    result: Dict[str, pd.DataFrame] = {}

    # Serve fresh from local store — only fetch what is stale or missing
    to_fetch: List[str] = []
    for t in tickers:
        if not force_refresh:
            cached = load_stored_df(t, interval)
            if _is_fresh(cached, interval):
                result[t] = cached
                continue
        to_fetch.append(t)

    if not to_fetch:
        return result

    print(f"  Fetching {len(to_fetch)}/{len(tickers)} from Upstox ({interval})…")

    _days     = DAYS_LOOKBACK.get(interval, days)
    end_str   = dt.datetime.now().strftime("%Y-%m-%d")
    start_str = (dt.datetime.now() - dt.timedelta(days=_days)).strftime("%Y-%m-%d")
    api       = get_upstox_api_client(token)

    def _one(t: str) -> Tuple[str, Optional[pd.DataFrame]]:
        ikey = key_map.get(t, "")
        if not ikey:
            return t, None
        return t, fetch_candle_data_with_today(
            api, ikey, start_str, end_str, interval, token
        )

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_one, t): t for t in to_fetch}
        for fut in as_completed(futures):
            try:
                sym, df = fut.result()
                if df is not None and not df.empty:
                    store_df(sym, interval, df)
                    result[sym] = df
                else:
                    print(f"  ⚠  No data for {futures[fut]}")
            except Exception as e:
                print(f"  ⚠  [{futures[fut]}]: {e}")
            time.sleep(0.1)

    return result



# data_fetch/data_fetch.py

# Upstox intraday only supports these two intervals — everything else
# must be derived by resampling 1m data locally after fetching
UPSTOX_INTRADAY_INTERVAL_MAP: Dict[str, str] = {
    "1m":  "1minute",
    "5m":  "1minute",   # fetch 1m, resample to 5m locally
    "15m": "1minute",   # fetch 1m, resample to 15m locally
    "1h":  "30minute",  # fetch 30m, resample to 1h locally
    "4h":  "30minute",  # fetch 30m, resample to 4h locally
    "1d":  "30minute",  # fetch 30m, resample to daily locally
    "1wk": "30minute",  # fetch 30m, resample to weekly locally
}

# Resample rules: map your app interval → pandas resample offset
INTRADAY_RESAMPLE_MAP: Dict[str, Optional[str]] = {
    "1m":  None,    # no resample needed
    "5m":  "5min",
    "15m": "15min",
    "1h":  "60min",
    "4h":  "240min",
    "1d":  "1D",
    "1wk": "1W",
}

# Upstox intraday bars use 09:00 open-time boundaries after resample,
# but historical bars are labeled at 09:15 (NSE session start).
# Shift intraday timestamps after resample to align with historical.
INTRADAY_TIMESTAMP_OFFSET: Dict[str, int] = {
    # 1m/5m/15m: fetched as 1minute raw which Upstox labels from 09:15 (NSE open).
    # Resampling 1m→5m or 1m→15m preserves the 09:15 start — no offset needed.
    "1m":  0,
    "5m":  0,
    "15m": 0,
    # 1h/4h: fetched as 30minute which Upstox labels from 09:00 (clock boundary).
    # After resample the first bar is 09:00, but historical API labels it 09:15.
    # Shift +15min to align.
    "1h":  15,
    "4h":  15,
    "1d":  0,    # daily — only date matters, not time
    "1wk": 0,
}


def fetch_intraday_candle_data(
    token: str,
    instrument_key: str,
    interval: str,
) -> Optional[pd.DataFrame]:
    """
    Fetches TODAY's intraday candles via Upstox REST v2.

    Upstox intraday endpoint ONLY supports: 1minute, 30minute.
    All other intervals are derived by resampling locally after fetch.

    Endpoint: GET /v2/historical-candle/intraday/{instrument_key}/{interval}
    Returns None silently when market is closed or pre-open.
    """
    intraday_interval = UPSTOX_INTRADAY_INTERVAL_MAP.get(interval)
    if not intraday_interval:
        return None

    from urllib.parse import quote
    encoded_key = quote(instrument_key, safe="")
    url = (
        f"https://api.upstox.com/v2/historical-candle/intraday"
        f"/{encoded_key}/{intraday_interval}"
    )

    try:
        resp = requests.get(
            url,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            timeout=10,
        )

        if resp.status_code == 200:
            candles = resp.json().get("data", {}).get("candles", [])
            if not candles:
                return None  # pre-open or market closed — normal, not an error

            df = pd.DataFrame(
                candles,
                columns=["Timestamp", "Open", "High", "Low", "Close", "Volume", "Open Interest"],
            )
            df["Timestamp"] = pd.to_datetime(df["Timestamp"])
            df.set_index("Timestamp", inplace=True)
            if df.index.tz is not None:
                df.index = df.index.tz_localize(None)
            df.sort_index(inplace=True)
            df = df[~df.index.duplicated(keep="last")]
            df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
            df = df[df["Volume"] > 0]
            ist_offset   = dt.timezone(dt.timedelta(hours=5, minutes=30))
            today_ist    = dt.datetime.now(ist_offset).date()
            market_close = pd.Timestamp(f"{today_ist} 15:30:00")
            df = df[df.index < market_close]
            if df.empty:
                return None

            # Resample to target interval if needed
            resample_to = INTRADAY_RESAMPLE_MAP.get(interval)
            if resample_to:
                df = _resample_ohlcv(df, resample_to)

            # Align with Upstox historical timestamp convention (+15min for intraday)
            offset_min = INTRADAY_TIMESTAMP_OFFSET.get(interval, 0)
            if offset_min:
                df.index = df.index + pd.Timedelta(minutes=offset_min)

            return df if not df.empty else None

        elif resp.status_code in (400, 404):
            try:
                err_body = resp.json()
            except Exception:
                err_body = resp.text
            print(f"  ⚠  Intraday {resp.status_code} for {instrument_key} "
                  f"[interval={intraday_interval}]: {err_body}")
            return None
        else:
            print(f"  ⚠  Intraday API {resp.status_code} for {instrument_key}")
            return None

    except requests.exceptions.Timeout:
        print(f"  ⚠  Intraday fetch timeout for {instrument_key}")
        return None
    except Exception as e:
        print(f"  ⚠  Intraday fetch failed for {instrument_key}: {e}")
        return None


def _resample_ohlcv(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    """
    Resample a minute-bar DataFrame to a coarser interval.
    Standard OHLCV aggregation: Open=first, High=max, Low=min,
    Close=last, Volume=sum. Drops incomplete trailing candles.
    """
    resampled = df.resample(rule, label="left", closed="left").agg({
        "Open":   "first",
        "High":   "max",
        "Low":    "min",
        "Close":  "last",
        "Volume": "sum",
    }).dropna(subset=["Open"])

    # Drop the last candle — it's the current in-progress bar (incomplete)
    # Exception: for 1d/1wk we keep it since the whole point is to have today
    if rule not in ("1D", "1W") and len(resampled) > 1 and _market_open_ist():
        resampled = resampled.iloc[:-1]

    return resampled


def resample_intraday_to_daily(intraday_df: pd.DataFrame) -> pd.DataFrame:
    """
    Collapses today's intraday bars into a single daily candle.
    Used to append 'today so far' to a historical daily series.
    """
    if intraday_df is None or intraday_df.empty:
        return pd.DataFrame()

    daily = intraday_df.resample("1D").agg({
        "Open":   "first",
        "High":   "max",
        "Low":    "min",
        "Close":  "last",
        "Volume": "sum",
    }).dropna()
    return daily


def fetch_candle_data_with_today(
    api_instance,
    instrument_key: str,
    start: str,
    end: str,
    interval: str,
    token: str = "",   
    retries: int = 3,
    delay: float = 2.0,
) -> Optional[pd.DataFrame]:
    """
    Full fetch: historical (up to yesterday) + today's intraday stitched together.
    
    Strategy:
      1. Fetch historical candles via get_historical_candle_data1
      2. Fetch today's session via get_intraday_candle_data
      3. If interval is daily, resample intraday → single daily candle
      4. Concatenate, deduplicate (intraday wins for today's date)
      5. Return clean unified DataFrame
    
    This is the correct production pattern for Upstox.
    """
    # Step 1: Historical data (yesterday and before)
    hist_df = fetch_candle_data(
        api_instance, instrument_key, start, end, interval, retries, delay
    )

    # Step 2: Today's intraday data
    intra_df = None
    if token:
        intra_df = fetch_intraday_candle_data(token, instrument_key, interval)

    if intra_df is None or intra_df.empty:
        return hist_df  # market closed / outside hours — historical is fine

    # Step 3: For daily interval, collapse intraday to a single daily candle
    if interval in ("1d", "1wk"):
        today_df = resample_intraday_to_daily(intra_df)
    else:
        today_df = intra_df

    # Step 4: Stitch — historical base, today appended on top
    if hist_df is None or hist_df.empty:
        combined = today_df
    else:
        combined = pd.concat([hist_df, today_df])

    combined = combined[~combined.index.duplicated(keep="last")]
    combined.sort_index(inplace=True)
    combined = _add_prev_close(combined)
    return combined


# ═══════════════════════════════════════════════════════════════════════════════
# Upstox batch generator  (powers SSE progress stream on Data Fetch page)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_upstox_batch_iter(
    instrument_keys: List[str],
    interval: str,
    token: str,
    max_workers: int = 2,
    batch_size: int = 50,       # process in small batches to avoid memory overload
) -> Generator[Tuple[int, int, str, bool], None, None]:
    """
    Yields (done, total, symbol, ok) for SSE streaming.
    Processes in batches of batch_size to avoid overwhelming Upstox API.
    """
    _max      = UPSTOX_MAX_DAYS_PER_CALL.get(interval, 700)
    _days     = min(DAYS_LOOKBACK.get(interval, 700), _max)
    end_str   = dt.datetime.now().strftime("%Y-%m-%d")
    start_str = (dt.datetime.now() - dt.timedelta(days=_days)).strftime("%Y-%m-%d")
    sym_map   = instrument_key_to_symbol()
    api       = get_upstox_api_client(token)
    total     = len(instrument_keys)
    done      = 0

    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Process in small batches — avoids submitting 3000 futures at once
    for batch_start in range(0, total, batch_size):
        batch = instrument_keys[batch_start: batch_start + batch_size]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_key = {
                executor.submit(
                    fetch_candle_data_with_today,
                    api, ikey, start_str, end_str, interval, token
                ): ikey
                for ikey in batch
            }
            for future in as_completed(future_to_key):
                ikey    = future_to_key[future]
                sym     = sym_map.get(ikey, ikey)
                done   += 1
                err_msg = ""
                try:
                    df = future.result()
                    ok = df is not None and not df.empty
                    if ok:
                        store_df(sym, interval, df)
                    elif df is None:
                        err_msg = "no data returned"
                    else:
                        err_msg = "empty dataframe"
                except Exception as e:
                    ok      = False
                    err_msg = str(e)
                yield done, total, sym, ok, err_msg
                time.sleep(0.05)   # small delay between results

# ═══════════════════════════════════════════════════════════════════════════════
# Watchlists
# ═══════════════════════════════════════════════════════════════════════════════

NIFTY50 = [
    "RELIANCE.NS","TCS.NS","HDFCBANK.NS","ICICIBANK.NS","INFY.NS",
    "HINDUNILVR.NS","SBIN.NS","BAJFINANCE.NS","BHARTIARTL.NS","KOTAKBANK.NS",
    "LT.NS","AXISBANK.NS","ASIANPAINT.NS","MARUTI.NS","TITAN.NS",
    "SUNPHARMA.NS","ULTRACEMCO.NS","NESTLEIND.NS","WIPRO.NS","ITC.NS",
    "NTPC.NS","POWERGRID.NS","HCLTECH.NS","ONGC.NS","COALINDIA.NS",
    "JSWSTEEL.NS","TATAMOTORS.NS","TATASTEEL.NS","BPCL.NS","GRASIM.NS",
    "INDUSINDBK.NS","TECHM.NS","ADANIPORTS.NS","HINDALCO.NS","DRREDDY.NS",
    "DIVISLAB.NS","CIPLA.NS","EICHERMOT.NS","BAJAJ-AUTO.NS","HEROMOTOCO.NS",
    "BRITANNIA.NS","APOLLOHOSP.NS","M&M.NS","TATACONSUM.NS","SHRIRAMFIN.NS",
    "SBILIFE.NS","HDFCLIFE.NS","BAJAJFINSV.NS","ADANIENT.NS","LTIM.NS",
]
NIFTY_NEXT50 = [
    "IRCTC.NS","RVNL.NS","MOTHERSON.NS","TRENT.NS","JYOTICNC.NS",
    "ZOMATO.NS","NYKAA.NS","PAYTM.NS","DELHIVERY.NS","PFC.NS",
    "RECLTD.NS","CANBK.NS","BANKBARODA.NS","IOC.NS","GAIL.NS",
    "SAIL.NS","TITAGARH.NS","BEL.NS","HAL.NS","BHEL.NS",
    "IRFC.NS","NMDC.NS","NLCINDIA.NS","OIL.NS","CONCOR.NS",
]
NIFTY_MIDCAP = [
    "POLYCAB.NS","VOLTAS.NS","MPHASIS.NS","COFORGE.NS","PERSISTENT.NS",
    "LICI.NS","NAUKRI.NS","ASTRAL.NS","PIIND.NS","AARTIIND.NS",
]


def get_watchlist(name: str = "nifty50") -> List[str]:
    return {
        "nifty50":   NIFTY50,
        "next50":    NIFTY_NEXT50,
        "midcap":    NIFTY_MIDCAP,
        "all_nifty": NIFTY50 + NIFTY_NEXT50 + NIFTY_MIDCAP,
    }.get(name.lower(), NIFTY50)
