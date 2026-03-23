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

warnings.simplefilter("ignore", FutureWarning)
warnings.simplefilter("ignore", DeprecationWarning)

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
    "5m":  ("minutes", "5"),
    "15m": ("minutes", "15"),
    "1h":  ("minutes", "60"),
    "4h":  ("minutes", "240"),
    "1d":  ("days",    "1"),
    "1wk": ("weeks",   "1"),
}

DAYS_LOOKBACK: Dict[str, int] = {
    "5m": 5, "15m": 10, "1h": 30, "4h": 90, "1d": 700, "1wk": 2000,
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
    """Build all lookup maps from instrument master in one pass."""
    global _KEY_MAP, _NSE_SYM_MAP, _BSE_SYM_MAP
    _KEY_MAP = dict(zip(df["instrument_key"], df["trading_symbol"]))
    eq_df    = df[df["instrument_type"].isin(["EQ", "BE", "SM"])]
    nse      = eq_df[eq_df["exchange"] == "NSE"]
    bse      = eq_df[eq_df["exchange"] == "BSE"]
    _NSE_SYM_MAP = dict(zip(nse["trading_symbol"], nse["instrument_key"]))
    _BSE_SYM_MAP = dict(zip(bse["trading_symbol"], bse["instrument_key"]))


def get_instrument_df(force_refresh: bool = False) -> pd.DataFrame:
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

    resp = requests.get(UPSTOX_INSTR_URL, timeout=30)
    resp.raise_for_status()
    with gzip.GzipFile(fileobj=BytesIO(resp.content)) as gz:
        data = json.loads(gz.read())

    _INSTRUMENT_DF = pd.DataFrame(data)
    _INSTRUMENT_DF.to_parquet(cache)
    _build_maps(_INSTRUMENT_DF)
    return _INSTRUMENT_DF


def get_eq_instruments(exchanges: Tuple[str, ...] = ("NSE", "BSE")) -> pd.DataFrame:
    """Filter instrument master for EQ/BE/SM. Upstox uses 'NSE'/'BSE'."""
    df = get_instrument_df()
    return df[
        df["instrument_type"].isin(["EQ", "BE", "SM"]) &
        df["exchange"].isin(exchanges)
    ].copy()


def instrument_key_to_symbol() -> Dict[str, str]:
    """instrument_key → trading_symbol"""
    global _KEY_MAP
    if _KEY_MAP is None:
        get_instrument_df()
    return _KEY_MAP or {}


def symbol_to_instrument_key(exchange: str = "NSE") -> Dict[str, str]:
    """trading_symbol → instrument_key for the given exchange."""
    global _NSE_SYM_MAP, _BSE_SYM_MAP
    if _NSE_SYM_MAP is None:
        get_instrument_df()
    return _NSE_SYM_MAP if exchange == "NSE" else (_BSE_SYM_MAP or {})


def resolve_instrument_key(ticker: str) -> Optional[str]:
    """
    Auto-resolve instrument key from a ticker like 'RELIANCE.NS' or 'RELIANCE'.
    Tries NSE first, then BSE.
    """
    sym = ticker.replace(".NS", "").replace(".BO", "").replace(".BSE", "").upper()
    key = symbol_to_instrument_key("NSE").get(sym)
    if not key:
        key = symbol_to_instrument_key("BSE").get(sym)
    return key


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


def _is_fresh(cached: Optional[pd.DataFrame], interval: str) -> bool:
    """True if cached data is recent enough to skip re-fetch."""
    if cached is None or cached.empty:
        return False
    age_days = (pd.Timestamp.now() - cached.index[-1]).days
    stale    = 2 if interval in ("1d", "1wk") else 0
    return age_days <= stale


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
            if attempt < retries - 1:
                time.sleep(delay)
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
    end_str   = end   or dt.datetime.now().strftime("%Y-%m-%d")
    _days     = days  or DAYS_LOOKBACK.get(interval, 500)
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

    # 4. Fetch from Upstox
    api = get_upstox_api_client(token)
    df  = fetch_candle_data(api, instrument_key, start_str, end_str, interval)

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
        return t, fetch_candle_data(api, ikey, start_str, end_str, interval)

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
    _days     = DAYS_LOOKBACK.get(interval, 700)
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
                    fetch_candle_data, api, ikey, start_str, end_str, interval
                ): ikey
                for ikey in batch
            }
            for future in as_completed(future_to_key):
                ikey = future_to_key[future]
                sym  = sym_map.get(ikey, ikey)
                done += 1
                try:
                    df = future.result()
                    ok = df is not None and not df.empty
                    if ok:
                        store_df(sym, interval, df)
                except Exception:
                    ok = False
                yield done, total, sym, ok
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
