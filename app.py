"""
app.py  –  QuantScanner v3.1
=============================
Background fetch task flow:
  POST /api/data/fetch/start   → launches server-side thread, returns task_id
  GET  /api/data/fetch/status  → poll for progress (survives page navigation)
  POST /api/data/fetch/cancel  → graceful cancel

The fetch runs entirely on the server. Navigating away and back does NOT
interrupt it — the thread keeps running and the frontend reconnects by
polling /status with the task_id stored in localStorage.

Changes in this revision (v3.1 → cleaned):
  - Removed duplicate _load_keys() (was defined 3×); now imported from core.config
  - Merged duplicate crypto_fetch import blocks into one
  - Removed dead rename_map block inside _load_crypto_df (shadowed by module-level constant)
  - Removed duplicate _safe() / _sr() / _candles() / _serialise_zones() helpers;
    now imported from core.serializers
  - Removed duplicate _parse_states() inside crypto_scan(); uses parse_states() from core.serializers
  - Collapsed 3× repeated auth URL f-string into _auth_url() helper
  - Removed duplicate `from algos.pivot_engine import extrems, detect_trend, compute_atr, tag_signals`
  - Moved __main__ port-kill logic to run.py; __main__ block is now 2 lines
"""

from __future__ import annotations
import sys, os, json, time, threading, uuid

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

# ── Core config — replaces all _load_keys() calls ─────────────────────────────
from core.config      import UPSTOX_KEY, UPSTOX_SECRET, UPSTOX_REDIRECT, PORT, ROOT
from core.serializers import safe_dict, candles, serialise_zones, parse_states

# ── Flask ──────────────────────────────────────────────────────────────────────
from flask import Flask, jsonify, request, send_from_directory, Response, redirect
from flask_cors import CORS
import pandas as pd
import numpy as np

# ── Algos ──────────────────────────────────────────────────────────────────────
from algos.pivot_engine import (
    extrems, detect_trend, compute_atr, tag_signals,
    compute_pivot_velocity, compute_pivot_amplitude, compute_leg_ratio,
    UPTREND_LABELS, DOWNTREND_LABELS, _trend_matches,
)
from algos.trend_analysis import get_multiorder_structure, STATE_GROUPS, STATE_LABELS
from algos.zones          import scan_zones, buy_zone, sell_zone
from algos.confluence     import confluence_score, measure_edge
from algos.patterns       import detect_flag, detect_triangle, pivot_trend_strength
from algos.context        import SymbolContext

# ── Data ───────────────────────────────────────────────────────────────────────
from data_fetch.data_fetch import (
    fetch_ohlcv, fetch_batch, get_watchlist,
    list_stored, load_stored_df,
    load_token, save_token, is_token_valid,
    delete_stored, storage_summary,
    get_eq_instruments, instrument_key_to_symbol,
    fetch_upstox_batch_iter, _is_fresh,
    UPSTOX_AUTH_URL, UPSTOX_TOKEN_URL,
)
# Single merged import block — was duplicated with overlapping names
from data_fetch.crypto_fetch import (
    fetch_crypto_batch_iter,
    load_crypto_ohlcv,
    list_crypto_stored,
    delete_crypto_stored,
    CRYPTO_SYMBOLS,
    DERIVED_INTERVALS,
    _file_key,
)
from scanner      import scan_watchlist
from utils.logger import get_logger, read_log_tail, log_stats

log = get_logger(__name__)
log.info("QuantScanner starting", version="3.1")

FRONTEND_DIR = os.path.join(ROOT, "frontend")
app = Flask(__name__, static_folder=FRONTEND_DIR, static_url_path="")
CORS(app)

# ─────────────────────────────────────────────────────────────────────────────
# BACKGROUND FETCH TASK STATE
# Lives in the server process — completely unaffected by page navigation.
# ─────────────────────────────────────────────────────────────────────────────
_fetch_tasks:    dict = {}
_fetch_lock              = threading.Lock()
_active_task_id: str | None = None


# ─────────────────────────────────────────────────────────────────────────────
# Crypto helpers
# ─────────────────────────────────────────────────────────────────────────────

# Maps equity-style TF strings to crypto TF strings where they differ
_EQUITY_TO_CRYPTO_TF = {"1wk": "1w"}

# Column map: Binance stores lowercase, all algos expect Title Case
_CRYPTO_COL_RENAME = {
    "open": "Open", "high": "High",
    "low":  "Low",  "close": "Close", "volume": "Volume",
}

def _crypto_base(raw: str) -> str:
    """
    Normalise any crypto ticker string to a clean base symbol.

    Handles every format the frontend or scanner might send:
        "BTCUSDT_PERP"  → "BTC"
        "BTCUSDT"       → "BTC"
        "BTC_PERP"      → "BTC"
        "BTC"           → "BTC"
    """
    return (
        raw.upper()
           .replace("USDT_PERP", "")
           .replace("USDT",      "")
           .replace("_PERP",     "")
           .strip()
    )

def _load_crypto_df(base: str, interval: str) -> "pd.DataFrame | None":
    """
    Load crypto OHLCV and rename columns to Title Case so pivot_engine,
    scan_zones, compute_atr and all other algos work without modification.
    Also normalises equity-style TF strings (1wk → 1w).
    """
    interval = _EQUITY_TO_CRYPTO_TF.get(interval, interval)
    try:
        df = load_crypto_ohlcv(base, interval)
    except Exception:
        return None
    if df is None or df.empty:
        return None
    # Dead code removed: inline rename_map that was shadowed by _CRYPTO_COL_RENAME above
    return df.rename(columns={k: v for k, v in _CRYPTO_COL_RENAME.items() if k in df.columns})


# ─────────────────────────────────────────────────────────────────────────────
# Auth URL helper — was copy-pasted 3× across auth routes
# ─────────────────────────────────────────────────────────────────────────────

def _auth_url() -> str:
    return (
        f"{UPSTOX_AUTH_URL}?response_type=code"
        f"&client_id={UPSTOX_KEY}&redirect_uri={UPSTOX_REDIRECT}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Background fetch task
# ─────────────────────────────────────────────────────────────────────────────

def _run_fetch_task(
    task_id:        str,
    tf:             str,
    src:            str,
    universe:       str,
    wl:             str,
    force:          bool,
    token:          str,
    crypto_symbols: list | None = None,
    lookback_days:  int         = 730,
):
    """
    Daemon thread: runs the full fetch, writes progress into _fetch_tasks[task_id].
    The HTTP server keeps serving /api/data/fetch/status throughout.

    Supports three sources:
        "upstox"   — NSE/BSE equities via Upstox API
        "yfinance" — equities via yfinance
        "crypto"   — Binance USDT-M perpetual 1m OHLCV (no auth required)
    """
    def upd(**kw):
        with _fetch_lock:
            for k, v in kw.items():
                _fetch_tasks[task_id][k] = v

    def task_log(msg: str, kind: str = "inf"):
        with _fetch_lock:
            entries = _fetch_tasks[task_id]["log"]
            entries.append({"msg": msg, "kind": kind})
            if len(entries) > 500:
                _fetch_tasks[task_id]["log"] = entries[-500:]

    try:
        upd(status="running", title=f"Starting {src} fetch…")
        task_log(f"Starting {src} fetch — {tf if src != 'crypto' else '1m canonical'}", "inf")

        # ── UPSTOX ────────────────────────────────────────────────────────────
        if src == "upstox":
            ok_tok, _ = is_token_valid(token)
            if not ok_tok:
                upd(status="error", title="Token expired — re-authenticate.")
                task_log("Token expired — re-authenticate.", "err")
                return

            task_log("Downloading instrument master…", "inf")
            segments = tuple(e.strip() for e in universe.split(","))
            instr    = get_eq_instruments(segments)
            keys     = instr["instrument_key"].tolist()
            upd(total=len(keys))
            task_log(f"Found {len(keys)} instruments in {universe}", "inf")

            if not keys:
                upd(status="error", title=f"0 instruments found for {universe}")
                task_log(f"0 instruments found for {universe}", "err")
                return

            ok_n = err_n = 0
            for done, total, sym, ok, err_msg in fetch_upstox_batch_iter(keys, tf, token):
                if _fetch_tasks[task_id].get("status") == "cancelled":
                    task_log("Cancelled by user.", "inf")
                    return
                if ok:
                    ok_n += 1
                    task_log(f"✓ {sym}", "ok")
                else:
                    err_n += 1
                    task_log(f"✗ {sym}" + (f" — {err_msg}" if err_msg else ""), "err")
                upd(done=done, total=total, ok_count=ok_n, err_count=err_n)

        # ── YFINANCE ──────────────────────────────────────────────────────────
        elif src == "yfinance":
            tickers = get_watchlist(wl)
            total   = len(tickers)
            upd(total=total)
            task_log(f"Fetching {total} tickers via yfinance ({tf})…", "inf")
            ok_n = err_n = 0
            for i, ticker in enumerate(tickers, 1):
                if _fetch_tasks[task_id].get("status") == "cancelled":
                    task_log("Cancelled by user.", "inf")
                    return
                try:
                    df = fetch_ohlcv(ticker, interval=tf, force_refresh=force)
                    ok = not df.empty
                except Exception as e:
                    ok = False
                    task_log(f"✗ {ticker} — {e}", "err")
                if ok:
                    ok_n += 1
                    task_log(f"✓ {ticker}", "ok")
                else:
                    err_n += 1
                upd(done=i, total=total, ok_count=ok_n, err_count=err_n)
                time.sleep(0.05)

        # ── CRYPTO (Binance USDT-M perpetuals) ────────────────────────────────
        elif src == "crypto":
            targets = crypto_symbols or CRYPTO_SYMBOLS
            upd(total=len(targets))
            task_log(
                f"Binance Perp fetch — 1m canonical store  |  "
                f"{len(targets)} symbols  |  "
                f"lookback: {lookback_days}d (initial load only, subsequent runs append)",
                "inf"
            )
            task_log("No API key required — Binance public market data.", "inf")

            ok_n = err_n = 0
            for done, total, sym_key, ok in fetch_crypto_batch_iter(
                symbols=targets,
                lookback_days=lookback_days,
            ):
                if _fetch_tasks[task_id].get("status") == "cancelled":
                    task_log("Cancelled by user.", "inf")
                    return
                if ok:
                    ok_n += 1
                    task_log(f"✓ {sym_key}", "ok")
                else:
                    err_n += 1
                    task_log(f"✗ {sym_key}", "err")
                upd(done=done, total=total, ok_count=ok_n, err_count=err_n)

        else:
            upd(status="error", title=f"Unknown source: {src}")
            task_log(f"Unknown source '{src}'", "err")
            return

        # ── DONE ──────────────────────────────────────────────────────────────
        ok_n  = _fetch_tasks[task_id]["ok_count"]
        err_n = _fetch_tasks[task_id]["err_count"]
        final = f"Done — {ok_n} OK · {err_n} errors"
        upd(status="done", title=final)
        task_log(f"✅ {final}", "inf")

    except Exception as exc:
        import traceback as _tb
        msg = f"Fatal error: {exc}"
        upd(status="error", title=msg)
        task_log(msg, "err")
        task_log(_tb.format_exc(), "err")


# ─────────────────────────────────────────────────────────────────────────────
# Frontend pages
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/")
def page_scanner(): return send_from_directory(FRONTEND_DIR, "pages/index.html")

@app.route("/trend")
def page_trend():   return send_from_directory(FRONTEND_DIR, "pages/trend_viewer.html")

@app.route("/data")
def page_data():    return send_from_directory(FRONTEND_DIR, "pages/data_fetch.html")

@app.route("/<path:filename>")
def static_files(filename): return send_from_directory(FRONTEND_DIR, filename)


# ─────────────────────────────────────────────────────────────────────────────
# Health & logging
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/health")
def health(): return jsonify({"status": "ok", "version": "3.1"})


@app.route("/api/log", methods=["POST"])
def receive_frontend_log():
    """Receives log events from frontend JS. Written to rotating log with [FE] tag."""
    try:
        data    = request.get_json(silent=True) or {}
        level   = data.get("level",   "info").lower()
        module  = f"[FE] {data.get('module', 'frontend')}"
        message = data.get("message", "(no message)")
        context = data.get("context", {})

        fe_log = get_logger(module)
        if level == "error":
            fe_log.error(message, **context)
        elif level in ("warn", "warning"):
            fe_log.warning(message, **context)
        elif level == "debug":
            fe_log.debug(message, **context)
        else:
            fe_log.info(message, **context)

        return jsonify({"ok": True})
    except Exception as e:
        log.error("Frontend log endpoint failed", error=str(e))
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/logs")
def get_logs():
    """Returns last N log lines. Query params: n (default 200), level (filter)."""
    try:
        n            = min(int(request.args.get("n", 200)), 1000)
        level_filter = request.args.get("level", "").strip().upper()
        lines        = read_log_tail(n_lines=n, level_filter=level_filter)
        return jsonify({
            "lines":    lines,
            "count":    len(lines),
            "log_file": str(log_stats()["log_file"]),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/logs/stats")
def get_log_stats():
    """Returns log rotation status and file sizes."""
    try:
        stats = log_stats()
        stats["total_size"] = sum(f["size"] for f in stats["files"])
        return jsonify(stats)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────────────────────────────────────
# Scanner
# ─────────────────────────────────────────────────────────────────────────────

def _parse_scan_params(args) -> dict:
    """
    Shared param parser for /api/scan and /api/crypto/scan.
    Accepts request.args (ImmutableMultiDict) or any dict-like.
    Returns a dict of typed, validated scan parameters.
    """
    atr_zone_types_raw = args.get("atr_zone_types", "").strip()
    return {
        "direction":       args.get("direction",       "buy"),
        "interval":        args.get("interval",        "1d"),
        "order":           int(args.get("order",       5)),
        "zone_lookback":   int(args.get("zone_lookback",  20)),
        "legout_mult":     float(args.get("legout_mult",  1.35)),
        "trend_filter":    args.get("trend_filter",    ""),
        "strategy":        args.get("strategy",        "atr"),
        "multi_order":     args.get("multi_order", "false").lower() == "true",
        "order_low":       int(args.get("order_low",   5)),
        "order_mid":       int(args.get("order_mid",   10)),
        "order_high":      int(args.get("order_high",  20)),
        "atr_zone_types":  (
            [t.strip() for t in atr_zone_types_raw.split(",") if t.strip()]
            if atr_zone_types_raw else None
        ),
        "structure_low":   parse_states(args.get("structure_low",  "")),
        "structure_mid":   parse_states(args.get("structure_mid",  "")),
        "structure_high":  parse_states(args.get("structure_high", "")),
        "alignment_filter": args.get("alignment_filter", "any"),
    }


@app.route("/api/scan")
def scan():
    import time as _time
    _t0 = _time.perf_counter()

    p       = _parse_scan_params(request.args)
    tf_list = [t.strip() for t in p["trend_filter"].split(",") if t.strip()] or None

    log.info(
        "Scan request received",
        direction=p["direction"], interval=p["interval"],
        strategy=p["strategy"],   multi_order=p["multi_order"],
        trend_filter=tf_list or "any",
        order=p["order"],
    )

    stored = list_stored(p["interval"])
    if not stored:
        return jsonify({
            "results": [], "count": 0,
            "error": f"No data stored for interval '{p['interval']}'."
        })

    data_d = {}
    for item in stored:
        sym = item["symbol"]
        df  = load_stored_df(sym, p["interval"])
        if df is not None and not df.empty:
            data_d[sym] = df

    if not data_d:
        return jsonify({"results": [], "count": 0})

    df_res = scan_watchlist(
        tickers=list(data_d.keys()),
        data_dict=data_d,
        order=p["order"],
        zone_lookback=p["zone_lookback"],
        direction=p["direction"],
        trend_filter=tf_list,
        legout_mult=p["legout_mult"],
        strategy=p["strategy"],
        atr_zone_types=p["atr_zone_types"],
        multi_order=p["multi_order"],
        order_low=p["order_low"],
        order_mid=p["order_mid"],
        order_high=p["order_high"],
        structure_filter_low=p["structure_low"]   or None,
        structure_filter_mid=p["structure_mid"]   or None,
        structure_filter_high=p["structure_high"] or None,
        alignment_filter=p["alignment_filter"],
        interval=p["interval"],
    )

    if df_res.empty:
        return jsonify({"results": [], "count": 0})

    records = [safe_dict(r) for r in df_res.to_dict(orient="records")]
    log.info(
        "Scan response sent",
        matched=len(records),
        interval=p["interval"],
        elapsed_ms=round((_time.perf_counter() - _t0) * 1000),
    )
    return jsonify({"results": records, "count": len(records)})


@app.route("/api/structure_states")
def structure_states():
    """
    Returns state groups and labels for the frontend dropdowns.
    The frontend calls this once on load to populate the structure
    filter multi-selects without hardcoding state strings in JS.
    """
    return jsonify({
        "groups": {
            group: [
                {"value": state, "label": STATE_LABELS[state]}
                for state in states
            ]
            for group, states in STATE_GROUPS.items()
        },
        "all": [
            {"value": s, "label": STATE_LABELS[s]}
            for s in STATE_LABELS
            if s != "no_structure"
        ],
    })


@app.route("/api/symbols")
def symbols():
    interval = request.args.get("interval", "1d")
    items    = list_stored(interval)
    syms     = sorted([it["symbol"] for it in items])
    return jsonify({"symbols": syms, "count": len(syms)})


# ─────────────────────────────────────────────────────────────────────────────
# Chart data
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/chart/<path:ticker>")
def chart_data(ticker):
    interval         = request.args.get("interval",      "1d")
    days             = int(request.args.get("days",       300))
    order            = int(request.args.get("order",      5))
    legout_mult      = float(request.args.get("legout_mult", 1.35))
    max_base_candles = int(request.args.get("max_base",   8))
    strategy         = request.args.get("strategy",      "atr")
    zone_lookback    = int(request.args.get("zone_lookback", 0))
    multi_order      = request.args.get("multi_order", "false").lower() == "true"
    order_low        = int(request.args.get("order_low",  5))
    order_mid        = int(request.args.get("order_mid",  10))
    order_high       = int(request.args.get("order_high", 20))

    df = load_stored_df(ticker, interval)
    if df is None or df.empty or not _is_fresh(df, interval):
        df = fetch_ohlcv(ticker, interval=interval, days=days)
    if df is None or df.empty:
        return jsonify({"error": f"No data for {ticker}"}), 404

    pvts  = extrems(df, order=order)
    trend = detect_trend(pvts)

    pivot_list = [
        {"time":  int(pd.Timestamp(df.index[i]).timestamp()),
         "value": round(v, 2), "type": t}
        for i, v, t in pvts
    ]

    include_atr    = strategy in ("atr",  "both")
    include_consol = strategy in ("consolidation", "both")
    start_idx      = max(0, len(df) - zone_lookback) if zone_lookback > 0 else 0

    try:
        buy_zones_raw = scan_zones(
            df, direction="buy", legout_mult=legout_mult,
            max_base_candles=max_base_candles, start_idx=start_idx,
            include_atr=include_atr, include_consolidation=include_consol,
        )
    except Exception as e:
        print(f"[chart] buy scan_zones error: {e}")
        buy_zones_raw = []

    try:
        sell_zones_raw = scan_zones(
            df, direction="sell", legout_mult=legout_mult,
            max_base_candles=max_base_candles, start_idx=start_idx,
            include_atr=include_atr, include_consolidation=include_consol,
        )
    except Exception as e:
        print(f"[chart] sell scan_zones error: {e}")
        sell_zones_raw = []

    multiorder_data = None
    if multi_order:
        try:
            mo = get_multiorder_structure(
                df, order_low=order_low, order_mid=order_mid, order_high=order_high
            )
            multiorder_data = {
                "low":            _build_mo_level(mo["low"]),
                "mid":            _build_mo_level(mo["mid"]),
                "high":           _build_mo_level(mo["high"]),
                "alignment":      mo["alignment"],
                "combined_trend": mo.get("combined_trend", ""),
            }
        except Exception as e:
            print(f"[chart] multiorder error: {e}")

    payload = {
        "ticker":     ticker,
        "interval":   interval,
        "candles":    candles(df),
        "pivots":     pivot_list,
        "buy_zones":  serialise_zones(buy_zones_raw),
        "sell_zones": serialise_zones(sell_zones_raw),
        "trend":      trend,
        "meta": {
            "data_source":  "cache",
            "last_candle":  str(df.index[-1]) if not df.empty else None,
            "candle_count": len(df),
            "interval":     interval,
        },
    }

    if multiorder_data:
        payload["multiorder"] = multiorder_data

    if multi_order:
        mp = {}
        for lbl, ord_val in [("H", order_high), ("M", order_mid), ("L", order_low)]:
            mp[lbl] = [
                {
                    "time":  int(pd.Timestamp(df.index[i]).timestamp()),
                    "value": round(float(v), 2),
                    "type":  t,
                }
                for i, v, t in extrems(df, order=ord_val)
            ]
        payload["multiorder_pivots"] = mp

    return jsonify(payload)


# ─────────────────────────────────────────────────────────────────────────────
# Multi-order level serialiser — shared by chart_data and _trend
# ─────────────────────────────────────────────────────────────────────────────

def _build_mo_level(v: dict) -> dict:
    """
    Serialise a single multi-order level dict (low / mid / high) to a
    JSON-safe structure. Called from both chart_data() and _trend() so the
    shape is always identical — was duplicated inline in both previously.
    """
    vel = v.get("velocity",  {})
    amp = v.get("amplitude", {})
    lr  = v.get("leg_ratio", {})
    return {
        "order":       v.get("order", 0),
        "trend":       v.get("trend", ""),
        "score":       v.get("score", 0),
        "pivot_count": v.get("pivot_count", 0),
        "velocity": {
            "current_vel":  vel.get("current_vel",  0),
            "avg_velocity": vel.get("avg_velocity", 0),
            "acceleration": vel.get("acceleration", 0),
            "accel_label":  vel.get("accel_label",  ""),
            "trend_vel":    vel.get("trend_vel",    0),
        },
        "amplitude": {
            "avg_amplitude":   amp.get("avg_amplitude",   0),
            "recent_avg":      amp.get("recent_avg",      0),
            "regime":          amp.get("regime",          ""),
            "variance_pct":    amp.get("variance_pct",    0),
            "amplitude_trend": amp.get("amplitude_trend", ""),
        },
        "leg_ratio": {
            "ratio":        lr.get("ratio",        1),
            "recent_ratio": lr.get("recent_ratio", 1),
            "avg_bull":     lr.get("avg_bull",     0),
            "avg_bear":     lr.get("avg_bear",     0),
            "label":        lr.get("label",        ""),
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# Trend viewer
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/trend/<path:ticker>", methods=["GET"])
def trend_get(ticker): return _trend(ticker, request.args)

@app.route("/api/trend", methods=["POST"])
def trend_post():
    body = request.get_json(force=True, silent=True) or {}
    return _trend(body.get("ticker", ""), body)

def _trend(ticker, params):
    if not ticker:
        return jsonify({"error": "ticker required"}), 400

    interval    = params.get("interval",        "1d")
    _day_defaults = {"1m": 7, "5m": 30, "15m": 30, "1h": 90, "4h": 90}
    days        = int(params.get("days", _day_defaults.get(interval, 500)))
    order       = int(params.get("order",          5))
    min_str_pct = float(params.get("min_strength_pct", 0))
    atr_mult    = float(params.get("atr_mult",     0.0))
    multi_order = str(params.get("multi_order", "false")).lower() == "true"
    order_low   = int(params.get("order_low",  order))
    order_mid   = int(params.get("order_mid",  order * 2))
    order_high  = int(params.get("order_high", order * 4))

    df = fetch_ohlcv(ticker, interval=interval, days=days)
    if df.empty:
        return jsonify({"error": f"No data for {ticker}"}), 404

    last_price = float(df["Close"].iloc[-1])
    min_str    = last_price * min_str_pct / 100 if min_str_pct > 0 else 0.0

    ctx      = SymbolContext.build(df)
    pvts     = extrems(df, order=order, min_strength=min_str, min_strength_atr_mult=atr_mult)
    trend    = detect_trend(pvts)
    strength = pivot_trend_strength(pvts)

    try:
        cscore = confluence_score(df, order=order, pivots=pvts, atr_series=ctx.atr)
    except Exception:
        cscore = {}

    pivot_list = [
        {"time": int(pd.Timestamp(df.index[i]).timestamp()),
         "value": round(v, 2), "type": t, "bar": int(i)}
        for i, v, t in pvts
    ]

    tagged = tag_signals(df, pvts[-30:])
    try:   edge = measure_edge(tagged).to_dict(orient="records")
    except: edge = []

    atr_l = [
        {"time": int(pd.Timestamp(ts).timestamp()), "value": round(float(v), 4)}
        for ts, v in zip(df.index, ctx.atr)
        if not np.isnan(v)
    ]

    vel = compute_pivot_velocity(pvts)
    amp = compute_pivot_amplitude(pvts)
    lr  = compute_leg_ratio(pvts)

    payload = {
        "ticker":         ticker,
        "interval":       interval,
        "last_price":     round(last_price, 2),
        "trend":          trend,
        "pivot_count":    len(pvts),
        "trend_strength": safe_dict(strength),
        "confluence":     safe_dict(cscore),
        "pivots":         pivot_list,
        "candles":        candles(df),
        "atr":            atr_l[-200:],
        "edge":           [safe_dict(r) for r in edge],
        "velocity": {
            "current_vel":  vel["current_vel"],
            "avg_velocity": vel["avg_velocity"],
            "acceleration": vel["acceleration"],
            "accel_label":  vel["accel_label"],
            "trend_vel":    vel["trend_vel"],
        },
        "amplitude": {
            "avg_amplitude": amp["avg_amplitude"],
            "recent_avg":    amp["recent_avg"],
            "regime":        amp["regime"],
            "variance_pct":  amp["variance_pct"],
        },
        "leg_ratio": {
            "ratio":        lr["ratio"],
            "recent_ratio": lr["recent_ratio"],
            "avg_bull":     lr["avg_bull"],
            "avg_bear":     lr["avg_bear"],
            "label":        lr["label"],
        },
    }

    if multi_order:
        try:
            mo = get_multiorder_structure(
                df, order_low=order_low, order_mid=order_mid, order_high=order_high
            )
            payload["multiorder"] = {
                "low":            _build_mo_level(mo["low"]),
                "mid":            _build_mo_level(mo["mid"]),
                "high":           _build_mo_level(mo["high"]),
                "alignment":      mo["alignment"],
                "combined_trend": mo.get("combined_trend", ""),
            }
            if payload.get("confluence"):
                aln = mo["alignment"]
                net = (aln["bull_count"] - aln["bear_count"]) / 3.0
                payload["confluence"]["htf_score"] = (net + 1.0) / 2.0
                payload["confluence"]["htf_trend"] = aln.get("majority", "")
        except Exception as e:
            print(f"[trend] multiorder error: {e}")

    return jsonify(payload)


# ─────────────────────────────────────────────────────────────────────────────
# Token management
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/data/token/check")
def token_check():
    token = load_token()
    if not token:
        return jsonify({"valid": False, "reason": "no token"})
    ok, profile = is_token_valid(token)
    if ok:
        data = (profile or {}).get("data", {}) or {}
        return jsonify({"valid": True, "name": data.get("user_name", ""), "email": data.get("email", "")})
    return jsonify({"valid": False, "reason": "expired"})

@app.route("/api/data/token/set", methods=["POST"])
def token_set():
    body  = request.get_json(force=True, silent=True) or {}
    token = body.get("token", "").strip()
    if not token:
        return jsonify({"ok": False, "error": "empty token"}), 400
    save_token(token)
    return jsonify({"ok": True})

@app.route("/api/data/token/clear", methods=["POST"])
def token_clear():
    from data_fetch.data_fetch import TOKEN_FILE
    if TOKEN_FILE.exists():
        TOKEN_FILE.unlink()
    return jsonify({"ok": True})


# ─────────────────────────────────────────────────────────────────────────────
# OAuth — _auth_url() helper eliminates the 3× repeated f-string
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/data/auth/start")
def auth_start():
    return jsonify({"auth_url": _auth_url(), "redirect_uri": UPSTOX_REDIRECT})

@app.route("/api/data/auth/start-redirect")
def auth_start_redirect():
    return redirect(_auth_url())

@app.route("/api/data/auth/callback")
def auth_callback():
    import requests as req
    code = request.args.get("code", "")
    if not code:
        return "<h2>❌ No code received</h2>", 400
    r = req.post(UPSTOX_TOKEN_URL, data={
        "code": code, "client_id": UPSTOX_KEY, "client_secret": UPSTOX_SECRET,
        "redirect_uri": UPSTOX_REDIRECT, "grant_type": "authorization_code",
    }, headers={"accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"})
    if r.status_code != 200:
        return f"<h2>❌ Token exchange failed</h2><pre>{r.text}</pre>", 400
    save_token(r.json().get("access_token", ""))
    return """<html><body style="font-family:sans-serif;background:#0d0f12;color:#e2e8f0;
        display:flex;align-items:center;justify-content:center;height:100vh;margin:0">
        <div style="text-align:center"><div style="font-size:48px">✅</div>
        <h2 style="margin:16px 0 8px">Authentication successful!</h2>
        <p style="color:#7a8796">You can close this window.</p>
        <script>setTimeout(()=>window.close(),2000)</script></div></body></html>"""


# ─────────────────────────────────────────────────────────────────────────────
# Data fetch — background task endpoints
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/data/fetch/start", methods=["POST"])
def fetch_start():
    global _active_task_id

    if _active_task_id and _fetch_tasks.get(_active_task_id, {}).get("status") == "running":
        return jsonify({
            "ok":              False,
            "already_running": True,
            "task_id":         _active_task_id,
            "error":           "A fetch is already running",
        })

    body           = request.get_json(force=True, silent=True) or {}
    tf             = body.get("tf",       "1d")
    src            = body.get("src",      "yfinance")
    universe       = body.get("universe", "NSE_EQ")
    wl             = body.get("wl",       "nifty50")
    force          = bool(body.get("force", False))
    token          = load_token() or ""
    crypto_symbols = body.get("crypto_symbols", None)
    lookback_days  = int(body.get("lookback_days", 730))

    task_id = str(uuid.uuid4())[:8]
    with _fetch_lock:
        _fetch_tasks[task_id] = {
            "id":        task_id,
            "status":    "starting",
            "done":      0,
            "total":     0,
            "ok_count":  0,
            "err_count": 0,
            "title":     "Starting…",
            "interval":  tf if src != "crypto" else "1m",
            "log":       [],
            "params":    {
                "tf": tf, "src": src, "universe": universe, "wl": wl, "force": force,
                "crypto_symbols": crypto_symbols, "lookback_days": lookback_days,
            },
        }
    _active_task_id = task_id

    threading.Thread(
        target=_run_fetch_task,
        args=(task_id, tf, src, universe, wl, force, token, crypto_symbols, lookback_days),
        daemon=True,
    ).start()

    return jsonify({"ok": True, "task_id": task_id})


@app.route("/api/data/fetch/status")
def fetch_status():
    global _active_task_id
    task_id = request.args.get("task_id") or _active_task_id

    if not task_id or task_id not in _fetch_tasks:
        return jsonify({"status": "idle", "task_id": None})

    with _fetch_lock:
        t = dict(_fetch_tasks[task_id])
        t["log"] = list(t["log"])

    pct = int(t["done"] / max(t["total"], 1) * 100) if t["total"] else 0
    return jsonify({
        "task_id":   task_id,
        "status":    t["status"],
        "done":      t["done"],
        "total":     t["total"],
        "pct":       pct,
        "ok_count":  t["ok_count"],
        "err_count": t["err_count"],
        "title":     t["title"],
        "interval":  t["interval"],
        "log":       t["log"],
    })


@app.route("/api/data/fetch/cancel", methods=["POST"])
def fetch_cancel():
    body    = request.get_json(force=True, silent=True) or {}
    task_id = body.get("task_id") or _active_task_id
    if task_id and task_id in _fetch_tasks:
        with _fetch_lock:
            if _fetch_tasks[task_id]["status"] in ("running", "starting"):
                _fetch_tasks[task_id]["status"] = "cancelled"
                _fetch_tasks[task_id]["title"]  = "Cancelled by user"
    return jsonify({"ok": True})


@app.route("/api/data/fetch/stream")
def fetch_stream_legacy():
    """Legacy SSE shim — tells clients to use the new polling API."""
    def gen():
        yield f'data: {json.dumps({"type":"error","message":"Please use POST /api/data/fetch/start + GET /api/data/fetch/status instead of SSE."})}\n\n'
    return Response(gen(), mimetype="text/event-stream")


# ─────────────────────────────────────────────────────────────────────────────
# Data store API
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/data/store")
def store_list():
    itv = request.args.get("interval", None)
    s   = storage_summary()
    if itv:
        s["items"]         = [i for i in s["items"] if i["interval"] == itv]
        s["total_symbols"] = len(s["items"])
    return jsonify(s)

@app.route("/api/data/store/delete", methods=["POST"])
def store_delete():
    body     = request.get_json(force=True, silent=True) or {}
    symbol   = body.get("symbol",   "")
    interval = body.get("interval", "")
    if not symbol or not interval:
        return jsonify({"ok": False, "error": "symbol and interval required"}), 400
    return jsonify({"ok": delete_stored(symbol, interval)})


# ─────────────────────────────────────────────────────────────────────────────
# Crypto scanner
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/crypto/scan")
def crypto_scan():
    """
    Run the zone + structure scanner over stored crypto perp symbols.

    Accepts the same query parameters as /api/scan so the frontend
    scanner.js can call this with zero changes to its params payload.
    Differences from equity scan:
      - Data is always loaded from the 1m canonical store and resampled
      - Interval defaults to "15m" (not "1d")
      - Symbols are the stored perp symbols (no watchlist concept)
    """
    import time as _time
    _t0 = _time.perf_counter()

    # Reuse the shared param parser — supply default overrides for crypto
    args_with_crypto_defaults = request.args.to_dict()
    args_with_crypto_defaults.setdefault("interval", "15m")
    p = _parse_scan_params(args_with_crypto_defaults)

    # parse_states already called inside _parse_scan_params
    tf_list  = [t.strip() for t in p["trend_filter"].split(",") if t.strip()] or None
    interval = _EQUITY_TO_CRYPTO_TF.get(p["interval"], p["interval"])

    supported = {"1m"} | set(DERIVED_INTERVALS.keys())
    if interval not in supported:
        return jsonify({
            "error":     f"Interval '{interval}' not supported for crypto.",
            "supported": sorted(supported),
        }), 400

    stored = list_crypto_stored()
    data_d = {}
    for item in stored:
        sym_key = item["symbol"]
        base    = _crypto_base(sym_key)
        df      = _load_crypto_df(base, interval)
        if df is not None and not df.empty:
            data_d[sym_key] = df

    if not data_d:
        return jsonify({
            "results": [], "count": 0,
            "error":   f"No crypto data stored for interval '{interval}'. "
                       f"Fetch 1m data first, then request a derived interval.",
        })

    log.info(
        "Crypto scan request",
        direction=p["direction"], interval=interval,
        symbols=len(data_d), strategy=p["strategy"],
    )

    df_res = scan_watchlist(
        tickers=list(data_d.keys()),
        data_dict=data_d,
        order=p["order"],
        zone_lookback=p["zone_lookback"],
        direction=p["direction"],
        trend_filter=tf_list,
        legout_mult=p["legout_mult"],
        strategy=p["strategy"],
        atr_zone_types=p["atr_zone_types"],
        multi_order=p["multi_order"],
        order_low=p["order_low"],
        order_mid=p["order_mid"],
        order_high=p["order_high"],
        structure_filter_low=p["structure_low"]   or None,
        structure_filter_mid=p["structure_mid"]   or None,
        structure_filter_high=p["structure_high"] or None,
        alignment_filter=p["alignment_filter"],
        interval=interval,
    )

    if df_res.empty:
        return jsonify({"results": [], "count": 0})

    records = [safe_dict(r) for r in df_res.to_dict(orient="records")]
    log.info(
        "Crypto scan complete",
        matched=len(records), interval=interval,
        elapsed_ms=round((_time.perf_counter() - _t0) * 1000),
    )
    return jsonify({"results": records, "count": len(records)})


@app.route("/api/crypto/chart/<path:base>")
def crypto_chart(base: str):
    """
    Returns OHLCV + pivots + zones for a single crypto perp symbol.

    Mirrors the contract of /api/chart/<ticker> exactly so Chart.js
    can call it with the same response parsing logic.

    Path param:
        base: accepts any format — "BTC", "BTCUSDT", "BTCUSDT_PERP"

    Query params:  same as /api/chart/<ticker>
    """
    base_clean    = _crypto_base(base)
    interval      = _EQUITY_TO_CRYPTO_TF.get(
        request.args.get("interval", "15m"),
        request.args.get("interval", "15m"),
    )
    order            = int(request.args.get("order",        5))
    legout_mult      = float(request.args.get("legout_mult", 1.35))
    strategy         = request.args.get("strategy",        "atr")
    zone_lookback    = int(request.args.get("zone_lookback", 20))
    multi_order      = request.args.get("multi_order", "false").lower() == "true"
    order_low        = int(request.args.get("order_low",    5))
    order_mid        = int(request.args.get("order_mid",   10))
    order_high       = int(request.args.get("order_high",  20))
    max_base_candles = int(request.args.get("max_base",     8))
    candle_limit     = int(request.args.get("candle_limit", 1000))

    df = _load_crypto_df(base_clean, interval)
    if df is None or df.empty:
        return jsonify({
            "error": (
                f"No data for {base_clean} at interval '{interval}'. "
                f"Ensure 1m data has been fetched, then retry."
            )
        }), 404

    if hasattr(df.index, "tz") and df.index.tz is not None:
        df = df.copy()
        df.index = df.index.tz_localize(None)

    try:
        return _crypto_chart_inner(
            base_clean, df, interval, order, legout_mult,
            strategy, zone_lookback, multi_order,
            order_low, order_mid, order_high, max_base_candles, candle_limit,
        )
    except Exception as exc:
        import traceback as _tb
        tb_str = _tb.format_exc()
        log.error("crypto_chart exception", base=base_clean, interval=interval, error=str(exc))
        return jsonify({"error": str(exc), "traceback": tb_str}), 500


def _crypto_chart_inner(base_clean, df, interval, order, legout_mult,
                        strategy, zone_lookback, multi_order,
                        order_low, order_mid, order_high, max_base_candles,
                        candle_limit=1000):
    df_chart = df.tail(candle_limit).copy() if len(df) > candle_limit else df
    pvts  = extrems(df, order=order)
    trend = detect_trend(pvts)
    atr   = compute_atr(df)

    pivot_list = [
        {"time":  int(pd.Timestamp(df.index[i]).timestamp()),
         "value": round(float(v), 8), "type": t}
        for i, v, t in pvts
    ]

    include_atr    = strategy in ("atr",  "both")
    include_consol = strategy in ("consolidation", "both")
    start_idx      = max(0, len(df) - zone_lookback) if zone_lookback > 0 else 0

    try:
        buy_zones_raw = scan_zones(
            df, direction="buy", legout_mult=legout_mult,
            max_base_candles=max_base_candles, start_idx=start_idx,
            include_atr=include_atr, include_consolidation=include_consol,
        )
    except Exception as e:
        log.warning("crypto_chart: buy zones failed", base=base_clean, error=str(e))
        buy_zones_raw = []

    try:
        sell_zones_raw = scan_zones(
            df, direction="sell", legout_mult=legout_mult,
            max_base_candles=max_base_candles, start_idx=start_idx,
            include_atr=include_atr, include_consolidation=include_consol,
        )
    except Exception as e:
        log.warning("crypto_chart: sell zones failed", base=base_clean, error=str(e))
        sell_zones_raw = []

    multiorder_pivots = None
    if multi_order:
        try:
            multiorder_pivots = {}
            for lbl, ord_val in [("H", order_high), ("M", order_mid), ("L", order_low)]:
                multiorder_pivots[lbl] = [
                    {"time":  int(pd.Timestamp(df.index[i]).timestamp()),
                     "value": round(float(v), 8), "type": t}
                    for i, v, t in extrems(df, order=ord_val)
                ]
        except Exception as e:
            log.warning("crypto_chart: multiorder pivots failed", error=str(e))
            multiorder_pivots = None

    atr_val = float(atr.iloc[-1]) if not atr.empty else None
    atr_pct = None
    if atr_val and float(df["Close"].iloc[-1]) > 0:
        atr_pct = round(atr_val / float(df["Close"].iloc[-1]) * 100, 4)

    payload = {
        "ticker":      _file_key(base_clean),
        "interval":    interval,
        "asset_class": "crypto",
        "candles":     candles(df_chart),
        "pivots":      pivot_list,
        "buy_zones":   serialise_zones(buy_zones_raw),
        "sell_zones":  serialise_zones(sell_zones_raw),
        "trend":       trend,
        "atr":         atr_val,
        "atr_pct":     atr_pct,
    }
    if multiorder_pivots:
        payload["multiorder_pivots"] = multiorder_pivots

    return jsonify(payload)


# ─────────────────────────────────────────────────────────────────────────────
# Crypto utility routes
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/crypto/intervals")
def crypto_intervals():
    """
    Returns the canonical interval list for crypto perps.
    Frontend calls this on init to populate the TF buttons dynamically.
    """
    return jsonify({
        "intervals": [
            {"value": "1m",  "label": "1m",  "group": "minutes"},
            {"value": "3m",  "label": "3m",  "group": "minutes"},
            {"value": "5m",  "label": "5m",  "group": "minutes"},
            {"value": "15m", "label": "15m", "group": "minutes"},
            {"value": "1h",  "label": "1H",  "group": "hours"},
            {"value": "4h",  "label": "4H",  "group": "hours"},
        ],
        "default": "15m",
    })

@app.route("/api/crypto/store")
def crypto_store():
    """Storage info for all crypto perp symbols. Powers the Crypto tab in the store browser."""
    items    = list_crypto_stored()
    total_kb = sum(it["size_kb"] for it in items)
    return jsonify({
        "total_symbols": len(items),
        "total_size_mb": round(total_kb / 1024, 2),
        "items":         items,
        "source":        "binance_perp",
    })

@app.route("/api/crypto/symbols")
def crypto_symbols_route():
    """List of all crypto symbol keys that have 1m data stored."""
    items = list_crypto_stored()
    return jsonify({"symbols": [it["symbol"] for it in items], "count": len(items)})

@app.route("/api/crypto/store/delete", methods=["POST"])
def crypto_store_delete():
    """Delete all stored data (raw 1m + all derived TFs) for a crypto symbol."""
    body = request.get_json(force=True, silent=True) or {}
    base = _crypto_base(body.get("base", ""))
    if not base:
        return jsonify({"ok": False, "error": "base symbol required e.g. 'BTC'"}), 400
    return jsonify({"ok": delete_crypto_stored(base), "deleted": base})


# ─────────────────────────────────────────────────────────────────────────────
# Debug
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/debug/pivots/<path:ticker>")
def debug_pivots(ticker):
    import traceback as tb
    interval   = request.args.get("interval", "1d")
    orders     = [int(x.strip()) for x in request.args.get("orders", "5,10,20").split(",") if x.strip()]

    df = load_stored_df(ticker, interval)
    if df is None or df.empty:
        df = fetch_ohlcv(ticker, interval=interval, days=500)
    if df is None or df.empty:
        return jsonify({"error": f"no data for {ticker}"}), 404

    report = {"ticker": ticker, "interval": interval, "candles": len(df), "orders": {}}

    print(f"\n{'='*60}")
    print(f"DEBUG PIVOTS  ticker={ticker}  interval={interval}  rows={len(df)}")

    candle_times  = [int(pd.Timestamp(ts).timestamp()) for ts in df.index]
    candle_dups   = len(candle_times) - len(set(candle_times))
    candle_sorted = all(candle_times[i] < candle_times[i+1] for i in range(len(candle_times)-1))
    report["candle_duplicates"]   = candle_dups
    report["candle_times_sorted"] = candle_sorted

    if candle_dups > 0:
        print(f"  [CANDLES] WARN: {candle_dups} duplicate timestamps")
    if not candle_sorted:
        print(f"  [CANDLES] WARN: NOT sorted ascending")
    else:
        print(f"  [CANDLES] OK  : {len(df)} bars, sorted, 0 dups")

    for order in orders:
        try:
            pvts       = extrems(df, order=order)
            pivot_list = [
                {"time": int(pd.Timestamp(df.index[i]).timestamp()),
                 "value": round(float(v), 2), "type": t}
                for i, v, t in pvts
            ]
            times     = [p["time"]  for p in pivot_list]
            values    = [p["value"] for p in pivot_list]
            dups      = len(times) - len(set(times))
            sorted_ok = all(times[i] < times[i+1] for i in range(len(times)-1))
            has_nan   = any(v != v for v in values)
            has_none  = any(v is None for v in values)

            report["orders"][order] = {
                "pivot_count":  len(pvts),
                "duplicates":   dups,
                "sorted":       sorted_ok,
                "has_nan":      has_nan,
                "has_none":     has_none,
                "sample_last3": pivot_list[-3:],
            }

            status = "OK  " if (dups==0 and sorted_ok and not has_nan and not has_none) else "FAIL"
            print(f"  [ORDER={order:>3}] {status}: {len(pvts):>4} pivots  "
                  f"sorted={sorted_ok}  dups={dups}  nan={has_nan}  none={has_none}")

            if dups > 0:
                seen = set(); dup_times = []
                for t in times:
                    if t in seen: dup_times.append(t)
                    seen.add(t)
                print(f"    Dup timestamps: {dup_times[:5]}")

            if not sorted_ok:
                for i in range(len(times)-1):
                    if times[i] >= times[i+1]:
                        print(f"    Out-of-order idx={i}: {times[i]} >= {times[i+1]}")
                        break

        except Exception as e:
            tb.print_exc()
            report["orders"][order] = {"error": str(e)}
            print(f"  [ORDER={order}] EXCEPTION: {e}")

    print(f"{'='*60}\n")
    return jsonify(report)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point — port management moved to run.py
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from run import start
    start(app)
