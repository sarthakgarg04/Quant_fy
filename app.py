"""
app.py  –  QuantScanner v3.1
=============================
Key change vs v3.0:
  The /api/data/fetch/stream SSE endpoint is REPLACED by:
    POST /api/data/fetch/start   → launches server-side thread, returns task_id
    GET  /api/data/fetch/status  → poll for progress (survives page navigation)
    POST /api/data/fetch/cancel  → graceful cancel

  The fetch now runs entirely on the server. Navigating away and back
  does NOT interrupt it — the thread keeps running and the frontend
  reconnects by polling /status with the task_id stored in localStorage.
"""

from __future__ import annotations
import sys, os, json, time, threading, uuid

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

from algos.pivot_engine import (
    extrems, detect_trend, compute_atr, tag_signals,
    detect_trend_multiorder, compute_pivot_velocity,
    compute_pivot_amplitude, compute_leg_ratio,
    UPTREND_LABELS, DOWNTREND_LABELS, _trend_matches,
)

def _load_keys():
    try:
        import keys as _k
        k = getattr(_k, "api_key",      "").strip()
        s = getattr(_k, "secret",       "").strip()
        r = getattr(_k, "redirect_uri", "").strip()
        if k:
            print(f"  ✅ Credentials from keys.py  (key: {k[:8]}…)")
            return k, s, r
    except ImportError:
        pass
    k = os.environ.get("UPSTOX_API_KEY",    "").strip()
    s = os.environ.get("UPSTOX_API_SECRET", "").strip()
    r = os.environ.get("UPSTOX_REDIRECT",   "http://127.0.0.1:5050/api/data/auth/callback").strip()
    if k:
        print(f"  ✅ Credentials from env vars  (key: {k[:8]}…)")
    else:
        print("  ⚠  No API credentials — create keys.py or set UPSTOX_API_KEY.")
    return k, s, r

UPSTOX_KEY, UPSTOX_SECRET, UPSTOX_REDIRECT = _load_keys()

from flask import Flask, jsonify, request, send_from_directory, Response, redirect
from flask_cors import CORS
import pandas as pd
import numpy as np

from data_fetch.data_fetch import (
    fetch_ohlcv, fetch_batch, get_watchlist,
    list_stored, load_stored_df,
    load_token, save_token, is_token_valid,
    delete_stored, storage_summary,
    get_eq_instruments, instrument_key_to_symbol,
    fetch_upstox_batch_iter,
    UPSTOX_AUTH_URL, UPSTOX_TOKEN_URL,
)
from algos.pivot_engine import extrems, detect_trend, compute_atr, tag_signals
from algos.zones        import scan_zones, buy_zone, sell_zone
from algos.confluence   import confluence_score, measure_edge
from algos.patterns     import detect_flag, detect_triangle, pivot_trend_strength
from scanner            import scan_watchlist

FRONTEND_DIR = os.path.join(ROOT, "frontend")
app = Flask(__name__, static_folder=FRONTEND_DIR, static_url_path="")
CORS(app)

# ─────────────────────────────────────────────────────────────────────────────
# BACKGROUND FETCH TASK STATE
# Lives in the server process — completely unaffected by page navigation.
# ─────────────────────────────────────────────────────────────────────────────
_fetch_tasks:    dict = {}   # task_id -> task_dict
_fetch_lock              = threading.Lock()
_active_task_id: str | None = None


def _run_fetch_task(task_id: str, tf: str, src: str, universe: str,
                    wl: str, force: bool, token: str):
    """
    Daemon thread that executes the full fetch and writes progress
    into _fetch_tasks[task_id].  The HTTP server keeps serving
    /api/data/fetch/status requests throughout.
    """
    def upd(**kw):
        with _fetch_lock:
            for k, v in kw.items():
                _fetch_tasks[task_id][k] = v

    def log(msg: str, kind: str = "inf"):
        with _fetch_lock:
            entries = _fetch_tasks[task_id]["log"]
            entries.append({"msg": msg, "kind": kind})
            if len(entries) > 300:
                _fetch_tasks[task_id]["log"] = entries[-300:]

    try:
        upd(status="running", title=f"Starting {src} fetch — {tf}")
        log(f"Starting {src} fetch — {tf}", "inf")

        if src == "upstox":
            ok_tok, _ = is_token_valid(token)
            if not ok_tok:
                upd(status="error", title="Token expired — re-authenticate.")
                log("Token expired — re-authenticate.", "err")
                return

            log("Downloading instrument master…", "inf")
            segments = tuple(e.strip() for e in universe.split(","))
            instr    = get_eq_instruments(segments)
            keys     = instr["instrument_key"].tolist()
            upd(total=len(keys))
            log(f"Found {len(keys)} instruments in {universe}", "inf")

            if not keys:
                upd(status="error", title=f"0 instruments found for {universe}")
                log(f"0 instruments found for {universe}", "err")
                return

            ok_n = err_n = 0
            for done, total, sym, ok in fetch_upstox_batch_iter(keys, tf, token):
                # Check for cancellation
                if _fetch_tasks[task_id].get("status") == "cancelled":
                    log("Cancelled by user.", "inf")
                    return
                if ok: ok_n += 1
                else:  err_n += 1
                upd(done=done, total=total, ok_count=ok_n, err_count=err_n)
                log(f"{'✓' if ok else '✗'} {sym}", "ok" if ok else "err")

        else:  # yfinance
            tickers = get_watchlist(wl)
            total   = len(tickers)
            upd(total=total)
            log(f"Fetching {total} tickers via yfinance…", "inf")
            ok_n = err_n = 0
            for i, ticker in enumerate(tickers, 1):
                if _fetch_tasks[task_id].get("status") == "cancelled":
                    log("Cancelled by user.", "inf")
                    return
                try:
                    df = fetch_ohlcv(ticker, interval=tf, force_refresh=force)
                    ok = not df.empty
                except Exception:
                    ok = False
                if ok: ok_n += 1
                else:  err_n += 1
                upd(done=i, total=total, ok_count=ok_n, err_count=err_n)
                log(f"{'✓' if ok else '✗'} {ticker}", "ok" if ok else "err")
                time.sleep(0.05)

        final = f"Done — {_fetch_tasks[task_id]['ok_count']} OK · {_fetch_tasks[task_id]['err_count']} errors"
        upd(status="done", title=final)
        log(f"✅ {final}", "inf")

    except Exception as exc:
        import traceback
        msg = f"Error: {exc}"
        upd(status="error", title=msg)
        log(msg, "err")
        traceback.print_exc()


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _safe(v):
    if isinstance(v, float) and (np.isnan(v) or np.isinf(v)): return None
    if isinstance(v, np.integer):  return int(v)
    if isinstance(v, np.floating): return float(v)
    if isinstance(v, bool):        return bool(v)
    return v

def _sr(d): return {k: _safe(v) for k, v in d.items()}

def _candles(df: pd.DataFrame) -> list:
    out = []
    for ts, row in df.iterrows():
        t = int(pd.Timestamp(ts).timestamp())
        out.append({"time": t,
                    "open":   round(float(row["Open"]),  2),
                    "high":   round(float(row["High"]),  2),
                    "low":    round(float(row["Low"]),   2),
                    "close":  round(float(row["Close"]), 2),
                    "volume": float(row.get("Volume", 0))})
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Frontend pages
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/")
def page_scanner(): return send_from_directory(FRONTEND_DIR, "index.html")

@app.route("/trend")
def page_trend():   return send_from_directory(FRONTEND_DIR, "trend_viewer.html")

@app.route("/data")
def page_data():    return send_from_directory(FRONTEND_DIR, "data_fetch.html")

@app.route("/<path:filename>")
def static_files(filename): return send_from_directory(FRONTEND_DIR, filename)


# ─────────────────────────────────────────────────────────────────────────────
# Health
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/health")
def health(): return jsonify({"status": "ok", "version": "3.1"})


# ─────────────────────────────────────────────────────────────────────────────
# Scanner
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/scan")
def scan():
    direction     = request.args.get("direction",      "buy")
    interval      = request.args.get("interval",       "1d")
    order         = int(request.args.get("order",      5))
    zone_lookback = int(request.args.get("zone_lookback", 20))
    legout_mult   = float(request.args.get("legout_mult",  1.35))
    trend_filter  = request.args.get("trend_filter",   "")
    strategy      = request.args.get("strategy",       "atr")
    multi_order   = request.args.get("multi_order",  "false").lower() == "true"
    order_low     = int(request.args.get("order_low",  5))
    order_mid     = int(request.args.get("order_mid",  10))
    order_high    = int(request.args.get("order_high", 20))
    trend_low     = request.args.get("trend_low",  "any")
    trend_mid     = request.args.get("trend_mid",  "any")
    trend_high    = request.args.get("trend_high", "any")

    tf_list = [t.strip() for t in trend_filter.split(",") if t.strip()] or None

    stored = list_stored(interval)
    if not stored:
        return jsonify({"results": [], "count": 0,
                        "error": f"No data stored for interval '{interval}'."})

    data_d = {}
    for item in stored:
        sym = item["symbol"]
        df  = load_stored_df(sym, interval)
        if df is not None and not df.empty:
            data_d[sym] = df

    if not data_d:
        return jsonify({"results": [], "count": 0})

    df_res = scan_watchlist(
        tickers=list(data_d.keys()), data_dict=data_d,
        order=order, zone_lookback=zone_lookback,
        direction=direction, trend_filter=tf_list,
        legout_mult=legout_mult, strategy=strategy,
        multi_order=multi_order,
        order_low=order_low, order_mid=order_mid, order_high=order_high,
        trend_low=trend_low, trend_mid=trend_mid, trend_high=trend_high,
    )

    if df_res.empty:
        return jsonify({"results": [], "count": 0})

    records = [_sr(r) for r in df_res.to_dict(orient="records")]
    return jsonify({"results": records, "count": len(records)})


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
    if df is None or df.empty:
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
        buy_zones_raw = scan_zones(df, direction="buy", legout_mult=legout_mult,
                                   max_base_candles=max_base_candles, start_idx=start_idx,
                                   include_atr=include_atr, include_consolidation=include_consol)
    except Exception as e:
        print(f"[chart] buy scan_zones error: {e}"); buy_zones_raw = []

    try:
        sell_zones_raw = scan_zones(df, direction="sell", legout_mult=legout_mult,
                                    max_base_candles=max_base_candles, start_idx=start_idx,
                                    include_atr=include_atr, include_consolidation=include_consol)
    except Exception as e:
        print(f"[chart] sell scan_zones error: {e}"); sell_zones_raw = []

    def _ser(zones):
        out = []
        for z in zones:
            try:
                out.append({
                    "time_start":      int(pd.Timestamp(z["time_start"]).timestamp()),
                    "time_end":        int(pd.Timestamp(z["time_end"]).timestamp()),
                    "price_high":      z["price_high"],
                    "price_low":       z["price_low"],
                    "zone_type":       z.get("zone_type",       "rbr"),
                    "status":          z.get("status",          "fresh"),
                    "base_bars":       z.get("base_bars",        0),
                    "legout_strength": z.get("legout_strength",  0.0),
                    "vol_score":       z.get("vol_score",        0.0),
                })
            except Exception:
                pass
        return out

    multiorder_data = None
    if multi_order:
        try:
            mo = detect_trend_multiorder(df, order_low=order_low, order_mid=order_mid, order_high=order_high)
            def _mol(lv):
                v   = mo[lv]; vel = v["velocity"]; amp = v["amplitude"]; lr = v["leg_ratio"]
                return {"order": v["order"], "trend": v["trend"], "score": v["score"], "pivot_count": v["pivot_count"],
                        "velocity":  {"current_vel": vel["current_vel"], "avg_velocity": vel["avg_velocity"], "acceleration": vel["acceleration"], "accel_label": vel["accel_label"], "trend_vel": vel["trend_vel"]},
                        "amplitude": {"avg_amplitude": amp["avg_amplitude"], "recent_avg": amp["recent_avg"], "regime": amp["regime"], "variance_pct": amp["variance_pct"], "amplitude_trend": amp["amplitude_trend"]},
                        "leg_ratio": {"ratio": lr["ratio"], "recent_ratio": lr["recent_ratio"], "avg_bull": lr["avg_bull"], "avg_bear": lr["avg_bear"], "label": lr["label"]}}
            multiorder_data = {"low": _mol("low"), "mid": _mol("mid"), "high": _mol("high"),
                               "alignment": mo["alignment"], "combined_trend": mo["combined_trend"]}
        except Exception as e:
            print(f"[chart] multiorder error: {e}")

    payload = {
        "ticker": ticker, "interval": interval,
        "candles": _candles(df), "pivots": pivot_list,
        "buy_zones":  _ser(buy_zones_raw),
        "sell_zones": _ser(sell_zones_raw),
        "trend": trend,
    }
    if multiorder_data:
        payload["multiorder"] = multiorder_data
    return jsonify(payload)


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

    interval    = params.get("interval",          "1d")
    days        = int(params.get("days",           500))
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

    pvts     = extrems(df, order=order, min_strength=min_str, min_strength_atr_mult=atr_mult)
    trend    = detect_trend(pvts)
    strength = pivot_trend_strength(pvts)

    try:   cscore = confluence_score(df, order=order)
    except: cscore = {}

    pivot_list = [{"time": int(pd.Timestamp(df.index[i]).timestamp()), "value": round(v, 2), "type": t, "bar": int(i)} for i, v, t in pvts]

    tagged = tag_signals(df, pvts[-30:])
    try:   edge = measure_edge(tagged).to_dict(orient="records")
    except: edge = []

    atr_s = compute_atr(df)
    atr_l = [{"time": int(pd.Timestamp(ts).timestamp()), "value": round(float(v), 4)} for ts, v in zip(df.index, atr_s) if not np.isnan(v)]

    vel = compute_pivot_velocity(pvts)
    amp = compute_pivot_amplitude(pvts)
    lr  = compute_leg_ratio(pvts)

    payload = {
        "ticker": ticker, "interval": interval, "last_price": round(last_price, 2),
        "trend": trend, "pivot_count": len(pvts),
        "trend_strength": _sr(strength), "confluence": _sr(cscore),
        "pivots": pivot_list, "candles": _candles(df), "atr": atr_l[-200:],
        "edge": [_sr(r) for r in edge],
        "velocity":  {"current_vel": vel["current_vel"], "avg_velocity": vel["avg_velocity"], "acceleration": vel["acceleration"], "accel_label": vel["accel_label"], "trend_vel": vel["trend_vel"]},
        "amplitude": {"avg_amplitude": amp["avg_amplitude"], "recent_avg": amp["recent_avg"], "regime": amp["regime"], "variance_pct": amp["variance_pct"]},
        "leg_ratio": {"ratio": lr["ratio"], "recent_ratio": lr["recent_ratio"], "avg_bull": lr["avg_bull"], "avg_bear": lr["avg_bear"], "label": lr["label"]},
    }

    if multi_order:
        try:
            mo = detect_trend_multiorder(df, order_low=order_low, order_mid=order_mid, order_high=order_high)
            payload["multiorder"] = {
                "low":  {"order": mo["low"]["order"],  "trend": mo["low"]["trend"],  "score": mo["low"]["score"],  "velocity": mo["low"]["velocity"],  "amplitude": mo["low"]["amplitude"],  "leg_ratio": mo["low"]["leg_ratio"]},
                "mid":  {"order": mo["mid"]["order"],  "trend": mo["mid"]["trend"],  "score": mo["mid"]["score"],  "velocity": mo["mid"]["velocity"],  "amplitude": mo["mid"]["amplitude"],  "leg_ratio": mo["mid"]["leg_ratio"]},
                "high": {"order": mo["high"]["order"], "trend": mo["high"]["trend"], "score": mo["high"]["score"], "velocity": mo["high"]["velocity"], "amplitude": mo["high"]["amplitude"], "leg_ratio": mo["high"]["leg_ratio"]},
                "alignment": mo["alignment"], "combined_trend": mo["combined_trend"],
            }
            if "confluence" in payload and payload["confluence"]:
                payload["confluence"]["htf_score"] = mo["alignment"] / 3.0
                payload["confluence"]["htf_trend"] = mo["combined_trend"]
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
        return jsonify({"valid": True, "name": data.get("user_name",""), "email": data.get("email","")})
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
    if TOKEN_FILE.exists(): TOKEN_FILE.unlink()
    return jsonify({"ok": True})


# ─────────────────────────────────────────────────────────────────────────────
# OAuth
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/data/auth/start")
def auth_start():
    auth_url = f"{UPSTOX_AUTH_URL}?response_type=code&client_id={UPSTOX_KEY}&redirect_uri={UPSTOX_REDIRECT}"
    return jsonify({"auth_url": auth_url, "redirect_uri": UPSTOX_REDIRECT})

@app.route("/api/data/auth/start-redirect")
def auth_start_redirect():
    url = f"{UPSTOX_AUTH_URL}?response_type=code&client_id={UPSTOX_KEY}&redirect_uri={UPSTOX_REDIRECT}"
    return redirect(url)

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
    token = r.json().get("access_token", "")
    save_token(token)
    return """<html><body style="font-family:sans-serif;background:#0d0f12;color:#e2e8f0;display:flex;align-items:center;justify-content:center;height:100vh;margin:0">
    <div style="text-align:center"><div style="font-size:48px">✅</div>
    <h2 style="margin:16px 0 8px">Authentication successful!</h2>
    <p style="color:#7a8796">You can close this window.</p>
    <script>setTimeout(()=>window.close(),2000)</script></div></body></html>"""


# ─────────────────────────────────────────────────────────────────────────────
# DATA FETCH — Background task endpoints
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/data/fetch/start", methods=["POST"])
def fetch_start():
    global _active_task_id

    # Check if one is already running
    if _active_task_id and _fetch_tasks.get(_active_task_id, {}).get("status") == "running":
        return jsonify({
            "ok": False,
            "already_running": True,
            "task_id": _active_task_id,
            "error": "A fetch is already running",
        })

    body     = request.get_json(force=True, silent=True) or {}
    tf       = body.get("tf",       "1d")
    src      = body.get("src",      "yfinance")
    universe = body.get("universe", "NSE_EQ")
    wl       = body.get("wl",       "nifty50")
    force    = bool(body.get("force", False))
    token    = load_token() or ""

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
            "interval":  tf,
            "log":       [],
            "params":    {"tf": tf, "src": src, "universe": universe, "wl": wl, "force": force},
        }
    _active_task_id = task_id

    t = threading.Thread(
        target=_run_fetch_task,
        args=(task_id, tf, src, universe, wl, force, token),
        daemon=True,
    )
    t.start()

    return jsonify({"ok": True, "task_id": task_id})


@app.route("/api/data/fetch/status")
def fetch_status():
    global _active_task_id
    task_id = request.args.get("task_id")

    # If no task_id given, return the currently active task
    if not task_id:
        task_id = _active_task_id

    if not task_id or task_id not in _fetch_tasks:
        return jsonify({"status": "idle", "task_id": None})

    with _fetch_lock:
        t = dict(_fetch_tasks[task_id])
        # Shallow copy log list
        t["log"] = list(t["log"])

    pct = int(t["done"] / max(t["total"], 1) * 100) if t["total"] else 0

    return jsonify({
        "task_id":   task_id,
        "status":    t["status"],   # "starting" | "running" | "done" | "error" | "cancelled"
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


# Keep the SSE endpoint as a legacy shim so any old clients still work
# but it immediately redirects to the new pattern
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
        s["items"] = [i for i in s["items"] if i["interval"] == itv]
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
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import socket, subprocess, platform

    port = int(os.environ.get("PORT", 5050))

    def _port_in_use(p):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(("127.0.0.1", p)) == 0

    def _kill_port(p):
        try:
            if platform.system() == "Darwin":
                result = subprocess.run(["lsof", "-ti", f"tcp:{p}"], capture_output=True, text=True)
                pids = result.stdout.strip().split()
                for pid in pids: subprocess.run(["kill", "-9", pid], capture_output=True)
                return bool(pids)
            else:
                result = subprocess.run(["fuser", "-k", f"{p}/tcp"], capture_output=True, text=True)
                return result.returncode == 0
        except Exception as e:
            print(f"  ⚠  Could not auto-kill port {p}: {e}")
            return False

    if _port_in_use(port):
        print(f"  Port {port} is in use — killing stale process…")
        killed = _kill_port(port)
        if killed:
            time.sleep(0.8)
            if _port_in_use(port):
                port += 1
        else:
            port += 1

    print(f"""
╔══════════════════════════════════════════════════╗
║  ⚡  QuantScanner  v3.1                          ║
╠══════════════════════════════════════════════════╣
║  Dashboard    →  http://localhost:{port}            ║
║  Trend Viewer →  http://localhost:{port}/trend      ║
║  Data Fetch   →  http://localhost:{port}/data       ║
║  API health   →  http://localhost:{port}/api/health ║
╚══════════════════════════════════════════════════╝
""")
    app.run(debug=False, port=port, host="0.0.0.0", use_reloader=False, threaded=True)