/* ═══════════════════════════════════════════════════════════════
   api.js — all backend API calls
   Single place to change URLs, add auth headers, logging, retries.
   Depends on: nothing (pure fetch wrappers)

   Changes v3.4:
     • Added logEvent() — fire-and-forget frontend logger
       Sends structured log events to POST /api/log
       Never blocks the UI; failures are silently swallowed
     • All existing functions unchanged
═══════════════════════════════════════════════════════════════ */

const API = (() => {

  const BASE = '/api';

  /* ── Internal fetch wrapper ─────────────────────────────── */
  async function _get(path, params) {
    const url = params
      ? `${BASE}${path}?${new URLSearchParams(params)}`
      : `${BASE}${path}`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status} — ${path}`);
    return res.json();
  }

  async function _post(path, body) {
    const res = await fetch(`${BASE}${path}`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(body),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status} — ${path}`);
    return res.json();
  }

  /* ── Frontend logger ────────────────────────────────────────
     Fire-and-forget: sends a log event to the backend.
     Never awaited, never blocks the UI thread.
     Backend writes it to the same rotating log file with [FE] tag.

     Usage:
       API.logEvent('info',  'scanner', 'TF button clicked', { tf: '1d' })
       API.logEvent('warn',  'chart',   'Chart load slow',   { ms: 4200 })
       API.logEvent('error', 'scanner', 'Scan failed',       { err: e.message })
       API.logEvent('debug', 'scanner', 'selStock called',   { idx: 3 })

     level: 'debug' | 'info' | 'warn' | 'error'
     module: short string identifying the JS module
     message: human-readable description
     context: optional flat object of key/value pairs
  ─────────────────────────────────────────────────────────── */
  function logEvent(level, module, message, context = {}) {
    // Fire and forget — do NOT await this
    fetch(`${BASE}/log`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ level, module, message, context }),
    }).catch(() => {
      // Silently swallow — logging must never crash the app
    });
  }

  /* ── Scanner ────────────────────────────────────────────── */
  function scan(params) {
    return _get('/scan', params);
  }

  function symbols(interval) {
    return _get('/symbols', { interval });
  }

  /* ── Chart data ─────────────────────────────────────────── */
  function chartData(ticker, params) {
    return _get(`/chart/${encodeURIComponent(ticker)}`, params);
  }

  /* ── Trend / pivots ─────────────────────────────────────── */
  function trendData(ticker, params) {
    return _get(`/trend/${encodeURIComponent(ticker)}`, params);
  }

  /* ── Health ─────────────────────────────────────────────── */
  function health() {
    return _get('/health');
  }

  /* ── Token management ───────────────────────────────────── */
  function checkToken() {
    return _get('/data/token/check');
  }

  function setToken(token) {
    return _post('/data/token/set', { token });
  }

  function clearToken() {
    return _post('/data/token/clear', {});
  }

  /* ── OAuth ──────────────────────────────────────────────── */
  function authStart() {
    return _get('/data/auth/start');
  }

  /* ── Data fetch task ────────────────────────────────────── */
  function fetchStart(params) {
    return _post('/data/fetch/start', params);
  }

  function fetchStatus(taskId) {
    return _get('/data/fetch/status', taskId ? { task_id: taskId } : undefined);
  }

  function fetchCancel(taskId) {
    return _post('/data/fetch/cancel', { task_id: taskId });
  }

  /* ── Log viewer ─────────────────────────────────────────── */
  function getLogs(n = 200, level = '') {
    const params = { n };
    if (level) params.level = level;
    return _get('/logs', params);
  }

  function getLogStats() {
    return _get('/logs/stats');
  }

  /* ── Store ──────────────────────────────────────────────── */
  function storeList(interval) {
    return _get('/data/store', interval ? { interval } : undefined);
  }

  function storeDelete(symbol, interval) {
    return _post('/data/store/delete', { symbol, interval });
  }

  function storeSummary() {
    return _get('/data/store/summary');
  }

  /* ── Public surface ─────────────────────────────────────── */
  return {
    logEvent,
    scan,
    symbols,
    chartData,
    trendData,
    health,
    checkToken,
    setToken,
    clearToken,
    authStart,
    fetchStart,
    fetchStatus,
    fetchCancel,
    getLogs,
    getLogStats,
    storeList,
    storeDelete,
    storeSummary,
  };
})();

window.API = API;
