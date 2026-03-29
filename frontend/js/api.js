/* ═══════════════════════════════════════════════════════════════
   api.js — all backend API calls
   Single place to change URLs, add auth headers, logging, retries.
   Depends on: nothing (pure fetch wrappers)
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

  /* ── Store ──────────────────────────────────────────────── */
  function storeList(interval) {
    return _get('/data/store', interval ? { interval } : undefined);
  }

  function storeDelete(symbol, interval) {
    return _post('/data/store/delete', { symbol, interval });
  }

  /* ── Debug ──────────────────────────────────────────────── */
  function debugPivots(ticker, interval, orders) {
    return _get(`/debug/pivots/${encodeURIComponent(ticker)}`,
      { interval: interval || '1d', orders: orders || '5,10,20' });
  }

  return {
    scan, symbols,
    chartData, trendData,
    health,
    checkToken, setToken, clearToken,
    authStart,
    fetchStart, fetchStatus, fetchCancel,
    storeList, storeDelete,
    debugPivots,
  };
})();

window.API = API;
