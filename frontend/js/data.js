/* ═══════════════════════════════════════════════════════════════
   data.js — Data Fetch page controller
   Depends on: shared.js, api.js
═══════════════════════════════════════════════════════════════ */

const DataFetch = (() => {

  const $ = id => document.getElementById(id);

  let source        = 'upstox';
  let curTF         = '1d';
  let storeFilter   = null;
  let storeAll      = [];
  let pollTimer     = null;
  let currentTaskId = null;
  let lastLogCount  = 0;

  const TASK_ID_KEY = 'qs_fetch_task_id';

  /* ════════════════════════════════════════════════════════════
     AUTH
  ════════════════════════════════════════════════════════════ */
  async function checkAuth() {
    try {
      const d = await API.checkToken();
      if (d.valid) {
        $('auth-icon').textContent   = '✅';
        $('auth-name').textContent   = d.name  || 'Authenticated';
        $('auth-sub').textContent    = d.email || 'Upstox token valid';
        $('auth-box').style.borderColor = 'var(--green)';
        $('logout-btn').style.display   = 'inline-flex';
      } else {
        $('auth-icon').textContent   = '🔒';
        $('auth-name').textContent   = 'Not authenticated';
        $('auth-sub').textContent    = d.reason === 'expired'
          ? 'Token expired – re-authenticate' : 'Token required for Upstox';
        $('auth-box').style.borderColor = 'var(--border)';
        $('logout-btn').style.display   = 'none';
      }
    } catch (_) {}
  }

  function doOAuth() {
    API.authStart().then(d => {
      if (d.auth_url) window.open(d.auth_url, '_blank', 'width=600,height=700');
      const poll = setInterval(async () => {
        const d2 = await API.checkToken();
        if (d2.valid) { clearInterval(poll); checkAuth(); }
      }, 3000);
      setTimeout(() => clearInterval(poll), 120000);
    });
  }

  function togglePaste() {
    const a = $('paste-area');
    a.style.display = a.style.display === 'none' ? 'block' : 'none';
  }

  async function pasteToken() {
    const t = $('token-inp').value.trim();
    if (!t) { alert('Token is empty'); return; }
    await API.setToken(t);
    $('paste-area').style.display = 'none';
    checkAuth();
  }

  async function clearTokenFn() {
    if (!confirm('Clear saved token?')) return;
    await API.clearToken();
    checkAuth();
  }

  /* ════════════════════════════════════════════════════════════
     SOURCE / TF
  ════════════════════════════════════════════════════════════ */
  function setSource(src, persist) {
    source = src;
    $('src-up').classList.toggle('active', src === 'upstox');
    $('src-yf').classList.toggle('active', src === 'yfinance');
    $('univ-sec').style.display = src === 'upstox'   ? 'block' : 'none';
    $('wl-sec').style.display   = src === 'yfinance' ? 'block' : 'none';
    $('src-hint').textContent   = src === 'upstox'
      ? 'Primary source. Requires valid Upstox token.'
      : 'Fallback source. No auth required.';
    if (persist !== false) QS.saveState({ data_src: src });
  }

  function selTF(btn) {
    document.querySelectorAll('.tf-pill').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    curTF = btn.dataset.tf;
    QS.saveState({ data_tf: curTF });
  }

  function selectTFVal(tf) {
    const btn = document.querySelector(`.tf-pill[data-tf="${tf}"]`);
    if (btn) selTF(btn);
  }

  /* ════════════════════════════════════════════════════════════
     FETCH
  ════════════════════════════════════════════════════════════ */
  async function startFetch() {
    const btn = $('fetch-btn');

    // Check if already running
    try {
      const sc = await API.fetchStatus();
      if (sc.status === 'running' || sc.status === 'starting') {
        currentTaskId = sc.task_id;
        localStorage.setItem(TASK_ID_KEY, currentTaskId);
        $('reconnect-banner').classList.add('show');
        _startPolling(true);
        return;
      }
    } catch (_) {}

    // Reset UI
    $('prog-log').innerHTML     = '';
    $('prog-bar').style.width   = '0%';
    $('prog-count').textContent = '';
    $('prog-title').textContent = 'Starting…';
    $('reconnect-banner').classList.remove('show');
    lastLogCount = 0;

    btn.disabled = true; btn.textContent = '⏳ Starting…';
    _setRunningUI(true);

    try {
      const d = await API.fetchStart({
        tf:       curTF,
        src:      source,
        universe: $('univ-sel').value,
        wl:       $('wl-sel').value,
        force:    $('force-chk').checked,
      });

      if (!d.ok) {
        if (d.already_running) {
          currentTaskId = d.task_id;
          localStorage.setItem(TASK_ID_KEY, currentTaskId);
          $('reconnect-banner').classList.add('show');
          _startPolling(true);
        } else {
          $('prog-title').textContent = d.error || 'Start failed';
          _setRunningUI(false);
          btn.disabled = false; btn.textContent = '▶ Start Fetch';
        }
        return;
      }

      currentTaskId = d.task_id;
      localStorage.setItem(TASK_ID_KEY, currentTaskId);
      _startPolling(false);
    } catch (e) {
      $('prog-title').textContent = `Error: ${e.message}`;
      _setRunningUI(false);
      btn.disabled = false; btn.textContent = '▶ Start Fetch';
    }
  }

  async function cancelFetch() {
    if (!currentTaskId) return;
    await API.fetchCancel(currentTaskId);
  }

  function _startPolling(isReconnect) {
    _stopPolling();
    if (isReconnect) lastLogCount = 0;
    _pollStatus(isReconnect);
    pollTimer = setInterval(() => _pollStatus(false), 800);
  }

  function _stopPolling() {
    if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
  }

  async function _pollStatus(isReconnect) {
    if (!currentTaskId) return;
    try {
      const d = await API.fetchStatus(currentTaskId);

      if (d.status === 'idle' || !d.task_id) {
        _stopPolling();
        _setRunningUI(false);
        $('prog-title').textContent = 'Ready — server may have restarted.';
        localStorage.removeItem(TASK_ID_KEY);
        return;
      }

      $('prog-bar').style.width   = d.pct + '%';
      $('prog-title').textContent = d.title || '…';
      $('prog-count').textContent = d.total > 0 ? `${d.done} / ${d.total}` : '';

      const entries = d.log || [];
      if (isReconnect && lastLogCount === 0 && entries.length) {
        $('prog-log').innerHTML = '';
        entries.forEach(e => _appendLog(e.msg, e.kind));
        lastLogCount = entries.length;
      } else {
        entries.slice(lastLogCount).forEach(e => _appendLog(e.msg, e.kind));
        lastLogCount = entries.length;
      }

      if (d.status === 'done') {
        _stopPolling();
        _setRunningUI(false);
        localStorage.removeItem(TASK_ID_KEY);
        $('reconnect-banner').classList.remove('show');
        $('prog-bar').style.width = '100%';
        setTimeout(refreshStore, 1000);
      } else if (d.status === 'error' || d.status === 'cancelled') {
        _stopPolling();
        _setRunningUI(false);
        localStorage.removeItem(TASK_ID_KEY);
        $('reconnect-banner').classList.remove('show');
      } else {
        _setRunningUI(true);
      }
    } catch (_) {}
  }

  function _setRunningUI(running) {
    $('fetch-btn').disabled    = running;
    $('fetch-btn').textContent = running ? '⏳ Fetch running…' : '▶ Start Fetch';
    $('cancel-btn').classList.toggle('show', running);
    $('live-pill').classList.toggle('show', running);
  }

  function _appendLog(msg, kind) {
    const log = $('prog-log');
    const el  = document.createElement('div');
    el.className   = kind === 'ok' ? 'log-ok' : kind === 'err' ? 'log-err' : 'log-inf';
    el.textContent = msg;
    log.appendChild(el);
    log.scrollTop = log.scrollHeight;
  }

  /* ════════════════════════════════════════════════════════════
     STORE BROWSER
  ════════════════════════════════════════════════════════════ */
  async function refreshStore() {
    try {
      const d = await API.storeList();
      storeAll = d.items || [];

      $('ss-tot').textContent = d.total_symbols;
      $('ss-mb').textContent  = d.total_size_mb + ' MB';
      $('ss-ivs').textContent = Object.keys(d.by_interval || {}).join(', ') || '–';

      const ivs = Object.keys(d.by_interval || {});
      $('itv-tabs').innerHTML = ['all', ...ivs].map(iv => {
        const cnt    = iv === 'all' ? d.total_symbols : (d.by_interval[iv] || 0);
        const active = (iv === 'all' && !storeFilter) || storeFilter === iv;
        return `<button class="itv-tab${active?' active':''}" `+
               `onclick="DataFetch.filterStore('${iv}')">`+
               `${iv === 'all' ? 'All' : iv} (${cnt})</button>`;
      }).join('');

      _renderTable();
    } catch (_) {}
  }

  function filterStore(iv) {
    storeFilter = iv === 'all' ? null : iv;
    document.querySelectorAll('.itv-tab').forEach(b =>
      b.classList.toggle('active',
        b.textContent.startsWith(iv === 'all' ? 'All' : iv))
    );
    _renderTable();
  }

  function _renderTable() {
    const items = storeFilter ? storeAll.filter(r => r.interval === storeFilter) : storeAll;
    const body  = $('store-body');
    if (!items.length) {
      body.innerHTML =
        `<div class="empty-st" style="padding:60px 0">`+
        `<div class="e-ico">🗄</div>`+
        `<div class="e-txt">No data for this timeframe</div></div>`;
      return;
    }
    body.innerHTML =
      `<table class="store-table">
        <thead><tr>
          <th>Symbol</th><th>Interval</th><th>Rows</th>
          <th>Last Date</th><th>Size</th><th></th>
        </tr></thead>
        <tbody>
          ${items.map(r => `<tr>
            <td style="font-weight:600;font-family:var(--mono)">${r.symbol}</td>
            <td><span style="background:var(--s3);color:var(--accent);font-size:10px;padding:2px 6px;border-radius:4px;font-family:var(--mono)">${r.interval}</span></td>
            <td>${(r.rows || 0).toLocaleString()}</td>
            <td style="color:var(--muted)">${r.last_date}</td>
            <td style="color:var(--muted2)">${r.size_kb} KB</td>
            <td><button class="del-btn" onclick="DataFetch.delSym('${r.symbol}','${r.interval}')">✕</button></td>
          </tr>`).join('')}
        </tbody>
      </table>`;
  }

  async function delSym(sym, interval) {
    if (!confirm(`Delete ${sym} (${interval})?`)) return;
    await API.storeDelete(sym, interval);
    refreshStore();
  }

  /* ════════════════════════════════════════════════════════════
     INIT
  ════════════════════════════════════════════════════════════ */
  function init() {
    QS.renderNav('data', 'df-nav-status');

    // Restore prefs
    const s = QS.loadState();
    if (s.data_src)  setSource(s.data_src, false);
    if (s.data_tf)   selectTFVal(s.data_tf);
    if (s.data_univ) $('univ-sel').value = s.data_univ;
    if (s.data_wl)   $('wl-sel').value   = s.data_wl;

    ['univ-sel','wl-sel'].forEach(id =>
      $(id).addEventListener('change', () =>
        QS.saveState({ data_univ: $('univ-sel').value, data_wl: $('wl-sel').value })
      )
    );

    // Reconnect to any running task
    const savedId = localStorage.getItem(TASK_ID_KEY);
    if (savedId) { currentTaskId = savedId; _pollStatus(true); }

    checkAuth();
    refreshStore();
  }

  return {
    init,
    checkAuth, doOAuth, togglePaste, pasteToken, clearToken: clearTokenFn,
    setSource, selTF,
    startFetch, cancelFetch,
    refreshStore, filterStore, delSym,
  };
})();

window.DataFetch = DataFetch;
document.addEventListener('DOMContentLoaded', DataFetch.init);
