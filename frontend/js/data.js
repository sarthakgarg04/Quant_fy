/* ═══════════════════════════════════════════════════════════════════════════
   data.js  —  Data Fetch page controller
   Depends on: shared.js, api.js

   Sections
   ─────────
   1. State & constants
   2. Auth (Upstox)
   3. Source switching
   4. Crypto UI helpers  (chips, lookback)
   5. Fetch trigger & polling
   6. Store browser  (equity tab + crypto tab)
   7. Init
═══════════════════════════════════════════════════════════════════════════ */

const DataFetch = (() => {

  /* ─────────────────────────────────────────────────────────────────────────
     1. STATE & CONSTANTS
  ───────────────────────────────────────────────────────────────────────── */

  const $ = id => document.getElementById(id);

  // Default crypto symbol list — mirrors CRYPTO_SYMBOLS in crypto_fetch.py
  const DEFAULT_CRYPTO_SYMBOLS = [
    "BTC", "ETH", "SOL", "BNB", "XRP",
    "LINK", "AVAX", "DOGE", "ADA", "DOT",
  ];

  let source          = "upstox";   // "upstox" | "yfinance" | "crypto"
  let curTF           = "1d";       // active equity timeframe
  let lookbackDays    = 730;        // crypto lookback (days)
  let cryptoSymbols   = [...DEFAULT_CRYPTO_SYMBOLS];  // selected crypto symbols
  let storeTab        = "equity";   // "equity" | "crypto"
  let storeFilter     = null;       // equity interval filter
  let storeAll        = [];         // all equity store items
  let storeAllCrypto  = [];         // all crypto store items
  let pollTimer       = null;
  let currentTaskId   = null;
  let lastLogCount    = 0;

  const TASK_ID_KEY   = "qs_fetch_task_id";

  /* ─────────────────────────────────────────────────────────────────────────
     2. AUTH  (Upstox only — shown/hidden based on source)
  ───────────────────────────────────────────────────────────────────────── */

  async function checkAuth() {
    try {
      const d = await API.checkToken();
      if (d.valid) {
        $("auth-icon").textContent      = "✅";
        $("auth-name").textContent      = d.name  || "Authenticated";
        $("auth-sub").textContent       = d.email || "Upstox token valid";
        $("auth-box").style.borderColor = "var(--green)";
        $("logout-btn").style.display   = "inline-flex";
      } else {
        $("auth-icon").textContent      = "🔒";
        $("auth-name").textContent      = "Not authenticated";
        $("auth-sub").textContent       = d.reason === "expired"
          ? "Token expired – re-authenticate"
          : "Token required for Upstox";
        $("auth-box").style.borderColor = "var(--border)";
        $("logout-btn").style.display   = "none";
      }
    } catch (_) {}
  }

  function doOAuth() {
    API.authStart().then(d => {
      if (d.auth_url) window.open(d.auth_url, "_blank", "width=600,height=700");
      const poll = setInterval(async () => {
        const d2 = await API.checkToken();
        if (d2.valid) { clearInterval(poll); checkAuth(); }
      }, 3000);
      setTimeout(() => clearInterval(poll), 120_000);
    });
  }

  function togglePaste() {
    const a = $("paste-area");
    a.style.display = a.style.display === "none" ? "block" : "none";
  }

  async function pasteToken() {
    const t = $("token-inp").value.trim();
    if (!t) { alert("Token is empty"); return; }
    await API.setToken(t);
    $("paste-area").style.display = "none";
    checkAuth();
  }

  async function clearTokenFn() {
    if (!confirm("Clear saved token?")) return;
    await API.clearToken();
    checkAuth();
  }

  /* ─────────────────────────────────────────────────────────────────────────
     3. SOURCE SWITCHING
     Shows/hides the correct config sections for each source.
  ───────────────────────────────────────────────────────────────────────── */

  function setSource(src, persist = true) {
    source = src;

    // Button active states
    $("src-up").classList.toggle("active",     src === "upstox");
    $("src-yf").classList.toggle("active",     src === "yfinance");
    $("src-crypto").classList.toggle("active", src === "crypto");

    const isCrypto  = src === "crypto";
    const isUpstox  = src === "upstox";
    const isYF      = src === "yfinance";

    // Show / hide sections
    $("auth-sec").style.display    = isUpstox  ? "block" : "none";
    $("tf-sec").style.display      = isCrypto  ? "none"  : "block";
    $("univ-sec").style.display    = isUpstox  ? "block" : "none";
    $("wl-sec").style.display      = isYF      ? "block" : "none";
    $("crypto-sec").style.display  = isCrypto  ? "block" : "none";
    $("force-sec").style.display   = isCrypto  ? "none"  : "flex";

    // Hint text
    const hints = {
      upstox:   "Primary source. Requires valid Upstox token.",
      yfinance: "Fallback source. No auth required.",
      crypto:   "Binance USDT-M perpetuals. No API key required. Fetches 1m canonical store.",
    };
    $("src-hint").textContent = hints[src] || "";

    if (persist) QS.saveState({ data_src: src });

    // Switch store tab to match source
    if (isCrypto) setStoreTab("crypto", false);
    else          setStoreTab("equity", false);
  }

  /* ─────────────────────────────────────────────────────────────────────────
     4. CRYPTO UI HELPERS
  ───────────────────────────────────────────────────────────────────────── */

  function _renderCryptoChips() {
    const container = $("crypto-chips");
    container.innerHTML = DEFAULT_CRYPTO_SYMBOLS.map(sym => {
      const active = cryptoSymbols.includes(sym);
      return `<button
        class="crypto-chip${active ? " active" : ""}"
        data-sym="${sym}"
        onclick="DataFetch.toggleCryptoSym('${sym}')"
      >${sym}</button>`;
    }).join("");
  }

  function toggleCryptoSym(sym) {
    if (cryptoSymbols.includes(sym)) {
      if (cryptoSymbols.length === 1) return;  // always keep ≥1
      cryptoSymbols = cryptoSymbols.filter(s => s !== sym);
    } else {
      cryptoSymbols = [...cryptoSymbols, sym];
    }
    _renderCryptoChips();
  }

  function setLookback(btn) {
    document.querySelectorAll(".lookback-btn").forEach(b => b.classList.remove("active"));
    btn.classList.add("active");
    lookbackDays = parseInt(btn.dataset.days, 10);
  }

  /* ─────────────────────────────────────────────────────────────────────────
     5. FETCH TRIGGER & POLLING
  ───────────────────────────────────────────────────────────────────────── */

  function selTF(btn) {
    document.querySelectorAll(".tf-pill").forEach(b => b.classList.remove("active"));
    btn.classList.add("active");
    curTF = btn.dataset.tf;
    QS.saveState({ data_tf: curTF });
  }

  function selectTFVal(tf) {
    const btn = document.querySelector(`.tf-pill[data-tf="${tf}"]`);
    if (btn) selTF(btn);
  }

  async function startFetch() {
    const btn = $("fetch-btn");

    // Reconnect if a task is already running on the server
    try {
      const sc = await API.fetchStatus();
      if (sc.status === "running" || sc.status === "starting") {
        currentTaskId = sc.task_id;
        localStorage.setItem(TASK_ID_KEY, currentTaskId);
        $("reconnect-banner").classList.add("show");
        _startPolling(true);
        return;
      }
    } catch (_) {}

    // Reset progress UI
    $("prog-log").innerHTML     = "";
    $("prog-bar").style.width   = "0%";
    $("prog-count").textContent = "";
    $("prog-title").textContent = "Starting…";
    $("reconnect-banner").classList.remove("show");
    lastLogCount = 0;

    btn.disabled    = true;
    btn.textContent = "⏳ Starting…";
    _setRunningUI(true);

    // Build request body based on active source
    const body = {
      tf:       curTF,
      src:      source,
      universe: $("univ-sel") ? $("univ-sel").value : "NSE_EQ",
      wl:       $("wl-sel")   ? $("wl-sel").value   : "nifty50",
      force:    $("force-chk") ? $("force-chk").checked : false,
    };

    if (source === "crypto") {
      body.crypto_symbols = cryptoSymbols;
      body.lookback_days  = lookbackDays;
    }

    try {
      const d = await API.fetchStart(body);

      if (!d.ok) {
        if (d.already_running) {
          currentTaskId = d.task_id;
          localStorage.setItem(TASK_ID_KEY, currentTaskId);
          $("reconnect-banner").classList.add("show");
          _startPolling(true);
        } else {
          $("prog-title").textContent = d.error || "Start failed";
          _setRunningUI(false);
          btn.disabled    = false;
          btn.textContent = "▶ Start Fetch";
        }
        return;
      }

      currentTaskId = d.task_id;
      localStorage.setItem(TASK_ID_KEY, currentTaskId);
      _startPolling(false);

    } catch (e) {
      $("prog-title").textContent = `Error: ${e.message}`;
      _setRunningUI(false);
      btn.disabled    = false;
      btn.textContent = "▶ Start Fetch";
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

      if (d.status === "idle" || !d.task_id) {
        _stopPolling();
        _setRunningUI(false);
        $("prog-title").textContent = "Ready — server may have restarted.";
        localStorage.removeItem(TASK_ID_KEY);
        return;
      }

      // Progress bar & counters
      $("prog-bar").style.width   = (d.pct || 0) + "%";
      $("prog-title").textContent = d.title || "…";
      $("prog-count").textContent = d.total > 0
        ? `${d.done} / ${d.total}  ·  ✓ ${d.ok_count}  ✗ ${d.err_count}`
        : "";

      // Append new log lines only
      const entries = d.log || [];
      if (isReconnect && lastLogCount === 0 && entries.length) {
        $("prog-log").innerHTML = "";
        entries.forEach(e => _appendLog(e.msg, e.kind));
        lastLogCount = entries.length;
      } else {
        entries.slice(lastLogCount).forEach(e => _appendLog(e.msg, e.kind));
        lastLogCount = entries.length;
      }

      // Terminal states
      if (d.status === "done") {
        _stopPolling();
        _setRunningUI(false);
        localStorage.removeItem(TASK_ID_KEY);
        $("reconnect-banner").classList.remove("show");
        $("prog-bar").style.width = "100%";
        setTimeout(refreshStore, 1000);   // auto-refresh store on completion
      } else if (d.status === "error" || d.status === "cancelled") {
        _stopPolling();
        _setRunningUI(false);
        localStorage.removeItem(TASK_ID_KEY);
        $("reconnect-banner").classList.remove("show");
      } else {
        _setRunningUI(true);
      }
    } catch (_) {}
  }

  function _setRunningUI(running) {
    const btn = $("fetch-btn");
    btn.disabled    = running;
    btn.textContent = running ? "⏳ Fetch running…" : "▶ Start Fetch";
    $("cancel-btn").classList.toggle("show", running);
    $("live-pill").classList.toggle("show",  running);
  }

  function _appendLog(msg, kind) {
    const log = $("prog-log");
    const el  = document.createElement("div");
    el.className   = kind === "ok"  ? "log-ok"
                   : kind === "err" ? "log-err"
                   :                  "log-inf";
    el.textContent = msg;
    log.appendChild(el);
    log.scrollTop  = log.scrollHeight;
  }

  /* ─────────────────────────────────────────────────────────────────────────
     6. STORE BROWSER
     Two tabs: Equity (existing /api/data/store) + Crypto (/api/crypto/store)
  ───────────────────────────────────────────────────────────────────────── */

  function setStoreTab(tab, doRefresh = true) {
    storeTab = tab;
    $("tab-equity").classList.toggle("active", tab === "equity");
    $("tab-crypto").classList.toggle("active", tab === "crypto");
    if (doRefresh) refreshStore();
  }

  async function refreshStore() {
    if (storeTab === "crypto") {
      await _refreshCryptoStore();
    } else {
      await _refreshEquityStore();
    }
  }

  // ── Equity store (existing API) ───────────────────────────────────────────
  async function _refreshEquityStore() {
    try {
      const d = await API.storeList();
      storeAll = d.items || [];

      $("ss-tot").textContent  = d.total_symbols;
      $("ss-mb").textContent   = (d.total_size_mb || 0) + " MB";
      $("ss-ivs").textContent  = Object.keys(d.by_interval || {}).join(", ") || "–";
      $("ss-path").textContent = "data_store/";

      const ivs = Object.keys(d.by_interval || {});
      $("itv-tabs").innerHTML = ["all", ...ivs].map(iv => {
        const cnt    = iv === "all" ? d.total_symbols : (d.by_interval[iv] || 0);
        const active = (iv === "all" && !storeFilter) || storeFilter === iv;
        return `<button class="itv-tab${active ? " active" : ""}"
                        onclick="DataFetch.filterStore('${iv}')">
                  ${iv === "all" ? "All" : iv} (${cnt})
                </button>`;
      }).join("");

      _renderEquityTable();
    } catch (_) {}
  }

  function filterStore(iv) {
    storeFilter = iv === "all" ? null : iv;
    document.querySelectorAll(".itv-tab").forEach(b =>
      b.classList.toggle("active",
        b.textContent.trim().startsWith(iv === "all" ? "All" : iv))
    );
    _renderEquityTable();
  }

  function _renderEquityTable() {
    const items = storeFilter
      ? storeAll.filter(r => r.interval === storeFilter)
      : storeAll;
    _renderTable(items, "equity");
  }

  // ── Crypto store ──────────────────────────────────────────────────────────
  async function _refreshCryptoStore() {
    try {
      const d = await fetch("/api/crypto/store").then(r => r.json());
      storeAllCrypto = d.items || [];

      $("ss-tot").textContent  = d.total_symbols;
      $("ss-mb").textContent   = (d.total_size_mb || 0) + " MB";
      $("ss-ivs").textContent  = "1m canonical";
      $("ss-path").textContent = "data_store/crypto/raw_1m/";
      $("itv-tabs").innerHTML  = "";   // no interval filter for crypto

      _renderTable(storeAllCrypto, "crypto");
    } catch (_) {}
  }

  // ── Shared table renderer ─────────────────────────────────────────────────
  function _renderTable(items, mode) {
    const body = $("store-body");
    if (!items.length) {
      body.innerHTML = `
        <div class="empty-st" style="padding:60px 0">
          <div class="e-ico">${mode === "crypto" ? "₿" : "🗄"}</div>
          <div class="e-txt">No data ${mode === "crypto" ? "for crypto perps" : "for this timeframe"}</div>
          <div class="e-sub">Fetch data using the controls on the left</div>
        </div>`;
      return;
    }

    const isCrypto = mode === "crypto";

    body.innerHTML = `
      <table class="store-table">
        <thead><tr>
          <th>Symbol</th>
          <th>Interval</th>
          <th>Rows</th>
          ${isCrypto ? "<th>First Date</th>" : ""}
          <th>Last Date</th>
          <th>Size</th>
          <th></th>
        </tr></thead>
        <tbody>
          ${items.map(r => `
            <tr>
              <td style="font-weight:600;font-family:var(--mono)">${r.symbol}</td>
              <td>
                <span style="background:var(--s3);color:${isCrypto ? "#f59e0b" : "var(--accent)"};
                             font-size:10px;padding:2px 6px;border-radius:4px;
                             font-family:var(--mono)">
                  ${r.interval}
                </span>
              </td>
              <td>${(r.rows || 0).toLocaleString()}</td>
              ${isCrypto ? `<td style="color:var(--muted)">${r.first_date || "–"}</td>` : ""}
              <td style="color:var(--muted)">${r.last_date || "–"}</td>
              <td style="color:var(--muted2)">${r.size_kb} KB</td>
              <td>
                <button class="del-btn"
                  onclick="${isCrypto
                    ? `DataFetch.delCryptSym('${r.symbol}')`
                    : `DataFetch.delSym('${r.symbol}','${r.interval}')`
                  }">✕</button>
              </td>
            </tr>`).join("")}
        </tbody>
      </table>`;
  }

  // ── Delete helpers ────────────────────────────────────────────────────────
  async function delSym(sym, interval) {
    if (!confirm(`Delete ${sym} (${interval})?`)) return;
    await API.storeDelete(sym, interval);
    refreshStore();
  }

  async function delCryptSym(symKey) {
    // symKey = "BTCUSDT_PERP" — strip to base "BTC"
    const base = symKey.replace("USDT_PERP", "").replace("USDT", "").replace("_PERP", "");
    if (!confirm(`Delete all data for ${symKey} (raw 1m + derived TFs)?`)) return;
    await fetch("/api/crypto/store/delete", {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify({ base }),
    });
    refreshStore();
  }

  /* ─────────────────────────────────────────────────────────────────────────
     7. INIT
  ───────────────────────────────────────────────────────────────────────── */

  function init() {
    QS.renderNav("data", "df-nav-status");

    // Restore persisted preferences
    const s = QS.loadState();
    if (s.data_src)  setSource(s.data_src, false);
    if (s.data_tf)   selectTFVal(s.data_tf);
    if (s.data_univ && $("univ-sel")) $("univ-sel").value = s.data_univ;
    if (s.data_wl   && $("wl-sel"))   $("wl-sel").value   = s.data_wl;

    // Persist equity dropdowns on change
    ["univ-sel", "wl-sel"].forEach(id => {
      const el = $(id);
      if (el) el.addEventListener("change", () =>
        QS.saveState({ data_univ: $("univ-sel")?.value, data_wl: $("wl-sel")?.value })
      );
    });

    // Render crypto chips (static — always all 10 shown, togglable)
    _renderCryptoChips();

    // Reconnect to any fetch task still running on the server
    const savedId = localStorage.getItem(TASK_ID_KEY);
    if (savedId) {
      currentTaskId = savedId;
      $("reconnect-banner").classList.add("show");
      _pollStatus(true);
      pollTimer = setInterval(() => _pollStatus(false), 800);
    }

    checkAuth();
    refreshStore();
  }

  /* ─────────────────────────────────────────────────────────────────────────
     Public API
  ───────────────────────────────────────────────────────────────────────── */
  return {
    init,
    // Auth
    checkAuth, doOAuth, togglePaste, pasteToken,
    clearToken: clearTokenFn,
    // Source & TF
    setSource, selTF,
    // Crypto UI
    toggleCryptoSym, setLookback,
    // Fetch
    startFetch, cancelFetch,
    // Store
    refreshStore, setStoreTab, filterStore,
    delSym, delCryptSym,
  };

})();

window.DataFetch = DataFetch;
document.addEventListener("DOMContentLoaded", DataFetch.init);
