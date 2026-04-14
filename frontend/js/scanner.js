/* ═══════════════════════════════════════════════════════════════
   scanner.js  v3.4
   ─────────────────────────────────────────────────────────────
   Fixes vs v3.3:
   1. Structure dropdown stays open when clicking checkboxes
      — document click handler now checks relatedTarget properly;
      dropdown content gets its own stopPropagation via event
      delegation on the wrapper, not on individual .tdo elements.
   2. Low order dropdown in multi-order mode now works
      — root cause: the global document click handler was closing
      ALL .tdd elements immediately after _sfToggle opened one,
      because the button click bubbled up. Fixed by tracking which
      dropdown is open and ignoring clicks inside it.
   3. Result rows show H / M / L structure states explicitly.
   4. Volume bars added to chart via LWC histogram series.
   Depends on: shared.js, api.js, chart.js
═══════════════════════════════════════════════════════════════ */

const Scanner = (() => {

  /* ── State ──────────────────────────────────────────────── */
  let rows       = [];
  let activeIdx  = -1;
  let chartTF    = '1d';
  let currentRow = null;

  let assetClass = "equity";   // "equity" | "crypto"
  let _activePivotLevel = 'L'; 
 
  // State keys — all persisted to localStorage via QS.saveState
  // (localStorage survives page navigation; sessionStorage does not)
  const $  = id => document.getElementById(id);
  const $$ = sel => document.querySelectorAll(sel);

  /* ── Zone colors ─────────────────────────────────────────── */
  const ZT = {
    rbr:           { bg:'rgba(34,197,94,.1)',   c:'#22c55e', b:'rgba(34,197,94,.3)'  },
    dbr:           { bg:'rgba(45,212,191,.1)',  c:'#2dd4bf', b:'rgba(45,212,191,.3)' },
    dbd:           { bg:'rgba(239,68,68,.1)',   c:'#ef4444', b:'rgba(239,68,68,.3)'  },
    rbd:           { bg:'rgba(249,115,22,.1)',  c:'#f97316', b:'rgba(249,115,22,.3)' },
    consolidation: { bg:'rgba(167,139,250,.1)', c:'#a78bfa', b:'rgba(167,139,250,.3)'},
  };

  /* ── Structure state helpers ─────────────────────────────── */
  const STRUCT_BULL = new Set([
    'trending_up',
    'top_coil',
    'top_expanding',
    'bottom_expanding_breakout',
    'bottom_pullback_breakout',
    'coiling_to_up',
    'expanding_to_up',
    'structure_up',
  ]);
  const STRUCT_BEAR = new Set([
    'trending_down',
    'top_expanding_breakdown',
    'top_pullback_breakdown',
    'bottom_coil',
    'bottom_expanding',
    'coiling_to_down',
    'expanding_to_down',
    'structure_down',
  ]);

  function structColor(s) {
    if (!s || s === 'none' || s === 'no_structure') return 'var(--muted)';
    if (STRUCT_BULL.has(s)) return '#22c55e';
    if (STRUCT_BEAR.has(s)) return '#ef4444';
    return '#f59e0b';
  }
  function _structLabel(s) {
    return (s || '—').replace(/_/g, ' ');
  }

  /* ════════════════════════════════════════════════════════════
     STRUCTURE STATE DROPDOWNS
     ─────────────────────────────────────────────────────────
     BUG FIX 1: Dropdown stays open during multi-select
     Root cause: the global `document.addEventListener('click')`
     in init() was firing after every checkbox click and closing
     all .tdd panels. Fixed by:
       a) Using a module-level `_openDd` ref to track which dd
          is currently open.
       b) The document click handler only closes if the click
          was OUTSIDE the currently open dropdown wrapper.
       c) Each dropdown wrapper gets `onclick="event.stopPropagation()"`
          so internal clicks never bubble to the document handler.

     BUG FIX 2: Low-order dropdown not responding
     Root cause: When multi-order mode renders all three dropdowns
     via innerHTML, the LOW dropdown's button had id="sftrig-low"
     but there was a second element with id="sftrig-low" from the
     single-order render (still in DOM but hidden). getElementById
     always returns the first match, which was the hidden one.
     Fixed by: single-wrap is emptied (not just hidden) when
     switching to multi mode, and vice versa.
  ════════════════════════════════════════════════════════════ */
  // Structure filter — delegated to shared Filters module
  const _F = Filters.create('sc');
  Filters._register(_F);
  const _selected = _F.selected;   // keep _selected alias for existing code

  /* Build ONE dropdown for a given level.
     Key: unique IDs use level + a random suffix to guarantee
     no ID collision when both single and multi wrappers exist.  */

  function _renderStructureFilters() {
    const moOn = $('moen')?.checked;
    _F.renderStructureFilters(
      $('sf-single-wrap'),
      $('sf-multi-wrap'),
      moOn,
      { high: $('moh')?.value, mid: $('mom')?.value, low: $('mol')?.value }
    );
  }

  /* Toggle a structure dropdown open/closed.
     wrapperId identifies the specific wrapper so we can track
     which one is open without relying on getElementById finding
     the wrong element when IDs are duplicated.               */
  function __sfToggle(e, level, wrapperId) { Filters._sfToggle(e, 'sc', level, wrapperId); }
  function __sfAllToggle(level, checked)   { Filters._sfAllToggle('sc', level, checked); }
  function __sfChange(level)               { Filters._sfChange('sc', level); }

  function _getStructureParam(level) { return _F.getStructureParam(level); }

  /* ════════════════════════════════════════════════════════════
     SIDEBAR
  ════════════════════════════════════════════════════════════ */
  function toggleSB() {
    const sb = $('sidebar');
    if (!sb) return;
    sb.classList.toggle('col');
    _saveUIState();
  }

  /* ════════════════════════════════════════════════════════════
     FORM HELPERS
  ════════════════════════════════════════════════════════════ */
  function stp(id, dv) {
    const el   = $(id);
    const step = parseFloat(el.step) || 1;
    el.value   = step < 1
      ? parseFloat((parseFloat(el.value) + dv).toFixed(2))
      : Math.round(parseFloat(el.value) + dv);
    if (['order','moh','mom','mol'].includes(id)) _updatePivotButtons();
    _saveUIState();
  }

  function setDir(btn) {
    $$('#dir-seg .seg-btn').forEach(b => b.className = 'seg-btn');
    btn.className = 'seg-btn ' + (btn.dataset.v === 'buy' ? 'buy' : 'sell');
    _saveUIState();
  }


  function setAsset(btn) {
    assetClass = btn.dataset.v;  // 'equity' | 'crypto'
    const isCrypto = assetClass === 'crypto';
  
    // Update asset segment buttons
    document.querySelectorAll('#asset-seg .seg-btn').forEach(b => {
      b.className = 'seg-btn' + (b.dataset.v === assetClass ? ' buy' : '');
    });
  
    // Show/hide scan TF buttons based on data-asset attribute
    document.querySelectorAll('#tfc-row .tfc').forEach(b => {
      const attr = b.dataset.asset || 'both';
      const visible = attr === 'both'
        || (isCrypto  && attr === 'crypto')
        || (!isCrypto && attr === 'equity');
      b.style.display = visible ? '' : 'none';
    });
  
    // Set sensible default active TF for the selected asset class
    const defaultTF = isCrypto ? '15m' : '1d';
    document.querySelectorAll('#tfc-row .tfc').forEach(b => {
      b.classList.toggle('on', b.dataset.v === defaultTF && b.offsetParent !== null);
    });
  
    // Show/hide crypto note
    const note = document.getElementById('crypto-tf-note');
    if (note) note.style.display = isCrypto ? 'block' : 'none';
  
    // Update asset badge in results header
    const badge = document.getElementById('asset-badge');
    if (badge) {
      badge.textContent        = isCrypto ? '₿ CRYPTO' : 'EQUITY';
      badge.style.background   = isCrypto ? 'rgba(245,158,11,0.12)' : 'rgba(99,102,241,0.12)';
      badge.style.borderColor  = isCrypto ? 'rgba(245,158,11,0.3)'  : 'rgba(99,102,241,0.3)';
      badge.style.color        = isCrypto ? '#f59e0b'                : 'var(--accent)';
    }
  
    // Clear stale results from the other asset class
    rows       = [];
    currentRow = null;
    activeIdx  = -1;
    const rlist = document.getElementById('rlist');
    if (rlist) rlist.innerHTML = `
      <div class="empty-st">
        <div class="e-ico">${isCrypto ? '₿' : '📊'}</div>
        <div class="e-txt">Press Scan to find ${isCrypto ? 'crypto' : 'equity'} setups</div>
        <div class="e-sub">Configure filters → Run Scan</div>
      </div>`;
    const rcnt = document.getElementById('rcnt');
    if (rcnt) rcnt.textContent = '—';
  
    // ── NEW: Show/hide chart TF buttons based on asset class ──────────────
    document.querySelectorAll('.tfg .tfb').forEach(b => {
      const attr = b.dataset.asset || 'both';
      const visible = attr === 'both'
        || (isCrypto  && attr === 'crypto')
        || (!isCrypto && attr === 'equity');
      b.style.display = visible ? '' : 'none';
    });

    // ── NEW: Reset chart TF to sensible default for the new asset class ───
    const defaultChartTF = isCrypto ? '15m' : '1d';
    chartTF = defaultChartTF;
    document.querySelectorAll('.tfg .tfb').forEach(b => {
      b.classList.toggle('on', b.dataset.tf === defaultChartTF && b.offsetParent !== null);
    });

    // Persist — use localStorage so asset class survives page navigation
    QS.saveState({ sc_asset: assetClass });
  }


  function getDir() {
    const a = $('dir-seg').querySelector('.buy,.sell');
    return a?.dataset.v || 'buy';
  }

 

  function setTFC(btn) {
    // Only deactivate buttons belonging to the current asset class
    // Buttons carry data-asset="equity|crypto|both"
    document.querySelectorAll('#tfc-row .tfc').forEach(b => {
      const asset = b.dataset.asset || 'both';
      if (asset === 'both' || asset === assetClass) {
        b.classList.remove('on');
      }
    });
    btn.classList.add('on');
    if (typeof _saveUIState === 'function') _saveUIState();
  }
  
  function getScanTF() {
    // Find the active button that is visible (not display:none)
    const active = document.querySelector('#tfc-row .tfc.on');
    if (active && active.offsetParent !== null) return active.dataset.v;
    // Fallback: first visible button
    const first = [...document.querySelectorAll('#tfc-row .tfc')]
      .find(b => b.offsetParent !== null);
    return first?.dataset.v || (assetClass === 'crypto' ? '15m' : '1d');
  }

  function onStrategyChange() {
    const val = $('strat').value;
    $('atr-sub-row').style.display = (val === 'atr' || val === 'both') ? 'flex' : 'none';
    _saveUIState();
  }

  function getAtrZoneTypes() {
    const types = [];
    if ($('atr-rbr')?.checked) types.push('rbr');
    if ($('atr-dbr')?.checked) types.push('dbr');
    if ($('atr-dbd')?.checked) types.push('dbd');
    if ($('atr-rbd')?.checked) types.push('rbd');
    return types.length ? types : ['rbr','dbr'];
  }

  /* Legacy trend dropdown */
  function toggleTDD(e) {
    e.stopPropagation();
    $('tdd').classList.toggle('op');
    $('ttrig').classList.toggle('op');
  }
  function allTrends(cb) {
    $$('.tc').forEach(c => c.checked = cb.checked);
    _updateTrendLabel(); _saveUIState();
  }
  function _updateTrendLabel() {
    const all = $$('.tc');
    const chk = [...all].filter(c => c.checked);
    $('all-t').checked       = chk.length === all.length;
    $('all-t').indeterminate = chk.length > 0 && chk.length < all.length;
    $('tlbl').textContent    = chk.length === all.length ? 'All trends'
      : chk.length === 0 ? 'No trends' : `${chk.length} selected`;
  }
  function getSelTrends() {
    const all = [...$$('.tc')];
    const chk = all.filter(x => x.checked).map(x => x.value);
    return chk.length === all.length ? '' : chk.join(',');
  }

  function toggleMO(enabled) {
    $('mob').classList.toggle('hid', !enabled);
    // Hide single-order input when multi-order is on, show when off
    const row = $('single-order-row');
    if (row) row.style.display = enabled ? 'none' : 'block';
    _updatePivotButtons();
    _renderStructureFilters();
    _saveUIState();
  }

  /* ════════════════════════════════════════════════════════════
     PIVOT ORDER BUTTONS
  ════════════════════════════════════════════════════════════ */
  function _updatePivotButtons() {
    const moEnabled = $('moen').checked;
    const container = $('pivot-order-btns');
    if (!container) return;
    container.innerHTML = '';

    const expectedLevels = moEnabled ? ['H','M','L'] : ['single'];

    // Reset selection if it's not valid for current mode
    if (!_activePivotLevel || !expectedLevels.includes(_activePivotLevel)) {
      _activePivotLevel = moEnabled ? 'L' : 'single';
    }
    Chart.setActiveLevels([_activePivotLevel]);

    if (!moEnabled) {
      const btn = document.createElement('button');
      btn.className     = 'pob on';
      btn.dataset.level = 'single';
      btn.innerHTML     = `<span class="pob-dot" style="background:#3b82f6"></span>Order ${$('order').value}`;
      btn.onclick       = () => _onPivotToggle('single', btn);
      container.appendChild(btn);
    } else {
      const orders = { H: $('moh').value||20, M: $('mom').value||10, L: $('mol').value||5 };
      ['H','M','L'].forEach(lv => {
        const mc  = Chart.moColors[lv];
        const btn = document.createElement('button');
        btn.className     = `pob ${mc.cls}` + (_activePivotLevel === lv ? ' on' : '');
        btn.dataset.level = lv;
        btn.innerHTML     = `<span class="pob-dot" style="background:${mc.dot}"></span>${mc.label}(${orders[lv]})`;
        btn.onclick       = () => _onPivotToggle(lv, btn);
        container.appendChild(btn);
      });
    }
    _updatePivotLegend();
  }

  function _onPivotToggle(level, btn) {
    _activePivotLevel = level;
    Chart.setActiveLevels([level]);
    $$('#pivot-order-btns .pob').forEach(b =>
      b.classList.toggle('on', b.dataset.level === level)
    );
    _updatePivotLegend();
  }

  function _updatePivotLegend() {
    const leg = $('pivot-order-legend'); if (!leg) return;
    if (!$('moen').checked) { leg.textContent = `order=${$('order').value}`; return; }
    const active = Chart.activeLevels;
    const parts  = [];
    if (active.includes('H')) parts.push(`H=${$('moh').value}`);
    if (active.includes('M')) parts.push(`M=${$('mom').value}`);
    if (active.includes('L')) parts.push(`L=${$('mol').value}`);
    leg.textContent = parts.join(' · ');
  }

  /* ════════════════════════════════════════════════════════════
     SCAN
  ════════════════════════════════════════════════════════════ */
  async function runScan() {
    _updatePivotButtons();
    const btn = document.getElementById("scanbtn");
    btn.disabled    = true;
    btn.textContent = "⏳  Scanning…";
    QS.setNavStatus("scanning…");
  
    document.getElementById("rlist").innerHTML = Array(6).fill(0).map(() =>
      `<div class="shimrow">
        <div class="sh" style="height:11px;width:40%"></div>
        <div class="sh" style="height:9px;width:60%;margin-top:4px"></div>
      </div>`
    ).join("");
    document.getElementById("rcnt").textContent = "…";
    activeIdx = -1;
  
    try {
      const mo  = document.getElementById("moen").checked;
      const tr  = getSelTrends();
  
      // Params are identical for both equity and crypto — same scanner engine
      const params = {
        direction:      getDir(),
        interval:       getScanTF(),
        order:          document.getElementById("order").value,
        zone_lookback:  document.getElementById("zlb").value,
        legout_mult:    document.getElementById("legout").value,
        strategy:       document.getElementById("strat").value,
        atr_zone_types: getAtrZoneTypes().join(","),
        multi_order:    mo,
        order_low:      document.getElementById("mol").value,
        order_mid:      document.getElementById("mom").value,
        order_high:     document.getElementById("moh").value,
        structure_low:  _getStructureParam("low"),
        structure_mid:  mo ? _getStructureParam("mid")  : "",
        structure_high: mo ? _getStructureParam("high") : "",
        alignment_filter: document.getElementById("align-filter")?.value || "any",
      };
      if (tr) params.trend_filter = tr;
  
      // Route to correct endpoint based on asset class
      let d;
      if (assetClass === "crypto") {
        d = await API.cryptoScan(params);
      } else {
        d = await API.scan(params);
      }
  
      rows = d.results || [];
      document.getElementById("rcnt").textContent = rows.length;
      QS.setNavStatus(`${rows.length} found`, `${rows.length} setups`);
      _buildList();
      // Save everything — rows go to localStorage so they survive page navigation
      _saveUIState();
      QS.saveState({ sc_rows: rows, sc_activeIdx: activeIdx });
  
    } catch (e) {
      document.getElementById("rlist").innerHTML =
        `<div class="empty-st"><span style="color:var(--red)">⚠ ${e.message}</span></div>`;
      QS.setNavStatus("error");
    } finally {
      btn.disabled    = false;
      btn.textContent = "▶  Run Scan";
    }
  }

  /* ════════════════════════════════════════════════════════════
     RESULTS LIST
     ─────────────────────────────────────────────────────────
     FIX 3: Each result row now shows H / M / L structure states
     explicitly as three small coloured badges so users know
     which order level triggered the filter match.
     The old single "structure badge" was ambiguous.
  ════════════════════════════════════════════════════════════ */
  function _structBadge(orderLabel, state, orderNum) {
    const col = structColor(state);
    const lbl = _structLabel(state);
    return `<span style="
        display:inline-flex;align-items:center;gap:3px;
        font-size:8px;font-family:var(--mono);font-weight:700;
        padding:1px 5px;border-radius:3px;white-space:nowrap;
        background:${col}18;border:1px solid ${col}44;color:${col}">
      <span style="opacity:.6">${orderLabel}${orderNum?'('+orderNum+')':''}</span>
      ${lbl}
    </span>`;
  }

  function _buildList() {
    const list  = $('rlist'); if (!list) return;
    const moOn  = $('moen')?.checked;

    if (!rows.length) {
      list.innerHTML = `<div class="empty-st"><div class="e-ico">◈</div>
        <div class="e-txt">No results found</div>
        <div class="e-sub">Broaden structure filter or increase lookback</div></div>`;
      return;
    }

    list.innerHTML = rows.map((r, i) => {
      const z   = ZT[r.zone_type] || ZT.rbr;
      const ac  = activeIdx === i ? ' on' : '';
      const isTrendOnly = !r.zone_type || r.zone_type === 'none';

      // Structure state badges — always show all three orders
      const hState  = r.struct_high || 'no_structure';
      const mState  = r.struct_mid  || 'no_structure';
      const lState  = r.struct_low  || 'no_structure';
      const hOrder  = r.mo_order_high;
      const mOrder  = r.mo_order_mid;
      const lOrder  = r.mo_order_low;

      // In single-order mode all three are the same — show just one
      const singleMode = !moOn;
      const structRow = singleMode
        ? _structBadge('', lState, lOrder)
        : `${_structBadge('H', hState, hOrder)}${_structBadge('M', mState, mOrder)}${_structBadge('L', lState, lOrder)}`;

      return `<div class="sr${ac}" data-i="${i}" onclick="Scanner.selStock(${i})">
        <div style="min-width:0;flex:1;overflow:hidden">
          <div style="display:flex;align-items:center;gap:5px">
            <div class="srn">${r.ticker.replace('.NS','')}</div>
            <span class="tbadge ${QS.trendClass(r.trend)}" style="flex-shrink:0">${QS.trendLabel(r.trend)}</span>
          </div>
          <div style="display:flex;flex-wrap:wrap;gap:3px;margin-top:3px">
            ${structRow}
          </div>
        </div>
        <div style="display:flex;flex-direction:column;align-items:flex-end;gap:3px;flex-shrink:0;padding-left:4px">
          <div class="srp">${QS.inr(r.close)}</div>
          ${isTrendOnly
            ? `<span style="font-size:9px;color:var(--muted);font-family:var(--mono)">${r.trend_strength||'—'}</span>`
            : `<span class="sra">${r.bars_ago}b</span>
               <span class="srt" style="background:${z.bg};color:${z.c};border:1px solid ${z.b}">
                 ${(r.zone_type||'rbr').toUpperCase()}
               </span>`
          }
        </div>
      </div>`;
    }).join('');
  }

  function _displayTicker(ticker) {
    if (assetClass === "crypto") {
      return ticker.replace("_PERP", "");
    }
    return ticker.replace(".NS", "").replace(".BO", "");
  }
  
  /**
   * Format price appropriately for the asset class.
   * Equity: Indian Rupee formatting  (QS.inr)
   * Crypto: USD formatting with dynamic decimal places
   *   BTC at ~65000 → "65,432.10"
   *   DOGE at ~0.15 → "0.1523"
   */
  function _formatPrice(price) {
    if (assetClass !== "crypto") return QS.inr(price);
    if (!price) return "—";
    if (price >= 1000)  return "$" + price.toLocaleString("en-US", { maximumFractionDigits: 2 });
    if (price >= 1)     return "$" + price.toFixed(4);
    return "$" + price.toFixed(6);
  }


  function _markActive(i) {
    $$('.sr.on').forEach(e => e.classList.remove('on'));
    const el = document.querySelector(`.sr[data-i="${i}"]`);
    if (el) el.classList.add('on');
    activeIdx = i;
  }

  function sortBy(k) {
    $('s-age').classList.toggle('on', k === 'bars_ago');
    $('s-atr').classList.toggle('on', k === 'atr_pct');
    rows.sort((a, b) => (a[k]||0) - (b[k]||0));
    _buildList();
    if (activeIdx >= 0) _markActive(activeIdx);
  }

  /* ════════════════════════════════════════════════════════════
     STOCK SELECTION + CHART LOAD
  ════════════════════════════════════════════════════════════ */
  /* ════════════════════════════════════════════════════════════
   CHART CONTEXT
   
   Two modes based on whether chartTF matches the scan interval:
     SCAN_CONTEXT  → pivot params locked to scan_params (lineage)
     EXPLORE       → pivot params from current UI inputs
  ════════════════════════════════════════════════════════════ */

  // Returns true if current chartTF matches the TF this row was scanned on
  function _inScanContext(row) {
    const scanTF = (row.scan_params || {}).interval || getScanTF();
    return chartTF === scanTF;
  }

  // Build chart params — locked to scan_params in scan context,
  // free UI inputs in explore mode
  function _buildChartParams(row) {
    const sp = row.scan_params || {};
 
    // Crypto always uses the crypto chart endpoint — override the API call
    // via a flag we check in selStock() below
    if (assetClass === "crypto") {
      return {
        interval:      chartTF,
        order:         sp.order         ?? document.getElementById("order").value,
        legout_mult:   sp.legout_mult   ?? document.getElementById("legout").value,
        strategy:      sp.strategy      ?? document.getElementById("strat").value,
        zone_lookback: sp.zone_lookback ?? document.getElementById("zlb").value,
        multi_order:   sp.multi_order   ?? document.getElementById("moen").checked,
        order_low:     sp.order_low     ?? document.getElementById("mol").value,
        order_mid:     sp.order_mid     ?? document.getElementById("mom").value,
        order_high:    sp.order_high    ?? document.getElementById("moh").value,
      };
    }

    if (_inScanContext(row) && sp.order) {
      // ── SCAN CONTEXT: use lineage-locked params ──────────────────────
      API.logEvent('info', 'scanner', 'Chart params: SCAN CONTEXT', {
        ticker: row.ticker, tf: chartTF,
        order: sp.order, order_low: sp.order_low,
      });
      return {
        interval:      chartTF,
        order:         sp.order,
        legout_mult:   sp.legout_mult   ?? $('legout').value,
        strategy:      sp.strategy      ?? $('strat').value,
        zone_lookback: sp.zone_lookback ?? $('zlb').value,
        multi_order:   sp.multi_order   ?? $('moen').checked,
        order_low:     sp.order_low,
        order_mid:     sp.order_mid,
        order_high:    sp.order_high,
      };
    }

    // ── EXPLORE MODE: use current UI inputs ──────────────────────────
    API.logEvent('info', 'scanner', 'Chart params: EXPLORE MODE', {
      ticker: row.ticker, chartTF,
      scanTF: sp.interval || getScanTF(),
      order: $('order').value,
    });
    return {
      interval:      chartTF,
      order:         $('order').value,
      legout_mult:   $('legout').value,
      strategy:      $('strat').value,
      zone_lookback: $('zlb').value,
      multi_order:   $('moen').checked,
      order_low:     $('mol').value,
      order_mid:     $('mom').value,
      order_high:    $('moh').value,
    };
  }

  // Update the context badge in the chart header
  function _updateContextBadge(row) {
    const badge = $('tf-context-badge');
    if (!badge) return;
    const sp = row.scan_params || {};
    const scanTF = sp.interval || getScanTF();

    if (chartTF === scanTF) {
      badge.style.display = 'none';
      badge.textContent   = '';
    } else {
      badge.textContent   = `exploring ${chartTF.toUpperCase()} · scanned on ${scanTF.toUpperCase()}`;
      badge.style.display = 'inline-block';
    }
  }

  // Zone legin marker is only valid on the scan TF
  function _getZoneLegInTs(row) {
    return _inScanContext(row) ? row.zone_legin_ts : null;
  }

  /* ────────────────────────────────────────────────────────── */

  async function selStock(i, fromTFButton = false) {
    console.log("selStock called", i);
    console.log("ROW DATA:", rows[i]);
    console.log("Loading chart for:", rows[i].ticker);
    
    const row = rows[i];
    if (!row) {
      API.logEvent('warn', 'scanner', 'selStock: invalid index', { i, rowsLen: rows.length });
      return;
    }

    // On first click from results list, snap TF to scan interval → land in scan context
    if (!fromTFButton && !currentRow) {
      const scanTF = (row.scan_params || {}).interval || getScanTF();
      if (chartTF !== scanTF) {
        chartTF = scanTF;
        document.querySelectorAll('.tfg .tfb').forEach(b =>
          b.classList.toggle('on', b.dataset.tf === chartTF)
        );
      }
    }

    // Guard: already showing this ticker on this TF
    if (currentRow && currentRow.ticker === row.ticker && currentRow._tf === chartTF) {
      API.logEvent('debug', 'scanner', 'selStock guard hit', { ticker: row.ticker, tf: chartTF });
      _markActive(i);
      return;
    }

    API.logEvent('info', 'scanner', 'selStock: loading chart', {
      ticker: row.ticker, tf: chartTF, idx: i,
      mode: _inScanContext(row) ? 'scan_context' : 'explore',
    });

    _markActive(i);
    currentRow = { ...row, _tf: chartTF };
    _populateInfoDrawer(currentRow);

    // Header
    // $('cptk').textContent = row.ticker.replace('.NS', '');
    // $('cppr').textContent = QS.inr(row.close);

    $('cptk').textContent = assetClass === 'crypto'
      ? row.ticker.replace('_PERP','')
      : row.ticker.replace('.NS','').replace('.BO','');
    $('cppr').textContent = assetClass === 'crypto'
      ? (row.close >= 1000 ? '$' + row.close.toLocaleString('en-US',{maximumFractionDigits:2})
        : row.close >= 1   ? '$' + row.close.toFixed(4)
        : '$' + row.close.toFixed(6))
      : QS.inr(row.close);


    const te = $('cptr');
    te.textContent   = QS.trendLabel(row.trend);
    te.className     = 'tbadge ' + QS.trendClass(row.trend);
    te.style.display = 'inline-block';

    // Metrics — suppress scan-specific values when exploring a different TF
    const inCtx = _inScanContext(row);
    $('m1').textContent = row.atr_pct + '%';
    $('m2').textContent = row.trend_strength || '—';
    $('m3').textContent = inCtx ? (row.zones_count ?? '—') : '—';
    $('m4').textContent = inCtx && row.bars_ago != null ? row.bars_ago + 'b' : '—';
    $('m5').textContent = row.pivot_count ?? '—';
    const vp = (row.vel_current || 0) >= 0;
    $('m6').textContent = `${vp ? '+' : ''}${row.vel_current}`;
    $('m6').className   = 'mv ' + (vp ? 'pos' : 'neg');
    $('m7').textContent = (row.lr_ratio || 0) + '×';
    $('m7').className   = 'mv ' + ((row.lr_ratio || 1) >= 1 ? 'pos' : 'neg');
    $('m8').textContent = row.amp_avg || '—';

    // Context badge
    _updateContextBadge(row);

    // Load chart with correct params for current mode
    async function _loadChartForRow(row, direction, zoneLeginTs) {
      if (assetClass === "crypto") {
        // Strip _PERP so the API receives "BTCUSDT" or "BTC"
        const cryptoBase = row.ticker.replace("_PERP", "");
        await Chart.load(
          cryptoBase,
          _buildChartParams(row),
          direction,
          zoneLeginTs,
          { endpoint: "crypto" },   // Chart.js uses this to call /api/crypto/chart
        );
      } else {
        await Chart.load(row.ticker, _buildChartParams(row), direction, zoneLeginTs);
      }
    }

    API.logEvent('info', 'scanner', 'Chart loaded', { ticker: row.ticker, tf: chartTF });
    // Persist active selection to localStorage
    QS.saveState({ sc_activeIdx: activeIdx });

    await _loadChartForRow(row, getDir(), row.zone_legin_ts);
    _populateInfoDrawer(row);
  }

  function setChartTF(tf, btn) {
    const prevTF = chartTF;
    chartTF = tf;
    document.querySelectorAll('.tfg .tfb').forEach(b => b.classList.remove('on'));
    btn.classList.add('on');
    Chart.bustCache(tf);
    _updatePivotButtons();
    _saveUIState();

    API.logEvent('info', 'scanner', 'Chart TF changed', {
      from: prevTF, to: tf,
      ticker: currentRow ? currentRow.ticker : null,
    });

    if (activeIdx >= 0 && rows[activeIdx]) {
      currentRow = null;               // force guard bypass in selStock
      selStock(activeIdx, true);       // fromTFButton=true → don't snap TF back
    } else {
      API.logEvent('warn', 'scanner', 'TF changed but no active symbol', { tf });
    }
  }

  /* ════════════════════════════════════════════════════════════
     PIVOT DRILL-DOWN — isolate one order level on the chart
  ════════════════════════════════════════════════════════════ */

  /**
   * Called when user clicks a level row in the drawer.
   * Isolates that level's pivot series on the chart and scrolls
   * to the most recent pivots for that level.
   *
   * level: 'H' | 'M' | 'L'
   * el:    the clicked row element (for active-state styling)
   */
  function _focusPivotLevel(level, el) {
    // Update active state styling on drawer rows
    document.querySelectorAll('#dws [data-level]').forEach(row => {
      row.classList.remove('pf-active');
      row.style.background   = 'var(--s2)';
      row.style.borderColor  = 'var(--border)';
    });
    el.classList.add('pf-active');
    el.style.background  = 'var(--s3)';
    el.style.borderColor = 'var(--border2)';

    // Isolate this level on the chart — dim all others
    Chart.setActiveLevels([level]);

    // Sync the pivot-order-btns in the chart header
    document.querySelectorAll('#pivot-order-btns .pob').forEach(b => {
      b.classList.toggle('on', b.dataset.level === level);
    });

    // Scroll chart to show the most recent pivots for this level
    Chart.scrollToLevel(level);
  }

  /**
   * Reset to showing all pivot levels — called by the ↺ all button.
   */
  function _resetPivotFocus() {
    // Clear active state on drawer rows
    document.querySelectorAll('#dws [data-level]').forEach(row => {
      row.classList.remove('pf-active');
      row.style.background  = 'var(--s2)';
      row.style.borderColor = 'var(--border)';
    });

    // Restore all levels on chart
    const moEnabled = $('moen').checked;
    const allLevels = moEnabled ? ['H', 'M', 'L'] : ['single'];
    Chart.setActiveLevels(allLevels);

    // Sync pivot-order-btns
    document.querySelectorAll('#pivot-order-btns .pob').forEach(b => {
      b.classList.add('on');
    });

    // Scroll back to latest candle
    Chart.scrollToLatest();
  }

  /* ════════════════════════════════════════════════════════════
     DETAIL DRAWER
  ════════════════════════════════════════════════════════════ */
  function openDrawer(e, i) {
    e.stopPropagation();
    const r = rows[i]; if (!r) return;
  
    /* Existing modal population — unchanged */
    $('dwtk').textContent = r.ticker.replace('.NS','').replace('.BO','');
    $('dwsb').textContent = `${QS.trendLabel(r.trend)} · ${r.pivot_count} pivots · ATR ${r.atr_pct}%`;
    $('dwpr').textContent = QS.inr(r.close);
    $('dwtr').textContent = QS.trendLabel(r.trend);
    $('dwtr').className   = 'tbadge ' + QS.trendClass(r.trend);
    $('dws').innerHTML    = _buildDrawerHTML(r);
    $('dw').classList.add('open');
    $('dov').classList.add('show');
  
    /* Also populate the hover drawer with the same content */
    _populateInfoDrawer(r);
  }
 
  function closeDrawer() {
    $('dw').classList.remove('open');
    $('dov').classList.remove('show');
  }
  
  
  /* ── STEP 3: Helper to populate InfoDrawer for zone scanner ─ */
  
  function _populateInfoDrawer(r) {
    if (!r) return;
  
    const ticker   = (r.ticker || '').replace('.NS','').replace('.BO','').replace('_PERP','');
    const subtitle = `ATR ${r.atr_pct ?? '—'}%`;
  
    /* Trend pill */
    const trendHtml = InfoDrawer.trendPill(r.trend);
  
    /* Zone info */
    const zoneHtml = (r.zone_type && r.zone_type !== 'none') ? InfoDrawer.section('Zone',
      InfoDrawer.statGrid([
        ['Type',     (r.zone_type||'—').toUpperCase()],
        ['Status',   r.zone_status  || '—'],
        ['Bars ago', r.bars_ago != null ? r.bars_ago + 'b' : '—'],
        ['Zones',    r.zones_count  ?? '—'],
        ['High',     r.zone_high != null ? QS.inr(r.zone_high) : '—'],
        ['Low',      r.zone_low  != null ? QS.inr(r.zone_low)  : '—'],
      ])
    ) : '';
  
    /* Structure */
    const moOn   = document.getElementById('moen')?.checked;
    const hState = r.struct_high || 'no_structure';
    const mState = r.struct_mid  || 'no_structure';
    const lState = r.struct_low  || 'no_structure';
  
    const stColor = s => {
      if (STRUCT_BULL.has(s)) return '#22c55e';
      if (STRUCT_BEAR.has(s)) return '#ef4444';
      return '#f59e0b';
    };
  
    const structBody = moOn
      ? [['H', hState, r.mo_order_high], ['M', mState, r.mo_order_mid], ['L', lState, r.mo_order_low]]
          .map(([lv, st, ord]) => InfoDrawer.moBar(lv, st, ord, stColor(st))).join('')
      : `<div style="font-size:11px;font-family:var(--mono);color:${stColor(lState)}">${(lState||'').replace(/_/g,' ')}</div>`;
  
    const structHtml = InfoDrawer.section('Structure', structBody);
  
    /* Momentum */
    const vp = (r.vel_current || 0) >= 0;
    const ac = r.vel_label === 'accelerating' ? '↑ Accel'
            : r.vel_label === 'decelerating' ? '↓ Decel' : '→ Steady';
  
    const momentumHtml = InfoDrawer.section('Momentum',
      InfoDrawer.statGrid([
        ['Vel Δ',    `<span class="${vp?'pos':'neg'}">${vp?'+':''}${r.vel_current??'—'}</span>`],
        ['Accel',    ac],
        ['LR Ratio', `<span class="${(r.lr_ratio||1)>=1?'pos':'neg'}">${r.lr_ratio||'—'}×</span>`],
        ['Amp Avg',  r.amp_avg  || '—'],
        ['Pivots',   r.pivot_count ?? '—'],
        ['ATR %',    (r.atr_pct ?? '—') + '%'],
      ])
    );
  
    /* Debug pivots */
    let debugHtml = '';
    if (moOn && (r.debug_pivots_H || r.debug_pivots_M || r.debug_pivots_L)) {
      const pvtTable = (pivots, label, col) => {
        if (!pivots?.length) return '';
        return `<div style="margin-bottom:8px">
          <div style="font-size:9px;font-weight:700;font-family:var(--mono);
                      color:${col};margin-bottom:4px">
            ${label} <span style="color:var(--muted2);font-weight:400">(${pivots.length})</span>
          </div>
          <table style="width:100%;border-collapse:collapse;font-size:10px;
                        font-family:var(--mono);line-height:1.7">
            ${pivots.map(p => `
              <tr style="border-bottom:1px solid var(--s3)">
                <td style="color:var(--muted2)">${p.d}</td>
                <td style="text-align:right">${Number(p.v).toLocaleString('en-IN',{maximumFractionDigits:2})}</td>
                <td style="text-align:center;font-weight:700;
                    color:${p.t==='T'?'#ef4444':'#22c55e'}">${p.t}</td>
              </tr>`).join('')}
          </table>
        </div>`;
      };
      debugHtml = InfoDrawer.section('Debug Pivots',
        pvtTable(r.debug_pivots_H, `HIGH ord=${r.mo_order_high||'?'}`, '#7c3aed') +
        pvtTable(r.debug_pivots_M, `MID  ord=${r.mo_order_mid||'?'}`,  '#1d4ed8') +
        pvtTable(r.debug_pivots_L, `LOW  ord=${r.mo_order_low||'?'}`,  '#15803d')
      );
    }
  
    InfoDrawer.populate(
      ticker,
      subtitle,
      trendHtml + zoneHtml + structHtml + momentumHtml + debugHtml
    );
  }

  function _buildDrawerHTML(r) {
    const vp  = (r.vel_current||0) >= 0;
    const vs  = vp ? '+' : '';
    const ac  = r.vel_label==='accelerating' ? '↑ Accel'
              : r.vel_label==='decelerating' ? '↓ Decel' : '→ Steady';
    const vw  = Math.min(Math.abs(r.vel_current||0) / Math.max(r.vel_avg||.001,.001) * 45, 45);
    const zt  = ZT[r.zone_type] || ZT.rbr;
    const zsc = r.zone_status==='fresh' ? '#22c55e' : r.zone_status==='tested' ? '#f59e0b' : 'var(--muted2)';
    const isTrendOnly = !r.zone_type || r.zone_type === 'none';

    // Three-order structure table
    const wasMO    = r.scan_params?.multi_order;
    const aln      = r.struct_alignment || '—';
    const bull     = r.struct_bull_count ?? 0;
    const bear     = r.struct_bear_count ?? 0;
    const alnColor = aln==='bullish'?'#22c55e' : aln==='bearish'?'#ef4444' : '#f59e0b';

    let structHtml;
    if (!wasMO) {
      // Single-order: show one row only, no H/M/L labels, no focus buttons
      const col  = structColor(r.struct_low);
      const slbl = _structLabel(r.struct_low);
      const ord  = r.mo_order_low || (r.scan_params?.order) || '—';
      structHtml = `
      <div>
        <div class="dwst">Pivot Structure</div>
        <div style="display:flex;align-items:center;gap:8px;padding:8px 10px;
                    background:var(--s2);border:1px solid var(--border);border-radius:var(--r2)">
          <span style="font-size:9px;font-family:var(--mono);color:var(--muted2)">Order ${ord}</span>
          <span style="font-size:12px;font-weight:600;color:${col}">${slbl}</span>
        </div>
      </div>`;
    } else {
      // Multi-order: full H/M/L table with focus buttons
      const levels = [
        { lbl:'HIGH', order: r.mo_order_high, state: r.struct_high, pair: r.struct_high_pair, cls:'moH' },
        { lbl:'MID',  order: r.mo_order_mid,  state: r.struct_mid,  pair: r.struct_mid_pair,  cls:'moM' },
        { lbl:'LOW',  order: r.mo_order_low,  state: r.struct_low,  pair: r.struct_low_pair,  cls:'moL' },
      ];
      structHtml = `
      <div>
        <div class="dwst">Pivot Structure
          <span style="font-size:9px;font-weight:400;text-transform:none;letter-spacing:0;
                color:${alnColor};margin-left:4px">
            ${aln} · 🟢${bull} 🔴${bear}
          </span>
          <button onclick="Scanner._resetPivotFocus()" title="Reset to show all levels"
            style="margin-left:6px;padding:1px 6px;font-size:9px;font-family:var(--mono);
                   background:var(--s2);border:1px solid var(--border);border-radius:3px;
                   color:var(--muted);cursor:pointer"
            onmouseover="this.style.color='var(--text)'"
            onmouseout="this.style.color='var(--muted)'">↺ all</button>
        </div>
        <div style="font-size:9px;color:var(--muted2);margin-bottom:8px;font-family:var(--mono)">
          Click a level to isolate its pivots on the chart
        </div>
        ${levels.map(lv => {
          const col    = structColor(lv.state);
          const slbl   = _structLabel(lv.state);
          const chartLv = lv.lbl === 'HIGH' ? 'H' : lv.lbl === 'MID' ? 'M' : 'L';
          return `<div class="mobr" data-level="${chartLv}"
                    style="background:var(--s2);border:1px solid var(--border);
                           border-radius:var(--r2);padding:7px 10px;margin-bottom:6px;cursor:pointer"
                    onclick="Scanner._focusPivotLevel('${chartLv}',this)">
            <span class="molk ${lv.cls}" style="font-size:9px">${lv.lbl}(${lv.order||'—'})</span>
            <div class="mobi">
              <div class="mobt" style="color:${col};font-weight:600">${slbl}</div>
              ${lv.pair ? `<div style="font-size:9px;color:var(--muted2);font-family:var(--mono)">${lv.pair}</div>` : ''}
            </div>
            <span style="font-size:9px;color:var(--accent);font-family:var(--mono)">focus</span>
          </div>`;
        }).join('')}
      </div>`;
    }

    const zoneHtml = isTrendOnly ? `
    <div>
      <div class="dwst">Strategy</div>
      <div style="font-size:11px;color:var(--muted);background:var(--s2);
           border:1px solid var(--border);border-radius:var(--r2);padding:10px 12px">
        Trend-only scan — no zone required.
      </div>
    </div>` : `
    <div>
      <div class="dwst">Zone Info</div>
      <div class="ig2">
        <div class="it"><div class="itl">Zone High</div><div class="itv">${QS.inr(r.zone_high)}</div></div>
        <div class="it"><div class="itl">Zone Low</div><div class="itv">${QS.inr(r.zone_low)}</div></div>
        <div class="it"><div class="itl">Type</div><div class="itv" style="color:${zt.c}">${(r.zone_type||'rbr').toUpperCase()}</div></div>
        <div class="it"><div class="itl">Status</div><div class="itv" style="color:${zsc}">${r.zone_status||'—'}</div></div>
        <div class="it"><div class="itl">Age</div><div class="itv">${r.bars_ago!=null?r.bars_ago+' bars':'—'}</div></div>
        <div class="it"><div class="itl">Vol Score</div><div class="itv">${r.vol_score!=null?(r.vol_score*100).toFixed(0)+'%':'—'}</div></div>
      </div>
    </div>`;

    const mainHtml = `${structHtml}${zoneHtml}
    <div>
      <div class="dwst">Velocity</div>
      <div class="vb">
        <div class="vc"></div>
        <div class="vf" style="${vp
          ?`left:50%;width:${vw}%;background:rgba(34,197,94,.25)`
          :`right:50%;width:${vw}%;background:rgba(239,68,68,.25)`}"></div>
        <span class="vlbl" style="color:${vp?'#22c55e':'#ef4444'}">${vs}${r.vel_current} pts/bar · ${ac}</span>
      </div>
      <div class="ig2" style="margin-top:6px">
        <div class="it"><div class="itl">Avg Velocity</div><div class="itv">${r.vel_avg}</div></div>
        <div class="it"><div class="itl">Acceleration</div>
          <div class="itv" style="color:${r.vel_label==='accelerating'?'#22c55e':r.vel_label==='decelerating'?'#ef4444':'var(--text)'}">
            ${r.vel_label||'—'}</div></div>
      </div>
    </div>
    <div>
      <div class="dwst">Amplitude</div>
      <div class="ig3">
        <div class="it"><div class="itl">Avg</div><div class="itv">${r.amp_avg}</div><div class="its">${r.amp_regime||'—'}</div></div>
        <div class="it"><div class="itl">Recent</div><div class="itv">${r.amp_recent}</div><div class="its">${(r.amp_variance||0).toFixed(1)}% var</div></div>
        <div class="it"><div class="itl">Regime</div>
          <div class="itv" style="font-size:10px;${r.amp_regime==='expanding'?'color:#22c55e':r.amp_regime==='contracting'?'color:#ef4444':''}">
            ${r.amp_regime||'—'}</div></div>
      </div>
    </div>
    <div>
      <div class="dwst">Leg Ratio</div>
      <div class="ig3">
        <div class="it"><div class="itl">Ratio</div>
          <div class="itv" style="color:${(r.lr_ratio||1)>=1?'#22c55e':'#ef4444'}">${r.lr_ratio}×</div>
          <div class="its">${(r.lr_label||'').replace(/_/g,' ')}</div></div>
        <div class="it"><div class="itl">Bull Avg</div><div class="itv pos">${r.lr_bull}</div></div>
        <div class="it"><div class="itl">Bear Avg</div><div class="itv neg">${r.lr_bear}</div></div>
      </div>
    </div>
    <div style="padding-bottom:8px">
      <div class="dwst">Market Context</div>
      <div class="ig2">
        <div class="it"><div class="itl">ATR %</div><div class="itv">${r.atr_pct}%</div></div>
        <div class="it"><div class="itl">ATR pts</div><div class="itv">${QS.inr(r.atr)}</div></div>
        <div class="it"><div class="itl">Pivot Count</div><div class="itv">${r.pivot_count}</div></div>
        <div class="it"><div class="itl">Trend Strength</div><div class="itv" style="font-size:10px">${r.trend_strength||'—'}</div></div>
      </div>
    </div>`;

    /* ── DEBUG PIVOTS ────────────────────────────────────────────────────────
       Renders the exact pivot list used at scan-time so you can cross-check
       against what the chart draws at runtime.
       Single-order → r.debug_pivots  (flat list)
       Multi-order  → r.debug_pivots_H / _M / _L  (one table per level)
       Backend must populate these fields via _debug_pivot_fields() in scanner.py.
       If the fields are absent (old scan result in sessionStorage) the section
       is silently omitted — no errors, no visible change.
    ──────────────────────────────────────────────────────────────────────── */
    function _pvtTable(pivots, label, dotColor) {
      if (!pivots || !pivots.length) return '';
      return `
        <div style="margin-bottom:10px">
          <div style="font-size:9px;font-weight:700;font-family:var(--mono);
                      color:${dotColor};margin-bottom:5px;letter-spacing:.3px">
            ${label}
            <span style="color:var(--muted2);font-weight:400">(${pivots.length} shown)</span>
          </div>
          <table style="width:100%;border-collapse:collapse;font-size:10px;font-family:var(--mono);line-height:1.7">
            <thead>
              <tr style="border-bottom:1px solid var(--border)">
                <th style="color:var(--muted2);font-weight:400;text-align:left;padding-bottom:3px">Date</th>
                <th style="color:var(--muted2);font-weight:400;text-align:right">Value</th>
                <th style="color:var(--muted2);font-weight:400;text-align:center;width:28px">T/B</th>
              </tr>
            </thead>
            <tbody>
              ${pivots.map(p => `
                <tr style="border-bottom:1px solid var(--s3)">
                  <td style="color:var(--muted2);padding:1px 0">${p.d}</td>
                  <td style="color:var(--text);text-align:right">${Number(p.v).toLocaleString('en-IN', {maximumFractionDigits:2})}</td>
                  <td style="text-align:center;font-weight:700;color:${p.t==='T'?'#ef4444':'#22c55e'}">${p.t}</td>
                </tr>`).join('')}
            </tbody>
          </table>
        </div>`;
    }

    let debugBlock = '';
    if (wasMO && (r.debug_pivots_H || r.debug_pivots_M || r.debug_pivots_L)) {
      debugBlock = `
        <div style="margin-top:14px;padding-bottom:8px">
          <div class="dwst">🔬 Debug Pivots
            <span style="font-weight:400;text-transform:none;letter-spacing:0;
                         color:var(--muted2);font-size:9px;margin-left:4px">scan-time · last 30 each</span>
          </div>
          ${_pvtTable(r.debug_pivots_H, `HIGH  ord=${r.mo_order_high||'?'}`, '#7c3aed')}
          ${_pvtTable(r.debug_pivots_M, `MID   ord=${r.mo_order_mid||'?'}`,  '#1d4ed8')}
          ${_pvtTable(r.debug_pivots_L, `LOW   ord=${r.mo_order_low||'?'}`,  '#3b82f6')}
        </div>`;
    } else if (!wasMO && r.debug_pivots?.length) {
      debugBlock = `
        <div style="margin-top:14px;padding-bottom:8px">
          <div class="dwst">🔬 Debug Pivots
            <span style="font-weight:400;text-transform:none;letter-spacing:0;
                         color:var(--muted2);font-size:9px;margin-left:4px">scan-time · last ${r.debug_pivots.length}</span>
          </div>
          ${_pvtTable(r.debug_pivots, `Order ${r.scan_params?.order||'?'}`, '#3b82f6')}
        </div>`;
    }

    return mainHtml + debugBlock;
  }

  /* ════════════════════════════════════════════════════════════
     PERSISTENT STATE
  ════════════════════════════════════════════════════════════ */
  function _saveUIState() {
    const trendVals = [...$$('.tc')].map(c => ({ v: c.value, chk: c.checked }));
    QS.saveState({
      sc_asset:   assetClass,
      sc_dir:     getDir(),      sc_tf:       getScanTF(),
      sc_chartTF: chartTF,      sc_strategy: $('strat')?.value,
      sc_order:   $('order')?.value, sc_zlb:  $('zlb')?.value,
      sc_legout:  $('legout')?.value,
      sc_moEn:    $('moen')?.checked,
      sc_moh:     $('moh')?.value, sc_mom: $('mom')?.value, sc_mol: $('mol')?.value,
      sc_align:   $('align-filter')?.value || 'any',
      sc_atrRbr:  $('atr-rbr')?.checked, sc_atrDbr: $('atr-dbr')?.checked,
      sc_atrDbd:  $('atr-dbd')?.checked, sc_atrRbd: $('atr-rbd')?.checked,
      sc_sbCol:   $('sidebar')?.classList.contains('col'),
      sc_trends:  trendVals,
      sc_sf_low:  [..._selected.low],
      sc_sf_mid:  [..._selected.mid],
      sc_sf_high: [..._selected.high],
    });
  }

  function _restoreUIState() {
    const s = QS.loadState();

    // ── Restore asset class first — everything else depends on it ──────────
    if (s.sc_asset && s.sc_asset !== assetClass) {
      const btn = document.querySelector(`#asset-seg [data-v="${s.sc_asset}"]`);
      if (btn) setAsset(btn);
    }

    if (s.sc_dir) {
      $$('#dir-seg .seg-btn').forEach(b => {
        b.className = 'seg-btn';
        if (b.dataset.v === s.sc_dir) b.className = 'seg-btn ' + s.sc_dir;
      });
    }
    if (s.sc_tf) $$('#tfc-row .tfc').forEach(b => b.classList.toggle('on', b.dataset.v === s.sc_tf));
    if (s.sc_chartTF) {
      chartTF = s.sc_chartTF;
      $$('.tfg .tfb').forEach(b => b.classList.toggle('on', b.dataset.tf === chartTF));
    }
    if (s.sc_strategy && $('strat')) { $('strat').value = s.sc_strategy; onStrategyChange(); }
    if (s.sc_order  && $('order'))  $('order').value  = s.sc_order;
    if (s.sc_zlb    && $('zlb'))    $('zlb').value    = s.sc_zlb;
    if (s.sc_legout && $('legout')) $('legout').value = s.sc_legout;
    ['rbr','dbr','dbd','rbd'].forEach(t => {
      if ($(`atr-${t}`) && s[`sc_atr${t.charAt(0).toUpperCase()+t.slice(1)}`] !== undefined)
        $(`atr-${t}`).checked = s[`sc_atr${t.charAt(0).toUpperCase()+t.slice(1)}`];
    });
    if (s.sc_moh && $('moh')) $('moh').value = s.sc_moh;
    if (s.sc_mom && $('mom')) $('mom').value = s.sc_mom;
    if (s.sc_mol && $('mol')) $('mol').value = s.sc_mol;
    if (s.sc_moEn !== undefined && $('moen')) { $('moen').checked = s.sc_moEn; toggleMO(s.sc_moEn); }
    if (s.sc_align && $('align-filter')) $('align-filter').value = s.sc_align;
    if (s.sc_sbCol) $('sidebar')?.classList.add('col');
    if (Array.isArray(s.sc_trends) && s.sc_trends.length) {
      $$('.tc').forEach(cb => {
        const e = s.sc_trends.find(t => t.v === cb.value);
        if (e) cb.checked = e.chk;
      });
      _updateTrendLabel();
    }
    if (Array.isArray(s.sc_sf_low))  { _F.selected.low  = new Set(s.sc_sf_low);  }
    if (Array.isArray(s.sc_sf_mid))  { _F.selected.mid  = new Set(s.sc_sf_mid);  }
    if (Array.isArray(s.sc_sf_high)) { _F.selected.high = new Set(s.sc_sf_high); }

    // Re-render dropdowns with restored selections
    _renderStructureFilters();
    _updatePivotButtons();

    // ── Restore scan rows from localStorage — survives page navigation ──────
    if (Array.isArray(s.sc_rows) && s.sc_rows.length) {
      rows = s.sc_rows;
      $('rcnt').textContent = rows.length;
      QS.setNavStatus(`${rows.length} found`, `${rows.length} setups`);
      _buildList();
      const savedIdx = s.sc_activeIdx ?? -1;
      if (savedIdx >= 0) setTimeout(() => selStock(savedIdx), 500);
    }
  }

  /* ════════════════════════════════════════════════════════════
     INIT
  ════════════════════════════════════════════════════════════ */
  async function init() {
    QS.renderNav('scanner', 'nst', 'nbadge');
    InfoDrawer.attach(document.getElementById('cp'));

    await Filters.loadStateGroups();
    _renderStructureFilters();
    _F.installDocClickHandler();

    // _restoreUIState handles asset class restoration from localStorage —
    // no need for the old sessionStorage read here
    _restoreUIState();

    // Sync single-order row visibility to actual MO state after restore
    const row = $('single-order-row');
    if (row) row.style.display = $('moen')?.checked ? 'none' : 'block';

    // Global click handler — closes trend dropdown only
    // (structure dropdowns are handled by Filters module)
    document.addEventListener('click', e => {
      if (!e.target.closest('#tdd') && !e.target.closest('#ttrig')) {
        $('tdd')?.classList.remove('op');
        $('ttrig')?.classList.remove('op');
      }
    });

    $('tdd').addEventListener('click', e => e.stopPropagation());

    $$('.tc').forEach(cb =>
      cb.addEventListener('change', () => { _updateTrendLabel(); _saveUIState(); })
    );
    if ($('strat')) $('strat').addEventListener('change', onStrategyChange);
    ['atr-rbr','atr-dbr','atr-dbd','atr-rbd'].forEach(id => {
      $(`${id}`)?.addEventListener('change', _saveUIState);
    });
    ['order','moh','mom','mol'].forEach(id => {
      const el = $(id); if (!el) return;
      el.addEventListener('input',  () => { _updatePivotButtons(); _saveUIState(); });
      el.addEventListener('change', () => { _updatePivotButtons(); _saveUIState(); });
    });
    ['strat','zlb','legout','align-filter'].forEach(id => {
      $(id)?.addEventListener('change', _saveUIState);
    });

    window.addEventListener('beforeunload', _saveUIState);
  }

  /* ── Public surface ─────────────────────────────────────── */
  return {
    init, runScan, selStock, sortBy, setChartTF,
    toggleSB, stp, setDir, setTFC, setAsset,
    toggleTDD, allTrends, toggleMO, onStrategyChange,
    openDrawer, closeDrawer,
    // Pivot drill-down — called from drawer onclick
    _focusPivotLevel, _resetPivotFocus,
    // Structure dropdown handlers — exposed for inline HTML onchange/onclick
    __sfToggle, __sfAllToggle, __sfChange,
    // Exposed for align-filter onchange in HTML
    _saveUIState,
  };
})();

window.Scanner = Scanner;
document.addEventListener('DOMContentLoaded', Scanner.init);
