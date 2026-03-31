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

  const SS_ROWS_KEY = 'qs_scanner_rows_v1';
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
    'trending_up','breakout','coiling_to_up','expanding_to_up',
    'bottoming','bottom_breaking','structure_up',
  ]);
  const STRUCT_BEAR = new Set([
    'trending_down','breakdown','coiling_to_down','expanding_to_down',
    'topping','top_breaking','structure_down',
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
  let _stateGroups = {};
  let _stateAll    = [];
  const _selected  = { low: new Set(), mid: new Set(), high: new Set() };

  // Track the currently open structure dropdown wrapper element
  let _openDdWrapper = null;

  async function _loadStateGroups() {
    try {
      const d      = await fetch('/api/structure_states').then(r => r.json());
      _stateGroups = d.groups || {};
      _stateAll    = d.all    || [];
    } catch (_) {
      _stateGroups = {
        'Bullish – Continuation': [
          {value:'trending_up',       label:'Trending Up'},
        ],
        'Bullish – Reversal': [
          {value:'breakout',          label:'Breakout'},
          {value:'bottom_breaking',   label:'Bottom Breaking'},
          {value:'bottoming',         label:'Bottoming'},
        ],
        'Bullish – Setup': [
          {value:'coiling_to_up',     label:'Coiling → Up'},
          {value:'expanding_to_up',   label:'Expanding → Up'},
          {value:'structure_up',      label:'Structure Up'},
        ],
        'Bearish – Continuation': [
          {value:'trending_down',     label:'Trending Down'},
        ],
        'Bearish – Reversal': [
          {value:'breakdown',         label:'Breakdown'},
          {value:'top_breaking',      label:'Top Breaking'},
          {value:'topping',           label:'Topping'},
        ],
        'Bearish – Setup': [
          {value:'coiling_to_down',   label:'Coiling → Down'},
          {value:'expanding_to_down', label:'Expanding → Down'},
          {value:'structure_down',    label:'Structure Down'},
        ],
        'Neutral': [
          {value:'structure_expanding', label:'Expanding'},
          {value:'structure_coiling',   label:'Coiling'},
        ],
      };
      _stateAll = Object.values(_stateGroups).flat();
    }
  }

  /* Build ONE dropdown for a given level.
     Key: unique IDs use level + a random suffix to guarantee
     no ID collision when both single and multi wrappers exist.  */
  function _buildStructureDropdown(level) {
    const sel    = _selected[level];
    const allSel = sel.size === 0;
    const lbl    = allSel ? 'Any state' : `${sel.size} selected`;
    // Unique wrapper ID — used to track _openDdWrapper
    const wrId   = `sfwrap-${level}`;
    const ddId   = `sfdd-${level}`;
    const trigId = `sftrig-${level}`;
    const lblId  = `sflbl-${level}`;
    const allId  = `sfall-${level}`;

    let optHtml = `
      <div style="padding:7px 12px;border-bottom:1px solid var(--border)">
        <label style="display:flex;align-items:center;gap:8px;cursor:pointer;
               font-size:12px;color:var(--accent);font-family:var(--mono)">
          <input type="checkbox" id="${allId}" ${allSel ? 'checked' : ''}
                 onchange="Scanner.__sfAllToggle('${level}',this.checked)">
          All states
        </label>
      </div>`;

    for (const [grpName, states] of Object.entries(_stateGroups)) {
      optHtml += `<div class="tdg">${grpName}</div>`;
      for (const {value, label} of states) {
        const chk = (sel.size === 0 || sel.has(value)) ? 'checked' : '';
        optHtml += `
          <div class="tdo">
            <input type="checkbox" class="sfc-${level}" value="${value}" ${chk}
                   onchange="Scanner.__sfChange('${level}')">
            <span style="color:${structColor(value)}">${label}</span>
          </div>`;
      }
    }

    // CRITICAL: onclick on the wrapper div stops bubbling so clicks
    // inside never reach the document handler that closes all dropdowns.
    return `
      <div class="tdw" id="${wrId}" onclick="event.stopPropagation()">
        <button class="trig" id="${trigId}"
                onclick="Scanner.__sfToggle(event,'${level}','${wrId}')">
          <span id="${lblId}">${lbl}</span>
          <span class="tarr">▼</span>
        </button>
        <div class="tdd" id="${ddId}" style="max-height:260px;overflow-y:auto">
          ${optHtml}
        </div>
      </div>`;
  }

  function _renderStructureFilters() {
    const moOn  = $('moen') && $('moen').checked;
    const sw    = $('sf-single-wrap');
    const mw    = $('sf-multi-wrap');
    if (!sw || !mw) return;

    // Always clear both wrappers to remove duplicate IDs from DOM
    sw.innerHTML = '';
    mw.innerHTML = '';

    if (!moOn) {
      sw.innerHTML     = _buildStructureDropdown('low');
      sw.style.display = 'block';
      mw.style.display = 'none';
    } else {
      mw.innerHTML = `
        <div style="display:grid;grid-template-columns:36px 1fr;align-items:start;
                    gap:6px;margin-bottom:8px">
          <span class="molk moH" style="font-size:9px;padding-top:8px">HIGH</span>
          <div>${_buildStructureDropdown('high')}</div>
        </div>
        <div style="display:grid;grid-template-columns:36px 1fr;align-items:start;
                    gap:6px;margin-bottom:8px">
          <span class="molk moM" style="font-size:9px;padding-top:8px">MID</span>
          <div>${_buildStructureDropdown('mid')}</div>
        </div>
        <div style="display:grid;grid-template-columns:36px 1fr;align-items:start;
                    gap:6px">
          <span class="molk moL" style="font-size:9px;padding-top:8px">LOW</span>
          <div>${_buildStructureDropdown('low')}</div>
        </div>`;
      mw.style.display = 'block';
      sw.style.display = 'none';
    }
    // Reset open tracking after re-render
    _openDdWrapper = null;
  }

  /* Toggle a structure dropdown open/closed.
     wrapperId identifies the specific wrapper so we can track
     which one is open without relying on getElementById finding
     the wrong element when IDs are duplicated.               */
  function __sfToggle(e, level, wrapperId) {
    e.stopPropagation();
    const dd  = $(`sfdd-${level}`);
    const trg = $(`sftrig-${level}`);
    if (!dd) return;

    const isOpen = dd.classList.contains('op');

    // Close everything first
    $$('.tdd').forEach(d => d.classList.remove('op'));
    $$('.trig').forEach(t => t.classList.remove('op'));
    _openDdWrapper = null;

    if (!isOpen) {
      dd.classList.add('op');
      trg && trg.classList.add('op');
      _openDdWrapper = $(wrapperId);
    }
  }

  function __sfAllToggle(level, checked) {
    $$(`.sfc-${level}`).forEach(cb => { cb.checked = checked; });
    __sfChange(level);
  }

  function __sfChange(level) {
    const sel    = _selected[level];
    sel.clear();
    const allCbs = [...$$(`.sfc-${level}`)];
    const chkCbs = allCbs.filter(c => c.checked);
    const allSel = chkCbs.length === allCbs.length;

    if (!allSel) chkCbs.forEach(c => sel.add(c.value));

    const lbl = $(`sflbl-${level}`);
    if (lbl) lbl.textContent = allSel ? 'Any state' : `${sel.size} selected`;

    const allCb = $(`sfall-${level}`);
    if (allCb) {
      allCb.checked       = allSel;
      allCb.indeterminate = !allSel && chkCbs.length > 0;
    }
    _saveUIState();
    // DO NOT close the dropdown — user may want to select more
  }

  function _getStructureParam(level) {
    const sel = _selected[level];
    return sel.size > 0 ? [...sel].join(',') : '';
  }

  /* ════════════════════════════════════════════════════════════
     SIDEBAR
  ════════════════════════════════════════════════════════════ */
  function toggleSB() {
    $('sidebar').classList.toggle('col');
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
  function getDir() {
    const a = $('dir-seg').querySelector('.buy,.sell');
    return a?.dataset.v || 'buy';
  }

  function setTFC(btn) {
    $$('#tfc-row .tfc').forEach(b => b.classList.remove('on'));
    btn.classList.add('on');
    _saveUIState();
  }
  function getScanTF() {
    return $('tfc-row').querySelector('.tfc.on')?.dataset.v || '1d';
  }

  function onStrategyChange() {
    const val = $('strat').value;
    $('atr-sub-row').style.display       = (val === 'atr' || val === 'both') ? 'flex' : 'none';
    $('trend-only-notice').style.display = val === 'trend_only' ? 'block' : 'none';
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
    container.innerHTML = '';
    Chart.setActiveLevels(moEnabled ? ['H','M','L'] : ['single']);

    if (!moEnabled) {
      const btn = document.createElement('button');
      btn.className = 'pob on'; btn.dataset.level = 'single';
      btn.innerHTML = `<span class="pob-dot" style="background:#3b82f6"></span>Order ${$('order').value}`;
      btn.onclick = () => _onPivotToggle('single', btn);
      container.appendChild(btn);
    } else {
      const orders = { H: $('moh').value||20, M: $('mom').value||10, L: $('mol').value||5 };
      ['H','M','L'].forEach(lv => {
        const mc  = Chart.moColors[lv];
        const btn = document.createElement('button');
        btn.className = `pob on ${mc.cls}`; btn.dataset.level = lv;
        btn.innerHTML = `<span class="pob-dot" style="background:${mc.dot}"></span>${mc.label}(${orders[lv]})`;
        btn.onclick   = () => _onPivotToggle(lv, btn);
        container.appendChild(btn);
      });
    }
    _updatePivotLegend();
  }

  function _onPivotToggle(level, btn) {
    const newLevels = Chart.toggleLevel(level);
    $$('#pivot-order-btns .pob').forEach(b =>
      b.classList.toggle('on', newLevels.includes(b.dataset.level))
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
    const btn = $('scanbtn');
    btn.disabled = true; btn.textContent = '⏳  Scanning…';
    QS.setNavStatus('scanning…');
    $('rlist').innerHTML = Array(6).fill(0).map(() =>
      `<div class="shimrow"><div class="sh" style="height:11px;width:40%"></div>`+
      `<div class="sh" style="height:9px;width:60%;margin-top:4px"></div></div>`
    ).join('');
    $('rcnt').textContent = '…';
    activeIdx = -1;

    try {
      const mo       = $('moen').checked;
      const strategy = $('strat').value;
      const atrTypes = (strategy === 'atr' || strategy === 'both') ? getAtrZoneTypes() : [];
      const tr       = getSelTrends();

      const params = {
        direction:     getDir(),     interval:      getScanTF(),
        order:         $('order').value, zone_lookback: $('zlb').value,
        legout_mult:   $('legout').value, strategy,
        atr_zone_types: atrTypes.join(','),
        multi_order:   mo,
        order_low:     $('mol').value, order_mid: $('mom').value,
        order_high:    $('moh').value,
        structure_low:  _getStructureParam('low'),
        structure_mid:  mo ? _getStructureParam('mid')  : '',
        structure_high: mo ? _getStructureParam('high') : '',
        alignment_filter: $('align-filter')?.value || 'any',
      };
      if (tr) params.trend_filter = tr;

      const d = await API.scan(params);
      rows = d.results || [];
      $('rcnt').textContent = rows.length;
      QS.setNavStatus(`${rows.length} found`, `${rows.length} setups`);
      _buildList();
      _saveUIState();
      try { sessionStorage.setItem(SS_ROWS_KEY, JSON.stringify({ rows, activeIdx })); } catch(_){}
    } catch (e) {
      $('rlist').innerHTML = `<div class="empty-st"><span style="color:var(--red)">⚠ ${e.message}</span></div>`;
      QS.setNavStatus('error');
    } finally {
      btn.disabled = false; btn.textContent = '▶  Run Scan';
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
        <button class="ibtn" onclick="Scanner.openDrawer(event,${i})" title="Details">ⓘ</button>
      </div>`;
    }).join('');
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

    // Header
    $('cptk').textContent = row.ticker.replace('.NS', '');
    $('cppr').textContent = QS.inr(row.close);
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
    await Chart.load(
      row.ticker,
      _buildChartParams(row),
      row.direction || getDir(),
      _getZoneLegInTs(row),
    );

    API.logEvent('info', 'scanner', 'Chart loaded', { ticker: row.ticker, tf: chartTF });
    try { sessionStorage.setItem(SS_ROWS_KEY, JSON.stringify({ rows, activeIdx })); } catch (_) {}
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
     DETAIL DRAWER
  ════════════════════════════════════════════════════════════ */
  function openDrawer(e, i) {
    e.stopPropagation();
    const r = rows[i]; if (!r) return;
    $('dwtk').textContent = r.ticker.replace('.NS','');
    $('dwsb').textContent = `${QS.trendLabel(r.trend)} · ${r.pivot_count} pivots · ATR ${r.atr_pct}%`;
    $('dwpr').textContent = QS.inr(r.close);
    $('dwtr').textContent = QS.trendLabel(r.trend);
    $('dwtr').className   = 'tbadge ' + QS.trendClass(r.trend);
    $('dws').innerHTML    = _buildDrawerHTML(r);
    $('dw').classList.add('open');
    $('dov').classList.add('show');
  }
  function closeDrawer() {
    $('dw').classList.remove('open');
    $('dov').classList.remove('show');
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
    const levels = [
      { lbl:'HIGH', order: r.mo_order_high, state: r.struct_high, pair: r.struct_high_pair, cls:'moH' },
      { lbl:'MID',  order: r.mo_order_mid,  state: r.struct_mid,  pair: r.struct_mid_pair,  cls:'moM' },
      { lbl:'LOW',  order: r.mo_order_low,  state: r.struct_low,  pair: r.struct_low_pair,  cls:'moL' },
    ];
    const aln      = r.struct_alignment || '—';
    const bull     = r.struct_bull_count ?? '—';
    const bear     = r.struct_bear_count ?? '—';
    const alnColor = aln==='bullish'?'#22c55e' : aln==='bearish'?'#ef4444' : '#f59e0b';

    const structHtml = `
    <div>
      <div class="dwst">Pivot Structure
        <span style="font-size:9px;font-weight:400;text-transform:none;letter-spacing:0;
              color:${alnColor};margin-left:4px">
          ${aln} · 🟢${bull} 🔴${bear}
        </span>
      </div>
      ${levels.map(lv => {
        const col  = structColor(lv.state);
        const slbl = _structLabel(lv.state);
        return `<div style="display:grid;grid-template-columns:70px 1fr auto;
                     align-items:center;gap:8px;margin-bottom:7px">
          <span class="molk ${lv.cls}" style="font-size:9px">${lv.lbl}(${lv.order||'?'})</span>
          <div>
            <div style="font-size:11px;font-weight:600;color:${col}">${slbl}</div>
            <div style="font-size:9px;color:var(--muted2);margin-top:1px">${lv.pair||'—'}</div>
          </div>
          <div style="width:6px;height:6px;border-radius:50%;background:${col};flex-shrink:0"></div>
        </div>`;
      }).join('')}
    </div>`;

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

    return `${structHtml}${zoneHtml}
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
  }

  /* ════════════════════════════════════════════════════════════
     PERSISTENT STATE
  ════════════════════════════════════════════════════════════ */
  function _saveUIState() {
    const trendVals = [...$$('.tc')].map(c => ({ v: c.value, chk: c.checked }));
    QS.saveState({
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
    if (s.sc_moEn !== undefined && $('moen')) { $('moen').checked = s.sc_moEn; toggleMO(s.sc_moEn); }
    if (s.sc_moh && $('moh')) $('moh').value = s.sc_moh;
    if (s.sc_mom && $('mom')) $('mom').value = s.sc_mom;
    if (s.sc_mol && $('mol')) $('mol').value = s.sc_mol;
    if (s.sc_align && $('align-filter')) $('align-filter').value = s.sc_align;
    if (s.sc_sbCol) $('sidebar').classList.add('col');
    if (Array.isArray(s.sc_trends) && s.sc_trends.length) {
      $$('.tc').forEach(cb => {
        const e = s.sc_trends.find(t => t.v === cb.value);
        if (e) cb.checked = e.chk;
      });
      _updateTrendLabel();
    }
    if (Array.isArray(s.sc_sf_low))  _selected.low  = new Set(s.sc_sf_low);
    if (Array.isArray(s.sc_sf_mid))  _selected.mid  = new Set(s.sc_sf_mid);
    if (Array.isArray(s.sc_sf_high)) _selected.high = new Set(s.sc_sf_high);

    // Re-render dropdowns with restored selections
    _renderStructureFilters();
    _updatePivotButtons();

    // Restore rows
    try {
      const saved = JSON.parse(sessionStorage.getItem(SS_ROWS_KEY) || 'null');
      if (saved?.rows?.length) {
        rows = saved.rows;
        $('rcnt').textContent = rows.length;
        QS.setNavStatus(`${rows.length} found`, `${rows.length} setups`);
        _buildList();
        if (saved.activeIdx >= 0) setTimeout(() => selStock(saved.activeIdx), 500);
      }
    } catch(_) {}
  }

  /* ════════════════════════════════════════════════════════════
     INIT
  ════════════════════════════════════════════════════════════ */
  async function init() {
    QS.renderNav('scanner', 'nst', 'nbadge');

    await _loadStateGroups();
    _renderStructureFilters();

    // Global click handler — closes structure dropdowns ONLY if click
    // was outside the currently open wrapper.
    document.addEventListener('click', (e) => {
      // Close legacy trend dropdown
      $('tdd').classList.remove('op');
      $('ttrig').classList.remove('op');

      // Close structure dropdowns only if click outside open wrapper
      if (_openDdWrapper && !_openDdWrapper.contains(e.target)) {
        $$('.tdd').forEach(d => d.classList.remove('op'));
        $$('.trig').forEach(t => t.classList.remove('op'));
        _openDdWrapper = null;
      }
    });

    // Legacy trend dropdown — stop internal clicks propagating
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

    _restoreUIState();
    window.addEventListener('beforeunload', _saveUIState);
  }

  /* ── Public surface ─────────────────────────────────────── */
  return {
    init, runScan, selStock, sortBy, setChartTF,
    toggleSB, stp, setDir, setTFC,
    toggleTDD, allTrends, toggleMO, onStrategyChange,
    openDrawer, closeDrawer,
    // Structure dropdown handlers — exposed for inline HTML onchange/onclick
    __sfToggle, __sfAllToggle, __sfChange,
    // Exposed for align-filter onchange in HTML
    _saveUIState,
  };
})();

window.Scanner = Scanner;
document.addEventListener('DOMContentLoaded', Scanner.init);
