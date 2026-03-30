/* ═══════════════════════════════════════════════════════════════
   scanner.js  v3.3
   ─────────────────────────────────────────────────────────────
   Changes vs v3.2
   • Structure state multi-selects replace trend_low/mid/high.
   • State groups loaded from /api/structure_states on init.
   • Single-order mode shows one structure filter (LOW only).
   • Multi-order mode shows three filters (LOW / MID / HIGH)
     plus an alignment selector.
   • All state is persisted to localStorage via QS.saveState.
   Depends on: shared.js, api.js, chart.js
═══════════════════════════════════════════════════════════════ */

const Scanner = (() => {

  /* ── State ──────────────────────────────────────────────── */
  let rows       = [];
  let activeIdx  = -1;
  let chartTF    = '1d';
  let currentRow = null;

  const SS_ROWS_KEY = 'qs_scanner_rows_v1';

  const $ = id => document.getElementById(id);

  /* ── Zone type colors ───────────────────────────────────── */
  const ZT = {
    rbr:           { bg:'rgba(34,197,94,.1)',   c:'#22c55e', b:'rgba(34,197,94,.3)'  },
    dbr:           { bg:'rgba(45,212,191,.1)',  c:'#2dd4bf', b:'rgba(45,212,191,.3)' },
    dbd:           { bg:'rgba(239,68,68,.1)',   c:'#ef4444', b:'rgba(239,68,68,.3)'  },
    rbd:           { bg:'rgba(249,115,22,.1)',  c:'#f97316', b:'rgba(249,115,22,.3)' },
    consolidation: { bg:'rgba(167,139,250,.1)', c:'#a78bfa', b:'rgba(167,139,250,.3)'},
  };

  /* ── Structure state colors ─────────────────────────────── */
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
  function structBg(s) {
    if (STRUCT_BULL.has(s)) return 'rgba(34,197,94,.12)';
    if (STRUCT_BEAR.has(s)) return 'rgba(239,68,68,.12)';
    return 'rgba(245,158,11,.12)';
  }

  /* ════════════════════════════════════════════════════════════
     STRUCTURE STATE DROPDOWNS
     Loaded dynamically from /api/structure_states on init.
     Three levels: low / mid / high
  ════════════════════════════════════════════════════════════ */
  let _stateGroups = {};   // {groupName: [{value,label}, ...]}
  let _stateAll    = [];   // [{value, label}]

  // Selected sets per level — empty = "any"
  const _selected = { low: new Set(), mid: new Set(), high: new Set() };

  async function _loadStateGroups() {
    try {
      const d = await fetch('/api/structure_states').then(r => r.json());
      _stateGroups = d.groups || {};
      _stateAll    = d.all    || [];
    } catch (_) {
      // Fallback: hardcode the groups if API unreachable
      _stateGroups = {
        "Bullish – Continuation": [
          {value:"trending_up",    label:"Trending Up"},
        ],
        "Bullish – Reversal": [
          {value:"breakout",         label:"Breakout"},
          {value:"bottom_breaking",  label:"Bottom Breaking"},
          {value:"bottoming",        label:"Bottoming"},
        ],
        "Bullish – Setup": [
          {value:"coiling_to_up",    label:"Coiling → Up"},
          {value:"expanding_to_up",  label:"Expanding → Up"},
          {value:"structure_up",     label:"Structure Up"},
        ],
        "Bearish – Continuation": [
          {value:"trending_down",    label:"Trending Down"},
        ],
        "Bearish – Reversal": [
          {value:"breakdown",        label:"Breakdown"},
          {value:"top_breaking",     label:"Top Breaking"},
          {value:"topping",          label:"Topping"},
        ],
        "Bearish – Setup": [
          {value:"coiling_to_down",  label:"Coiling → Down"},
          {value:"expanding_to_down",label:"Expanding → Down"},
          {value:"structure_down",   label:"Structure Down"},
        ],
        "Neutral": [
          {value:"structure_expanding",label:"Expanding"},
          {value:"structure_coiling",  label:"Coiling"},
        ],
      };
      _stateAll = Object.values(_stateGroups).flat();
    }
  }

  function _buildStructureDropdown(level) {
    /* Build the HTML for one structure filter dropdown.
       level: "low" | "mid" | "high"
       Returns the outer container HTML string.            */
    const sel    = _selected[level];
    const allSel = sel.size === 0;
    const label  = allSel ? 'Any state' : `${sel.size} selected`;

    let optHtml = `
      <div style="padding:7px 12px;border-bottom:1px solid var(--border)">
        <label style="display:flex;align-items:center;gap:8px;cursor:pointer;
               font-size:12px;color:var(--accent);font-family:var(--mono)">
          <input type="checkbox" id="sfall-${level}"
                 ${allSel ? 'checked' : ''}
                 onchange="Scanner._sfAllToggle('${level}',this.checked)">
          All states
        </label>
      </div>`;

    for (const [grpName, states] of Object.entries(_stateGroups)) {
      optHtml += `<div class="tdg">${grpName}</div>`;
      for (const {value, label: lbl} of states) {
        const chk = sel.size === 0 || sel.has(value) ? 'checked' : '';
        optHtml += `
          <div class="tdo">
            <input type="checkbox" class="sfc-${level}" value="${value}" ${chk}
                   onchange="Scanner._sfChange('${level}')">
            <span style="color:${structColor(value)}">${lbl}</span>
          </div>`;
      }
    }

    return `
      <div class="tdw">
        <button class="trig" id="sftrig-${level}"
                onclick="Scanner._sfToggle(event,'${level}')">
          <span id="sflbl-${level}">${label}</span>
          <span class="tarr">▼</span>
        </button>
        <div class="tdd" id="sfdd-${level}">${optHtml}</div>
      </div>`;
  }

  function _renderStructureFilters() {
    const moOn   = $('moen') && $('moen').checked;
    const single = $('sf-single-wrap');
    const multi  = $('sf-multi-wrap');
    if (!single || !multi) return;

    if (!moOn) {
      single.innerHTML = _buildStructureDropdown('low');
      single.style.display = 'block';
      multi.style.display  = 'none';
    } else {
      multi.innerHTML = `
        <div style="display:grid;grid-template-columns:40px 1fr;
                    align-items:center;gap:6px;margin-bottom:6px">
          <span class="molk moH" style="font-size:9px">HIGH</span>
          ${_buildStructureDropdown('high')}
        </div>
        <div style="display:grid;grid-template-columns:40px 1fr;
                    align-items:center;gap:6px;margin-bottom:6px">
          <span class="molk moM" style="font-size:9px">MID</span>
          ${_buildStructureDropdown('mid')}
        </div>
        <div style="display:grid;grid-template-columns:40px 1fr;
                    align-items:center;gap:6px">
          <span class="molk moL" style="font-size:9px">LOW</span>
          ${_buildStructureDropdown('low')}
        </div>`;
      multi.style.display  = 'block';
      single.style.display = 'none';
    }
  }

  /* Toggle one structure dropdown open/closed */
  function _sfToggle(e, level) {
    e.stopPropagation();
    const dd   = $(`sfdd-${level}`);
    const trig = $(`sftrig-${level}`);
    if (!dd) return;
    const isOpen = dd.classList.contains('op');
    // Close all other dropdowns first
    document.querySelectorAll('.tdd').forEach(d => d.classList.remove('op'));
    document.querySelectorAll('.trig').forEach(t => t.classList.remove('op'));
    if (!isOpen) { dd.classList.add('op'); trig && trig.classList.add('op'); }
  }

  /* "All states" master checkbox toggled */
  function _sfAllToggle(level, checked) {
    document.querySelectorAll(`.sfc-${level}`).forEach(cb => {
      cb.checked = checked;
    });
    _sfChange(level);
  }

  /* Any individual state checkbox changed */
  function _sfChange(level) {
    const sel = _selected[level];
    sel.clear();
    const allCbs = [...document.querySelectorAll(`.sfc-${level}`)];
    const chkCbs = allCbs.filter(c => c.checked);
    const allSel = chkCbs.length === allCbs.length;

    if (!allSel) {
      chkCbs.forEach(c => sel.add(c.value));
    }
    // Update label
    const lbl = $(`sflbl-${level}`);
    if (lbl) lbl.textContent = allSel ? 'Any state' : `${sel.size} selected`;
    // Update "all" checkbox state
    const allCb = $(`sfall-${level}`);
    if (allCb) {
      allCb.checked       = allSel;
      allCb.indeterminate = !allSel && chkCbs.length > 0;
    }
    _saveUIState();
  }

  function _getStructureParam(level) {
    const sel = _selected[level];
    return sel.size > 0 ? [...sel].join(',') : '';
  }

  /* ════════════════════════════════════════════════════════════
     SIDEBAR TOGGLE
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
    $('dir-seg').querySelectorAll('.seg-btn').forEach(b => b.className = 'seg-btn');
    btn.className = 'seg-btn ' + (btn.dataset.v === 'buy' ? 'buy' : 'sell');
    _saveUIState();
  }
  function getDir() {
    const a = $('dir-seg').querySelector('.buy,.sell');
    return a?.dataset.v || 'buy';
  }

  function setTFC(btn) {
    $('tfc-row').querySelectorAll('.tfc').forEach(b => b.classList.remove('on'));
    btn.classList.add('on');
    _saveUIState();
  }
  function getScanTF() {
    return $('tfc-row').querySelector('.tfc.on')?.dataset.v || '1d';
  }

  /* Strategy + ATR sub-row */
  function onStrategyChange() {
    const val = $('strat').value;
    $('atr-sub-row').style.display      = (val === 'atr' || val === 'both') ? 'flex' : 'none';
    $('trend-only-notice').style.display = val === 'trend_only' ? 'block' : 'none';
    _saveUIState();
  }
  function getAtrZoneTypes() {
    const types = [];
    if ($('atr-rbr') && $('atr-rbr').checked) types.push('rbr');
    if ($('atr-dbr') && $('atr-dbr').checked) types.push('dbr');
    if ($('atr-dbd') && $('atr-dbd').checked) types.push('dbd');
    if ($('atr-rbd') && $('atr-rbd').checked) types.push('rbd');
    return types.length ? types : ['rbr','dbr'];
  }

  /* Trend label dropdown (legacy — kept for optional pre-filter) */
  function toggleTDD(e) {
    e.stopPropagation();
    $('tdd').classList.toggle('op');
    $('ttrig').classList.toggle('op');
  }
  function allTrends(cb) {
    document.querySelectorAll('.tc').forEach(c => c.checked = cb.checked);
    _updateTrendLabel();
    _saveUIState();
  }
  function _updateTrendLabel() {
    const all = document.querySelectorAll('.tc');
    const chk = [...all].filter(c => c.checked);
    $('all-t').checked       = chk.length === all.length;
    $('all-t').indeterminate = chk.length > 0 && chk.length < all.length;
    $('tlbl').textContent    = chk.length === all.length ? 'All trends'
      : chk.length === 0 ? 'No trends' : `${chk.length} selected`;
  }
  function getSelTrends() {
    const all = [...document.querySelectorAll('.tc')];
    const chk = all.filter(x => x.checked).map(x => x.value);
    return chk.length === all.length ? '' : chk.join(',');
  }

  /* MO toggle */
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
      btn.innerHTML =
        `<span class="pob-dot" style="background:#3b82f6"></span>Order ${$('order').value}`;
      btn.onclick = () => _onPivotToggle('single', btn);
      container.appendChild(btn);
    } else {
      const orders = {
        H: parseInt($('moh').value)||20,
        M: parseInt($('mom').value)||10,
        L: parseInt($('mol').value)||5,
      };
      ['H','M','L'].forEach(lv => {
        const mc  = Chart.moColors[lv];
        const btn = document.createElement('button');
        btn.className = `pob on ${mc.cls}`; btn.dataset.level = lv;
        btn.innerHTML =
          `<span class="pob-dot" style="background:${mc.dot}"></span>`+
          `${mc.label}(${orders[lv]})`;
        btn.onclick = () => _onPivotToggle(lv, btn);
        container.appendChild(btn);
      });
    }
    _updatePivotLegend();
  }

  function _onPivotToggle(level, btn) {
    const newLevels = Chart.toggleLevel(level);
    document.querySelectorAll('#pivot-order-btns .pob').forEach(b =>
      b.classList.toggle('on', newLevels.includes(b.dataset.level))
    );
    _updatePivotLegend();
  }

  function _updatePivotLegend() {
    const moEnabled = $('moen').checked;
    const leg = $('pivot-order-legend');
    if (!leg) return;
    if (!moEnabled) { leg.textContent = `order=${$('order').value}`; return; }
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
      const atrTypes = (strategy === 'atr' || strategy === 'both')
        ? getAtrZoneTypes() : [];
      const tr = getSelTrends();

      const params = {
        direction:    getDir(),
        interval:     getScanTF(),
        order:        $('order').value,
        zone_lookback:$('zlb').value,
        legout_mult:  $('legout').value,
        strategy,
        atr_zone_types: atrTypes.join(','),
        multi_order:  mo,
        order_low:    $('mol').value,
        order_mid:    $('mom').value,
        order_high:   $('moh').value,
        // Structure state filters
        structure_low:  _getStructureParam('low'),
        structure_mid:  mo ? _getStructureParam('mid')  : '',
        structure_high: mo ? _getStructureParam('high') : '',
        alignment_filter: $('align-filter') ? $('align-filter').value : 'any',
      };
      if (tr) params.trend_filter = tr;

      const d = await API.scan(params);
      rows = d.results || [];
      $('rcnt').textContent = rows.length;
      QS.setNavStatus(`${rows.length} found`, `${rows.length} setups`);
      _buildList();
      _saveUIState();
      try {
        sessionStorage.setItem(SS_ROWS_KEY, JSON.stringify({ rows, activeIdx }));
      } catch(_) {}
    } catch (e) {
      $('rlist').innerHTML =
        `<div class="empty-st"><span style="color:var(--red)">⚠ ${e.message}</span></div>`;
      QS.setNavStatus('error');
    } finally {
      btn.disabled = false; btn.textContent = '▶  Run Scan';
    }
  }

  /* ════════════════════════════════════════════════════════════
     RESULTS LIST
  ════════════════════════════════════════════════════════════ */
  function _buildList() {
    const list = $('rlist');
    if (!rows.length) {
      list.innerHTML = `<div class="empty-st"><div class="e-ico">◈</div>`+
        `<div class="e-txt">No results found</div>`+
        `<div class="e-sub">Broaden structure filter or increase lookback</div></div>`;
      return;
    }
    list.innerHTML = rows.map((r, i) => {
      const z   = ZT[r.zone_type] || ZT.rbr;
      const ac  = activeIdx === i ? ' on' : '';
      const isTrendOnly = !r.zone_type || r.zone_type === 'none';
      // Structure badge for the result row
      const moOn     = $('moen') && $('moen').checked;
      const structSt = moOn ? r.struct_low : r.struct_low;
      const structLbl= (structSt || '').replace(/_/g,' ');
      const sCol     = structColor(structSt);
      const sBg      = structBg(structSt);

      return `<div class="sr${ac}" data-i="${i}" onclick="Scanner.selStock(${i})">
        <div>
          <div class="srn">${r.ticker.replace('.NS','')}</div>
          <div class="srm">
            <span class="tbadge ${QS.trendClass(r.trend)}">${QS.trendLabel(r.trend)}</span>
            <span style="font-size:9px;padding:1px 5px;border-radius:3px;
                  background:${sBg};color:${sCol};font-family:var(--mono)">
              ${structLbl||'—'}
            </span>
          </div>
        </div>
        <div class="srp">${QS.inr(r.close)}</div>
        <div class="srz">
          ${isTrendOnly
            ? `<span class="sra" style="color:var(--muted)">${r.trend_strength||'—'}</span>`
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
    document.querySelectorAll('.sr.on').forEach(e => e.classList.remove('on'));
    const el = document.querySelector(`.sr[data-i="${i}"]`);
    if (el) el.classList.add('on');
    activeIdx = i;
  }

  function sortBy(k) {
    $('s-age').classList.toggle('on', k === 'bars_ago');
    $('s-atr').classList.toggle('on', k === 'atr_pct');
    rows.sort((a, b) => (a[k] || 0) - (b[k] || 0));
    _buildList();
    if (activeIdx >= 0) _markActive(activeIdx);
  }

  /* ════════════════════════════════════════════════════════════
     STOCK SELECTION + CHART LOAD
  ════════════════════════════════════════════════════════════ */
  async function selStock(i) {
    const row = rows[i]; if (!row) return;
    if (currentRow && currentRow.ticker === row.ticker && currentRow._tf === chartTF) {
      _markActive(i); return;
    }
    _markActive(i);
    currentRow = { ...row, _tf: chartTF };

    $('cptk').textContent = row.ticker.replace('.NS','');
    $('cppr').textContent = QS.inr(row.close);
    const te = $('cptr');
    te.textContent   = QS.trendLabel(row.trend);
    te.className     = 'tbadge ' + QS.trendClass(row.trend);
    te.style.display = 'inline-block';

    $('m1').textContent = row.atr_pct + '%';
    $('m2').textContent = row.trend_strength || '—';
    $('m3').textContent = row.zones_count ?? '—';
    $('m4').textContent = row.bars_ago != null ? row.bars_ago + 'b' : '—';
    $('m5').textContent = row.pivot_count ?? '—';
    const vp = (row.vel_current || 0) >= 0;
    $('m6').textContent = `${vp?'+':''}${row.vel_current}`;
    $('m6').className   = 'mv ' + (vp ? 'pos' : 'neg');
    $('m7').textContent = (row.lr_ratio || 0) + '×';
    $('m7').className   = 'mv ' + ((row.lr_ratio||1) >= 1 ? 'pos' : 'neg');
    $('m8').textContent = row.amp_avg || '—';

    await Chart.load(row.ticker, {
      interval:    chartTF,    order:      $('order').value,
      legout_mult: $('legout').value, strategy: $('strat').value,
      zone_lookback: $('zlb').value, multi_order: $('moen').checked,
      order_low: $('mol').value, order_mid: $('mom').value,
      order_high: $('moh').value,
    }, row.direction || getDir(), row.zone_legin_ts);

    try {
      sessionStorage.setItem(SS_ROWS_KEY, JSON.stringify({ rows, activeIdx }));
    } catch(_) {}
  }

  function setChartTF(tf, btn) {
    chartTF = tf;
    document.querySelectorAll('.tfg .tfb').forEach(b => b.classList.remove('on'));
    btn.classList.add('on');
    Chart.bustCache(tf);
    _updatePivotButtons();
    _saveUIState();
    if (currentRow) { currentRow._tf = tf; selStock(activeIdx); }
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
    const vp  = (r.vel_current || 0) >= 0;
    const vs  = vp ? '+' : '';
    const ac  = r.vel_label === 'accelerating' ? '↑ Accel'
              : r.vel_label === 'decelerating' ? '↓ Decel' : '→ Steady';
    const vw  = Math.min(Math.abs(r.vel_current||0) / Math.max(r.vel_avg||.001,.001) * 45, 45);
    const zt  = ZT[r.zone_type] || ZT.rbr;
    const zsc = r.zone_status==='fresh' ? '#22c55e'
              : r.zone_status==='tested'? '#f59e0b' : 'var(--muted2)';
    const isTrendOnly = !r.zone_type || r.zone_type === 'none';

    // Structure state block
    const lo  = r.struct_low   || '—';
    const mid = r.struct_mid   || '—';
    const hi  = r.struct_high  || '—';
    const aln = r.struct_alignment || '—';
    const bull= r.struct_bull_count ?? '—';
    const bear= r.struct_bear_count ?? '—';

    function _structRow(order, state, pair) {
      const col  = structColor(state);
      const lbl  = (state||'—').replace(/_/g,' ');
      const pair_= pair || '—';
      return `<div class="it">
        <div class="itl">Order ${order}</div>
        <div class="itv" style="color:${col};font-size:11px">${lbl}</div>
        <div class="its">${pair_}</div>
      </div>`;
    }

    const structHtml = `
    <div>
      <div class="dwst">Pivot Structure</div>
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px;
           padding:7px 10px;background:var(--s2);border:1px solid var(--border);
           border-radius:var(--r2)">
        <span style="font-size:10px;color:var(--muted);font-family:var(--mono)">
          Alignment:
        </span>
        <span style="font-size:11px;font-weight:600;font-family:var(--mono);
              color:${aln==='bullish'?'#22c55e':aln==='bearish'?'#ef4444':'#f59e0b'}">
          ${aln}
        </span>
        <span style="font-size:10px;color:var(--muted);margin-left:auto">
          🟢${bull} 🔴${bear}
        </span>
      </div>
      <div class="ig3">
        ${_structRow(r.mo_order_high||'H', hi,  r.struct_high_pair)}
        ${_structRow(r.mo_order_mid ||'M', mid, r.struct_mid_pair)}
        ${_structRow(r.mo_order_low ||'L', lo,  r.struct_low_pair)}
      </div>
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
    const trendVals = [...document.querySelectorAll('.tc')]
      .map(c => ({ v: c.value, chk: c.checked }));
    QS.saveState({
      sc_dir:    getDir(),     sc_tf:      getScanTF(),
      sc_chartTF: chartTF,    sc_strategy: $('strat')?.value,
      sc_order:  $('order')?.value, sc_zlb: $('zlb')?.value,
      sc_legout: $('legout')?.value,
      sc_moEn:   $('moen')?.checked,
      sc_moh:    $('moh')?.value, sc_mom: $('mom')?.value,
      sc_mol:    $('mol')?.value,
      sc_align:  $('align-filter')?.value || 'any',
      sc_atrRbr: $('atr-rbr')?.checked, sc_atrDbr: $('atr-dbr')?.checked,
      sc_atrDbd: $('atr-dbd')?.checked, sc_atrRbd: $('atr-rbd')?.checked,
      sc_sbCol:  $('sidebar')?.classList.contains('col'),
      sc_trends: trendVals,
      // Structure state selections
      sc_sf_low:  [..._selected.low],
      sc_sf_mid:  [..._selected.mid],
      sc_sf_high: [..._selected.high],
    });
  }

  function _restoreUIState() {
    const s = QS.loadState();

    if (s.sc_dir) {
      $('dir-seg').querySelectorAll('.seg-btn').forEach(b => {
        b.className = 'seg-btn';
        if (b.dataset.v === s.sc_dir) b.className = 'seg-btn ' + s.sc_dir;
      });
    }
    if (s.sc_tf) {
      $('tfc-row').querySelectorAll('.tfc').forEach(b =>
        b.classList.toggle('on', b.dataset.v === s.sc_tf)
      );
    }
    if (s.sc_chartTF) {
      chartTF = s.sc_chartTF;
      document.querySelectorAll('.tfg .tfb').forEach(b =>
        b.classList.toggle('on', b.dataset.tf === chartTF)
      );
    }
    if (s.sc_strategy && $('strat')) {
      $('strat').value = s.sc_strategy; onStrategyChange();
    }
    if (s.sc_order  && $('order'))  $('order').value  = s.sc_order;
    if (s.sc_zlb    && $('zlb'))    $('zlb').value    = s.sc_zlb;
    if (s.sc_legout && $('legout')) $('legout').value = s.sc_legout;
    if ($('atr-rbr') && s.sc_atrRbr !== undefined) $('atr-rbr').checked = s.sc_atrRbr;
    if ($('atr-dbr') && s.sc_atrDbr !== undefined) $('atr-dbr').checked = s.sc_atrDbr;
    if ($('atr-dbd') && s.sc_atrDbd !== undefined) $('atr-dbd').checked = s.sc_atrDbd;
    if ($('atr-rbd') && s.sc_atrRbd !== undefined) $('atr-rbd').checked = s.sc_atrRbd;
    if (s.sc_moEn !== undefined && $('moen')) {
      $('moen').checked = s.sc_moEn; toggleMO(s.sc_moEn);
    }
    if (s.sc_moh && $('moh')) $('moh').value = s.sc_moh;
    if (s.sc_mom && $('mom')) $('mom').value = s.sc_mom;
    if (s.sc_mol && $('mol')) $('mol').value = s.sc_mol;
    if (s.sc_align && $('align-filter')) $('align-filter').value = s.sc_align;
    if (s.sc_sbCol) $('sidebar').classList.add('col');
    if (Array.isArray(s.sc_trends) && s.sc_trends.length) {
      document.querySelectorAll('.tc').forEach(cb => {
        const e = s.sc_trends.find(t => t.v === cb.value);
        if (e) cb.checked = e.chk;
      });
      _updateTrendLabel();
    }
    // Restore structure selections — then re-render dropdowns
    if (Array.isArray(s.sc_sf_low))  { _selected.low  = new Set(s.sc_sf_low);  }
    if (Array.isArray(s.sc_sf_mid))  { _selected.mid  = new Set(s.sc_sf_mid);  }
    if (Array.isArray(s.sc_sf_high)) { _selected.high = new Set(s.sc_sf_high); }
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
    } catch (_) {}
  }

  /* ════════════════════════════════════════════════════════════
     INIT
  ════════════════════════════════════════════════════════════ */
  async function init() {
    QS.renderNav('scanner', 'nst', 'nbadge');

    // Load state groups from API then render structure dropdowns
    await _loadStateGroups();
    _renderStructureFilters();

    document.addEventListener('click', () => {
      document.querySelectorAll('.tdd').forEach(d => d.classList.remove('op'));
      document.querySelectorAll('.trig').forEach(t => t.classList.remove('op'));
    });
    $('tdd').addEventListener('click', e => e.stopPropagation());
    document.querySelectorAll('.tc').forEach(cb =>
      cb.addEventListener('change', () => { _updateTrendLabel(); _saveUIState(); })
    );
    if ($('strat')) $('strat').addEventListener('change', onStrategyChange);
    ['atr-rbr','atr-dbr','atr-dbd','atr-rbd'].forEach(id => {
      if ($(id)) $(id).addEventListener('change', _saveUIState);
    });
    ['order','moh','mom','mol'].forEach(id => {
      const el = $(id); if (!el) return;
      el.addEventListener('input',  () => { _updatePivotButtons(); _saveUIState(); });
      el.addEventListener('change', () => { _updatePivotButtons(); _saveUIState(); });
    });
    ['strat','zlb','legout','align-filter'].forEach(id => {
      const el = $(id); if (el) el.addEventListener('change', _saveUIState);
    });

    _restoreUIState();
    window.addEventListener('beforeunload', _saveUIState);
  }

  return {
    init, runScan, selStock, sortBy, setChartTF,
    toggleSB, stp, setDir, setTFC, toggleTDD,
    allTrends, toggleMO, onStrategyChange,
    openDrawer, closeDrawer,
    // Structure dropdown internals exposed for inline HTML onchange
    _sfToggle, _sfAllToggle, _sfChange,
  };
})();

window.Scanner = Scanner;
document.addEventListener('DOMContentLoaded', Scanner.init);
