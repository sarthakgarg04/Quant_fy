/* ═══════════════════════════════════════════════════════════════
   trend_scanner.js  —  Trend Scanner page controller
   Combines the scanner engine (trend_only strategy, all filters,
   equity + crypto, multi-order) with single-symbol analysis.
   Depends on: shared.js, api.js, chart.js, filters.js
═══════════════════════════════════════════════════════════════ */

const TrendScanner = (() => {

  const $  = id => document.getElementById(id);
  const $$ = sel => document.querySelectorAll(sel);

  /* ── Page state ─────────────────────────────────────────── */
  let rows       = [];
  let activeIdx  = -1;
  let chartTF    = '1d';
  let currentRow = null;
  let assetClass = 'equity';

  /* ── Structure filters — delegated to shared Filters module ─
     Uses namespace 'ts' so IDs are ts-sfwrap-low, ts-sfdd-low …
     matching the HTML placeholders ts-sf-single-wrap / ts-sf-multi-wrap */
  const _F      = Filters.create('ts');
  Filters._register(_F);
  const _selected = _F.selected;   // { low: Set, mid: Set, high: Set }

  const SS_KEY      = 'qs_ts_state_v1';
  const SS_ROWS_KEY = 'qs_ts_rows_v1';

  /* ── Structure state display helpers ─────────────────────── */
  const BULL_STATES = new Set([
    'trending_up',
    'top_coil',
    'top_expanding',
    'bottom_expanding_breakout',
    'bottom_pullback_breakout',
    'coiling_to_up',
    'expanding_to_up',
    'structure_up',
  ]);
  const BEAR_STATES = new Set([
    'trending_down',
    'top_expanding_breakdown',
    'top_pullback_breakdown',
    'bottom_coil',
    'bottom_expanding',
    'coiling_to_down',
    'expanding_to_down',
    'structure_down',
  ]);

  function _stateColor(s) {
    if (!s || s === 'no_structure') return 'var(--muted)';
    if (BULL_STATES.has(s)) return '#22c55e';
    if (BEAR_STATES.has(s)) return '#ef4444';
    return '#f59e0b';
  }
  function _stateClass(s) {
    if (BULL_STATES.has(s)) return 'ss-bull';
    if (BEAR_STATES.has(s)) return 'ss-bear';
    return 'ss-neut';
  }
  function _stateLabel(s) { return (s || '—').replace(/_/g, ' '); }

  /* ════════════════════════════════════════════════════════════
     SIDEBAR
  ════════════════════════════════════════════════════════════ */
  function toggleSB() {
    $('ts-sidebar').classList.toggle('col');
    _saveState();
  }

  /* ════════════════════════════════════════════════════════════
     ASSET CLASS
  ════════════════════════════════════════════════════════════ */
  function setAsset(btn) {
    assetClass = btn.dataset.v;
    const isCrypto = assetClass === 'crypto';

    $$('#ts-asset-seg .seg-btn').forEach(b => {
      b.className = 'seg-btn' + (b.dataset.v === assetClass ? ' buy' : '');
    });

    // Scan TF visibility
    $$('#ts-tfc-row .tfc').forEach(b => {
      const a = b.dataset.asset || 'both';
      b.style.display = (a === 'both' || (isCrypto ? a === 'crypto' : a === 'equity')) ? '' : 'none';
    });
    const defaultScanTF = isCrypto ? '15m' : '1d';
    $$('#ts-tfc-row .tfc').forEach(b =>
      b.classList.toggle('on', b.dataset.v === defaultScanTF && b.offsetParent !== null)
    );

    // Chart TF visibility
    $$('.tfg .tfb').forEach(b => {
      const a = b.dataset.asset || 'both';
      b.style.display = (a === 'both' || (isCrypto ? a === 'crypto' : a === 'equity')) ? '' : 'none';
    });
    chartTF = isCrypto ? '15m' : '1d';
    $$('.tfg .tfb').forEach(b =>
      b.classList.toggle('on', b.dataset.tf === chartTF && b.offsetParent !== null)
    );

    // Asset badge
    const badge = $('ts-asset-badge');
    if (badge) {
      badge.textContent       = isCrypto ? '₿ CRYPTO' : 'EQUITY';
      badge.style.background  = isCrypto ? 'rgba(245,158,11,.12)' : 'rgba(99,102,241,.12)';
      badge.style.borderColor = isCrypto ? 'rgba(245,158,11,.3)'  : 'rgba(99,102,241,.3)';
      badge.style.color       = isCrypto ? '#f59e0b'              : 'var(--accent)';
    }
    const rcnt = $('ts-rcnt');
    if (rcnt) rcnt.className = 'ts-rcnt' + (isCrypto ? ' crypto' : '');

    // Clear stale results
    rows = []; currentRow = null; activeIdx = -1;
    $('ts-rlist').innerHTML = `<div class="ts-empty">
      <div class="ts-empty-ico">${isCrypto ? '₿' : '📊'}</div>
      <div class="ts-empty-txt">Press Scan to find ${isCrypto ? 'crypto' : 'equity'} setups</div>
      <div class="ts-empty-sub">Configure filters → Run Scan</div></div>`;
    $('ts-rcnt').textContent = '—';

    try { sessionStorage.setItem('qs_ts_asset', assetClass); } catch (_) {}
  }

  /* ════════════════════════════════════════════════════════════
     TIMEFRAME
  ════════════════════════════════════════════════════════════ */
  function setTFC(btn) {
    $$('#ts-tfc-row .tfc').forEach(b => b.classList.remove('on'));
    btn.classList.add('on');
    _saveState();
  }

  function getScanTF() {
    const active = document.querySelector('#ts-tfc-row .tfc.on');
    if (active && active.offsetParent !== null) return active.dataset.v;
    const first = [...$$('#ts-tfc-row .tfc')].find(b => b.offsetParent !== null);
    return first?.dataset.v || (assetClass === 'crypto' ? '15m' : '1d');
  }

  function setChartTF(tf, btn) {
    chartTF = tf;
    $$('.tfg .tfb').forEach(b => b.classList.remove('on'));
    btn.classList.add('on');
    Chart.bustCache(tf);
    _saveState();
    if (activeIdx >= 0 && rows[activeIdx]) {
      currentRow = null;
      _loadChart(rows[activeIdx]);
    }
  }

  /* ════════════════════════════════════════════════════════════
     PIVOT ORDER STEPPER
  ════════════════════════════════════════════════════════════ */
  function stp(id, dv) {
    const el = $(id);
    if (!el) return;
    el.value = Math.max(parseInt(el.min || 1), Math.round(parseFloat(el.value) + dv));
    _renderStructureFilters();
    _updatePivotButtons();
    _saveState();
  }

  /* ════════════════════════════════════════════════════════════
     MULTI-ORDER TOGGLE
     Uses same .moc/.mob/.hid pattern as Zone Scanner
  ════════════════════════════════════════════════════════════ */
  function toggleMO(on) {
    const mob = $('ts-mob');
    if (mob) mob.classList.toggle('hid', !on);
    _renderStructureFilters();
    _updatePivotButtons();
    _saveState();
  }

  /* ════════════════════════════════════════════════════════════
     STRUCTURE FILTERS — fully delegated to Filters module
  ════════════════════════════════════════════════════════════ */
  function _renderStructureFilters() {
    const mo = $('ts-moen')?.checked;
    _F.renderStructureFilters(
      $('ts-sf-single-wrap'),
      $('ts-sf-multi-wrap'),
      mo,
      {
        high: $('ts-moh')?.value || '20',
        mid:  $('ts-mom')?.value || '10',
        low:  $('ts-mol')?.value || '5',
      }
    );
  }

  /* ════════════════════════════════════════════════════════════
     PIVOT ORDER BUTTONS — identical logic to Zone Scanner
     Container id: ts-pivot-order-btns
  ════════════════════════════════════════════════════════════ */
  function _updatePivotButtons() {
    const mo        = $('ts-moen')?.checked;
    const container = $('ts-pivot-order-btns');
    if (!container) return;
    container.innerHTML = '';

    Chart.setActiveLevels(mo ? ['H', 'M', 'L'] : ['single']);

    // Use scan_params order if a row is active (scan context), else UI input
    const sp = currentRow?.scan_params || {};

    if (!mo) {
      const order = sp.order || $('ts-order')?.value || 5;
      const btn   = document.createElement('button');
      btn.className     = 'pob on';
      btn.dataset.level = 'single';
      btn.innerHTML     = `<span class="pob-dot" style="background:#3b82f6"></span>Order ${order}`;
      btn.onclick       = () => _onPivotToggle('single', btn);
      container.appendChild(btn);
    } else {
      const orders = {
        H: sp.order_high || parseInt($('ts-moh')?.value) || 20,
        M: sp.order_mid  || parseInt($('ts-mom')?.value) || 10,
        L: sp.order_low  || parseInt($('ts-mol')?.value) || 5,
      };
      ['H', 'M', 'L'].forEach(lv => {
        const mc  = Chart.moColors[lv];
        const btn = document.createElement('button');
        btn.className     = `pob on ${mc.cls}`;
        btn.dataset.level = lv;
        btn.innerHTML     = `<span class="pob-dot" style="background:${mc.dot}"></span>${mc.label}(${orders[lv]})`;
        btn.onclick       = () => _onPivotToggle(lv, btn);
        container.appendChild(btn);
      });
    }
  }

  function _onPivotToggle(level, btn) {
    const newLevels = Chart.toggleLevel(level);
    document.querySelectorAll('#ts-pivot-order-btns .pob').forEach(b =>
      b.classList.toggle('on', newLevels.includes(b.dataset.level))
    );
  }

  function _getStructureParam(level) {
    return _F.getStructureParam(level);
  }

  /* ════════════════════════════════════════════════════════════
     TREND DROPDOWN
  ════════════════════════════════════════════════════════════ */
  function toggleTDD(e) {
    e.stopPropagation();
    $('ts-tdd').classList.toggle('op');
    $('ts-ttrig').classList.toggle('op');
  }

  function allTrends(cb) {
    $$('.ts-tc').forEach(c => { c.checked = cb.checked; });
    _updateTrendLabel();
  }

  function _updateTrendLabel() {
    const all = [...$$('.ts-tc')];
    const chk = all.filter(c => c.checked);
    $('ts-tlbl').textContent = (chk.length === all.length || chk.length === 0)
      ? 'All trends' : `${chk.length} selected`;
    if ($('ts-all-t')) $('ts-all-t').checked = chk.length === all.length;
  }

  function _getSelTrends() {
    const all = [...$$('.ts-tc')];
    const chk = all.filter(c => c.checked);
    if (chk.length === 0 || chk.length === all.length) return '';
    return chk.map(c => c.value).join(',');
  }

  /* ════════════════════════════════════════════════════════════
     SCAN
  ════════════════════════════════════════════════════════════ */
  async function runScan() {
    const btn = $('ts-scanbtn');
    btn.disabled = true; btn.textContent = '⏳  Scanning…';
    QS.setNavStatus('scanning…');

    $('ts-rlist').innerHTML = Array(6).fill(0).map(() =>
      `<div class="ts-shim">
        <div class="ts-sh" style="height:11px;width:45%;border-radius:3px"></div>
        <div class="ts-sh" style="height:9px;width:65%;border-radius:3px;margin-top:5px"></div>
      </div>`).join('');
    $('ts-rcnt').textContent = '…';
    activeIdx = -1;

    try {
      const mo = $('ts-moen').checked;
      const tr = _getSelTrends();
      const ord = $('ts-order').value;

      const params = {
        direction:        'buy',
        interval:         getScanTF(),
        order:            ord,
        zone_lookback:    0,
        strategy:         'trend_only',
        multi_order:      mo,
        order_low:        mo ? $('ts-mol').value : ord,
        order_mid:        mo ? $('ts-mom').value : ord,
        order_high:       mo ? $('ts-moh').value : ord,
        structure_low:    _getStructureParam('low'),
        structure_mid:    mo ? _getStructureParam('mid')  : '',
        structure_high:   mo ? _getStructureParam('high') : '',
        alignment_filter: $('ts-align').value || 'any',
      };
      if (tr) params.trend_filter = tr;

      const d = assetClass === 'crypto'
        ? await API.cryptoScan(params)
        : await API.scan(params);

      rows = d.results || [];
      $('ts-rcnt').textContent = rows.length;
      QS.setNavStatus(`${rows.length} found`, `${rows.length} setups`);
      _buildList();
      _saveState();
      try { sessionStorage.setItem(SS_ROWS_KEY, JSON.stringify({ rows, activeIdx })); } catch (_) {}

    } catch (e) {
      $('ts-rlist').innerHTML =
        `<div class="ts-empty"><span style="color:var(--red)">⚠ ${e.message}</span></div>`;
      QS.setNavStatus('error');
    } finally {
      btn.disabled = false; btn.textContent = '▶  Run Scan';
    }
  }

  /* ════════════════════════════════════════════════════════════
     RESULTS LIST
  ════════════════════════════════════════════════════════════ */
  function _buildList() {
    if (!rows.length) {
      $('ts-rlist').innerHTML = `<div class="ts-empty">
        <div class="ts-empty-ico">◈</div>
        <div class="ts-empty-txt">No results found</div>
        <div class="ts-empty-sub">Try broadening your filters</div></div>`;
      return;
    }

    $('ts-rlist').innerHTML = rows.map((r, i) => {
      const ticker = assetClass === 'crypto'
        ? r.ticker.replace('_PERP', '')
        : r.ticker.replace('.NS', '').replace('.BO', '');

      const price = assetClass === 'crypto'
        ? (r.close >= 1000
            ? '$' + r.close.toLocaleString('en-US', { maximumFractionDigits: 2 })
            : r.close >= 1
              ? '$' + r.close.toFixed(4)
              : '$' + r.close.toFixed(6))
        : QS.inr(r.close);

      const trend       = r.trend || '';
      const trendBadge  = `<span class="tsr-badge ${QS.trendClass(trend)}"
        style="${QS.trendStyle(trend)}">${QS.trendLabel(trend)}</span>`;

      const lo  = r.struct_low;
      const mid = r.struct_mid;
      const hi  = r.struct_high;
      const wasMO = r.scan_params?.multi_order;

      const structBadges = wasMO
        ? [
            hi  ? `<span class="tsr-badge ${_stateClass(hi)}"  title="H(${r.mo_order_high||'—'})">H: ${_stateLabel(hi)}</span>`  : '',
            mid ? `<span class="tsr-badge ${_stateClass(mid)}" title="M(${r.mo_order_mid||'—'})">M: ${_stateLabel(mid)}</span>` : '',
            lo  ? `<span class="tsr-badge ${_stateClass(lo)}"  title="L(${r.mo_order_low||'—'})">L: ${_stateLabel(lo)}</span>`  : '',
          ].filter(Boolean).join('')
        : lo
          ? `<span class="tsr-badge ${_stateClass(lo)}">${_stateLabel(lo)}</span>`
          : '';

      return `<div class="tsr${activeIdx === i ? ' on' : ''}" data-i="${i}"
              onclick="TrendScanner.selStock(${i})">
        <div class="tsr-top">
          <div style="display:flex;align-items:center;gap:5px;min-width:0;flex:1;overflow:hidden">
            <span class="tsr-ticker">${ticker}</span>
            ${trendBadge}
          </div>
          <span class="tsr-price" style="flex-shrink:0;padding-left:6px">${price}</span>
        </div>
        <div style="display:flex;flex-direction:column;gap:3px;margin-top:4px">
          ${structBadges}
          ${r.atr_pct != null
            ? `<span style="font-size:9px;color:var(--muted);font-family:var(--mono)">ATR ${r.atr_pct}%</span>`
            : ''}
        </div>
      </div>`;
    }).join('');
  }

  function _markActive(i) {
    $$('.tsr.on').forEach(el => el.classList.remove('on'));
    const el = document.querySelector(`.tsr[data-i="${i}"]`);
    if (el) el.classList.add('on');
    activeIdx = i;
  }

  /* ════════════════════════════════════════════════════════════
     SYMBOL SELECTION + CHART
  ════════════════════════════════════════════════════════════ */
  async function selStock(i) {
    const row = rows[i];
    if (!row) return;

    // Snap chart TF to scan TF on first click
    if (!currentRow) {
      const scanTF = (row.scan_params || {}).interval || getScanTF();
      if (chartTF !== scanTF) {
        chartTF = scanTF;
        $$('.tfg .tfb').forEach(b => b.classList.toggle('on', b.dataset.tf === chartTF));
      }
    }

    // Guard: same ticker + same TF already loaded
    if (currentRow?.ticker === row.ticker && currentRow?._tf === chartTF) {
      _markActive(i); return;
    }

    _markActive(i);
    currentRow = { ...row, _tf: chartTF };

    // Chart header
    const ticker = assetClass === 'crypto'
      ? row.ticker.replace('_PERP', '')
      : row.ticker.replace('.NS', '').replace('.BO', '');
    $('ts-aticker').textContent = ticker;
    $('ts-aprice').textContent  = assetClass === 'crypto'
      ? (row.close >= 1000
          ? '$' + row.close.toLocaleString('en-US', { maximumFractionDigits: 2 })
          : row.close >= 1 ? '$' + row.close.toFixed(4) : '$' + row.close.toFixed(6))
      : QS.inr(row.close);

    const te = $('ts-atrend');
    te.textContent   = QS.trendLabel(row.trend);
    te.className     = 'tbadge ' + QS.trendClass(row.trend);
    te.style.display = 'inline-block';

    // Bottom metrics bar
    $('ts-m1').textContent = row.atr_pct + '%';
    $('ts-m2').textContent = row.trend_strength || '—';
    $('ts-m3').textContent = row.pivot_count ?? '—';
    const vp = (row.vel_current || 0) >= 0;
    $('ts-m4').textContent = `${vp ? '+' : ''}${row.vel_current ?? '—'}`;
    $('ts-m4').className   = 'mv ' + (vp ? 'pos' : 'neg');
    $('ts-m5').textContent = (row.lr_ratio || 0) + '×';
    $('ts-m5').className   = 'mv ' + ((row.lr_ratio || 1) >= 1 ? 'pos' : 'neg');
    $('ts-m6').textContent = row.amp_avg || '—';

    // Load chart + analysis in parallel
    _updatePivotButtons();   // refresh badge to match actual chart order
    await Promise.all([_loadChart(row), _loadAnalysis(row)]);

    try { sessionStorage.setItem(SS_ROWS_KEY, JSON.stringify({ rows, activeIdx })); } catch (_) {}
  }

  async function _loadChart(row) {
    const mo  = $('ts-moen').checked;
    const ord = $('ts-order').value;
    const params = {
      interval:    chartTF,
      order:       mo ? $('ts-mol').value : ord,
      legout_mult: 1.35,
      strategy:    'trend_only',
      multi_order: mo,
      order_low:   $('ts-mol').value,
      order_mid:   $('ts-mom').value,
      order_high:  $('ts-moh').value,
    };

    if (assetClass === 'crypto') {
      await Chart.load(row.ticker.replace('_PERP', ''), params, 'buy', null, { endpoint: 'crypto' });
    } else {
      await Chart.load(row.ticker, params, 'buy', null);
    }
  }

  /* ════════════════════════════════════════════════════════════
     ANALYSIS PANEL (right-side stats column)
  ════════════════════════════════════════════════════════════ */
  async function _loadAnalysis(row) {
    const ticker   = (row.ticker || '').replace('_PERP','').replace('.NS','').replace('.BO','');
    const subtitle = `ATR ${row.atr_pct ?? '—'}%`;
  
    /* Trend pill */
    const trendHtml = InfoDrawer.trendPill(row.trend);
  
    /* Key stats */
    const vp = (row.vel_current || 0) >= 0;
    const statsHtml = InfoDrawer.section('Stats',
      InfoDrawer.statGrid([
        ['ATR %',    (row.atr_pct ?? '—') + '%'],
        ['Strength', row.trend_strength || '—'],
        ['Pivots',   row.pivot_count    ?? '—'],
        ['Vel Δ',    `<span class="${vp?'pos':'neg'}">${vp?'+':''}${row.vel_current??'—'}</span>`],
        ['LR Ratio', `<span class="${(row.lr_ratio||1)>=1?'pos':'neg'}">${row.lr_ratio||'—'}×</span>`],
        ['Amp Avg',  row.amp_avg || '—'],
      ])
    );
  
    /* Multi-order */
    let moHtml = '';
    if (row.struct_low || row.struct_mid || row.struct_high) {
      const alignTxt = row.struct_alignment ? `· ${row.struct_alignment}` : '';
      const stColor  = s => {
        if (BULL_STATES.has(s)) return '#22c55e';
        if (BEAR_STATES.has(s)) return '#ef4444';
        return '#f59e0b';
      };
      moHtml = InfoDrawer.section(
        `Multi-Order <span style="font-size:9px;color:var(--amber);font-weight:400;
        text-transform:none;letter-spacing:0;margin-left:4px">${alignTxt}</span>`,
        [['HIGH', row.struct_high, row.mo_order_high],
        ['MID',  row.struct_mid,  row.mo_order_mid],
        ['LOW',  row.struct_low,  row.mo_order_low]]
          .map(([lv, st, ord]) => st ? InfoDrawer.moBar(lv, st, ord, stColor(st)) : '')
          .join('')
      );
    }
  
    /* Structure metrics */
    const structHtml = InfoDrawer.section('Structure',
      InfoDrawer.statGrid([
        ['Vel Accel', row.vel_label    || '—'],
        ['Vel Avg',   row.vel_avg      ?? '—'],
        ['Amp Regime',row.amp_regime   || '—'],
        ['Amp Var',   row.amp_variance != null ? row.amp_variance + '%' : '—'],
        ['LR Recent', row.lr_recent    != null ? row.lr_recent + '×' : '—'],
        ['LR Bull',   row.lr_bull      != null ? row.lr_bull   + '×' : '—'],
      ])
    );
  
    /* Confluence placeholder — filled async below */
    const confHtml = InfoDrawer.section('Confluence',
      `<div id="qs-idr-cbars">
        ${InfoDrawer.confBar('Trend', 0, '#3b82f6')}
        ${InfoDrawer.confBar('Zone',  0, '#22c55e')}
        ${InfoDrawer.confBar('HTF',   0, '#a78bfa')}
        ${InfoDrawer.confBar('Vol',   0, '#f59e0b')}
      </div>`
    );
  
    /* Edge table placeholder */
    const edgeHtml = InfoDrawer.section('Edge',
      `<table style="width:100%;border-collapse:collapse;font-size:10px;font-family:var(--mono)">
        <thead><tr style="border-bottom:1px solid var(--border)">
          <th style="color:var(--muted2);font-weight:400;padding-bottom:3px">Type</th>
          <th style="color:var(--muted2);font-weight:400">Bars</th>
          <th style="color:var(--muted2);font-weight:400">N</th>
          <th style="color:var(--muted2);font-weight:400">Hit%</th>
          <th style="color:var(--muted2);font-weight:400">Avg R</th>
          <th style="color:var(--muted2);font-weight:400">PO</th>
        </tr></thead>
        <tbody id="qs-idr-etb">
          <tr><td colspan="6" style="color:var(--muted);text-align:center;padding:6px">Loading…</td></tr>
        </tbody>
      </table>`
    );
  
    /* Debug pivots */
    let debugHtml = '';
    const wasMO = row.scan_params?.multi_order;
    if (wasMO && (row.debug_pivots_H || row.debug_pivots_M || row.debug_pivots_L)) {
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
        pvtTable(row.debug_pivots_H, `HIGH ord=${row.mo_order_high||'?'}`, '#7c3aed') +
        pvtTable(row.debug_pivots_M, `MID  ord=${row.mo_order_mid||'?'}`,  '#1d4ed8') +
        pvtTable(row.debug_pivots_L, `LOW  ord=${row.mo_order_low||'?'}`,  '#15803d')
      );
    }
  
    /* Populate drawer immediately with sync content */
    InfoDrawer.populate(
      ticker, subtitle,
      trendHtml + statsHtml + moHtml + structHtml + confHtml + edgeHtml + debugHtml
    );
  
    /* Async: fill confluence + edge after populate */
    try {
      const apiTicker = assetClass === 'crypto'
        ? row.ticker.replace('_PERP','').replace('USDT','') + 'USDT'
        : row.ticker;
      const mo = $('ts-moen').checked;
      const d  = await API.trendData(apiTicker, {
        interval:    getScanTF(),
        order:       mo ? $('ts-mol').value : $('ts-order').value,
        multi_order: mo,
        order_low:   $('ts-mol').value,
        order_mid:   $('ts-mom').value,
        order_high:  $('ts-moh').value,
      });
  
      const c = d.confluence || {};
      const cbarsEl = document.getElementById('qs-idr-cbars');
      if (cbarsEl) {
        cbarsEl.innerHTML =
          InfoDrawer.confBar('Trend', c.trend_score || 0, '#3b82f6') +
          InfoDrawer.confBar('Zone',  c.zone_score  || 0, '#22c55e') +
          InfoDrawer.confBar('HTF',   c.htf_score   || 0, '#a78bfa') +
          InfoDrawer.confBar('Vol',   c.vol_score   || 0, '#f59e0b');
      }
  
      const etbEl = document.getElementById('qs-idr-etb');
      if (etbEl) {
        etbEl.innerHTML = d.edge?.length
          ? d.edge.map(r => `
              <tr style="border-bottom:1px solid var(--s3)">
                <td style="color:${r.type==='B'?'var(--green)':'var(--red)'}">${r.type==='B'?'Bull':'Bear'}</td>
                <td>${r.horizon}b</td><td>${r.n}</td>
                <td class="${r.hit_rate>0.5?'pos':'neg'}">${r.hit_rate!=null?(r.hit_rate*100).toFixed(0)+'%':'–'}</td>
                <td class="${r.avg_R>0?'pos':'neg'}">${r.avg_R!=null?r.avg_R.toFixed(2)+'%':'–'}</td>
                <td>${r.payoff!=null?r.payoff.toFixed(2):'–'}</td>
              </tr>`).join('')
          : `<tr><td colspan="6" style="color:var(--muted);text-align:center;padding:8px">
              Not enough history</td></tr>`;
      }
    } catch (_) {
      const cbarsEl = document.getElementById('qs-idr-cbars');
      if (cbarsEl) cbarsEl.innerHTML =
        `<div style="font-size:10px;color:var(--muted)">Confluence unavailable</div>`;
    }
  }

  /* ════════════════════════════════════════════════════════════
     STATE PERSISTENCE
  ════════════════════════════════════════════════════════════ */
  function _saveState() {
    try {
      sessionStorage.setItem(SS_KEY, JSON.stringify({
        asset:   assetClass,
        tf:      getScanTF(),
        chartTF,
        order:   $('ts-order')?.value,
        moEn:    $('ts-moen')?.checked,
        moh:     $('ts-moh')?.value,
        mom:     $('ts-mom')?.value,
        mol:     $('ts-mol')?.value,
        align:   $('ts-align')?.value,
        sfLow:   [..._F.selected.low],
        sfMid:   [..._F.selected.mid],
        sfHigh:  [..._F.selected.high],
        sbCol:   $('ts-sidebar')?.classList.contains('col'),
      }));
    } catch (_) {}
  }

  function _restoreState() {
    try {
      const s = JSON.parse(sessionStorage.getItem(SS_KEY) || 'null');
      if (!s) return;

      // Restore asset class first — everything else depends on it
      if (s.asset === 'crypto') {
        const btn = document.querySelector('#ts-asset-seg [data-v="crypto"]');
        if (btn) setAsset(btn);
      }

      if (s.tf)      $$('#ts-tfc-row .tfc').forEach(b => b.classList.toggle('on', b.dataset.v === s.tf));
      if (s.chartTF) {
        chartTF = s.chartTF;
        $$('.tfg .tfb').forEach(b => b.classList.toggle('on', b.dataset.tf === chartTF));
      }
      if (s.order && $('ts-order'))  $('ts-order').value = s.order;
      if (s.moh   && $('ts-moh'))   $('ts-moh').value   = s.moh;
      if (s.mom   && $('ts-mom'))   $('ts-mom').value   = s.mom;
      if (s.mol   && $('ts-mol'))   $('ts-mol').value   = s.mol;
      if (s.align && $('ts-align')) $('ts-align').value = s.align;
      if (s.sbCol) $('ts-sidebar')?.classList.add('col');

      // Restore structure filter selections before re-rendering
      if (Array.isArray(s.sfLow))  _F.selected.low  = new Set(s.sfLow);
      if (Array.isArray(s.sfMid))  _F.selected.mid  = new Set(s.sfMid);
      if (Array.isArray(s.sfHigh)) _F.selected.high = new Set(s.sfHigh);

      // Restore MO toggle — this also re-renders structure filters
      if (s.moEn && $('ts-moen')) {
        $('ts-moen').checked = true;
        toggleMO(true);
      } else {
        _renderStructureFilters();
      }

      // Restore rows
      const saved = JSON.parse(sessionStorage.getItem(SS_ROWS_KEY) || 'null');
      if (saved?.rows?.length) {
        rows = saved.rows;
        $('ts-rcnt').textContent = rows.length;
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
    QS.renderNav('trend', 'ts-nav-status');
    Chart.init('ts-cw');
    InfoDrawer.attach(document.querySelector('.ts-chart-col'));

    // Trend dropdown — close on outside click
    document.addEventListener('click', e => {
      if (!e.target.closest('#ts-tdd') && !e.target.closest('#ts-ttrig')) {
        $('ts-tdd')?.classList.remove('op');
        $('ts-ttrig')?.classList.remove('op');
      }
    });

    // Structure dropdowns — handled by Filters module via installDocClickHandler
    _F.installDocClickHandler();

    $$('.ts-tc').forEach(cb => cb.addEventListener('change', _updateTrendLabel));

    // Load structure state groups (shared cache — only hits network once)
    await Filters.loadStateGroups();

    _restoreState();

    // Ensure MO panel and structure dropdowns reflect actual checkbox state
    toggleMO($('ts-moen')?.checked || false);
    _updatePivotButtons();
    window.addEventListener('beforeunload', _saveState);
  }

  /* ── Public surface ─────────────────────────────────────── */
  return {
    init, runScan, selStock,
    setAsset, setTFC, getScanTF, setChartTF,
    toggleSB, stp, toggleMO,
    toggleTDD, allTrends,
    _updatePivotButtons,
  };
})();

window.TrendScanner = TrendScanner;
document.addEventListener('DOMContentLoaded', TrendScanner.init);
