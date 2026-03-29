/* ═══════════════════════════════════════════════════════════════
   scanner.js — Scanner page controller
   Depends on: shared.js, api.js, chart.js
═══════════════════════════════════════════════════════════════ */

const Scanner = (() => {

  /* ── State ──────────────────────────────────────────────── */
  let rows       = [];
  let activeIdx  = -1;
  let sortKey    = 'bars_ago';
  let chartTF    = '1d';
  let currentRow = null;

  const SS_KEY = 'qs_scanner_v1';

  const $ = id => document.getElementById(id);

  /* ── Zone type colors ───────────────────────────────────── */
  const ZT = {
    rbr:           { bg:'rgba(34,197,94,.1)',   c:'#22c55e', b:'rgba(34,197,94,.3)'  },
    dbr:           { bg:'rgba(45,212,191,.1)',  c:'#2dd4bf', b:'rgba(45,212,191,.3)' },
    dbd:           { bg:'rgba(239,68,68,.1)',   c:'#ef4444', b:'rgba(239,68,68,.3)'  },
    rbd:           { bg:'rgba(249,115,22,.1)',  c:'#f97316', b:'rgba(249,115,22,.3)' },
    consolidation: { bg:'rgba(167,139,250,.1)', c:'#a78bfa', b:'rgba(167,139,250,.3)'},
  };

  /* ════════════════════════════════════════════════════════════
     SIDEBAR
  ════════════════════════════════════════════════════════════ */
  function toggleSB() {
    const sb = $('sidebar');
    sb.classList.toggle('col');
    if (sb.classList.contains('col')) {
      $('tdd').classList.remove('op');
      $('ttrig').classList.remove('op');
    }
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
  }

  function setDir(btn) {
    $('dir-seg').querySelectorAll('.seg-btn').forEach(b => b.className = 'seg-btn');
    btn.className = 'seg-btn ' + (btn.dataset.v === 'buy' ? 'buy' : 'sell');
  }
  function getDir() {
    const a = $('dir-seg').querySelector('.buy,.sell');
    return a?.dataset.v || 'buy';
  }

  function setTFC(btn) {
    $('tfc-row').querySelectorAll('.tfc').forEach(b => b.classList.remove('on'));
    btn.classList.add('on');
  }
  function getScanTF() {
    return $('tfc-row').querySelector('.tfc.on')?.dataset.v || '1d';
  }

  /* Trend dropdown */
  function toggleTDD(e) {
    e.stopPropagation();
    $('tdd').classList.toggle('op');
    $('ttrig').classList.toggle('op');
  }
  function allTrends(cb) {
    document.querySelectorAll('.tc').forEach(c => c.checked = cb.checked);
    _updateTrendLabel();
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
      const order = $('order').value;
      const btn   = document.createElement('button');
      btn.className     = 'pob on';
      btn.dataset.level = 'single';
      btn.innerHTML =
        `<span class="pob-dot" style="background:#3b82f6"></span>Order ${order}`;
      btn.onclick = () => _onPivotToggle('single', btn);
      container.appendChild(btn);
    } else {
      const orders = {
        H: parseInt($('moh').value) || 20,
        M: parseInt($('mom').value) || 10,
        L: parseInt($('mol').value) || 5,
      };
      ['H','M','L'].forEach(lv => {
        const mc  = Chart.moColors[lv];
        const btn = document.createElement('button');
        btn.className     = `pob on ${mc.cls}`;
        btn.dataset.level = lv;
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
    document.querySelectorAll('#pivot-order-btns .pob').forEach(b => {
      b.classList.toggle('on', newLevels.includes(b.dataset.level));
    });
    _updatePivotLegend();
  }

  function _updatePivotLegend() {
    const moEnabled = $('moen').checked;
    const leg = $('pivot-order-legend');
    if (!leg) return;
    if (!moEnabled) {
      leg.textContent = `order=${$('order').value}`;
    } else {
      const active = Chart.activeLevels;
      const parts  = [];
      if (active.includes('H')) parts.push(`H=${$('moh').value}`);
      if (active.includes('M')) parts.push(`M=${$('mom').value}`);
      if (active.includes('L')) parts.push(`L=${$('mol').value}`);
      leg.textContent = parts.join(' · ');
    }
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
      `<div class="shimrow">`+
      `<div class="sh" style="height:11px;width:40%"></div>`+
      `<div class="sh" style="height:9px;width:60%;margin-top:4px"></div>`+
      `</div>`
    ).join('');
    $('rcnt').textContent = '…';
    activeIdx = -1;

    try {
      const mo = $('moen').checked;
      const tr = getSelTrends();
      const params = {
        direction: getDir(), interval: getScanTF(),
        order:         $('order').value,
        zone_lookback: $('zlb').value,
        legout_mult:   $('legout').value,
        strategy:      $('strat').value,
        multi_order:   mo,
        order_low:     $('mol').value,
        order_mid:     $('mom').value,
        order_high:    $('moh').value,
        trend_low:     $('molt').value,
        trend_mid:     $('momt').value,
        trend_high:    $('moht').value,
      };
      if (tr) params.trend_filter = tr;

      const d = await API.scan(params);
      rows = d.results || [];
      $('rcnt').textContent = rows.length;
      QS.setNavStatus(`${rows.length} found`, `${rows.length} setups`);
      _buildList();
      _saveState();
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
      list.innerHTML =
        `<div class="empty-st">`+
        `<div class="e-ico">◈</div>`+
        `<div class="e-txt">No zones found</div>`+
        `<div class="e-sub">Broaden trend filter or increase lookback</div>`+
        `</div>`;
      return;
    }
    list.innerHTML = rows.map((r, i) => {
      const z  = ZT[r.zone_type] || ZT.rbr;
      const pr = QS.inr(r.close);
      const ac = activeIdx === i ? ' on' : '';
      return `<div class="sr${ac}" data-i="${i}" onclick="Scanner.selStock(${i})">
        <div>
          <div class="srn">${r.ticker.replace('.NS','')}</div>
          <div class="srm">
            <span class="tbadge ${QS.trendClass(r.trend)}">${QS.trendLabel(r.trend)}</span>
            <span style="font-family:var(--mono);font-size:9px">ATR ${r.atr_pct}%</span>
          </div>
        </div>
        <div class="srp">${pr}</div>
        <div class="srz">
          <span class="sra">${r.bars_ago}b</span>
          <span class="srt" style="background:${z.bg};color:${z.c};border:1px solid ${z.b}">
            ${(r.zone_type||'rbr').toUpperCase()}
          </span>
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
    sortKey = k;
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
    const row = rows[i];
    if (!row) return;

    // Skip if same stock + same TF already showing
    if (currentRow && currentRow.ticker === row.ticker && currentRow._tf === chartTF) {
      _markActive(i);
      return;
    }

    _markActive(i);
    currentRow = { ...row, _tf: chartTF };

    // Update header immediately (feels fast)
    $('cptk').textContent = row.ticker.replace('.NS','');
    $('cppr').textContent = QS.inr(row.close);
    const te = $('cptr');
    te.textContent   = QS.trendLabel(row.trend);
    te.className     = 'tbadge ' + QS.trendClass(row.trend);
    te.style.display = 'inline-block';

    // Metric bar
    $('m1').textContent = row.atr_pct + '%';
    $('m2').textContent = row.trend_strength || '—';
    $('m3').textContent = row.zones_count ?? '—';
    $('m4').textContent = row.bars_ago + 'b';
    $('m5').textContent = row.pivot_count ?? '—';
    const vp = (row.vel_current || 0) >= 0;
    $('m6').textContent = `${vp?'+':''}${row.vel_current}`;
    $('m6').className   = 'mv ' + (vp ? 'pos' : 'neg');
    $('m7').textContent = (row.lr_ratio || 0) + '×';
    $('m7').className   = 'mv ' + ((row.lr_ratio || 1) >= 1 ? 'pos' : 'neg');
    $('m8').textContent = row.amp_avg || '—';

    await Chart.load(row.ticker, {
      interval:      chartTF,
      order:         $('order').value,
      legout_mult:   $('legout').value,
      strategy:      $('strat').value,
      zone_lookback: $('zlb').value,
      multi_order:   $('moen').checked,
      order_low:     $('mol').value,
      order_mid:     $('mom').value,
      order_high:    $('moh').value,
    }, row.direction || getDir(), row.zone_legin_ts);

    _saveState();
  }

  function setChartTF(tf, btn) {
    chartTF = tf;
    document.querySelectorAll('.tfg .tfb').forEach(b => b.classList.remove('on'));
    btn.classList.add('on');
    Chart.bustCache(tf);
    _updatePivotButtons();
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
    const vp      = (r.vel_current || 0) >= 0;
    const vs      = vp ? '+' : '';
    const ac      = r.vel_label === 'accelerating' ? '↑ Accel'
                  : r.vel_label === 'decelerating' ? '↓ Decel' : '→ Steady';
    const vw      = Math.min(Math.abs(r.vel_current||0) / Math.max(r.vel_avg||.001,.001) * 45, 45);
    const zt      = ZT[r.zone_type] || ZT.rbr;
    const zsc     = r.zone_status === 'fresh'  ? '#22c55e'
                  : r.zone_status === 'tested' ? '#f59e0b' : 'var(--muted2)';

    let moHtml = '';
    if (r.mo_low_trend) {
      const lvs  = [
        { l:'H', c:'moH', o:r.mo_order_high, t:r.mo_high_trend, s:r.mo_high_score },
        { l:'M', c:'moM', o:r.mo_order_mid,  t:r.mo_mid_trend,  s:r.mo_mid_score  },
        { l:'L', c:'moL', o:r.mo_order_low,  t:r.mo_low_trend,  s:r.mo_low_score  },
      ];
      const dots = '●'.repeat(r.mo_alignment||0) + '○'.repeat(3-(r.mo_alignment||0));
      moHtml = `<div>
        <div class="dwst">Multi-Order Trend
          <span style="color:var(--amber);font-size:9px;font-weight:400">${dots} ${r.mo_alignment}/3</span>
        </div>
        ${lvs.map(lv => {
          const col = QS.trendColor(lv.t||'sw');
          const p   = Math.round(((lv.s||0)+1)/2*100);
          return `<div class="mobr">
            <span class="molk ${lv.c}">${lv.l}(${lv.o})</span>
            <div class="mobi">
              <span class="mobt" style="color:${col}">${QS.trendLabel(lv.t)}</span>
              <div class="mobtr"><div class="mobtf" style="width:${p}%;background:${col}"></div></div>
            </div>
            <span class="mobp">${p}%</span>
          </div>`;
        }).join('')}
      </div>`;
    }

    return `${moHtml}
    <div>
      <div class="dwst">Zone Info</div>
      <div class="ig2">
        <div class="it"><div class="itl">Zone High</div><div class="itv">${QS.inr(r.zone_high)}</div></div>
        <div class="it"><div class="itl">Zone Low</div><div class="itv">${QS.inr(r.zone_low)}</div></div>
        <div class="it"><div class="itl">Type</div><div class="itv" style="color:${zt.c}">${(r.zone_type||'rbr').toUpperCase()}</div></div>
        <div class="it"><div class="itl">Status</div><div class="itv" style="color:${zsc}">${r.zone_status||'—'}</div></div>
        <div class="it"><div class="itl">Age</div><div class="itv">${r.bars_ago} bars</div></div>
        <div class="it"><div class="itl">Vol Score</div><div class="itv">${r.vol_score!=null?(r.vol_score*100).toFixed(0)+'%':'—'}</div></div>
      </div>
    </div>
    <div>
      <div class="dwst">Velocity</div>
      <div class="vb">
        <div class="vc"></div>
        <div class="vf" style="${vp
          ? `left:50%;width:${vw}%;background:rgba(34,197,94,.25)`
          : `right:50%;width:${vw}%;background:rgba(239,68,68,.25)`}"></div>
        <span class="vlbl" style="color:${vp?'#22c55e':'#ef4444'}">${vs}${r.vel_current} pts/bar · ${ac}</span>
      </div>
      <div class="ig2" style="margin-top:6px">
        <div class="it"><div class="itl">Avg Velocity</div><div class="itv">${r.vel_avg}</div></div>
        <div class="it"><div class="itl">Acceleration</div>
          <div class="itv" style="color:${r.vel_label==='accelerating'?'#22c55e':r.vel_label==='decelerating'?'#ef4444':'var(--text)'}">
            ${r.vel_label||'—'}
          </div>
        </div>
      </div>
    </div>
    <div>
      <div class="dwst">Amplitude</div>
      <div class="ig3">
        <div class="it"><div class="itl">Avg</div><div class="itv">${r.amp_avg}</div><div class="its">${r.amp_regime||'—'}</div></div>
        <div class="it"><div class="itl">Recent</div><div class="itv">${r.amp_recent}</div><div class="its">${(r.amp_variance||0).toFixed(1)}% var</div></div>
        <div class="it"><div class="itl">Regime</div>
          <div class="itv" style="font-size:10px;${r.amp_regime==='expanding'?'color:#22c55e':r.amp_regime==='contracting'?'color:#ef4444':''}">
            ${r.amp_regime||'—'}
          </div>
        </div>
      </div>
    </div>
    <div>
      <div class="dwst">Leg Ratio</div>
      <div class="ig3">
        <div class="it"><div class="itl">Ratio</div>
          <div class="itv" style="color:${(r.lr_ratio||1)>=1?'#22c55e':'#ef4444'}">${r.lr_ratio}×</div>
          <div class="its">${(r.lr_label||'').replace(/_/g,' ')}</div>
        </div>
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
     SESSION STATE
  ════════════════════════════════════════════════════════════ */
  function _saveState() {
    try {
      sessionStorage.setItem(SS_KEY, JSON.stringify({
        rows, activeIdx, chartTF,
        dir:      getDir(),
        tf:       getScanTF(),
        strategy: $('strat').value,
        order:    $('order').value,
        zlb:      $('zlb').value,
        legout:   $('legout').value,
        moEn:     $('moen').checked,
        sbCol:    $('sidebar').classList.contains('col'),
      }));
    } catch (_) {}
  }

  function _restoreState() {
    try {
      const s = JSON.parse(sessionStorage.getItem(SS_KEY) || 'null');
      if (!s) return;
      if (s.strategy) $('strat').value = s.strategy;
      if (s.order)    $('order').value  = s.order;
      if (s.zlb)      $('zlb').value    = s.zlb;
      if (s.legout)   $('legout').value = s.legout;
      if (s.moEn)   { $('moen').checked = true; toggleMO(true); }
      if (s.sbCol)    $('sidebar').classList.add('col');
      if (s.tf) $('tfc-row').querySelectorAll('.tfc').forEach(b =>
        b.classList.toggle('on', b.dataset.v === s.tf)
      );
      if (s.chartTF) {
        chartTF = s.chartTF;
        document.querySelectorAll('.tfg .tfb').forEach(b =>
          b.classList.toggle('on', b.textContent.toLowerCase() === chartTF)
        );
      }
      if (s.dir) {
        $('dir-seg').querySelectorAll('.seg-btn').forEach(b => {
          b.className = 'seg-btn';
          if (b.dataset.v === s.dir) b.className = 'seg-btn ' + s.dir;
        });
      }
      if (s.rows?.length) {
        rows = s.rows;
        $('rcnt').textContent = rows.length;
        QS.setNavStatus(`${rows.length} found`, `${rows.length} setups`);
        _buildList();
        if (s.activeIdx >= 0) setTimeout(() => selStock(s.activeIdx), 400);
      }
    } catch (_) {}
  }

  /* ════════════════════════════════════════════════════════════
     INIT
  ════════════════════════════════════════════════════════════ */
  function init() {
    QS.renderNav('scanner', 'nst', 'nbadge');

    // Trend dropdown — close on outside click
    document.addEventListener('click', () => {
      $('tdd').classList.remove('op');
      $('ttrig').classList.remove('op');
    });
    $('tdd').addEventListener('click', e => e.stopPropagation());
    document.querySelectorAll('.tc').forEach(cb =>
      cb.addEventListener('change', _updateTrendLabel)
    );

    // Live order input → button label sync
    ['order','moh','mom','mol'].forEach(id => {
      const el = $(id);
      if (!el) return;
      el.addEventListener('input',  _updatePivotButtons);
      el.addEventListener('change', _updatePivotButtons);
    });

    _updatePivotButtons();
    _restoreState();

    window.addEventListener('beforeunload', _saveState);
  }

  /* ── Public surface ─────────────────────────────────────── */
  return {
    init,
    runScan,
    selStock,
    sortBy,
    setChartTF,
    toggleSB,
    stp,
    setDir,
    setTFC,
    toggleTDD,
    allTrends,
    toggleMO,
    openDrawer,
    closeDrawer,
  };
})();

window.Scanner = Scanner;
document.addEventListener('DOMContentLoaded', Scanner.init);
