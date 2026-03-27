/* ═══════════════════════════════════════════════════════════════
   chart_fixes.js  v5
   Save to frontend/chart_fixes.js
   Add before </body> in index.html:
     <script src="/chart_fixes.js"></script>

   Root cause fixed in v5:
     LightweightCharts throws "Value is null" when ANY data point
     in setData() or setMarkers() has null/undefined/NaN value.
     This crashes the chart instance permanently until page reload.

   Fixes:
     1. Strict data sanitisation before EVERY setData / setMarkers call
     2. chart instance recreated if it enters error state
     3. Correct scroll (latest candle at 80% from left)
     4. Dynamic pivot order buttons from live sidebar inputs
═══════════════════════════════════════════════════════════════ */
(function () {
  'use strict';

  const $ = id => document.getElementById(id);

  /* ─── chart lifecycle state ─────────────────────────────── */
  let _ready  = false;   // chart + cs created at least once
  let _broken = false;   // chart entered error state — must recreate
  let _seq    = 0;       // load sequence counter

  /* ═══════════════════════════════════════════════════════════
     sanitise  —  the single most important function in this file.

     LightweightCharts requirements for setData():
       • Array must be sorted strictly ascending by .time
       • No duplicate .time values
       • .value / .open / .high / .low / .close must be finite numbers
       • No null, undefined, NaN, Infinity

     If ANY element violates these rules the library throws
     "Value is null" and corrupts the chart instance.
  ═══════════════════════════════════════════════════════════ */
  function sanitiseData(arr) {
    if (!Array.isArray(arr)) return [];

    // 1. Filter out rows with null/undefined/NaN in any numeric field
    const clean = arr.filter(d => {
      if (d == null || d.time == null) return false;
      // OHLCV candle
      if ('open' in d) {
        return isFinite(d.open) && isFinite(d.high) &&
               isFinite(d.low)  && isFinite(d.close);
      }
      // Line / pivot
      return d.value != null && isFinite(d.value);
    });

    // 2. Sort ascending by time
    clean.sort((a, b) => a.time - b.time);

    // 3. Remove duplicate timestamps (keep last occurrence)
    const seen = new Set();
    const deduped = [];
    for (let i = clean.length - 1; i >= 0; i--) {
      if (!seen.has(clean[i].time)) {
        seen.add(clean[i].time);
        deduped.unshift(clean[i]);
      }
    }
    return deduped;
  }

  function sanitiseMarkers(markers) {
    if (!Array.isArray(markers)) return [];
    return markers
      .filter(m => m != null && m.time != null && isFinite(m.time))
      .sort((a, b) => a.time - b.time);
  }

  /* ─── safeColor ─────────────────────────────────────────────
     LightweightCharts only accepts:
       #RRGGBB  (7 chars)   or   #RRGGBBAA  (9 chars)
       or the literals: 'transparent', 'white', 'black'
     3-digit shorthand (#666) + alpha suffix ('55') = '#66655'
     which is 5 chars and throws "Cannot parse color".
     This function normalises before anything reaches LWC.
  ─────────────────────────────────────────────────────────── */
  function safeColor(c, fallback) {
    if (fallback === undefined) fallback = '#888888';
    if (typeof c !== 'string' || c === '') return fallback;
    c = c.trim();
    if (c === 'transparent' || c === 'white' || c === 'black') return c;
    // Expand 3-digit #RGB → #RRGGBB
    if (/^#[0-9a-fA-F]{3}$/.test(c))
      return '#' + c[1]+c[1] + c[2]+c[2] + c[3]+c[3];
    // Expand 4-digit #RGBA → #RRGGBBAA
    if (/^#[0-9a-fA-F]{4}$/.test(c))
      return '#' + c[1]+c[1] + c[2]+c[2] + c[3]+c[3] + c[4]+c[4];
    // Already valid 6 or 8 digit
    if (/^#[0-9a-fA-F]{6}([0-9a-fA-F]{2})?$/.test(c)) return c;
    console.warn('[safeColor] invalid:', c, '→', fallback);
    return fallback;
  }

  /* ═══════════════════════════════════════════════════════════
     initChart  —  creates chart+cs, or repairs a broken one.
     On every call: clears zone + pivot series from prior stock.
  ═══════════════════════════════════════════════════════════ */
  function initChart() {
    // Clean extra series (zones + pivot lines)
    if (xtra && xtra.length) {
      xtra.forEach(s => { try { chart && chart.removeSeries(s); } catch (_) {} });
      xtra = [];
    }
    if (pivotSeries) {
      Object.values(pivotSeries).flat().forEach(s => {
        try { chart && chart.removeSeries(s); } catch (_) {}
      });
      pivotSeries = {};
    }

    // If chart is healthy, nothing more to do
    if (_ready && !_broken) return;

    // Need to (re)create — tear down old instance if present
    if (chart) {
      try { chart.remove(); } catch (_) {}
      chart = null; cs = null;
    }
    _ready  = false;
    _broken = false;

    const wrap   = $('cw');
    const loader = $('cload');
    wrap.innerHTML = '';
    wrap.appendChild(loader);

    chart = LightweightCharts.createChart(wrap, {
      width:  wrap.clientWidth,
      height: wrap.clientHeight || 500,
      layout: { background: { color: '#0d0f12' }, textColor: '#8892a0' },
      grid:   { vertLines: { color: '#1a1f28' }, horzLines: { color: '#1a1f28' } },
      crosshair:       { mode: LightweightCharts.CrosshairMode.Normal },
      rightPriceScale: { borderColor: '#2a3040' },
      timeScale: {
        borderColor:    '#2a3040',
        timeVisible:    true,
        secondsVisible: false,
        rightOffset:    12,
      },
    });

    cs = chart.addCandlestickSeries({
      upColor:       '#22c55e', downColor:       '#ef4444',
      borderUpColor: '#22c55e', borderDownColor: '#ef4444',
      wickUpColor:   '#22c55e', wickDownColor:   '#ef4444',
    });

    new ResizeObserver(() => {
      if (chart) chart.applyOptions({
        width:  wrap.clientWidth,
        height: wrap.clientHeight || 500,
      });
    }).observe(wrap);

    _ready = true;
  }

  /* ═══════════════════════════════════════════════════════════
     loadChart  —  main entry point called on stock click
  ═══════════════════════════════════════════════════════════ */
  async function loadChart(row) {
    // If chart broke on last load, recreate it cleanly
    if (_broken) _ready = false;
    initChart();

    const seq = ++_seq;
    $('cload').classList.add('show');

    try {
      /* 1 — fetch OHLCV + zones */
      const url =
        `${API}/chart/${encodeURIComponent(row.ticker)}`
        + `?interval=${chartTF}`
        + `&order=${$('order').value}`
        + `&legout_mult=${$('legout').value}`
        + `&strategy=${$('strat').value}`
        + `&zone_lookback=${$('zlb').value}`
        + `&multi_order=true`
        + `&order_low=${$('mol').value}`
        + `&order_mid=${$('mom').value}`
        + `&order_high=${$('moh').value}`;

      let d;
      try {
        const res = await fetch(url);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        d = await res.json();
      } catch (e) {
        console.warn('[chart] fetch failed:', row.ticker, e.message);
        return;
      }
      if (seq !== _seq) return;

      if (!d || d.error || !Array.isArray(d.candles) || d.candles.length === 0) {
        console.warn('[chart] no candle data for', row.ticker);
        return;
      }

      /* 2 — sanitise + set candles */
      const candles = sanitiseData(d.candles);
      if (candles.length === 0) {
        console.warn('[chart] candles empty after sanitise:', row.ticker);
        return;
      }

      try {
        cs.setData(candles);
      } catch (e) {
        // Chart is now broken — mark and bail
        console.error('[chart] cs.setData threw — marking chart broken:', e.message);
        _broken = true;
        return;
      }

      /* 3 — zones (sync) */
      const dir   = row.direction || getDir();
      const zones = dir === 'buy' ? (d.buy_zones || []) : (d.sell_zones || []);
      renderZones(zones, candles, row.zone_legin_ts);

      /* 4 — pivots (async, must complete before scroll) */
      await _renderPivots(d, row, seq);
      if (seq !== _seq) return;

      /* 5 — scroll: latest candle at ~80% from left
             scrollToRealTime() → last bar at right edge
             scrollToPosition(+12) → shift LEFT 12 bars → breathing room */
      chart.timeScale().scrollToRealTime();
      requestAnimationFrame(() => {
        if (seq !== _seq) return;
        try { chart.timeScale().scrollToPosition(12, false); } catch (_) {}
      });

    } catch (unexpected) {
      if (seq !== _seq) return;
      console.error('[chart] unexpected error:', unexpected);
      _broken = true;
    } finally {
      if (seq === _seq) $('cload').classList.remove('show');
    }
  }

  /* ═══════════════════════════════════════════════════════════
     _renderPivots  —  fetch pivot data, sanitise, add to chart
  ═══════════════════════════════════════════════════════════ */
  async function _renderPivots(d, row, seq) {
    const moEnabled = $('moen').checked;

    if (!moEnabled) {
      _addPivotSeries(d.pivots || [], '#3b82f6', 'single');
      return;
    }

    const OH = parseInt($('moh').value) || 20;
    const OM = parseInt($('mom').value) || 10;
    const OL = parseInt($('mol').value) || 5;

    const [pvH, pvM, pvL] = await Promise.all([
      _fetchPivots(row.ticker, chartTF, OH),
      _fetchPivots(row.ticker, chartTF, OM),
      _fetchPivots(row.ticker, chartTF, OL),
    ]);

    if (seq !== _seq) return;

    // Fall back to primary pivots if a level returned nothing
    const fallback = d.pivots || [];
    _addPivotSeries(pvH.length ? pvH : fallback, '#7c3aed', 'H');
    _addPivotSeries(pvM.length ? pvM : fallback, '#1d4ed8', 'M');
    _addPivotSeries(pvL.length ? pvL : fallback, '#16a34a', 'L');

    applyPivotVisibility();
  }

  /* ═══════════════════════════════════════════════════════════
     _addPivotSeries  —  sanitise → addLineSeries → setData → setMarkers
     Each step wrapped so one bad pivot set can't kill the chart.
  ═══════════════════════════════════════════════════════════ */
  function _addPivotSeries(pivots, color, levelKey) {
    if (!chart) return [];

    // Sanitise pivot data — this is what prevents "Value is null"
    const clean = sanitiseData(
      (pivots || []).map(p => ({
        time:  p.time,
        value: p.value != null && isFinite(p.value) ? p.value : null,
        type:  p.type,
      }))
    ).filter(p => p.value != null);   // final null guard

    if (clean.length === 0) {
      pivotSeries[levelKey] = [];
      return [];
    }

    let s;
    try {
      s = chart.addLineSeries({
        color:                  'transparent',
        lineWidth:              1,
        crosshairMarkerVisible: false,
        lastValueVisible:       false,
        priceLineVisible:       false,
      });
    } catch (e) {
      console.error('[pivots] addLineSeries failed:', e.message);
      _broken = true;
      return [];
    }

    try {
      s.setData(clean.map(p => ({ time: p.time, value: p.value })));
    } catch (e) {
      console.error('[pivots] setData failed:', levelKey, e.message,
                    '| sample:', clean.slice(0, 3));
      _broken = true;
      try { chart.removeSeries(s); } catch (_) {}
      return [];
    }

    try {
      const markers = sanitiseMarkers(
        clean.map(p => ({
          time:     p.time,
          position: p.type === 'T' ? 'aboveBar' : 'belowBar',
          color:    p.type === 'T' ? '#ef4444'  : '#22c55e',
          shape:    p.type === 'T' ? 'arrowDown' : 'arrowUp',
          size:     1,
        }))
      );
      if (markers.length) s.setMarkers(markers);
    } catch (e) {
      // Markers failed but series+data are fine — not fatal
      console.warn('[pivots] setMarkers failed:', levelKey, e.message);
    }

    xtra.push(s);
    pivotSeries[levelKey] = [s];
    return [s];
  }

  /* ═══════════════════════════════════════════════════════════
     _fetchPivots  —  cached, never throws (returns [] on error)
  ═══════════════════════════════════════════════════════════ */
  async function _fetchPivots(ticker, tf, order) {
    const key = `${ticker}|${tf}|${order}`;
    if (_pivotCache[key]) return _pivotCache[key];
    try {
      const r = await fetch(
        `${API}/trend/${encodeURIComponent(ticker)}?interval=${tf}&order=${order}&days=500`
      );
      if (!r.ok) return [];
      const data = await r.json();
      _pivotCache[key] = data.pivots || [];
      return _pivotCache[key];
    } catch (_) {
      return [];
    }
  }

  /* ═══════════════════════════════════════════════════════════
     renderZones  —  sanitised zone rendering
     (replaces original so null zone prices can't crash chart)
  ═══════════════════════════════════════════════════════════ */
  function renderZones(zones, candles, activeLegInTs) {
    if (!chart || !candles.length || !zones.length) return;

    const first = candles[0].time;
    const last  = candles[candles.length - 1].time;
    const bSec  = candles.length > 1
      ? last - candles[candles.length - 2].time
      : 86400;

    const ZC = {
      rbr:           '#22c55e', dbr: '#2dd4bf',
      dbd:           '#ef4444', rbd: '#f97316',
      consolidation: '#a78bfa',
    };

    zones.forEach(z => {
      // Guard against null prices — this is another source of "Value is null"
      if (z.price_high == null || z.price_low == null) return;
      if (!isFinite(z.price_high) || !isFinite(z.price_low)) return;
      if (z.price_high <= z.price_low) return;

      const ts0 = Math.max(z.time_start, first);
      const ts1 = Math.max(last + bSec * 5, z.time_end + bSec * 3);
      if (ts0 > last) return;

      const inv  = z.status === 'invalidated';
      const isA  = activeLegInTs && Math.abs(z.time_start - activeLegInTs) <= 86400;
      // IMPORTANT: always use 6-digit hex so appending 2-char alpha gives
      // a valid 8-digit #RRGGBBAA. 3-digit shorthand (#666) + '55' = '#66655' → invalid.
      const col  = inv ? '#666666' : (ZC[z.zone_type] || '#22c55e');
      // safeColor() expands any shorthand hex and validates before LWC sees it
      const lineColor  = safeColor(col + (inv ? '55' : isA ? 'ff' : 'bb'));
      const fillColor  = safeColor(col + (inv ? '0d' : isA ? '44' : '22'));
      const fillColor2 = safeColor(col + '0d');

      try {
        const mkLine = (price) => {
          const s = chart.addLineSeries({
            color:                  lineColor,
            lineWidth:              isA ? 2 : 1,
            lineStyle:              inv ? 2 : 0,
            crosshairMarkerVisible: false,
            lastValueVisible:       false,
            priceLineVisible:       false,
          });
          const pts = sanitiseData([
            { time: ts0, value: price },
            { time: ts1, value: price },
          ]);
          if (pts.length) s.setData(pts);
          xtra.push(s);
          return s;
        };

        mkLine(z.price_high);
        mkLine(z.price_low);

        const fill = chart.addBaselineSeries({
          baseValue:        { type: 'price', price: z.price_low },
          topLineColor:     'transparent',
          topFillColor1:    fillColor,
          topFillColor2:    fillColor2,
          bottomLineColor:  'transparent',
          bottomFillColor1: 'transparent',
          bottomFillColor2: 'transparent',
          lineWidth:                0,
          crosshairMarkerVisible:   false,
          lastValueVisible:         false,
          priceLineVisible:         false,
        });
        const fillPts = sanitiseData([
          { time: ts0, value: z.price_high },
          { time: ts1, value: z.price_high },
        ]);
        if (fillPts.length) fill.setData(fillPts);
        xtra.push(fill);

      } catch (e) {
        console.warn('[zones] render error:', e.message);
      }
    });
  }

  /* ═══════════════════════════════════════════════════════════
     updatePivotOrderButtons  —  live labels from sidebar inputs
  ═══════════════════════════════════════════════════════════ */
  function updatePivotOrderButtons() {
    const moEnabled = $('moen').checked;
    const container = $('pivot-order-btns');
    container.innerHTML = '';
    activePivotLevels   = new Set();

    if (!moEnabled) {
      const order = $('order').value;
      const btn   = document.createElement('button');
      btn.className     = 'pob on';
      btn.dataset.level = 'single';
      btn.innerHTML =
        `<span class="pob-dot" style="background:#3b82f6"></span>Order ${order}`;
      btn.onclick = () => togglePivotLevel('single', btn);
      container.appendChild(btn);
      activePivotLevels.add('single');
    } else {
      const orders = {
        H: parseInt($('moh').value) || 20,
        M: parseInt($('mom').value) || 10,
        L: parseInt($('mol').value) || 5,
      };
      ['H', 'M', 'L'].forEach(lv => {
        const mc  = MO_COLORS[lv];
        const btn = document.createElement('button');
        btn.className     = `pob on ${mc.cls}`;
        btn.dataset.level = lv;
        btn.innerHTML =
          `<span class="pob-dot" style="background:${mc.dot}"></span>`+
          `${mc.label}(${orders[lv]})`;
        btn.onclick = () => togglePivotLevel(lv, btn);
        container.appendChild(btn);
        activePivotLevels.add(lv);
      });
    }
    updatePivotLegend();
  }

  /* ═══════════════════════════════════════════════════════════
     stp / setChTF / runScan  —  utility overrides
  ═══════════════════════════════════════════════════════════ */
  function stp(id, dv) {
    const el   = $(id);
    const step = parseFloat(el.step) || 1;
    el.value   = step < 1
      ? parseFloat((parseFloat(el.value) + dv).toFixed(2))
      : Math.round(parseFloat(el.value) + dv);
    if (['order','moh','mom','mol'].includes(id)) updatePivotOrderButtons();
  }

  function setChTF(tf, btn) {
    chartTF = tf;
    document.querySelectorAll('.tfg .tfb').forEach(b => b.classList.remove('on'));
    btn.classList.add('on');
    Object.keys(_pivotCache).forEach(k => {
      if (k.split('|')[1] === tf) delete _pivotCache[k];
    });
    updatePivotOrderButtons();
    if (currentRow) { currentRow._tf = tf; loadChart(currentRow); }
  }

  async function runScan() {
    updatePivotOrderButtons();
    const btn = $('scanbtn');
    btn.disabled = true; btn.textContent = '⏳  Scanning…';
    $('nst').textContent = 'scanning…';
    $('rlist').innerHTML = Array(6).fill(0).map(() =>
      `<div class="shrow">`+
      `<div class="sh" style="height:11px;width:40%"></div>`+
      `<div class="sh" style="height:9px;width:60%;margin-top:4px"></div>`+
      `</div>`
    ).join('');
    $('rcnt').textContent = '…';
    activeIdx = -1;
    try {
      const mo = $('moen').checked;
      const tr = getSelTrends();
      const p  = new URLSearchParams({
        direction: getDir(), interval: getTF(),
        order: $('order').value, zone_lookback: $('zlb').value,
        legout_mult: $('legout').value, strategy: $('strat').value,
        multi_order: mo,
        order_low: $('mol').value, order_mid: $('mom').value,
        order_high: $('moh').value,
        trend_low: $('molt').value, trend_mid: $('momt').value,
        trend_high: $('moht').value,
      });
      if (tr) p.set('trend_filter', tr);
      const r = await fetch(`${API}/scan?${p}`);
      const d = await r.json();
      rows = d.results || [];
      $('rcnt').textContent     = rows.length;
      $('nbadge').style.display = 'block';
      $('nbadge').textContent   = `${rows.length} setups`;
      $('nst').textContent      = `${rows.length} found`;
      buildList();
      saveState();
    } catch (e) {
      $('rlist').innerHTML =
        `<div class="empty-st"><span style="color:var(--red)">⚠ ${e.message}</span></div>`;
      $('nst').textContent = 'error';
    } finally {
      btn.disabled = false; btn.textContent = '▶  Run Scan';
    }
  }

  /* ── expose to global scope ─────────────────────────────── */
  window.initChart               = initChart;
  window.loadChart               = loadChart;
  window.renderZones             = renderZones;
  window.updatePivotOrderButtons = updatePivotOrderButtons;
  window.stp                     = stp;
  window.setChTF                 = setChTF;
  window.runScan                 = runScan;

  /* ── wire live order input → button label sync ──────────── */
  ['order','moh','mom','mol'].forEach(id => {
    const el = $(id);
    if (!el) return;
    el.addEventListener('input',  updatePivotOrderButtons);
    el.addEventListener('change', updatePivotOrderButtons);
  });

  /* ── defer initial label render one tick so restoreState runs first ── */
  setTimeout(updatePivotOrderButtons, 0);

})();