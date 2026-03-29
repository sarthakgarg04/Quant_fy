/* ═══════════════════════════════════════════════════════════════
   chart.js — LightweightCharts engine
   Consolidates: qs-shared.js chart factory + index.html inline
   chart functions + chart_fixes.js overrides.
   chart_fixes.js is no longer needed.

   Depends on: shared.js (QS.safeColor), api.js (API.trendData)
   Exports:    window.Chart (the chart controller object)
═══════════════════════════════════════════════════════════════ */

const Chart = (() => {

  /* ── State ──────────────────────────────────────────────── */
  let _chart    = null;   // LWC chart instance
  let _cs       = null;   // candlestick series
  let _xtra     = [];     // extra series (zones, pivot lines) — cleaned each load
  let _pivotSeries = {};  // { single|H|M|L: [series, ...] }
  let _ready    = false;
  let _broken   = false;
  let _seq      = 0;      // load sequence — prevents stale renders

  /* pivot visibility state — managed externally by scanner.js */
  let _activeLevels = new Set(['single']);

  /* pivot cache: "ticker|tf|order" → pivot array */
  const _cache = {};

  /* Zone type → base color (6-digit, safe for alpha concat) */
  const ZONE_COLORS = {
    rbr:           '#22c55e',
    dbr:           '#2dd4bf',
    dbd:           '#ef4444',
    rbd:           '#f97316',
    consolidation: '#a78bfa',
  };

  const MO_COLORS = {
    H: { dot: '#7c3aed', label: 'HIGH', cls: 'moH' },
    M: { dot: '#1d4ed8', label: 'MID',  cls: 'moM' },
    L: { dot: '#16a34a', label: 'LOW',  cls: 'moL' },
  };

  /* ═══════════════════════════════════════════════════════════
     Data sanitisation
     LWC throws "Value is null" or "Cannot parse color" on any
     null/NaN/bad-hex value. Sanitise BEFORE every setData call.
  ═══════════════════════════════════════════════════════════ */
  function _sanitise(arr) {
    if (!Array.isArray(arr) || arr.length === 0) return [];

    const clean = arr.filter(d => {
      if (d == null || d.time == null) return false;
      if ('open' in d)   // OHLCV candle
        return isFinite(d.open) && isFinite(d.high) &&
               isFinite(d.low)  && isFinite(d.close);
      return d.value != null && isFinite(d.value);
    });

    clean.sort((a, b) => a.time - b.time);

    // Deduplicate — keep last occurrence of duplicate timestamps
    const seen = new Set();
    const out  = [];
    for (let i = clean.length - 1; i >= 0; i--) {
      if (!seen.has(clean[i].time)) {
        seen.add(clean[i].time);
        out.unshift(clean[i]);
      }
    }
    return out;
  }

  function _sanitiseMarkers(markers) {
    if (!Array.isArray(markers)) return [];
    return markers
      .filter(m => m != null && m.time != null && isFinite(m.time))
      .sort((a, b) => a.time - b.time);
  }

  /* ═══════════════════════════════════════════════════════════
     init — create or repair chart instance
     Called before every loadChart.
     On healthy chart: only clears zone+pivot series, keeps cs.
     On broken chart:  tears down and rebuilds from scratch.
  ═══════════════════════════════════════════════════════════ */
  function init(containerId) {
    // Always clear extra series
    _xtra.forEach(s => { try { _chart && _chart.removeSeries(s); } catch (_) {} });
    _xtra = [];
    Object.values(_pivotSeries).flat().forEach(s => {
      try { _chart && _chart.removeSeries(s); } catch (_) {}
    });
    _pivotSeries = {};

    if (_ready && !_broken) return;   // chart+cs healthy — done

    // Tear down broken instance
    if (_chart) { try { _chart.remove(); } catch (_) {} }
    _chart = null; _cs = null; _ready = false; _broken = false;

    const wrap   = document.getElementById(containerId || 'cw');
    const loader = document.getElementById('cload');
    wrap.innerHTML = '';
    if (loader) wrap.appendChild(loader);

    _chart = LightweightCharts.createChart(wrap, {
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

    _cs = _chart.addCandlestickSeries({
      upColor:       '#22c55e', downColor:       '#ef4444',
      borderUpColor: '#22c55e', borderDownColor: '#ef4444',
      wickUpColor:   '#22c55e', wickDownColor:   '#ef4444',
    });

    new ResizeObserver(() => {
      if (_chart) _chart.applyOptions({
        width:  wrap.clientWidth,
        height: wrap.clientHeight || 500,
      });
    }).observe(wrap);

    _ready = true;
  }

  /* ═══════════════════════════════════════════════════════════
     load — main entry point
     ticker:    string
     params:    { interval, order, legout_mult, strategy,
                  zone_lookback, multi_order,
                  order_low, order_mid, order_high }
     direction: 'buy' | 'sell'
     activeLegInTs: unix timestamp of the active zone's legin
  ═══════════════════════════════════════════════════════════ */
  async function load(ticker, params, direction, activeLegInTs) {
    if (_broken) _ready = false;
    init();

    const seq = ++_seq;
    _showLoader(true);

    try {
      /* 1 — fetch OHLCV + zones */
      let d;
      try {
        d = await API.chartData(ticker, { ...params, multi_order: true });
      } catch (e) {
        console.warn('[chart] fetch failed:', ticker, e.message);
        return;
      }
      if (seq !== _seq) return;

      if (!d || d.error || !Array.isArray(d.candles) || d.candles.length === 0) {
        console.warn('[chart] no candle data:', ticker, d && d.error);
        return;
      }

      /* 2 — sanitise + set candles */
      const candles = _sanitise(d.candles);
      if (!candles.length) { console.warn('[chart] empty after sanitise:', ticker); return; }

      try {
        _cs.setData(candles);
      } catch (e) {
        console.error('[chart] cs.setData threw:', e.message);
        _broken = true;
        return;
      }

      /* 3 — zones (sync) */
      const dir   = direction || 'buy';
      const zones = dir === 'buy' ? (d.buy_zones || []) : (d.sell_zones || []);
      _renderZones(zones, candles, activeLegInTs);

      /* 4 — pivots (async — await before scroll) */
      await _renderPivots(d, ticker, params);
      if (seq !== _seq) return;

      /* 5 — scroll: latest candle at ~80% from left
             scrollToRealTime() → pin last bar to right edge
             scrollToPosition(+12) → shift left 12 bars → right padding */
      _chart.timeScale().scrollToRealTime();
      requestAnimationFrame(() => {
        if (seq !== _seq) return;
        try { _chart.timeScale().scrollToPosition(12, false); } catch (_) {}
      });

    } catch (e) {
      if (seq !== _seq) return;
      console.error('[chart] unexpected:', e);
      _broken = true;
    } finally {
      if (seq === _seq) _showLoader(false);
    }
  }

  /* ═══════════════════════════════════════════════════════════
     _renderPivots
  ═══════════════════════════════════════════════════════════ */
  async function _renderPivots(d, ticker, params) {
    const moEnabled = params.multi_order === true || params.multi_order === 'true';
    const seq = _seq;

    if (!moEnabled) {
      _pivotSeries['single'] = _addPivotSeries(d.pivots || [], '#3b82f6', 'single');
    } else {
      const OH = parseInt(params.order_high) || 20;
      const OM = parseInt(params.order_mid)  || 10;
      const OL = parseInt(params.order_low)  || 5;
      const tf = params.interval || '1d';

      const [pvH, pvM, pvL] = await Promise.all([
        _fetchPivots(ticker, tf, OH),
        _fetchPivots(ticker, tf, OM),
        _fetchPivots(ticker, tf, OL),
      ]);
      if (seq !== _seq) return;

      _pivotSeries['H'] = _addPivotSeries(pvH.length ? pvH : (d.pivots||[]), '#7c3aed', 'H');
      _pivotSeries['M'] = _addPivotSeries(pvM.length ? pvM : (d.pivots||[]), '#1d4ed8', 'M');
      _pivotSeries['L'] = _addPivotSeries(pvL.length ? pvL : (d.pivots||[]), '#16a34a', 'L');
    }

    _applyVisibility();
  }

  /* ═══════════════════════════════════════════════════════════
     _addPivotSeries — sanitise → addLineSeries → setData → setMarkers
  ═══════════════════════════════════════════════════════════ */
  function _addPivotSeries(pivots, color, levelKey) {
    if (!_chart) return [];

    const clean = _sanitise(
      (pivots || []).map(p => ({
        time:  p.time,
        value: p.value != null && isFinite(p.value) ? p.value : null,
        type:  p.type,
      }))
    ).filter(p => p.value != null);

    if (!clean.length) { _pivotSeries[levelKey] = []; return []; }

    let s;
    try {
      s = _chart.addLineSeries({
        color: 'transparent', lineWidth: 1,
        crosshairMarkerVisible: false,
        lastValueVisible: false, priceLineVisible: false,
      });
    } catch (e) {
      console.error('[pivots] addLineSeries failed:', e.message);
      _broken = true;
      return [];
    }

    try {
      s.setData(clean.map(p => ({ time: p.time, value: p.value })));
    } catch (e) {
      console.error('[pivots] setData failed:', levelKey, e.message);
      _broken = true;
      try { _chart.removeSeries(s); } catch (_) {}
      return [];
    }

    try {
      const markers = _sanitiseMarkers(clean.map(p => ({
        time:     p.time,
        position: p.type === 'T' ? 'aboveBar' : 'belowBar',
        color:    p.type === 'T' ? '#ef4444'  : '#22c55e',
        shape:    p.type === 'T' ? 'arrowDown': 'arrowUp',
        size:     1,
      })));
      if (markers.length) s.setMarkers(markers);
    } catch (e) {
      console.warn('[pivots] setMarkers failed:', levelKey, e.message);
    }

    _xtra.push(s);
    _pivotSeries[levelKey] = [s];
    return [s];
  }

  /* ═══════════════════════════════════════════════════════════
     _fetchPivots — cached, never throws
  ═══════════════════════════════════════════════════════════ */
  async function _fetchPivots(ticker, tf, order) {
    const key = `${ticker}|${tf}|${order}`;
    if (_cache[key]) return _cache[key];
    try {
      const data = await API.trendData(ticker, { interval: tf, order, days: 500 });
      _cache[key] = data.pivots || [];
      return _cache[key];
    } catch (_) {
      return [];
    }
  }

  /* ═══════════════════════════════════════════════════════════
     _renderZones — sanitised zone rendering
  ═══════════════════════════════════════════════════════════ */
  function _renderZones(zones, candles, activeLegInTs) {
    if (!_chart || !candles.length || !zones.length) return;

    const first = candles[0].time;
    const last  = candles[candles.length - 1].time;
    const bSec  = candles.length > 1
      ? last - candles[candles.length - 2].time : 86400;

    zones.forEach(z => {
      if (z.price_high == null || z.price_low == null)    return;
      if (!isFinite(z.price_high) || !isFinite(z.price_low)) return;
      if (z.price_high <= z.price_low) return;

      const ts0 = Math.max(z.time_start, first);
      const ts1 = Math.max(last + bSec * 5, z.time_end + bSec * 3);
      if (ts0 > last) return;

      const inv  = z.status === 'invalidated';
      const isA  = activeLegInTs && Math.abs(z.time_start - activeLegInTs) <= 86400;
      // Always 6-digit base colors so alpha concat gives valid 8-digit #RRGGBBAA
      const base = inv ? '#666666' : (ZONE_COLORS[z.zone_type] || '#22c55e');
      const lineColor  = QS.safeColor(base + (inv ? '55' : isA ? 'ff' : 'bb'));
      const fillColor  = QS.safeColor(base + (inv ? '0d' : isA ? '44' : '22'));
      const fillColor2 = QS.safeColor(base + '0d');

      try {
        const mkLine = price => {
          const s = _chart.addLineSeries({
            color: lineColor, lineWidth: isA ? 2 : 1, lineStyle: inv ? 2 : 0,
            crosshairMarkerVisible: false, lastValueVisible: false, priceLineVisible: false,
          });
          const pts = _sanitise([{ time: ts0, value: price }, { time: ts1, value: price }]);
          if (pts.length) s.setData(pts);
          _xtra.push(s);
        };
        mkLine(z.price_high);
        mkLine(z.price_low);

        const fill = _chart.addBaselineSeries({
          baseValue:        { type: 'price', price: z.price_low },
          topLineColor:     'transparent',
          topFillColor1:    fillColor,
          topFillColor2:    fillColor2,
          bottomLineColor:  'transparent',
          bottomFillColor1: 'transparent',
          bottomFillColor2: 'transparent',
          lineWidth: 0,
          crosshairMarkerVisible: false, lastValueVisible: false, priceLineVisible: false,
        });
        const fp = _sanitise([{ time: ts0, value: z.price_high }, { time: ts1, value: z.price_high }]);
        if (fp.length) fill.setData(fp);
        _xtra.push(fill);
      } catch (e) {
        console.warn('[zones] render error:', e.message);
      }
    });
  }

  /* ═══════════════════════════════════════════════════════════
     Pivot visibility toggle (called by scanner.js)
  ═══════════════════════════════════════════════════════════ */
  function setActiveLevels(levels) {
    _activeLevels = new Set(levels);
    _applyVisibility();
  }

  function toggleLevel(level) {
    if (_activeLevels.has(level)) {
      if (_activeLevels.size === 1) return;   // don't deselect last
      _activeLevels.delete(level);
    } else {
      _activeLevels.add(level);
    }
    _applyVisibility();
    return [..._activeLevels];
  }

  function _applyVisibility() {
    Object.entries(_pivotSeries).forEach(([level, seriesArr]) => {
      const visible = _activeLevels.has(level);
      (seriesArr || []).forEach(s => {
        try { s.applyOptions({ visible }); } catch (_) {}
      });
    });
  }

  /* ── Cache management ───────────────────────────────────── */
  function bustCache(tf) {
    Object.keys(_cache).forEach(k => {
      if (!tf || k.split('|')[1] === tf) delete _cache[k];
    });
  }

  /* ── Loader helper ──────────────────────────────────────── */
  function _showLoader(show) {
    const el = document.getElementById('cload');
    if (el) el.classList.toggle('show', show);
  }

  /* ── Scroll ─────────────────────────────────────────────── */
  function scrollToLatest() {
    if (!_chart) return;
    try {
      _chart.timeScale().scrollToRealTime();
      requestAnimationFrame(() => {
        try { _chart.timeScale().scrollToPosition(12, false); } catch (_) {}
      });
    } catch (_) {}
  }

  /* ── Public surface ─────────────────────────────────────── */
  return {
    init,
    load,
    toggleLevel,
    setActiveLevels,
    bustCache,
    scrollToLatest,
    get activeLevels() { return [..._activeLevels]; },
    get moColors() { return MO_COLORS; },
  };
})();

window.Chart = Chart;
