/* ═══════════════════════════════════════════════════════════════
   chart.js — LightweightCharts engine  v3.4
   ─────────────────────────────────────────────────────────────
   New in v3.4:
   • Volume histogram rendered as a separate price scale on the
     right side of the chart (same panel, bottom 20% of height),
     using LWC's built-in histogram series with priceScaleId:'vol'.
     Up bars: green (same colour as bullish candles, 50% opacity).
     Down bars: red (same as bearish, 50% opacity).
     Volume scale is hidden (no labels) so it doesn't clutter.

   Depends on: shared.js (QS.safeColor), api.js (API.trendData)
═══════════════════════════════════════════════════════════════ */

const Chart = (() => {

  /* ── State ──────────────────────────────────────────────── */
  let _chart    = null;
  let _cs       = null;       // candlestick series
  let _vol      = null;       // volume histogram series
  let _xtra     = [];         // zone + pivot series — cleared each load
  let _pivotSeries = {};
  let _ready    = false;
  let _broken   = false;
  let _seq      = 0;

  let _activeLevels = new Set(['single']);

  const _cache = {};   // pivot cache keyed "ticker|tf|order"

  /* Zone type → base 6-digit hex color */
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

  /* ── Data sanitisation ────────────────────────────────────── */
  function _sanitise(arr) {
    if (!Array.isArray(arr) || !arr.length) return [];
    const clean = arr.filter(d => {
      if (d == null || d.time == null) return false;
      if ('open' in d)
        return isFinite(d.open) && isFinite(d.high) && isFinite(d.low) && isFinite(d.close);
      return d.value != null && isFinite(d.value);
    });
    clean.sort((a, b) => a.time - b.time);
    const seen = new Set(); const out = [];
    for (let i = clean.length - 1; i >= 0; i--) {
      if (!seen.has(clean[i].time)) { seen.add(clean[i].time); out.unshift(clean[i]); }
    }
    return out;
  }

  function _sanitiseMarkers(markers) {
    if (!Array.isArray(markers)) return [];
    return markers.filter(m => m != null && m.time != null && isFinite(m.time))
                  .sort((a, b) => a.time - b.time);
  }

  /* ── init ─────────────────────────────────────────────────── */
  function init(containerId) {
    // Clear extra series
    _xtra.forEach(s => { try { _chart && _chart.removeSeries(s); } catch(_){} });
    _xtra = [];
    Object.values(_pivotSeries).flat().forEach(s => {
      try { _chart && _chart.removeSeries(s); } catch(_) {}
    });
    _pivotSeries = {};

    if (_ready && !_broken) return;

    if (_chart) { try { _chart.remove(); } catch(_){} }
    _chart = null; _cs = null; _vol = null; _ready = false; _broken = false;

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

    /* Candlestick series */
    _cs = _chart.addCandlestickSeries({
      upColor:       '#22c55e', downColor:       '#ef4444',
      borderUpColor: '#22c55e', borderDownColor: '#ef4444',
      wickUpColor:   '#22c55e', wickDownColor:   '#ef4444',
    });

    /* Volume histogram series
       priceScaleId:'vol' creates a separate scale.
       scaleMargins keeps it in the bottom 20% of the chart area
       without splitting the pane.                               */
    _vol = _chart.addHistogramSeries({
      priceFormat:    { type: 'volume' },
      priceScaleId:   'vol',
      color:          'rgba(34,197,94,0.5)',
      lastValueVisible: false,
      priceLineVisible: false,
    });
    _chart.priceScale('vol').applyOptions({
      scaleMargins: { top: 0.80, bottom: 0.00 },
      visible:      false,    // hide the volume price axis labels
    });

    new ResizeObserver(() => {
      if (_chart) _chart.applyOptions({
        width:  wrap.clientWidth,
        height: wrap.clientHeight || 500,
      });
    }).observe(wrap);

    _ready = true;
  }

  /* ── load ─────────────────────────────────────────────────── */
  async function load(ticker, params, direction, activeLegInTs) {
    if (_broken) _ready = false;
    init();

    const seq = ++_seq;
    _showLoader(true);

    try {
      let d;
      try {
        d = await API.chartData(ticker, { ...params, multi_order: true });
      } catch (e) {
        console.warn('[chart] fetch failed:', ticker, e.message);
        return;
      }
      if (seq !== _seq) return;

      if (!d || d.error || !Array.isArray(d.candles) || !d.candles.length) {
        console.warn('[chart] no candle data:', ticker, d?.error);
        return;
      }

      /* ── 1. Candles ────────────────────────────────────────── */
      const candles = _sanitise(d.candles);
      if (!candles.length) { console.warn('[chart] empty after sanitise'); return; }

      try { _cs.setData(candles); }
      catch (e) { console.error('[chart] cs.setData:', e.message); _broken = true; return; }

      /* ── 2. Volume bars ────────────────────────────────────── */
      if (_vol) {
        const volData = _sanitise(
          candles.map(c => ({
            time:  c.time,
            value: c.volume || 0,
            // Colour bar based on candle direction
            color: c.close >= c.open
              ? 'rgba(34,197,94,0.45)'    // green — up candle
              : 'rgba(239,68,68,0.45)',   // red   — down candle
          }))
        );
        try { _vol.setData(volData); } catch(e) { console.warn('[chart] vol.setData:', e.message); }
      }

      /* ── 3. Zones ──────────────────────────────────────────── */
      const dir   = direction || 'buy';
      const zones = dir === 'buy' ? (d.buy_zones||[]) : (d.sell_zones||[]);
      _renderZones(zones, candles, activeLegInTs);

      /* ── 4. Pivots ─────────────────────────────────────────── */
      await _renderPivots(d, ticker, params);
      if (seq !== _seq) return;

      /* ── 5. Scroll to latest ───────────────────────────────── */
      _chart.timeScale().scrollToRealTime();
      requestAnimationFrame(() => {
        if (seq !== _seq) return;
        try { _chart.timeScale().scrollToPosition(12, false); } catch(_) {}
      });

    } catch (e) {
      if (seq !== _seq) return;
      console.error('[chart] unexpected:', e);
      _broken = true;
    } finally {
      if (seq === _seq) _showLoader(false);
    }
  }

  /* ── _renderPivots ────────────────────────────────────────── */
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

  /* ── _addPivotSeries ─────────────────────────────────────── */
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
      console.error('[pivots] addLineSeries:', e.message);
      _broken = true; return [];
    }

    try { s.setData(clean.map(p => ({ time: p.time, value: p.value }))); }
    catch (e) {
      console.error('[pivots] setData:', levelKey, e.message);
      _broken = true;
      try { _chart.removeSeries(s); } catch(_) {}
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
    } catch (e) { console.warn('[pivots] setMarkers:', levelKey, e.message); }

    _xtra.push(s);
    _pivotSeries[levelKey] = [s];
    return [s];
  }

  /* ── _fetchPivots ────────────────────────────────────────── */
  async function _fetchPivots(ticker, tf, order) {
    const key = `${ticker}|${tf}|${order}`;
    if (_cache[key]) return _cache[key];
    try {
      const data = await API.trendData(ticker, { interval: tf, order, days: 500 });
      _cache[key] = data.pivots || [];
      return _cache[key];
    } catch(_) { return []; }
  }

  /* ── _renderZones ────────────────────────────────────────── */
  function _renderZones(zones, candles, activeLegInTs) {
    if (!_chart || !candles.length || !zones.length) return;

    const first = candles[0].time;
    const last  = candles[candles.length - 1].time;
    const bSec  = candles.length > 1
      ? last - candles[candles.length - 2].time : 86400;

    zones.forEach(z => {
      if (z.price_high == null || z.price_low == null) return;
      if (!isFinite(z.price_high) || !isFinite(z.price_low)) return;
      if (z.price_high <= z.price_low) return;

      const ts0  = Math.max(z.time_start, first);
      const ts1  = Math.max(last + bSec * 5, z.time_end + bSec * 3);
      if (ts0 > last) return;

      const inv  = z.status === 'invalidated';
      const isA  = activeLegInTs && Math.abs(z.time_start - activeLegInTs) <= 86400;
      const base  = inv ? '#666666' : (ZONE_COLORS[z.zone_type] || '#22c55e');
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
        const fp = _sanitise([
          { time: ts0, value: z.price_high },
          { time: ts1, value: z.price_high },
        ]);
        if (fp.length) fill.setData(fp);
        _xtra.push(fill);
      } catch (e) { console.warn('[zones] render error:', e.message); }
    });
  }

  /* ── Pivot visibility ────────────────────────────────────── */
  function setActiveLevels(levels) {
    _activeLevels = new Set(levels);
    _applyVisibility();
  }

  function toggleLevel(level) {
    if (_activeLevels.has(level)) {
      if (_activeLevels.size === 1) return;
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
        try { s.applyOptions({ visible }); } catch(_) {}
      });
    });
  }

  /* ── Cache + loader ──────────────────────────────────────── */
  function bustCache(tf) {
    Object.keys(_cache).forEach(k => {
      if (!tf || k.split('|')[1] === tf) delete _cache[k];
    });
  }

  function _showLoader(show) {
    const el = document.getElementById('cload');
    if (el) el.classList.toggle('show', show);
  }

  function scrollToLatest() {
    if (!_chart) return;
    try {
      _chart.timeScale().scrollToRealTime();
      requestAnimationFrame(() => {
        try { _chart.timeScale().scrollToPosition(12, false); } catch(_) {}
      });
    } catch(_) {}
  }

  /* ── Public ──────────────────────────────────────────────── */
  return {
    init, load,
    toggleLevel, setActiveLevels,
    bustCache, scrollToLatest,
    get activeLevels() { return [..._activeLevels]; },
    get moColors()     { return MO_COLORS; },
  };
})();

window.Chart = Chart;
