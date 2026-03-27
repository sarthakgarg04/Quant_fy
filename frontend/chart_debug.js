/* ═══════════════════════════════════════════════════════════════════
   chart_debug.js
   Add TEMPORARILY to index.html just before </body>:
     <script src="/chart_debug.js"></script>
   
   Then open browser DevTools console.
   Click any stock — you'll see full logs per chart load.
   Also call from console: debugPivots('BAJAJCON')
═══════════════════════════════════════════════════════════════════ */
(function () {

  const C = {
    ok:   'color:#22c55e;font-weight:bold',
    warn: 'color:#f59e0b;font-weight:bold',
    err:  'color:#ef4444;font-weight:bold',
    info: 'color:#93c5fd;font-weight:bold',
    dim:  'color:#8892a0',
  };

  function validateSeries(label, data) {
    if (!Array.isArray(data) || data.length === 0) {
      console.warn(`%c  [${label}] EMPTY array — will not render`, C.err);
      return false;
    }
    const times   = data.map(d => d.time);
    const dups    = times.length - new Set(times).size;
    let unsorted  = 0;
    for (let i = 0; i < times.length - 1; i++) {
      if (times[i] >= times[i + 1]) unsorted++;
    }
    const hasNaN  = data.some(d => d.value !== undefined && isNaN(d.value));
    const hasNull = data.some(d => d.value === null || d.value === undefined);
    const ok = dups === 0 && unsorted === 0 && !hasNaN && !hasNull;
    console.log(
      `%c  [${label}]%c  n=${data.length}  dups=${dups}  `+
      `unsorted=${unsorted}  nan=${hasNaN}  null=${hasNull}  `+
      `→ ${ok ? '✓ OK' : '✗ FAIL'}`,
      ok ? C.ok : C.err, C.dim
    );
    if (!ok) {
      console.log('%c    first:', C.dim, data.slice(0, 2));
      console.log('%c    last: ', C.dim, data.slice(-2));
    }
    return ok;
  }

  /* Intercept loadChart */
  const _orig = window.loadChart;
  window.loadChart = async function (row) {
    console.group(`%c▶ loadChart  ${row.ticker}  tf=${window.chartTF}`, C.info);
    console.time('  duration');
    const origFetch = window.fetch;
    let n = 0;

    window.fetch = async function (...args) {
      const url = typeof args[0] === 'string' ? args[0] : args[0].url;
      const isChart = url.includes('/api/chart/');
      const isTrend = url.includes('/api/trend/');
      if (!isChart && !isTrend) return origFetch(...args);

      n++;
      const order = url.match(/order=(\d+)/)?.[1] || '?';
      const label = isChart ? 'chart-api' : `trend(order=${order})`;
      const t0    = performance.now();
      console.log(`%c  → fetch ${label}`, C.dim);

      try {
        const res   = await origFetch(...args);
        const ms    = (performance.now() - t0).toFixed(0);
        console.log(`%c  ← ${label}  HTTP ${res.status}  ${ms}ms`, res.ok ? C.dim : C.err);

        if (res.ok) {
          const clone = res.clone();
          clone.json().then(d => {
            if (isChart) {
              validateSeries('candles', d.candles);
              if (d.pivots) validateSeries('pivots(primary)', d.pivots);
              console.log(`%c    buy_zones=${(d.buy_zones||[]).length}  sell_zones=${(d.sell_zones||[]).length}`, C.dim);
            } else {
              validateSeries(`pivots(order=${order})`, d.pivots || []);
            }
          }).catch(() => {});
        }
        return res;
      } catch (e) {
        console.error(`%c  ← ${label}  NETWORK ERROR: ${e.message}`, C.err);
        throw e;
      }
    };

    try {
      await _orig.call(this, row);
      console.log(`%c  fetches made: ${n}`, C.dim);
    } catch (e) {
      console.error('%c  loadChart threw:', C.err, e);
    } finally {
      window.fetch = origFetch;
      console.timeEnd('  duration');
      console.groupEnd();
    }
  };

  /* Intercept addLineSeries + addCandlestickSeries to catch LWC errors */
  const _origCreate = LightweightCharts.createChart;
  LightweightCharts.createChart = function (...args) {
    const c = _origCreate(...args);
    const _origAddLine = c.addLineSeries.bind(c);
    const _origAddCandle = c.addCandlestickSeries.bind(c);

    c.addLineSeries = function (...a) {
      try { return _origAddLine(...a); }
      catch (e) { console.error('%c[LWC] addLineSeries THREW:', C.err, e.message); throw e; }
    };
    c.addCandlestickSeries = function (...a) {
      try { return _origAddCandle(...a); }
      catch (e) { console.error('%c[LWC] addCandlestickSeries THREW:', C.err, e.message); throw e; }
    };
    return c;
  };

  /* Server-side debug helper — call from browser console */
  window.debugPivots = async function (ticker, interval, orders) {
    interval = interval || '1d';
    orders   = orders   || '5,10,20';
    console.group(`%c[debugPivots] ${ticker}  interval=${interval}  orders=${orders}`, C.info);
    const r = await fetch(`/api/debug/pivots/${ticker}?interval=${interval}&orders=${orders}`);
    const d = await r.json();
    console.log('%c  candles:', C.info,
      `${d.candles}  sorted=${d.candle_times_sorted}  dups=${d.candle_duplicates}`);
    Object.entries(d.orders || {}).forEach(([ord, info]) => {
      if (info.error) {
        console.log(`%c  order=${ord}: ERROR: ${info.error}`, C.err);
      } else {
        const ok = info.duplicates===0 && info.sorted && !info.has_nan && !info.has_none;
        console.log(
          `%c  order=${ord}:  ${info.pivot_count} pivots  `+
          `sorted=${info.sorted}  dups=${info.duplicates}  → ${ok?'✓':'✗'}`,
          ok ? C.ok : C.err
        );
        if (!ok) console.log('%c    sample:', C.dim, info.sample_last3);
      }
    });
    console.groupEnd();
  };

  console.log('%c[chart_debug.js] ready — call debugPivots("BAJAJCON") in console', C.warn);
})();
