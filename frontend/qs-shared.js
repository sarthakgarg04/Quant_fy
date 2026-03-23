/* qs-shared.js – shared across all QuantScanner pages */

// ── Nav HTML (injected into every page) ─────────────────────────────────────
const NAV_LINKS = [
  { href: '/',      label: 'Scanner',      id: 'scanner' },
  { href: '/trend', label: 'Trend Viewer', id: 'trend'   },
  { href: '/data',  label: 'Data Fetch',   id: 'data'    },
];

function renderNav(activePage, rightSlot = '') {
  const links = NAV_LINKS.map(l => `
    <a href="${l.href}" class="qs-nav-link${l.id === activePage ? ' active' : ''}">${l.label}</a>
  `).join('');
  document.getElementById('qs-nav').innerHTML = `
    <a href="/" class="qs-logo"><span class="qs-logo-icon">⚡</span>QuantScanner</a>
    ${links}
    <div class="qs-nav-spacer"></div>
    ${rightSlot}
  `;
}

// ── State persistence (localStorage) ────────────────────────────────────────
const QS_STATE_KEY = 'qs_state_v1';

function saveState(patch) {
  try {
    const cur = loadState();
    localStorage.setItem(QS_STATE_KEY, JSON.stringify({ ...cur, ...patch }));
  } catch(e) {}
}

function loadState() {
  try {
    return JSON.parse(localStorage.getItem(QS_STATE_KEY) || '{}');
  } catch(e) { return {}; }
}

function getState(key, def) {
  const s = loadState();
  return s[key] !== undefined ? s[key] : def;
}

// ── Trend classification helpers ────────────────────────────────────────────
const UP_TRENDS = new Set([
  'strong_uptrend','continuous_uptrend','up','weakening_uptrend',
  'semi-up','bullish_mitigation',
]);
const DN_TRENDS = new Set([
  'strong_downtrend','continuous_downtrend','down','weakening_downtrend',
]);

function trendBadgeClass(t) {
  if (UP_TRENDS.has(t)) return 'badge-up';
  if (DN_TRENDS.has(t)) return 'badge-dn';
  return 'badge-flat';
}

function trendBadge(t) {
  return `<span class="badge ${trendBadgeClass(t)}">${(t||'').replace(/_/g,' ')}</span>`;
}

function trendHeaderStyle(t) {
  if (UP_TRENDS.has(t)) return 'background:#14532d;color:#86efac;';
  if (DN_TRENDS.has(t)) return 'background:#7f1d1d;color:#fca5a5;';
  return 'background:var(--surface3);color:var(--muted);';
}

function scoreColor(s) {
  if (s > 0.70) return 'var(--green)';
  if (s > 0.55) return 'var(--amber)';
  return 'var(--accent)';
}

function pct(v) { return v != null ? Math.round(v * 100) + '%' : '–'; }

// ── Lightweight Charts factory ───────────────────────────────────────────────
function createQSChart(container, height) {
  const h = height || (container.clientHeight || 400);
  const c = LightweightCharts.createChart(container, {
    width:  container.clientWidth,
    height: h,
    layout:    { background: { color: '#0d0f12' }, textColor: '#7a8796' },
    grid:      { vertLines: { color: '#1a1f28' }, horzLines: { color: '#1a1f28' } },
    crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
    rightPriceScale: { borderColor: '#28303d' },
    timeScale: { borderColor: '#28303d', timeVisible: true, secondsVisible: false },
  });
  new ResizeObserver(() => {
    c.applyOptions({ width: container.clientWidth, height: container.clientHeight || h });
  }).observe(container);
  return c;
}

function addCandleSeries(chart) {
  return chart.addCandlestickSeries({
    upColor: '#22c55e',   downColor: '#ef4444',
    borderUpColor: '#22c55e', borderDownColor: '#ef4444',
    wickUpColor: '#22c55e',   wickDownColor: '#ef4444',
  });
}

function addLineSeries(chart, color, width, dash) {
  return chart.addLineSeries({
    color, lineWidth: width || 1, lineStyle: dash || 0,
    crosshairMarkerVisible: false,
    lastValueVisible: false,
    priceLineVisible: false,
  });
}
