/* ═══════════════════════════════════════════════════════════════
   shared.js — single shared utility layer
   Replaces: qs-shared.js + shared.js (both removed)
   Exports to window.QS namespace.
═══════════════════════════════════════════════════════════════ */

const QS = (() => {

  /* ── Trend classification sets ──────────────────────────── */
  const UP = new Set([
    'strong_uptrend','continuous_uptrend','up',
    'semi-up','weakening_uptrend','bullish_mitigation',
  ]);
  const DN = new Set([
    'strong_downtrend','continuous_downtrend',
    'down','weakening_downtrend',
  ]);

  /* ── Trend helpers ──────────────────────────────────────── */
  function trendClass(t) {
    if (UP.has(t)) return 't-up';
    if (DN.has(t)) return 't-dn';
    return 't-co';
  }

  function trendStyle(t) {
    if (UP.has(t)) return 'background:#14532d;color:#86efac;';
    if (DN.has(t)) return 'background:#7f1d1d;color:#fca5a5;';
    return 'background:#1e2530;color:#8892a0;';
  }

  function trendColor(t) {
    if (UP.has(t)) return '#22c55e';
    if (DN.has(t)) return '#ef4444';
    if (t === 'transitioning') return '#f59e0b';
    return '#8892a0';
  }

  function trendLabel(t) {
    return (t || '').replace(/_/g, ' ');
  }

  function trendBadge(t) {
    return `<span class="tbadge ${trendClass(t)}">${trendLabel(t)}</span>`;
  }

  /* ── Score color ────────────────────────────────────────── */
  function scoreColor(s) {
    if (s > 0.70) return 'var(--green)';
    if (s > 0.55) return 'var(--amber)';
    return 'var(--accent)';
  }

  /* ── Formatters ─────────────────────────────────────────── */
  function pct(v) {
    return v != null ? Math.round(v * 100) + '%' : '–';
  }

  function inr(v, decimals) {
    if (v == null) return '–';
    return '₹' + Number(v).toLocaleString('en-IN', {
      maximumFractionDigits: decimals != null ? decimals : 2,
    });
  }

  /* ── Nav injection ──────────────────────────────────────── */
  const PAGES = [
    { key: 'scanner', href: '/',      label: 'Zone Scanner'  },
    { key: 'trend',   href: '/trend', label: 'Trend Scanner' },
    { key: 'data',    href: '/data',  label: 'Data Fetch'    },
  ];

  function renderNav(activePage, statusId, badgeId) {
    const links = PAGES.map(p =>
      `<a href="${p.href}" class="nav-link${p.key === activePage ? ' active' : ''}">`+
      `${p.label}</a>`
    ).join('');

    const el = document.getElementById('qs-nav');
    if (!el) return;

    el.innerHTML =
      `<a class="nav-logo" href="/">`+
      `<div class="nav-logo-icon">⚡</div>QuantScanner</a>`+
      links +
      `<div class="nav-spacer"></div>`+
      `<div class="nav-status">`+
      `<span class="nav-live"></span>`+
      `<span id="${statusId || 'nav-status'}">ready</span></div>`+
      `<div class="nav-badge" id="${badgeId || 'nav-badge'}"></div>`;
  }

  function setNavStatus(text, badgeText, statusId, badgeId) {
    const s = document.getElementById(statusId || 'nav-status');
    const b = document.getElementById(badgeId  || 'nav-badge');
    if (s) s.textContent = text || '';
    if (b) {
      if (badgeText) {
        b.textContent   = badgeText;
        b.style.display = 'block';
      } else {
        b.style.display = 'none';
      }
    }
  }

  /* ── App-state persistence (localStorage) ───────────────── */
  const STATE_KEY = 'qs_state_v2';

  function saveState(patch) {
    try {
      const cur = loadState();
      localStorage.setItem(STATE_KEY, JSON.stringify({ ...cur, ...patch }));
    } catch (_) {}
  }

  function loadState() {
    try {
      return JSON.parse(localStorage.getItem(STATE_KEY) || '{}');
    } catch (_) { return {}; }
  }

  function getState(key, def) {
    const s = loadState();
    return s[key] !== undefined ? s[key] : def;
  }

  /* ── Color validator ────────────────────────────────────── */
  function safeColor(c, fallback) {
    fallback = fallback || '#888888';
    if (typeof c !== 'string' || !c) return fallback;
    c = c.trim();
    if (c === 'transparent' || c === 'white' || c === 'black') return c;
    if (/^#[0-9a-fA-F]{3}$/.test(c))
      return '#' + c[1]+c[1] + c[2]+c[2] + c[3]+c[3];
    if (/^#[0-9a-fA-F]{4}$/.test(c))
      return '#' + c[1]+c[1] + c[2]+c[2] + c[3]+c[3] + c[4]+c[4];
    if (/^#[0-9a-fA-F]{6}([0-9a-fA-F]{2})?$/.test(c)) return c;
    return fallback;
  }

  return {
    UP, DN,
    trendClass, trendStyle, trendColor, trendLabel, trendBadge,
    scoreColor, pct, inr,
    renderNav, setNavStatus,
    saveState, loadState, getState,
    safeColor,
  };
})();

window.QS = QS;
