/* ═══════════════════════════════════════════════════════════════
   info_drawer.js  —  Shared hover-reveal info drawer  v2
   ═══════════════════════════════════════════════════════════════
   Provides a consistent right-edge hover drawer on both the
   Zone Scanner and Trend Scanner pages.

   Usage
   -----
   InfoDrawer.attach(chartColEl)       — call once on page init
   InfoDrawer.populate(ticker, sub, html) — call on symbol select
   InfoDrawer.clear()                  — call to reset
═══════════════════════════════════════════════════════════════ */

const InfoDrawer = (() => {

  let _attached = false;

  /* ── CSS ─────────────────────────────────────────────────── */
  const CSS = `
/* ── Trigger strip: full height, wide enough to reliably catch hover ── */
#qs-idr-trigger {
  position: absolute;
  top: 0; right: 0; bottom: 0;
  width: 32px;          /* wider than the pull tab — easier to hover */
  z-index: 60;
  cursor: e-resize;
}

/* ── Drawer shell ─────────────────────────────────────────── */
#qs-idr-shell {
  position: absolute;
  top: 0; right: 0; bottom: 0;
  width: 268px;
  background: var(--surface);
  border-left: 1px solid var(--border);
  box-shadow: -10px 0 28px rgba(0,0,0,.45);
  display: flex;
  flex-direction: column;
  transform: translateX(100%);
  transition: transform .2s cubic-bezier(.4,0,.2,1);
  z-index: 55;
  overflow: hidden;
}

/* Open when hovering trigger OR the drawer itself */
#qs-idr-trigger:hover ~ #qs-idr-shell,
#qs-idr-shell:hover {
  transform: translateX(0);
}

/* ── Pull tab (visible tab at right edge when closed) ──────── */
#qs-idr-pull {
  position: absolute;
  right: 0; top: 50%;
  transform: translateY(-50%);
  width: 18px; height: 64px;
  background: var(--s3);
  border: 1px solid var(--border);
  border-right: none;
  border-radius: 4px 0 0 4px;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 7px;
  color: var(--muted2);
  writing-mode: vertical-rl;
  letter-spacing: 1.5px;
  font-family: var(--mono);
  font-weight: 700;
  text-transform: uppercase;
  z-index: 58;
  pointer-events: none;
  transition: opacity .2s;
}
#qs-idr-trigger:hover ~ #qs-idr-pull,
#qs-idr-shell:hover ~ #qs-idr-pull,
#qs-idr-trigger:hover ~ #qs-idr-shell ~ #qs-idr-pull { opacity: 0; }

/* ── Drawer header ────────────────────────────────────────── */
.qs-idr-hdr {
  height: 44px;
  flex-shrink: 0;
  display: flex;
  align-items: center;
  padding: 0 14px;
  border-bottom: 1px solid var(--border);
  gap: 8px;
}
.qs-idr-hdr-lbl {
  font-size: 9px; font-weight: 700; font-family: var(--mono);
  color: var(--muted2); text-transform: uppercase; letter-spacing: 1px;
  flex-shrink: 0;
}
#qs-idr-ticker {
  font-size: 13px; font-weight: 700; font-family: var(--mono);
  color: var(--text); flex: 1; min-width: 0;
  overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
#qs-idr-sub {
  font-size: 9px; color: var(--muted); font-family: var(--mono);
  white-space: nowrap; flex-shrink: 0; max-width: 96px;
  overflow: hidden; text-overflow: ellipsis;
}

/* ── Scrollable content area ──────────────────────────────── */
.qs-idr-scroll {
  flex: 1; overflow-y: auto; padding: 12px;
  display: flex; flex-direction: column; gap: 12px;
}

/* ── Empty state ──────────────────────────────────────────── */
.qs-idr-empty {
  display: flex; flex-direction: column;
  align-items: center; justify-content: center;
  height: 160px; gap: 6px; color: var(--muted2);
}
.qs-idr-empty-ico { font-size: 20px; }
.qs-idr-empty-txt { font-size: 11px; font-family: var(--mono); }

/* ── Shared content styles (used by both pages) ────────────── */
.idr-sec-lbl {
  font-size: 8px; font-weight: 700; font-family: var(--mono);
  color: var(--muted2); text-transform: uppercase; letter-spacing: 1px;
  margin-bottom: 7px;
}
.idr-stat-grid {
  display: grid; grid-template-columns: 1fr 1fr; gap: 5px;
}
.idr-stat {
  background: var(--s2);
  border: 1px solid var(--border);
  border-radius: 5px;
  padding: 6px 8px;
}
.idr-stat-l {
  font-size: 9px; color: var(--muted2); font-family: var(--mono);
  margin-bottom: 2px;
}
.idr-stat-v {
  font-size: 11px; font-weight: 700; font-family: var(--mono);
  color: var(--text);
}
.idr-trend-pill {
  border-radius: 6px; padding: 7px 10px;
  font-size: 13px; font-weight: 700; font-family: var(--mono);
  text-align: center;
}
.idr-trend-up   { background: #14532d; color: #86efac; }
.idr-trend-dn   { background: #7f1d1d; color: #fca5a5; }
.idr-trend-co   { background: #1e2530; color: #8892a0; }
.idr-mo-bar     { margin-bottom: 7px; }
.idr-mo-hdr     { display: flex; justify-content: space-between; margin-bottom: 3px; }
.idr-mo-lbl     { font-size: 9px; font-weight: 700; font-family: var(--mono); }
.idr-mo-trend   { font-size: 9px; color: var(--muted); font-family: var(--mono); }
.idr-track      { height: 4px; background: var(--s3); border-radius: 2px; overflow: hidden; }
.idr-fill       { height: 100%; border-radius: 2px; }
.idr-conf-row   { display: flex; justify-content: space-between;
                   font-size: 9px; color: var(--muted); margin-bottom: 3px; }
`;

  /* ── DOM shell ───────────────────────────────────────────── */
  function _buildShell() {
    /* Trigger — spans full height, wider than pull tab */
    const trigger = document.createElement('div');
    trigger.id = 'qs-idr-trigger';

    /* Drawer */
    const shell = document.createElement('div');
    shell.id = 'qs-idr-shell';
    shell.innerHTML = `
      <div class="qs-idr-hdr">
        <span class="qs-idr-hdr-lbl">Analysis</span>
        <span id="qs-idr-ticker">—</span>
        <span id="qs-idr-sub"></span>
      </div>
      <div class="qs-idr-scroll" id="qs-idr-scroll">
        <div class="qs-idr-empty" id="qs-idr-empty">
          <div class="qs-idr-empty-ico">◈</div>
          <div class="qs-idr-empty-txt">Select a symbol</div>
        </div>
        <div id="qs-idr-content" style="display:none;flex-direction:column;gap:12px"></div>
      </div>`;

    /* Pull tab */
    const pull = document.createElement('div');
    pull.id = 'qs-idr-pull';
    pull.textContent = 'INFO';

    return { trigger, shell, pull };
  }

  /* ── attach ──────────────────────────────────────────────── */
  function attach(colEl) {
    if (_attached || !colEl) return;

    /* Inject CSS once */
    if (!document.getElementById('qs-idr-css')) {
      const s = document.createElement('style');
      s.id = 'qs-idr-css';
      s.textContent = CSS;
      document.head.appendChild(s);
    }

    /* Ensure position:relative on parent */
    if (getComputedStyle(colEl).position === 'static') {
      colEl.style.position = 'relative';
    }

    const { trigger, shell, pull } = _buildShell();

    /* Order matters for CSS sibling selectors:
       trigger must come BEFORE shell, shell BEFORE pull */
    colEl.appendChild(trigger);
    colEl.appendChild(shell);
    colEl.appendChild(pull);

    _attached = true;
  }

  /* ── populate ────────────────────────────────────────────── */
  function populate(ticker, subtitle, contentHTML) {
    const tickerEl  = document.getElementById('qs-idr-ticker');
    const subEl     = document.getElementById('qs-idr-sub');
    const emptyEl   = document.getElementById('qs-idr-empty');
    const contentEl = document.getElementById('qs-idr-content');
    if (!tickerEl || !contentEl) return;

    tickerEl.textContent    = ticker   || '—';
    subEl.textContent       = subtitle || '';
    emptyEl.style.display   = 'none';
    contentEl.style.display = 'flex';
    contentEl.innerHTML     = contentHTML || '';
  }

  /* ── clear ───────────────────────────────────────────────── */
  function clear() {
    const tickerEl  = document.getElementById('qs-idr-ticker');
    const emptyEl   = document.getElementById('qs-idr-empty');
    const contentEl = document.getElementById('qs-idr-content');
    if (!tickerEl) return;
    tickerEl.textContent    = '—';
    document.getElementById('qs-idr-sub').textContent = '';
    emptyEl.style.display   = 'flex';
    contentEl.style.display = 'none';
    contentEl.innerHTML     = '';
  }

  /* ── HTML helpers (shared by both page content renderers) ── */

  function trendPill(trend) {
    const cls = QS.UP.has(trend) ? 'idr-trend-up'
              : QS.DN.has(trend) ? 'idr-trend-dn' : 'idr-trend-co';
    return `<div class="idr-trend-pill ${cls}">${QS.trendLabel(trend)}</div>`;
  }

  function statGrid(pairs) {
    return `<div class="idr-stat-grid">
      ${pairs.map(([l, v]) => `
        <div class="idr-stat">
          <div class="idr-stat-l">${l}</div>
          <div class="idr-stat-v">${v}</div>
        </div>`).join('')}
    </div>`;
  }

  function section(label, bodyHTML) {
    return `<div>
      <div class="idr-sec-lbl">${label}</div>
      ${bodyHTML}
    </div>`;
  }

  function moBar(lvLabel, state, ordNum, stateColor) {
    const pct = 65; /* just a visual width indicator */
    return `<div class="idr-mo-bar">
      <div class="idr-mo-hdr">
        <span class="idr-mo-lbl" style="color:${stateColor}">${lvLabel}(${ordNum||'—'})</span>
        <span class="idr-mo-trend">${(state||'').replace(/_/g,' ')}</span>
      </div>
      <div class="idr-track">
        <div class="idr-fill" style="width:${pct}%;background:${stateColor}"></div>
      </div>
    </div>`;
  }

  function confBar(label, value, color) {
    const pct = Math.round((value || 0) * 100);
    return `<div>
      <div class="idr-conf-row"><span>${label}</span><span>${pct}%</span></div>
      <div class="idr-track">
        <div class="idr-fill" style="width:${pct}%;background:${color}"></div>
      </div>
    </div>`;
  }

  return {
    attach, populate, clear,
    /* helpers exposed for page-level renderers */
    trendPill, statGrid, section, moBar, confBar,
  };

})();

window.InfoDrawer = InfoDrawer;
