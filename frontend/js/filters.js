/* ═══════════════════════════════════════════════════════════════
   filters.js  —  Shared filter UI components
   ═══════════════════════════════════════════════════════════════
   Single source of truth for every filter widget used by both
   Zone Scanner (scanner.js) and Trend Scanner (trend_scanner.js).

   Exports window.Filters — an object of factory functions.
   Each page instantiates its own FilterSet with a namespace prefix
   so IDs never collide even if both pages were hypothetically
   mounted at the same time.

   Usage:
     const F = Filters.create('sc');   // Zone Scanner  → IDs: sc-sfdd-low …
     const F = Filters.create('ts');   // Trend Scanner → IDs: ts-sfdd-low …

   Each FilterSet exposes:
     F.loadStateGroups()          → fetch /api/structure_states, build cache
     F.renderStructureFilters(containerEl, moOn, orderValues)
     F.renderMOPanel(containerEl, values)
     F.getStructureParam(level)   → comma-separated selected states string
     F.getSelTrends(checkboxSel)  → comma-separated selected trend values
     F.selected                   → { low: Set, mid: Set, high: Set }

   The rendered HTML reuses the existing CSS classes from
   components.css (.trig .tdd .tdw .tdo .tdg .tarr .moc .mob …)
   so visual parity with Zone Scanner is guaranteed.
═══════════════════════════════════════════════════════════════ */

const Filters = (() => {

  /* ── Shared state-group cache (loaded once, shared across instances) ── */
  let _stateGroups = {};
  let _stateAll    = [];
  let _groupsLoaded = false;

  /* ── Bullish / Bearish sets for colour coding ─────────────────────────
     Kept in sync with BULLISH_STATES / BEARISH_STATES in trend_analysis.py  */
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

  function _stateColor(v) {
    if (BULL_STATES.has(v)) return '#22c55e';
    if (BEAR_STATES.has(v)) return '#ef4444';
    return '#f59e0b';
  }

  /* ── Load structure states from backend (cached, called once) ─────────
     The /api/structure_states endpoint derives its response directly from
     STATE_GROUPS + STATE_LABELS in trend_analysis.py, so it is always
     the authoritative source. The hardcoded fallback has been removed —
     if the API is unavailable the dropdowns will show empty rather than
     stale data. A console warning is emitted so the issue is visible.    */
  async function loadStateGroups() {
    if (_groupsLoaded) return;
    try {
      const r = await fetch('/api/structure_states').then(res => res.json());
      if (r?.groups) {
        _stateGroups  = r.groups;
        _stateAll     = r.all || Object.values(r.groups).flat();
        _groupsLoaded = true;
        return;
      }
    } catch (err) {
      console.warn('[Filters] /api/structure_states unavailable — structure dropdowns will be empty.', err);
    }
    /* API failed — leave _stateGroups empty so dropdowns render nothing
       rather than showing stale hardcoded values that no longer match
       the backend state strings.                                        */
    _stateGroups  = {};
    _stateAll     = [];
    _groupsLoaded = true;
  }

  /* ════════════════════════════════════════════════════════════
     FilterSet factory — one instance per page
  ════════════════════════════════════════════════════════════ */
  function create(ns) {
    /* ns = namespace prefix, e.g. 'sc' or 'ts' */
    const selected = { low: new Set(), mid: new Set(), high: new Set() };
    let _openDdWrapper = null;

    /* ── ID helpers ─────────────────────────────────────────── */
    const _id = suffix => `${ns}-${suffix}`;
    const _el = suffix => document.getElementById(_id(suffix));

    /* ── Build one structure dropdown ─────────────────────────
       Uses ns-prefixed IDs to avoid collisions across pages.  */
    function _buildStructureDropdown(level) {
      const sel    = selected[level];
      const allSel = sel.size === 0;
      const lbl    = allSel ? 'Any state' : `${sel.size} selected`;
      const wrId   = `${ns}-sfwrap-${level}`;
      const ddId   = `${ns}-sfdd-${level}`;
      const trigId = `${ns}-sftrig-${level}`;
      const lblId  = `${ns}-sflbl-${level}`;
      const allId  = `${ns}-sfall-${level}`;

      let optHtml = `
        <div style="padding:7px 12px;border-bottom:1px solid var(--border)">
          <label style="display:flex;align-items:center;gap:8px;cursor:pointer;
                 font-size:12px;color:var(--accent);font-family:var(--mono)">
            <input type="checkbox" id="${allId}" ${allSel ? 'checked' : ''}
                   onchange="Filters._sfAllToggle('${ns}','${level}',this.checked)">
            All states
          </label>
        </div>`;

      for (const [grpName, states] of Object.entries(_stateGroups)) {
        optHtml += `<div class="tdg">${grpName}</div>`;
        for (const { value, label } of states) {
          const chk = (sel.size === 0 || sel.has(value)) ? 'checked' : '';
          optHtml += `
            <div class="tdo">
              <input type="checkbox" class="${ns}-sfc-${level}" value="${value}" ${chk}
                     onchange="Filters._sfChange('${ns}','${level}')">
              <span style="color:${_stateColor(value)}">${label}</span>
            </div>`;
        }
      }

      return `
        <div class="tdw" id="${wrId}" onclick="event.stopPropagation()">
          <button class="trig" id="${trigId}"
                  onclick="Filters._sfToggle(event,'${ns}','${level}','${wrId}')">
            <span id="${lblId}">${lbl}</span>
            <span class="tarr">▼</span>
          </button>
          <div class="tdd" id="${ddId}" style="max-height:260px;overflow-y:auto">
            ${optHtml}
          </div>
        </div>`;
    }

    /* ── Render structure filter section ────────────────────── */
    function renderStructureFilters(singleWrap, multiWrap, moOn, orderValues) {
      if (!singleWrap || !multiWrap) return;
      singleWrap.innerHTML = '';
      multiWrap.innerHTML  = '';

      if (!moOn) {
        singleWrap.innerHTML = _buildStructureDropdown('low');
        singleWrap.style.display = 'block';
        multiWrap.style.display  = 'none';
      } else {
        const ord = orderValues || { high: '', mid: '', low: '' };
        multiWrap.innerHTML = `
          <div style="display:grid;grid-template-columns:36px 1fr;align-items:start;gap:6px;margin-bottom:8px">
            <span class="molk moH" style="font-size:9px;padding-top:8px">H${ord.high ? `(${ord.high})` : ''}</span>
            <div>${_buildStructureDropdown('high')}</div>
          </div>
          <div style="display:grid;grid-template-columns:36px 1fr;align-items:start;gap:6px;margin-bottom:8px">
            <span class="molk moM" style="font-size:9px;padding-top:8px">M${ord.mid ? `(${ord.mid})` : ''}</span>
            <div>${_buildStructureDropdown('mid')}</div>
          </div>
          <div style="display:grid;grid-template-columns:36px 1fr;align-items:start;gap:6px">
            <span class="molk moL" style="font-size:9px;padding-top:8px">L${ord.low ? `(${ord.low})` : ''}</span>
            <div>${_buildStructureDropdown('low')}</div>
          </div>`;
        multiWrap.style.display  = 'block';
        singleWrap.style.display = 'none';
      }
      _openDdWrapper = null;
    }

    /* ── Re-sync checkbox states without full re-render ─────── */
    function _syncCheckboxes(level) {
      const sel    = selected[level];
      const allCbs = [...document.querySelectorAll(`.${ns}-sfc-${level}`)];
      if (!allCbs.length) return;
      allCbs.forEach(cb => {
        cb.checked = sel.size === 0 || sel.has(cb.value);
      });
      const allCb = document.getElementById(`${ns}-sfall-${level}`);
      const chk   = allCbs.filter(c => c.checked);
      if (allCb) {
        allCb.checked       = chk.length === allCbs.length;
        allCb.indeterminate = chk.length > 0 && chk.length < allCbs.length;
      }
      const lbl = document.getElementById(`${ns}-sflbl-${level}`);
      if (lbl) lbl.textContent = sel.size > 0 ? `${sel.size} selected` : 'Any state';
    }

    /* ── Public helpers ─────────────────────────────────────── */
    function getStructureParam(level) {
      return selected[level].size > 0 ? [...selected[level]].join(',') : '';
    }

    function getSelTrends(checkboxSelector) {
      const all = [...document.querySelectorAll(checkboxSelector)];
      const chk = all.filter(c => c.checked);
      if (chk.length === 0 || chk.length === all.length) return '';
      return chk.map(c => c.value).join(',');
    }

    /* ── Document click handler (call once from page init) ─── */
    function installDocClickHandler() {
      document.addEventListener('click', e => {
        if (_openDdWrapper && !_openDdWrapper.contains(e.target)) {
          document.querySelectorAll('.tdd').forEach(d => d.classList.remove('op'));
          document.querySelectorAll('.trig').forEach(t => t.classList.remove('op'));
          _openDdWrapper = null;
        }
      });
    }

    return {
      ns,
      selected,
      loadStateGroups,
      renderStructureFilters,
      getStructureParam,
      getSelTrends,
      installDocClickHandler,
      _toggle(level, wrapperId) {
        const dd  = document.getElementById(`${ns}-sfdd-${level}`);
        const trg = document.getElementById(`${ns}-sftrig-${level}`);
        if (!dd) return;
        const isOpen = dd.classList.contains('op');
        document.querySelectorAll('.tdd').forEach(d => d.classList.remove('op'));
        document.querySelectorAll('.trig').forEach(t => t.classList.remove('op'));
        _openDdWrapper = null;
        if (!isOpen) {
          dd.classList.add('op');
          trg && trg.classList.add('op');
          _openDdWrapper = document.getElementById(wrapperId);
        }
      },
      _allToggle(level, checked) {
        document.querySelectorAll(`.${ns}-sfc-${level}`).forEach(cb => { cb.checked = checked; });
        this._change(level);
      },
      _change(level) {
        const sel    = selected[level];
        sel.clear();
        const allCbs = [...document.querySelectorAll(`.${ns}-sfc-${level}`)];
        const chkCbs = allCbs.filter(c => c.checked);
        const allSel = chkCbs.length === allCbs.length;
        if (!allSel) chkCbs.forEach(c => sel.add(c.value));
        const lbl = document.getElementById(`${ns}-sflbl-${level}`);
        if (lbl) lbl.textContent = allSel ? 'Any state' : `${sel.size} selected`;
        const allCb = document.getElementById(`${ns}-sfall-${level}`);
        if (allCb) {
          allCb.checked       = allSel;
          allCb.indeterminate = !allSel && chkCbs.length > 0;
        }
      },
    };
  }

  /* ── Global dispatch — called from inline onclick HTML attrs ── */
  const _instances = {};

  function _register(instance) {
    _instances[instance.ns] = instance;
  }

  function _sfToggle(event, ns, level, wrapperId) {
    event.stopPropagation();
    _instances[ns]?._toggle(level, wrapperId);
  }

  function _sfAllToggle(ns, level, checked) {
    _instances[ns]?._allToggle(level, checked);
  }

  function _sfChange(ns, level) {
    _instances[ns]?._change(level);
  }

  return {
    create,
    _register,
    _sfToggle,
    _sfAllToggle,
    _sfChange,
    loadStateGroups,
    get stateGroups() { return _stateGroups; },
    get stateAll()    { return _stateAll;    },
  };
})();

window.Filters = Filters;
