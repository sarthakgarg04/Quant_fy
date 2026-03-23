/* shared.js – inject nav + apply shared CSS vars on every page */

/* ── Design tokens (all pages share these) ────────────────────────── */
const QS = {
  API: '/api',
  UPT: new Set(['strong_uptrend','continuous_uptrend','up','weakening_uptrend','semi-up','bullish_mitigation']),
  DNT: new Set(['strong_downtrend','continuous_downtrend','down','weakening_downtrend']),
  tStyle(t){
    if(QS.UPT.has(t)) return 'background:#14532d;color:#86efac;';
    if(QS.DNT.has(t)) return 'background:#7f1d1d;color:#fca5a5;';
    return 'background:#1e2530;color:#8892a0;';
  },
  tClass(t){
    if(QS.UPT.has(t)) return 'badge-up';
    if(QS.DNT.has(t)) return 'badge-dn';
    return 'badge-co';
  },
  pct(v){ return v!=null ? Math.round(v*100)+'%' : '–'; },
  scol(s){ return s>0.70?'#22c55e':s>0.55?'#f59e0b':'#3b82f6'; },

  /* Inject the nav bar. Call with current page key: 'scanner'|'trend'|'data' */
  nav(active){
    const pages = [
      {key:'scanner', href:'/',       label:'Scanner'},
      {key:'trend',   href:'/trend',  label:'Trend Viewer'},
      {key:'data',    href:'/data',   label:'Data Fetch'},
    ];
    const links = pages.map(p =>
      `<a href="${p.href}" class="${p.key===active?'active':''}">${p.label}</a>`
    ).join('');
    const el = document.createElement('nav');
    el.id = 'qs-nav';
    el.innerHTML = `
      <span class="logo">⚡ QuantScanner</span>
      ${links}
      <div class="spacer"></div>
      <span id="nav-badge" class="badge" style="display:none"></span>`;
    document.body.prepend(el);
  },

  /* Set text in the nav badge */
  navBadge(text, show=true){
    const b = document.getElementById('nav-badge');
    if(b){ b.textContent = text; b.style.display = show?'':'none'; }
  },
};
window.QS = QS;
