/* ═══════════════════════════════════════════════════════════════
   trend.js — Trend Viewer page controller
   Depends on: shared.js, api.js, chart.js
═══════════════════════════════════════════════════════════════ */

const Trend = (() => {

  const $ = id => document.getElementById(id);
  let curTicker = null;
  let chartTF   = '1d';

  /* ── MO toggle ──────────────────────────────────────────── */
  function toggleMO(enabled) {
    const inp = $('tv-mo-inputs');
    inp.style.opacity       = enabled ? '1'    : '.35';
    inp.style.pointerEvents = enabled ? 'auto' : 'none';
  }

  /* ── Analyse ────────────────────────────────────────────── */
  async function analyse() {
    const ticker = ($('tkr').value || '').trim().toUpperCase();
    if (!ticker) return;
    curTicker = ticker;

    const moEnabled = $('tv-mo-enable').checked;

    QS.saveState({
      trend_tkr:   ticker,
      trend_tfs:   $('tfs').value,
      trend_days:  $('days').value,
      trend_ord:   $('ord').value,
      trend_msp:   $('msp').value,
      trend_atr:   $('atr').value,
      tv_mo_enable: moEnabled,
      tv_mo_high:  $('tv-mo-high').value,
      tv_mo_mid:   $('tv-mo-mid').value,
      tv_mo_low:   $('tv-mo-low').value,
    });

    const params = {
      interval:         $('tfs').value,
      days:             $('days').value,
      order:            $('ord').value,
      min_strength_pct: $('msp').value,
      atr_mult:         $('atr').value,
      multi_order:      moEnabled,
      order_low:        $('tv-mo-low').value,
      order_mid:        $('tv-mo-mid').value,
      order_high:       $('tv-mo-high').value,
    };

    $('tv-overlay').classList.add('show');
    $('abtn').disabled = true;

    try {
      const d = await API.trendData(ticker, params);
      if (d.error) { alert(d.error); return; }

      // Draw chart (pivot-only, no zones for trend viewer)
      await Chart.load(ticker, {
        interval:    params.interval,
        order:       params.order,
        multi_order: moEnabled,
        order_low:   params.order_low,
        order_mid:   params.order_mid,
        order_high:  params.order_high,
        // pass candles directly via a local override approach:
        _candles:    d.candles,
        _pivots:     d.pivots,
      }, 'buy', null);

      // Update header
      $('tv-ticker').textContent = ticker;
      $('tv-price').textContent  = QS.inr(d.last_price);
      const tb = $('tv-trend-badge');
      tb.textContent  = QS.trendLabel(d.trend);
      tb.style.cssText = `font-size:11px;padding:2px 10px;border-radius:99px;${QS.trendStyle(d.trend)}`;

      _renderResults(d);
    } catch (e) {
      alert('Error: ' + e.message);
    } finally {
      $('tv-overlay').classList.remove('show');
      $('abtn').disabled = false;
    }
  }

  function _renderResults(d) {
    $('tv-res').style.display = 'flex';

    // Trend display
    const td  = $('tv-tdisp');
    td.textContent = QS.trendLabel(d.trend) || '–';
    td.className   = 'trend-display ' +
      (QS.UP.has(d.trend) ? 'tup' : QS.DN.has(d.trend) ? 'tdn' : 'tco');

    const c  = d.confluence  || {};
    const ts = d.trend_strength || {};

    $('tv-sgs').innerHTML = [
      ['Confluence', QS.pct(c.score)],
      ['ATR %',      (c.atr_pct || 0).toFixed(2) + '%'],
      ['Trend Str.', ts.classification || '–'],
      ['Pivots',     d.pivot_count || 0],
      ['Vol Score',  QS.pct(c.vol_score)],
      ['Zone Score', QS.pct(c.zone_score)],
    ].map(([l, v]) =>
      `<div class="tv-stat"><div class="tv-stat-l">${l}</div><div class="tv-stat-v">${v}</div></div>`
    ).join('');

    // Structure metrics
    const vel = d.velocity  || {};
    const amp = d.amplitude || {};
    const lr  = d.leg_ratio || {};
    const vp  = (vel.current_vel || 0) >= 0;
    const ai  = vel.accel_label === 'accelerating' ? '↑'
              : vel.accel_label === 'decelerating' ? '↓' : '→';

    $('tv-struct-grid').innerHTML = [
      ['Velocity',     `${vp?'+':''}${vel.current_vel??'–'} pts/bar`,
                       `${ai} ${vel.accel_label||'–'}`],
      ['Avg Velocity', `${vel.avg_velocity??'–'}`,
                       `net: ${vel.trend_vel??'–'}`],
      ['Amplitude',    `${amp.avg_amplitude??'–'} avg`, amp.regime||'–'],
      ['Recent Amp',   `${amp.recent_avg??'–'}`,
                       `${amp.variance_pct??'–'}% variance`],
      ['Leg Ratio',    `${lr.ratio??'–'}×`,
                       (lr.label||'–').replace(/_/g,' ')],
      ['Recent Ratio', `${lr.recent_ratio??'–'}×`,
                       `Bull ${lr.avg_bull??'–'} / Bear ${lr.avg_bear??'–'}`],
    ].map(([l, v, s]) =>
      `<div class="tv-stat">
         <div class="tv-stat-l">${l}</div>
         <div class="tv-stat-v">${v}</div>
         ${s ? `<div style="font-size:10px;color:var(--muted);margin-top:1px">${s}</div>` : ''}
       </div>`
    ).join('');

    // Multi-order breakdown
    const moSec = $('tv-mo-section');
    const mo    = d.multiorder;
    if (mo) {
      moSec.style.display = 'block';
      const dots = '●'.repeat(mo.alignment||0) + '○'.repeat(3-(mo.alignment||0));
      $('tv-mo-align').textContent =
        `${dots} ${mo.alignment}/3 · ${QS.trendLabel(mo.combined_trend)}`;

      $('tv-mo-bars').innerHTML = ['high','mid','low'].map(lv => {
        const lvd = mo[lv] || {};
        const t   = lvd.trend || '–';
        const s   = lvd.score ?? 0;
        const pw  = Math.round((s + 1) / 2 * 100);
        const col = QS.trendColor(t);
        const vd  = lvd.velocity  || {};
        const ad  = lvd.amplitude || {};
        const ld  = lvd.leg_ratio || {};
        return `<div style="margin-bottom:8px">
          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:3px">
            <span style="font-size:11px;font-weight:700;color:${col}">${lv.toUpperCase()}(${lvd.order})</span>
            <span style="font-size:11px;color:${col}">${QS.trendLabel(t)}</span>
            <span style="font-size:10px;color:var(--muted)">${pw}%</span>
          </div>
          <div class="conf-track">
            <div class="conf-fill" style="width:${pw}%;background:${col}"></div>
          </div>
          <div style="display:flex;gap:12px;margin-top:4px;font-size:10px;color:var(--muted)">
            <span>Vel: ${vd.current_vel??'–'}</span>
            <span>Amp: ${ad.avg_amplitude??'–'}</span>
            <span>LR: ${ld.ratio??'–'}×</span>
          </div>
        </div>`;
      }).join('');
    } else {
      moSec.style.display = 'none';
    }

    // Confluence bars
    $('tv-cbars').innerHTML = [
      ['Trend', c.trend_score||0, '#3b82f6'],
      ['Zone',  c.zone_score ||0, '#22c55e'],
      ['HTF',   mo ? (mo.alignment/3) : (c.htf_score||0), mo ? '#f59e0b' : '#a78bfa'],
      ['Vol',   c.vol_score  ||0, '#f59e0b'],
    ].map(([l, v, col]) =>
      `<div class="conf-bar">
         <div class="conf-row">
           <span>${l}${mo && l==='HTF' ? ' (alignment)' : ''}</span>
           <span>${Math.round(v * 100)}%</span>
         </div>
         <div class="conf-track">
           <div class="conf-fill" style="width:${Math.round(v*100)}%;background:${col}"></div>
         </div>
       </div>`
    ).join('');

    // Edge table
    $('tv-etb').innerHTML = (d.edge?.length)
      ? d.edge.map(r =>
          `<tr>
            <td style="color:${r.type==='B'?'var(--green)':'var(--red)'}">${r.type==='B'?'Bull':'Bear'}</td>
            <td>${r.horizon}b</td><td>${r.n}</td>
            <td class="${r.hit_rate>0.5?'pos':'neg'}">${r.hit_rate!=null?(r.hit_rate*100).toFixed(0)+'%':'–'}</td>
            <td class="${r.avg_R>0?'pos':'neg'}">${r.avg_R!=null?r.avg_R.toFixed(2)+'%':'–'}</td>
            <td>${r.payoff!=null?r.payoff.toFixed(2):'–'}</td>
          </tr>`
        ).join('')
      : `<tr><td colspan="6" style="color:var(--muted);text-align:center;padding:10px">Not enough pivot history</td></tr>`;
  }

  /* ── TF quick-switch ────────────────────────────────────── */
  function quickTF(tf, btn) {
    $('tfs').value = tf;
    document.querySelectorAll('#tfg .tfb').forEach(b => b.classList.remove('on'));
    btn.classList.add('on');
    if (curTicker) analyse();
  }

  /* ── Init ───────────────────────────────────────────────── */
  function init() {
    QS.renderNav('trend', 'tv-nav-status');
    Chart.init('tv-chart-wrap');

    // Restore last state
    const s = QS.loadState();
    if (s.trend_tkr)  $('tkr').value  = s.trend_tkr;
    if (s.trend_tfs)  $('tfs').value  = s.trend_tfs;
    if (s.trend_days) $('days').value = s.trend_days;
    if (s.trend_ord)  $('ord').value  = s.trend_ord;
    if (s.trend_msp)  $('msp').value  = s.trend_msp;
    if (s.trend_atr)  $('atr').value  = s.trend_atr;
    if (s.tv_mo_enable) {
      $('tv-mo-enable').checked = true;
      toggleMO(true);
    }
    if (s.tv_mo_high) $('tv-mo-high').value = s.tv_mo_high;
    if (s.tv_mo_mid)  $('tv-mo-mid').value  = s.tv_mo_mid;
    if (s.tv_mo_low)  $('tv-mo-low').value  = s.tv_mo_low;
    if (s.trend_tkr) setTimeout(analyse, 400);
  }

  return { init, analyse, toggleMO, quickTF };
})();

window.Trend = Trend;
document.addEventListener('DOMContentLoaded', Trend.init);
