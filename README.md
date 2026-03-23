# ⚡ Quant Scanner – Improved Project

A fully revamped technical analysis repo combining fast pivot detection, zone scanning, confluence scoring, and a live dashboard frontend with TradingView Lightweight Charts.

---

## Project Structure

```
quant_project/
├── algos/
│   ├── pivot_engine.py      # Core pivot detection (vectorised, 10-50x faster)
│   ├── zones.py             # RBR/DBR zone scanner (unified, parameterised)
│   ├── jratio.py            # J-Ratio (vectorised, was O(n²) loop)
│   ├── patterns.py          # Flag, triangle, channel, divergence
│   └── confluence.py        # Confluence scorer + HTF context + position sizing
├── data_fetch/
│   └── data_fetch.py        # Unified adapter (yfinance + disk caching)
├── scanner.py               # Multi-stock scanner engine
├── api/
│   └── app.py               # Flask REST API (port 5050)
├── frontend/
│   ├── index.html           # Scanner dashboard (filtered stocks + chart)
│   └── trend_viewer.html    # Trend structure visualiser (interactive params)
├── notebooks/
│   ├── 01_zones_finder_plotter.ipynb   # Zone finder (replaces original)
│   ├── 02_strategy_scanner.ipynb       # Watchlist scanner
│   └── 03_edge_measurement.ipynb       # Forward-return edge analysis
└── requirements.txt
```

---

## Setup

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start the API server
cd api
python app.py
# Server runs at http://localhost:5050

# 3. Open the frontend
# Open frontend/index.html in your browser
# (or serve with: python -m http.server 8080 --directory frontend)

# 4. Run notebooks
cd notebooks
jupyter notebook
```

---

## Key Improvements

### Pivot Engine (`algos/pivot_engine.py`)
- **Vectorised** using `pd.rolling` — 10-50x faster than the original Python loop
- **Strict T/B alternation enforced** in a single cleanup pass (original had duplicate same-type pivots)
- **ATR-based `min_strength`** filter to remove noise pivots
- **Wilder's ATR** (correct smoothing) replaces the mixed EWM implementation
- Dead code removed — single `extrems()` entry point

### Zone Scanner (`algos/zones.py`)
- All magic numbers (`1.35`, `0.55`, `0.65`) extracted as named parameters with a `multiplier` arg
- ATR computed once per call, not per-loop iteration
- `buy_zone` and `sell_zone` share a unified `_zone_scanner()` core
- 5m variant is just `buy_zone(..., multiplier=0.85)` — no duplicate code

### J-Ratio (`algos/jratio.py`)
- Was O(n²) Python loop over numpy arrays
- Rewritten using vectorised `pd.Series` ops — ~50x faster on 5k-bar frames

### New: Confluence Scorer (`algos/confluence.py`)
- Combines trend score + zone quality + HTF alignment + volatility regime
- `HTFContext` class auto-detects weekly/daily structure
- ATR-based position sizing engine
- Forward-return edge measurement with hit-rate, avg R, payoff ratio

### New: Data Pipeline (`data_fetch/data_fetch.py`)
- Disk-cache using Parquet (no re-downloading on notebook restart)
- Parallel batch fetch with `ThreadPoolExecutor`
- Built-in Nifty 50 / Next 50 / Midcap watchlists
- Path is portable (no hardcoded Mac paths)

### API + Frontend
- Flask REST API serves OHLCV, pivots, zones, trend data
- **Scanner page**: ranked setups with confluence scores, one-click chart
- **Trend Viewer**: input any symbol + parameters, see full pivot structure with TradingView charts

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | Health check |
| GET | `/api/scan?watchlist=nifty50&direction=buy` | Run scanner |
| GET | `/api/chart/<ticker>?interval=1d&order=5` | OHLCV + pivots + zones |
| GET | `/api/trend/<ticker>?order=5&days=500` | Full trend analysis |
| POST | `/api/trend` | Trend with body params |

### Scan parameters
- `watchlist`: `nifty50`, `next50`, `midcap`, `all_nifty`
- `direction`: `buy` | `sell`
- `interval`: `1d`, `1wk`, `1h`
- `min_confluence`: float 0–1 (default `0.45`)
- `order`: int (default `5`)

### Trend parameters
- `order`: pivot look-left/right bars
- `min_strength_pct`: min amplitude as % of price
- `atr_mult`: min amplitude as ATR multiples
- `days`: lookback days

---

## Notebooks

| Notebook | Purpose |
|----------|---------|
| `01_zones_finder_plotter.ipynb` | Single-stock zone + pivot chart (replaces original) |
| `02_strategy_scanner.ipynb` | Scan watchlist, ranked results, plot top setup |
| `03_edge_measurement.ipynb` | Forward-return tagging, hit-rate, walk-forward split |
