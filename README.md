# HW4 Finder

Minimal local app to ingest Tesla listings from MarketCheck, store in SQLite, and show:
- Model Y in MA that are likely HW4 using VIN thresholds
- Model 3 in MA with year >= 2024
- Only currently available listings from the latest successful refresh

## Stack
- Python 3.11+
- FastAPI + Jinja templates
- SQLAlchemy + SQLite
- requests

## Quick start
```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
uvicorn app.main:app --reload
```

Then open: `http://127.0.0.1:8000/`

## GitHub Pages daily hosting
This repo can also be hosted as a static GitHub Pages site that rebuilds once per day.

### What stays private
- `MARKETCHECK_API_KEY` should be stored only as a GitHub Actions secret.
- The generated Pages site exports only safe listing fields and safe trend history.
- Raw listing payloads, `.env`, and local SQLite DB are not published.

### One-time GitHub setup
1. In GitHub repo settings, add Actions secret `MARKETCHECK_API_KEY`.
2. In GitHub repo settings, enable Pages and set the source to `GitHub Actions`.
3. Run the workflow `Build and Deploy GitHub Pages` once manually, or wait for the daily schedule.

### What the workflow does
- imports `history_snapshots.safe.json` from the repo if present
- imports `listing_seen.safe.json` from the repo if present so hosted `days seen` stays stable across daily rebuilds
- fetches current MarketCheck listings using the GitHub secret
- rebuilds a static site into `site/`
- deploys `site/` to GitHub Pages
- commits back `history_snapshots.safe.json` and `listing_seen.safe.json` so trend history and `days seen` persist across daily runs

### Local static build
You can test the Pages build locally:
```bash
python scripts/build_pages_site.py
```

Generated output:
- `site/index.html`
- `site/data/listings.json`
- `site/data/listings.csv`
- `site/data/history_snapshots.safe.json`
- `history_snapshots.safe.json`
- `listing_seen.safe.json`

## Configure MarketCheck endpoints
This repo does **not** hardcode endpoint paths. Set them after confirming MarketCheck docs.

Options:
- Edit `app/endpoints.json`
- Or set env vars in `.env`:
  - `MARKETCHECK_SEARCH_ENDPOINT`
  - `MARKETCHECK_HEALTH_ENDPOINT`

TODO markers are included in `.env.example`, `app/endpoints.json`, and `app/marketcheck.py`.

## Probe API shape before full refresh
Run one request, print JSON keys, and save sample payload:

```bash
python scripts/test_marketcheck.py --state MA
```

This writes `sample_response.json` in the repo root.

## App routes
- `GET /` dashboard with Refresh + Export buttons and filter query params
- `POST /refresh` fetch + upsert latest listings
- `GET /export.csv` export filtered current results
- `GET /export.json` export filtered current results
- `GET /history/export.json` export safe trend history snapshots (`filter_snapshots` only)
- `POST /history/import.json` import safe trend history snapshots

Supported filter query params:
- `state`
- `min_price`, `max_price`
- `min_miles`, `max_miles`
- `trim`
- `year_min`, `year_max`

## HW4 heuristic for Model Y
- VIN must be 17 chars
- 11th VIN digit:
  - `F` (Fremont): HW4 likely if serial (last 6 digits) >= `789500`
  - `A` (Austin): HW4 likely if serial (last 6 digits) >= `131200`

## Test
```bash
pytest -q
```

Unit tests cover:
- VIN parsing / HW4 threshold logic
- Upsert dedupe behavior (VIN, source+url, fallback fingerprint)

## Safe history export/import (GitHub-friendly)
History trends are stored in SQLite table `filter_snapshots`.  
To share safely across machines, export/import only this table (no raw listings, no API key).

CLI:
```bash
python scripts/history_portability.py export --out history_snapshots.safe.json
python scripts/history_portability.py import --in history_snapshots.safe.json
```

Optional state-scoped export:
```bash
python scripts/history_portability.py export --state MA --out history_snapshots.safe.json
```

`history_snapshots.safe.json` is safe to commit to GitHub.
`listing_seen.safe.json` is also safe to commit to GitHub. It stores only hashed listing identities plus first/last seen timestamps, not raw VINs, listing URLs, or API secrets.

## Availability behavior
Listings that disappear from MarketCheck are marked unavailable after the next successful refresh.
The dashboard, exports, and trend history all use only listings currently marked available.

