# HW4 Finder

Minimal local app to ingest Tesla listings from MarketCheck, store in SQLite, and show:
- Model Y in MA that are likely HW4 using VIN thresholds
- Model 3 in MA with year >= 2024

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

