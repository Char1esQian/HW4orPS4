from __future__ import annotations

import csv
import io
from urllib.parse import urlencode

from fastapi import Depends, FastAPI, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.database import get_db, init_db
from app.ingestion import (
    ListingFilters,
    query_export_rows,
    query_model3_2024,
    query_model_y_hw4,
    refresh_marketcheck,
)
from app.models import Listing, RunLog

app = FastAPI(title="HW4 Finder", version="0.1.0")
templates = Jinja2Templates(directory="app/templates")


@app.on_event("startup")
def startup_event() -> None:
    init_db()


def _build_filters(
    settings_state: str,
    state: str | None = None,
    min_price: int | None = None,
    max_price: int | None = None,
    min_miles: int | None = None,
    max_miles: int | None = None,
    trim: str | None = None,
    year_min: int | None = None,
    year_max: int | None = None,
) -> ListingFilters:
    return ListingFilters(
        state=(state or settings_state).upper(),
        min_price=min_price,
        max_price=max_price,
        min_miles=min_miles,
        max_miles=max_miles,
        trim=trim,
        year_min=year_min,
        year_max=year_max,
    )


def _parse_optional_int(value: str | None) -> int | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _serialize_listing(row: Listing) -> dict:
    return {
        "id": row.id,
        "source": row.source,
        "url": row.url,
        "vin": row.vin,
        "model": row.model,
        "year": row.year,
        "trim": row.trim,
        "price": row.price,
        "mileage": row.mileage,
        "city": row.city,
        "state": row.state,
        "dealer_name": row.dealer_name,
        "first_seen": row.first_seen.isoformat() if row.first_seen else None,
        "last_seen": row.last_seen.isoformat() if row.last_seen else None,
        "hw4_likely": row.hw4_likely,
        "hw4_reason": row.hw4_reason,
        "raw": row.raw,
    }


@app.get("/")
def index(
    request: Request,
    state: str | None = Query(None),
    min_price: str | None = Query(None),
    max_price: str | None = Query(None),
    min_miles: str | None = Query(None),
    max_miles: str | None = Query(None),
    trim: str | None = Query(None),
    year_min: str | None = Query(None),
    year_max: str | None = Query(None),
    run_status: str | None = Query(None),
    found: int | None = Query(None),
    upserted: int | None = Query(None),
    error: str | None = Query(None),
    db: Session = Depends(get_db),
):
    settings = get_settings()
    filters = _build_filters(
        settings.default_state,
        state=state,
        min_price=_parse_optional_int(min_price),
        max_price=_parse_optional_int(max_price),
        min_miles=_parse_optional_int(min_miles),
        max_miles=_parse_optional_int(max_miles),
        trim=trim,
        year_min=_parse_optional_int(year_min),
        year_max=_parse_optional_int(year_max),
    )
    model_y = query_model_y_hw4(db, filters)
    model_3 = query_model3_2024(db, filters)
    latest_run = db.execute(
        select(RunLog).order_by(RunLog.started_at.desc()).limit(1)
    ).scalar_one_or_none()

    query_dict = dict(request.query_params)
    for key in ("run_status", "found", "upserted", "error"):
        query_dict.pop(key, None)
    qs = urlencode(query_dict)
    suffix = f"?{qs}" if qs else ""

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "filters": filters,
            "model_y_results": model_y,
            "model_3_results": model_3,
            "refresh_action": f"/refresh{suffix}",
            "export_csv_url": f"/export.csv{suffix}",
            "export_json_url": f"/export.json{suffix}",
            "run_status": run_status,
            "found": found,
            "upserted": upserted,
            "error": error,
            "latest_run": latest_run,
        },
    )


@app.post("/refresh")
def refresh(
    request: Request,
    state: str | None = Query(None),
    db: Session = Depends(get_db),
):
    settings = get_settings()
    run = refresh_marketcheck(db, state=(state or settings.default_state))

    query_dict = dict(request.query_params)
    query_dict["run_status"] = run.status
    query_dict["found"] = str(run.items_found)
    query_dict["upserted"] = str(run.items_upserted)
    if run.error_text:
        query_dict["error"] = run.error_text
    redirect_to = "/"
    if query_dict:
        redirect_to = f"/?{urlencode(query_dict)}"
    return RedirectResponse(url=redirect_to, status_code=303)


@app.get("/export.json")
def export_json(
    state: str | None = Query(None),
    min_price: str | None = Query(None),
    max_price: str | None = Query(None),
    min_miles: str | None = Query(None),
    max_miles: str | None = Query(None),
    trim: str | None = Query(None),
    year_min: str | None = Query(None),
    year_max: str | None = Query(None),
    db: Session = Depends(get_db),
):
    settings = get_settings()
    filters = _build_filters(
        settings.default_state,
        state=state,
        min_price=_parse_optional_int(min_price),
        max_price=_parse_optional_int(max_price),
        min_miles=_parse_optional_int(min_miles),
        max_miles=_parse_optional_int(max_miles),
        trim=trim,
        year_min=_parse_optional_int(year_min),
        year_max=_parse_optional_int(year_max),
    )
    rows = query_export_rows(db, filters)
    return JSONResponse(content=[_serialize_listing(row) for row in rows])


@app.get("/export.csv")
def export_csv(
    state: str | None = Query(None),
    min_price: str | None = Query(None),
    max_price: str | None = Query(None),
    min_miles: str | None = Query(None),
    max_miles: str | None = Query(None),
    trim: str | None = Query(None),
    year_min: str | None = Query(None),
    year_max: str | None = Query(None),
    db: Session = Depends(get_db),
):
    settings = get_settings()
    filters = _build_filters(
        settings.default_state,
        state=state,
        min_price=_parse_optional_int(min_price),
        max_price=_parse_optional_int(max_price),
        min_miles=_parse_optional_int(min_miles),
        max_miles=_parse_optional_int(max_miles),
        trim=trim,
        year_min=_parse_optional_int(year_min),
        year_max=_parse_optional_int(year_max),
    )
    rows = query_export_rows(db, filters)

    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(
        [
            "id",
            "source",
            "url",
            "vin",
            "model",
            "year",
            "trim",
            "price",
            "mileage",
            "city",
            "state",
            "dealer_name",
            "first_seen",
            "last_seen",
            "hw4_likely",
            "hw4_reason",
        ]
    )
    for row in rows:
        writer.writerow(
            [
                row.id,
                row.source,
                row.url or "",
                row.vin or "",
                row.model or "",
                row.year if row.year is not None else "",
                row.trim or "",
                row.price if row.price is not None else "",
                row.mileage if row.mileage is not None else "",
                row.city or "",
                row.state or "",
                row.dealer_name or "",
                row.first_seen.isoformat() if row.first_seen else "",
                row.last_seen.isoformat() if row.last_seen else "",
                str(row.hw4_likely),
                row.hw4_reason or "",
            ]
        )

    return Response(
        content=out.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="hw4finder_export.csv"'},
    )
