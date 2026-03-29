from __future__ import annotations

import csv
import io
from datetime import datetime
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
    describe_filter_conditions,
    query_export_rows,
    query_filter_snapshot_history,
    query_model3_2024,
    query_model_y_hw4,
    query_trim_options,
    refresh_marketcheck,
    scan_fsd_mentions,
    track_filter_snapshot,
)
from app.models import Listing, RunLog, utcnow

app = FastAPI(title="HW4 Finder", version="0.1.0")
templates = Jinja2Templates(directory="app/templates")
DEFAULT_MAX_MILES = 40000


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
    clean_title_values: tuple[str, ...] = (),
    one_owner_values: tuple[str, ...] = (),
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
        clean_title_values=clean_title_values,
        one_owner_values=one_owner_values,
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


def _parse_max_miles(value: str | None) -> int:
    parsed = _parse_optional_int(value)
    if parsed is None:
        return DEFAULT_MAX_MILES
    return max(0, parsed)


def _parse_carfax_values(values: list[str] | None) -> tuple[str, ...]:
    if not values:
        return ()

    parsed: list[str] = []
    seen: set[str] = set()
    for raw in values:
        text = str(raw).strip().lower()
        if text in {"1", "true", "yes", "y"}:
            normalized = "yes"
        elif text in {"0", "false", "no", "n"}:
            normalized = "no"
        elif text in {"unknown", "unk", "none", "null"}:
            normalized = "unknown"
        else:
            continue

        if normalized in seen:
            continue
        seen.add(normalized)
        parsed.append(normalized)
    return tuple(parsed)


def _merge_history_values(*groups: list[str] | None) -> tuple[str, ...]:
    merged: list[str] = []
    for group in groups:
        if group:
            merged.extend(group)
    return _parse_carfax_values(merged)


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
        "days_seen": _days_seen(row.first_seen),
        "hw4_likely": row.hw4_likely,
        "hw4_reason": row.hw4_reason,
        "raw": row.raw,
    }


def _days_seen(first_seen: datetime | None) -> int | None:
    if first_seen is None:
        return None
    delta = utcnow().date() - first_seen.date()
    return max(0, delta.days)


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
    clean_title: list[str] | None = Query(None),
    one_owner: list[str] | None = Query(None),
    carfax_clean_title: list[str] | None = Query(None),
    carfax_1_owner: list[str] | None = Query(None),
    autocheck_clean_title: list[str] | None = Query(None),
    autocheck_1_owner: list[str] | None = Query(None),
    run_status: str | None = Query(None),
    found: int | None = Query(None),
    upserted: int | None = Query(None),
    error: str | None = Query(None),
    fsd_scan_status: str | None = Query(None),
    fsd_scanned: int | None = Query(None),
    fsd_new: int | None = Query(None),
    fsd_scan_error: str | None = Query(None),
    db: Session = Depends(get_db),
):
    settings = get_settings()
    filters = _build_filters(
        settings.default_state,
        state=state,
        min_price=_parse_optional_int(min_price),
        max_price=_parse_optional_int(max_price),
        min_miles=_parse_optional_int(min_miles),
        max_miles=_parse_max_miles(max_miles),
        trim=trim,
        year_min=_parse_optional_int(year_min),
        year_max=_parse_optional_int(year_max),
        clean_title_values=_merge_history_values(
            clean_title, carfax_clean_title, autocheck_clean_title
        ),
        one_owner_values=_merge_history_values(
            one_owner, carfax_1_owner, autocheck_1_owner
        ),
    )
    model_y = query_model_y_hw4(db, filters)
    model_3 = query_model3_2024(db, filters)
    track_filter_snapshot(db, filters, model_y_rows=model_y, model_3_rows=model_3)
    history_rows = query_filter_snapshot_history(db, filters)
    history_model_y = [
        {
            "date": row.snapshot_date.isoformat(),
            "model_y_count": row.model_y_count,
            "price_lowest": row.model_y_price_lowest,
            "price_q1": row.model_y_price_q1,
            "price_median": row.model_y_price_median,
            "price_q3": row.model_y_price_q3,
        }
        for row in history_rows
    ]
    history_model_3 = [
        {
            "date": row.snapshot_date.isoformat(),
            "model_3_count": row.model_3_count,
            "price_lowest": row.model_3_price_lowest,
            "price_q1": row.model_3_price_q1,
            "price_median": row.model_3_price_median,
            "price_q3": row.model_3_price_q3,
        }
        for row in history_rows
    ]
    trim_options = query_trim_options(db, filters)
    latest_run = db.execute(
        select(RunLog).order_by(RunLog.started_at.desc()).limit(1)
    ).scalar_one_or_none()

    query_pairs = [
        (key, value)
        for key, value in request.query_params.multi_items()
        if key
        not in {
            "run_status",
            "found",
            "upserted",
            "error",
            "fsd_scan_status",
            "fsd_scanned",
            "fsd_new",
            "fsd_scan_error",
        }
    ]
    qs = urlencode(query_pairs, doseq=True)
    suffix = f"?{qs}" if qs else ""

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "filters": filters,
            "model_y_results": model_y,
            "model_3_results": model_3,
            "trim_options": trim_options,
            "history_model_y": history_model_y,
            "history_model_3": history_model_3,
            "history_filter_description": describe_filter_conditions(filters),
            "default_max_miles": DEFAULT_MAX_MILES,
            "refresh_action": f"/refresh{suffix}",
            "scan_fsd_action": f"/scan-fsd{suffix}",
            "export_csv_url": f"/export.csv{suffix}",
            "export_json_url": f"/export.json{suffix}",
            "run_status": run_status,
            "found": found,
            "upserted": upserted,
            "error": error,
            "fsd_scan_status": fsd_scan_status,
            "fsd_scanned": fsd_scanned,
            "fsd_new": fsd_new,
            "fsd_scan_error": fsd_scan_error,
            "days_seen_for": _days_seen,
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

    query_pairs = list(request.query_params.multi_items())
    query_pairs.append(("run_status", run.status))
    query_pairs.append(("found", str(run.items_found)))
    query_pairs.append(("upserted", str(run.items_upserted)))
    if run.error_text:
        query_pairs.append(("error", run.error_text))
    redirect_to = "/"
    if query_pairs:
        redirect_to = f"/?{urlencode(query_pairs, doseq=True)}"
    return RedirectResponse(url=redirect_to, status_code=303)


@app.post("/scan-fsd")
def scan_fsd(
    request: Request,
    state: str | None = Query(None),
    min_price: str | None = Query(None),
    max_price: str | None = Query(None),
    min_miles: str | None = Query(None),
    max_miles: str | None = Query(None),
    trim: str | None = Query(None),
    year_min: str | None = Query(None),
    year_max: str | None = Query(None),
    clean_title: list[str] | None = Query(None),
    one_owner: list[str] | None = Query(None),
    carfax_clean_title: list[str] | None = Query(None),
    carfax_1_owner: list[str] | None = Query(None),
    autocheck_clean_title: list[str] | None = Query(None),
    autocheck_1_owner: list[str] | None = Query(None),
    db: Session = Depends(get_db),
):
    settings = get_settings()
    filters = _build_filters(
        settings.default_state,
        state=state,
        min_price=_parse_optional_int(min_price),
        max_price=_parse_optional_int(max_price),
        min_miles=_parse_optional_int(min_miles),
        max_miles=_parse_max_miles(max_miles),
        trim=trim,
        year_min=_parse_optional_int(year_min),
        year_max=_parse_optional_int(year_max),
        clean_title_values=_merge_history_values(
            clean_title, carfax_clean_title, autocheck_clean_title
        ),
        one_owner_values=_merge_history_values(
            one_owner, carfax_1_owner, autocheck_1_owner
        ),
    )

    query_pairs = [
        (key, value)
        for key, value in request.query_params.multi_items()
        if key not in {"fsd_scan_status", "fsd_scanned", "fsd_new", "fsd_scan_error"}
    ]

    try:
        scanned, newly_found = scan_fsd_mentions(db, filters)
        query_pairs.append(("fsd_scan_status", "success"))
        query_pairs.append(("fsd_scanned", str(scanned)))
        query_pairs.append(("fsd_new", str(newly_found)))
    except Exception as exc:  # noqa: BLE001
        query_pairs.append(("fsd_scan_status", "failed"))
        query_pairs.append(("fsd_scanned", "0"))
        query_pairs.append(("fsd_new", "0"))
        query_pairs.append(("fsd_scan_error", str(exc)))

    redirect_to = "/"
    if query_pairs:
        redirect_to = f"/?{urlencode(query_pairs, doseq=True)}"
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
    clean_title: list[str] | None = Query(None),
    one_owner: list[str] | None = Query(None),
    carfax_clean_title: list[str] | None = Query(None),
    carfax_1_owner: list[str] | None = Query(None),
    autocheck_clean_title: list[str] | None = Query(None),
    autocheck_1_owner: list[str] | None = Query(None),
    db: Session = Depends(get_db),
):
    settings = get_settings()
    filters = _build_filters(
        settings.default_state,
        state=state,
        min_price=_parse_optional_int(min_price),
        max_price=_parse_optional_int(max_price),
        min_miles=_parse_optional_int(min_miles),
        max_miles=_parse_max_miles(max_miles),
        trim=trim,
        year_min=_parse_optional_int(year_min),
        year_max=_parse_optional_int(year_max),
        clean_title_values=_merge_history_values(
            clean_title, carfax_clean_title, autocheck_clean_title
        ),
        one_owner_values=_merge_history_values(
            one_owner, carfax_1_owner, autocheck_1_owner
        ),
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
    clean_title: list[str] | None = Query(None),
    one_owner: list[str] | None = Query(None),
    carfax_clean_title: list[str] | None = Query(None),
    carfax_1_owner: list[str] | None = Query(None),
    autocheck_clean_title: list[str] | None = Query(None),
    autocheck_1_owner: list[str] | None = Query(None),
    db: Session = Depends(get_db),
):
    settings = get_settings()
    filters = _build_filters(
        settings.default_state,
        state=state,
        min_price=_parse_optional_int(min_price),
        max_price=_parse_optional_int(max_price),
        min_miles=_parse_optional_int(min_miles),
        max_miles=_parse_max_miles(max_miles),
        trim=trim,
        year_min=_parse_optional_int(year_min),
        year_max=_parse_optional_int(year_max),
        clean_title_values=_merge_history_values(
            clean_title, carfax_clean_title, autocheck_clean_title
        ),
        one_owner_values=_merge_history_values(
            one_owner, carfax_1_owner, autocheck_1_owner
        ),
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
            "days_seen",
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
                _days_seen(row.first_seen) if row.first_seen else "",
                str(row.hw4_likely),
                row.hw4_reason or "",
            ]
        )

    return Response(
        content=out.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="hw4finder_export.csv"'},
    )
