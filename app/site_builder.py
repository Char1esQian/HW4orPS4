from __future__ import annotations

import csv
import hashlib
import io
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.ingestion import (
    ListingFilters,
    build_fingerprint,
    describe_filter_conditions,
    export_filter_snapshot_payload,
    query_filter_snapshot_history,
    query_model3_2024,
    query_model_y_hw4,
    query_trim_options,
    track_filter_snapshot,
)
from app.models import Listing, RunLog, utcnow

DEFAULT_MAX_MILES = 40000
LISTING_SEEN_EXPORT_SCHEMA = "hw4finder.listing_seen.v1"


def build_default_filters(default_state: str) -> ListingFilters:
    return ListingFilters(
        state=(default_state or "MA").upper(),
        max_miles=DEFAULT_MAX_MILES,
    )


def _days_seen(first_seen: datetime | None) -> int | None:
    if first_seen is None:
        return None
    delta = utcnow().date() - first_seen.date()
    return max(0, delta.days)


def _listing_seen_identity(row: Listing) -> str:
    if row.vin:
        base = f"vin|{row.vin}"
    elif row.url:
        base = f"url|{row.source}|{row.url}"
    else:
        raw = row.raw if isinstance(row.raw, dict) else {}
        heading = raw.get("heading")
        fingerprint = row.fingerprint or build_fingerprint(row.source, heading, row.price, row.city)
        base = f"fingerprint|{row.source}|{fingerprint}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def import_listing_seen_payload(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    if payload.get("schema") != LISTING_SEEN_EXPORT_SCHEMA:
        raise ValueError("Unsupported listing seen payload schema.")

    entries: dict[str, dict[str, Any]] = {}
    for item in payload.get("listings", []):
        if not isinstance(item, dict):
            continue
        identity = str(item.get("identity") or "").strip()
        if not identity:
            continue
        first_seen_raw = item.get("first_seen")
        last_seen_raw = item.get("last_seen")
        try:
            first_seen = (
                datetime.fromisoformat(str(first_seen_raw)) if first_seen_raw else None
            )
        except ValueError:
            first_seen = None
        try:
            last_seen = datetime.fromisoformat(str(last_seen_raw)) if last_seen_raw else None
        except ValueError:
            last_seen = None
        entries[identity] = {
            "first_seen": first_seen,
            "last_seen": last_seen,
            "is_available": bool(item.get("is_available", False)),
        }
    return entries


def merge_listing_seen_history(
    session: Session,
    persisted_entries: dict[str, dict[str, Any]],
) -> int:
    updated = 0
    rows = list(session.execute(select(Listing)).scalars().all())
    for row in rows:
        entry = persisted_entries.get(_listing_seen_identity(row))
        if not entry:
            continue
        prior_first_seen = entry.get("first_seen")
        if isinstance(prior_first_seen, datetime) and prior_first_seen < row.first_seen:
            row.first_seen = prior_first_seen
            updated += 1
        prior_last_seen = entry.get("last_seen")
        if isinstance(prior_last_seen, datetime) and prior_last_seen > row.last_seen:
            row.last_seen = prior_last_seen
            updated += 1
    if updated:
        session.commit()
    return updated


def export_listing_seen_payload(session: Session) -> dict[str, Any]:
    rows = list(
        session.execute(
            select(Listing).where(Listing.source == "marketcheck").order_by(Listing.first_seen.asc())
        ).scalars()
    )
    return {
        "schema": LISTING_SEEN_EXPORT_SCHEMA,
        "exported_at": utcnow().isoformat(),
        "count": len(rows),
        "listings": [
            {
                "identity": _listing_seen_identity(row),
                "first_seen": row.first_seen.isoformat() if row.first_seen else None,
                "last_seen": row.last_seen.isoformat() if row.last_seen else None,
                "is_available": bool(row.is_available),
            }
            for row in rows
        ],
    }


def _safe_raw_subset(row: Listing) -> dict[str, Any]:
    raw = row.raw if isinstance(row.raw, dict) else {}
    dealer = raw.get("dealer") if isinstance(raw.get("dealer"), dict) else {}
    mc_dealership = (
        raw.get("mc_dealership") if isinstance(raw.get("mc_dealership"), dict) else {}
    )
    return {
        "exterior_color": raw.get("exterior_color"),
        "base_ext_color": raw.get("base_ext_color"),
        "interior_color": raw.get("interior_color"),
        "base_int_color": raw.get("base_int_color"),
        "carfax_clean_title": raw.get("carfax_clean_title"),
        "carfax_1_owner": raw.get("carfax_1_owner"),
        "autocheck_clean_title": raw.get("autocheck_clean_title"),
        "autocheck_1_owner": raw.get("autocheck_1_owner"),
        "_fsd_mentioned": raw.get("_fsd_mentioned"),
        "_fsd_source": raw.get("_fsd_source"),
        "dealer": {
            "latitude": dealer.get("latitude"),
            "longitude": dealer.get("longitude"),
        },
        "mc_dealership": {
            "latitude": mc_dealership.get("latitude"),
            "longitude": mc_dealership.get("longitude"),
        },
    }


def serialize_listing_safe(row: Listing) -> dict[str, Any]:
    raw_subset = _safe_raw_subset(row)
    dealer = raw_subset.get("dealer", {})
    mc_dealership = raw_subset.get("mc_dealership", {})
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
        "dealer_latitude": dealer.get("latitude") or mc_dealership.get("latitude"),
        "dealer_longitude": dealer.get("longitude") or mc_dealership.get("longitude"),
        "first_seen": row.first_seen.isoformat() if row.first_seen else None,
        "last_seen": row.last_seen.isoformat() if row.last_seen else None,
        "days_seen": _days_seen(row.first_seen),
        "hw4_likely": row.hw4_likely,
        "hw4_reason": row.hw4_reason,
    }


def _serialize_listing_for_template(row: Listing) -> dict[str, Any]:
    safe_row = serialize_listing_safe(row)
    safe_row["first_seen"] = row.first_seen
    safe_row["last_seen"] = row.last_seen
    safe_row["raw"] = _safe_raw_subset(row)
    return safe_row


def _history_rows_model_y(rows: list[Any]) -> list[dict[str, Any]]:
    return [
        {
            "date": row.snapshot_date.isoformat(),
            "count": row.model_y_count,
            "model_y_count": row.model_y_count,
            "price_lowest": row.model_y_price_lowest,
            "price_q1": row.model_y_price_q1,
            "price_median": row.model_y_price_median,
            "price_q3": row.model_y_price_q3,
        }
        for row in rows
    ]


def _history_rows_model_3(rows: list[Any]) -> list[dict[str, Any]]:
    return [
        {
            "date": row.snapshot_date.isoformat(),
            "count": row.model_3_count,
            "model_3_count": row.model_3_count,
            "price_lowest": row.model_3_price_lowest,
            "price_q1": row.model_3_price_q1,
            "price_median": row.model_3_price_median,
            "price_q3": row.model_3_price_q3,
        }
        for row in rows
    ]


def build_site_payload(session: Session, default_state: str) -> dict[str, Any]:
    filters = build_default_filters(default_state)
    model_y_rows = query_model_y_hw4(session, filters)
    model_3_rows = query_model3_2024(session, filters)
    track_filter_snapshot(session, filters, model_y_rows=model_y_rows, model_3_rows=model_3_rows)
    history_rows = query_filter_snapshot_history(session, filters)
    trim_options = query_trim_options(session, filters)
    latest_run = session.execute(
        select(RunLog).order_by(RunLog.started_at.desc()).limit(1)
    ).scalar_one_or_none()

    return {
        "static_mode": True,
        "generated_at": utcnow().isoformat(),
        "filters": filters,
        "history_filter_description": describe_filter_conditions(filters),
        "filter_description": describe_filter_conditions(filters),
        "state": filters.state,
        "max_miles": filters.max_miles,
        "default_max_miles": DEFAULT_MAX_MILES,
        "trim_options": trim_options,
        "model_y_results": [_serialize_listing_for_template(row) for row in model_y_rows],
        "model_3_results": [_serialize_listing_for_template(row) for row in model_3_rows],
        "json_model_y_results": [serialize_listing_safe(row) for row in model_y_rows],
        "json_model_3_results": [serialize_listing_safe(row) for row in model_3_rows],
        "history_model_y": _history_rows_model_y(history_rows),
        "history_model_3": _history_rows_model_3(history_rows),
        "refresh_action": "#",
        "scan_fsd_action": "#",
        "export_csv_url": "./data/listings.csv",
        "export_json_url": "./data/listings.json",
        "export_history_url": "./data/history_snapshots.safe.json",
        "run_status": None,
        "found": None,
        "upserted": None,
        "error": None,
        "fsd_scan_status": None,
        "fsd_scanned": None,
        "fsd_new": None,
        "fsd_scan_error": None,
        "latest_run": {
            "status": latest_run.status if latest_run else None,
            "started_at": latest_run.started_at.isoformat() if latest_run and latest_run.started_at else None,
            "ended_at": latest_run.ended_at.isoformat() if latest_run and latest_run.ended_at else None,
            "items_found": latest_run.items_found if latest_run else 0,
            "items_upserted": latest_run.items_upserted if latest_run else 0,
            "error_text": latest_run.error_text if latest_run else None,
        },
    }


def render_static_index(payload: dict[str, Any], output_dir: Path) -> None:
    templates_dir = Path(__file__).resolve().parent / "templates"
    env = Environment(
        loader=FileSystemLoader(str(templates_dir)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    template = env.get_template("index.html")
    output_dir.mkdir(parents=True, exist_ok=True)
    html = template.render(days_seen_for=_days_seen, **payload)
    (output_dir / "index.html").write_text(html, encoding="utf-8")


def write_site_payload_files(
    session: Session,
    output_dir: Path,
    default_state: str,
) -> dict[str, Any]:
    payload = build_site_payload(session, default_state)
    data_dir = output_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    json_model_y_results = payload["json_model_y_results"]
    json_model_3_results = payload["json_model_3_results"]
    site_payload = {
        "generated_at": payload["generated_at"],
        "filter_description": payload["filter_description"],
        "state": payload["state"],
        "max_miles": payload["max_miles"],
        "model_y_results": json_model_y_results,
        "model_3_results": json_model_3_results,
        "history_model_y": payload["history_model_y"],
        "history_model_3": payload["history_model_3"],
        "latest_run": payload["latest_run"],
    }

    (data_dir / "site_payload.json").write_text(
        json.dumps(site_payload, indent=2),
        encoding="utf-8",
    )

    current_rows = json_model_y_results + json_model_3_results
    (data_dir / "listings.json").write_text(
        json.dumps(current_rows, indent=2),
        encoding="utf-8",
    )

    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(
        [
            "id",
            "model",
            "year",
            "trim",
            "price",
            "mileage",
            "city",
            "state",
            "dealer_name",
            "days_seen",
            "hw4_likely",
            "hw4_reason",
            "url",
            "vin",
            "last_seen",
        ]
    )
    for row in current_rows:
        writer.writerow(
            [
                row.get("id", ""),
                row.get("model", ""),
                row.get("year", ""),
                row.get("trim", ""),
                row.get("price", ""),
                row.get("mileage", ""),
                row.get("city", ""),
                row.get("state", ""),
                row.get("dealer_name", ""),
                row.get("days_seen", ""),
                row.get("hw4_likely", ""),
                row.get("hw4_reason", ""),
                row.get("url", ""),
                row.get("vin", ""),
                row.get("last_seen", ""),
            ]
        )
    (data_dir / "listings.csv").write_text(out.getvalue(), encoding="utf-8")

    history_payload = export_filter_snapshot_payload(session, state=default_state)
    (data_dir / "history_snapshots.safe.json").write_text(
        json.dumps(history_payload, indent=2),
        encoding="utf-8",
    )

    render_static_index(payload, output_dir)
    return payload
