from __future__ import annotations

import csv
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
    describe_filter_conditions,
    export_filter_snapshot_payload,
    query_filter_snapshot_history,
    query_model3_2024,
    query_model_y_hw4,
    track_filter_snapshot,
)
from app.models import Listing, RunLog, utcnow

DEFAULT_MAX_MILES = 40000


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


def serialize_listing_safe(row: Listing) -> dict[str, Any]:
    raw = row.raw if isinstance(row.raw, dict) else {}
    dealer = raw.get("dealer") if isinstance(raw.get("dealer"), dict) else {}
    mc_dealership = (
        raw.get("mc_dealership") if isinstance(raw.get("mc_dealership"), dict) else {}
    )
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


def _history_rows_model_y(rows: list[Any]) -> list[dict[str, Any]]:
    return [
        {
            "date": row.snapshot_date.isoformat(),
            "count": row.model_y_count,
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
    latest_run = session.execute(
        select(RunLog).order_by(RunLog.started_at.desc()).limit(1)
    ).scalar_one_or_none()

    return {
        "generated_at": utcnow().isoformat(),
        "filter_description": describe_filter_conditions(filters),
        "state": filters.state,
        "max_miles": filters.max_miles,
        "model_y_results": [serialize_listing_safe(row) for row in model_y_rows],
        "model_3_results": [serialize_listing_safe(row) for row in model_3_rows],
        "history_model_y": _history_rows_model_y(history_rows),
        "history_model_3": _history_rows_model_3(history_rows),
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
    template = env.get_template("pages_index.html")
    output_dir.mkdir(parents=True, exist_ok=True)
    html = template.render(payload=payload)
    (output_dir / "index.html").write_text(html, encoding="utf-8")


def write_site_payload_files(
    session: Session,
    output_dir: Path,
    default_state: str,
) -> dict[str, Any]:
    payload = build_site_payload(session, default_state)
    data_dir = output_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    (data_dir / "site_payload.json").write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    current_rows = payload["model_y_results"] + payload["model_3_results"]
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
