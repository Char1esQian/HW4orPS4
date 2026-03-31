from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.ingestion import export_filter_snapshot_payload, import_filter_snapshot_payload
from app.models import Base, FilterSnapshot


def build_session() -> Session:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    return factory()


def test_export_history_payload_is_safe_and_structured() -> None:
    session = build_session()
    row = FilterSnapshot(
        snapshot_date=date(2026, 2, 10),
        filter_signature="abc123",
        filter_description="state=MA | max_miles=40000",
        state="MA",
        max_miles=40000,
        clean_title_values=["yes"],
        one_owner_values=["unknown"],
        model_y_count=5,
        model_3_count=2,
        model_y_price_lowest=33000,
        model_y_price_q1=35000,
        model_y_price_median=37000,
        model_y_price_q3=39000,
        model_3_price_lowest=28000,
        model_3_price_q1=30000,
        model_3_price_median=32000,
        model_3_price_q3=34000,
    )
    session.add(row)
    session.commit()

    payload = export_filter_snapshot_payload(session)
    assert payload["schema"] == "hw4finder.filter_snapshots.v1"
    assert payload["count"] == 1
    entry = payload["snapshots"][0]
    assert entry["filter_signature"] == "abc123"
    assert entry["snapshot_date"] == "2026-02-10"
    assert "raw" not in entry
    assert "vin" not in entry
    assert entry["model_y_count"] == 5


def test_import_history_payload_inserts_and_updates() -> None:
    session = build_session()
    payload = {
        "schema": "hw4finder.filter_snapshots.v1",
        "snapshots": [
            {
                "snapshot_date": "2026-02-11",
                "filter_signature": "sig-1",
                "filter_description": "state=MA",
                "state": "MA",
                "clean_title_values": ["yes"],
                "one_owner_values": ["unknown"],
                "model_y_count": 3,
                "model_3_count": 4,
                "model_y_price_median": 36000,
            }
        ],
    }
    first = import_filter_snapshot_payload(session, payload)
    assert first["inserted"] == 1
    assert first["updated"] == 0

    payload["snapshots"][0]["model_y_count"] = 9
    second = import_filter_snapshot_payload(session, payload)
    assert second["inserted"] == 0
    assert second["updated"] == 1

    row = session.execute(
        select(FilterSnapshot).where(
            FilterSnapshot.filter_signature == "sig-1",
            FilterSnapshot.snapshot_date == date(2026, 2, 11),
        )
    ).scalar_one()
    assert row.model_y_count == 9

