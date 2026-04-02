from __future__ import annotations

from datetime import datetime

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.models import Base, Listing
from app.site_builder import (
    export_listing_seen_payload,
    import_listing_seen_payload,
    merge_listing_seen_history,
)


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


def test_listing_seen_round_trip_uses_hashed_identity_only() -> None:
    session = build_session()
    row = Listing(
        source="marketcheck",
        url="https://example.com/car",
        vin="5YJ3E1EA5LF784963",
        model="3",
        year=2024,
        price=35000,
        mileage=12000,
        city="Boston",
        state="MA",
        first_seen=datetime(2026, 3, 20, 12, 0, 0),
        last_seen=datetime(2026, 3, 30, 12, 0, 0),
        is_available=True,
        hw4_likely=False,
        hw4_reason="HW4 heuristic applies only to Model Y.",
        raw={"heading": "Model 3", "dealer": {"latitude": "42.1", "longitude": "-71.0"}},
    )
    session.add(row)
    session.commit()

    payload = export_listing_seen_payload(session)
    assert payload["count"] == 1
    item = payload["listings"][0]
    assert "identity" in item
    assert "vin" not in str(item).lower()
    assert "https://example.com/car" not in str(item)

    imported = import_listing_seen_payload(payload)
    assert len(imported) == 1


def test_merge_listing_seen_history_preserves_earlier_first_seen() -> None:
    session = build_session()
    row = Listing(
        source="marketcheck",
        url="https://example.com/car",
        vin="5YJ3E1EA5LF784963",
        model="3",
        year=2024,
        price=35000,
        mileage=12000,
        city="Boston",
        state="MA",
        first_seen=datetime(2026, 4, 2, 12, 0, 0),
        last_seen=datetime(2026, 4, 2, 12, 0, 0),
        is_available=True,
        hw4_likely=False,
        hw4_reason="HW4 heuristic applies only to Model Y.",
        raw={"heading": "Model 3"},
    )
    session.add(row)
    session.commit()

    prior_payload = {
        "schema": "hw4finder.listing_seen.v1",
        "exported_at": "2026-04-02T12:00:00",
        "count": 1,
        "listings": [
            {
                "identity": export_listing_seen_payload(session)["listings"][0]["identity"],
                "first_seen": "2026-03-20T12:00:00",
                "last_seen": "2026-04-01T12:00:00",
                "is_available": False,
            }
        ],
    }
    changed = merge_listing_seen_history(session, import_listing_seen_payload(prior_payload))
    assert changed == 1

    refreshed = session.execute(select(Listing)).scalar_one()
    assert refreshed.first_seen == datetime(2026, 3, 20, 12, 0, 0)
    assert refreshed.last_seen == datetime(2026, 4, 2, 12, 0, 0)
