from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.ingestion import (
    ListingFilters,
    adapt_marketcheck_item,
    mark_unavailable_listings,
    query_export_rows,
    upsert_listings,
)
from app.models import Base, Listing, RunLog


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


def test_upsert_dedupe_by_vin() -> None:
    session = build_session()
    now = datetime.utcnow()
    payload1 = adapt_marketcheck_item(
        {
            "vin": "7SAYGDEE8RF789500",
            "model": "Model Y",
            "year": 2024,
            "price": 45000,
            "miles": 5000,
            "city": "Cambridge",
            "state": "MA",
            "heading": "Model Y LR",
            "url": "https://example.com/car1",
        }
    )
    payload2 = adapt_marketcheck_item(
        {
            "vin": "7SAYGDEE8RF789500",
            "model": "Model Y",
            "year": 2024,
            "price": 44000,
            "miles": 5200,
            "city": "Cambridge",
            "state": "MA",
            "heading": "Model Y LR updated",
            "url": "https://example.com/car1-updated",
        }
    )

    upsert_listings(session, [payload1], now=now)
    upsert_listings(session, [payload2], now=now + timedelta(minutes=5))

    rows = list(session.execute(select(Listing)).scalars().all())
    assert len(rows) == 1
    assert rows[0].price == 44000
    assert rows[0].is_available is True
    assert rows[0].last_seen > rows[0].first_seen


def test_upsert_dedupe_by_source_url() -> None:
    session = build_session()
    payload1 = adapt_marketcheck_item(
        {
            "model": "Model 3",
            "year": 2024,
            "price": 39000,
            "city": "Boston",
            "state": "MA",
            "heading": "Model 3",
            "url": "https://example.com/shared",
        }
    )
    payload2 = adapt_marketcheck_item(
        {
            "model": "Model 3",
            "year": 2024,
            "price": 38500,
            "city": "Boston",
            "state": "MA",
            "heading": "Model 3 update",
            "url": "https://example.com/shared",
        }
    )
    upsert_listings(session, [payload1])
    upsert_listings(session, [payload2])
    rows = list(session.execute(select(Listing)).scalars().all())
    assert len(rows) == 1
    assert rows[0].price == 38500


def test_upsert_dedupe_by_fingerprint_when_vin_and_url_missing() -> None:
    session = build_session()
    payload1 = adapt_marketcheck_item(
        {
            "model": "Model 3",
            "year": 2024,
            "price": 41000,
            "city": "Somerville",
            "state": "MA",
            "heading": "No VIN listing",
        }
    )
    payload2 = adapt_marketcheck_item(
        {
            "model": "Model 3",
            "year": 2024,
            "price": 41000,
            "city": "Somerville",
            "state": "MA",
            "heading": "No VIN listing",
        }
    )
    upsert_listings(session, [payload1])
    upsert_listings(session, [payload2])

    rows = list(session.execute(select(Listing)).scalars().all())
    assert len(rows) == 1
    assert rows[0].price == 41000


def test_upsert_fingerprint_allows_distinct_price_without_vin_or_url() -> None:
    session = build_session()
    payload1 = adapt_marketcheck_item(
        {
            "model": "Model 3",
            "year": 2024,
            "price": 41000,
            "city": "Somerville",
            "state": "MA",
            "heading": "No VIN listing",
        }
    )
    payload2 = adapt_marketcheck_item(
        {
            "model": "Model 3",
            "year": 2024,
            "price": 40500,
            "city": "Somerville",
            "state": "MA",
            "heading": "No VIN listing",
        }
    )
    upsert_listings(session, [payload1])
    upsert_listings(session, [payload2])

    rows = list(session.execute(select(Listing)).scalars().all())
    assert len(rows) == 2


def test_upsert_dedupe_duplicate_vin_within_single_batch() -> None:
    session = build_session()
    payload1 = adapt_marketcheck_item(
        {
            "vin": "7SAYGDEE8RF789500",
            "model": "Model Y",
            "year": 2024,
            "price": 45000,
            "miles": 5000,
            "city": "Cambridge",
            "state": "MA",
            "heading": "Model Y LR",
            "url": "https://example.com/car1",
        }
    )
    payload2 = adapt_marketcheck_item(
        {
            "vin": "7SAYGDEE8RF789500",
            "model": "Model Y",
            "year": 2024,
            "price": 44000,
            "miles": 5200,
            "city": "Cambridge",
            "state": "MA",
            "heading": "Model Y LR updated",
            "url": "https://example.com/car1-updated",
        }
    )

    upsert_listings(session, [payload1, payload2])
    rows = list(session.execute(select(Listing)).scalars().all())
    assert len(rows) == 1
    assert rows[0].price == 44000


def test_mark_unavailable_listings_marks_stale_rows_only() -> None:
    session = build_session()
    cutoff = datetime.utcnow()
    stale_payload = adapt_marketcheck_item(
        {
            "vin": "7SAYGDEE8RF789500",
            "model": "Model Y",
            "year": 2024,
            "price": 45000,
            "miles": 5000,
            "city": "Cambridge",
            "state": "MA",
            "heading": "Stale Model Y",
            "url": "https://example.com/stale",
        }
    )
    fresh_payload = adapt_marketcheck_item(
        {
            "vin": "5YJ3E1EA5LF784963",
            "model": "Model 3",
            "year": 2024,
            "price": 35000,
            "miles": 12000,
            "city": "Boston",
            "state": "MA",
            "heading": "Fresh Model 3",
            "url": "https://example.com/fresh",
        }
    )

    upsert_listings(session, [stale_payload], now=cutoff - timedelta(days=1))
    upsert_listings(session, [fresh_payload], now=cutoff + timedelta(minutes=1))

    changed = mark_unavailable_listings(
        session,
        state="MA",
        cutoff_started_at=cutoff,
    )
    assert changed == 1

    rows = list(session.execute(select(Listing).order_by(Listing.url.asc())).scalars().all())
    assert len(rows) == 2
    by_url = {row.url: row for row in rows}
    assert by_url["https://example.com/stale"].is_available is False
    assert by_url["https://example.com/fresh"].is_available is True


def test_query_export_rows_uses_latest_successful_refresh_cutoff() -> None:
    session = build_session()
    cutoff = datetime.utcnow()
    session.add(
        RunLog(
            status="success",
            started_at=cutoff,
            ended_at=cutoff + timedelta(minutes=1),
            items_found=1,
            items_upserted=1,
        )
    )
    session.commit()

    stale_payload = adapt_marketcheck_item(
        {
            "vin": "7SAYGDEE8RF789500",
            "model": "Model Y",
            "year": 2024,
            "price": 45000,
            "miles": 5000,
            "city": "Cambridge",
            "state": "MA",
            "heading": "Stale Model Y",
            "url": "https://example.com/stale-y",
        }
    )
    fresh_payload = adapt_marketcheck_item(
        {
            "vin": "5YJ3E1EA5RF123456",
            "model": "Model 3",
            "year": 2024,
            "price": 35000,
            "miles": 12000,
            "city": "Boston",
            "state": "MA",
            "heading": "Fresh Model 3",
            "url": "https://example.com/fresh-3",
        }
    )
    upsert_listings(session, [stale_payload], now=cutoff - timedelta(days=1))
    upsert_listings(session, [fresh_payload], now=cutoff + timedelta(minutes=1))

    rows = query_export_rows(session, ListingFilters(state="MA"))
    assert len(rows) == 1
    assert rows[0].url == "https://example.com/fresh-3"
