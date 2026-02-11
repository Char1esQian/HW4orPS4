from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.ingestion import adapt_marketcheck_item, upsert_listings
from app.models import Base, Listing


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
