from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.config import Settings, normalize_state_scope, parse_state_scope
from app.ingestion import (
    ListingFilters,
    _filter_signature,
    adapt_marketcheck_item,
    describe_filter_conditions,
    mark_unavailable_listings,
    query_export_rows,
    upsert_listings,
)
from app.marketcheck import MarketCheckClient
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


def build_settings(**overrides: Any) -> Settings:
    values: dict[str, Any] = dict(
        marketcheck_api_key="",
        marketcheck_base_url="https://example.invalid",
        default_state="MA",
        database_url="sqlite:///:memory:",
        marketcheck_timeout_seconds=1,
        marketcheck_page_size=2,
        marketcheck_max_pages=5,
        marketcheck_api_key_header="x-api-key",
        marketcheck_api_key_query_param="api_key",
        marketcheck_api_key_in_query=False,
        listings_key="listings",
        total_pages_key=None,
        page_param="start",
        page_size_param="rows",
        fsd_page_scan_enabled=False,
        fsd_page_scan_timeout_seconds=1,
        fsd_page_scan_workers=1,
        endpoints={"search_listings": "/v2/search/car/active"},
    )
    values.update(overrides)
    return Settings(**values)


def make_payload(vin: str, state: str, url: str) -> dict[str, Any]:
    return adapt_marketcheck_item(
        {
            "vin": vin,
            "model": "Model 3",
            "year": 2024,
            "price": 35000,
            "miles": 12000,
            "city": "Somewhere",
            "state": state,
            "heading": "Model 3",
            "url": url,
        }
    )


def test_parse_state_scope_normalizes_case_whitespace_order_and_duplicates() -> None:
    assert parse_state_scope("nj, ny,pa") == ("NJ", "NY", "PA")
    assert parse_state_scope("PA,NJ,NY,nj") == ("NJ", "NY", "PA")
    assert parse_state_scope("ma") == ("MA",)
    assert parse_state_scope("") == ()
    assert parse_state_scope(None) == ()


def test_normalize_state_scope_falls_back_to_default() -> None:
    assert normalize_state_scope("ny,nj") == "NJ,NY"
    assert normalize_state_scope("MA") == "MA"
    assert normalize_state_scope("", default="NJ") == "NJ"
    assert normalize_state_scope(None, default="nj, ny") == "NJ,NY"


def test_single_state_signature_matches_legacy_format() -> None:
    # Existing history_snapshots.safe.json entries were hashed with state="MA";
    # multi-state support must not change that signature.
    legacy_payload = {
        "state": "MA",
        "min_price": None,
        "max_price": None,
        "min_miles": None,
        "max_miles": 40000,
        "trim": None,
        "year_min": None,
        "year_max": None,
        "clean_title_values": [],
        "one_owner_values": [],
    }
    packed = json.dumps(legacy_payload, sort_keys=True, separators=(",", ":"))
    expected = hashlib.sha256(packed.encode("utf-8")).hexdigest()
    assert _filter_signature(ListingFilters(state="MA", max_miles=40000)) == expected
    assert _filter_signature(ListingFilters(state="ma", max_miles=40000)) == expected


def test_multi_state_signature_is_order_independent() -> None:
    a = ListingFilters(state="NJ,NY,PA")
    b = ListingFilters(state="pa, nj, ny")
    assert _filter_signature(a) == _filter_signature(b)
    assert describe_filter_conditions(b).startswith("state=NJ,NY,PA")


def test_fetch_requests_each_state_in_scope_separately() -> None:
    client = MarketCheckClient(build_settings())
    seen_params: list[dict[str, Any]] = []

    def fake_request_json(endpoint_key: str, params: dict[str, Any] | None = None, retries: int = 4):
        assert endpoint_key == "search_listings"
        seen_params.append(dict(params or {}))
        state = params["state"]
        # One short page per state so pagination stops after the first call.
        return {"listings": [{"vin": f"{state}-VIN", "state": state}], "num_found": 1}

    client.request_json = fake_request_json  # type: ignore[method-assign]
    items = client.fetch_marketcheck_listings(state="ny, nj")

    assert [p["state"] for p in seen_params] == ["NJ", "NY"]
    assert [item["vin"] for item in items] == ["NJ-VIN", "NY-VIN"]
    assert all(p["make"] == "Tesla" for p in seen_params)


def test_query_and_mark_unavailable_respect_multi_state_scope() -> None:
    session = build_session()
    cutoff = datetime.utcnow()
    session.add(
        RunLog(
            status="success",
            started_at=cutoff,
            ended_at=cutoff + timedelta(minutes=1),
            items_found=3,
            items_upserted=3,
        )
    )
    session.commit()

    nj = make_payload("5YJ3E1EA5RF000001", "NJ", "https://example.com/nj")
    ny = make_payload("5YJ3E1EA5RF000002", "NY", "https://example.com/ny")
    ct = make_payload("5YJ3E1EA5RF000003", "CT", "https://example.com/ct")
    upsert_listings(session, [nj, ny, ct], now=cutoff + timedelta(minutes=1))

    rows = query_export_rows(session, ListingFilters(state="NJ,NY"))
    assert sorted(row.state for row in rows) == ["NJ", "NY"]

    # A stale listing is only retired when its state is inside the refresh scope.
    stale_nj = make_payload("5YJ3E1EA5RF000004", "NJ", "https://example.com/stale-nj")
    stale_ct = make_payload("5YJ3E1EA5RF000005", "CT", "https://example.com/stale-ct")
    upsert_listings(session, [stale_nj, stale_ct], now=cutoff - timedelta(days=1))

    changed = mark_unavailable_listings(session, state="NJ,NY", cutoff_started_at=cutoff)
    assert changed == 1
    by_url = {
        row.url: row for row in session.execute(select(Listing)).scalars().all()
    }
    assert by_url["https://example.com/stale-nj"].is_available is False
    assert by_url["https://example.com/stale-ct"].is_available is True
