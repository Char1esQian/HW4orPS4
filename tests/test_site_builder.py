from __future__ import annotations

from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.models import Base, Listing
from app.site_builder import serialize_listing_safe


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


def test_serialize_listing_safe_omits_raw_payload() -> None:
    session = build_session()
    row = Listing(
        source="marketcheck",
        url="https://example.com/car",
        vin="5YJ3E1EA5LF784963",
        model="3",
        year=2024,
        trim="Long Range",
        price=35000,
        mileage=12000,
        city="Boston",
        state="MA",
        dealer_name="Carvana",
        first_seen=datetime(2026, 3, 20, 12, 0, 0),
        last_seen=datetime(2026, 3, 30, 12, 0, 0),
        is_available=True,
        hw4_likely=False,
        hw4_reason="HW4 heuristic applies only to Model Y.",
        raw={"secret": "do-not-export"},
    )
    session.add(row)
    session.commit()

    payload = serialize_listing_safe(row)
    assert payload["dealer_name"] == "Carvana"
    assert payload["days_seen"] is not None
    assert "raw" not in payload
    assert "secret" not in str(payload)


def test_parse_pages_tabs_root_first_then_slugged_subfolders() -> None:
    from app.site_builder import PagesTab, parse_pages_tabs

    tabs = parse_pages_tabs("Massachusetts=MA; New Jersey = pa,nj,ny ;")
    assert tabs == [
        PagesTab(label="Massachusetts", states="MA", slug=""),
        PagesTab(label="New Jersey", states="NJ,NY,PA", slug="new-jersey"),
    ]
    assert parse_pages_tabs(None) == []
    assert parse_pages_tabs("") == []


def test_parse_pages_tabs_rejects_malformed_entries() -> None:
    import pytest

    from app.site_builder import parse_pages_tabs

    with pytest.raises(ValueError):
        parse_pages_tabs("Massachusetts")
    with pytest.raises(ValueError):
        parse_pages_tabs("=MA")


def test_site_tab_links_are_relative_to_active_tab() -> None:
    from app.site_builder import _site_tab_links, parse_pages_tabs

    tabs = parse_pages_tabs("Massachusetts=MA;New Jersey=NJ,NY,PA")
    from_root = _site_tab_links(tabs, tabs[0])
    assert [(t["href"], t["active"]) for t in from_root] == [("./", True), ("./new-jersey/", False)]
    from_nj = _site_tab_links(tabs, tabs[1])
    assert [(t["href"], t["active"]) for t in from_nj] == [("../", False), ("../new-jersey/", True)]
