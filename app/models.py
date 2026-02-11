from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, DateTime, Index, Integer, JSON, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utcnow() -> datetime:
    return datetime.utcnow()


class Base(DeclarativeBase):
    pass


class Listing(Base):
    __tablename__ = "listings"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    source: Mapped[str] = mapped_column(String(32), nullable=False, default="marketcheck")
    url: Mapped[str | None] = mapped_column(Text, nullable=True)
    vin: Mapped[str | None] = mapped_column(String(17), nullable=True)
    model: Mapped[str | None] = mapped_column(String(8), nullable=True)
    year: Mapped[int | None] = mapped_column(Integer, nullable=True)
    trim: Mapped[str | None] = mapped_column(String(120), nullable=True)
    price: Mapped[int | None] = mapped_column(Integer, nullable=True)
    mileage: Mapped[int | None] = mapped_column(Integer, nullable=True)
    city: Mapped[str | None] = mapped_column(String(120), nullable=True)
    state: Mapped[str | None] = mapped_column(String(2), nullable=True)
    dealer_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    first_seen: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)
    last_seen: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)
    hw4_likely: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    hw4_reason: Mapped[str] = mapped_column(Text, nullable=False, default="")
    fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True)
    raw: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)

    __table_args__ = (
        Index(
            "uq_listings_vin",
            "vin",
            unique=True,
            sqlite_where=vin.is_not(None),  # type: ignore[name-defined]
        ),
        Index(
            "uq_listings_source_url",
            "source",
            "url",
            unique=True,
            sqlite_where=url.is_not(None),  # type: ignore[name-defined]
        ),
        Index(
            "uq_listings_source_fingerprint",
            "source",
            "fingerprint",
            unique=True,
            sqlite_where=fingerprint.is_not(None),  # type: ignore[name-defined]
        ),
    )


class RunLog(Base):
    __tablename__ = "run_logs"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="running")
    items_found: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    items_upserted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_text: Mapped[str | None] = mapped_column(Text, nullable=True)

