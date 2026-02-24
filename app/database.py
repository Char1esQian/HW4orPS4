from __future__ import annotations

from collections.abc import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.config import get_settings
from app.models import Base


def _build_engine():
    settings = get_settings()
    if settings.database_url.startswith("sqlite"):
        return create_engine(
            settings.database_url,
            connect_args={"check_same_thread": False},
            future=True,
        )
    return create_engine(settings.database_url, future=True)


engine = _build_engine()
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def _ensure_filter_snapshot_columns() -> None:
    if not engine.url.drivername.startswith("sqlite"):
        return

    target_columns: dict[str, str] = {
        "model_y_price_lowest": "INTEGER",
        "model_y_price_q1": "INTEGER",
        "model_y_price_median": "INTEGER",
        "model_y_price_q3": "INTEGER",
        "model_3_price_lowest": "INTEGER",
        "model_3_price_q1": "INTEGER",
        "model_3_price_median": "INTEGER",
        "model_3_price_q3": "INTEGER",
    }

    with engine.begin() as conn:
        table_exists = conn.exec_driver_sql(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='filter_snapshots'"
        ).fetchone()
        if not table_exists:
            return

        existing_rows = conn.exec_driver_sql("PRAGMA table_info(filter_snapshots)").fetchall()
        existing = {str(row[1]) for row in existing_rows if len(row) > 1}
        for name, sql_type in target_columns.items():
            if name in existing:
                continue
            conn.exec_driver_sql(
                f"ALTER TABLE filter_snapshots ADD COLUMN {name} {sql_type} NULL"
            )


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    _ensure_filter_snapshot_columns()


def get_db() -> Iterator[Session]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()

