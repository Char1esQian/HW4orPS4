from __future__ import annotations

import concurrent.futures
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from urllib.parse import urlparse

import requests
from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.hw4 import is_hw4_likely_model_y, normalize_vin
from app.marketcheck import MarketCheckClient
from app.models import FilterSnapshot, Listing, RunLog, utcnow

FSD_PATTERN = re.compile(r"\bfsd\b|full[\s-]*self[\s-]*driving", re.IGNORECASE)
HISTORY_EXPORT_SCHEMA = "hw4finder.filter_snapshots.v1"


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        digits = "".join(ch for ch in value if ch.isdigit())
        if digits:
            return int(digits)
    return None


def _to_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if value == 1:
            return True
        if value == 0:
            return False
        return None
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "yes", "y"}:
            return True
        if text in {"0", "false", "no", "n"}:
            return False
    return None


def _normalize_state(value: str | None) -> str | None:
    if not value:
        return None
    text = value.strip().upper()
    if len(text) >= 2:
        return text[:2]
    return text or None


def normalize_model(value: str | None) -> str | None:
    if not value:
        return None
    text = value.strip().lower().replace("-", " ")
    if "model y" in text:
        return "Y"
    if "model 3" in text or text.endswith(" 3") or text == "3":
        return "3"
    if text == "y":
        return "Y"
    return None


def build_fingerprint(
    source: str, heading: str | None, price: int | None, city: str | None
) -> str:
    key = f"{source}|{(heading or '').strip().lower()}|{price or ''}|{(city or '').strip().lower()}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _iter_text_values(value: Any):
    if value is None:
        return
    if isinstance(value, str):
        yield value
        return
    if isinstance(value, dict):
        for child in value.values():
            yield from _iter_text_values(child)
        return
    if isinstance(value, (list, tuple, set)):
        for child in value:
            yield from _iter_text_values(child)


def _text_mentions_fsd(text: str) -> bool:
    lowered = text.lower()
    if "not fsd" in lowered or "no fsd" in lowered:
        return False
    return bool(FSD_PATTERN.search(text))


def _payload_mentions_fsd(item: dict[str, Any]) -> bool:
    for text in _iter_text_values(item):
        if _text_mentions_fsd(text):
            return True
    return False


def _page_mentions_fsd(url: str, timeout_seconds: int) -> bool:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
    }
    response = requests.get(url, headers=headers, timeout=max(2, timeout_seconds))
    response.raise_for_status()
    return _text_mentions_fsd(response.text)


def _enrich_fsd_from_pages(
    adapted_listings: list[dict[str, Any]],
    timeout_seconds: int,
    workers: int,
) -> None:
    jobs: dict[concurrent.futures.Future[bool], dict[str, Any]] = {}
    max_workers = max(1, workers)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for payload in adapted_listings:
            raw = payload.get("raw")
            if not isinstance(raw, dict):
                continue
            if bool(raw.get("_fsd_mentioned")):
                continue
            url = payload.get("url")
            if not isinstance(url, str) or not url.strip():
                continue
            future = executor.submit(_page_mentions_fsd, url.strip(), timeout_seconds)
            jobs[future] = payload

        for future, payload in jobs.items():
            raw = payload.get("raw")
            if not isinstance(raw, dict):
                continue
            try:
                mentioned = future.result()
            except Exception:
                mentioned = False

            if mentioned:
                raw["_fsd_mentioned"] = True
                raw["_fsd_source"] = "page"


def _pick(item: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = item.get(key)
        if value not in (None, ""):
            return value
    return None


def _pick_nested(item: dict[str, Any], nested: tuple[str, str], fallback: Any = None) -> Any:
    parent = item.get(nested[0])
    if isinstance(parent, dict):
        value = parent.get(nested[1])
        if value not in (None, ""):
            return value
    return fallback


def _domain_to_vendor_name(raw: str | None) -> str | None:
    if not raw:
        return None
    text = raw.strip()
    if not text:
        return None

    candidate = text
    if "://" in text:
        parsed = urlparse(text)
        candidate = parsed.hostname or ""
    else:
        parsed = urlparse(f"//{text}")
        candidate = parsed.hostname or text

    candidate = candidate.strip().lower()
    if not candidate:
        return None
    if ":" in candidate:
        candidate = candidate.split(":", 1)[0]
    if candidate.startswith("www."):
        candidate = candidate[4:]

    parts = [p for p in candidate.split(".") if p]
    if not parts:
        return None
    if len(parts) == 1:
        base = parts[0]
    elif len(parts) >= 3 and parts[-2] in {"co", "com", "org", "net", "gov", "ac"}:
        base = parts[-3]
    else:
        base = parts[-2]

    base = base.replace("-", " ").replace("_", " ").strip()
    if not base:
        return None
    return " ".join(word.capitalize() for word in base.split())


def derive_vendor_name(item: dict[str, Any], url: str | None = None) -> str | None:
    for candidate in (
        _pick(item, "dealer_name"),
        _pick_nested(item, ("dealer", "name")),
        _pick_nested(item, ("mc_dealership", "name")),
    ):
        if candidate:
            text = str(candidate).strip()
            if text:
                return text

    source = _pick(item, "source")
    if source:
        source_text = str(source).strip().lower()
        # Skip short/internal source markers (e.g., "mc"), and only parse source when it
        # looks like a hostname/URL.
        if source_text not in {"mc", "marketcheck"} and (
            "." in source_text or "://" in source_text or "/" in source_text
        ):
            source_name = _domain_to_vendor_name(source_text)
            if source_name:
                return source_name

    url_name = _domain_to_vendor_name(url)
    if url_name:
        return url_name
    return None


def adapt_marketcheck_item(item: dict[str, Any]) -> dict[str, Any]:
    raw_model = _pick(item, "model", "model_name")
    heading = _pick(item, "heading", "title")
    model = normalize_model(str(raw_model or heading or ""))

    vin = normalize_vin(str(_pick(item, "vin") or "").strip() or None)
    year = _to_int(_pick(item, "year") or _pick_nested(item, ("build", "year")))
    trim = _pick(item, "trim") or _pick_nested(item, ("build", "trim"))
    price = _to_int(_pick(item, "price", "msrp", "selling_price"))
    mileage = _to_int(_pick(item, "miles", "mileage", "odometer"))
    city = _pick(item, "city") or _pick_nested(item, ("dealer", "city"))
    state = _normalize_state(
        _pick(item, "state") or _pick_nested(item, ("dealer", "state"))
    )
    url = _pick(item, "vdp_url", "url", "vehicle_url")
    dealer_name = derive_vendor_name(item, str(url) if url else None)
    photos = (
        _pick(item, "photo_links", "photos", "image_urls")
        or _pick_nested(item, ("media", "photo_links"))
    )
    fsd_from_api = _payload_mentions_fsd(item)
    raw_payload = dict(item)
    raw_payload["_fsd_mentioned"] = fsd_from_api
    raw_payload["_fsd_source"] = "api" if fsd_from_api else "none"

    hw4_likely = False
    hw4_reason = "HW4 heuristic applies only to Model Y."
    if model == "Y":
        hw4_likely, hw4_reason = is_hw4_likely_model_y(vin)

    return {
        "source": "marketcheck",
        "url": str(url) if url else None,
        "vin": vin,
        "model": model,
        "year": year,
        "trim": str(trim) if trim else None,
        "price": price,
        "mileage": mileage,
        "city": str(city) if city else None,
        "state": state,
        "dealer_name": str(dealer_name) if dealer_name else None,
        "hw4_likely": hw4_likely,
        "hw4_reason": hw4_reason,
        "heading": str(heading) if heading else None,
        "photos": photos,
        "raw": raw_payload,
    }


def _find_existing(session: Session, payload: dict[str, Any]) -> Listing | None:
    source = payload["source"]
    vin = payload.get("vin")
    url = payload.get("url")
    fingerprint = payload.get("fingerprint")

    if vin:
        existing = session.execute(
            select(Listing).where(Listing.vin == vin).limit(1)
        ).scalar_one_or_none()
        if existing:
            return existing

    if url:
        existing = session.execute(
            select(Listing)
            .where(and_(Listing.source == source, Listing.url == url))
            .limit(1)
        ).scalar_one_or_none()
        if existing:
            return existing

    if fingerprint:
        return session.execute(
            select(Listing)
            .where(and_(Listing.source == source, Listing.fingerprint == fingerprint))
            .limit(1)
        ).scalar_one_or_none()
    return None


def _prepare_payload(payload: dict[str, Any]) -> dict[str, Any]:
    prepared = dict(payload)
    prepared["source"] = str(prepared.get("source") or "marketcheck")

    raw_vin = prepared.get("vin")
    if raw_vin is None:
        prepared["vin"] = None
    else:
        prepared["vin"] = normalize_vin(str(raw_vin))

    raw_url = prepared.get("url")
    if raw_url is None:
        prepared["url"] = None
    else:
        url = str(raw_url).strip()
        prepared["url"] = url or None

    price = _to_int(prepared.get("price"))
    heading = prepared.get("heading")
    city = prepared.get("city")
    vin = prepared.get("vin")
    url = prepared.get("url")
    if vin or url:
        prepared["fingerprint"] = None
    else:
        prepared["fingerprint"] = build_fingerprint(
            prepared["source"],
            str(heading) if heading is not None else None,
            price,
            str(city) if city is not None else None,
        )
    return prepared


def _batch_dedupe_payloads(adapted_listings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[str, ...], dict[str, Any]] = {}
    for payload in adapted_listings:
        prepared = _prepare_payload(payload)
        source = prepared["source"]
        vin = prepared.get("vin")
        url = prepared.get("url")
        fingerprint = prepared.get("fingerprint")

        if vin:
            key = ("vin", vin)
        elif url:
            key = ("url", source, url)
        else:
            # Fingerprint is always set when VIN and URL are both missing.
            key = ("fingerprint", source, str(fingerprint))

        # Keep the latest entry in the batch for a stable "last write wins" behavior.
        deduped[key] = prepared
    return list(deduped.values())


def upsert_listings(
    session: Session,
    adapted_listings: list[dict[str, Any]],
    now: datetime | None = None,
) -> int:
    now = now or utcnow()
    upserted = 0

    for payload in _batch_dedupe_payloads(adapted_listings):
        price = _to_int(payload.get("price"))
        city = payload.get("city")

        existing = _find_existing(session, payload)
        if existing:
            existing.url = payload.get("url")
            existing.vin = payload.get("vin")
            existing.model = payload.get("model")
            existing.year = _to_int(payload.get("year"))
            existing.trim = payload.get("trim")
            existing.price = price
            existing.mileage = _to_int(payload.get("mileage"))
            existing.city = city
            existing.state = _normalize_state(payload.get("state"))
            existing.dealer_name = payload.get("dealer_name")
            existing.is_available = True
            existing.hw4_likely = bool(payload.get("hw4_likely", False))
            existing.hw4_reason = str(payload.get("hw4_reason") or "")
            existing.fingerprint = payload.get("fingerprint")
            existing.raw = payload.get("raw") or {}
            existing.last_seen = now
            upserted += 1
            continue

        listing = Listing(
            source=str(payload.get("source") or "marketcheck"),
            url=payload.get("url"),
            vin=payload.get("vin"),
            model=payload.get("model"),
            year=_to_int(payload.get("year")),
            trim=payload.get("trim"),
            price=price,
            mileage=_to_int(payload.get("mileage")),
            city=payload.get("city"),
            state=_normalize_state(payload.get("state")),
            dealer_name=payload.get("dealer_name"),
            first_seen=now,
            last_seen=now,
            is_available=True,
            hw4_likely=bool(payload.get("hw4_likely", False)),
            hw4_reason=str(payload.get("hw4_reason") or ""),
            fingerprint=payload.get("fingerprint"),
            raw=payload.get("raw") or {},
        )
        session.add(listing)
        upserted += 1

    session.commit()
    return upserted


def mark_unavailable_listings(
    session: Session,
    *,
    state: str,
    cutoff_started_at: datetime,
    source: str = "marketcheck",
    models: tuple[str, ...] = ("3", "Y"),
) -> int:
    stmt = select(Listing).where(
        Listing.source == source,
        Listing.state == state.upper(),
        Listing.model.in_(models),
        Listing.is_available.is_(True),
        Listing.last_seen < cutoff_started_at,
    )
    rows = list(session.execute(stmt).scalars().all())
    for row in rows:
        row.is_available = False
    session.commit()
    return len(rows)


def fetch_marketcheck_listings(
    state: str = "MA",
    make: str = "Tesla",
    models: list[str] | None = None,
    extra_filters: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    settings = get_settings()
    client = MarketCheckClient(settings)
    return client.fetch_marketcheck_listings(
        state=state, make=make, models=models or ["Model 3", "Model Y"], extra_filters=extra_filters
    )


def refresh_marketcheck(
    session: Session, state: str | None = None, extra_filters: dict[str, Any] | None = None
) -> RunLog:
    settings = get_settings()
    run = RunLog(status="running", started_at=utcnow(), items_found=0, items_upserted=0)
    session.add(run)
    session.commit()

    try:
        listings = fetch_marketcheck_listings(
            state=(state or settings.default_state).upper(),
            make="Tesla",
            models=["Model 3", "Model Y"],
            extra_filters=extra_filters,
        )
        adapted = [adapt_marketcheck_item(item) for item in listings]
        if settings.fsd_page_scan_enabled:
            _enrich_fsd_from_pages(
                adapted,
                timeout_seconds=settings.fsd_page_scan_timeout_seconds,
                workers=settings.fsd_page_scan_workers,
            )
        upserted = upsert_listings(session, adapted)
        mark_unavailable_listings(
            session,
            state=(state or settings.default_state).upper(),
            cutoff_started_at=run.started_at,
        )
        run.status = "success"
        run.items_found = len(listings)
        run.items_upserted = upserted
    except Exception as exc:  # noqa: BLE001
        session.rollback()
        run.status = "failed"
        run.error_text = str(exc)
    finally:
        run.ended_at = utcnow()
        session.add(run)
        session.commit()
    return run


def scan_fsd_mentions(session: Session, filters: "ListingFilters") -> tuple[int, int]:
    settings = get_settings()
    rows = query_export_rows(session, filters)

    candidates: list[Listing] = []
    for row in rows:
        if not row.url:
            continue
        raw = row.raw if isinstance(row.raw, dict) else {}
        if bool(raw.get("_fsd_mentioned")):
            continue
        candidates.append(row)

    if not candidates:
        return 0, 0

    scanned = 0
    newly_marked = 0
    updates: dict[str, dict[str, Any]] = {}
    max_workers = max(1, settings.fsd_page_scan_workers)
    timeout_seconds = max(2, settings.fsd_page_scan_timeout_seconds)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        jobs: dict[concurrent.futures.Future[bool], Listing] = {}
        for row in candidates:
            if not row.url:
                continue
            jobs[executor.submit(_page_mentions_fsd, row.url, timeout_seconds)] = row

        for future, row in jobs.items():
            scanned += 1
            try:
                mentioned = future.result()
            except Exception:
                mentioned = False

            raw = dict(row.raw) if isinstance(row.raw, dict) else {}
            if mentioned:
                raw["_fsd_mentioned"] = True
                raw["_fsd_source"] = "page"
                updates[row.id] = raw
                newly_marked += 1

    if updates:
        for row in rows:
            raw = updates.get(row.id)
            if raw is not None:
                row.raw = raw
        session.commit()

    return scanned, newly_marked


def _normalized_filter_values(values: tuple[str, ...]) -> tuple[str, ...]:
    ordered = ("yes", "no", "unknown")
    incoming = {str(v).strip().lower() for v in values}
    return tuple(v for v in ordered if v in incoming)


def _filter_signature(filters: "ListingFilters") -> str:
    payload = {
        "state": filters.state.upper(),
        "min_price": filters.min_price,
        "max_price": filters.max_price,
        "min_miles": filters.min_miles,
        "max_miles": filters.max_miles,
        "trim": (filters.trim or "").strip().lower() or None,
        "year_min": filters.year_min,
        "year_max": filters.year_max,
        "clean_title_values": list(_normalized_filter_values(filters.clean_title_values)),
        "one_owner_values": list(_normalized_filter_values(filters.one_owner_values)),
    }
    packed = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(packed.encode("utf-8")).hexdigest()


def describe_filter_conditions(filters: "ListingFilters") -> str:
    parts = [f"state={filters.state.upper()}"]
    if filters.min_price is not None:
        parts.append(f"min_price={filters.min_price}")
    if filters.max_price is not None:
        parts.append(f"max_price={filters.max_price}")
    if filters.min_miles is not None:
        parts.append(f"min_miles={filters.min_miles}")
    if filters.max_miles is not None:
        parts.append(f"max_miles={filters.max_miles}")
    if filters.trim:
        parts.append(f"trim~{filters.trim}")
    if filters.year_min is not None:
        parts.append(f"year_min={filters.year_min}")
    if filters.year_max is not None:
        parts.append(f"year_max={filters.year_max}")
    if filters.clean_title_values:
        parts.append(
            "clean_title in {" + ", ".join(_normalized_filter_values(filters.clean_title_values)) + "}"
        )
    if filters.one_owner_values:
        parts.append(
            "one_owner in {" + ", ".join(_normalized_filter_values(filters.one_owner_values)) + "}"
        )
    return " | ".join(parts)


def _price_percentile(sorted_values: list[int], percentile: float) -> int | None:
    if not sorted_values:
        return None
    n = len(sorted_values)
    if n == 1:
        return sorted_values[0]

    pos = (n - 1) * percentile
    lo = int(pos)
    hi = min(lo + 1, n - 1)
    if lo == hi:
        return sorted_values[lo]
    frac = pos - lo
    value = sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * frac
    return int(round(value))


def _compute_price_stats(rows: list[Listing]) -> dict[str, int | None]:
    values = sorted(int(row.price) for row in rows if isinstance(row.price, int) and row.price > 0)
    if not values:
        return {"lowest": None, "q1": None, "median": None, "q3": None}
    return {
        "lowest": values[0],
        "q1": _price_percentile(values, 0.25),
        "median": _price_percentile(values, 0.5),
        "q3": _price_percentile(values, 0.75),
    }


def track_filter_snapshot(
    session: Session,
    filters: "ListingFilters",
    model_y_rows: list[Listing],
    model_3_rows: list[Listing],
) -> bool:
    snapshot_date = utcnow().date()
    signature = _filter_signature(filters)
    existing = session.execute(
        select(FilterSnapshot)
        .where(
            FilterSnapshot.filter_signature == signature,
            FilterSnapshot.snapshot_date == snapshot_date,
        )
        .limit(1)
    ).scalar_one_or_none()
    if existing:
        return False

    model_y_count = len(model_y_rows)
    model_3_count = len(model_3_rows)
    model_y_price_stats = _compute_price_stats(model_y_rows)
    model_3_price_stats = _compute_price_stats(model_3_rows)

    row = FilterSnapshot(
        snapshot_date=snapshot_date,
        filter_signature=signature,
        filter_description=describe_filter_conditions(filters),
        state=filters.state.upper(),
        min_price=filters.min_price,
        max_price=filters.max_price,
        min_miles=filters.min_miles,
        max_miles=filters.max_miles,
        trim=filters.trim,
        year_min=filters.year_min,
        year_max=filters.year_max,
        clean_title_values=list(_normalized_filter_values(filters.clean_title_values)),
        one_owner_values=list(_normalized_filter_values(filters.one_owner_values)),
        model_y_count=max(0, int(model_y_count)),
        model_3_count=max(0, int(model_3_count)),
        model_y_price_lowest=model_y_price_stats["lowest"],
        model_y_price_q1=model_y_price_stats["q1"],
        model_y_price_median=model_y_price_stats["median"],
        model_y_price_q3=model_y_price_stats["q3"],
        model_3_price_lowest=model_3_price_stats["lowest"],
        model_3_price_q1=model_3_price_stats["q1"],
        model_3_price_median=model_3_price_stats["median"],
        model_3_price_q3=model_3_price_stats["q3"],
    )
    session.add(row)
    session.commit()
    return True


def query_filter_snapshot_history(
    session: Session, filters: "ListingFilters"
) -> list[FilterSnapshot]:
    signature = _filter_signature(filters)
    stmt = (
        select(FilterSnapshot)
        .where(FilterSnapshot.filter_signature == signature)
        .order_by(FilterSnapshot.snapshot_date.asc())
    )
    return list(session.execute(stmt).scalars().all())


def _snapshot_to_safe_dict(row: FilterSnapshot) -> dict[str, Any]:
    return {
        "snapshot_date": row.snapshot_date.isoformat(),
        "filter_signature": row.filter_signature,
        "filter_description": row.filter_description,
        "state": row.state,
        "min_price": row.min_price,
        "max_price": row.max_price,
        "min_miles": row.min_miles,
        "max_miles": row.max_miles,
        "trim": row.trim,
        "year_min": row.year_min,
        "year_max": row.year_max,
        "clean_title_values": list(row.clean_title_values or []),
        "one_owner_values": list(row.one_owner_values or []),
        "model_y_count": row.model_y_count,
        "model_3_count": row.model_3_count,
        "model_y_price_lowest": row.model_y_price_lowest,
        "model_y_price_q1": row.model_y_price_q1,
        "model_y_price_median": row.model_y_price_median,
        "model_y_price_q3": row.model_y_price_q3,
        "model_3_price_lowest": row.model_3_price_lowest,
        "model_3_price_q1": row.model_3_price_q1,
        "model_3_price_median": row.model_3_price_median,
        "model_3_price_q3": row.model_3_price_q3,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


def export_filter_snapshot_payload(
    session: Session, state: str | None = None
) -> dict[str, Any]:
    stmt = select(FilterSnapshot)
    if state:
        stmt = stmt.where(FilterSnapshot.state == state.strip().upper())
    stmt = stmt.order_by(FilterSnapshot.snapshot_date.asc(), FilterSnapshot.filter_signature.asc())
    rows = list(session.execute(stmt).scalars().all())
    snapshots = [_snapshot_to_safe_dict(row) for row in rows]
    return {
        "schema": HISTORY_EXPORT_SCHEMA,
        "exported_at": utcnow().isoformat(),
        "count": len(snapshots),
        "snapshots": snapshots,
    }


def _parse_snapshot_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value.strip())
    raise ValueError("snapshot_date is required and must be an ISO date string")


def _parse_snapshot_int(value: Any) -> int | None:
    parsed = _to_int(value)
    if parsed is None:
        return None
    return parsed


def _parse_snapshot_str_list(value: Any) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for entry in value:
        text = str(entry).strip().lower()
        if text not in {"yes", "no", "unknown"}:
            continue
        if text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


def _snapshot_signature_from_entry(entry: dict[str, Any]) -> str:
    signature = str(entry.get("filter_signature") or "").strip()
    if signature:
        return signature

    filters = ListingFilters(
        state=str(entry.get("state") or "").strip().upper() or "MA",
        min_price=_parse_snapshot_int(entry.get("min_price")),
        max_price=_parse_snapshot_int(entry.get("max_price")),
        min_miles=_parse_snapshot_int(entry.get("min_miles")),
        max_miles=_parse_snapshot_int(entry.get("max_miles")),
        trim=(str(entry.get("trim")).strip() if entry.get("trim") else None),
        year_min=_parse_snapshot_int(entry.get("year_min")),
        year_max=_parse_snapshot_int(entry.get("year_max")),
        clean_title_values=tuple(_parse_snapshot_str_list(entry.get("clean_title_values"))),
        one_owner_values=tuple(_parse_snapshot_str_list(entry.get("one_owner_values"))),
    )
    return _filter_signature(filters)


def import_filter_snapshot_payload(
    session: Session, payload: dict[str, Any] | list[dict[str, Any]]
) -> dict[str, Any]:
    if isinstance(payload, list):
        raw_snapshots = payload
    elif isinstance(payload, dict):
        snapshots_field = payload.get("snapshots")
        if isinstance(snapshots_field, list):
            raw_snapshots = snapshots_field
        else:
            raw_snapshots = []
    else:
        raw_snapshots = []

    deduped: dict[tuple[str, date], dict[str, Any]] = {}
    invalid = 0

    for raw in raw_snapshots:
        if not isinstance(raw, dict):
            invalid += 1
            continue
        try:
            snapshot_date = _parse_snapshot_date(raw.get("snapshot_date"))
            signature = _snapshot_signature_from_entry(raw)
            if not signature:
                invalid += 1
                continue
            deduped[(signature, snapshot_date)] = raw
        except Exception:
            invalid += 1
            continue

    inserted = 0
    updated = 0

    for (signature, snapshot_date), raw in deduped.items():
        existing = session.execute(
            select(FilterSnapshot)
            .where(
                FilterSnapshot.filter_signature == signature,
                FilterSnapshot.snapshot_date == snapshot_date,
            )
            .limit(1)
        ).scalar_one_or_none()

        state_value = _normalize_state(
            str(raw.get("state")).strip() if raw.get("state") is not None else None
        )
        trim_value = str(raw.get("trim")).strip() if raw.get("trim") else None
        clean_title_values = _parse_snapshot_str_list(raw.get("clean_title_values"))
        one_owner_values = _parse_snapshot_str_list(raw.get("one_owner_values"))
        description = str(raw.get("filter_description") or "").strip()
        if not description:
            description = "Imported snapshot"

        row_values = {
            "filter_signature": signature,
            "snapshot_date": snapshot_date,
            "filter_description": description,
            "state": state_value,
            "min_price": _parse_snapshot_int(raw.get("min_price")),
            "max_price": _parse_snapshot_int(raw.get("max_price")),
            "min_miles": _parse_snapshot_int(raw.get("min_miles")),
            "max_miles": _parse_snapshot_int(raw.get("max_miles")),
            "trim": trim_value,
            "year_min": _parse_snapshot_int(raw.get("year_min")),
            "year_max": _parse_snapshot_int(raw.get("year_max")),
            "clean_title_values": clean_title_values,
            "one_owner_values": one_owner_values,
            "model_y_count": max(0, _to_int(raw.get("model_y_count")) or 0),
            "model_3_count": max(0, _to_int(raw.get("model_3_count")) or 0),
            "model_y_price_lowest": _parse_snapshot_int(raw.get("model_y_price_lowest")),
            "model_y_price_q1": _parse_snapshot_int(raw.get("model_y_price_q1")),
            "model_y_price_median": _parse_snapshot_int(raw.get("model_y_price_median")),
            "model_y_price_q3": _parse_snapshot_int(raw.get("model_y_price_q3")),
            "model_3_price_lowest": _parse_snapshot_int(raw.get("model_3_price_lowest")),
            "model_3_price_q1": _parse_snapshot_int(raw.get("model_3_price_q1")),
            "model_3_price_median": _parse_snapshot_int(raw.get("model_3_price_median")),
            "model_3_price_q3": _parse_snapshot_int(raw.get("model_3_price_q3")),
        }

        if existing:
            for key, value in row_values.items():
                setattr(existing, key, value)
            updated += 1
            continue

        session.add(FilterSnapshot(**row_values))
        inserted += 1

    session.commit()
    return {
        "schema": HISTORY_EXPORT_SCHEMA,
        "input_count": len(raw_snapshots),
        "valid_unique_count": len(deduped),
        "invalid_count": invalid,
        "inserted": inserted,
        "updated": updated,
    }


@dataclass
class ListingFilters:
    state: str
    min_price: int | None = None
    max_price: int | None = None
    min_miles: int | None = None
    max_miles: int | None = None
    trim: str | None = None
    year_min: int | None = None
    year_max: int | None = None
    clean_title_values: tuple[str, ...] = ()
    one_owner_values: tuple[str, ...] = ()


def _apply_common_filters(stmt, filters: ListingFilters):
    stmt = stmt.where(Listing.state == filters.state.upper())

    if filters.min_price is not None:
        stmt = stmt.where(Listing.price.is_not(None), Listing.price >= filters.min_price)
    if filters.max_price is not None:
        stmt = stmt.where(Listing.price.is_not(None), Listing.price <= filters.max_price)
    if filters.min_miles is not None:
        stmt = stmt.where(Listing.mileage.is_not(None), Listing.mileage >= filters.min_miles)
    if filters.max_miles is not None:
        stmt = stmt.where(Listing.mileage.is_not(None), Listing.mileage <= filters.max_miles)
    if filters.trim:
        stmt = stmt.where(Listing.trim.is_not(None), Listing.trim.ilike(f"%{filters.trim}%"))
    if filters.year_min is not None:
        stmt = stmt.where(Listing.year.is_not(None), Listing.year >= filters.year_min)
    if filters.year_max is not None:
        stmt = stmt.where(Listing.year.is_not(None), Listing.year <= filters.year_max)
    return stmt


def _history_state(raw: dict[str, Any], candidate_keys: tuple[str, ...]) -> str:
    value: Any = None
    for key in candidate_keys:
        if key in raw:
            value = raw.get(key)
            break

    if value is None:
        vehicle_history = raw.get("vehicle_history")
        if isinstance(vehicle_history, dict):
            for key in candidate_keys:
                if key in vehicle_history:
                    value = vehicle_history.get(key)
                    break

    bool_value = _to_bool(value)
    if bool_value is True:
        return "yes"
    if bool_value is False:
        return "no"
    return "unknown"


def _combined_history_states(*states: str) -> set[str]:
    known = {state for state in states if state in {"yes", "no"}}
    if known:
        return known
    return {"unknown"}


def _matches_carfax_filters(row: Listing, filters: ListingFilters) -> bool:
    raw = row.raw or {}
    if not isinstance(raw, dict):
        raw = {}

    clean_title_state = _history_state(
        raw,
        ("carfax_clean_title", "carfax_clean_title_flag"),
    )
    one_owner_state = _history_state(
        raw,
        ("carfax_1_owner", "carfax_one_owner"),
    )
    autocheck_clean_title_state = _history_state(
        raw,
        ("autocheck_clean_title", "auto_check_clean_title", "autocheck_clean_title_flag"),
    )
    autocheck_one_owner_state = _history_state(
        raw,
        ("autocheck_1_owner", "auto_check_1_owner", "autocheck_one_owner"),
    )

    combined_clean_title_states = _combined_history_states(
        clean_title_state, autocheck_clean_title_state
    )
    if filters.clean_title_values and not (
        set(filters.clean_title_values) & combined_clean_title_states
    ):
        return False

    combined_one_owner_states = _combined_history_states(
        one_owner_state, autocheck_one_owner_state
    )
    if filters.one_owner_values and not (
        set(filters.one_owner_values) & combined_one_owner_states
    ):
        return False

    return True


def _latest_successful_refresh_started_at(session: Session) -> datetime | None:
    return session.execute(
        select(RunLog.started_at)
        .where(RunLog.status == "success")
        .order_by(RunLog.started_at.desc())
        .limit(1)
    ).scalar_one_or_none()


def query_model_y_hw4(session: Session, filters: ListingFilters) -> list[Listing]:
    active_cutoff = _latest_successful_refresh_started_at(session)
    stmt = select(Listing).where(
        Listing.source == "marketcheck",
        Listing.is_available.is_(True),
        Listing.model == "Y",
        Listing.hw4_likely.is_(True),
    )
    if active_cutoff is not None:
        stmt = stmt.where(Listing.last_seen >= active_cutoff)
    stmt = _apply_common_filters(stmt, filters)
    stmt = stmt.order_by(Listing.last_seen.desc(), Listing.price.asc())
    rows = list(session.execute(stmt).scalars().all())
    return [row for row in rows if _matches_carfax_filters(row, filters)]


def query_model3_2024(session: Session, filters: ListingFilters) -> list[Listing]:
    active_cutoff = _latest_successful_refresh_started_at(session)
    stmt = select(Listing).where(
        Listing.source == "marketcheck",
        Listing.is_available.is_(True),
        Listing.model == "3",
        Listing.year.is_not(None),
        Listing.year >= 2024,
    )
    if active_cutoff is not None:
        stmt = stmt.where(Listing.last_seen >= active_cutoff)
    stmt = _apply_common_filters(stmt, filters)
    stmt = stmt.order_by(Listing.last_seen.desc(), Listing.price.asc())
    rows = list(session.execute(stmt).scalars().all())
    return [row for row in rows if _matches_carfax_filters(row, filters)]


def query_export_rows(session: Session, filters: ListingFilters) -> list[Listing]:
    active_cutoff = _latest_successful_refresh_started_at(session)
    base = select(Listing).where(
        Listing.source == "marketcheck",
        Listing.is_available.is_(True),
        or_(
            and_(Listing.model == "Y", Listing.hw4_likely.is_(True)),
            and_(Listing.model == "3", Listing.year.is_not(None), Listing.year >= 2024),
        ),
    )
    if active_cutoff is not None:
        base = base.where(Listing.last_seen >= active_cutoff)
    stmt = _apply_common_filters(base, filters)
    stmt = stmt.order_by(Listing.model.asc(), Listing.price.asc())
    rows = list(session.execute(stmt).scalars().all())
    return [row for row in rows if _matches_carfax_filters(row, filters)]


def query_trim_options(session: Session, filters: ListingFilters) -> list[str]:
    active_cutoff = _latest_successful_refresh_started_at(session)
    base = select(Listing).where(
        Listing.source == "marketcheck",
        Listing.is_available.is_(True),
        Listing.trim.is_not(None),
        or_(
            and_(Listing.model == "Y", Listing.hw4_likely.is_(True)),
            and_(Listing.model == "3", Listing.year.is_not(None), Listing.year >= 2024),
        ),
    )
    if active_cutoff is not None:
        base = base.where(Listing.last_seen >= active_cutoff)
    filters_without_trim = ListingFilters(
        state=filters.state,
        min_price=filters.min_price,
        max_price=filters.max_price,
        min_miles=filters.min_miles,
        max_miles=filters.max_miles,
        trim=None,
        year_min=filters.year_min,
        year_max=filters.year_max,
        clean_title_values=filters.clean_title_values,
        one_owner_values=filters.one_owner_values,
    )
    stmt = _apply_common_filters(base, filters_without_trim)
    stmt = stmt.order_by(Listing.trim.asc(), Listing.last_seen.desc())

    seen: set[str] = set()
    trims: list[str] = []
    for row in session.execute(stmt).scalars().all():
        if not _matches_carfax_filters(row, filters_without_trim):
            continue
        text = str(row.trim or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        trims.append(text)
    return trims
