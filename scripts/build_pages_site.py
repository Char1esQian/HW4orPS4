from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import get_settings
from app.database import SessionLocal, init_db
from app.ingestion import import_filter_snapshot_payload, refresh_marketcheck
from app.site_builder import (
    export_listing_seen_payload,
    import_listing_seen_payload,
    merge_listing_seen_history,
    write_site_payload_files,
)


def _read_json_file(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def main() -> int:
    settings = get_settings()
    output_dir = ROOT / "site"
    history_path = ROOT / "history_snapshots.safe.json"
    listing_seen_path = ROOT / "listing_seen.safe.json"

    init_db()
    with SessionLocal() as session:
        if history_path.exists():
            payload = _read_json_file(history_path)
            import_filter_snapshot_payload(session, payload)
        persisted_listing_seen: dict = {}
        if listing_seen_path.exists():
            listing_seen_payload = _read_json_file(listing_seen_path)
            persisted_listing_seen = import_listing_seen_payload(listing_seen_payload)

        refresh_marketcheck(session, state=settings.default_state)
        if persisted_listing_seen:
            merge_listing_seen_history(session, persisted_listing_seen)
        write_site_payload_files(session, output_dir=output_dir, default_state=settings.default_state)

        # Persist safe trend history in-repo so scheduled GitHub Actions runs accumulate trends.
        site_history = output_dir / "data" / "history_snapshots.safe.json"
        history_path.write_text(site_history.read_text(encoding="utf-8-sig"), encoding="utf-8")
        listing_seen_path.write_text(
            json.dumps(export_listing_seen_payload(session), indent=2),
            encoding="utf-8",
        )

    print(f"Built static site at {output_dir}")
    print(f"Updated safe history file at {history_path}")
    print(f"Updated safe listing seen file at {listing_seen_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
