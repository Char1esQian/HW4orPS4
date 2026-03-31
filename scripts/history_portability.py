from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.database import SessionLocal, init_db
from app.ingestion import export_filter_snapshot_payload, import_filter_snapshot_payload


def cmd_export(output_path: Path, state: str | None) -> int:
    init_db()
    with SessionLocal() as session:
        payload = export_filter_snapshot_payload(session, state=state)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Exported {payload.get('count', 0)} snapshots to {output_path}")
    return 0


def cmd_import(input_path: Path) -> int:
    if not input_path.exists():
        print(f"Input file does not exist: {input_path}")
        return 1

    payload = json.loads(input_path.read_text(encoding="utf-8"))
    init_db()
    with SessionLocal() as session:
        result = import_filter_snapshot_payload(session, payload)

    print(
        "Import complete: "
        f"input={result.get('input_count', 0)}, "
        f"valid_unique={result.get('valid_unique_count', 0)}, "
        f"invalid={result.get('invalid_count', 0)}, "
        f"inserted={result.get('inserted', 0)}, "
        f"updated={result.get('updated', 0)}"
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export/import safe trend history snapshots for HW4 Finder."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    export_cmd = sub.add_parser("export", help="Export safe history snapshots to JSON")
    export_cmd.add_argument(
        "--out",
        default="history_snapshots.safe.json",
        help="Output JSON file path",
    )
    export_cmd.add_argument(
        "--state",
        default=None,
        help="Optional state filter (example: MA)",
    )

    import_cmd = sub.add_parser("import", help="Import safe history snapshots from JSON")
    import_cmd.add_argument(
        "--in",
        dest="input_path",
        default="history_snapshots.safe.json",
        help="Input JSON file path",
    )

    args = parser.parse_args()
    if args.command == "export":
        state = str(args.state).upper() if args.state else None
        return cmd_export(Path(args.out), state=state)
    if args.command == "import":
        return cmd_import(Path(args.input_path))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

