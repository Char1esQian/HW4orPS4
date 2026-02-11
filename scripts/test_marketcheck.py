from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import get_settings
from app.marketcheck import MarketCheckClient


def _extract_first_item(payload: Any) -> dict[str, Any] | None:
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                return item
        return None

    if isinstance(payload, dict):
        for key in ("listings", "results", "data"):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                for item in candidate:
                    if isinstance(item, dict):
                        return item
    return None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Make one MarketCheck request, print keys, and write sample_response.json."
    )
    parser.add_argument("--state", default=None, help="State code (defaults to DEFAULT_STATE)")
    parser.add_argument("--make", default="Tesla", help="Vehicle make")
    parser.add_argument(
        "--models",
        default="Model 3,Model Y",
        help="Comma-separated models to include",
    )
    args = parser.parse_args()

    settings = get_settings()
    client = MarketCheckClient(settings)
    state = (args.state or settings.default_state).upper()
    models = [m.strip() for m in args.models.split(",") if m.strip()]
    page_param_value = 1
    if settings.page_param.strip().lower() in {"start", "offset"}:
        page_param_value = 0
    params = {
        "state": state,
        "make": args.make,
        # TODO: Confirm exact model filter parameter name in MarketCheck docs.
        "model": ",".join(models),
        settings.page_param: page_param_value,
        settings.page_size_param: 1,
    }

    endpoint = "health_test" if "health_test" in settings.endpoints else "search_listings"
    print(f"Using endpoint key: {endpoint}")
    try:
        payload = client.request_json(endpoint, params=params)
    except Exception as exc:  # noqa: BLE001
        print(f"Request failed: {exc}")
        return 1

    if isinstance(payload, dict):
        print("Top-level keys:", ", ".join(sorted(payload.keys())))
    elif isinstance(payload, list):
        print(f"Top-level payload is list of length: {len(payload)}")
    else:
        print(f"Top-level payload type: {type(payload).__name__}")

    first_item = _extract_first_item(payload)
    if first_item:
        print("First listing keys:", ", ".join(sorted(first_item.keys())))
    else:
        print("No listing object found in response.")

    output_path = ROOT / "sample_response.json"
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote sample response to: {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
