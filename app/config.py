from __future__ import annotations

import json
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()


def _as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _as_int(value: str | None, default: int) -> int:
    if value is None or not value.strip():
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _load_endpoints(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    return {
        str(key): str(val)
        for key, val in raw.items()
        if not str(key).startswith("_") and isinstance(val, str)
    }


@dataclass(frozen=True)
class Settings:
    marketcheck_api_key: str
    marketcheck_base_url: str
    default_state: str
    database_url: str
    marketcheck_timeout_seconds: int
    marketcheck_page_size: int
    marketcheck_max_pages: int
    marketcheck_api_key_header: str
    marketcheck_api_key_query_param: str
    marketcheck_api_key_in_query: bool
    listings_key: str
    total_pages_key: str | None
    page_param: str
    page_size_param: str
    endpoints: dict[str, str]

    def endpoint(self, key: str) -> str:
        value = self.endpoints.get(key, "")
        if not value:
            raise KeyError(f"Missing MarketCheck endpoint mapping for key: {key}")
        return value


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    app_dir = Path(__file__).resolve().parent
    endpoint_file = Path(
        os.getenv("MARKETCHECK_ENDPOINTS_FILE", str(app_dir / "endpoints.json"))
    )
    endpoints = _load_endpoints(endpoint_file)
    env_overrides: dict[str, Any] = {
        "search_listings": os.getenv("MARKETCHECK_SEARCH_ENDPOINT"),
        "health_test": os.getenv("MARKETCHECK_HEALTH_ENDPOINT"),
    }
    for key, value in env_overrides.items():
        if value:
            endpoints[key] = value

    return Settings(
        marketcheck_api_key=os.getenv("MARKETCHECK_API_KEY", "").strip(),
        marketcheck_base_url=os.getenv("MARKETCHECK_BASE_URL", "").rstrip("/"),
        default_state=os.getenv("DEFAULT_STATE", "MA").upper(),
        database_url=os.getenv("DATABASE_URL", "sqlite:///./hw4finder.db"),
        marketcheck_timeout_seconds=_as_int(
            os.getenv("MARKETCHECK_TIMEOUT_SECONDS"), default=25
        ),
        marketcheck_page_size=_as_int(os.getenv("MARKETCHECK_PAGE_SIZE"), default=50),
        marketcheck_max_pages=_as_int(os.getenv("MARKETCHECK_MAX_PAGES"), default=20),
        marketcheck_api_key_header=os.getenv(
            "MARKETCHECK_API_KEY_HEADER", "x-api-key"
        ),
        marketcheck_api_key_query_param=os.getenv(
            "MARKETCHECK_API_KEY_QUERY_PARAM", "api_key"
        ),
        marketcheck_api_key_in_query=_as_bool(
            os.getenv("MARKETCHECK_API_KEY_IN_QUERY"), default=False
        ),
        listings_key=os.getenv("MARKETCHECK_LISTINGS_KEY", "listings"),
        total_pages_key=os.getenv("MARKETCHECK_TOTAL_PAGES_KEY", "num_pages"),
        page_param=os.getenv("MARKETCHECK_PAGE_PARAM", "page"),
        page_size_param=os.getenv("MARKETCHECK_PAGE_SIZE_PARAM", "rows"),
        endpoints=endpoints,
    )
