from __future__ import annotations

import time
from typing import Any
from urllib.parse import urljoin

import requests

from app.config import Settings


class MarketCheckError(RuntimeError):
    pass


class MarketCheckClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.session = requests.Session()

    def _build_url(self, endpoint_key: str) -> str:
        if not self.settings.marketcheck_base_url:
            raise MarketCheckError(
                "MARKETCHECK_BASE_URL is empty. Set it in .env before refreshing."
            )
        path = self.settings.endpoint(endpoint_key)
        if path.startswith("/TODO_") or "TODO" in path:
            raise MarketCheckError(
                "Endpoint path is not configured. Update app/endpoints.json or MARKETCHECK_*_ENDPOINT."
            )
        return urljoin(f"{self.settings.marketcheck_base_url}/", path.lstrip("/"))

    def _headers(self) -> dict[str, str]:
        headers: dict[str, str] = {"accept": "application/json"}
        if self.settings.marketcheck_api_key and not self.settings.marketcheck_api_key_in_query:
            headers[self.settings.marketcheck_api_key_header] = (
                self.settings.marketcheck_api_key
            )
        return headers

    def request_json(
        self,
        endpoint_key: str,
        params: dict[str, Any] | None = None,
        retries: int = 4,
    ) -> Any:
        if retries < 1:
            retries = 1

        params = dict(params or {})
        if self.settings.marketcheck_api_key and self.settings.marketcheck_api_key_in_query:
            params[self.settings.marketcheck_api_key_query_param] = (
                self.settings.marketcheck_api_key
            )

        url = self._build_url(endpoint_key)
        last_error: Exception | None = None
        for attempt in range(retries):
            try:
                response = self.session.get(
                    url,
                    params=params,
                    headers=self._headers(),
                    timeout=self.settings.marketcheck_timeout_seconds,
                )
                if response.status_code == 429 or response.status_code >= 500:
                    if attempt < retries - 1:
                        time.sleep(2**attempt)
                        continue
                response.raise_for_status()
                return response.json()
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                if attempt < retries - 1:
                    time.sleep(2**attempt)
                    continue
                break

        raise MarketCheckError(f"MarketCheck request failed: {last_error}")

    def _extract_items(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            preferred = payload.get(self.settings.listings_key)
            if isinstance(preferred, list):
                return [item for item in preferred if isinstance(item, dict)]
            for key in ("listings", "results", "data"):
                value = payload.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]
        return []

    def _extract_total_pages(self, payload: Any) -> int | None:
        if not isinstance(payload, dict):
            return None

        keys: list[str] = []
        if self.settings.total_pages_key:
            keys.append(self.settings.total_pages_key)
        keys.extend(["num_pages", "total_pages"])
        for key in keys:
            value = payload.get(key)
            if isinstance(value, int):
                return value
            if isinstance(value, str) and value.isdigit():
                return int(value)
        return None

    def _extract_total_found(self, payload: Any) -> int | None:
        if not isinstance(payload, dict):
            return None
        for key in ("num_found", "total_found", "total", "count"):
            value = payload.get(key)
            if isinstance(value, int):
                return value
            if isinstance(value, str) and value.isdigit():
                return int(value)
        return None

    def fetch_marketcheck_listings(
        self,
        state: str = "MA",
        make: str = "Tesla",
        models: list[str] | None = None,
        extra_filters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        models = models or ["Model 3", "Model Y"]
        page_size = self.settings.marketcheck_page_size
        all_items: list[dict[str, Any]] = []
        page_param_name = self.settings.page_param
        uses_offset_pagination = page_param_name.strip().lower() in {"start", "offset"}

        for page_index in range(self.settings.marketcheck_max_pages):
            page_value: int
            if uses_offset_pagination:
                page_value = page_index * page_size
            else:
                page_value = page_index + 1

            params: dict[str, Any] = {
                "state": state,
                "make": make,
                # TODO: Confirm exact model param name/shape from MarketCheck docs.
                "model": ",".join(models),
                page_param_name: page_value,
                self.settings.page_size_param: page_size,
            }
            if extra_filters:
                params.update(extra_filters)

            payload = self.request_json("search_listings", params=params)
            items = self._extract_items(payload)
            if not items:
                break

            all_items.extend(items)

            total_pages = self._extract_total_pages(payload)
            if total_pages is not None and (page_index + 1) >= total_pages:
                break

            total_found = self._extract_total_found(payload)
            if total_found is not None and len(all_items) >= total_found:
                break

            if len(items) < page_size:
                break

        return all_items
