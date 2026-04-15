from __future__ import annotations

import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen

from app.schemas.provider_client import ProviderClientConfig, ProviderClientFetchResult


class OddsHttpClient:
    def build_url(self, config: ProviderClientConfig) -> str:
        parsed = urlparse(config.base_url)
        existing = dict(parse_qsl(parsed.query, keep_blank_values=False))

        params: dict[str, str] = {}
        if config.sport:
            params["sport"] = config.sport
        if config.regions:
            params["regions"] = config.regions
        if config.markets:
            params["markets"] = config.markets
        if config.bookmakers:
            params["bookmakers"] = config.bookmakers
        if config.odds_format:
            params["oddsFormat"] = config.odds_format
        if config.date_format:
            params["dateFormat"] = config.date_format
        if config.api_key:
            params["apiKey"] = config.api_key

        merged = {**existing, **params}
        new_query = urlencode(merged)
        return urlunparse(parsed._replace(query=new_query))

    def fetch(self, config: ProviderClientConfig) -> ProviderClientFetchResult:
        endpoint = self.build_url(config)
        req = Request(endpoint, headers={"Accept": "application/json", "User-Agent": "stavki-bot/0"})
        try:
            with urlopen(req, timeout=int(config.timeout_seconds)) as resp:
                status_code = getattr(resp, "status", None)
                raw = resp.read()

            try:
                text = raw.decode("utf-8", errors="replace")
                parsed_json: dict[str, Any] | list[Any] = json.loads(text)
            except Exception:
                return ProviderClientFetchResult(
                    ok=False,
                    source_name="odds_http",
                    endpoint=endpoint,
                    status_code=int(status_code) if status_code is not None else None,
                    error="unable_to_parse_json",
                    payload=None,
                )

            if isinstance(parsed_json, list):
                payload: dict[str, Any] = {"source_name": "odds_http", "data": parsed_json}
            elif isinstance(parsed_json, dict):
                payload = dict(parsed_json)
                payload.setdefault("source_name", "odds_http")
            else:
                return ProviderClientFetchResult(
                    ok=False,
                    source_name="odds_http",
                    endpoint=endpoint,
                    status_code=int(status_code) if status_code is not None else None,
                    error="json_is_not_object_or_list",
                    payload=None,
                )

            return ProviderClientFetchResult(
                ok=True,
                source_name=str(payload.get("source_name") or "odds_http"),
                endpoint=endpoint,
                status_code=int(status_code) if status_code is not None else None,
                error=None,
                payload=payload,
            )
        except HTTPError as e:
            return ProviderClientFetchResult(
                ok=False,
                source_name="odds_http",
                endpoint=endpoint,
                status_code=int(getattr(e, "code", 0) or 0) or None,
                error=f"http_error: {getattr(e, 'reason', 'unknown')}",
                payload=None,
            )
        except URLError as e:
            return ProviderClientFetchResult(
                ok=False,
                source_name="odds_http",
                endpoint=endpoint,
                status_code=None,
                error=f"url_error: {getattr(e, 'reason', 'unknown')}",
                payload=None,
            )
        except Exception as e:
            return ProviderClientFetchResult(
                ok=False,
                source_name="odds_http",
                endpoint=endpoint,
                status_code=None,
                error=f"exception: {type(e).__name__}",
                payload=None,
            )

