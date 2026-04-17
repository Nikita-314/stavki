from __future__ import annotations

import json
import logging
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen

from app.schemas.provider_client import ProviderClientConfig, ProviderClientFetchResult


logger = logging.getLogger(__name__)


class OddsHttpClient:
    def _mask_secret(self, value: str | None) -> str | None:
        if value is None:
            return None
        secret = str(value).strip()
        if not secret:
            return None
        if len(secret) <= 6:
            return "*" * len(secret)
        return f"{secret[:3]}***{secret[-3:]}"

    def _public_endpoint(self, url: str) -> str:
        parsed = urlparse(url)
        query = [(key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=False) if key != "apiKey"]
        return urlunparse(parsed._replace(query=urlencode(query)))

    def _public_query_params(self, url: str) -> dict[str, str]:
        parsed = urlparse(url)
        return {key: value for key, value in parse_qsl(parsed.query, keep_blank_values=False) if key != "apiKey"}

    def _body_snippet(self, raw: bytes | str | None, limit: int = 300) -> str | None:
        if raw is None:
            return None
        text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)
        compact = " ".join(text.split())
        return compact[:limit] if compact else None

    def _classify_auth_status(self, *, config: ProviderClientConfig, status_code: int | None, body_snippet: str | None) -> str:
        if not str(config.api_key or "").strip():
            return "no_key"
        if status_code == 200:
            return "ok"
        body = str(body_snippet or "").lower()
        if status_code == 401 and "out_of_usage_credits" in body:
            return "out_of_usage_credits"
        if status_code == 401:
            return "unauthorized"
        if status_code is not None:
            return "http_error"
        return "request_error"

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
        public_endpoint = self._public_endpoint(endpoint)
        public_params = self._public_query_params(endpoint)
        key_raw = str(config.api_key or "").strip()
        key_present = bool(key_raw)
        key_masked = self._mask_secret(key_raw)
        key_length = len(key_raw)
        logger.info(
            "[FOOTBALL][LIVE] request provider=odds_http endpoint=%s params=%s key_present=%s key_length=%s key_masked=%s",
            public_endpoint,
            public_params,
            "yes" if key_present else "no",
            key_length,
            key_masked or "—",
        )
        req = Request(endpoint, headers={"Accept": "application/json", "User-Agent": "stavki-bot/0"})
        try:
            with urlopen(req, timeout=int(config.timeout_seconds)) as resp:
                status_code = getattr(resp, "status", None)
                raw = resp.read()
            body_snippet = self._body_snippet(raw)
            auth_status = self._classify_auth_status(
                config=config,
                status_code=int(status_code) if status_code is not None else None,
                body_snippet=body_snippet,
            )
            logger.info(
                "[FOOTBALL][LIVE] response status=%s auth_status=%s body=%s",
                status_code,
                auth_status,
                body_snippet or "—",
            )

            try:
                text = raw.decode("utf-8", errors="replace")
                parsed_json: dict[str, Any] | list[Any] = json.loads(text)
            except Exception:
                return ProviderClientFetchResult(
                    ok=False,
                    source_name="odds_http",
                    endpoint=public_endpoint,
                    status_code=int(status_code) if status_code is not None else None,
                    error="unable_to_parse_json",
                    payload=None,
                    auth_status=auth_status,
                    response_body_snippet=body_snippet,
                    key_present=key_present,
                    key_masked=key_masked,
                    key_length=key_length,
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
                    endpoint=public_endpoint,
                    status_code=int(status_code) if status_code is not None else None,
                    error="json_is_not_object_or_list",
                    payload=None,
                    auth_status=auth_status,
                    response_body_snippet=body_snippet,
                    key_present=key_present,
                    key_masked=key_masked,
                    key_length=key_length,
                )

            return ProviderClientFetchResult(
                ok=True,
                source_name=str(payload.get("source_name") or "odds_http"),
                endpoint=public_endpoint,
                status_code=int(status_code) if status_code is not None else None,
                error=None,
                payload=payload,
                auth_status=auth_status,
                response_body_snippet=body_snippet,
                key_present=key_present,
                key_masked=key_masked,
                key_length=key_length,
            )
        except HTTPError as e:
            body_snippet = self._body_snippet(e.read())
            auth_status = self._classify_auth_status(
                config=config,
                status_code=int(getattr(e, "code", 0) or 0) or None,
                body_snippet=body_snippet,
            )
            logger.info(
                "[FOOTBALL][LIVE] response status=%s auth_status=%s body=%s",
                getattr(e, "code", None),
                auth_status,
                body_snippet or "—",
            )
            return ProviderClientFetchResult(
                ok=False,
                source_name="odds_http",
                endpoint=public_endpoint,
                status_code=int(getattr(e, "code", 0) or 0) or None,
                error=f"http_error: {getattr(e, 'reason', 'unknown')}",
                payload=None,
                auth_status=auth_status,
                response_body_snippet=body_snippet,
                key_present=key_present,
                key_masked=key_masked,
                key_length=key_length,
            )
        except URLError as e:
            logger.info("[FOOTBALL][LIVE] request failed auth_status=request_error body=%s", getattr(e, "reason", "unknown"))
            return ProviderClientFetchResult(
                ok=False,
                source_name="odds_http",
                endpoint=public_endpoint,
                status_code=None,
                error=f"url_error: {getattr(e, 'reason', 'unknown')}",
                payload=None,
                auth_status="request_error",
                response_body_snippet=self._body_snippet(getattr(e, "reason", None)),
                key_present=key_present,
                key_masked=key_masked,
                key_length=key_length,
            )
        except Exception as e:
            logger.info("[FOOTBALL][LIVE] request failed auth_status=request_error body=%s", type(e).__name__)
            return ProviderClientFetchResult(
                ok=False,
                source_name="odds_http",
                endpoint=public_endpoint,
                status_code=None,
                error=f"exception: {type(e).__name__}",
                payload=None,
                auth_status="request_error",
                response_body_snippet=type(e).__name__,
                key_present=key_present,
                key_masked=key_masked,
                key_length=key_length,
            )

