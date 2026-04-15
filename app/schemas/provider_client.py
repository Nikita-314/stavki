from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class ProviderClientFetchResult(BaseModel):
    ok: bool
    source_name: str
    endpoint: str
    status_code: int | None
    error: str | None
    payload: dict[str, Any] | None


class ProviderClientConfig(BaseModel):
    base_url: str
    api_key: str | None = None
    sport: str | None = None
    regions: str | None = None
    markets: str | None = None
    bookmakers: str | None = None
    odds_format: str | None = None
    date_format: str | None = None
    timeout_seconds: int = 20

