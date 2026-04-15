from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class HttpFetchResult(BaseModel):
    url: str
    ok: bool
    status_code: int | None
    content_type: str | None
    error: str | None
    payload: dict[str, Any] | list[Any] | None

