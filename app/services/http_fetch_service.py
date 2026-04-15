from __future__ import annotations

import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from app.schemas.http_fetch import HttpFetchResult


class HttpFetchService:
    def fetch_json(self, url: str, timeout_seconds: int = 20) -> HttpFetchResult:
        req = Request(url, headers={"Accept": "application/json", "User-Agent": "stavki-bot/0"})
        try:
            with urlopen(req, timeout=timeout_seconds) as resp:
                status_code = getattr(resp, "status", None)
                content_type = resp.headers.get("Content-Type")
                raw = resp.read()

            try:
                text = raw.decode("utf-8", errors="replace")
                parsed: dict[str, Any] | list[Any] = json.loads(text)
            except Exception:
                return HttpFetchResult(
                    url=url,
                    ok=False,
                    status_code=int(status_code) if status_code is not None else None,
                    content_type=content_type,
                    error="unable_to_parse_json",
                    payload=None,
                )

            return HttpFetchResult(
                url=url,
                ok=True,
                status_code=int(status_code) if status_code is not None else None,
                content_type=content_type,
                error=None,
                payload=parsed,
            )
        except HTTPError as e:
            return HttpFetchResult(
                url=url,
                ok=False,
                status_code=int(getattr(e, "code", 0) or 0) or None,
                content_type=getattr(e, "headers", None).get("Content-Type") if getattr(e, "headers", None) else None,
                error=f"http_error: {getattr(e, 'reason', 'unknown')}",
                payload=None,
            )
        except URLError as e:
            return HttpFetchResult(
                url=url,
                ok=False,
                status_code=None,
                content_type=None,
                error=f"url_error: {getattr(e, 'reason', 'unknown')}",
                payload=None,
            )
        except Exception as e:
            return HttpFetchResult(
                url=url,
                ok=False,
                status_code=None,
                content_type=None,
                error=f"exception: {type(e).__name__}",
                payload=None,
            )

