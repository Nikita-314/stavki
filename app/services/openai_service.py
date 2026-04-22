from __future__ import annotations

import logging
from dataclasses import dataclass

import httpx

from app.core.config import Settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OpenAIHealthCheckResult:
    ok: bool
    error_text: str | None = None
    http_status: int | None = None


class OpenAIService:
    """Infrastructure-only OpenAI client (health checks)."""

    _BASE_URL = "https://api.openai.com"

    async def health_check(self, settings: Settings) -> OpenAIHealthCheckResult:
        """
        Minimal, cheap check:
        - If key missing -> ok=False with explicit error (caller decides whether to notify).
        - Else: GET /v1/models and treat 200 as OK.
        """
        key = (settings.openai_api_key or "").strip()
        if not key:
            return OpenAIHealthCheckResult(ok=False, error_text="disabled_no_api_key", http_status=None)

        url = f"{self._BASE_URL}/v1/models"
        headers = {"Authorization": f"Bearer {key}"}
        timeout = httpx.Timeout(connect=5.0, read=8.0, write=5.0, pool=5.0)
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                r = await client.get(url, headers=headers)
            if r.status_code == 200:
                return OpenAIHealthCheckResult(ok=True, error_text=None, http_status=200)
            # Keep response snippet small (no secrets)
            body = (r.text or "").strip()
            if len(body) > 400:
                body = body[:400] + "..."
            return OpenAIHealthCheckResult(
                ok=False,
                error_text=f"http_{r.status_code}: {body or 'empty_body'}",
                http_status=int(r.status_code),
            )
        except Exception as exc:  # noqa: BLE001
            return OpenAIHealthCheckResult(ok=False, error_text=f"request_error: {exc!s}", http_status=None)

