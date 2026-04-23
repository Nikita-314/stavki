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


@dataclass(frozen=True)
class OpenAITestRequestResult:
    success: bool
    text_response: str | None = None
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

    async def test_simple_request(self, settings: Settings) -> OpenAITestRequestResult:
        """
        Debug-only: make a real minimal model call to verify API key works.

        Uses Responses API:
          POST /v1/responses
          {"model": "gpt-4o-mini", "input": "Say OK"}
        """
        key = (settings.openai_api_key or "").strip()
        if not key:
            return OpenAITestRequestResult(success=False, error_text="disabled_no_api_key", http_status=None)

        url = f"{self._BASE_URL}/v1/responses"
        headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
        payload = {"model": "gpt-4o-mini", "input": "Say OK"}
        timeout = httpx.Timeout(connect=8.0, read=20.0, write=8.0, pool=8.0)
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                r = await client.post(url, headers=headers, json=payload)
            txt = (r.text or "").strip()
            if len(txt) > 1200:
                txt = txt[:1200] + "..."
            if r.status_code != 200:
                return OpenAITestRequestResult(
                    success=False,
                    error_text=f"http_{r.status_code}: {txt or 'empty_body'}",
                    http_status=int(r.status_code),
                )
            # Try to extract text output (best-effort)
            out_text: str | None = None
            try:
                data = r.json()
                # common shapes: output_text or output[].content[].text
                if isinstance(data, dict) and isinstance(data.get("output_text"), str):
                    out_text = data.get("output_text")
                if out_text is None and isinstance(data, dict) and isinstance(data.get("output"), list):
                    for item in data["output"]:
                        if not isinstance(item, dict):
                            continue
                        content = item.get("content")
                        if not isinstance(content, list):
                            continue
                        for c in content:
                            if isinstance(c, dict) and isinstance(c.get("text"), str) and c.get("text"):
                                out_text = c.get("text")
                                break
                        if out_text:
                            break
            except Exception:
                out_text = None
            if out_text:
                out_text = out_text.strip()
                if len(out_text) > 500:
                    out_text = out_text[:500] + "..."
            return OpenAITestRequestResult(success=True, text_response=out_text, error_text=None, http_status=200)
        except Exception as exc:  # noqa: BLE001
            return OpenAITestRequestResult(success=False, error_text=f"request_error: {exc!s}", http_status=None)

