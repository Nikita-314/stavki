from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.providers.winline_adapter import WinlineAdapter
from app.schemas.provider_adapter import ProviderAdapterResult
from app.schemas.provider_models import ProviderBatchIngestResult
from app.services.ingestion_service import IngestionService


class WinlineSourceService:
    """Service wrapper for previewing and ingesting normalized Winline line payloads."""

    def preview_payload(self, payload: dict[str, Any]) -> ProviderAdapterResult:
        adapter = WinlineAdapter()
        raw = adapter.parse_payload(payload)
        return adapter.to_candidates(raw)

    async def ingest_payload(
        self, session: AsyncSession, payload: dict[str, Any]
    ) -> tuple[ProviderAdapterResult, ProviderBatchIngestResult]:
        adapter_result = self.preview_payload(payload)
        ingest_result = await IngestionService().ingest_candidates_with_filter_and_dedup(
            session,
            adapter_result.candidates,
        )
        return adapter_result, ingest_result
