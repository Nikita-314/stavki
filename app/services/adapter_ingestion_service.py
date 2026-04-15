from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.providers.generic_odds_adapter import GenericOddsAdapter
from app.schemas.provider_adapter import ProviderAdapterResult
from app.schemas.provider_models import ProviderBatchIngestResult
from app.services.ingestion_service import IngestionService


class AdapterIngestionService:
    def preview_payload(self, payload: dict[str, Any]) -> ProviderAdapterResult:
        adapter = GenericOddsAdapter()
        raw = adapter.parse_payload(payload)
        return adapter.to_candidates(raw)

    async def ingest_payload(
        self, session: AsyncSession, payload: dict[str, Any]
    ) -> tuple[ProviderAdapterResult, ProviderBatchIngestResult]:
        adapter_result = self.preview_payload(payload)
        ing = await IngestionService().ingest_candidates_with_filter_and_dedup(session, adapter_result.candidates)
        return adapter_result, ing

