from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.providers.winline_result_adapter import WinlineResultAdapter
from app.schemas.event_result import EventResultInput, EventResultProcessingResult
from app.services.result_ingestion_service import ResultIngestionService


class WinlineResultService:
    """Service wrapper for previewing and processing normalized Winline settlement payloads."""

    def preview_result_payload(self, payload: dict[str, Any]) -> list[EventResultInput]:
        adapter = WinlineResultAdapter()
        raw = adapter.parse_result_payload(payload)
        return adapter.to_event_results(raw)

    async def process_result_payload(
        self, session: AsyncSession, payload: dict[str, Any]
    ) -> list[EventResultProcessingResult]:
        event_results = self.preview_result_payload(payload)
        processing_results: list[EventResultProcessingResult] = []

        for event_result in event_results:
            processing_results.append(
                await ResultIngestionService().process_event_result(session, event_result)
            )

        return processing_results
