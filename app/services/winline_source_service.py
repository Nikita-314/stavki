from __future__ import annotations

import json
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.providers.winline_adapter import WinlineAdapter
from app.schemas.provider_adapter import ProviderAdapterResult
from app.schemas.provider_models import ProviderBatchIngestResult
from app.services.ingestion_service import IngestionService


class WinlineSourceService:
    """Service wrapper for previewing and ingesting normalized Winline line payloads."""

    def __init__(self, mock_snapshot: dict[str, Any] | None = None) -> None:
        self._mock_snapshot = mock_snapshot

    def build_payload(self) -> dict[str, Any]:
        """Build an adapter-ready payload from a mock Winline snapshot.

        Current source is intentionally mock-only. It expects a structure similar to:
        - `events`: list[event]
        - `championships`: list[championship]
        - `lines`: list[line]

        The returned payload matches the adapter contract:
        - `events`
        - `championships_by_id`
        - `lines_by_event_id`
        """
        snapshot = self._get_mock_snapshot()

        events = snapshot.get("events", [])
        championships = snapshot.get("championships", [])
        lines = snapshot.get("lines", [])

        if not isinstance(events, list):
            events = []
        if not isinstance(championships, list):
            championships = []
        if not isinstance(lines, list):
            lines = []

        championships_by_id: dict[str, dict[str, Any]] = {}
        for item in championships:
            if not isinstance(item, dict):
                continue
            championship_id = item.get("id")
            if championship_id in (None, ""):
                continue
            championships_by_id[str(championship_id)] = item

        lines_by_event_id: dict[str, list[dict[str, Any]]] = {}
        for item in lines:
            if not isinstance(item, dict):
                continue
            event_id = item.get("idEvent")
            if event_id in (None, ""):
                continue
            lines_by_event_id.setdefault(str(event_id), []).append(item)

        # TODO: replace mock with real API extraction
        # TODO: integrate with websocket / live updates
        # TODO: handle live (isLive=1)
        return {
            "events": events,
            "championships_by_id": championships_by_id,
            "lines_by_event_id": lines_by_event_id,
        }

    def preview(self) -> None:
        payload = self.build_payload()

        adapter = WinlineAdapter()
        raw = adapter.parse_payload(payload)
        result = adapter.to_candidates(raw)

        print("=== PAYLOAD ===")
        print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        print("=== RAW ===")
        print(json.dumps(raw.model_dump(mode="json"), ensure_ascii=False, indent=2))
        print("=== CANDIDATES ===")
        print(json.dumps(result.model_dump(mode="json"), ensure_ascii=False, indent=2))

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

    def _get_mock_snapshot(self) -> dict[str, Any]:
        if isinstance(self._mock_snapshot, dict):
            return self._mock_snapshot

        return {
            "events": [
                {
                    "id": 15398418,
                    "idSport": 1,
                    "idChampionship": 1,
                    "date": "2026-04-20T18:00:00Z",
                    "isLive": 0,
                    "members": ["Арсенал", "Челси"],
                }
            ],
            "championships": [
                {
                    "id": 1,
                    "name": "АПЛ",
                }
            ],
            "lines": [
                {
                    "id": 1001,
                    "idEvent": 15398418,
                    "idTipMarket": 101,
                    "idTipEvent": 5,
                    "koef": None,
                    "V": ["1.85", "2.05"],
                    "freeTextR": "Победа",
                    "R": ["Арсенал", "Челси"],
                },
                {
                    "id": 1002,
                    "idEvent": 15398418,
                    "idTipMarket": 202,
                    "idTipEvent": 7,
                    "koef": "2.5",
                    "V": ["1.90", "1.90"],
                    "freeTextR": "Тотал @NP@",
                    "R": ["Больше", "Меньше"],
                },
            ],
        }


if __name__ == "__main__":
    WinlineSourceService().preview()
