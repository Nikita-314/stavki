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
        return self._assemble_payload_from_snapshot(self._get_mock_snapshot())

    def _assemble_payload_from_snapshot(self, snapshot: dict[str, Any]) -> dict[str, Any]:
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

    def preview_multi(self) -> None:
        """Run build_payload -> parse_payload -> to_candidates for each sport snapshot (adapter smoke test)."""
        adapter = WinlineAdapter()
        for name, snapshot in self._get_multi_mock_snapshots().items():
            payload = self._assemble_payload_from_snapshot(snapshot)
            raw = adapter.parse_payload(payload)
            result = adapter.to_candidates(raw)
            print(f"=== SPORT: {name} ===")
            print("events:", len(raw.events))
            print("markets:", len(raw.markets))
            print("candidates:", len(result.candidates))
            print("skipped:", result.skipped_items)
            print("sample candidates:")
            for cand in result.candidates[:3]:
                m = cand.match
                mk = cand.market
                print(f"- {m.match_name} | {mk.market_label} | {mk.selection} | odds={mk.odds_value}")
            if not result.candidates:
                print("  (none)")

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

    def _default_football_snapshot(self) -> dict[str, Any]:
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
                {
                    "id": 1003,
                    "idEvent": 15398418,
                    "idTipMarket": 303,
                    "idTipEvent": 7,
                    "koef": "0.5",
                    "V": ["1.90"],
                    "freeTextR": "@1HT@ тотал [a]",
                    "R": ["Больше"],
                },
                {
                    "id": 1004,
                    "idEvent": 15398418,
                    "idTipMarket": 104,
                    "idTipEvent": 5,
                    "koef": None,
                    "V": ["2.50", "3.20", "2.80"],
                    "freeTextR": "1X2",
                    "R": ["1", "X", "2"],
                },
                {
                    "id": 9001,
                    "idEvent": 15398418,
                    "idTipMarket": 9001,
                    "idTipEvent": 5,
                    "koef": None,
                    "V": ["1.00"],
                    "freeTextR": "1X2",
                    "R": ["1"],
                },
                {
                    "id": 9002,
                    "idEvent": 15398418,
                    "idTipMarket": 9002,
                    "idTipEvent": 99,
                    "koef": None,
                    "V": ["1.90"],
                    "freeTextR": "WeirdUnknownMarket",
                    "R": ["1"],
                },
            ],
        }

    def _get_mock_snapshot(self) -> dict[str, Any]:
        if isinstance(self._mock_snapshot, dict):
            return self._mock_snapshot

        return self._default_football_snapshot()

    def _get_multi_mock_snapshots(self) -> dict[str, dict[str, Any]]:
        return {
            "football": self._default_football_snapshot(),
            "cs2": {
                "events": [
                    {
                        "id": 2001,
                        "idSport": "cs2",
                        "idChampionship": 10,
                        "date": "2026-04-20T15:00:00Z",
                        "isLive": 0,
                        "members": ["NAVI", "G2"],
                    }
                ],
                "championships": [{"id": 10, "name": "CS2 Major"}],
                "lines": [
                    {
                        "id": 1,
                        "idEvent": 2001,
                        "idTipMarket": 2,
                        "koef": "",
                        "V": [1.8, 2.0],
                        "isLive": 0,
                        "freeTextR": "Match Winner",
                        "R": ["1", "2"],
                        "idTipEvent": 5,
                    }
                ],
            },
            "dota2": {
                "events": [
                    {
                        "id": 3001,
                        "idSport": "dota2",
                        "idChampionship": 20,
                        "date": "2026-04-20T16:00:00Z",
                        "isLive": 0,
                        "members": ["Team Spirit", "Liquid"],
                    }
                ],
                "championships": [{"id": 20, "name": "Dota Pro League"}],
                "lines": [
                    {
                        "id": 2,
                        "idEvent": 3001,
                        "idTipMarket": 2,
                        "koef": "",
                        "V": [1.6, 2.3],
                        "isLive": 0,
                        "freeTextR": "Match Winner",
                        "R": ["1", "2"],
                        "idTipEvent": 5,
                    }
                ],
            },
        }


if __name__ == "__main__":
    WinlineSourceService().preview_multi()
