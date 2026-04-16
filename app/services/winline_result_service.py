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

    def preview_multi(self) -> None:
        """Mock preview: raw result snapshots -> EventResultInput per sport label."""
        adapter = WinlineResultAdapter()
        for name, snapshot in self._get_multi_mock_result_snapshots().items():
            raw = adapter.parse_result_payload(snapshot)
            event_results = adapter.to_event_results(raw)
            print(f"=== RESULT SPORT: {name} ===")
            print("raw_results:", len(raw.results))
            print("event_results:", len(event_results))
            print("sample:")
            for er in event_results[:5]:
                ws = (
                    er.winner_selection
                    if er.winner_selection is not None
                    else "None"
                )
                print(
                    f"- event={er.event_external_id} | sport={er.sport.value} | "
                    f"winner={ws} | void={er.is_void}"
                )
            if not event_results:
                print("  (none)")

    def _get_multi_mock_result_snapshots(self) -> dict[str, dict[str, Any]]:
        return {
            "football": {
                "results": [
                    {
                        "event_id": 15398418,
                        "sport": "football",
                        "winner": "HOME",
                        "is_void": False,
                        "settled_at": "2026-04-20T21:05:00Z",
                        "status": "finished",
                        "score": "2:1",
                        "home_team": "Арсенал",
                        "away_team": "Челси",
                    },
                    {
                        "event_id": 15398419,
                        "sport": "football",
                        "winner": "DRAW",
                        "is_void": False,
                        "settled_at": "2026-04-20T21:10:00Z",
                        "status": "finished",
                        "score": "1:1",
                    },
                    {
                        "event_id": None,
                        "sport": "football",
                        "winner": "HOME",
                    },
                    {
                        "event_id": 15398420,
                        "sport": "unknown_sport",
                        "winner": "HOME",
                        "settled_at": "2026-04-20T21:15:00Z",
                        "status": "finished",
                    },
                    {
                        "event_id": 15398421,
                        "sport": "football",
                        "is_void": True,
                        "settled_at": "2026-04-20T21:20:00Z",
                        "status": "void",
                    },
                ]
            },
            "cs2": {
                "results": [
                    {
                        "event_id": 2001,
                        "sport": "cs2",
                        "winner": "HOME",
                        "is_void": False,
                        "settled_at": "2026-04-20T18:20:00Z",
                        "status": "finished",
                    }
                ]
            },
            "dota2": {
                "results": [
                    {
                        "event_id": 3001,
                        "sport": "dota2",
                        "winner": "AWAY",
                        "is_void": False,
                        "settled_at": "2026-04-20T19:00:00Z",
                        "status": "finished",
                    }
                ]
            },
        }


if __name__ == "__main__":
    WinlineResultService().preview_multi()
