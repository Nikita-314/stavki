from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation
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
        if not raw.events and not raw.markets:
            raw = adapter.parse_payload(self._build_preview_adapter_payload(payload))
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
                    "idSport": "football",
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
                    "koef": None,
                    "V": ["1.85", "2.05"],
                    "tipLine": {
                        "freeTextR": "Победа",
                        "R": ["Арсенал", "Челси"],
                    },
                },
                {
                    "id": 1002,
                    "idEvent": 15398418,
                    "idTipMarket": 202,
                    "koef": "2.5",
                    "V": ["1.90", "1.90"],
                    "tipLine": {
                        "freeTextR": "Тотал @NP@",
                        "R": ["Больше", "Меньше"],
                    },
                },
            ],
        }

    def _build_preview_adapter_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Temporary bridge so `python -m` preview can show candidates from mock data."""
        events_raw = payload.get("events", [])
        championships_by_id = payload.get("championships_by_id", {})
        lines_by_event_id = payload.get("lines_by_event_id", {})

        if not isinstance(events_raw, list) or not isinstance(championships_by_id, dict) or not isinstance(lines_by_event_id, dict):
            return {"events": [], "markets": []}

        events: list[dict[str, Any]] = []
        markets: list[dict[str, Any]] = []

        for raw_event in events_raw:
            if not isinstance(raw_event, dict):
                continue

            event_id = raw_event.get("id")
            championship_id = raw_event.get("idChampionship")
            members = raw_event.get("members")
            if event_id in (None, "") or not isinstance(members, list) or len(members) < 2:
                continue

            home_team = self._clean_text(members[0])
            away_team = self._clean_text(members[1])
            if not home_team or not away_team:
                continue

            championship = championships_by_id.get(str(championship_id)) or championships_by_id.get(championship_id) or {}
            tournament_name = self._clean_text(championship.get("name")) if isinstance(championship, dict) else ""

            events.append(
                {
                    "event_external_id": str(event_id),
                    "sport": self._clean_text(raw_event.get("idSport")) or "football",
                    "tournament_name": tournament_name or "Unknown tournament",
                    "match_name": f"{home_team} vs {away_team}",
                    "home_team": home_team,
                    "away_team": away_team,
                    "event_start_at": self._clean_text(raw_event.get("date")) or None,
                    "is_live": bool(raw_event.get("isLive")),
                }
            )

            line_items = lines_by_event_id.get(str(event_id)) or lines_by_event_id.get(event_id) or []
            if not isinstance(line_items, list):
                continue

            for raw_line in line_items:
                if not isinstance(raw_line, dict):
                    continue

                tip_line = raw_line.get("tipLine")
                selections = tip_line.get("R") if isinstance(tip_line, dict) else None
                odds = raw_line.get("V")
                market_type = raw_line.get("idTipMarket")
                if not isinstance(selections, list) or not isinstance(odds, list) or market_type in (None, ""):
                    continue

                market_label = self._build_market_label(
                    free_text=tip_line.get("freeTextR") if isinstance(tip_line, dict) else None,
                    market_param=raw_line.get("koef"),
                )

                for idx, selection_value in enumerate(selections):
                    if idx >= len(odds):
                        continue

                    selection = self._clean_text(selection_value)
                    odds_value = self._parse_decimal(odds[idx])
                    if not selection or odds_value is None:
                        continue

                    markets.append(
                        {
                            "event_external_id": str(event_id),
                            "market_type": str(market_type),
                            "market_label": market_label,
                            "selection": selection,
                            "odds_value": str(odds_value),
                            "section_name": None,
                            "subsection_name": None,
                            "search_hint": f"{home_team} {away_team} {market_label} {selection}".strip(),
                        }
                    )

        return {"events": events, "markets": markets}

    def _build_market_label(self, *, free_text: Any, market_param: Any) -> str:
        text = self._clean_text(free_text) or "market"
        param = self._clean_text(market_param)
        text = text.replace("@NP@", "").replace("@1HT@", "1H").strip()
        text = " ".join(text.split())
        if param:
            return f"{text} {param}".strip()
        return text

    def _clean_text(self, value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip()

    def _parse_decimal(self, value: Any) -> Decimal | None:
        if value in (None, ""):
            return None
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError):
            return None


if __name__ == "__main__":
    WinlineSourceService().preview()
