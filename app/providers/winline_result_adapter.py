from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import ValidationError

from app.core.enums import SportType
from app.schemas.event_result import EventResultInput
from app.schemas.winline_raw import WinlineRawResultItem, WinlineRawResultPayload


class WinlineResultAdapter:
    """Normalize Winline settlement payloads into settlement-ready event results."""

    def parse_result_payload(self, payload: dict[str, Any]) -> WinlineRawResultPayload:
        """Parse raw Winline result payload into normalized items.

        Supports flexible keys per item (see _extract_event_id, _parse_settled_at, etc.).
        Skips list entries without a usable event id. Preserves full item in raw_json.
        """
        if not isinstance(payload, dict):
            return WinlineRawResultPayload(source_name="winline", results=[])

        results_raw = payload.get("results")
        if not isinstance(results_raw, list):
            return WinlineRawResultPayload(source_name="winline", results=[])

        results: list[WinlineRawResultItem] = []
        for item in results_raw:
            if not isinstance(item, dict):
                continue

            event_id = self._extract_event_id(item)
            if event_id is None or event_id == "":
                continue

            winner_raw = self._first_present(
                item,
                ("winner", "winner_selection", "result", "winner_side"),
            )
            normalized_winner = self._normalize_winner_selection(winner_raw)
            is_void = self._detect_is_void(item)
            settled_at = self._parse_settled_at(item)

            try:
                results.append(
                    WinlineRawResultItem(
                        event_external_id=str(event_id),
                        winner_selection=normalized_winner,
                        is_void=is_void,
                        settled_at=settled_at,
                        raw_json=dict(item),
                    )
                )
            except (ValidationError, TypeError, ValueError):
                continue

        return WinlineRawResultPayload(source_name="winline", results=results)

    def to_event_results(self, raw: WinlineRawResultPayload) -> list[EventResultInput]:
        event_results: list[EventResultInput] = []

        for item in raw.results:
            sport = self._extract_sport(item)
            if sport is None:
                continue

            eid = self._clean_text(item.event_external_id)
            if not eid:
                continue

            if not item.is_void:
                ws = self._clean_text(item.winner_selection)
                if not ws:
                    continue

            try:
                event_results.append(
                    EventResultInput(
                        event_external_id=eid,
                        sport=sport,
                        winner_selection=item.winner_selection,
                        is_void=bool(item.is_void),
                        settled_at=item.settled_at,
                        result_payload_json=item.raw_json,
                    )
                )
            except (ValidationError, TypeError, ValueError):
                continue

        return event_results

    def _first_present(self, item: dict[str, Any], keys: tuple[str, ...]) -> Any:
        for key in keys:
            if key in item and item[key] is not None and item[key] != "":
                return item[key]
        return None

    def _extract_event_id(self, item: dict[str, Any]) -> Any | None:
        for key in ("event_id", "event_external_id", "idEvent", "id"):
            if key not in item:
                continue
            v = item[key]
            if v is None or v == "":
                continue
            return v
        return None

    def _normalize_winner_selection(self, value: Any) -> str | None:
        """Normalize winner tokens for settlement comparison; unknown values stay trimmed as-is."""
        if value is None:
            return None
        s = str(value).strip()
        if not s:
            return None

        upper = s.upper()
        direct = {
            "HOME",
            "AWAY",
            "DRAW",
            "YES",
            "NO",
            "OVER",
            "UNDER",
        }
        if upper in direct:
            return upper

        key = s.lower()
        mapping = {
            "1": "HOME",
            "2": "AWAY",
            "x": "DRAW",
            "да": "YES",
            "нет": "NO",
            "больше": "OVER",
            "меньше": "UNDER",
        }
        if key in mapping:
            return mapping[key]

        return s

    def _detect_is_void(self, raw_item: dict[str, Any]) -> bool:
        if raw_item.get("is_void") is True:
            return True

        void_statuses = {
            "void",
            "cancelled",
            "canceled",
            "refund",
            "returned",
            "annulled",
        }
        for key in ("status", "state", "result_status"):
            v = raw_item.get(key)
            if not isinstance(v, str):
                continue
            if v.strip().lower() in void_statuses:
                return True
        return False

    def _parse_settled_at(self, item: dict[str, Any]) -> datetime | None:
        for key in ("settled_at", "finished_at", "completed_at", "result_at", "updated_at"):
            v = item.get(key)
            if v is None or v == "":
                continue
            dt = self._parse_datetime(v)
            if dt is not None:
                return dt
        return None

    def _parse_datetime(self, value: Any) -> datetime | None:
        text = self._clean_text(value)
        if not text:
            return None
        try:
            if text.endswith("Z"):
                text = text.replace("Z", "+00:00")
            return datetime.fromisoformat(text)
        except ValueError:
            return None

    def _extract_sport(self, item: WinlineRawResultItem) -> SportType | None:
        raw = item.raw_json or {}
        value = (
            raw.get("sport")
            or raw.get("sport_name")
            or raw.get("sport_slug")
            or raw.get("sport_key")
        )
        if value is None or value == "":
            return None
        return self._map_sport(value)

    def _map_sport(self, value: Any) -> SportType | None:
        if isinstance(value, int):
            if value == 1:
                return SportType.FOOTBALL
            return None
        if isinstance(value, str) and value.strip().isdigit():
            try:
                if int(value.strip()) == 1:
                    return SportType.FOOTBALL
            except ValueError:
                pass

        normalized = " ".join(str(value or "").strip().lower().replace("-", " ").replace("_", " ").split())
        if normalized in {"football", "soccer"}:
            return SportType.FOOTBALL
        if normalized in {"cs2", "cs 2", "counter strike", "counter strike 2"}:
            return SportType.CS2
        if normalized in {"dota2", "dota 2"}:
            return SportType.DOTA2
        return None

    def _clean_text(self, value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip()
