"""Bridge raw Winline-like result payloads into normalized `source_name` + `results`."""

from __future__ import annotations

from typing import Any


class WinlineRawResultBridgeService:
    """Normalize already-normalized or raw Winline-ish result payloads."""

    def detect_payload_shape(self, payload: dict[str, Any] | None) -> str:
        if not isinstance(payload, dict):
            return "unsupported"
        if isinstance(payload.get("results"), list):
            return "normalized_results"
        rows = self._extract_raw_result_rows(payload)
        if rows:
            return "raw_winline_results"
        return "unsupported"

    def normalize_raw_winline_result_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        shape = self.detect_payload_shape(payload)
        if shape == "normalized_results":
            return self._normalize_from_already_normalized(payload)
        if shape == "raw_winline_results":
            return self._normalize_from_winline_raw(payload)
        raise ValueError("unsupported_shape")

    def _normalize_from_already_normalized(self, payload: dict[str, Any]) -> dict[str, Any]:
        out: list[dict[str, Any]] = []
        for row in payload.get("results") or []:
            if not isinstance(row, dict):
                continue
            event_id = self._extract_event_id(row)
            sport = self._extract_sport(row)
            if not event_id or not sport:
                continue
            winner = self._extract_winner_selection(row)
            is_void = self._extract_is_void(row)
            if winner is None and not is_void:
                continue
            out.append(
                {
                    "event_external_id": event_id,
                    "sport": sport,
                    "winner_selection": winner,
                    "is_void": is_void,
                    "settled_at": self._extract_settled_at(row),
                }
            )
        if not out:
            raise ValueError("no_normalized_results")
        return {
            "source_name": str(payload.get("source_name") or "winline"),
            "results": out,
        }

    def _normalize_from_winline_raw(self, payload: dict[str, Any]) -> dict[str, Any]:
        rows = self._extract_raw_result_rows(payload)
        if not rows:
            raise ValueError("no_result_rows")
        out: list[dict[str, Any]] = []
        for row in rows:
            norm = self._normalize_result_row(row)
            if norm is not None:
                out.append(norm)
        if not out:
            raise ValueError("no_normalized_results")
        return {
            "source_name": str(payload.get("source_name") or "winline"),
            "results": out,
        }

    def _extract_raw_result_rows(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        candidates: list[Any] = []
        for key in ("result", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                candidates.extend(value)

        data = payload.get("data")
        if isinstance(data, dict):
            for key in ("results", "result", "items"):
                value = data.get(key)
                if isinstance(value, list):
                    candidates.extend(value)

        rows = [row for row in candidates if isinstance(row, dict)]
        if rows:
            return rows

        # Single row payload fallback.
        if any(
            key in payload
            for key in ("event_external_id", "external_event_id", "event_id", "idEvent", "winner", "status", "result")
        ):
            return [payload]
        return []

    def _normalize_result_row(self, row: dict[str, Any]) -> dict[str, Any] | None:
        event_id = self._extract_event_id(row)
        sport = self._extract_sport(row)
        if not event_id or not sport:
            return None
        winner = self._extract_winner_selection(row)
        is_void = self._extract_is_void(row)
        if winner is None and not is_void:
            return None
        return {
            "event_external_id": event_id,
            "sport": sport,
            "winner_selection": winner,
            "is_void": is_void,
            "settled_at": self._extract_settled_at(row),
        }

    def _extract_event_id(self, row: dict[str, Any]) -> str | None:
        value = self._first_value(
            row,
            "event_external_id",
            "external_event_id",
            "event_id",
            "idEvent",
            "id",
            "event.id",
            "event.event_id",
            "event.external_event_id",
        )
        if value is None:
            return None
        return str(value).strip() or None

    def _extract_sport(self, row: dict[str, Any]) -> str | None:
        value = self._first_value(
            row,
            "sport",
            "sport_name",
            "sport_slug",
            "sport_key",
            "idSport",
            "event.sport",
            "event.sport_name",
            "event.sport_slug",
            "event.sport_key",
            "event.idSport",
        )
        return self._map_sport_from_raw(value)

    def _extract_winner_selection(self, row: dict[str, Any]) -> str | None:
        value = self._first_value(
            row,
            "winner_selection",
            "winner",
            "result",
            "outcome",
            "side",
            "win",
            "event_result",
        )
        selection = self._normalize_selection(value)
        if selection is None:
            return None
        return self._resolve_selection_for_settlement(row, selection)

    def _extract_is_void(self, row: dict[str, Any]) -> bool:
        bool_value = self._first_value(
            row,
            "is_void",
            "isVoid",
            "canceled",
            "cancelled",
            "refund",
            "returned",
            "annulled",
        )
        if isinstance(bool_value, bool):
            return bool_value
        if isinstance(bool_value, (int, float)):
            return bool(bool_value)
        if isinstance(bool_value, str):
            if bool_value.strip().lower() in {"true", "1", "yes", "да"}:
                return True

        status = self._first_value(row, "status", "result_status", "event.status")
        if status is None:
            return False
        st = str(status).strip().lower()
        return st in {
            "void",
            "cancelled",
            "canceled",
            "refund",
            "returned",
            "annulled",
            "return",
        }

    def _extract_settled_at(self, row: dict[str, Any]) -> str | None:
        value = self._first_value(
            row,
            "settled_at",
            "finished_at",
            "completed_at",
            "result_at",
            "updated_at",
            "event.settled_at",
            "event.finished_at",
        )
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    def _normalize_selection(self, value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, dict):
            value = value.get("value") or value.get("name") or value.get("label")
        text = str(value).strip()
        if not text:
            return None
        key = text.lower()
        mapping = {
            "1": "HOME",
            "home": "HOME",
            "п1": "HOME",
            "2": "AWAY",
            "away": "AWAY",
            "п2": "AWAY",
            "x": "DRAW",
            "draw": "DRAW",
            "ничья": "DRAW",
            "yes": "YES",
            "да": "YES",
            "no": "NO",
            "нет": "NO",
            "over": "OVER",
            "больше": "OVER",
            "under": "UNDER",
            "меньше": "UNDER",
        }
        return mapping.get(key, text)

    def _map_sport_from_raw(self, value: Any) -> str | None:
        if value is None:
            return None
        key = str(value).strip().lower()
        if key in {"1", "football", "soccer"}:
            return "football"
        if key in {"2", "cs2", "counter_strike", "counter-strike", "cs"}:
            return "cs2"
        if key in {"3", "dota2", "dota 2", "dota"}:
            return "dota2"
        return None

    def _resolve_selection_for_settlement(self, row: dict[str, Any], selection: str) -> str:
        event = row.get("event")
        members = event.get("members") if isinstance(event, dict) else None
        home, away = self._extract_members(members)
        if selection == "HOME" and home:
            return home
        if selection == "AWAY" and away:
            return away
        if selection == "DRAW":
            return "Draw"
        if selection == "YES":
            return "Yes"
        if selection == "NO":
            return "No"
        return selection

    def _extract_members(self, members: Any) -> tuple[str, str]:
        if not isinstance(members, list) or len(members) < 2:
            return "", ""
        names: list[str] = []
        for row in members[:2]:
            if isinstance(row, dict):
                name = row.get("name") or row.get("title") or row.get("memberName") or ""
            else:
                name = row
            names.append(str(name).strip())
        if len(names) < 2:
            return "", ""
        return names[0], names[1]

    def _first_value(self, row: dict[str, Any], *paths: str) -> Any:
        for path in paths:
            cur: Any = row
            ok = True
            for part in path.split("."):
                if isinstance(cur, dict) and part in cur:
                    cur = cur[part]
                else:
                    ok = False
                    break
            if ok:
                return cur
        return None
