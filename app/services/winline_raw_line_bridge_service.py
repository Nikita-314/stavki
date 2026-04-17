"""Bridge raw Winline-like line payloads into normalized `events` + `markets`.

Supports:
- normalized_events_markets: payload already close to adapter input
- raw_winline_events_lines: payload with `events` + `lines` (+ optional `championships`)
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from app.services.winline_mapping_rules import (
    WINLINE_LINE_MARKET_TYPE_RULES,
    WINLINE_SECTION_RULES,
    WINLINE_SELECTION_RULES,
)


class WinlineRawLineBridgeService:
    """Convert raw Winline-ish JSON into ingestion-friendly normalized payload."""

    def detect_payload_shape(self, payload: dict[str, Any] | None) -> str:
        if not isinstance(payload, dict):
            return "unsupported"
        has_events = isinstance(payload.get("events"), list)
        has_markets = isinstance(payload.get("markets"), list)
        has_lines = isinstance(payload.get("lines"), list)
        if has_events and has_markets:
            return "normalized_events_markets"
        if has_events and has_lines:
            return "raw_winline_events_lines"
        return "unsupported"

    def normalize_raw_winline_line_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        shape = self.detect_payload_shape(payload)
        if shape == "normalized_events_markets":
            return self._normalize_from_already_normalized(payload)
        if shape == "raw_winline_events_lines":
            return self._normalize_from_winline_raw(payload)
        raise ValueError("unsupported_shape")

    def _normalize_from_already_normalized(self, payload: dict[str, Any]) -> dict[str, Any]:
        events_out: list[dict[str, Any]] = []
        for e in payload.get("events") or []:
            if not isinstance(e, dict):
                continue
            eid = e.get("external_event_id") or e.get("event_external_id")
            if not eid:
                continue
            events_out.append(
                {
                    "external_event_id": str(eid),
                    "sport": str(e.get("sport", "")),
                    "tournament_name": str(e.get("tournament_name", "")),
                    "match_name": str(e.get("match_name", "")),
                    "home_team": str(e.get("home_team", "")),
                    "away_team": str(e.get("away_team", "")),
                    "event_start_at": e.get("event_start_at"),
                    "is_live": bool(e.get("is_live", False)),
                }
            )

        markets_out: list[dict[str, Any]] = []
        for m in payload.get("markets") or []:
            if not isinstance(m, dict):
                continue
            eid = m.get("external_event_id") or m.get("event_external_id")
            odds = m.get("odds_value")
            if not eid or odds is None:
                continue
            markets_out.append(
                {
                    "external_event_id": str(eid),
                    "bookmaker": "winline",
                    "market_type": str(m.get("market_type", "")),
                    "market_label": str(m.get("market_label", "")),
                    "selection": str(m.get("selection", "")),
                    "odds_value": odds if isinstance(odds, (int, float, Decimal, str)) else str(odds),
                    "section_name": m.get("section_name"),
                    "subsection_name": m.get("subsection_name"),
                    "search_hint": m.get("search_hint"),
                }
            )

        return {
            "source_name": str(payload.get("source_name") or "winline"),
            "events": events_out,
            "markets": markets_out,
        }

    def _normalize_from_winline_raw(self, payload: dict[str, Any]) -> dict[str, Any]:
        championships_map = self._build_championships_map(payload.get("championships"))
        events = self._build_events_from_raw(payload.get("events"), championships_map)
        if not events:
            raise ValueError("no_valid_events")
        markets = self._build_markets_from_raw(payload.get("lines"), events)
        if not markets:
            raise ValueError("no_supported_markets")
        return {
            "source_name": str(payload.get("source_name") or "winline"),
            "events": events,
            "markets": markets,
        }

    def _build_championships_map(self, championships: Any) -> dict[str, str]:
        out: dict[str, str] = {}
        if not isinstance(championships, list):
            return out
        for row in championships:
            if not isinstance(row, dict):
                continue
            cid = row.get("id")
            if cid is None:
                continue
            name = row.get("name") or row.get("championshipName") or row.get("title") or ""
            out[str(cid)] = str(name)
        return out

    def _build_events_from_raw(
        self,
        events: Any,
        championships_map: dict[str, str],
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        if not isinstance(events, list):
            return out
        for event in events:
            if not isinstance(event, dict):
                continue
            eid = event.get("id")
            if eid is None:
                continue
            sport = self._map_sport_from_raw(event.get("idSport"))
            if sport is None:
                continue
            members = event.get("members")
            home_team, away_team = self._extract_members(members)
            if not home_team or not away_team:
                continue
            championship_id = event.get("idChampionship")
            tournament = championships_map.get(str(championship_id), "")
            match_name = f"{home_team} vs {away_team}"
            out.append(
                {
                    "external_event_id": str(eid),
                    "sport": sport,
                    "tournament_name": tournament,
                    "match_name": match_name,
                    "home_team": home_team,
                    "away_team": away_team,
                    "event_start_at": self._normalize_dt(event.get("date")),
                    "is_live": bool(event.get("isLive", False)),
                }
            )
        return out

    def _build_markets_from_raw(
        self,
        lines: Any,
        events: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        if not isinstance(lines, list):
            return out
        events_by_id = {str(e["external_event_id"]): e for e in events}
        for line in lines:
            if not isinstance(line, dict):
                continue
            event_id = line.get("idEvent")
            if event_id is None:
                continue
            event = events_by_id.get(str(event_id))
            if event is None:
                continue

            market_type, market_type_source = self.resolve_line_market_type(line)
            market_label = self._normalize_market_label_from_raw(line, market_type)
            section_name, subsection_name, section_source = self.resolve_line_section_names(
                line=line,
                market_type=market_type,
                market_label=market_label,
            )

            odds_values = self._as_list(line.get("V"))
            selections = self._as_list(line.get("R"))
            if not odds_values and line.get("koef") is not None:
                odds_values = [line.get("koef")]
            if not selections and line.get("freeTextR") is not None:
                selections = [line.get("freeTextR")]

            outcome_count = max(len(odds_values), len(selections))
            for idx in range(outcome_count):
                raw_sel = selections[idx] if idx < len(selections) else None
                raw_odds = odds_values[idx] if idx < len(odds_values) else None
                selection, selection_source = self.resolve_line_selection(
                    raw_selection=raw_sel,
                    idx=idx,
                    line=line,
                    event=event,
                    market_type=market_type,
                )
                odds = self._safe_decimal(raw_odds)
                if not selection or odds is None or odds <= Decimal("1"):
                    continue
                out.append(
                    {
                        "external_event_id": str(event_id),
                        "bookmaker": "winline",
                        "market_type": market_type,
                        "market_label": market_label,
                        "selection": selection,
                        "odds_value": str(odds),
                        "section_name": section_name,
                        "subsection_name": subsection_name,
                        "search_hint": self._derive_search_hint_from_raw(
                            event=event,
                            market_label=market_label,
                            selection=selection,
                        ),
                        "raw_mapping_debug": {
                            "market_type_source": market_type_source,
                            "selection_source": selection_source,
                            "section_source": section_source,
                        },
                    }
                )
        return out

    def _map_sport_from_raw(self, raw: Any) -> str | None:
        if raw is None:
            return None
        key = str(raw).strip().lower()
        if key in {"1", "football", "soccer"}:
            return "football"
        if key in {"2", "cs2", "counter_strike", "counter-strike", "cs"}:
            return "cs2"
        if key in {"3", "dota2", "dota 2", "dota"}:
            return "dota2"
        return None

    def resolve_line_market_type(self, line: dict[str, Any]) -> tuple[str, str]:
        raw_id = str(line.get("idTipMarket", "")).strip()
        raw_tip_event = str(line.get("idTipEvent", "")).strip()
        raw_text = self._line_text_blob(line)
        for rule in WINLINE_LINE_MARKET_TYPE_RULES:
            ids = rule.get("id_tip_market")
            if ids and raw_id in ids:
                return str(rule["market_type"]), str(rule.get("source") or "rule:idTipMarket")
            tip_events = rule.get("id_tip_event")
            if tip_events and raw_tip_event in tip_events:
                return str(rule["market_type"]), str(rule.get("source") or "rule:idTipEvent")
            text_contains = rule.get("text_contains")
            if text_contains and any(str(token).lower() in raw_text for token in text_contains):
                return str(rule["market_type"]), str(rule.get("source") or "rule:text")
        return self._map_market_type_from_raw(line), "fallback:heuristic"

    def _map_market_type_from_raw(self, line: dict[str, Any]) -> str:
        raw_id = str(line.get("idTipMarket", "")).strip()
        raw_text = self._line_text_blob(line)

        if raw_id in {"1", "17", "20"}:
            return "1x2"
        if raw_id in {"2", "3", "18", "28"}:
            return "total_goals"
        if raw_id in {"4", "5", "19", "29"}:
            return "handicap"
        if raw_id in {"6", "26"}:
            return "both_teams_to_score"
        if raw_id in {"7", "8", "30"}:
            return "match_winner"

        if any(x in raw_text for x in {"обе забьют", "both teams", "btts"}):
            return "both_teams_to_score"
        if any(x in raw_text for x in {"фора", "handicap"}):
            return "handicap"
        if any(x in raw_text for x in {"тотал", "total", "over", "under"}):
            return "total_goals"
        if any(x in raw_text for x in {"1x2", "full time result", "исход"}):
            return "1x2"
        return "match_winner"

    def resolve_line_section_names(
        self,
        *,
        line: dict[str, Any],
        market_type: str,
        market_label: str,
    ) -> tuple[str, str, str]:
        raw_text = self._line_text_blob(line)
        for rule in WINLINE_SECTION_RULES:
            if rule.get("market_type") == market_type:
                return (
                    str(rule.get("section_name") or "Main"),
                    str(rule.get("subsection_name") or market_label),
                    str(rule.get("source") or "rule:market_type"),
                )
            text_contains = rule.get("text_contains")
            if text_contains and any(str(token).lower() in raw_text for token in text_contains):
                return (
                    str(rule.get("section_name") or "Main"),
                    str(rule.get("subsection_name") or market_label),
                    str(rule.get("source") or "rule:text"),
                )
        free = (str(line.get("freeTextR", "")).strip() if line.get("freeTextR") is not None else "")
        if free:
            return free, market_label, "fallback:freeTextR"
        section = self._map_section_name_from_raw(line, market_type)
        return section, market_label, "fallback:heuristic"

    def _normalize_market_label_from_raw(self, line: dict[str, Any], market_type: str) -> str:
        free = (str(line.get("freeTextR", "")).strip() if line.get("freeTextR") is not None else "")
        if free:
            return free
        mapping = {
            "1x2": "Full Time Result",
            "total_goals": "Total Goals",
            "handicap": "Handicap",
            "both_teams_to_score": "Both Teams To Score",
            "match_winner": "Match Winner",
        }
        return mapping.get(market_type, market_type)

    def resolve_line_selection(
        self,
        *,
        raw_selection: Any,
        idx: int,
        line: dict[str, Any],
        event: dict[str, Any],
        market_type: str,
    ) -> tuple[str, str]:
        raw = (str(raw_selection).strip() if raw_selection is not None else "")
        rl = raw.lower()
        home = str(event.get("home_team", "")).strip()
        away = str(event.get("away_team", "")).strip()

        if market_type in {"1x2", "match_winner"}:
            if rl in {"1", "home", "п1"}:
                return home, "fallback:winner_home"
            if rl in {"2", "away", "п2"}:
                return away, "fallback:winner_away"
            if rl in {"x", "draw", "ничья"}:
                return "Draw", "fallback:winner_draw"
            if raw:
                return raw, "fallback:raw"
            if idx == 0:
                return home, "fallback:index0"
            if idx == 1:
                if market_type == "1x2" and len(self._as_list(line.get("R"))) >= 3:
                    return "Draw", "fallback:index1_draw"
                return away, "fallback:index1_away"
            if idx == 2:
                return away, "fallback:index2_away"

        if market_type == "both_teams_to_score":
            if rl in {"yes", "да", "оба забьют: да"}:
                return "Yes", "fallback:btts_yes"
            if rl in {"no", "нет", "оба забьют: нет"}:
                return "No", "fallback:btts_no"
            if idx == 0:
                return "Yes", "fallback:index0_yes"
            if idx == 1:
                return "No", "fallback:index1_no"

        if market_type in {"total_goals", "handicap"}:
            koef = str(line.get("koef", "")).strip().replace(",", ".")
            if market_type == "total_goals" and raw:
                if rl in {"больше", "over", "o", "тб", "b"} and koef:
                    return f"Больше {koef}", "fallback:total_with_threshold"
                if rl in {"меньше", "under", "u", "тм", "m"} and koef:
                    return f"Меньше {koef}", "fallback:total_with_threshold"
                return raw, "fallback:raw"
            if raw:
                return raw, "fallback:raw"
            ft = str(line.get("freeTextR", "")).strip()
            if ft:
                return ft, "fallback:freeTextR"

        for rule in WINLINE_SELECTION_RULES:
            aliases = {str(x).lower() for x in (rule.get("aliases") or set())}
            if rl and rl in aliases:
                normalized = str(rule.get("normalized") or raw)
                if normalized == "HOME":
                    return home or normalized, str(rule.get("source") or "rule:selection")
                if normalized == "AWAY":
                    return away or normalized, str(rule.get("source") or "rule:selection")
                if normalized == "DRAW":
                    return "Draw", str(rule.get("source") or "rule:selection")
                if normalized == "YES":
                    return "Yes", str(rule.get("source") or "rule:selection")
                if normalized == "NO":
                    return "No", str(rule.get("source") or "rule:selection")
                return normalized, str(rule.get("source") or "rule:selection")
        return raw, "fallback:raw"

    def _map_section_name_from_raw(self, line: dict[str, Any], market_type: str) -> str:
        free = (str(line.get("freeTextR", "")).strip() if line.get("freeTextR") is not None else "")
        if free:
            return free
        mapping = {
            "1x2": "Main",
            "match_winner": "Main",
            "total_goals": "Totals",
            "handicap": "Handicap",
            "both_teams_to_score": "Goals",
        }
        return mapping.get(market_type, "Main")

    def _derive_search_hint_from_raw(
        self,
        *,
        event: dict[str, Any],
        market_label: str,
        selection: str,
    ) -> str:
        home = str(event.get("home_team", "")).strip()
        away = str(event.get("away_team", "")).strip()
        return " ".join(x for x in [home, away, market_label, selection] if x).strip()

    def _line_text_blob(self, line: dict[str, Any]) -> str:
        return " ".join(
            str(v)
            for v in (
                line.get("freeTextR"),
                line.get("marketName"),
                line.get("title"),
                line.get("name"),
                line.get("idTipEvent"),
            )
            if v is not None
        ).lower()

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

    def _normalize_dt(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.isoformat()
        s = str(value).strip()
        return s or None

    def _safe_decimal(self, value: Any) -> Decimal | None:
        if value is None or value == "":
            return None
        try:
            return Decimal(str(value).replace(",", "."))
        except Exception:
            return None

    def _as_list(self, value: Any) -> list[Any]:
        if isinstance(value, list):
            return value
        if value is None:
            return []
        return [value]
