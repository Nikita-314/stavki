from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from pydantic import ValidationError

from app.core.enums import BookmakerType, SportType
from app.schemas.provider_adapter import ProviderAdapterResult
from app.schemas.provider_models import ProviderMatch, ProviderOddsMarket, ProviderSignalCandidate
from app.schemas.winline_raw import WinlineRawEventItem, WinlineRawMarketItem, WinlineRawPayload


class WinlineAdapter:
    """Normalize future Winline line payloads into provider candidates.

    This adapter stays fetch-agnostic on purpose. It only converts a payload that is already
    available in memory into the normalized Winline raw schemas and then into provider
    candidates compatible with the current ingestion pipeline.

    Practical TODO for the real Winline mapper:
    - support list payloads where each item is already an event-like object
    - support nested payloads where events live under a data/container/result node
    - support event-card payloads where markets arrive separately from the line list
    - support market groups / sections / tabs that need flattening before outcome parsing
    - support nested outcomes / coefficients arrays where the actual selection and odds live

    Current real-world Winline shape already confirmed manually:
    - event object contains:
      - `id`
      - `idSport`
      - `idChampionship`
      - `date`
      - `isLive`
      - `members`
    - championship object contains:
      - `id`
      - `name`
    - line object may be nested (`tipLine.freeTextR`, `tipLine.R`) or flat (`freeTextR`, `R` on line)
    - line object also may include:
      - `idTipEvent` (section hint for UI)

    Important:
    - one Winline line object does not equal one selection
    - one Winline line object may expand into multiple normalized markets
      by zipping `tipLine.R` with `V`
    """

    def parse_payload(self, payload: dict[str, Any]) -> WinlineRawPayload:
        """Parse a Winline line payload into normalized raw schemas.

        Supported normalized input shape:
        - `payload["events"]`: list of event dicts
        - `payload["markets"]`: list of already normalized market dicts

        Supported real Winline-derived input shape:
        - `payload["events"]`: list of raw Winline event dicts
        - `payload["championships_by_id"]`: dict[int|str, championship dict]
        - `payload["lines_by_event_id"]`: dict[event_id, list[line dict]]

        Real-event mapping currently uses:
        - event_external_id <- `event.id`
        - sport <- `event.idSport`
        - tournament_name <- `championship.name`
        - match_name <- `event.members[0] + " vs " + event.members[1]`
        - home_team <- `event.members[0]`
        - away_team <- `event.members[1]`
        - event_start_at <- `event.date`
        - is_live <- `event.isLive`

        Real-market mapping uses:
        - market_type <- `line.idTipMarket`
        - market_label <- cleaned `freeTextR` (or `tipLine.freeTextR`) + optional `koef`
        - selection <- `R[index]` (or `tipLine.R[index]`)
        - odds_value <- `line.V[index]`
        - section_name <- `_map_section_name(line.idTipEvent)`
        - subsection_name <- None
        - search_hint <- teams + market label + selection
        """
        if not isinstance(payload, dict):
            return WinlineRawPayload(source_name="winline", events=[], markets=[])

        events_raw = payload.get("events")
        markets_raw = payload.get("markets")

        # Path 1: already normalized skeleton/sample payload
        if isinstance(events_raw, list) and isinstance(markets_raw, list):
            return self._parse_normalized_payload(events_raw=events_raw, markets_raw=markets_raw)

        # Path 2: real Winline-derived payload assembled after DevTools/manual extraction
        championships_by_id = payload.get("championships_by_id")
        lines_by_event_id = payload.get("lines_by_event_id")

        if (
            isinstance(events_raw, list)
            and isinstance(championships_by_id, dict)
            and isinstance(lines_by_event_id, dict)
        ):
            return self._parse_real_winline_payload(
                events_raw=events_raw,
                championships_by_id=championships_by_id,
                lines_by_event_id=lines_by_event_id,
            )

        return WinlineRawPayload(source_name="winline", events=[], markets=[])

    def _parse_normalized_payload(
        self,
        *,
        events_raw: list[Any],
        markets_raw: list[Any],
    ) -> WinlineRawPayload:
        events: list[WinlineRawEventItem] = []
        markets: list[WinlineRawMarketItem] = []

        for item in events_raw:
            if not isinstance(item, dict):
                continue
            # TODO: event_external_id -> real event id path; skip event if missing.
            # TODO: sport -> real sport field path; fallback to parent event metadata if needed.
            # TODO: tournament_name -> real league/tournament path; fallback from category node.
            # TODO: match_name -> real event title path; or build from home_team + away_team.
            # TODO: home_team -> real team1/home path; skip event if missing.
            # TODO: away_team -> real team2/away path; skip event if missing.
            # TODO: event_start_at -> real start time path; leave None if absent.
            # TODO: is_live -> real live flag/status path; fallback from status if needed.
            try:
                events.append(WinlineRawEventItem.model_validate({**item, "raw_json": item}))
            except (ValidationError, TypeError, ValueError):
                continue

        for item in markets_raw:
            if not isinstance(item, dict):
                continue
            # TODO: market_type -> real provider market code path; fallback from label only if needed.
            # TODO: market_label -> real market title path; fallback from market_type if needed.
            # TODO: selection -> real outcome title path; skip market if missing.
            # TODO: odds_value -> real coefficient/price path; skip market if not parseable.
            # TODO: section_name -> real parent group/tab path; leave None if absent.
            # TODO: subsection_name -> real subgroup path; leave None if absent.
            # TODO: search_hint -> real searchable text path or build from teams + selection.
            try:
                markets.append(WinlineRawMarketItem.model_validate({**item, "raw_json": item}))
            except (ValidationError, TypeError, ValueError):
                continue

        return WinlineRawPayload(source_name="winline", events=events, markets=markets)

    def _parse_real_winline_payload(
        self,
        *,
        events_raw: list[Any],
        championships_by_id: dict[Any, Any],
        lines_by_event_id: dict[Any, Any],
    ) -> WinlineRawPayload:
        events: list[WinlineRawEventItem] = []
        markets: list[WinlineRawMarketItem] = []

        for raw_event in events_raw:
            if not isinstance(raw_event, dict):
                continue

            normalized_event = self._build_event_from_winline(
                raw_event=raw_event,
                championships_by_id=championships_by_id,
            )
            if normalized_event is None:
                continue

            events.append(normalized_event)

            raw_event_id = raw_event.get("id")
            line_items = (
                lines_by_event_id.get(raw_event_id)
                or lines_by_event_id.get(str(raw_event_id))
                or []
            )
            if not isinstance(line_items, list):
                continue

            for raw_line in line_items:
                if not isinstance(raw_line, dict):
                    continue

                markets.extend(
                    self._build_markets_from_winline_line(
                        raw_event=raw_event,
                        raw_line=raw_line,
                    )
                )

        return WinlineRawPayload(source_name="winline", events=events, markets=markets)

    def _build_event_from_winline(
        self,
        *,
        raw_event: dict[str, Any],
        championships_by_id: dict[Any, Any],
    ) -> WinlineRawEventItem | None:
        event_id = raw_event.get("id")
        sport_value = raw_event.get("idSport")
        championship_id = raw_event.get("idChampionship")
        members = raw_event.get("members")
        event_date = raw_event.get("date")
        is_live = raw_event.get("isLive")

        if event_id in (None, ""):
            return None
        if not isinstance(members, list) or len(members) < 2:
            return None

        home_team = self._clean_text(members[0])
        away_team = self._clean_text(members[1])
        if not home_team or not away_team:
            return None

        championship = (
            championships_by_id.get(championship_id)
            or championships_by_id.get(str(championship_id))
            or {}
        )
        tournament_name = self._clean_text(championship.get("name")) if isinstance(championship, dict) else ""
        if not tournament_name:
            tournament_name = "Unknown tournament"

        sport = self._map_sport(sport_value)
        if sport is None:
            return None

        match_name = f"{home_team} vs {away_team}"
        event_start_at = self._parse_datetime(event_date)

        try:
            return WinlineRawEventItem(
                event_external_id=str(event_id),
                sport=sport.value,
                tournament_name=tournament_name,
                match_name=match_name,
                home_team=home_team,
                away_team=away_team,
                event_start_at=event_start_at,
                is_live=bool(is_live),
                raw_json=raw_event,
            )
        except (ValidationError, TypeError, ValueError):
            return None

    def _build_markets_from_winline_line(
        self,
        *,
        raw_event: dict[str, Any],
        raw_line: dict[str, Any],
    ) -> list[WinlineRawMarketItem]:
        event_id = raw_line.get("idEvent")
        if event_id in (None, ""):
            return []

        tip_line = raw_line.get("tipLine")
        if isinstance(tip_line, dict):
            selections = tip_line.get("R")
            template_label = tip_line.get("freeTextR")
        else:
            selections = raw_line.get("R")
            template_label = raw_line.get("freeTextR")

        odds = raw_line.get("V")
        market_type = raw_line.get("idTipMarket")
        market_param = raw_line.get("koef")
        id_tip_event = raw_line.get("idTipEvent")

        if not isinstance(selections, list) or not isinstance(odds, list):
            return []
        if market_type in (None, ""):
            return []

        label_source = template_label if self._clean_text(template_label) else str(market_type)
        base_market_label = self._cleanup_market_label_text(label_source, market_param=market_param)
        section_name = self._map_section_name(id_tip_event)

        home_team, away_team = self._extract_teams(raw_event)
        result: list[WinlineRawMarketItem] = []

        for idx, selection_value in enumerate(selections):
            if idx >= len(odds):
                continue

            selection = self._cleanup_selection(selection_value)
            if not selection:
                continue

            odds_value = self._parse_decimal(odds[idx])
            if odds_value is None:
                continue

            search_hint = self._build_search_hint(
                home_team=home_team,
                away_team=away_team,
                market_label=base_market_label,
                selection=selection,
            )

            normalized_market = {
                "event_external_id": str(event_id),
                "market_type": str(market_type),
                "market_label": base_market_label,
                "selection": selection,
                "odds_value": odds_value,
                "section_name": section_name,
                "subsection_name": None,
                "search_hint": search_hint,
                "raw_json": {
                    "line": raw_line,
                    "outcome_index": idx,
                    "raw_selection": selection_value,
                    "raw_odds": odds[idx],
                },
            }

            try:
                result.append(WinlineRawMarketItem.model_validate(normalized_market))
            except (ValidationError, TypeError, ValueError):
                continue

        return result

    def to_candidates(self, raw: WinlineRawPayload) -> ProviderAdapterResult:
        events_by_id: dict[str, WinlineRawEventItem] = {event.event_external_id: event for event in raw.events}
        candidates: list[ProviderSignalCandidate] = []
        skipped = 0

        for market_item in raw.markets:
            event_item = events_by_id.get(market_item.event_external_id)
            if event_item is None:
                skipped += 1
                continue

            sport = self._map_sport(event_item.sport)
            if sport is None:
                skipped += 1
                continue

            try:
                match = ProviderMatch(
                    external_event_id=event_item.event_external_id,
                    sport=sport,
                    tournament_name=event_item.tournament_name,
                    match_name=event_item.match_name,
                    home_team=event_item.home_team,
                    away_team=event_item.away_team,
                    event_start_at=event_item.event_start_at,
                    is_live=bool(event_item.is_live),
                    source_name=raw.source_name,
                )
                provider_market = ProviderOddsMarket(
                    bookmaker=BookmakerType.WINLINE,
                    market_type=market_item.market_type,
                    market_label=market_item.market_label or market_item.market_type,
                    selection=market_item.selection,
                    odds_value=market_item.odds_value,
                    section_name=market_item.section_name,
                    subsection_name=market_item.subsection_name,
                    search_hint=market_item.search_hint,
                )
                candidates.append(
                    ProviderSignalCandidate(
                        match=match,
                        market=provider_market,
                        min_entry_odds=market_item.odds_value,
                        predicted_prob=None,
                        implied_prob=None,
                        edge=None,
                        model_name=None,
                        model_version_name=None,
                        feature_snapshot_json={
                            "source_name": "winline",
                            "raw_event_id": event_item.event_external_id,
                            "raw_market_type": market_item.market_type,
                            "adapter": "winline",
                        },
                    )
                )
            except (ValidationError, TypeError, ValueError):
                skipped += 1
                continue

        return ProviderAdapterResult(
            source_name=raw.source_name,
            total_events=len(raw.events),
            total_markets=len(raw.markets),
            created_candidates=len(candidates),
            skipped_items=skipped,
            candidates=candidates,
        )

    def _map_section_name(self, id_tip_event: Any) -> str | None:
        if id_tip_event is None or id_tip_event == "":
            return "Main"
        try:
            key = int(id_tip_event)
        except (TypeError, ValueError):
            return "Main"
        mapping = {
            3: "Handicap",
            4: "Totals",
            5: "1X2",
            6: "Handicap",
            7: "Totals",
            15: "Both Teams To Score",
        }
        return mapping.get(key, "Main")

    def _cleanup_market_label_text(self, raw: Any, *, market_param: Any) -> str:
        """Replace-based cleanup for Winline freeTextR templates (signals-friendly, not full i18n)."""
        text = self._clean_text(raw)
        if not text:
            text = "market"

        text = text.replace("Обе забьют (@NP@)", "Both Teams To Score")
        text = text.replace("@1HT@ тотал [a]", "1st half total")
        text = text.replace("@1HT@ исход 1X2", "1st half 1X2")
        text = text.replace("@1HT@ фора [a]", "1st half handicap")
        text = text.replace("Тотал [a] (@NP@)", "Total")
        text = text.replace("Фора [a] (@NP@)", "Handicap")
        text = text.replace("@1HT@", "1st half ")
        text = text.replace("@NP@", "")
        text = text.replace("[a]", "")
        text = " ".join(text.split())

        param = self._clean_text(market_param)
        if param:
            return f"{text} {param}".strip()
        return text

    def _cleanup_selection(self, value: Any) -> str:
        """Trim; skip empty. Keeps typical Winline outcome strings (1/X/2, totals, yes/no, team names)."""
        s = self._clean_text(value)
        return s

    def _build_search_hint(
        self,
        *,
        home_team: str,
        away_team: str,
        market_label: str,
        selection: str,
    ) -> str:
        parts = [home_team, away_team, market_label, selection]
        return " ".join(part for part in parts if part).strip()

    def _extract_teams(self, raw_event: dict[str, Any]) -> tuple[str, str]:
        members = raw_event.get("members")
        if not isinstance(members, list) or len(members) < 2:
            return "", ""
        return self._clean_text(members[0]), self._clean_text(members[1])

    def _clean_text(self, value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip()

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

    def _parse_decimal(self, value: Any) -> Decimal | None:
        if value is None or value == "":
            return None
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError):
            return None

    def _map_sport(self, value: Any) -> SportType | None:
        if isinstance(value, int):
            if value == 1:
                return SportType.FOOTBALL
            return None
        if isinstance(value, str) and value.strip().isdigit():
            try:
                n = int(value.strip())
                if n == 1:
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