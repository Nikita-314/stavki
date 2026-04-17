from __future__ import annotations

from typing import Any

from pydantic import ValidationError

from app.core.enums import BookmakerType, SportType
from app.providers.base_adapter import BaseProviderAdapter
from app.schemas.provider_adapter import ProviderAdapterResult
from app.schemas.provider_models import ProviderMatch, ProviderOddsMarket, ProviderSignalCandidate
from app.schemas.provider_raw import RawEventItem, RawMarketItem, RawProviderPayload


class GenericOddsAdapter(BaseProviderAdapter):
    def parse_payload(self, payload: dict[str, Any]) -> RawProviderPayload:
        source_name = str(payload.get("source_name") or "generic_odds")

        events_raw = payload.get("events", [])
        markets_raw = payload.get("markets", [])

        events: list[RawEventItem] = []
        markets: list[RawMarketItem] = []

        if isinstance(events_raw, list):
            for item in events_raw:
                if not isinstance(item, dict):
                    continue
                try:
                    ev = RawEventItem.model_validate({**item, "raw_json": item})
                    events.append(ev)
                except (ValidationError, TypeError, ValueError):
                    continue

        if isinstance(markets_raw, list):
            for item in markets_raw:
                if not isinstance(item, dict):
                    continue
                try:
                    mk = RawMarketItem.model_validate({**item, "raw_json": item})
                    markets.append(mk)
                except (ValidationError, TypeError, ValueError):
                    continue

        return RawProviderPayload(source_name=source_name, events=events, markets=markets)

    def to_candidates(self, raw: RawProviderPayload) -> ProviderAdapterResult:
        events_by_id: dict[str, RawEventItem] = {e.external_event_id: e for e in raw.events}

        candidates: list[ProviderSignalCandidate] = []
        skipped = 0

        for m in raw.markets:
            ev = events_by_id.get(m.external_event_id)
            if ev is None:
                skipped += 1
                continue

            sport = self._map_sport(ev.sport)
            bookmaker = self._map_bookmaker(m.bookmaker)
            if sport is None or bookmaker is None:
                skipped += 1
                continue

            try:
                match = ProviderMatch(
                    external_event_id=ev.external_event_id,
                    sport=sport,
                    tournament_name=ev.tournament_name,
                    match_name=ev.match_name,
                    home_team=ev.home_team,
                    away_team=ev.away_team,
                    event_start_at=ev.event_start_at,
                    is_live=bool(ev.is_live),
                    source_name=raw.source_name,
                )

                market = ProviderOddsMarket(
                    bookmaker=bookmaker,
                    market_type=m.market_type,
                    market_label=m.market_label,
                    selection=m.selection,
                    odds_value=m.odds_value,
                    section_name=m.section_name,
                    subsection_name=m.subsection_name,
                    search_hint=m.search_hint,
                )

                candidates.append(
                    ProviderSignalCandidate(
                        match=match,
                        market=market,
                        min_entry_odds=m.odds_value,
                        predicted_prob=None,
                        implied_prob=None,
                        edge=None,
                        model_name=None,
                        model_version_name=None,
                        feature_snapshot_json={
                            "source_name": raw.source_name,
                            "raw_event_id": ev.external_event_id,
                            "raw_market_type": m.market_type,
                            "source_market_type": m.market_type,
                            "source_market_label": m.market_label,
                            "source_selection": m.selection,
                            "source_odds_value": str(m.odds_value),
                            "source_section_name": m.section_name,
                            "source_subsection_name": m.subsection_name,
                            "source_bookmaker": m.bookmaker,
                            "source_raw_market_json": m.raw_json,
                            "adapter": "generic_odds",
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

    def _map_sport(self, value: str) -> SportType | None:
        v = (value or "").strip().lower()
        if v == "cs2":
            return SportType.CS2
        if v in {"dota2", "dota 2"}:
            return SportType.DOTA2
        if v in {"football", "soccer"}:
            return SportType.FOOTBALL
        return None

    def _map_bookmaker(self, value: str) -> BookmakerType | None:
        v = (value or "").strip().lower()
        if v == "fonbet":
            return BookmakerType.FONBET
        if v == "winline":
            return BookmakerType.WINLINE
        if v == "betboom":
            return BookmakerType.BETBOOM
        return None

