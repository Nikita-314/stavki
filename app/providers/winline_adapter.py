from __future__ import annotations

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

    Current skeleton expects a pre-normalized shape with `events` and `markets`. After the
    manual DevTools investigation, replace the placeholder extraction points inside
    `parse_payload()` with the real JSON paths from Winline.
    """

    def parse_payload(self, payload: dict[str, Any]) -> WinlineRawPayload:
        """Parse a future Winline line payload into normalized raw schemas.

        Event-level fields that later need real Winline JSON paths:
        - `event_external_id`
        - `sport`
        - `tournament_name`
        - `match_name`
        - `home_team`
        - `away_team`
        - `event_start_at`
        - `is_live`

        Market-level fields that later need real Winline JSON paths:
        - `event_external_id`
        - `market_type`
        - `market_label`
        - `selection`
        - `odds_value`
        - `section_name`
        - `subsection_name`
        - `search_hint`

        Practical mapper plan once DevTools payloads are captured:
        - locate the real event container in list payload or nested payload
        - extract event-level fields using provider JSON paths found in the runbook
        - flatten event-card market groups into plain market items
        - extract outcome titles and coefficients from the real outcome nodes
        - normalize `section_name`, `subsection_name`, and `search_hint` after raw extraction

        Until real payload paths are known, malformed inputs return an empty normalized
        payload rather than guessing provider structure.
        """
        if not isinstance(payload, dict):
            return WinlineRawPayload(source_name='winline', events=[], markets=[])

        events_raw = payload.get('events')
        markets_raw = payload.get('markets')
        if not isinstance(events_raw, list) or not isinstance(markets_raw, list):
            return WinlineRawPayload(source_name='winline', events=[], markets=[])

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
                events.append(WinlineRawEventItem.model_validate({**item, 'raw_json': item}))
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
                markets.append(WinlineRawMarketItem.model_validate({**item, 'raw_json': item}))
            except (ValidationError, TypeError, ValueError):
                continue

        return WinlineRawPayload(source_name='winline', events=events, markets=markets)

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
                            'source_name': 'winline',
                            'raw_event_id': event_item.event_external_id,
                            'raw_market_type': market_item.market_type,
                            'adapter': 'winline',
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
        normalized = ' '.join((value or '').strip().lower().replace('-', ' ').replace('_', ' ').split())
        if normalized in {'football', 'soccer'}:
            return SportType.FOOTBALL
        if normalized in {'cs2', 'cs 2', 'counter strike', 'counter strike 2'}:
            return SportType.CS2
        if normalized in {'dota2', 'dota 2'}:
            return SportType.DOTA2
        return None
