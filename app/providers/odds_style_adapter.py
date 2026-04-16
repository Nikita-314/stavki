from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import ValidationError

from app.core.enums import BookmakerType, SportType
from app.providers.base_adapter import BaseProviderAdapter
from app.schemas.provider_adapter import ProviderAdapterResult
from app.schemas.provider_models import ProviderMatch, ProviderOddsMarket, ProviderSignalCandidate
from app.schemas.provider_raw import RawEventItem, RawMarketItem, RawProviderPayload


class OddsStyleAdapter(BaseProviderAdapter):
    """Concrete adapter for an odds-style payload with nested bookmakers/markets/outcomes."""

    def parse_payload(self, payload: dict[str, Any]) -> RawProviderPayload:
        source_name = str(payload.get("source_name") or "odds_style")
        data = payload.get("data", [])

        events: list[RawEventItem] = []
        markets: list[RawMarketItem] = []

        if not isinstance(data, list):
            return RawProviderPayload(source_name=source_name, events=[], markets=[])

        for ev in data:
            if not isinstance(ev, dict):
                continue
            try:
                external_event_id = str(ev.get("id") or "").strip()
                if not external_event_id:
                    continue

                sport_key = ev.get("sport_key")
                sport_title = ev.get("sport_title")
                sport = str(sport_key or sport_title or "").strip()
                if not sport:
                    continue

                home_team = str(ev.get("home_team") or "").strip()
                away_team = str(ev.get("away_team") or "").strip()
                if not home_team or not away_team:
                    continue

                commence_time = ev.get("commence_time")
                event_start_at: datetime | None = None
                if isinstance(commence_time, str) and commence_time.strip():
                    s = commence_time.strip()
                    # Accept ISO strings with trailing Z.
                    try:
                        if s.endswith("Z"):
                            s = s[:-1] + "+00:00"
                        event_start_at = datetime.fromisoformat(s)
                    except Exception:
                        event_start_at = None

                tournament_name = str(ev.get("tournament_name") or sport_title or "unknown_tournament").strip()
                match_name = f"{home_team} vs {away_team}"

                events.append(
                    RawEventItem(
                        external_event_id=external_event_id,
                        sport=sport,
                        tournament_name=tournament_name,
                        match_name=match_name,
                        home_team=home_team,
                        away_team=away_team,
                        event_start_at=event_start_at,
                        is_live=False,
                        raw_json=ev,
                    )
                )
            except (ValidationError, TypeError, ValueError):
                continue

            bookmakers = ev.get("bookmakers", [])
            if not isinstance(bookmakers, list):
                continue

            for b in bookmakers:
                if not isinstance(b, dict):
                    continue
                b_key = b.get("key")
                b_title = b.get("title")
                bookmaker = str(b_key or b_title or "").strip()
                if not bookmaker:
                    continue

                b_markets = b.get("markets", [])
                if not isinstance(b_markets, list):
                    continue

                for mk in b_markets:
                    if not isinstance(mk, dict):
                        continue
                    market_key = str(mk.get("key") or "").strip()
                    if not market_key:
                        continue

                    outcomes = mk.get("outcomes", [])
                    if not isinstance(outcomes, list):
                        continue

                    for oc in outcomes:
                        if not isinstance(oc, dict):
                            continue
                        name = str(oc.get("name") or "").strip()
                        price = oc.get("price")
                        if not name or price is None:
                            continue
                        try:
                            odds_value = Decimal(str(price))
                        except Exception:
                            continue

                        search_hint = f"{events[-1].home_team} {events[-1].away_team} {name}"
                        raw_json = {"outcome": oc, "market": {"key": market_key}, "bookmaker": {"key": b_key, "title": b_title}}
                        try:
                            markets.append(
                                RawMarketItem(
                                    external_event_id=events[-1].external_event_id,
                                    bookmaker=bookmaker,
                                    market_type=market_key,
                                    market_label=market_key,
                                    selection=name,
                                    odds_value=odds_value,
                                    section_name=str(ev.get("sport_title") or "").strip() or None,
                                    subsection_name=str(b_title or "").strip() or None,
                                    search_hint=search_hint,
                                    raw_json=raw_json,
                                )
                            )
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

            market_type = self._map_market_type(m.market_type)

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
                    market_type=market_type,
                    market_label=m.market_label or market_type,
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
                            "adapter": "odds_style",
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
        # The Odds API uses sport keys like `soccer_epl`, `soccer_uefa_champs_league`.
        if v in {"soccer", "football"} or v.startswith("soccer_"):
            return SportType.FOOTBALL
        if v in {"cs2", "counter_strike"}:
            return SportType.CS2
        if v in {"dota2", "dota 2"}:
            return SportType.DOTA2
        return None

    def _map_bookmaker(self, value: str) -> BookmakerType | None:
        v = (value or "").strip().lower()
        if v == "fonbet":
            return BookmakerType.FONBET
        if v == "winline":
            return BookmakerType.WINLINE
        if v == "betboom":
            return BookmakerType.BETBOOM
        if v == "betboom" or v == "betboom bookmaker":
            return BookmakerType.BETBOOM
        # Titles (case-insensitive)
        if v == "fonbet":
            return BookmakerType.FONBET
        if v == "winline":
            return BookmakerType.WINLINE
        if v == "betboom":
            return BookmakerType.BETBOOM
        return None

    def _map_market_type(self, value: str) -> str:
        v = (value or "").strip().lower()
        mapping = {
            "h2h": "match_winner",
            "moneyline": "match_winner",
            "totals": "total_goals",
            "spreads": "handicap",
        }
        return mapping.get(v, v or value)

