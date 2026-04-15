from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from app.core.enums import BookmakerType, SportType
from app.providers.base import BaseCandidateProvider
from app.schemas.provider_models import (
    ProviderMatch,
    ProviderOddsMarket,
    ProviderSignalCandidate,
)


class MockCandidateProvider(BaseCandidateProvider):
    async def fetch_candidates(self) -> list[ProviderSignalCandidate]:
        """Return a fixed set of candidates for local end-to-end ingestion checks."""
        now = datetime.now(tz=timezone.utc)

        cs2_match = ProviderMatch(
            external_event_id="cs2_10001",
            sport=SportType.CS2,
            tournament_name="PGL CS2 Open",
            match_name="Team Spirit vs NAVI",
            home_team="Team Spirit",
            away_team="NAVI",
            event_start_at=now,
            is_live=False,
            source_name="mock",
        )

        dota_match = ProviderMatch(
            external_event_id="dota2_20001",
            sport=SportType.DOTA2,
            tournament_name="DreamLeague",
            match_name="Team Liquid vs Gaimin Gladiators",
            home_team="Team Liquid",
            away_team="Gaimin Gladiators",
            event_start_at=now,
            is_live=True,
            source_name="mock",
        )

        football_match = ProviderMatch(
            external_event_id="football_30001",
            sport=SportType.FOOTBALL,
            tournament_name="РПЛ",
            match_name="Зенит vs Спартак",
            home_team="Зенит",
            away_team="Спартак",
            event_start_at=now,
            is_live=False,
            source_name="mock",
        )

        # 1) Valid CS2 candidate
        c1 = ProviderSignalCandidate(
            match=cs2_match,
            market=ProviderOddsMarket(
                bookmaker=BookmakerType.FONBET,
                market_type="match_winner",
                market_label="Match winner",
                selection="Team Spirit",
                odds_value=Decimal("1.85"),
                section_name="CS2",
                subsection_name="Match",
                search_hint="spirit navi winner",
            ),
            min_entry_odds=Decimal("1.70"),
            predicted_prob=Decimal("0.58"),
            implied_prob=Decimal("0.5405"),
            edge=Decimal("0.0395"),
            model_name="baseline_rules",
            model_version_name="v0",
            signal_score=Decimal("0.72"),
            feature_snapshot_json={"rating_diff": 0.12, "map_pool": "balanced"},
        )

        # 2) Duplicate of c1 inside the batch (should be deduped)
        c2 = c1.model_copy(deep=True)

        # 3) Valid Dota2 candidate with alias market_type that should normalize: "moneyline" -> "match_winner"
        c3 = ProviderSignalCandidate(
            match=dota_match,
            market=ProviderOddsMarket(
                bookmaker=BookmakerType.WINLINE,
                market_type="moneyline",
                market_label="Winner",
                selection="Team Liquid",
                odds_value=Decimal("2.10"),
                section_name="Dota2",
                search_hint="liquid gg moneyline",
            ),
            min_entry_odds=Decimal("1.95"),
            predicted_prob=Decimal("0.49"),
            implied_prob=Decimal("0.4762"),
            edge=Decimal("0.0138"),
            model_name="baseline_rules",
            model_version_name="v0",
            signal_score=Decimal("0.61"),
            explanation_json={"rule": "value_if_edge_positive"},
        )

        # 4) Football candidate with market_type not allowed (should be rejected by filter)
        c4 = ProviderSignalCandidate(
            match=football_match,
            market=ProviderOddsMarket(
                bookmaker=BookmakerType.BETBOOM,
                market_type="corners_total",
                market_label="Total corners",
                selection="Over 9.5",
                odds_value=Decimal("1.90"),
            ),
            min_entry_odds=Decimal("1.60"),
            model_name="baseline_rules",
            model_version_name="v0",
        )

        # 6) Candidate with odds out of configured range (should be rejected by odds_above_max)
        c6 = ProviderSignalCandidate(
            match=football_match.model_copy(update={"external_event_id": "football_30002"}),
            market=ProviderOddsMarket(
                bookmaker=BookmakerType.FONBET,
                market_type="1x2",
                market_label="Full time result",
                selection="Спартак",
                odds_value=Decimal("12.50"),
            ),
            min_entry_odds=Decimal("2.00"),
        )

        # 7) Candidate with odds below configured min (should be rejected by odds_below_min)
        c7 = ProviderSignalCandidate(
            match=dota_match.model_copy(update={"external_event_id": "dota2_20002"}),
            market=ProviderOddsMarket(
                bookmaker=BookmakerType.BETBOOM,
                market_type="maps_total",
                market_label="Maps total",
                selection="Over 2.5",
                odds_value=Decimal("1.10"),
            ),
            min_entry_odds=Decimal("1.20"),
        )

        return [c1, c2, c3, c4, c6, c7]

