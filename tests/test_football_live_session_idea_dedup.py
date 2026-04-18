"""Локальная проверка ключа идеи и дедупа в рамках football live-сессии (без БД, без Telegram)."""
from __future__ import annotations

import unittest
from datetime import datetime, timezone
from decimal import Decimal

from app.core.enums import BookmakerType, SportType
from app.schemas.provider_models import ProviderMatch, ProviderOddsMarket, ProviderSignalCandidate
from app.services.football_live_session_service import (
    FootballLiveSessionService,
    build_live_idea_key,
    reset_live_session_for_tests,
)


def _football_live_candidate(
    *,
    eid: str,
    market_type: str,
    selection: str,
    odds: str = "2.05",
) -> ProviderSignalCandidate:
    now = datetime.now(tz=timezone.utc)
    match = ProviderMatch(
        external_event_id=eid,
        sport=SportType.FOOTBALL,
        tournament_name="Test Cup",
        match_name="Home FC vs Away FC",
        home_team="Home FC",
        away_team="Away FC",
        event_start_at=now,
        is_live=True,
        source_name="test",
    )
    market = ProviderOddsMarket(
        bookmaker=BookmakerType.WINLINE,
        market_type=market_type,
        market_label="Test market",
        selection=selection,
        odds_value=Decimal(odds),
        section_name="Football",
    )
    return ProviderSignalCandidate(
        match=match,
        market=market,
        min_entry_odds=Decimal("1.50"),
        predicted_prob=Decimal("0.55"),
        implied_prob=Decimal("0.50"),
        edge=Decimal("0.05"),
        model_name="test",
        model_version_name="v0",
        signal_score=Decimal("85"),
    )


class FootballLiveIdeaDedupTests(unittest.TestCase):
    def tearDown(self) -> None:
        reset_live_session_for_tests()

    def test_build_live_idea_key_same_match_same_idea_equal(self) -> None:
        a = _football_live_candidate(eid="evt-1", market_type="total_goals", selection="Over 2.5")
        b = _football_live_candidate(eid="evt-1", market_type="total_goals", selection="Over 2.5")
        self.assertEqual(build_live_idea_key(a), build_live_idea_key(b))

    def test_build_live_idea_key_same_match_different_selection_differs(self) -> None:
        a = _football_live_candidate(eid="evt-1", market_type="total_goals", selection="Over 2.5")
        b = _football_live_candidate(eid="evt-1", market_type="total_goals", selection="Under 2.5")
        self.assertNotEqual(build_live_idea_key(a), build_live_idea_key(b))

    def test_session_blocks_same_idea_allows_other_on_same_match(self) -> None:
        """Память идей не зависит от флага active — достаточно reset + register (как внутри live-пайплайна)."""
        svc = FootballLiveSessionService()
        same_idea = _football_live_candidate(eid="evt-42", market_type="match_winner", selection="home fc")
        other_idea = _football_live_candidate(eid="evt-42", market_type="match_winner", selection="away fc")
        k_same = build_live_idea_key(same_idea)
        k_other = build_live_idea_key(other_idea)
        self.assertFalse(svc.has_idea(k_same))
        svc.register_idea_sent(k_same)
        self.assertTrue(svc.has_idea(k_same))
        self.assertFalse(svc.has_idea(k_other))


if __name__ == "__main__":
    unittest.main()
