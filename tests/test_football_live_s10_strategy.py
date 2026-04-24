from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

from app.core.enums import BookmakerType, SportType
from app.schemas.provider_models import ProviderMatch, ProviderOddsMarket, ProviderSignalCandidate
from app.services.football_bet_formatter_service import FootballBetFormatterService
from app.services.football_live_strategy_service import evaluate_s10_live_team_total_over_controlled


def _candidate(
    *,
    selection: str,
    score_home: int,
    score_away: int,
    minute: int,
    odds: str = "1.80",
    tournament_name: str = "Malaysia Super League",
) -> ProviderSignalCandidate:
    match = ProviderMatch(
        external_event_id="evt-1",
        sport=SportType.FOOTBALL,
        tournament_name=tournament_name,
        match_name="Melaka vs Sabah",
        home_team="Melaka",
        away_team="Sabah",
        event_start_at=datetime.now(timezone.utc),
        is_live=True,
        source_name="winline",
    )
    market = ProviderOddsMarket(
        bookmaker=BookmakerType.WINLINE,
        market_type="total_goals",
        market_label="Individual Total",
        selection=selection,
        odds_value=Decimal(odds),
        section_name="Goals",
    )
    return ProviderSignalCandidate(
        match=match,
        market=market,
        min_entry_odds=Decimal("1.50"),
        predicted_prob=Decimal("0.55"),
        implied_prob=Decimal("0.52"),
        edge=Decimal("0.03"),
        model_name="test",
        model_version_name="v1",
        signal_score=Decimal("61"),
        feature_snapshot_json={
            "football_analytics": {
                "minute": minute,
                "score_home": score_home,
                "score_away": score_away,
                "red_cards_home": 0,
                "red_cards_away": 0,
            }
        },
    )


def test_formatter_renders_team_total_as_itb() -> None:
    pres = FootballBetFormatterService().format_bet(
        market_type="total_goals",
        market_label="Individual Total",
        selection="IT1 Over 0.5",
        home_team="Melaka",
        away_team="Sabah",
        section_name="Goals",
    )
    assert pres.main_label == "ИТБ Melaka 0.5"


def test_s10_passes_when_team_total_needs_one_goal() -> None:
    cand = _candidate(selection="IT1 Over 1.0", score_home=1, score_away=0, minute=34)
    dec = asyncio.run(evaluate_s10_live_team_total_over_controlled(cand))
    assert dec.passed is True
    assert dec.strategy_id == "S10_LIVE_TEAM_TOTAL_OVER_CONTROLLED"


def test_s10_rejects_when_two_goals_needed() -> None:
    cand = _candidate(selection="IT1 Over 1.5", score_home=0, score_away=0, minute=29)
    dec = asyncio.run(evaluate_s10_live_team_total_over_controlled(cand))
    assert dec.passed is False
    assert "goals_needed_gt1" in (dec.reasons or [])


def test_s10_rejects_when_line_already_won() -> None:
    cand = _candidate(selection="IT1 Over 0.5", score_home=1, score_away=0, minute=29)
    dec = asyncio.run(evaluate_s10_live_team_total_over_controlled(cand))
    assert dec.passed is False
    assert "already_won_line" in (dec.reasons or [])


def test_s10_allows_20_only_for_team_with_zero_goals_on_05() -> None:
    cand = _candidate(selection="IT2 Over 0.5", score_home=2, score_away=0, minute=52)
    dec = asyncio.run(evaluate_s10_live_team_total_over_controlled(cand))
    assert dec.passed is True


def test_s10_rejects_selected_team_red_card() -> None:
    cand = _candidate(selection="IT1 Over 0.5", score_home=0, score_away=0, minute=40)
    cand.feature_snapshot_json["football_analytics"]["red_cards_home"] = 1
    dec = asyncio.run(evaluate_s10_live_team_total_over_controlled(cand))
    assert dec.passed is False
    assert "red_card_block" in (dec.reasons or [])
