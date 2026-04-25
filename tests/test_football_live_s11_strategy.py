from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

from app.core.enums import BookmakerType, SportType
from app.schemas.provider_models import ProviderMatch, ProviderOddsMarket, ProviderSignalCandidate
from app.services.football_bet_formatter_service import FootballBetFormatterService
from app.services.football_live_strategy_service import evaluate_s11_live_match_total_over_need_1_controlled


def _candidate(
    *,
    selection: str,
    score_home: int,
    score_away: int,
    minute: int,
    odds: str = "1.80",
    market_label: str = "Тотал [a] (@NP@)",
    tournament_name: str = "Malaysia Super League",
) -> ProviderSignalCandidate:
    match = ProviderMatch(
        external_event_id="evt-s11",
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
        market_label=market_label,
        selection=selection,
        odds_value=Decimal(odds),
        section_name="Totals",
        subsection_name="Goals",
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
            }
        },
    )


def test_formatter_renders_match_total_over_short() -> None:
    pres = FootballBetFormatterService().format_bet(
        market_type="total_goals",
        market_label="Тотал [a] (@NP@)",
        selection="Больше 1.5",
        home_team="Melaka",
        away_team="Sabah",
        section_name="Totals",
        subsection_name="Goals",
    )
    assert pres.main_label == "ТБ 1.5"


def test_s11_passes_match_total_over_when_one_goal_needed() -> None:
    cand = _candidate(selection="Больше 2.5", score_home=1, score_away=1, minute=52, odds="2.05")
    dec = asyncio.run(evaluate_s11_live_match_total_over_need_1_controlled(cand))
    assert dec.passed is True
    assert dec.strategy_id == "S11_LIVE_MATCH_TOTAL_OVER_NEED_1_CONTROLLED"


def test_s11_rejects_when_two_goals_needed() -> None:
    cand = _candidate(selection="Больше 1.5", score_home=0, score_away=0, minute=45, odds="1.80")
    dec = asyncio.run(evaluate_s11_live_match_total_over_need_1_controlled(cand))
    assert dec.passed is False
    assert "goals_needed_not_1" in (dec.reasons or [])


def test_s11_rejects_team_total() -> None:
    cand = _candidate(selection="IT1 Over 0.5", score_home=0, score_away=0, minute=45, odds="1.80")
    dec = asyncio.run(evaluate_s11_live_match_total_over_need_1_controlled(cand))
    assert dec.passed is False
    assert "market_not_match_total" in (dec.reasons or [])


def test_s11_rejects_blocked_competition() -> None:
    cand = _candidate(
        selection="Больше 0.5",
        score_home=0,
        score_away=0,
        minute=45,
        odds="1.80",
        tournament_name="Women Super League",
    )
    dec = asyncio.run(evaluate_s11_live_match_total_over_need_1_controlled(cand))
    assert dec.passed is False
    assert "competition_blocked" in (dec.reasons or [])


def test_s11_rejects_cyrillic_youth_marker() -> None:
    cand = _candidate(
        selection="Больше 0.5",
        score_home=0,
        score_away=0,
        minute=45,
        odds="1.80",
        tournament_name="1-я Лига (до19)",
    )
    dec = asyncio.run(evaluate_s11_live_match_total_over_need_1_controlled(cand))
    assert dec.passed is False
    assert "competition_blocked" in (dec.reasons or [])
