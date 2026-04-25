from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from app.core.enums import BookmakerType, SportType
from app.schemas.provider_models import ProviderMatch, ProviderOddsMarket, ProviderSignalCandidate
from app.services.football_live_analytic_ranker_service import FootballLiveAnalyticRankerService


def _candidate(
    *,
    market_type: str,
    market_label: str,
    selection: str,
    minute: int = 50,
    score_home: int = 0,
    score_away: int = 0,
    odds: str = "1.80",
    tournament_name: str = "Premier League",
    section_name: str | None = None,
    subsection_name: str | None = None,
    api: bool = False,
) -> ProviderSignalCandidate:
    fs = {
        "football_analytics": {
            "minute": minute,
            "score_home": score_home,
            "score_away": score_away,
        }
    }
    if api:
        fs["api_football_team_intelligence"] = {
            "confidence_score": 100.0,
            "avg_goals_for_home": 1.6,
            "avg_goals_for_away": 1.4,
            "standings_edge": {"available": True, "rank_edge_home_minus_away": 3},
            "h2h_matches": 5,
            "h2h_home_wins": 3,
            "h2h_away_wins": 1,
            "common_opponent_edge": {"count": 2, "edge_home_minus_away": 0.5},
        }
    return ProviderSignalCandidate(
        match=ProviderMatch(
            external_event_id="evt-ranker",
            sport=SportType.FOOTBALL,
            tournament_name=tournament_name,
            match_name="Home vs Away",
            home_team="Home",
            away_team="Away",
            event_start_at=datetime.now(timezone.utc),
            is_live=True,
            source_name="winline",
        ),
        market=ProviderOddsMarket(
            bookmaker=BookmakerType.WINLINE,
            market_type=market_type,
            market_label=market_label,
            selection=selection,
            odds_value=Decimal(odds),
            section_name=section_name,
            subsection_name=subsection_name,
        ),
        min_entry_odds=Decimal("1.40"),
        feature_snapshot_json=fs,
    )


def test_1x2_00_without_api_is_hard_blocked() -> None:
    row = FootballLiveAnalyticRankerService().evaluate(
        _candidate(market_type="1x2", market_label="1X2", selection="1", score_home=0, score_away=0)
    )
    assert row is not None
    assert row["send_eligible"] is False
    assert row["block_reason"] == "ft_1x2_00_without_api_intelligence"


def test_match_total_over_need_1_is_eligible() -> None:
    row = FootballLiveAnalyticRankerService().evaluate(
        _candidate(
            market_type="total_goals",
            market_label="Тотал [a] (@NP@)",
            selection="Больше 1.5",
            score_home=1,
            score_away=0,
            section_name="Totals",
        )
    )
    assert row is not None
    assert row["market_type"] == "match_total_over_need_1"
    assert row["goals_needed_to_win"] == 1
    assert row["send_eligible"] is True


def test_team_total_over_need_1_is_eligible() -> None:
    row = FootballLiveAnalyticRankerService().evaluate(
        _candidate(
            market_type="total_goals",
            market_label="Инд. тотал 1",
            selection="ИТ1 Больше 0.5",
            score_home=0,
            score_away=0,
            section_name="Team totals",
        )
    )
    assert row is not None
    assert row["market_type"] == "team_total_over_need_1"
    assert row["goals_needed_to_win"] == 1
    assert row["send_eligible"] is True


def test_goals_needed_gt_1_is_blocked() -> None:
    row = FootballLiveAnalyticRankerService().evaluate(
        _candidate(
            market_type="total_goals",
            market_label="Тотал [a] (@NP@)",
            selection="Больше 2.5",
            score_home=0,
            score_away=0,
            section_name="Totals",
        )
    )
    assert row is not None
    assert row["goals_needed_to_win"] == 3
    assert row["send_eligible"] is False
    assert row["block_reason"] == "goals_needed_not_1"


def test_youth_competition_is_blocked() -> None:
    row = FootballLiveAnalyticRankerService().evaluate(
        _candidate(
            market_type="total_goals",
            market_label="Тотал [a] (@NP@)",
            selection="Больше 0.5",
            tournament_name="League U19",
            section_name="Totals",
        )
    )
    assert row is not None
    assert row["send_eligible"] is False
    assert row["risk_level"] == "high"
    assert row["block_reason"] == "competition_blocked"


def test_cyrillic_women_marker_is_blocked() -> None:
    row = FootballLiveAnalyticRankerService().evaluate(
        _candidate(
            market_type="total_goals",
            market_label="Тотал [a] (@NP@)",
            selection="Больше 1.5",
            score_home=1,
            score_away=0,
            tournament_name="Аргентина (ж)",
            section_name="Totals",
        )
    )
    assert row is not None
    assert row["send_eligible"] is False
    assert row["block_reason"] == "competition_blocked"
