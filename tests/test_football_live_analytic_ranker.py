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
    home_team: str = "Home",
    away_team: str = "Away",
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
            match_name=f"{home_team} vs {away_team}",
            home_team=home_team,
            away_team=away_team,
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
    assert "blocked_1x2_without_api_intelligence" in str(row["block_reason"])
    assert "blocked_1x2_00_without_pressure" in str(row["block_reason"])


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
    assert "goals_needed_not_1" in str(row["block_reason"])


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
    assert "competition_blocked" in str(row["block_reason"])
    assert "blocked_high_risk_preview" in str(row["block_reason"])


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
    assert "competition_blocked" in str(row["block_reason"])


def test_cyrillic_reserve_team_marker_is_blocked() -> None:
    row = FootballLiveAnalyticRankerService().evaluate(
        _candidate(
            market_type="total_goals",
            market_label="Тотал [a] (@NP@)",
            selection="Больше 1.5",
            score_home=1,
            score_away=0,
            away_team="Спорт Уанкайо (рез)",
            section_name="Totals",
        )
    )
    assert row is not None
    assert row["send_eligible"] is False
    assert row["preview_bucket"] == "blocked"
    assert "competition_blocked" in str(row["block_reason"])


def test_exotic_result_like_is_blocked() -> None:
    row = FootballLiveAnalyticRankerService().evaluate(
        _candidate(
            market_type="match_winner",
            market_label="Европейский гандикап",
            selection="1",
            score_home=1,
            score_away=1,
            api=True,
        )
    )
    assert row is not None
    assert row["send_eligible"] is False
    assert "blocked_exotic_result_like" in str(row["block_reason"])


def test_1x2_trailing_side_is_blocked() -> None:
    row = FootballLiveAnalyticRankerService().evaluate(
        _candidate(
            market_type="1x2",
            market_label="1X2",
            selection="1",
            score_home=1,
            score_away=2,
            api=True,
        )
    )
    assert row is not None
    assert row["send_eligible"] is False
    assert "blocked_trailing_side_1x2" in str(row["block_reason"])


def test_late_match_total_over_is_blocked() -> None:
    row = FootballLiveAnalyticRankerService().evaluate(
        _candidate(
            market_type="total_goals",
            market_label="Тотал [a] (@NP@)",
            selection="Больше 3.5",
            minute=80,
            score_home=2,
            score_away=1,
            odds="1.80",
            section_name="Totals",
        )
    )
    assert row is not None
    assert row["goals_needed_to_win"] == 1
    assert row["send_eligible"] is False
    assert "blocked_late_total_over" in str(row["block_reason"])


def test_team_total_high_odds_is_blocked() -> None:
    row = FootballLiveAnalyticRankerService().evaluate(
        _candidate(
            market_type="total_goals",
            market_label="Инд. тотал 1",
            selection="ИТ1 Больше 1.5",
            score_home=1,
            score_away=0,
            odds="3.11",
            section_name="Team totals",
        )
    )
    assert row is not None
    assert row["goals_needed_to_win"] == 1
    assert row["send_eligible"] is False
    assert row["preview_bucket"] == "watchlist"
    assert "blocked_team_total_high_odds" in str(row["block_reason"])


def test_match_total_low_odds_is_blocked() -> None:
    row = FootballLiveAnalyticRankerService().evaluate(
        _candidate(
            market_type="total_goals",
            market_label="Тотал [a] (@NP@)",
            selection="Больше 0.5",
            minute=47,
            score_home=0,
            score_away=0,
            odds="1.25",
            section_name="Totals",
        )
    )
    assert row is not None
    assert row["send_eligible"] is False
    assert row["preview_bucket"] == "watchlist"
    assert "blocked_total_odds_window" in str(row["block_reason"])


def test_team_total_early_minute_is_blocked() -> None:
    row = FootballLiveAnalyticRankerService().evaluate(
        _candidate(
            market_type="total_goals",
            market_label="Инд. тотал 1",
            selection="ИТ1 Больше 0.5",
            minute=9,
            score_home=0,
            score_away=0,
            odds="1.80",
            section_name="Team totals",
        )
    )
    assert row is not None
    assert row["send_eligible"] is False
    assert "blocked_total_minute_window" in str(row["block_reason"])


def test_period_total_is_blocked() -> None:
    row = FootballLiveAnalyticRankerService().evaluate(
        _candidate(
            market_type="total_goals",
            market_label="1-й тайм Тотал [a] (@NP@)",
            selection="Больше 0.5",
            minute=45,
            score_home=0,
            score_away=0,
            odds="1.80",
            section_name="1-й тайм",
        )
    )
    assert row is not None
    assert row["send_eligible"] is False
    assert row["preview_bucket"] == "blocked"
    assert "blocked_period_total" in str(row["block_reason"])


def test_high_risk_late_total_can_be_watchlist_not_eligible() -> None:
    row = FootballLiveAnalyticRankerService().evaluate(
        _candidate(
            market_type="total_goals",
            market_label="Тотал [a] (@NP@)",
            selection="Больше 3.5",
            minute=78,
            score_home=2,
            score_away=1,
            odds="1.80",
            section_name="Totals",
        )
    )
    assert row is not None
    assert row["send_eligible"] is False
    assert row["preview_bucket"] == "watchlist"
    assert "blocked_late_total_over" in str(row["block_reason"])


def test_exotic_result_does_not_enter_watchlist() -> None:
    row = FootballLiveAnalyticRankerService().evaluate(
        _candidate(
            market_type="match_winner",
            market_label="Европейский гандикап",
            selection="1",
            score_home=1,
            score_away=1,
            api=True,
        )
    )
    assert row is not None
    assert row["preview_bucket"] == "blocked"


def test_1x2_with_api_can_enter_watchlist() -> None:
    row = FootballLiveAnalyticRankerService().evaluate(
        _candidate(
            market_type="1x2",
            market_label="1X2",
            selection="1",
            score_home=0,
            score_away=0,
            api=True,
        )
    )
    assert row is not None
    assert row["send_eligible"] is False
    assert row["preview_bucket"] == "watchlist"
    assert "blocked_1x2_00_without_pressure" in str(row["block_reason"])


def test_trailing_1x2_does_not_enter_watchlist() -> None:
    row = FootballLiveAnalyticRankerService().evaluate(
        _candidate(
            market_type="1x2",
            market_label="1X2",
            selection="1",
            score_home=1,
            score_away=2,
            api=True,
        )
    )
    assert row is not None
    assert row["preview_bucket"] == "blocked"


def test_team_total_odds_3_0_can_be_watchlist_not_eligible() -> None:
    row = FootballLiveAnalyticRankerService().evaluate(
        _candidate(
            market_type="total_goals",
            market_label="Инд. тотал 1",
            selection="ИТ1 Больше 1.5",
            score_home=1,
            score_away=0,
            odds="3.00",
            section_name="Team totals",
        )
    )
    assert row is not None
    assert row["send_eligible"] is False
    assert row["preview_bucket"] == "watchlist"
    assert "blocked_team_total_high_odds" in str(row["block_reason"])
