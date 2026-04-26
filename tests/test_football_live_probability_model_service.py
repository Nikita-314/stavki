from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from app.core.enums import BookmakerType, SportType
from app.schemas.provider_models import ProviderMatch, ProviderOddsMarket, ProviderSignalCandidate
from app.services.football_live_probability_model_service import FootballLiveProbabilityModelService


def _candidate(
    *,
    event_id: str,
    market_type: str,
    market_label: str,
    selection: str,
    odds: str,
    minute: int = 50,
    score_home: int = 1,
    score_away: int = 0,
    api: bool = False,
    section_name: str | None = None,
    subsection_name: str | None = None,
    tournament_name: str = "Premier League",
    match_name: str = "Home vs Away",
    home_team: str = "Home",
    away_team: str = "Away",
) -> ProviderSignalCandidate:
    fs: dict[str, object] = {
        "football_analytics": {
            "minute": minute,
            "score_home": score_home,
            "score_away": score_away,
        }
    }
    if api:
        fs["api_football_team_intelligence"] = {
            "confidence_score": 82.0,
            "avg_goals_for_home": 1.8,
            "avg_goals_for_away": 1.2,
            "avg_goals_against_home": 0.9,
            "avg_goals_against_away": 1.3,
            "standings_edge": {"rank_edge_home_minus_away": 4},
            "h2h_matches": 4,
            "h2h_home_wins": 3,
            "h2h_away_wins": 1,
            "common_opponent_edge": {"edge_home_minus_away": 0.5},
        }
    return ProviderSignalCandidate(
        match=ProviderMatch(
            external_event_id=event_id,
            sport=SportType.FOOTBALL,
            tournament_name=tournament_name,
            match_name=match_name,
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


def test_probability_model_returns_row_and_balanced_1x2_probs() -> None:
    svc = FootballLiveProbabilityModelService()
    rows = [
        _candidate(event_id="evt-1", market_type="1x2", market_label="1X2", selection="1", odds="2.00", api=True),
        _candidate(event_id="evt-1", market_type="1x2", market_label="1X2", selection="X", odds="3.40", api=True),
        _candidate(event_id="evt-1", market_type="1x2", market_label="1X2", selection="2", odds="4.20", api=True),
    ]
    res = svc.evaluate(rows)
    assert res.total_matches == 1
    assert len(res.top_raw) == 1
    row = res.top_raw[0]
    total = float(row["home_win_probability"]) + float(row["draw_probability"]) + float(row["away_win_probability"])
    assert 0.99 <= total <= 1.01


def test_probability_model_works_without_api_and_reduces_confidence() -> None:
    svc = FootballLiveProbabilityModelService()
    rows = [
        _candidate(event_id="evt-2", market_type="1x2", market_label="1X2", selection="1", odds="2.10", api=False),
        _candidate(event_id="evt-2", market_type="1x2", market_label="1X2", selection="X", odds="3.20", api=False),
        _candidate(event_id="evt-2", market_type="1x2", market_label="1X2", selection="2", odds="3.70", api=False),
    ]
    res = svc.evaluate(rows)
    assert res.total_matches == 1
    row = res.top_raw[0]
    assert row["api_intelligence_available"] is False
    assert int(row["confidence_score"]) < 90
    assert "api_intelligence" in list(row["missing_data"])


def test_probability_model_can_pick_total_over_as_best_bet() -> None:
    svc = FootballLiveProbabilityModelService()
    rows = [
        _candidate(event_id="evt-3", market_type="1x2", market_label="1X2", selection="1", odds="2.40", api=True),
        _candidate(event_id="evt-3", market_type="1x2", market_label="1X2", selection="X", odds="3.20", api=True),
        _candidate(event_id="evt-3", market_type="1x2", market_label="1X2", selection="2", odds="2.90", api=True),
        _candidate(
            event_id="evt-3",
            market_type="total_goals",
            market_label="Тотал [a] (@NP@)",
            selection="Больше 1.5",
            odds="1.95",
            minute=56,
            score_home=1,
            score_away=0,
            api=True,
            section_name="Totals",
        ),
    ]
    res = svc.evaluate(rows)
    row = res.top_raw[0]
    assert row["best_bet"] is not None
    assert float(row["model_probability"]) > 0.0
    assert float(row["implied_probability"]) > 0.0


def test_probability_model_counts_thresholds() -> None:
    svc = FootballLiveProbabilityModelService()
    rows = [
        _candidate(event_id="evt-4", market_type="1x2", market_label="1X2", selection="1", odds="2.00", api=True),
        _candidate(event_id="evt-4", market_type="1x2", market_label="1X2", selection="X", odds="3.20", api=True),
        _candidate(event_id="evt-4", market_type="1x2", market_label="1X2", selection="2", odds="4.00", api=True),
    ]
    res = svc.evaluate(rows)
    assert res.value_edge_7_count >= 0
    assert res.confidence_60_count == 1


def test_u20_high_edge_not_in_usable() -> None:
    svc = FootballLiveProbabilityModelService()
    rows = [
        _candidate(
            event_id="evt-u20",
            market_type="total_goals",
            market_label="Тотал [a] (@NP@)",
            selection="Больше 5.5",
            odds="4.20",
            minute=62,
            score_home=2,
            score_away=1,
            section_name="Totals",
            tournament_name="League U20",
            match_name="Home U20 vs Away U20",
        )
    ]
    res = svc.evaluate(rows)
    assert res.total_matches == 1
    assert res.usable_count == 0
    assert len(res.usable_top) == 0


def test_88_minute_high_edge_not_in_usable() -> None:
    svc = FootballLiveProbabilityModelService()
    rows = [
        _candidate(
            event_id="evt-late",
            market_type="total_goals",
            market_label="Тотал [a] (@NP@)",
            selection="Больше 1.5",
            odds="2.10",
            minute=88,
            score_home=1,
            score_away=0,
            section_name="Totals",
        )
    ]
    res = svc.evaluate(rows)
    assert res.usable_count == 0
    assert "late_gt_75" in list(res.top_raw[0].get("usable_blockers") or [])


def test_line_7_5_not_in_usable() -> None:
    svc = FootballLiveProbabilityModelService()
    rows = [
        _candidate(
            event_id="evt-high-line",
            market_type="total_goals",
            market_label="Тотал [a] (@NP@)",
            selection="Больше 7.5",
            odds="2.30",
            minute=52,
            score_home=4,
            score_away=1,
            section_name="Totals",
        )
    ]
    res = svc.evaluate(rows)
    assert res.usable_count == 0
    assert "match_total_line_gt_3_5" in list(res.top_raw[0].get("usable_blockers") or [])


def test_no_api_1x2_not_in_usable() -> None:
    svc = FootballLiveProbabilityModelService()
    rows = [
        _candidate(event_id="evt-raw-1x2", market_type="1x2", market_label="1X2", selection="1", odds="2.40", api=False),
        _candidate(event_id="evt-raw-1x2", market_type="1x2", market_label="1X2", selection="X", odds="3.10", api=False),
        _candidate(event_id="evt-raw-1x2", market_type="1x2", market_label="1X2", selection="2", odds="3.20", api=False),
    ]
    res = svc.evaluate(rows)
    assert res.usable_count == 0
    assert "1x2_without_api_blocked" in list(res.top_raw[0].get("usable_blockers") or [])


def test_normal_match_total_need1_can_be_usable() -> None:
    svc = FootballLiveProbabilityModelService()
    rows = [
        _candidate(
            event_id="evt-usable-total",
            market_type="total_goals",
            market_label="Тотал [a] (@NP@)",
            selection="Больше 2.5",
            odds="1.95",
            minute=60,
            score_home=1,
            score_away=1,
            api=False,
            section_name="Totals",
        )
    ]
    res = svc.evaluate(rows)
    assert res.usable_count == 1
    assert len(res.usable_top) == 1
    assert bool(res.usable_top[0].get("is_usable")) is True


def test_api_1x2_with_confidence_can_be_usable() -> None:
    svc = FootballLiveProbabilityModelService()
    rows = [
        _candidate(event_id="evt-api-1x2", market_type="1x2", market_label="1X2", selection="1", odds="2.10", api=True),
        _candidate(event_id="evt-api-1x2", market_type="1x2", market_label="1X2", selection="X", odds="3.20", api=True),
        _candidate(event_id="evt-api-1x2", market_type="1x2", market_label="1X2", selection="2", odds="4.20", api=True),
    ]
    res = svc.evaluate(rows)
    assert res.usable_count == 1
    assert len(res.usable_top) == 1
    assert bool(res.usable_top[0].get("is_usable")) is True
    assert str(res.usable_top[0].get("bet_kind")) == "ft_1x2"
