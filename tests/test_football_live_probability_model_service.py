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
            tournament_name="Premier League",
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
    assert len(res.top) == 1
    row = res.top[0]
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
    row = res.top[0]
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
    row = res.top[0]
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
