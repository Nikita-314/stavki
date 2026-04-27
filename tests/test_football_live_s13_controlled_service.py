from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from app.core.enums import BookmakerType, SignalStatus, SportType
from app.schemas.analytics import PredictionLogRead, SignalAnalyticsReport, SignalRead
from app.services.football_live_probability_ideas_service import FootballLiveProbabilityIdeasService
from app.services.football_live_s13_controlled_service import (
    S13_CONTROLLED_STRATEGY_ID,
    evaluate_s13_controlled_idea,
    select_s13_controlled_candidates,
)
from app.services.notification_service import NotificationService


def _row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "match": "Home vs Away",
        "tournament_name": "Premier League",
        "event_id": "evt-s13",
        "minute": 62,
        "score_home": 1,
        "score_away": 1,
        "home": "Home",
        "away": "Away",
        "best_bet": "ТБ 2.5",
        "best_bet_odds": "1.75",
        "bet_kind": "match_total_over",
        "line": 2.5,
        "goals_needed_to_win": 1,
        "implied_probability": 0.55,
        "model_probability": 0.62,
        "value_edge": 0.07,
        "confidence_score": 72,
        "is_usable": True,
        "risk_level": "medium",
        "api_intelligence_available": False,
        "over_next_goal_probability": 0.55,
        "source_market_type": "total_goals",
        "source_market_label": "Тотал [a] (@NP@)",
        "source_selection": "Больше 2.5",
        "source_section_name": "Totals",
        "source_subsection_name": None,
    }
    row.update(overrides)
    return row


def test_s13_controlled_selects_only_strict_usable_idea() -> None:
    out, meta = select_s13_controlled_candidates([_row()], enabled=True)
    assert len(out) == 1
    assert meta["evaluated"] == 1
    assert meta["after_gate"] == 1
    assert meta["sent"] == 1
    assert meta["blocked"] == 0
    assert meta["blocked_by_gate"] == 0
    assert meta["blocked_team_total_over"] == 0
    assert meta["blocked_overconfident_edge"] == 0
    assert meta["blocked_market_rules"] == 0
    cand = out[0]
    assert cand.model_name == S13_CONTROLLED_STRATEGY_ID
    assert cand.predicted_prob == Decimal("0.62")
    assert cand.explanation_json["football_live_strategy_id"] == S13_CONTROLLED_STRATEGY_ID


def test_s13_controlled_blocks_low_edge_low_confidence_and_high_risk() -> None:
    assert evaluate_s13_controlled_idea(_row(value_edge=0.049)).passed is False
    assert "edge_lt_0_05" in evaluate_s13_controlled_idea(_row(value_edge=0.049)).reasons
    assert evaluate_s13_controlled_idea(_row(value_edge=0.051, confidence_score=55)).passed is True
    assert "confidence_lt_55" in evaluate_s13_controlled_idea(_row(confidence_score=54)).reasons
    assert "risk_not_low_medium" in evaluate_s13_controlled_idea(_row(risk_level="high")).reasons


def test_s13_controlled_sorts_by_edge_times_confidence() -> None:
    low = _row(event_id="e-low", value_edge=0.09, confidence_score=55)
    high = _row(event_id="e-high", value_edge=0.09, confidence_score=80)
    out, _meta = select_s13_controlled_candidates([low, high], enabled=True)
    assert len(out) == 2
    assert out[0].signal_score == Decimal("80")
    assert out[1].signal_score == Decimal("55")


def test_s13_team_total_combat_always_blocked() -> None:
    r = _row(
        bet_kind="team_total_over",
        minute=50,
        line=1.5,
        best_bet="ИТБ Home 1.5",
        goals_needed_to_win=1,
        best_bet_odds="1.80",
    )
    d = evaluate_s13_controlled_idea(r)
    assert d.passed is False
    assert "blocked_team_total_over" in d.reasons
    out, meta = select_s13_controlled_candidates([r], enabled=True)
    assert out == []
    assert meta["blocked_team_total_over"] == 1


def test_s13_team_total_row_still_saveable_as_idea() -> None:
    r = _row(
        bet_kind="team_total_over",
        minute=50,
        line=1.5,
        best_bet="ИТБ Home 1.5",
        goals_needed_to_win=1,
        best_bet_odds="1.80",
    )
    assert FootballLiveProbabilityIdeasService()._row_is_saveable(r) is True


def test_s13_overconfident_edge_blocks() -> None:
    d = evaluate_s13_controlled_idea(_row(value_edge=0.10))
    assert d.passed is False
    assert "blocked_overconfident_edge" in d.reasons
    out, meta = select_s13_controlled_candidates([_row(value_edge=0.10)], enabled=True)
    assert out == []
    assert meta["blocked_overconfident_edge"] == 1


def test_s13_match_total_odds_above_2_20_blocked() -> None:
    assert "match_total_odds_combat_window" in evaluate_s13_controlled_idea(_row(best_bet_odds="2.21")).reasons


def test_s13_match_total_valid_passes() -> None:
    assert evaluate_s13_controlled_idea(_row(best_bet_odds="2.20", minute=40, line=3.5)).passed is True


def test_s13_ft_1x2_combat_passes_with_api_conf_edge_band() -> None:
    r = _row(
        bet_kind="ft_1x2",
        best_bet="П1",
        source_selection="1",
        source_market_type="1x2",
        line=None,
        goals_needed_to_win=None,
        value_edge=0.08,
        confidence_score=80,
        api_intelligence_available=True,
        score_home=1,
        score_away=0,
        over_next_goal_probability=0.5,
    )
    assert evaluate_s13_controlled_idea(r).passed is True


def test_s13_ft_1x2_requires_api_conf_75_and_edge_band() -> None:
    base: dict[str, object] = {
        "bet_kind": "ft_1x2",
        "best_bet": "П1",
        "source_selection": "1",
        "source_market_type": "1x2",
        "line": None,
        "goals_needed_to_win": None,
        "value_edge": 0.08,
        "confidence_score": 80,
        "api_intelligence_available": True,
        "score_home": 1,
        "score_away": 0,
    }
    assert "1x2_without_api_intelligence" in evaluate_s13_controlled_idea(_row(**{**base, "api_intelligence_available": False})).reasons
    assert "ft_1x2_confidence_lt_75_combat" in evaluate_s13_controlled_idea(_row(**{**base, "confidence_score": 74})).reasons
    assert "edge_lt_0_05" in evaluate_s13_controlled_idea(_row(**{**base, "value_edge": 0.04})).reasons
    assert "blocked_overconfident_edge" in evaluate_s13_controlled_idea(_row(**{**base, "value_edge": 0.10})).reasons


def test_s13_ft_1x2_00_no_pressure_blocked() -> None:
    r = _row(
        bet_kind="ft_1x2",
        best_bet="П1",
        source_selection="1",
        source_market_type="1x2",
        line=None,
        goals_needed_to_win=None,
        value_edge=0.08,
        confidence_score=80,
        api_intelligence_available=True,
        score_home=0,
        score_away=0,
        over_next_goal_probability=0.40,
    )
    assert "ft_1x2_00_no_pressure" in evaluate_s13_controlled_idea(r).reasons


def test_s13_ft_1x2_trailing_blocked() -> None:
    r = _row(
        bet_kind="ft_1x2",
        best_bet="П1",
        source_selection="1",
        source_market_type="1x2",
        line=None,
        goals_needed_to_win=None,
        value_edge=0.08,
        confidence_score=80,
        api_intelligence_available=True,
        score_home=0,
        score_away=1,
        over_next_goal_probability=0.55,
    )
    assert "ft_1x2_trailing_side_blocked" in evaluate_s13_controlled_idea(r).reasons


def test_s13_signal_message_contains_strategy_and_probability_block() -> None:
    now = datetime.now(timezone.utc)
    report = SignalAnalyticsReport(
        signal=SignalRead(
            id=1,
            created_at=now,
            updated_at=now,
            sport=SportType.FOOTBALL,
            bookmaker=BookmakerType.WINLINE,
            event_external_id="evt-s13",
            tournament_name="Premier League",
            match_name="Home vs Away",
            home_team="Home",
            away_team="Away",
            market_type="total_goals",
            market_label="Тотал [a] (@NP@)",
            selection="Больше 2.5",
            odds_at_signal=Decimal("1.75"),
            min_entry_odds=Decimal("1.01"),
            predicted_prob=Decimal("0.62"),
            implied_prob=Decimal("0.55"),
            edge=Decimal("0.07"),
            model_name=S13_CONTROLLED_STRATEGY_ID,
            model_version_name="controlled_v1",
            signal_score=Decimal("72"),
            status=SignalStatus.NEW,
            section_name="Totals",
            subsection_name=None,
            search_hint=None,
            is_live=True,
            event_start_at=now,
            signaled_at=now,
            notes="live_auto",
        ),
        prediction_logs=[
            PredictionLogRead(
                id=1,
                signal_id=1,
                feature_snapshot_json={
                    "football_analytics": {"minute": 62, "score_home": 1, "score_away": 1},
                    "football_live_s13_probability": _row(),
                },
                raw_model_output_json=None,
                explanation_json={
                    "football_live_strategy_id": S13_CONTROLLED_STRATEGY_ID,
                    "football_live_strategy_reasons": ["s13_controlled_usable", "match_ok"],
                },
                created_at=now,
            )
        ],
        entries=[],
        settlement=None,
        failure_reviews=[],
    )
    text = NotificationService().format_signal_message(report)
    assert "Стратегия: S13" in text
    assert "model_prob: 62.0%" in text
    assert "implied_prob: 55.0%" in text
    assert "edge: +7.0%" in text
    assert "confidence: 72" in text
    assert "S13 reasons:" in text
    assert "match_ok" in text
