from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from app.core.enums import BetResult, SportType
from app.services.result_ingestion_service import ResultIngestionService


def _signal(
    *,
    market_label: str,
    selection: str,
    home_team: str = "Antigua",
    away_team: str = "Malacateco",
) -> SimpleNamespace:
    return SimpleNamespace(
        sport=SportType.FOOTBALL,
        market_type="total_goals",
        market_label=market_label,
        selection=selection,
        home_team=home_team,
        away_team=away_team,
        section_name="Goals",
        subsection_name=None,
        odds_at_signal=Decimal("2.00"),
    )


def test_match_total_over_settles_win() -> None:
    svc = ResultIngestionService()
    signal = _signal(market_label="Тотал [a] (@NP@)", selection="Больше 2.5")
    res = svc._determine_result(
        signal=signal,
        is_void=False,
        winner_selection=None,
        result_payload_json={"score_home": 2, "score_away": 1},
    )
    assert res == BetResult.WIN


def test_match_total_under_settles_void_on_push() -> None:
    svc = ResultIngestionService()
    signal = _signal(market_label="Тотал [a] (@NP@)", selection="Меньше 3")
    res = svc._determine_result(
        signal=signal,
        is_void=False,
        winner_selection=None,
        result_payload_json={"score_home": 2, "score_away": 1},
    )
    assert res == BetResult.VOID


def test_team_total_over_settles_win() -> None:
    svc = ResultIngestionService()
    signal = _signal(
        market_label="Тотал [a] (@NP@) @2",
        selection="Больше 1.5",
        home_team="Huachipato",
        away_team="Audax Italiano",
    )
    res = svc._determine_result(
        signal=signal,
        is_void=False,
        winner_selection=None,
        result_payload_json={"score_home": 1, "score_away": 2},
    )
    assert res == BetResult.WIN


def test_team_total_under_settles_lose() -> None:
    svc = ResultIngestionService()
    signal = _signal(
        market_label="Тотал [a] (@NP@) @2",
        selection="Меньше 1",
        home_team="Huachipato",
        away_team="Audax Italiano",
    )
    res = svc._determine_result(
        signal=signal,
        is_void=False,
        winner_selection=None,
        result_payload_json={"score_home": 1, "score_away": 2},
    )
    assert res == BetResult.LOSE


def test_unknown_when_total_context_cannot_be_determined() -> None:
    svc = ResultIngestionService()
    signal = _signal(market_label="Totals", selection="Strange market text")
    res = svc._determine_result(
        signal=signal,
        is_void=False,
        winner_selection=None,
        result_payload_json={"score_home": 2, "score_away": 1},
    )
    assert res is None


def test_zero_goal_payload_is_not_lost_by_or_fallback() -> None:
    svc = ResultIngestionService()
    signal = _signal(market_label="Тотал [a] (@NP@)", selection="Больше 1.5")
    res = svc._determine_result(
        signal=signal,
        is_void=False,
        winner_selection=None,
        result_payload_json={"score_home": 1, "score_away": 0},
    )
    assert res == BetResult.LOSE
