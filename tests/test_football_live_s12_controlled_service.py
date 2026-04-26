from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from app.core.enums import BookmakerType, SportType
from app.schemas.provider_models import ProviderMatch, ProviderOddsMarket, ProviderSignalCandidate
from app.services.football_live_s12_controlled_service import (
    evaluate_s12_controlled_candidate,
    select_s12_controlled_candidates,
)


def _candidate(
    *,
    market_type: str = "total_goals",
    market_label: str = "Тотал [a] (@NP@)",
    selection: str = "Больше 1.5",
    tournament_name: str = "Premier League",
    section_name: str | None = "Totals",
    odds: str = "1.80",
) -> ProviderSignalCandidate:
    return ProviderSignalCandidate(
        match=ProviderMatch(
            external_event_id="evt-s12",
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
            subsection_name=None,
        ),
        min_entry_odds=Decimal("1.40"),
        feature_snapshot_json={"football_analytics": {"minute": 55, "score_home": 1, "score_away": 0}},
    )


def _row(**kwargs):
    base = {
        "preview_bucket": "eligible",
        "risk_level": "low",
        "market": "match_total_over",
        "market_type": "match_total_over_need_1",
        "goals_needed_to_win": 1,
        "minute": 55,
        "odds": "1.80",
    }
    base.update(kwargs)
    return base


def test_flag_off_no_candidates_selected() -> None:
    out, meta = select_s12_controlled_candidates([_candidate()], enabled=False)
    assert out == []
    assert meta == {"evaluated": 0, "sent": 0, "blocked": 0}


def test_flag_on_strict_match_total_over_need1_passes(monkeypatch) -> None:
    cand = _candidate()
    monkeypatch.setattr(
        "app.services.football_live_analytic_ranker_service.FootballLiveAnalyticRankerService.evaluate",
        lambda self, c: _row(),
    )
    out, meta = select_s12_controlled_candidates([cand], enabled=True)
    assert len(out) == 1
    assert meta["sent"] == 1


def test_watchlist_never_sent() -> None:
    d = evaluate_s12_controlled_candidate(_candidate(), rank_row=_row(preview_bucket="watchlist"))
    assert d.passed is False


def test_team_total_not_sent() -> None:
    d = evaluate_s12_controlled_candidate(
        _candidate(market_label="Инд. тотал 1", selection="ИТ1 Больше 0.5", section_name="Team totals"),
        rank_row=_row(market="team_total_over", market_type="team_total_over_need_1"),
    )
    assert d.passed is False


def test_1x2_not_sent() -> None:
    d = evaluate_s12_controlled_candidate(
        _candidate(market_type="1x2", market_label="1X2", selection="1", section_name=None),
        rank_row=_row(market="1x2", market_type="ft_1x2"),
    )
    assert d.passed is False


def test_odds_above_2_10_not_sent() -> None:
    d = evaluate_s12_controlled_candidate(_candidate(odds="2.40"), rank_row=_row(odds="2.40"))
    assert d.passed is False


def test_minute_above_70_not_sent() -> None:
    d = evaluate_s12_controlled_candidate(_candidate(), rank_row=_row(minute=71))
    assert d.passed is False


def test_youth_reserve_women_not_sent() -> None:
    d = evaluate_s12_controlled_candidate(
        _candidate(tournament_name="League U23"),
        rank_row=_row(),
    )
    assert d.passed is False
