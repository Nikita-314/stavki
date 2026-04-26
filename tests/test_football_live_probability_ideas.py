from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

from app.core.enums import BookmakerType, SportType
from app.db.models.football_live_probability_idea import FootballLiveProbabilityIdea
from app.schemas.provider_models import ProviderMatch, ProviderOddsMarket, ProviderSignalCandidate
from app.services.football_live_probability_ideas_service import FootballLiveProbabilityIdeasService
from app.services.football_live_probability_ideas_settlement_service import FootballLiveProbabilityIdeasSettlementService
from app.services.football_live_probability_model_service import FootballLiveProbabilityModelService


def _run(coro):
    return asyncio.run(coro)


def _candidate_for_value_edge(odds: str = "2.00") -> ProviderSignalCandidate:
    return ProviderSignalCandidate(
        match=ProviderMatch(
            external_event_id="evt-ve",
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
            market_type="1x2",
            market_label="1X2",
            selection="1",
            odds_value=Decimal(odds),
        ),
        min_entry_odds=Decimal("1.40"),
        feature_snapshot_json={
            "football_analytics": {"minute": 52, "score_home": 1, "score_away": 0},
            "api_football_team_intelligence": {"confidence_score": 80.0},
        },
    )


def test_save_usable_idea_and_skip_raw(monkeypatch) -> None:
    async def scenario() -> None:
        import app.services.football_live_probability_ideas_service as mod

        class _FakeSession:
            def __init__(self) -> None:
                self.rows: list[FootballLiveProbabilityIdea] = []

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return None

            def add_all(self, ideas):
                self.rows.extend(list(ideas))

            async def commit(self):
                return None

        holder = {"session": _FakeSession()}

        class _FakeSessionmaker:
            def __call__(self):
                return holder["session"]

        monkeypatch.setattr(mod, "_get_sessionmaker", lambda: _FakeSessionmaker())
        svc = FootballLiveProbabilityIdeasService()
        saved = await svc.persist_usable_rows(
            [
                {
                    "event_id": "e1",
                    "match": "A vs B",
                    "home": "A",
                    "away": "B",
                    "minute": 55,
                    "score_home": 1,
                    "score_away": 0,
                    "bet_kind": "match_total_over",
                    "best_bet": "ТБ 1.5",
                    "line": 1.5,
                    "best_bet_odds": 1.85,
                    "implied_probability": 0.5405,
                    "model_probability": 0.62,
                    "value_edge": 0.0795,
                    "confidence_score": 75,
                    "risk_level": "low",
                    "api_intelligence_available": True,
                    "reasons": ["x"],
                    "missing_data": [],
                    "is_usable": True,
                },
                {
                    "event_id": "e2",
                    "match": "C vs D",
                    "best_bet": "ТБ 6.5",
                    "is_usable": False,
                },
            ]
        )
        assert saved == 1
        assert len(holder["session"].rows) == 1
        assert holder["session"].rows[0].event_id == "e1"

    _run(scenario())


def test_settle_match_total_over() -> None:
    idea = FootballLiveProbabilityIdea(
        event_id="e-mto",
        match_name="A vs B",
        home_team="A",
        away_team="B",
        market="match_total_over",
        selection="ТБ 2.5",
        line=Decimal("2.5"),
        odds=Decimal("1.90"),
        risk_level="low",
    )
    svc = FootballLiveProbabilityIdeasSettlementService()
    res = svc._determine_result(idea, 2, 1)
    assert str(res.value) == "WIN"


def test_settle_team_total_over() -> None:
    idea = FootballLiveProbabilityIdea(
        event_id="e-tto",
        match_name="Home vs Away",
        home_team="Home",
        away_team="Away",
        market="team_total_over",
        selection="ИТБ Away 1.5",
        line=Decimal("1.5"),
        odds=Decimal("2.10"),
        risk_level="low",
    )
    svc = FootballLiveProbabilityIdeasSettlementService()
    res = svc._determine_result(idea, 1, 2)
    assert str(res.value) == "WIN"


def test_settle_1x2() -> None:
    idea = FootballLiveProbabilityIdea(
        event_id="e-1x2",
        match_name="Home vs Away",
        home_team="Home",
        away_team="Away",
        market="ft_1x2",
        selection="П1: Home",
        odds=Decimal("2.40"),
        risk_level="low",
    )
    svc = FootballLiveProbabilityIdeasSettlementService()
    res = svc._determine_result(idea, 2, 0)
    assert str(res.value) == "WIN"


def test_value_edge_calculation_matches_difference() -> None:
    svc = FootballLiveProbabilityModelService()
    base = _candidate_for_value_edge("2.00")
    rows = [
        base,
        ProviderSignalCandidate(
            match=base.match,
            market=ProviderOddsMarket(
                bookmaker=BookmakerType.WINLINE,
                market_type="1x2",
                market_label="1X2",
                selection="X",
                odds_value=Decimal("3.30"),
            ),
            min_entry_odds=Decimal("1.40"),
            feature_snapshot_json=base.feature_snapshot_json,
        ),
        ProviderSignalCandidate(
            match=base.match,
            market=ProviderOddsMarket(
                bookmaker=BookmakerType.WINLINE,
                market_type="1x2",
                market_label="1X2",
                selection="2",
                odds_value=Decimal("4.10"),
            ),
            min_entry_odds=Decimal("1.40"),
            feature_snapshot_json=base.feature_snapshot_json,
        ),
    ]
    res = svc.evaluate(rows)
    row = res.top_raw[0]
    implied = float(row["implied_probability"] or 0.0)
    model = float(row["model_probability"] or 0.0)
    edge = float(row["value_edge"] or 0.0)
    assert abs((model - implied) - edge) < 1e-6
