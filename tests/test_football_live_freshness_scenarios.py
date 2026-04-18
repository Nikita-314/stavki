"""Регрессия для football live-only freshness: сценарии stale / fresh (без HTTP)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from app.core.config import get_settings
from app.core.enums import BookmakerType, SportType
from app.schemas.provider_models import ProviderMatch, ProviderOddsMarket, ProviderSignalCandidate
from app.services.football_live_freshness_service import (
    evaluate_manual_live_source_freshness,
    filter_stale_live_football_candidates,
)


def _cand(eid: str, kickoff: datetime, minute: int | None = 88) -> ProviderSignalCandidate:
    fs = {"minute": minute} if minute is not None else None
    match = ProviderMatch(
        external_event_id=eid,
        sport=SportType.FOOTBALL,
        tournament_name="T",
        match_name="A vs B",
        home_team="A",
        away_team="B",
        event_start_at=kickoff,
        is_live=True,
        source_name="t",
    )
    market = ProviderOddsMarket(
        bookmaker=BookmakerType.WINLINE,
        market_type="mw",
        market_label="M",
        selection="A",
        odds_value=Decimal("2.0"),
        section_name="F",
    )
    return ProviderSignalCandidate(
        match=match,
        market=market,
        min_entry_odds=Decimal("1.5"),
        predicted_prob=Decimal("0.5"),
        implied_prob=Decimal("0.5"),
        edge=Decimal("0"),
        model_name="t",
        model_version_name="v0",
        signal_score=Decimal("60"),
        feature_snapshot_json=fs,
    )


def test_stale_manual_source_blocks_uploaded_at() -> None:
    settings = get_settings()
    old = (datetime.now(timezone.utc) - timedelta(hours=4)).isoformat()
    mf = evaluate_manual_live_source_freshness(uploaded_at=old, file_path=None, settings=settings)
    assert mf.stale is True
    assert "too_old" in mf.reason or mf.reason == "uploaded_at_too_old"


def test_stale_live_event_kickoff_drops_all_markets() -> None:
    settings = get_settings()
    now = datetime.now(timezone.utc)
    kickoff = now - timedelta(hours=9)
    a = _cand("e1", kickoff)
    b = _cand("e1", kickoff)
    kept, _rows, fe, se, dm = filter_stale_live_football_candidates(
        [a, b],
        source_mode="live",
        source_age_seconds=10.0,
        source_timestamp_iso=now.isoformat(),
        settings=settings,
    )
    assert len(kept) == 0
    assert fe == 0
    assert se >= 1
    assert dm == 2


def test_fresh_live_event_passes_freshness() -> None:
    settings = get_settings()
    now = datetime.now(timezone.utc)
    kickoff = now - timedelta(minutes=50)
    c = _cand("fresh", kickoff, minute=40)
    kept, rows, fe, se, dm = filter_stale_live_football_candidates(
        [c],
        source_mode="live",
        source_age_seconds=5.0,
        source_timestamp_iso=now.isoformat(),
        settings=settings,
    )
    assert len(kept) == 1
    assert fe == 1
    assert se == 0
    assert dm == 0
    assert rows[0].stale is False
