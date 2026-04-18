#!/usr/bin/env python3
"""
Практическая верификация football live-only freshness без сети и Telegram.

Запуск из корня репозитория:
  python3 scripts/verify_football_live_freshness_scenarios.py

Печатает JSON-трассы для сценариев A (stale) и B (fresh).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from dataclasses import asdict

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from app.core.config import get_settings
from app.core.enums import BookmakerType, SportType
from app.schemas.provider_models import ProviderMatch, ProviderOddsMarket, ProviderSignalCandidate
from app.services.football_live_freshness_service import (
    evaluate_manual_live_source_freshness,
    filter_stale_live_football_candidates,
)


def _candidate(
    *,
    eid: str,
    kickoff: datetime,
    is_live: bool = True,
    minute: int | None = None,
) -> ProviderSignalCandidate:
    fs: dict | None = None
    if minute is not None:
        fs = {"minute": minute}
    match = ProviderMatch(
        external_event_id=eid,
        sport=SportType.FOOTBALL,
        tournament_name="Verify Cup",
        match_name="Home FC vs Away FC",
        home_team="Home FC",
        away_team="Away FC",
        event_start_at=kickoff,
        is_live=is_live,
        source_name="verify",
    )
    market = ProviderOddsMarket(
        bookmaker=BookmakerType.WINLINE,
        market_type="match_winner",
        market_label="Матч",
        selection="Home FC",
        odds_value=Decimal("2.10"),
        section_name="Football",
    )
    return ProviderSignalCandidate(
        match=match,
        market=market,
        min_entry_odds=Decimal("1.50"),
        predicted_prob=Decimal("0.50"),
        implied_prob=Decimal("0.48"),
        edge=Decimal("0.02"),
        model_name="verify",
        model_version_name="v0",
        signal_score=Decimal("70"),
        feature_snapshot_json=fs,
    )


def main() -> int:
    settings = get_settings()
    now = datetime.now(timezone.utc)
    src_ts = now.isoformat()

    # ——— Сценарий A1: протухший semi_live_manual (uploaded_at) ———
    old_uploaded = (now - timedelta(hours=3)).isoformat()
    mf = evaluate_manual_live_source_freshness(
        uploaded_at=old_uploaded,
        file_path=None,
        settings=settings,
    )
    trace_a1 = {
        "scenario": "A1_stale_manual_source",
        "expected_cycle_message": "blocked_stale_manual_live_source",
        "bottleneck_if_that_message": "blocked_stale_manual_live_source",
        "manual_freshness": {
            "stale": mf.stale,
            "reason": mf.reason,
            "source_age_seconds": mf.age_seconds,
            "reference": mf.reference,
        },
        "ui_fields_would_be": {
            "source_mode": "semi_live_manual",
            "source_age_seconds": mf.age_seconds,
            "source_freshness_label": "stale" if mf.stale else "fresh",
            "stale_source": True,
            "live_freshness_candidates_before": 0,
            "live_freshness_events_accepted": 0,
            "live_freshness_stale_events_dropped": 0,
            "live_freshness_stale_markets_dropped": 0,
        },
    }

    # ——— Сценарий A2: live source свежий, но событие «ночное» (kickoff давно) ———
    stale_kickoff = now - timedelta(hours=10)
    c_stale_event = _candidate(eid="evt-night", kickoff=stale_kickoff, minute=12)
    kept_a2, rows_a2, fe_a2, se_a2, dm_a2 = filter_stale_live_football_candidates(
        [c_stale_event, _candidate(eid="evt-night", kickoff=stale_kickoff, minute=12)],
        source_mode="live",
        source_age_seconds=15.0,
        source_timestamp_iso=src_ts,
        settings=settings,
    )
    trace_a2 = {
        "scenario": "A2_stale_live_events_kickoff_too_old",
        "expected_cycle_message": "blocked_stale_live_events",
        "bottleneck_if_all_dropped": "blocked_stale_live_events",
        "source_mode": "live",
        "source_age_seconds": 15.0,
        "source_freshness_label": "fresh",
        "stale_source": False,
        "live_freshness_candidates_before": 2,
        "live_freshness_events_accepted": fe_a2,
        "live_freshness_stale_events_dropped": se_a2,
        "live_freshness_stale_markets_dropped": dm_a2,
        "candidates_kept_after_freshness": len(kept_a2),
        "live_freshness_sample_row": asdict(rows_a2[0]) if rows_a2 else None,
    }

    # ——— Сценарий B: свежий kickoff — matч не режется freshness ———
    fresh_kickoff = now - timedelta(minutes=40)
    c_fresh = _candidate(eid="evt-fresh", kickoff=fresh_kickoff, minute=55)
    kept_b, rows_b, fe_b, se_b, dm_b = filter_stale_live_football_candidates(
        [c_fresh],
        source_mode="live",
        source_age_seconds=8.0,
        source_timestamp_iso=src_ts,
        settings=settings,
    )
    trace_b = {
        "scenario": "B_fresh_live_passes_freshness_layer",
        "source_mode": "live",
        "source_age_seconds": 8.0,
        "source_freshness_label": "fresh",
        "stale_source": False,
        "live_freshness_candidates_before": 1,
        "live_freshness_events_accepted": fe_b,
        "live_freshness_stale_events_dropped": se_b,
        "live_freshness_stale_markets_dropped": dm_b,
        "candidates_kept_after_freshness": len(kept_b),
        "bottleneck_after_freshness_not_stale": "continues_to_candidate_filter",
        "live_freshness_row": asdict(rows_b[0]) if rows_b else None,
    }

    out = {
        "settings_snapshot": {
            "football_live_manual_max_age_minutes": settings.football_live_manual_max_age_minutes,
            "football_live_event_max_kickoff_age_hours": settings.football_live_event_max_kickoff_age_hours,
            "football_live_max_declared_live_minute": settings.football_live_max_declared_live_minute,
        },
        "A1_stale_manual_source": trace_a1,
        "A2_stale_live_event": trace_a2,
        "B_fresh": trace_b,
    }

    # Проверки
    assert mf.stale, "A1: старый uploaded_at должен давать stale manual"
    assert len(kept_a2) == 0 and se_a2 >= 1, "A2: ночной матч не должен проходить freshness"
    assert len(kept_b) == 1 and fe_b == 1 and se_b == 0, "B: свежий матч должен проходить freshness"

    print(json.dumps(out, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except AssertionError as e:
        print(json.dumps({"error": str(e)}, ensure_ascii=False), file=sys.stderr)
        sys.exit(1)
