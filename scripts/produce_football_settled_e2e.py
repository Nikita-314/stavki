#!/usr/bin/env python3
"""
Controlled football E2E: ingest 1x2 signals via IngestionService, settle via ResultIngestionService
(same code paths as production), then apply football outcome reasons + refresh post-match summary.

Idempotent in the sense: each run uses fresh event_external_id values (no dedup collision).
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
os.chdir(_ROOT)


def _candidate(
    eid: str,
    *,
    home_side: str,
    away_side: str,
    selection: str,
):
    from app.core.enums import BookmakerType, SportType
    from app.schemas.provider_models import ProviderMatch, ProviderOddsMarket, ProviderSignalCandidate

    return ProviderSignalCandidate(
        match=ProviderMatch(
            external_event_id=eid,
            sport=SportType.FOOTBALL,
            tournament_name="E2E Controlled Cup",
            match_name=f"{home_side} vs {away_side}",
            home_team=home_side,
            away_team=away_side,
            event_start_at=datetime.now(timezone.utc),
            is_live=False,
            source_name="e2e_controlled",
        ),
        market=ProviderOddsMarket(
            bookmaker=BookmakerType.WINLINE,
            market_type="1x2",
            market_label="Match result",
            selection=selection,
            odds_value=Decimal("1.95"),
        ),
        min_entry_odds=Decimal("1.90"),
        signal_score=Decimal("0.50"),
        notes="e2e_football_outcome",
        feature_snapshot_json={"e2e": True, "score_home": 0, "score_away": 0},
        explanation_json={
            "e2e": True,
            "football_live_send_path": "normal",
            "live_sanity": {"plausibility": "ok", "plausibility_score": 90},
        },
    )


async def _main() -> int:
    from app.core.config import get_settings
    from app.core.enums import SportType
    from app.db.models.signal import Signal
    from app.db.session import get_sessionmaker
    from app.schemas.event_result import EventResultInput
    from app.services.football_learning_service import FootballLearningService
    from app.services.football_signal_outcome_reason_service import (
        _refresh_football_postmatch_summary,
        build_football_postmatch_verify_report,
    )
    from app.services.ingestion_service import IngestionService
    from app.services.result_ingestion_service import ResultIngestionService
    from sqlalchemy import select
    from sqlalchemy.orm import selectinload

    u = uuid4().hex[:10]
    eid_win = f"e2e_fb_{u}_a"
    eid_lose = f"e2e_fb_{u}_b"
    c_win = _candidate(eid_win, home_side="E2E City", away_side="E2E Town", selection="home")
    c_lose = _candidate(eid_lose, home_side="E2E North", away_side="E2E South", selection="home")
    # Result: first pick wins (home W), second bet loses (away W).

    settings = get_settings()
    sm = get_sessionmaker(settings.database_url)

    async with sm() as session:
        ing = await IngestionService().ingest_candidates(session, [c_win, c_lose])
        if ing.created_signal_ids:
            pass
        else:
            print("INGEST: no signals created (all skipped?)", file=sys.stderr)
        await session.commit()
        sids = list(ing.created_signal_ids)

    if len(sids) < 2:
        print(f"expected 2 new signals, got {sids!r}. dedup/validation — abort.", file=sys.stderr)
        return 2

    async with sm() as session:
        pr1 = await ResultIngestionService().process_event_result(
            session,
            EventResultInput(
                event_external_id=eid_win,
                sport=SportType.FOOTBALL,
                winner_selection="home",
                is_void=False,
                settled_at=datetime.now(timezone.utc),
                result_payload_json={
                    "home_score": 2,
                    "away_score": 0,
                    "final_scoreline": "2-0",
                    "winner": "home",
                },
            ),
        )
        pr2 = await ResultIngestionService().process_event_result(
            session,
            EventResultInput(
                event_external_id=eid_lose,
                sport=SportType.FOOTBALL,
                winner_selection="away",
                is_void=False,
                settled_at=datetime.now(timezone.utc),
                result_payload_json={
                    "home_score": 0,
                    "away_score": 1,
                    "final_scoreline": "0-1",
                    "winner": "away",
                },
            ),
        )
        await _refresh_football_postmatch_summary(session)
        await session.commit()
        print(
            f"ingest: created_signal_ids={sids}  result1: settled={pr1.settled_signals}  result2: settled={pr2.settled_signals}"
        )

    # Read back
    async with sm() as session:
        for sid in sids:
            stmt = (
                select(Signal)
                .where(Signal.id == int(sid))
                .options(selectinload(Signal.settlement), selectinload(Signal.prediction_logs))
            )
            sig = (await session.execute(stmt)).scalar_one_or_none()
            if not sig or not sig.settlement:
                print(f"signal {sid} missing settlement", file=sys.stderr)
                continue
            pl0 = min(sig.prediction_logs, key=lambda p: p.id) if sig.prediction_logs else None
            snap = dict(pl0.feature_snapshot_json or {}) if pl0 else {}
            aud = snap.get("football_outcome_audit")
            ex = (pl0.explanation_json or {}) if pl0 else {}
            ex_set = (ex or {}).get("football_settlement")
            bet = f"{sig.market_type} / {sig.market_label} / {sig.selection}"
            sa = sig.signaled_at.isoformat() if sig.signaled_at else "—"
            print("---")
            print(f"signal_id={sig.id}  event_external_id={sig.event_external_id}")
            print(f"  match: {sig.home_team} — {sig.away_team}  | signaled_at={sa}")
            print(f"  bet: {bet}")
            print(f"  settlement: {sig.settlement.result.value}")
            print(
                f"  football_outcome_audit: {aud if isinstance(aud, dict) else 'MISSING'} (feature_snapshot_json)"
            )
            print(
                f"  football_settlement (explanation): keys={list(ex_set.keys()) if isinstance(ex_set, dict) else 'n/a'}"
            )

        rpt = await build_football_postmatch_verify_report(session, limit=30, detail_count=10)
        ag = await FootballLearningService().aggregate_outcome_reason_losses(session, lookback=200)
    print("=== build_football_postmatch_verify_report ===")
    print(rpt)
    print("=== aggregate_outcome_reason_losses ===", ag)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
