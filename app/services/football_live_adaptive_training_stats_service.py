"""Aggregates for football live combat → adaptive learning data coverage (no ML)."""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.enums import BetResult, SportType
from app.db.models.signal import Signal
from app.services.signal_runtime_diagnostics_service import SignalRuntimeDiagnosticsService

_MAX_SIGNALS_SCAN = 50_000


def _rationale_training_ready(ex: dict[str, Any] | None) -> bool:
    """Minimum fields adaptive + audits expect."""
    if not isinstance(ex, dict):
        return False
    r = ex.get("football_live_signal_rationale")
    if not isinstance(r, dict):
        return False
    if not r.get("why_selected_codes"):
        return False
    if not r.get("market_family"):
        return False
    if not str(r.get("send_path") or "").strip():
        return False
    if not isinstance(r.get("warnings"), dict):
        return False
    if not isinstance(r.get("live_context"), dict):
        return False
    if r.get("plausibility_score") is None and not str(r.get("plausibility_level") or "").strip():
        return False
    return True


def _has_outcome_audit(fs: dict[str, Any] | None) -> bool:
    if not isinstance(fs, dict):
        return False
    aud = fs.get("football_outcome_audit")
    if not isinstance(aud, dict):
        return False
    return bool(str(aud.get("outcome_reason_code") or "").strip())


async def compute_and_publish_football_live_adaptive_training_stats(session: AsyncSession) -> dict[str, Any]:
    """
    Counts combat live_auto signals; scans up to _MAX_SIGNALS_SCAN newest for JSON fields.
    Exact total_*; scanned-sample for rationale / settlement detail when table is huge.
    """
    cond = (
        (Signal.sport == SportType.FOOTBALL)
        & (Signal.is_live.is_(True))
        & (Signal.notes == "live_auto")
    )
    total = int((await session.execute(select(func.count()).select_from(Signal).where(cond))).scalar_one() or 0)

    stmt = (
        select(Signal)
        .where(cond)
        .options(selectinload(Signal.prediction_logs), selectinload(Signal.settlement))
        .order_by(Signal.id.desc())
        .limit(min(_MAX_SIGNALS_SCAN, max(total, 1)))
    )
    rows = list((await session.execute(stmt)).scalars().unique().all())

    n_any_rationale = 0
    n_rationale_ready = 0
    n_settled_winlose = 0
    n_with_outcome_code = 0
    n_adaptive_training_ready = 0
    n_missing_minute_when_score_in_rat = 0

    for sig in rows:
        if not sig.prediction_logs:
            continue
        pl0 = min(sig.prediction_logs, key=lambda p: p.id)
        ex = dict(pl0.explanation_json or {})
        fs = dict(pl0.feature_snapshot_json or {})
        rat = ex.get("football_live_signal_rationale")
        if isinstance(rat, dict):
            n_any_rationale += 1
        if _rationale_training_ready(ex):
            n_rationale_ready += 1
            rat = ex.get("football_live_signal_rationale")
            lc = rat.get("live_context") if isinstance(rat, dict) else {}
            fa = fs.get("football_analytics") if isinstance(fs.get("football_analytics"), dict) else {}
            has_min = lc.get("minute") is not None or fa.get("minute") is not None
            has_sc = (lc.get("score_home") is not None and lc.get("score_away") is not None) or (
                fa.get("score_home") is not None and fa.get("score_away") is not None
            )
            if not has_min and has_sc:
                n_missing_minute_when_score_in_rat += 1

        st = sig.settlement
        if st and st.result in (BetResult.WIN, BetResult.LOSE):
            n_settled_winlose += 1
            if _has_outcome_audit(fs):
                n_with_outcome_code += 1
            if _rationale_training_ready(ex) and _has_outcome_audit(fs):
                n_adaptive_training_ready += 1

    warn_ru: str | None = None
    if total > 0 and n_adaptive_training_ready < 25:
        warn_ru = (
            f"Мало сигналов, готовых для adaptive ({n_adaptive_training_ready} < 25 в выборке): "
            "нужны settled WIN/LOSE + полный rationale + outcome_reason_code в snapshot."
        )

    blob: dict[str, Any] = {
        "combat_live_auto_signals_total_exact": total,
        "signals_scanned_for_json": len(rows),
        "scan_capped": total > len(rows),
        "with_any_rationale_in_explanation": n_any_rationale,
        "with_training_ready_rationale": n_rationale_ready,
        "with_settlement_win_or_lose": n_settled_winlose,
        "with_outcome_reason_code_in_audit": n_with_outcome_code,
        "adaptive_training_ready_signals_count": n_adaptive_training_ready,
        "anomaly_missing_minute_but_has_score": n_missing_minute_when_score_in_rat,
        "adaptive_training_warning_ru": warn_ru,
    }

    SignalRuntimeDiagnosticsService().update(
        football_live_combat_signals_total=total,
        football_live_with_any_rationale_count=n_any_rationale,
        football_live_with_training_ready_rationale_count=n_rationale_ready,
        football_live_with_settlement_winlose_count=n_settled_winlose,
        football_live_with_outcome_reason_code_count=n_with_outcome_code,
        adaptive_training_ready_signals_count=n_adaptive_training_ready,
        football_live_adaptive_training_warning_ru=warn_ru,
        football_live_adaptive_training_stats_json=json.dumps(blob, ensure_ascii=False)[:20000],
    )
    return blob
