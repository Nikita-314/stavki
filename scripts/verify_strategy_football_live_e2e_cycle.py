from __future__ import annotations

import os
import sys

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from aiogram import Bot
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.core.config import get_settings
from app.core.enums import BetResult, SportType
from app.db.models.prediction_log import PredictionLog
from app.db.models.signal import Signal
from app.db.session import create_engine, create_sessionmaker
from app.services.auto_signal_service import AutoSignalService
from app.services.football_live_session_service import FootballLiveSessionService


@dataclass(frozen=True)
class StrategyMatchRow:
    event_id: str
    match_name: str
    tournament_name: str | None
    strategy_id: str
    strategy_name: str | None
    minute: int | None
    score_home: int | None
    score_away: int | None
    bet_text: str | None
    market_family: str | None
    odds: str | None
    base_signal_score: float | None
    effective_live_score: float | None
    final_gate_decision: str | None
    full_pipeline_decision: str | None
    created_signal_id: int | None
    telegram_sent: bool | None
    blocked_reason: str | None


def _safe_int(v: object) -> int | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _safe_float(v: object) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _extract_final_gate_map(cycle_debug: dict[str, Any]) -> dict[str, dict[str, Any]]:
    fg = cycle_debug.get("final_live_send_gate")
    if not isinstance(fg, dict):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for row in fg.get("per_match") or []:
        if not isinstance(row, dict):
            continue
        eid = str(row.get("event_id") or "").strip()
        if not eid:
            continue
        out[eid] = row
    return out


def _extract_strategy_rows(cycle_debug: dict[str, Any]) -> list[StrategyMatchRow]:
    matches = cycle_debug.get("matches")
    if not isinstance(matches, list):
        return []

    fg_map = _extract_final_gate_map(cycle_debug)
    out: list[StrategyMatchRow] = []
    for r in matches:
        if not isinstance(r, dict):
            continue
        sid = str(r.get("strategy_id") or "").strip()
        if not sid:
            continue
        eid = str(r.get("event_id") or "").strip()
        if not eid:
            continue
        fg = fg_map.get(eid) or {}
        tr = fg.get("delivery_trace_row") if isinstance(fg, dict) else None
        if not isinstance(tr, dict):
            tr = {}

        created_signal_id = _safe_int(tr.get("signal_id"))
        telegram_sent = tr.get("bot_send_message_effective")
        if isinstance(telegram_sent, str):
            telegram_sent = telegram_sent.strip().lower() in ("true", "1", "yes", "ok", "sent")
        elif telegram_sent is not None and not isinstance(telegram_sent, bool):
            telegram_sent = None

        base_score = None
        eff_score = None
        # Best effort: these values are reliably available on PredictionLog; in-cycle we only have best_candidate_score.
        # We will fill precise scores for created signals from DB later.
        out.append(
            StrategyMatchRow(
                event_id=eid,
                match_name=str(r.get("match_name") or ""),
                tournament_name=str(r.get("tournament_name") or "") or None,
                strategy_id=sid,
                strategy_name=None,
                minute=_safe_int(r.get("minute")),
                score_home=None,
                score_away=None,
                bet_text=str(r.get("best_bet_text") or "") or None,
                market_family=str(r.get("best_market_family") or "") or None,
                odds=str(r.get("best_candidate_odds") or "") or None,
                base_signal_score=base_score,
                effective_live_score=eff_score,
                final_gate_decision=str(fg.get("final_gate_decision") or "") or None,
                full_pipeline_decision=str(fg.get("full_pipeline_decision") or "") or None,
                created_signal_id=created_signal_id,
                telegram_sent=telegram_sent if isinstance(telegram_sent, bool) else None,
                blocked_reason=str(fg.get("blocked_reason") or "") or None,
            )
        )
    return out


async def _load_created_signals(sessionmaker, signal_ids: list[int]) -> list[Signal]:
    if not signal_ids:
        return []
    async with sessionmaker() as s:
        q = (
            select(Signal)
            .where(Signal.id.in_(signal_ids))
            .options(selectinload(Signal.prediction_logs), selectinload(Signal.settlement))
        )
        rows = (await s.execute(q)).scalars().all()
        return list(rows)


def _signal_strategy_fields(sig: Signal) -> dict[str, Any]:
    pls = sig.prediction_logs or []
    pl0: PredictionLog | None = pls[0] if pls else None
    snap = (pl0.feature_snapshot_json if pl0 else {}) or {}
    expl = (pl0.explanation_json if pl0 else {}) or {}

    strat_id = str(expl.get("football_live_strategy_id") or "").strip() or None
    strat_name = str(expl.get("football_live_strategy_name") or "").strip() or None
    strat_reasons = expl.get("football_live_strategy_reasons")
    reasons = [str(x) for x in strat_reasons if str(x).strip()] if isinstance(strat_reasons, list) else []

    fa = snap.get("football_analytics") if isinstance(snap.get("football_analytics"), dict) else {}
    minute = _safe_int(fa.get("minute"))
    sh = _safe_int(fa.get("score_home"))
    sa = _safe_int(fa.get("score_away"))

    la = snap.get("football_live_adaptive_learning") if isinstance(snap.get("football_live_adaptive_learning"), dict) else {}
    base_score = _safe_float(la.get("base_signal_score")) if la.get("enabled") else None
    eff_score = _safe_float(la.get("effective_live_score")) if la.get("enabled") else None
    if eff_score is None:
        eff_score = _safe_float(sig.signal_score)

    aud = snap.get("football_outcome_audit") if isinstance(snap.get("football_outcome_audit"), dict) else {}
    outcome_reason_code = str(aud.get("outcome_reason_code") or "").strip() or None

    return {
        "strategy_id": strat_id,
        "strategy_name": strat_name,
        "strategy_reasons": reasons[:8],
        "minute": minute,
        "score_home": sh,
        "score_away": sa,
        "base_signal_score": base_score,
        "effective_live_score": eff_score,
        "outcome_reason_code": outcome_reason_code,
    }


async def main() -> None:
    settings = get_settings()
    if settings.signal_chat_id is None:
        raise SystemExit("signal_chat_id is not configured (cannot verify Telegram send).")

    # Make sure session is active (combat path uses live session for idea dedup + counters).
    FootballLiveSessionService().start_session(persistent=True)

    engine = create_engine(settings.database_url, echo=False)
    sessionmaker = create_sessionmaker(engine)
    bot = Bot(token=settings.bot_token)
    started_at = _now_utc()
    try:
        res = await AutoSignalService().run_single_cycle(sessionmaker, bot=bot, dry_run=False)  # type: ignore
        cycle_debug = res.football_cycle_debug or {}

        print("=== COMBAT CYCLE SUMMARY ===")
        print("message:", res.message)
        print("created_signals_count:", res.created_signals_count)
        print("notifications_sent_count:", res.notifications_sent_count)
        print("")

        agg = (cycle_debug.get("football_pipeline_aggregate") or {}) if isinstance(cycle_debug, dict) else {}
        print("=== AGGREGATE ===")
        for k in (
            "total_live_matches_tracked",
            "after_scoring_pool",
            "matches_with_sendable_idea",
            "normal_sendable_matches",
            "soft_sendable_matches",
        ):
            print(f"{k}:", agg.get(k))

        lq = (cycle_debug.get("live_quality_summary") or {}) if isinstance(cycle_debug, dict) else {}
        lss = lq.get("live_send_stats") or {}
        if isinstance(lss, dict):
            print("strategy_stats:", lss.get("strategy_stats"))
            print("strategy_matches:", lss.get("strategy_matches"))
            print("after_final_gate_matches_with_send:", (cycle_debug.get("final_live_send_gate") or {}).get("matches_with_send"))
        print("")

        strategy_rows = _extract_strategy_rows(cycle_debug if isinstance(cycle_debug, dict) else {})
        s1 = sum(1 for r in strategy_rows if r.strategy_id.startswith("S1_"))
        s2 = sum(1 for r in strategy_rows if r.strategy_id.startswith("S2_"))
        print("=== STRATEGY MATCHES (PER MATCH) ===")
        print("strategy_matches_total:", len(strategy_rows), "S1:", s1, "S2:", s2)
        print("")

        # Show per strategy-match row (compact JSON lines).
        for r in strategy_rows[:30]:
            print(
                json.dumps(
                    {
                        "event_id": r.event_id,
                        "match": r.match_name,
                        "tournament": r.tournament_name,
                        "strategy_id": r.strategy_id,
                        "minute": r.minute,
                        "bet_text": r.bet_text,
                        "market_family": r.market_family,
                        "odds": r.odds,
                        "final_gate": r.final_gate_decision,
                        "full_pipeline": r.full_pipeline_decision,
                        "created_signal_id": r.created_signal_id,
                        "telegram_sent": r.telegram_sent,
                        "blocked_reason": r.blocked_reason,
                    },
                    ensure_ascii=False,
                    default=str,
                )
            )
        if len(strategy_rows) > 30:
            print(f"... truncated: {len(strategy_rows) - 30} more")
        print("")

        # Load created signals from DB and print 3–5 examples.
        created_ids = [int(x) for x in (res.created_signal_ids or []) if str(x).isdigit()]
        created_signals = await _load_created_signals(sessionmaker, created_ids)
        created_strategy_signals = []
        for sig in created_signals:
            if sig.sport != SportType.FOOTBALL or not bool(sig.is_live):
                continue
            f = _signal_strategy_fields(sig)
            if f.get("strategy_id"):
                created_strategy_signals.append((sig, f))

        print("=== CREATED STRATEGY-BASED SIGNALS (DB) ===")
        print("created_strategy_based_signals:", len(created_strategy_signals))
        for sig, f in created_strategy_signals[:5]:
            print(
                json.dumps(
                    {
                        "signal_id": sig.id,
                        "event_id": sig.event_external_id,
                        "match": sig.match_name,
                        "tournament": sig.tournament_name,
                        "bet": f"{sig.market_label} | {sig.selection}",
                        "odds": str(sig.odds_at_signal),
                        "minute": f.get("minute"),
                        "score_home": f.get("score_home"),
                        "score_away": f.get("score_away"),
                        "send_path": (
                            ((sig.prediction_logs[0].explanation_json or {}).get("football_live_signal_rationale") or {}).get(
                                "send_path"
                            )
                            if sig.prediction_logs
                            else None
                        ),
                        "strategy_id": f.get("strategy_id"),
                        "strategy_name": f.get("strategy_name"),
                        "strategy_reasons": f.get("strategy_reasons"),
                        "base_signal_score": f.get("base_signal_score"),
                        "effective_live_score": f.get("effective_live_score"),
                    },
                    ensure_ascii=False,
                    default=str,
                )
            )
        print("")

        # Settlement coverage (short window; may be too early).
        horizon = started_at - timedelta(hours=6)
        async with sessionmaker() as s:
            q = (
                select(Signal)
                .where(Signal.sport == SportType.FOOTBALL)
                .where(Signal.is_live.is_(True))
                .where(Signal.signaled_at >= horizon)
                .options(selectinload(Signal.prediction_logs), selectinload(Signal.settlement))
                .order_by(Signal.id.desc())
                .limit(250)
            )
            sigs = (await s.execute(q)).scalars().all()

        strat_sigs = []
        for sig in sigs:
            if not sig.prediction_logs:
                continue
            expl = sig.prediction_logs[0].explanation_json or {}
            sid = str(expl.get("football_live_strategy_id") or "").strip()
            if sid:
                strat_sigs.append(sig)

        settled = [s for s in strat_sigs if s.settlement is not None and s.settlement.settled_at is not None]
        win = [s for s in settled if s.settlement and s.settlement.result == BetResult.WIN]
        lose = [s for s in settled if s.settlement and s.settlement.result == BetResult.LOSE]
        void = [s for s in settled if s.settlement and s.settlement.result == BetResult.VOID]

        with_outcome_code = 0
        for sig in settled:
            f = _signal_strategy_fields(sig)
            if f.get("outcome_reason_code"):
                with_outcome_code += 1

        print("=== SETTLEMENT COVERAGE (strategy-based, last ~6h sample) ===")
        print("strategy_based_signals_total_sampled:", len(strat_sigs))
        print("settled_count:", len(settled), "WIN:", len(win), "LOSE:", len(lose), "VOID:", len(void))
        print("with_outcome_reason_code:", with_outcome_code)
    finally:
        await bot.session.close()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())

