from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.enums import BetResult, SportType
from app.db.models.signal import Signal


@dataclass(frozen=True)
class StrategySignalRow:
    signal_id: int
    event_id: str | None
    match_name: str
    tournament_name: str
    signaled_at: datetime
    odds: Decimal
    settlement_result: BetResult | None
    settled_at: datetime | None
    strategy_id: str
    strategy_name: str | None
    minute: int | None
    score_home: int | None
    score_away: int | None
    outcome_reason_code: str | None
    bet_text: str


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _safe_int(v: object) -> int | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _odds_bucket(o: float) -> str:
    if o < 1.3:
        return "<1.3"
    if o < 2.0:
        return "1.3–2"
    if o < 3.0:
        return "2–3"
    if o < 5.0:
        return "3–5"
    return "5+"


def _minute_bucket(m: int) -> str:
    if m < 0:
        return "unknown"
    if m < 20:
        return "0–20"
    if m < 40:
        return "20–40"
    if m < 60:
        return "40–60"
    return "60+"


def _score_state(h: int | None, a: int | None) -> str:
    if h is None or a is None:
        return "—"
    return f"{h}:{a}"


class FootballLiveStrategyPerformanceService:
    """Analytics only: strategy -> signal -> settlement -> outcome_reason_code.

    IMPORTANT: This service must NOT affect signal logic.
    """

    async def load_strategy_rows(
        self,
        session: AsyncSession,
        *,
        lookback_hours: int = 24 * 7,
        limit: int = 2500,
    ) -> list[StrategySignalRow]:
        horizon = _now_utc() - timedelta(hours=max(1, int(lookback_hours)))

        q = (
            select(Signal)
            .where(Signal.sport == SportType.FOOTBALL)
            .where(Signal.is_live.is_(True))
            .where(Signal.notes == "live_auto")
            .where(Signal.signaled_at >= horizon)
            .options(selectinload(Signal.prediction_logs), selectinload(Signal.settlement))
            .order_by(Signal.id.desc())
            .limit(max(50, int(limit)))
        )
        sigs = (await session.execute(q)).scalars().all()

        out: list[StrategySignalRow] = []
        for s in sigs:
            pls = s.prediction_logs or []
            if not pls:
                continue
            pl0 = pls[0]
            expl = pl0.explanation_json or {}
            strat_id = str(expl.get("football_live_strategy_id") or "").strip()
            if not strat_id:
                continue
            strat_name = str(expl.get("football_live_strategy_name") or "").strip() or None

            snap = pl0.feature_snapshot_json or {}
            fa = snap.get("football_analytics") if isinstance(snap.get("football_analytics"), dict) else {}
            minute = _safe_int(fa.get("minute"))
            sh = _safe_int(fa.get("score_home"))
            sa = _safe_int(fa.get("score_away"))

            aud = snap.get("football_outcome_audit") if isinstance(snap.get("football_outcome_audit"), dict) else {}
            ocode = str(aud.get("outcome_reason_code") or "").strip() or None

            rat = expl.get("football_live_signal_rationale") if isinstance(expl.get("football_live_signal_rationale"), dict) else {}
            bet_text = str(rat.get("bet_text") or "").strip()
            if not bet_text:
                bet_text = f"{s.market_label} | {s.selection}"

            settlement = s.settlement
            settled_at = settlement.settled_at if settlement is not None else None
            result = settlement.result if settlement is not None else None

            out.append(
                StrategySignalRow(
                    signal_id=int(s.id),
                    event_id=str(s.event_external_id) if s.event_external_id else None,
                    match_name=str(s.match_name or ""),
                    tournament_name=str(s.tournament_name or ""),
                    signaled_at=s.signaled_at,
                    odds=s.odds_at_signal,
                    settlement_result=result,
                    settled_at=settled_at,
                    strategy_id=strat_id,
                    strategy_name=strat_name,
                    minute=minute,
                    score_home=sh,
                    score_away=sa,
                    outcome_reason_code=ocode,
                    bet_text=bet_text,
                )
            )
        return out

    def build_report(self, rows: list[StrategySignalRow]) -> dict[str, Any]:
        total = len(rows)
        settled = [r for r in rows if r.settled_at is not None and r.settlement_result is not None]
        win = [r for r in settled if r.settlement_result == BetResult.WIN]
        lose = [r for r in settled if r.settlement_result == BetResult.LOSE]
        void = [r for r in settled if r.settlement_result == BetResult.VOID]

        denom = len(win) + len(lose)
        win_rate = (len(win) / denom) if denom > 0 else None

        odds_vals = []
        for r in rows:
            try:
                odds_vals.append(float(r.odds))
            except Exception:
                continue
        avg_odds = (sum(odds_vals) / len(odds_vals)) if odds_vals else None

        odds_bucket_all = Counter()
        for r in rows:
            try:
                odds_bucket_all[_odds_bucket(float(r.odds))] += 1
            except Exception:
                odds_bucket_all["—"] += 1

        by_strategy: dict[str, list[StrategySignalRow]] = defaultdict(list)
        for r in rows:
            by_strategy[r.strategy_id].append(r)

        by_strategy_report: dict[str, Any] = {}
        for sid, rs in sorted(by_strategy.items(), key=lambda kv: (-len(kv[1]), kv[0])):
            rs_set = [x for x in rs if x.settled_at is not None and x.settlement_result is not None]
            rs_win = [x for x in rs_set if x.settlement_result == BetResult.WIN]
            rs_lose = [x for x in rs_set if x.settlement_result == BetResult.LOSE]
            rs_void = [x for x in rs_set if x.settlement_result == BetResult.VOID]
            denom2 = len(rs_win) + len(rs_lose)
            wr = (len(rs_win) / denom2) if denom2 > 0 else None
            odds2 = []
            for x in rs:
                try:
                    odds2.append(float(x.odds))
                except Exception:
                    continue
            by_strategy_report[sid] = {
                "strategy_name": (rs[0].strategy_name if rs else None),
                "total": len(rs),
                "settled": len(rs_set),
                "WIN": len(rs_win),
                "LOSE": len(rs_lose),
                "VOID": len(rs_void),
                "win_rate": wr,
                "avg_odds": (sum(odds2) / len(odds2)) if odds2 else None,
            }

        # S1 factor breakdowns
        s1_rows = by_strategy.get("S1_LIVE_1X2_CONTROLLED", [])
        minute_b = Counter()
        score_b = Counter()
        odds_b = Counter()
        for r in s1_rows:
            if r.minute is not None:
                minute_b[_minute_bucket(int(r.minute))] += 1
            else:
                minute_b["—"] += 1
            score_b[_score_state(r.score_home, r.score_away)] += 1
            try:
                odds_b[_odds_bucket(float(r.odds))] += 1
            except Exception:
                odds_b["—"] += 1

        latest_settled = sorted(settled, key=lambda r: (r.settled_at or _now_utc()), reverse=True)[:10]
        with_outcome_code = sum(1 for r in settled if r.outcome_reason_code)

        return {
            "total": total,
            "settled": len(settled),
            "WIN": len(win),
            "LOSE": len(lose),
            "VOID": len(void),
            "win_rate": win_rate,
            "avg_odds": avg_odds,
            "odds_buckets": dict(odds_bucket_all),
            "by_strategy": by_strategy_report,
            "s1_breakdown": {
                "minute_buckets": dict(minute_b),
                "score_states_top": dict(score_b.most_common(12)),
                "odds_buckets": dict(odds_b),
            },
            "latest_settled": [
                {
                    "signal_id": r.signal_id,
                    "match": r.match_name,
                    "strategy_id": r.strategy_id,
                    "strategy_name": r.strategy_name,
                    "bet": r.bet_text,
                    "odds": str(r.odds),
                    "minute": r.minute,
                    "score": _score_state(r.score_home, r.score_away),
                    "result": (r.settlement_result.value if r.settlement_result else None),
                    "outcome_reason_code": r.outcome_reason_code,
                    "settled_at": (r.settled_at.isoformat() if r.settled_at else None),
                }
                for r in latest_settled
            ],
            "with_outcome_reason_code": with_outcome_code,
        }

