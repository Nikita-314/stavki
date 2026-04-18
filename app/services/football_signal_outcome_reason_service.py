from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.enums import BetResult, SportType
from app.db.models.prediction_log import PredictionLog
from app.db.models.signal import Signal
from app.schemas.event_result import EventResultInput
from app.schemas.provider_models import ProviderMatch, ProviderOddsMarket, ProviderSignalCandidate
from app.services.football_bet_formatter_service import FootballBetFormatterService
from app.services.football_signal_send_filter_service import FootballSignalSendFilterService
from app.services.signal_runtime_diagnostics_service import SignalRuntimeDiagnosticsService

logger = logging.getLogger(__name__)

# Documented for future reject analytics (usually no Signal row for these)
REJECTED_BEFORE_SEND_CODES: frozenset[str] = frozenset(
    {
        "blocked_invalid_live_market_text",
        "blocked_impossible_live_outcome",
        "blocked_low_live_plausibility",
        "blocked_send_filter",
        "blocked_integrity",
        "blocked_duplicate_idea",
        "blocked_db_dedup",
    }
)


@dataclass
class FootballOutcomeReasonResult:
    outcome_reason_code: str
    outcome_reason_text_ru: str
    settlement_bet_result: str
    final_scoreline: str | None
    final_match_winner: str | None
    analysis_confidence: str
    learning_tags: dict[str, Any] = field(default_factory=dict)
    feature_patch: dict[str, Any] = field(default_factory=dict)
    explanation_patch: dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def make(
        code: str,
        text: str,
        br: BetResult,
        sc: str | None,
        win: str | None,
        conf: str,
        learning: dict[str, Any],
    ) -> "FootballOutcomeReasonResult":
        audit = {
            "outcome_reason_code": code,
            "outcome_reason_text_ru": text,
            "settlement_bet_result": br.value,
            "final_scoreline": sc,
            "final_match_winner": win,
            "analysis_confidence": conf,
            "learning_tags": learning,
            "computed_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        return FootballOutcomeReasonResult(
            outcome_reason_code=code,
            outcome_reason_text_ru=text,
            settlement_bet_result=br.value,
            final_scoreline=sc,
            final_match_winner=win,
            analysis_confidence=conf,
            learning_tags=learning,
            feature_patch={"football_outcome_audit": audit},
            explanation_patch={"football_settlement": dict(audit)},
        )


def _int_score(v: Any) -> int | None:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)) and v >= 0 and not (isinstance(v, float) and (v != v or v < 0)):
        return int(v) if v >= 0 else None
    s = (str(v) or "").strip()
    if s.isdigit():
        return int(s)
    return None


def _parse_score_from_payload(payload: dict[str, Any] | None) -> tuple[int | None, int | None, str | None]:
    if not payload:
        return None, None, None
    L = {str(k).lower(): v for k, v in payload.items()}
    h = _int_score(
        L.get("home_score") or L.get("score_home") or L.get("goals_home") or L.get("homescore")
    )
    a = _int_score(
        L.get("away_score") or L.get("score_away") or L.get("goals_away") or L.get("awayscore")
    )
    for raw in (payload.get("final_scoreline") or payload.get("scoreline") or "",):
        if not raw or not isinstance(raw, str):
            continue
        m = re.search(r"(\d+)\s*[:–-]\s*(\d+)", raw)
        if m and h is None and a is None:
            h, a = int(m.group(1)), int(m.group(2))
    line: str | None
    if h is not None and a is not None:
        line = f"{h}:{a}"
    else:
        line = None
    return h, a, line


def _btts_both_scored(h: int | None, a: int | None) -> bool:
    if h is None or a is None:
        return False
    return h > 0 and a > 0


def _ru_unknown() -> tuple[str, str]:
    return "unknown_settlement_reason", "Недостаточно данных в payload/снимке для точного объяснения"


def _candidate_for_family(signal: Signal, snap0: dict, ex0: dict) -> ProviderSignalCandidate:
    match = ProviderMatch(
        external_event_id=str(signal.event_external_id or ""),
        sport=signal.sport,
        tournament_name=signal.tournament_name,
        match_name=signal.match_name,
        home_team=signal.home_team,
        away_team=signal.away_team,
        event_start_at=signal.event_start_at,
        is_live=bool(signal.is_live),
        source_name="db",
    )
    mkt = ProviderOddsMarket(
        bookmaker=signal.bookmaker,
        market_type=signal.market_type,
        market_label=signal.market_label,
        selection=signal.selection,
        odds_value=signal.odds_at_signal,
        section_name=signal.section_name,
        subsection_name=signal.subsection_name,
    )
    return ProviderSignalCandidate(
        match=match,
        market=mkt,
        min_entry_odds=signal.min_entry_odds,
        predicted_prob=signal.predicted_prob,
        implied_prob=signal.implied_prob,
        edge=signal.edge,
        model_name=signal.model_name,
        model_version_name=signal.model_version_name,
        signal_score=signal.signal_score,
        feature_snapshot_json=snap0,
        explanation_json=ex0,
    )


def _learning_from_snap(snap0: dict, ex0: dict) -> dict[str, Any]:
    ssa = (snap0.get("football_send_audit") or {}) if isinstance(snap0.get("football_send_audit"), dict) else {}
    sp = str(
        ssa.get("send_path")
        or (ex0.get("football_live_send_path") if ex0.get("football_live_send_path") in ("soft", "normal") else None)
        or "unknown"
    )
    ls = ssa.get("live_sanity")
    if not isinstance(ls, dict) and isinstance(ex0.get("live_sanity"), dict):
        ls = ex0.get("live_sanity")
    pls = 100
    plev = "ok"
    if isinstance(ls, dict):
        pls = int(ls.get("plausibility_score", 100) or 100)
        plev = str(ls.get("plausibility") or "ok")
    return {
        "send_path": sp,
        "live_plausibility": plev,
        "live_plausibility_score": pls,
    }


def _is_corner_match_market(lab: str) -> bool:
    lo = (lab or "").lower()
    return "углов" in lo and ("1x2" in lo or "исход" in lo or "рынк" in lo or "outcome" in lo)


def classify_football_outcome(
    signal: Signal,
    bet_result: BetResult,
    *,
    event_input: EventResultInput | None,
    feature_snapshot0: dict[str, Any],
    explanation0: dict[str, Any],
) -> FootballOutcomeReasonResult:
    """Heuristic, conservative: if facts missing, return unknown_settlement_reason."""
    snap0 = feature_snapshot0
    ex0 = explanation0
    fam_svc = FootballSignalSendFilterService()
    cand = _candidate_for_family(signal, snap0, ex0)
    fam = fam_svc.get_market_family(cand)
    mtype = (signal.market_type or "").strip().lower()
    mlab = (signal.market_label or "").lower()
    sel0 = (signal.selection or "") or ""
    sels = sel0.lower()
    learn = {**_learning_from_snap(snap0, ex0), "market_family": fam, "is_live": bool(signal.is_live)}

    pl = (event_input.result_payload_json or {}) if event_input and event_input.result_payload_json else {}
    is_void = bool(
        (event_input and event_input.is_void) or (pl or {}).get("is_void")
    ) or bet_result == BetResult.VOID

    if is_void or bet_result == BetResult.VOID:
        c, t = "canceled_or_refunded", "Возврат / отмена (void) по сеттлу"
        if not (event_input and event_input.is_void) and (pl or {}).get("is_void") is not True and bet_result == BetResult.VOID:
            c, t = "void_match", "Сеттл void — детализация отмены в payload не дана"
        return FootballOutcomeReasonResult.make(c, t, BetResult.VOID, None, None, "partial" if (event_input or pl) else "insufficient", learn)

    h, a, scoreline = _parse_score_from_payload(pl)
    wsel = (event_input.winner_selection or "").strip() if event_input else (pl or {}).get("winner") or (pl or {}).get("winner_selection")
    wsel = (str(wsel) if wsel else "").strip() or None
    conf = "full" if h is not None and a is not None else "partial" if wsel or pl else "insufficient"
    u = _ru_unknown()

    # — BTTS
    if fam == "btts" or ("обе" in mlab and "забьют" in mlab):
        if h is not None and a is not None:
            both = _btts_both_scored(h, a)
            yes = "да" in sels or "yes" in sels
            if bet_result == BetResult.WIN and both and yes:
                return FootballOutcomeReasonResult.make("won_btts_yes", "Оба соперника забили — сигнал «да»", bet_result, scoreline, wsel, conf, learn)
            if bet_result == BetResult.WIN and (not both) and ("нет" in sels or "no" in sels or (not yes and not sels and "y" not in sels)):
                return FootballOutcomeReasonResult.make("won_btts_no", "«Обе не» — в итоге подтвердилось", bet_result, scoreline, wsel, conf, learn)
            if bet_result == BetResult.LOSE and both and (not yes or "нет" in sels or "no" in sels or "n" in sels):
                return FootballOutcomeReasonResult.make(
                    "lost_btts_no_both_scored", "Сигнал против «обе головы», а в итоге оба забили", bet_result, scoreline, wsel, conf, learn
                )
            if bet_result == BetResult.LOSE and (not both) and yes:
                return FootballOutcomeReasonResult.make("lost_btts_yes_one_side_blank", "Ждали голову от обоих, по факту не сработало", bet_result, scoreline, wsel, conf, learn)
        c0, t0 = u[0], u[1]
        if bet_result == BetResult.WIN:
            c0, t0 = "won_btts_yes" if "да" in sels or "yes" in sels else "won_btts_no", "Расчёт в плюс — детализация счёта в payload не дана"
        return FootballOutcomeReasonResult.make(
            c0, t0, bet_result, scoreline, wsel, "insufficient" if h is None else "partial", learn
        )

    # — Totals
    if fam == "totals" or mtype in ("total_goals",) or "тотал" in mlab or "тотал" in sels:
        fmt = FootballBetFormatterService()
        ctx = fmt.describe_total_context(
            market_type=signal.market_type,
            market_label=signal.market_label or "",
            selection=signal.selection or "",
            home_team=signal.home_team,
            away_team=signal.away_team,
            section_name=signal.section_name,
            subsection_name=signal.subsection_name,
        )
        if h is not None and a is not None and ctx and ctx.total_line:
            try:
                line = float((ctx.total_line or "0").replace(",", "."))
            except ValueError:
                line = 0.0
            gsum = h + a
            scope = (ctx.target_scope or "").lower()
            team_goals: int
            if "home" in scope and "away" not in scope:
                team_goals = h
            elif "away" in scope and "home" not in scope:
                team_goals = a
            else:
                team_goals = gsum
            sideu = (ctx.total_side or "").upper()
            over = "ТБ" in sideu or "больше" in sels or (ctx.total_side or "").lower() in ("o", "over", "тб")
            if bet_result == BetResult.WIN and over and team_goals > line + 1e-6:
                return FootballOutcomeReasonResult.make("won_total_over", f"Итог {team_goals} / сумма > {line} — over сыграл", bet_result, scoreline, wsel, conf, learn)
            if bet_result == BetResult.WIN and not over and team_goals < line - 1e-6:
                return FootballOutcomeReasonResult.make("won_total_under", f"Итог {team_goals} < {line} — under / меньше сыграло", bet_result, scoreline, wsel, conf, learn)
            if bet_result == BetResult.LOSE and over and team_goals <= line + 1e-6:
                return FootballOutcomeReasonResult.make(
                    "lost_total_over_not_enough_goals", f"Брали over {line}, в матче (по пулу) {team_goals} — мало", bet_result, scoreline, wsel, conf, learn
                )
            if bet_result == BetResult.LOSE and (not over) and team_goals > line + 1e-6:
                return FootballOutcomeReasonResult.make("lost_total_under_too_many_goals", f"Under {line}, в матче (по пулу) {team_goals} — перебор", bet_result, scoreline, wsel, conf, learn)
        c0, t0 = u[0], u[1]
        if bet_result == BetResult.WIN:
            c0, t0 = "won_other_valid_market", "Тотал/линия: расчёт плюс (точные голы/линия не в payload)"
        else:
            c0, t0 = "lost_other_valid_market", "Тотал/линия: минус — без полного снимка счёта/линии"
        return FootballOutcomeReasonResult.make(c0, t0, bet_result, scoreline, wsel, "insufficient" if h is None else conf, learn)

    # — Handicap / EHC
    if mtype in ("handicap",) or "гандик" in mlab or "фора" in mlab:
        c0, t0 = (
            ("won_handicap", "По сеттлу: фора/гандик сыграл")
            if bet_result == BetResult.WIN
            else ("lost_handicap", "По сеттлу: фора/гандик не в сторону сигнала")
        )
        return FootballOutcomeReasonResult.make(c0, t0, bet_result, scoreline, wsel, conf, learn)

    # — Next goal
    if ("след" in mlab and "гол" in mlab) or "next goal" in mlab or "след" in sels:
        c0, t0 = (
            ("won_next_goal", "Событие «след. гол/момент» отыграно")
            if bet_result == BetResult.WIN
            else ("lost_next_goal_wrong_side", "След. гол/событие — в минус")
        )
        return FootballOutcomeReasonResult.make(c0, t0, bet_result, scoreline, wsel, conf, learn)

    # — 1X2 / match result (incl. corners 1X2 on corners)
    if mtype in ("1x2", "match_winner") or mlab in ("match result", "исход 1x2", "1x2"):
        if (fam in {"corners"} or _is_corner_match_market(signal.market_label or "")) and mtype in ("1x2", "match_winner"):
            c0, t0 = (
                ("won_match_result", "Победитель/исход по рынку (углы) подтверждён")
                if bet_result == BetResult.WIN
                else ("lost_match_result_wrong_side", "Итоговый исход по рынку (углы) — не в сторону")
            )
            return FootballOutcomeReasonResult.make(c0, t0, bet_result, scoreline, wsel, conf, learn)
        ssa = snap0.get("football_send_audit")
        lsn: dict[str, Any] | None = None
        if isinstance(ssa, dict) and isinstance(ssa.get("live_sanity"), dict):
            lsn = ssa.get("live_sanity")
        if not isinstance(lsn, dict) and isinstance(ex0.get("live_sanity"), dict):
            lsn = ex0.get("live_sanity")
        lsd = lsn or {}
        weak = str(lsd.get("plausibility")) in ("weak",) or int(lsd.get("plausibility_score", 100) or 100) < 50
        if bet_result == BetResult.WIN:
            return FootballOutcomeReasonResult.make("won_match_result", "Победитель/исход матча совпал с сигналом", bet_result, scoreline, wsel, conf, learn)
        if bet_result == BetResult.LOSE:
            if weak and bool(signal.is_live):
                return FootballOutcomeReasonResult.make(
                    "lost_late_comeback_bet",
                    "Live на слабой plausibility; итог — не в сторону",
                    bet_result,
                    scoreline,
                    wsel,
                    conf,
                    learn,
                )
            return FootballOutcomeReasonResult.make(
                "lost_match_result_wrong_side",
                "Победитель/исход матча не в сторону",
                bet_result,
                scoreline,
                wsel,
                conf,
                learn,
            )
        return FootballOutcomeReasonResult.make(u[0], u[1], bet_result, scoreline, wsel, conf, learn)

    # generic
    if bet_result == BetResult.WIN:
        return FootballOutcomeReasonResult.make("won_other_valid_market", u[1], bet_result, scoreline, wsel, conf, learn)
    if bet_result == BetResult.LOSE:
        return FootballOutcomeReasonResult.make("lost_other_valid_market", u[1], bet_result, scoreline, wsel, conf, learn)
    return FootballOutcomeReasonResult.make(u[0], u[1], bet_result, scoreline, wsel, "insufficient", learn)


class FootballSignalOutcomeReasonService:
    async def apply_to_signal(
        self,
        session: AsyncSession,
        signal: Signal,
        bet_result: BetResult,
        event_input: EventResultInput | None,
    ) -> FootballOutcomeReasonResult | None:
        if signal.sport != SportType.FOOTBALL:
            return None
        await session.refresh(signal, attribute_names=["prediction_logs"])
        if not signal.prediction_logs:
            logger.info("[FOOTBALL][OUTCOME_REASON] signal_id=%s: no prediction_log, skip", signal.id)
            return None
        pl0 = min(signal.prediction_logs, key=lambda p: p.id)
        f0 = dict(pl0.feature_snapshot_json or {})
        e0 = dict(pl0.explanation_json or {})
        r = classify_football_outcome(
            signal,
            bet_result,
            event_input=event_input,
            feature_snapshot0=f0,
            explanation0=e0,
        )
        merged = dict(f0)
        oa = dict(merged.get("football_outcome_audit") or {})
        oa.update((r.feature_patch or {}).get("football_outcome_audit") or {})
        merged["football_outcome_audit"] = oa
        exn = dict(e0)
        es = dict(exn.get("football_settlement") or {})
        es.update((r.explanation_patch or {}).get("football_settlement") or {})
        exn["football_settlement"] = es
        pl0.feature_snapshot_json = merged
        pl0.explanation_json = exn
        session.add(pl0)
        try:
            await _refresh_football_postmatch_summary(session)
        except Exception:
            logger.exception("football postmatch summary update failed")
        return r

    @staticmethod
    async def for_signal_id(
        session: AsyncSession,
        signal_id: int,
        bet_result: BetResult,
        event_input: EventResultInput | None = None,
    ) -> FootballOutcomeReasonResult | None:
        stmt = (
            select(Signal)
            .where(Signal.id == int(signal_id))
            .options(selectinload(Signal.prediction_logs))
        )
        sig = (await session.execute(stmt)).scalar_one_or_none()
        if not sig or sig.sport != SportType.FOOTBALL:
            return None
        return await FootballSignalOutcomeReasonService().apply_to_signal(session, sig, bet_result, event_input)


async def _refresh_football_postmatch_summary(session: AsyncSession) -> None:
    from app.db.repositories.signal_repository import SignalRepository

    srepo = SignalRepository()
    lim = 20
    sigs = await srepo.list_latest_settled_football_with_logs(session, limit=lim)
    n_w = n_l = n_v = 0
    loss_by_code: dict[str, int] = {}
    for s in sigs:
        st = s.settlement
        if not st:
            continue
        b = st.result
        if b == BetResult.WIN:
            n_w += 1
        elif b == BetResult.LOSE:
            n_l += 1
            pl0 = min(s.prediction_logs, key=lambda p: p.id) if s.prediction_logs else None
            if pl0 and isinstance(pl0.feature_snapshot_json, dict):
                aud = (pl0.feature_snapshot_json or {}).get("football_outcome_audit")
                c = (aud or {}).get("outcome_reason_code")
                if c:
                    loss_by_code[str(c)] = int(loss_by_code.get(str(c), 0) or 0) + 1
        elif b == BetResult.VOID:
            n_v += 1
    top = sorted(loss_by_code.items(), key=lambda x: -x[1])[:5]
    blob = {
        "sample": len(sigs),
        "wins": n_w,
        "losses": n_l,
        "voids": n_v,
        "loss_by_reason": dict(top),
    }
    SignalRuntimeDiagnosticsService().update(
        football_postmatch_settled_count=len(sigs),
        football_postmatch_wins_last=n_w,
        football_postmatch_losses_last=n_l,
        football_postmatch_voids_last=n_v,
        football_postmatch_top_loss_reasons=" | ".join(f"{a}:{b}" for a, b in top) if top else None,
        football_postmatch_status_lines_json=json.dumps(blob, ensure_ascii=False)[:20000],
    )
