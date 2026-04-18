"""Internal football LIVE signal rationale (machine-readable audit trail, not Telegram UX)."""

from __future__ import annotations

from collections import Counter
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.enums import BetResult, SportType
from app.db.models.settlement import Settlement
from app.db.models.signal import Signal
from app.schemas.provider_models import ProviderMatch, ProviderOddsMarket, ProviderSignalCandidate
from app.services.football_bet_formatter_service import FootballBetFormatterService
from app.services.football_signal_send_filter_service import FootballSignalSendFilterService

RATIONALE_SCHEMA_VERSION = 1

# Stable aggregation codes (snake_case, immutable once published)
CODE_SELECTED_CORE_MARKET = "selected_core_market"
CODE_SELECTED_AFTER_FINAL_LIVE_GATE = "selected_after_final_live_gate"
CODE_SELECTED_AFTER_LIVE_SANITY_OK = "selected_after_live_sanity_ok"
CODE_SELECTED_AFTER_TIMING_SANITY_OK = "selected_after_timing_sanity_ok"
CODE_SELECTED_BEST_SCORE_ON_MATCH = "selected_best_score_on_match"
CODE_SELECTED_SOFT_SEND_PATH = "selected_soft_send_path"
CODE_SELECTED_NORMAL_SEND_PATH = "selected_normal_send_path"
CODE_SELECTED_MAIN_OVER_EXOTIC_POLICY = "selected_main_market_over_exotic"
CODE_SELECTED_LIMITED_LIVE_CONTEXT = "selected_with_limited_live_context"
CODE_SELECTED_HIGH_PLAUSIBILITY = "selected_high_plausibility"
CODE_SELECTED_MEDIUM_PLAUSIBILITY = "selected_medium_plausibility"
CODE_SELECTED_LOW_PLAUSIBILITY = "selected_low_plausibility"


def _minute_from_snapshot(fs: dict[str, Any]) -> int | None:
    for key in ("minute", "match_minute", "time"):
        v = fs.get(key)
        if v is None:
            continue
        try:
            return int(v)
        except (TypeError, ValueError):
            continue
    fa = fs.get("football_analytics")
    if isinstance(fa, dict) and fa.get("minute") is not None:
        try:
            return int(fa.get("minute"))
        except (TypeError, ValueError):
            pass
    return None


def _min_goals_strict_over(line: float) -> int:
    return int(line + 0.5)


def build_football_live_signal_rationale(
    c: ProviderSignalCandidate,
    *,
    send_path: str,
    send_soft_label: str | None = None,
) -> dict[str, Any] | None:
    """Full pre-send rationale for football LIVE combat sends. Stored on prediction_log.explanation_json."""
    if not getattr(c.match, "is_live", False) or c.match.sport != SportType.FOOTBALL:
        return None

    fam_svc = FootballSignalSendFilterService()
    fmt = FootballBetFormatterService()
    fam = fam_svc.get_market_family(c)
    expl = dict(c.explanation_json or {})
    snap = dict(c.feature_snapshot_json or {})
    fa = snap.get("football_analytics") if isinstance(snap.get("football_analytics"), dict) else {}

    h = fa.get("score_home")
    a = fa.get("score_away")
    try:
        hi = int(h) if h is not None and not isinstance(h, bool) else None
    except (TypeError, ValueError):
        hi = None
    try:
        ai = int(a) if a is not None and not isinstance(a, bool) else None
    except (TypeError, ValueError):
        ai = None

    minute = _minute_from_snapshot(snap)
    period = fa.get("period") or fa.get("match_period") or snap.get("period")
    live_state = fa.get("live_state") or fa.get("status") or snap.get("live_state")

    ls = expl.get("live_sanity") if isinstance(expl.get("live_sanity"), dict) else {}
    passed_sanity = bool(ls.get("passed"))
    block_tok = str(ls.get("block_token") or "")
    final_gate = bool(ls.get("final_live_gate"))
    timing_ok = passed_sanity and block_tok in ("", "ok_live_sanity")

    pl_score_raw = ls.get("plausibility_score")
    try:
        pl_score = int(pl_score_raw) if pl_score_raw is not None else 100
    except (TypeError, ValueError):
        pl_score = 100
    pl_level = str(ls.get("plausibility") or "ok")

    limited_live = minute is None or hi is None or ai is None

    warnings: dict[str, bool] = {
        "late_stage_signal": bool(str(expl.get("football_live_late_stage_warning_ru") or "").strip()),
        "limited_live_context": limited_live,
        "missing_live_minute": minute is None,
        "missing_live_score": hi is None or ai is None,
        "weak_plausibility": pl_level == "weak" or pl_score < 50,
    }

    codes: list[str] = []
    if fam in ("result", "totals", "double_chance", "handicap", "btts"):
        codes.append(CODE_SELECTED_CORE_MARKET)
    codes.append(CODE_SELECTED_BEST_SCORE_ON_MATCH)
    if final_gate:
        codes.append(CODE_SELECTED_AFTER_FINAL_LIVE_GATE)
        codes.append(CODE_SELECTED_MAIN_OVER_EXOTIC_POLICY)
    if passed_sanity:
        codes.append(CODE_SELECTED_AFTER_LIVE_SANITY_OK)
    if timing_ok:
        codes.append(CODE_SELECTED_AFTER_TIMING_SANITY_OK)
    if str(send_path).lower() == "soft":
        codes.append(CODE_SELECTED_SOFT_SEND_PATH)
    else:
        codes.append(CODE_SELECTED_NORMAL_SEND_PATH)
    if limited_live:
        codes.append(CODE_SELECTED_LIMITED_LIVE_CONTEXT)
    if pl_score >= 80:
        codes.append(CODE_SELECTED_HIGH_PLAUSIBILITY)
    elif pl_score >= 50:
        codes.append(CODE_SELECTED_MEDIUM_PLAUSIBILITY)
    else:
        codes.append(CODE_SELECTED_LOW_PLAUSIBILITY)

    _pres = fmt.format_bet(
        market_type=c.market.market_type,
        market_label=c.market.market_label,
        selection=c.market.selection,
        home_team=c.match.home_team,
        away_team=c.match.away_team,
        section_name=c.market.section_name,
        subsection_name=c.market.subsection_name,
    )
    bet_text = _pres.main_label
    if _pres.detail_label:
        bet_text = f"{bet_text} ({_pres.detail_label})"

    sel_ctx: dict[str, Any] = {
        "what_needed_to_win": None,
        "line": None,
        "total_side": None,
        "target_scope": None,
    }
    if fam == "totals":
        ctx = fmt.describe_total_context(
            market_type=c.market.market_type,
            market_label=c.market.market_label,
            selection=c.market.selection,
            home_team=c.match.home_team,
            away_team=c.match.away_team,
            section_name=c.market.section_name,
            subsection_name=c.market.subsection_name,
        )
        if ctx and ctx.total_line:
            try:
                line_f = float(str(ctx.total_line).replace(",", "."))
            except ValueError:
                line_f = None
            else:
                sel_ctx["line"] = line_f
                sel_ctx["total_side"] = ctx.total_side
                sel_ctx["target_scope"] = ctx.target_scope
                scope_l = (ctx.target_scope or "").lower()
                goals = None
                if hi is not None and ai is not None:
                    if ("home" in scope_l or "it1" in scope_l) and "away" not in scope_l:
                        goals = hi
                    elif ("away" in scope_l or "it2" in scope_l) and "home" not in scope_l:
                        goals = ai
                    else:
                        goals = hi + ai
                side_u = (ctx.total_side or "").upper()
                over_like = side_u in ("ТБ", "O", "TB") or "БОЛЬШЕ" in (c.market.selection or "").upper()
                if line_f is not None and goals is not None and over_like:
                    need = max(0, _min_goals_strict_over(line_f) - int(goals))
                    sel_ctx["what_needed_to_win"] = f"match_goals_need_{need}_more_for_strict_over_{line_f}"
                elif line_f is not None and goals is not None and not over_like:
                    sel_ctx["what_needed_to_win"] = f"goals_must_stay_at_or_under_line_{line_f}_vs_current_{goals}"

    odds_val: str | float | None
    try:
        odds_val = float(c.market.odds_value) if c.market.odds_value is not None else None
    except Exception:
        odds_val = str(c.market.odds_value) if c.market.odds_value is not None else None

    summary_bits = sorted(set(codes))
    payload: dict[str, Any] = {
        "schema_version": RATIONALE_SCHEMA_VERSION,
        "kind": "football_live_combat_presend",
        "event_external_id": str(c.match.external_event_id or ""),
        "market_family": fam,
        "bet_text": bet_text[:500],
        "odds": odds_val,
        "signal_score": float(c.signal_score or 0.0),
        "send_path": str(send_path).lower(),
        "send_soft_label": send_soft_label,
        "is_main_market": bool(final_gate),
        "live_context": {
            "score_home": hi,
            "score_away": ai,
            "minute": minute,
            "period": period,
            "live_state": live_state,
        },
        "selection_context": sel_ctx,
        "why_selected_codes": summary_bits,
        "why_selected_summary": "|".join(summary_bits)[:900],
        "passed_filters": {
            "send_filter": True,
            "integrity": True,
            "final_live_gate": final_gate,
            "live_sanity": passed_sanity,
            "timing_sanity": timing_ok,
        },
        "plausibility_score": pl_score,
        "plausibility_level": pl_level,
        "live_sanity_block_token": block_tok or None,
        "warnings": warnings,
        "limited_live_context": limited_live,
        "scoring_reason_codes": list(expl.get("football_scoring_reason_codes") or []),
    }
    return payload


def slim_rationale_for_settlement(r: dict[str, Any]) -> dict[str, Any]:
    """Small copy merged into football_settlement for outcome rows."""
    return {
        "schema_version": r.get("schema_version"),
        "market_family": r.get("market_family"),
        "send_path": r.get("send_path"),
        "why_selected_codes": r.get("why_selected_codes"),
        "limited_live_context": r.get("limited_live_context"),
        "warnings": r.get("warnings"),
        "plausibility_score": r.get("plausibility_score"),
        "live_context": r.get("live_context"),
    }


async def aggregate_football_live_rationale_outcomes(
    session: AsyncSession,
    *,
    lookback: int = 400,
) -> dict[str, Any]:
    """WIN/LOSE aggregates: rationale codes, warnings, family×code on losses."""
    lim = max(30, min(2000, int(lookback)))
    stmt = (
        select(Signal, Settlement)
        .join(Settlement, Settlement.signal_id == Signal.id)
        .where(Signal.sport == SportType.FOOTBALL)
        .where(Settlement.result.in_([BetResult.WIN, BetResult.LOSE]))
        .options(selectinload(Signal.prediction_logs))
        .order_by(Settlement.id.desc())
        .limit(lim)
    )
    rows = list((await session.execute(stmt)).all())
    fam_svc = FootballSignalSendFilterService()

    win_codes: Counter[str] = Counter()
    lose_codes: Counter[str] = Counter()
    lose_warn: Counter[str] = Counter()
    lose_pair: Counter[tuple[str, str]] = Counter()
    late_lose = limited_lose = 0
    n_w = n_l = 0

    for signal, st in rows:
        if not signal.prediction_logs:
            continue
        pl0 = min(signal.prediction_logs, key=lambda p: p.id)
        ex0 = dict(pl0.explanation_json or {})
        rat = ex0.get("football_live_signal_rationale")
        if not isinstance(rat, dict):
            continue
        codes = [str(x) for x in (rat.get("why_selected_codes") or []) if x]
        warns = rat.get("warnings") if isinstance(rat.get("warnings"), dict) else {}
        cand = ProviderSignalCandidate(
            match=ProviderMatch(
                external_event_id=str(signal.event_external_id or ""),
                sport=SportType.FOOTBALL,
                tournament_name=signal.tournament_name,
                match_name=signal.match_name,
                home_team=signal.home_team,
                away_team=signal.away_team,
                event_start_at=signal.event_start_at,
                is_live=bool(signal.is_live),
                source_name="db",
            ),
            market=ProviderOddsMarket(
                bookmaker=signal.bookmaker,
                market_type=signal.market_type,
                market_label=signal.market_label,
                selection=signal.selection,
                odds_value=signal.odds_at_signal,
                section_name=signal.section_name,
                subsection_name=signal.subsection_name,
            ),
            feature_snapshot_json=dict(pl0.feature_snapshot_json or {}),
            explanation_json=ex0,
        )
        fam = fam_svc.get_market_family(cand)

        if st.result == BetResult.WIN:
            n_w += 1
            for c in codes:
                win_codes[c] += 1
        elif st.result == BetResult.LOSE:
            n_l += 1
            for c in codes:
                lose_codes[c] += 1
            for wk, wv in warns.items():
                if wv:
                    lose_warn[str(wk)] += 1
            if bool(warns.get("late_stage_signal")):
                late_lose += 1
            if bool(rat.get("limited_live_context")):
                limited_lose += 1
            primary = codes[0] if codes else "—"
            lose_pair[(str(fam), primary)] += 1

    def _top(ct: Counter[str], n: int = 12) -> list[dict[str, Any]]:
        return [{"code": k, "count": v} for k, v in ct.most_common(n)]

    return {
        "sample_limit": lim,
        "rows_used": len(rows),
        "wins_with_rationale": n_w,
        "losses_with_rationale": n_l,
        "why_codes_top_wins": _top(win_codes),
        "why_codes_top_losses": _top(lose_codes),
        "warnings_on_losses": _top(lose_warn, 8),
        "losses_late_stage_warning_hits": late_lose,
        "losses_limited_live_context_hits": limited_lose,
        "losses_by_market_family_primary_code": [
            {"market_family": a, "primary_code": b, "count": c}
            for (a, b), c in sorted(lose_pair.items(), key=lambda x: -x[1])[:15]
        ],
    }
