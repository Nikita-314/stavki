from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.core.enums import SportType
from app.schemas.provider_models import ProviderSignalCandidate
from app.services.football_bet_formatter_service import FootballBetFormatterService
from app.services.football_signal_send_filter_service import FootballSignalSendFilterService


@dataclass(frozen=True)
class FootballLiveStrategyDecision:
    passed: bool
    strategy_id: str | None = None
    strategy_name: str | None = None
    reasons: list[str] | None = None


def _lc_from_candidate(c: ProviderSignalCandidate) -> dict[str, Any]:
    ex = c.explanation_json or {}
    rat = ex.get("football_live_signal_rationale")
    if isinstance(rat, dict) and isinstance(rat.get("live_context"), dict):
        return dict(rat.get("live_context") or {})
    fs = c.feature_snapshot_json or {}
    fa = fs.get("football_analytics")
    if isinstance(fa, dict):
        # Normal path in our pipeline after analytics snapshot.
        return {
            "minute": fa.get("minute"),
            "score_home": fa.get("score_home"),
            "score_away": fa.get("score_away"),
            "period": fa.get("period"),
            "live_state": fa.get("live_state"),
        }
    return {}


def _selection_context_from_candidate(c: ProviderSignalCandidate) -> dict[str, Any]:
    ex = c.explanation_json or {}
    rat = ex.get("football_live_signal_rationale")
    if isinstance(rat, dict) and isinstance(rat.get("selection_context"), dict):
        return dict(rat.get("selection_context") or {})
    return {}


def _as_int(v: object) -> int | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _as_float(v: object) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _odds_float(c: ProviderSignalCandidate) -> float | None:
    try:
        return float(c.market.odds_value) if c.market.odds_value is not None else None
    except Exception:
        return None


def _norm(s: str) -> str:
    return (s or "").strip().lower().replace("ё", "е")


def _is_over_selection(sel: str) -> bool:
    s = _norm(sel)
    return "over" in s or "больше" in s or s.startswith("тб")


def _is_draw_selection(sel: str) -> bool:
    s = _norm(sel)
    return s in {"x", "draw", "ничья", "ничья ", "ничья."} or s == "х" or "нич" in s


def _selection_side_1x2(c: ProviderSignalCandidate) -> str | None:
    """Normalize 1X2 selection to one of: home|away|draw|unknown."""
    sel = _norm(str(c.market.selection or ""))
    if not sel:
        return None
    if _is_draw_selection(sel):
        return "draw"
    # Common provider tokens
    if sel in {"1", "1.0", "home", "п1"}:
        return "home"
    if sel in {"2", "2.0", "away", "п2"}:
        return "away"
    # Some feeds store team name as selection
    home = _norm(c.match.home_team)
    away = _norm(c.match.away_team)
    if sel and home and sel == home:
        return "home"
    if sel and away and sel == away:
        return "away"
    return None


def _min_goals_strict_over(line: float) -> int:
    # strict over: 2.5 => need 3 total goals
    return int(line + 0.5)


def evaluate_s1_live_1x2_controlled(c: ProviderSignalCandidate) -> FootballLiveStrategyDecision:
    lc = _lc_from_candidate(c)
    minute = _as_int(lc.get("minute"))
    sh = _as_int(lc.get("score_home"))
    sa = _as_int(lc.get("score_away"))
    odds = _odds_float(c)

    reasons: list[str] = []
    fam = FootballSignalSendFilterService().get_market_family(c)
    if fam != "result":
        reasons.append("market_not_result")
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)
    if minute is None or sh is None or sa is None:
        reasons.append("missing_live_context(minute/score)")
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)
    if odds is None:
        reasons.append("missing_odds")
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)

    # Calibrated bounds (data-driven): keep it strict, but not empty in real cycles.
    if not (3 <= minute <= 83):
        reasons.append("minute_window")
    if abs((sh - sa)) > 1:
        reasons.append("goal_diff")
    if (sh + sa) > 3:
        reasons.append("total_goals_cap")
    if not (1.35 <= odds <= 8.00):
        reasons.append("odds_window")
    side = _selection_side_1x2(c)
    if side is None:
        reasons.append("selection_mapping")
    if side == "draw" and sh != sa:
        reasons.append("draw_rule")

    if reasons:
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)
    return FootballLiveStrategyDecision(
        passed=True,
        strategy_id="S1_LIVE_1X2_CONTROLLED",
        strategy_name="Strategy 1: LIVE 1X2 (controlled state)",
        reasons=[
            "market=result(1X2)",
            "minute 3..83",
            "goal_diff<=1 and total_goals<=3",
            "odds 1.35..8.00",
        ],
    )


def evaluate_s2_live_total_over_need_1_2(c: ProviderSignalCandidate) -> FootballLiveStrategyDecision:
    lc = _lc_from_candidate(c)
    minute = _as_int(lc.get("minute"))
    sh = _as_int(lc.get("score_home"))
    sa = _as_int(lc.get("score_away"))
    odds = _odds_float(c)

    reasons2: list[str] = []
    fam_svc = FootballSignalSendFilterService()
    fam = fam_svc.get_market_family(c)
    if fam != "totals":
        reasons2.append("market_not_totals")
        return FootballLiveStrategyDecision(passed=False, reasons=reasons2)
    if minute is None or sh is None or sa is None:
        reasons2.append("missing_live_context(minute/score)")
        return FootballLiveStrategyDecision(passed=False, reasons=reasons2)
    if odds is None:
        reasons2.append("missing_odds")
        return FootballLiveStrategyDecision(passed=False, reasons=reasons2)

    if not (15 <= minute <= 75):
        reasons2.append("minute_window")
    if not (1.40 <= odds <= 2.60):
        reasons2.append("odds_window")
    if not _is_over_selection(str(c.market.selection or "")):
        reasons2.append("not_pure_over")

    # Derive strict-over remaining goals from the market itself (do not depend on presend rationale).
    fmt = FootballBetFormatterService()
    ctx = fmt.describe_total_context(
        market_type=c.market.market_type,
        market_label=c.market.market_label,
        selection=c.market.selection,
        home_team=c.match.home_team,
        away_team=c.match.away_team,
        section_name=c.market.section_name,
        subsection_name=c.market.subsection_name,
    )
    line_f: float | None = None
    target_scope: str | None = None
    if ctx and ctx.total_line:
        try:
            line_f = float(str(ctx.total_line).replace(",", "."))
        except ValueError:
            line_f = None
        target_scope = ctx.target_scope
    if line_f is None:
        reasons2.append("cannot_parse_total_line")
        return FootballLiveStrategyDecision(passed=False, reasons=reasons2)
    if target_scope and "match" not in _norm(target_scope) and "общ" not in _norm(target_scope):
        reasons2.append("not_match_total_scope")

    goals_now = sh + sa
    need_goals = max(0, _min_goals_strict_over(line_f) - int(goals_now))
    if need_goals not in (1, 2):
        if need_goals == 3:
            reasons2.append("need_more_eq_3")
        else:
            reasons2.append("need_more_other")

    if reasons2:
        return FootballLiveStrategyDecision(passed=False, reasons=reasons2)
    return FootballLiveStrategyDecision(
        passed=True,
        strategy_id="S2_LIVE_TOTAL_OVER_NEED_1_2",
        strategy_name="Strategy 2: LIVE Match Total OVER (need 1–2 goals)",
        reasons=[
            "market=totals(match)",
            "minute 15..75",
            "need_goals in {1,2} (strict-over remaining goals)",
            "odds 1.40..2.60",
        ],
    )


def evaluate_football_live_strategies(c: ProviderSignalCandidate) -> FootballLiveStrategyDecision:
    """Pick one explicit strategy for a candidate (or reject).

    IMPORTANT: This is the *primary* live signal definition. Existing filters/scoring remain guards,
    but a candidate must first match Strategy 1 or Strategy 2 to be considered for sending.
    """
    if c.match.sport != SportType.FOOTBALL or not bool(getattr(c.match, "is_live", False)):
        return FootballLiveStrategyDecision(passed=False, reasons=["not_football_live"])

    # Evaluate both strategies; pick the first that passes (S1 has priority on result markets).
    d1 = evaluate_s1_live_1x2_controlled(c)
    if d1.passed:
        return d1
    d2 = evaluate_s2_live_total_over_need_1_2(c)
    if d2.passed:
        return d2
    # If neither passed, return a compact union of reasons for debugging.
    rr = []
    rr.extend(list(d1.reasons or [])[:6])
    rr.extend(list(d2.reasons or [])[:6])
    if not rr:
        rr = ["no_strategy_match"]
    return FootballLiveStrategyDecision(passed=False, reasons=rr)

