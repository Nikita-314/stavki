from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.core.enums import SportType
from app.schemas.provider_models import ProviderSignalCandidate
from app.services.football_bet_formatter_service import FootballBetFormatterService
from app.services.football_live_pressure_service import FootballLivePressureService
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
    # Disabled: 1X2 is no longer the primary signal source (too noisy / templated).
    return FootballLiveStrategyDecision(passed=False, reasons=["disabled_1x2_primary"])


async def evaluate_s3_live_totals_over_pressure(
    c: ProviderSignalCandidate,
) -> FootballLiveStrategyDecision:
    """Primary strategy: LIVE match total OVER (1.5/2.0/2.5) with pressure stats."""
    lc = _lc_from_candidate(c)
    minute = _as_int(lc.get("minute"))
    sh = _as_int(lc.get("score_home"))
    sa = _as_int(lc.get("score_away"))
    odds = _odds_float(c)

    reasons: list[str] = []
    fam_svc = FootballSignalSendFilterService()
    fam = fam_svc.get_market_family(c)
    if fam != "totals":
        reasons.append("market_not_totals")
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)
    if minute is None or sh is None or sa is None:
        reasons.append("missing_live_context(minute/score)")
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)
    if odds is None:
        reasons.append("missing_odds")
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)

    # A) Minute window
    if minute < 15 or minute > 70:
        reasons.append("minute_window_15_70")

    # B) Score state: 0:0 or 1:0/0:1 only
    if not ((sh == 0 and sa == 0) or ((sh == 1 and sa == 0) or (sh == 0 and sa == 1))):
        reasons.append("score_state_not_00_or_10")
    if sh >= 2 or sa >= 2:
        reasons.append("score_has_2plus_goal")

    # D) Odds + selection must be pure over and in range
    if not (1.6 <= float(odds) <= 2.3):
        reasons.append("odds_window_1_6_2_3")
    if not _is_over_selection(str(c.market.selection or "")):
        reasons.append("not_pure_over")

    # Market line: allow only 1.5/2.0/2.5 (match total)
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
        reasons.append("cannot_parse_total_line")
    else:
        if target_scope and "match" not in _norm(target_scope) and "общ" not in _norm(target_scope):
            reasons.append("not_match_total_scope")
        if line_f not in (1.5, 2.0, 2.5):
            reasons.append("total_line_not_1_5_2_0_2_5")

    # C) Pressure stats: require at least 2 of 3
    pressure = await FootballLivePressureService().get_pressure(
        home_team=c.match.home_team,
        away_team=c.match.away_team,
    )
    if pressure is None:
        reasons.append("no_live_pressure_stats")
    else:
        shots_total = pressure.shots_total
        shots_on = pressure.shots_on_target
        corners = pressure.corners
        passed_pressure = 0
        if shots_total is not None and shots_total >= 6:
            passed_pressure += 1
        if shots_on is not None and shots_on >= 2:
            passed_pressure += 1
        if corners is not None and corners >= 3:
            passed_pressure += 1
        if passed_pressure < 2:
            reasons.append(
                f"pressure_low shots_total={shots_total} sot={shots_on} corners={corners}"
            )

        # Attach stats into explanation_json for rationale/inspection downstream.
        try:
            ex = dict(c.explanation_json or {})
            ex["football_live_pressure"] = {
                "source": pressure.source,
                "shots_total": shots_total,
                "shots_on_target": shots_on,
                "corners": corners,
                "fetched_at_epoch": pressure.fetched_at_epoch,
            }
            c.explanation_json = ex  # type: ignore[attr-defined]
        except Exception:
            pass

    if reasons:
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)
    return FootballLiveStrategyDecision(
        passed=True,
        strategy_id="S3_LIVE_TOTALS_OVER_PRESSURE",
        strategy_name="Strategy 3: LIVE Match Total OVER (pressure gate)",
        reasons=[
            "market=totals(match) over",
            "minute 15..70",
            "score in {0:0,1:0,0:1} and no side >=2",
            "total line in {1.5,2.0,2.5}",
            "odds 1.6..2.3",
            "pressure: >=2 of {shots>=6, SOT>=2, corners>=3}",
        ],
    )


def evaluate_s2_live_total_over_need_1_2(c: ProviderSignalCandidate) -> FootballLiveStrategyDecision:
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

    # Primary is LIVE totals-over with pressure.
    # S2 kept as fallback totals strategy (if pressure source fails or no stats available).
    # S1 (1X2) disabled above.
    # NOTE: evaluate_s3 is async (pressure fetch).
    # Callers must await evaluate_football_live_strategies_async instead.
    return FootballLiveStrategyDecision(passed=False, reasons=["strategy_dispatch_must_use_async"])


async def evaluate_football_live_strategies_async(c: ProviderSignalCandidate) -> FootballLiveStrategyDecision:
    if c.match.sport != SportType.FOOTBALL or not bool(getattr(c.match, "is_live", False)):
        return FootballLiveStrategyDecision(passed=False, reasons=["not_football_live"])

    d3 = await evaluate_s3_live_totals_over_pressure(c)
    if d3.passed:
        return d3
    # Strict mode: do not send without pressure stats; do not fall back to 1X2.
    rr: list[str] = []
    rr.extend(list(getattr(d3, "reasons", None) or [])[:12])
    if not rr:
        rr = ["no_strategy_match"]
    return FootballLiveStrategyDecision(passed=False, reasons=rr)

