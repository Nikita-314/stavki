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


def _is_exotic_result_market(c: ProviderSignalCandidate) -> bool:
    """Reject remainder/handicap/euro-handicap markets that can masquerade as result."""
    txt = " ".join(
        [
            str(c.market.section_name or ""),
            str(c.market.subsection_name or ""),
            str(c.market.market_label or ""),
            str(c.market.market_type or ""),
        ]
    ).lower()
    bad = [
        "remainder",
        "remain",
        "остат",
        "оставш",
        "handicap",
        "european",
        "европ",
        "фора",
        "hcp",
    ]
    return any(b in txt for b in bad)


def _result_subtype(c: ProviderSignalCandidate) -> str:
    """Classify result-like markets into subtypes; used to keep FT 1X2 clean."""
    mt = _norm(str(c.market.market_type or ""))
    txt = " ".join(
        [
            str(c.market.section_name or ""),
            str(c.market.subsection_name or ""),
            str(c.market.market_label or ""),
            str(c.market.selection or ""),
        ]
    )
    t = _norm(txt)

    if "следующий гол" in t or "next goal" in t:
        return "next_goal"
    if "остат" in t or "remainder" in t or "win the rest" in t or "выиграет остаток" in t:
        return "remainder"
    if "европ" in t or "european" in t or "фора" in t or "handicap" in t or "hcp" in t:
        return "european_handicap_or_handicap"
    if "интервал" in t or "с минут" in t or "interval" in t:
        return "interval_result"
    if "1-й тайм" in t or "2-й тайм" in t or "тайм" in t or "half" in t:
        return "period_result"
    if _is_exotic_result_market(c):
        return "exotic_result_like"

    if mt in {"1x2", "match_winner"}:
        # Treat as FT only if it doesn't look like a period/interval/remainder market.
        return "ft_1x2_candidate"
    return "other_result_like"


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
    # Disabled: we are switching to baseline+live confirmation mode (Sportmonks baseline + Winline live).
    reasons.append("disabled_pressure_strategy")

    if reasons:
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)
    return FootballLiveStrategyDecision(passed=False, reasons=reasons)


async def evaluate_s7_live_1x2_baseline_plus_winline_state(
    c: ProviderSignalCandidate,
) -> FootballLiveStrategyDecision:
    """Primary: 1X2 allowed only when Sportmonks baseline advantage is strong + Winline live state is controlled."""
    lc = _lc_from_candidate(c)
    minute = _as_int(lc.get("minute"))
    sh = _as_int(lc.get("score_home"))
    sa = _as_int(lc.get("score_away"))
    odds = _odds_float(c)

    reasons: list[str] = []
    fam_svc = FootballSignalSendFilterService()
    fam = fam_svc.get_market_family(c)
    if fam != "result":
        reasons.append("market_not_result")
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)
    mt = _norm(str(c.market.market_type or ""))
    if mt not in {"1x2", "match_winner"}:
        reasons.append("market_type_not_1x2")
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)
    if minute is None or sh is None or sa is None:
        reasons.append("missing_live_context(minute/score)")
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)
    if odds is None:
        reasons.append("missing_odds")
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)

    # A) Live state (Winline)
    if not (20 <= minute <= 65):
        reasons.append("minute_window_20_65")
    if not ((sh == 0 and sa == 0) or (sh == 1 and sa == 0) or (sh == 0 and sa == 1)):
        reasons.append("score_state_not_00_or_10")
    if not (1.70 <= float(odds) <= 3.80):
        reasons.append("odds_window_1_70_3_80")

    # Baseline
    side = _selection_side_1x2(c)
    if side not in {"home", "away"}:
        reasons.append("selection_not_team_side")
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)

    bsvc = FootballSportmonksBaselineService()
    bh = bsvc.build_team_baseline(c.match.home_team)
    ba = bsvc.build_team_baseline(c.match.away_team)
    if bh.score is None or ba.score is None:
        reasons.append("baseline_missing_or_insufficient")
    gap = None
    if bh.score is not None and ba.score is not None:
        gap = float(bh.score) - float(ba.score)
        # threshold: require clear advantage
        thr = 0.22
        if side == "home":
            if gap < thr:
                reasons.append(f"baseline_gap_below_threshold gap={gap:.3f} thr={thr}")
        else:
            if (-gap) < thr:
                reasons.append(f"baseline_gap_below_threshold gap={gap:.3f} thr={thr}")

    # Live restriction: do not take the advantaged team if it's already trailing 0:1 in this window.
    if side == "home" and sa == 1 and sh == 0:
        reasons.append("baseline_conflicts_with_trailing_state")
    if side == "away" and sh == 1 and sa == 0:
        reasons.append("baseline_conflicts_with_trailing_state")

    # Attach baseline into explanation_json for rationale/inspection.
    try:
        ex = dict(c.explanation_json or {})
        ex["sportmonks_baseline_home"] = bh.factors
        ex["sportmonks_baseline_away"] = ba.factors
        ex["sportmonks_baseline_home_score"] = bh.score
        ex["sportmonks_baseline_away_score"] = ba.score
        ex["sportmonks_baseline_gap_home_minus_away"] = gap
        c.explanation_json = ex  # type: ignore[attr-defined]
    except Exception:
        pass

    if reasons:
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)
    return FootballLiveStrategyDecision(
        passed=True,
        strategy_id="S7_LIVE_1X2_BASELINE_PLUS_WINLINE_STATE",
        strategy_name="Strategy 7: LIVE 1X2 (Sportmonks baseline + Winline state)",
        reasons=[
            "market=result(1X2)",
            "minute 20..65",
            "score in {0:0,1:0,0:1}",
            "odds 1.70..3.80",
            "baseline_gap above threshold",
            "no trailing-against-baseline",
        ],
    )


async def evaluate_s8_live_1x2_winline_strict(c: ProviderSignalCandidate) -> FootballLiveStrategyDecision:
    """Winline-only strict controlled 1X2 strategy (anti-template)."""
    lc = _lc_from_candidate(c)
    minute = _as_int(lc.get("minute"))
    sh = _as_int(lc.get("score_home"))
    sa = _as_int(lc.get("score_away"))
    odds = _odds_float(c)

    reasons: list[str] = []
    fam_svc = FootballSignalSendFilterService()
    fam = fam_svc.get_market_family(c)
    if fam != "result":
        reasons.append("market_not_result")
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)
    mt = _norm(str(c.market.market_type or ""))
    if mt not in {"1x2", "match_winner"}:
        reasons.append("market_type_not_1x2")
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)
    st = _result_subtype(c)
    if st != "ft_1x2_candidate":
        reasons.append(f"result_subtype_not_ft_1x2:{st}")
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)
    if _is_exotic_result_market(c):
        reasons.append("market_exotic_result_like")
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)
    if minute is None or sh is None or sa is None:
        reasons.append("missing_live_context(minute/score)")
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)
    if odds is None:
        reasons.append("missing_odds")
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)

    side = _selection_side_1x2(c)
    if side not in {"home", "away", "draw"}:
        reasons.append("selection_not_1x2_side")
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)

    # Base windows
    if minute < 15 or minute > 60:
        reasons.append("minute_window_15_60")
    if not ((sh == 0 and sa == 0) or (sh == 1 and sa == 0) or (sh == 0 and sa == 1)):
        reasons.append("score_state_not_00_or_10")
    # Odds window: keep strict, but allow a narrow high-odds slice for early 0:0 1X2 (avoid empty flow).
    if not (1.70 <= float(odds) <= 4.20):
        reasons.append("odds_window_1_70_4_20")
    if float(odds) > 3.40:
        # Only allow high odds in a controlled early 0:0 state, and only on a team side (no draw).
        if not (sh == 0 and sa == 0 and 20 <= minute <= 35 and side in {"home", "away"} and mt == "1x2"):
            reasons.append("odds_high_outside_narrow_00_window")

    # Anti-mess rules
    # 1) Disallow draw late
    if side == "draw" and minute >= 45:
        reasons.append("draw_too_late")

    # 2) Disallow taking a trailing side except a very narrow rescue case
    trailing = (side == "home" and sh < sa) or (side == "away" and sa < sh)
    if trailing:
        # Narrow allow: trailing by exactly 1 at <=30min and odds still <=2.00 (market still believes)
        allow = False
        if abs((sh or 0) - (sa or 0)) == 1 and minute <= 30 and float(odds) <= 2.00:
            allow = True
        if not allow:
            reasons.append("side_trailing_reject")

    # 3) Extra strict early 0:0 (avoid weak templated picks)
    if sh == 0 and sa == 0 and minute <= 20:
        if side == "draw":
            reasons.append("early_00_draw_reject")
        if float(odds) > 2.40:
            reasons.append("early_00_odds_too_high")

    if reasons:
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)
    return FootballLiveStrategyDecision(
        passed=True,
        strategy_id="S8_LIVE_1X2_WINLINE_STRICT",
        strategy_name="Strategy 8: LIVE 1X2 (Winline strict controlled)",
        reasons=[
            "market=result(1X2) only (non-exotic)",
            "minute 15..60",
            "score in {0:0,1:0,0:1}",
            "odds 1.70..4.20 (high odds only in narrow 0:0 window)",
            "no trailing side (except narrow rescue)",
            "no late draw",
            "extra strict early 0:0",
        ],
    )


async def evaluate_s9_live_totals_over_controlled(c: ProviderSignalCandidate) -> FootballLiveStrategyDecision:
    """Secondary: LIVE match total OVER with strict totals controls (Winline-only)."""
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

    mt = _norm(str(c.market.market_type or ""))
    # Provider normalization: some feeds label match totals as totals/total while still being total_goals.
    if mt not in {"total_goals", "totals", "total"}:
        reasons.append("market_type_not_total_goals")
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)

    if minute is None or sh is None or sa is None:
        reasons.append("missing_live_context(minute/score)")
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)
    if odds is None:
        reasons.append("missing_odds")
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)

    # Minute window
    if minute < 20 or minute > 70:
        reasons.append("minute_window_20_70")

    # Score window
    allowed_scores = {(0, 0), (1, 0), (0, 1), (1, 1)}
    if (sh, sa) not in allowed_scores:
        reasons.append("score_state_not_allowed")
    if (sh + sa) >= 3:
        reasons.append("goals_3plus_block")

    # Odds window
    if not (1.50 <= float(odds) <= 2.20):
        reasons.append("odds_window_1_50_2_20")

    # Only OVER, match total only, line allowlist
    if not _is_over_selection(str(c.market.selection or "")):
        reasons.append("not_pure_over")
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)

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
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)

    if line_f not in (1.5, 2.0, 2.5):
        reasons.append("total_line_not_1_5_2_0_2_5")

    # Block team totals / weird scopes
    if target_scope:
        ts = _norm(target_scope)
        if "match" not in ts and "общ" not in ts and "total" not in ts:
            reasons.append("not_match_total_scope")

    if reasons:
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)
    return FootballLiveStrategyDecision(
        passed=True,
        strategy_id="S9_LIVE_TOTALS_OVER_CONTROLLED",
        strategy_name="Strategy 9: LIVE TOTALS OVER (controlled)",
        reasons=[
            "market=totals(total_goals) over",
            "line in {1.5,2.0,2.5} (match total only)",
            "minute 20..70",
            "score in {0:0,1:0,0:1,1:1}",
            "odds 1.50..2.20",
            "block goals>=3",
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

    d8 = await evaluate_s8_live_1x2_winline_strict(c)
    if d8.passed:
        return d8
    d9 = await evaluate_s9_live_totals_over_controlled(c)
    if d9.passed:
        return d9
    rr: list[str] = []
    rr.extend(list(getattr(d8, "reasons", None) or [])[:12])
    rr.extend(list(getattr(d9, "reasons", None) or [])[:12])
    if not rr:
        rr = ["no_strategy_match"]
    return FootballLiveStrategyDecision(passed=False, reasons=rr)

