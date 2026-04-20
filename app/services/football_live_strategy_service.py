from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.core.enums import SportType
from app.schemas.provider_models import ProviderSignalCandidate


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


def evaluate_football_live_strategies(c: ProviderSignalCandidate) -> FootballLiveStrategyDecision:
    """Pick one explicit strategy for a candidate (or reject).

    IMPORTANT: This is the *primary* live signal definition. Existing filters/scoring remain guards,
    but a candidate must first match Strategy 1 or Strategy 2 to be considered for sending.
    """
    if c.match.sport != SportType.FOOTBALL or not bool(getattr(c.match, "is_live", False)):
        return FootballLiveStrategyDecision(passed=False, reasons=["not_football_live"])

    lc = _lc_from_candidate(c)
    minute = _as_int(lc.get("minute"))
    sh = _as_int(lc.get("score_home"))
    sa = _as_int(lc.get("score_away"))
    odds = _odds_float(c)

    if minute is None or sh is None or sa is None:
        return FootballLiveStrategyDecision(passed=False, reasons=["missing_live_context(minute/score)"])
    if odds is None:
        return FootballLiveStrategyDecision(passed=False, reasons=["missing_odds"])

    fam = ""
    rat0 = (c.explanation_json or {}).get("football_live_signal_rationale")
    if isinstance(rat0, dict):
        fam = str(rat0.get("market_family") or "").strip()
    if not fam:
        # fallback: best effort from market_type
        fam = "result" if _norm(c.market.market_type) in {"1x2", "match_winner"} else "totals" if "total" in _norm(c.market.market_type) else ""

    # ------------------------------------------------------------
    # Strategy 1: LIVE 1X2 (simple, transparent)
    # ------------------------------------------------------------
    # Goal: only match-result bets under controlled game state.
    # Rules:
    # - market family: result (1X2 / match winner)
    # - minute: 10..70
    # - abs(goal diff) <= 1
    # - total goals <= 3 (avoid crazy states)
    # - odds: 1.70..6.00
    # - if selection is Draw -> must be draw right now
    # - selection must be one of: home_team / away_team / Draw (no exotic tokens)
    if fam == "result":
        reasons: list[str] = []
        if not (10 <= minute <= 70):
            reasons.append("minute_out_of_range(10..70)")
        if abs((sh - sa)) > 1:
            reasons.append("goal_diff_gt_1")
        if (sh + sa) > 3:
            reasons.append("total_goals_gt_3")
        if not (1.70 <= odds <= 6.00):
            reasons.append("odds_out_of_range(1.70..6.00)")
        sel = _norm(c.market.selection)
        home = _norm(c.match.home_team)
        away = _norm(c.match.away_team)
        is_draw_sel = sel in {"x", "draw", "ничья", "ничья ", "ничья."} or "нич" in sel
        if not (sel == home or sel == away or is_draw_sel):
            reasons.append("selection_not_home_away_or_draw")
        if is_draw_sel and sh != sa:
            reasons.append("draw_selection_but_not_draw_score")

        if not reasons:
            return FootballLiveStrategyDecision(
                passed=True,
                strategy_id="S1_LIVE_1X2_CONTROLLED",
                strategy_name="Strategy 1: LIVE 1X2 (controlled state)",
                reasons=[
                    "market=result(1X2)",
                    "minute 10..70",
                    "goal_diff<=1 and total_goals<=3",
                    "odds 1.70..6.00",
                ],
            )
        return FootballLiveStrategyDecision(passed=False, reasons=reasons)

    # ------------------------------------------------------------
    # Strategy 2: LIVE Match Total OVER (needs 1–2 goals, not too late)
    # ------------------------------------------------------------
    # Goal: only match totals when the remaining-goals requirement is small.
    # Rules:
    # - market family: totals (match total)
    # - selection must be Over
    # - minute: 15..75
    # - we must be able to parse `what_needed_to_win` computed in rationale selection_context
    #   like: "match_goals_need_{k}_more_for_strict_over_{line}"
    # - k must be 1 or 2
    # - odds: 1.40..2.60
    if fam == "totals":
        reasons2: list[str] = []
        if not (15 <= minute <= 75):
            reasons2.append("minute_out_of_range(15..75)")
        if not (1.40 <= odds <= 2.60):
            reasons2.append("odds_out_of_range(1.40..2.60)")
        if not _is_over_selection(str(c.market.selection or "")):
            reasons2.append("not_over_selection")

        sel_ctx = _selection_context_from_candidate(c)
        wn = str(sel_ctx.get("what_needed_to_win") or "")
        need_goals: int | None = None
        if "match_goals_need_" in wn and "_more_for_strict_over_" in wn:
            try:
                mid = wn.split("match_goals_need_", 1)[1]
                need_goals = int(mid.split("_more_for_strict_over_", 1)[0])
            except Exception:
                need_goals = None
        if need_goals is None:
            reasons2.append("cannot_parse_need_goals")
        elif need_goals not in (1, 2):
            reasons2.append("need_goals_not_in(1,2)")

        if not reasons2:
            return FootballLiveStrategyDecision(
                passed=True,
                strategy_id="S2_LIVE_TOTAL_OVER_NEED_1_2",
                strategy_name="Strategy 2: LIVE Match Total OVER (need 1–2 goals)",
                reasons=[
                    "market=totals(match)",
                    "minute 15..75",
                    "need_goals in {1,2} (from strict-over requirement)",
                    "odds 1.40..2.60",
                ],
            )
        return FootballLiveStrategyDecision(passed=False, reasons=reasons2)

    return FootballLiveStrategyDecision(passed=False, reasons=["unsupported_market_family"])

