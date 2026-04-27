from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from app.core.enums import BookmakerType
from app.schemas.provider_models import ProviderOddsMarket, ProviderSignalCandidate

S13_CONTROLLED_STRATEGY_ID = "S13_LIVE_ANALYTIC_PROBABILITY_MODEL"
S13_CONTROLLED_STRATEGY_NAME = "S13 live analytic probability model"
S13_MAX_SIGNALS_PER_LIVE_CYCLE = 5


@dataclass(frozen=True)
class S13ControlledDecision:
    passed: bool
    reasons: list[str]


S13_GATE_MIN_EDGE = Decimal("0.05")
S13_GATE_MIN_CONFIDENCE = 55
S13_COMBAT_OVERCONFIDENT_EDGE_MIN = Decimal("0.10")
S13_FT_1X2_COMBAT_MIN_CONFIDENCE = 75
S13_1X2_00_MIN_OVER_NEXT_GOAL = Decimal("0.48")

S13_MARKET_RULE_REASONS = frozenset(
    {
        "match_total_goals_needed_not_1",
        "match_total_minute_window_35_75",
        "match_total_odds_combat_window",
        "match_total_line_gt_3_5",
        "1x2_without_api_intelligence",
        "ft_1x2_confidence_lt_75_combat",
        "ft_1x2_edge_combat_band",
        "ft_1x2_00_no_pressure",
        "ft_1x2_trailing_side_blocked",
        "unsupported_market_kind",
    }
)


def _row_send_score(row: dict[str, Any]) -> float:
    edge = _decimal(row.get("value_edge"))
    conf = _int(row.get("confidence_score"))
    if edge is None or conf is None:
        return -1.0
    return float(edge * Decimal(conf))


def evaluate_s13_controlled_idea(row: dict[str, Any]) -> S13ControlledDecision:
    reasons: list[str] = []
    if not bool(row.get("is_usable")):
        reasons.append("not_usable")
    edge = _decimal(row.get("value_edge"))
    if edge is None or edge < S13_GATE_MIN_EDGE:
        reasons.append("edge_lt_0_05")
    confidence = _int(row.get("confidence_score"))
    if confidence is None or confidence < S13_GATE_MIN_CONFIDENCE:
        reasons.append("confidence_lt_55")
    risk = str(row.get("risk_level") or "").lower().strip()
    if risk == "high" or risk not in {"low", "medium"}:
        reasons.append("risk_not_low_medium")
    if _competition_is_blocked(row):
        reasons.append("competition_blocked")
    if bool(row.get("is_corner")):
        reasons.append("corner_market_blocked")
    if bool(row.get("is_exotic")):
        reasons.append("exotic_market_blocked")
    if bool(row.get("is_period")):
        reasons.append("period_market_blocked")

    kind = str(row.get("bet_kind") or "")
    minute = _int(row.get("minute"))
    odds = _decimal(row.get("best_bet_odds"))
    line = _decimal(row.get("line"))
    goals_needed = _int(row.get("goals_needed_to_win"))
    api_ok = bool(row.get("api_intelligence_available"))

    # Combat-only: team totals disabled (ideas/preview unchanged).
    if kind == "team_total_over":
        reasons.append("blocked_team_total_over")
        return S13ControlledDecision(passed=False, reasons=reasons)

    if edge is not None and edge >= S13_COMBAT_OVERCONFIDENT_EDGE_MIN:
        reasons.append("blocked_overconfident_edge")
        return S13ControlledDecision(passed=False, reasons=reasons)

    if kind == "match_total_over":
        if goals_needed != 1:
            reasons.append("match_total_goals_needed_not_1")
        if minute is None or not (35 <= minute <= 75):
            reasons.append("match_total_minute_window_35_75")
        if odds is None or not (Decimal("1.35") <= odds <= Decimal("2.20")):
            reasons.append("match_total_odds_combat_window")
        if line is None or line > Decimal("3.5"):
            reasons.append("match_total_line_gt_3_5")
    elif kind == "ft_1x2":
        if not api_ok:
            reasons.append("1x2_without_api_intelligence")
        if confidence is None or confidence < S13_FT_1X2_COMBAT_MIN_CONFIDENCE:
            reasons.append("ft_1x2_confidence_lt_75_combat")
        if edge is None or not (S13_GATE_MIN_EDGE <= edge < S13_COMBAT_OVERCONFIDENT_EDGE_MIN):
            reasons.append("ft_1x2_edge_combat_band")
        sh = _int(row.get("score_home"))
        sa = _int(row.get("score_away"))
        if sh == 0 and sa == 0:
            ong = _decimal(row.get("over_next_goal_probability"))
            if ong is None or ong < S13_1X2_00_MIN_OVER_NEXT_GOAL:
                reasons.append("ft_1x2_00_no_pressure")
        if _ft_1x2_is_trailing_side(row):
            reasons.append("ft_1x2_trailing_side_blocked")
    else:
        reasons.append("unsupported_market_kind")

    return S13ControlledDecision(passed=not reasons, reasons=reasons)


def _ft_1x2_selection_side(row: dict[str, Any]) -> str | None:
    sel = str(row.get("source_selection") or "").strip().lower().replace("х", "x").replace("ё", "е")
    if sel in {"1", "п1", "p1", "home"}:
        return "home"
    if sel in {"2", "п2", "p2", "away"}:
        return "away"
    if sel in {"x", "draw", "ничья", "н"}:
        return "draw"
    home = str(row.get("home") or "").strip().lower().replace("ё", "е")
    away = str(row.get("away") or "").strip().lower().replace("ё", "е")
    if home and (sel == home or home in sel):
        return "home"
    if away and (sel == away or away in sel):
        return "away"
    return None


def _ft_1x2_is_trailing_side(row: dict[str, Any]) -> bool:
    side = _ft_1x2_selection_side(row)
    if side not in {"home", "away"}:
        return False
    sh = _int(row.get("score_home"))
    sa = _int(row.get("score_away"))
    if sh is None or sa is None:
        return False
    if side == "home" and sh < sa:
        return True
    if side == "away" and sa < sh:
        return True
    return False


def select_s13_controlled_candidates(
    rows: list[dict[str, Any]],
    *,
    enabled: bool,
) -> tuple[list[ProviderSignalCandidate], dict[str, int]]:
    """Return all gate-passed S13 candidates sorted by (value_edge * confidence_score) desc.

    Per-cycle cap (TOP-5) and cross-strategy dedup are applied in auto_signal_service.
    """
    if not enabled or not rows:
        return [], {
            "evaluated": 0,
            "after_gate": 0,
            "sent": 0,
            "blocked": 0,
            "blocked_by_gate": 0,
            "blocked_by_candidate_build": 0,
            "blocked_team_total_over": 0,
            "blocked_overconfident_edge": 0,
            "blocked_market_rules": 0,
        }
    evaluated = 0
    blocked_by_gate = 0
    blocked_by_candidate_build = 0
    blocked_team_total_over = 0
    blocked_overconfident_edge = 0
    blocked_market_rules = 0
    passed: list[tuple[dict[str, Any], list[str]]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        evaluated += 1
        decision = evaluate_s13_controlled_idea(row)
        if not decision.passed:
            blocked_by_gate += 1
            rs = set(decision.reasons)
            if "blocked_team_total_over" in rs:
                blocked_team_total_over += 1
            elif "blocked_overconfident_edge" in rs:
                blocked_overconfident_edge += 1
            elif rs & S13_MARKET_RULE_REASONS:
                blocked_market_rules += 1
            continue
        passed.append((row, decision.reasons))
    passed.sort(key=lambda t: _row_send_score(t[0]), reverse=True)
    out: list[ProviderSignalCandidate] = []
    for row, reasons in passed:
        cand = _idea_to_candidate(row, reasons)
        if cand is None:
            blocked_by_candidate_build += 1
            continue
        out.append(cand)
    after_gate = len(passed)
    blocked = int(blocked_by_gate + blocked_by_candidate_build)
    return out, {
        "evaluated": evaluated,
        "after_gate": after_gate,
        "sent": len(out),
        "blocked": blocked,
        "blocked_by_gate": blocked_by_gate,
        "blocked_by_candidate_build": blocked_by_candidate_build,
        "blocked_team_total_over": int(blocked_team_total_over),
        "blocked_overconfident_edge": int(blocked_overconfident_edge),
        "blocked_market_rules": int(blocked_market_rules),
    }


def _idea_to_candidate(row: dict[str, Any], reasons: list[str]) -> ProviderSignalCandidate | None:
    odds = _decimal(row.get("best_bet_odds"))
    if odds is None:
        return None
    from datetime import datetime, timezone

    from app.core.enums import SportType
    from app.schemas.provider_models import ProviderMatch

    match = ProviderMatch(
        external_event_id=str(row.get("event_id") or ""),
        sport=SportType.FOOTBALL,
        tournament_name=str(row.get("tournament_name") or ""),
        match_name=str(row.get("match") or ""),
        home_team=str(row.get("home") or ""),
        away_team=str(row.get("away") or ""),
        event_start_at=datetime.now(timezone.utc),
        is_live=True,
        source_name="winline",
    )
    market = ProviderOddsMarket(
        bookmaker=BookmakerType.WINLINE,
        market_type=str(row.get("source_market_type") or _fallback_market_type(row)),
        market_label=str(row.get("source_market_label") or row.get("best_bet") or ""),
        selection=str(row.get("source_selection") or row.get("best_bet") or ""),
        odds_value=odds,
        section_name=_str_or_none(row.get("source_section_name")),
        subsection_name=_str_or_none(row.get("source_subsection_name")),
    )
    minute = _int(row.get("minute"))
    sh = _int(row.get("score_home"))
    sa = _int(row.get("score_away"))
    fs = {
        "football_analytics": {
            "minute": minute,
            "score_home": sh,
            "score_away": sa,
        },
        "football_live_s13_probability": dict(row),
    }
    explanation = {
        "football_live_strategy_id": S13_CONTROLLED_STRATEGY_ID,
        "football_live_strategy_name": S13_CONTROLLED_STRATEGY_NAME,
        "football_live_strategy_reasons": ["s13_controlled_usable", *list(reasons or [])],
    }
    return ProviderSignalCandidate(
        match=match,
        market=market,
        min_entry_odds=Decimal("1.01"),
        predicted_prob=_decimal(row.get("model_probability")),
        implied_prob=_decimal(row.get("implied_probability")),
        edge=_decimal(row.get("value_edge")),
        model_name=S13_CONTROLLED_STRATEGY_ID,
        model_version_name="controlled_v1",
        signal_score=Decimal(str(row.get("confidence_score") or str(S13_GATE_MIN_CONFIDENCE))),
        notes="live_auto",
        feature_snapshot_json=fs,
        raw_model_output_json={"s13_probability_idea": dict(row)},
        explanation_json=explanation,
    )


def _fallback_market_type(row: dict[str, Any]) -> str:
    kind = str(row.get("bet_kind") or "")
    if kind == "ft_1x2":
        return "1x2"
    return "total_goals"


def _competition_is_blocked(row: dict[str, Any]) -> bool:
    blob = " ".join(
        [
            str(row.get("competition") or ""),
            str(row.get("tournament_name") or ""),
            str(row.get("match") or ""),
            str(row.get("home") or ""),
            str(row.get("away") or ""),
        ]
    ).lower().replace("ё", "е")
    if "(ж)" in blob or "[ж]" in blob:
        return True
    if re.search(r"(?<![a-zа-я0-9])рез(?:\.|(?=$|[^a-zа-я0-9]))", blob):
        return True
    return bool(
        re.search(
            r"\b(?:u1[789]|u2[013]|youth|women|woman|reserve|reserves|friendly|amateur|amateurs|товарищ|жен(?:щины)?|люб\.?|любител[ьи]|до\s*(?:17|18|19|20|21|23)|резерв\w*|дубл\w*|кибер|esoccer)\b",
            blob,
        )
    )


def _int(v: object) -> int | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        return int(v)
    except Exception:
        return None


def _decimal(v: object) -> Decimal | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        return Decimal(str(v).replace(",", "."))
    except Exception:
        return None


def _str_or_none(v: object) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None
