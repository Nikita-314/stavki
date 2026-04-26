from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from app.core.enums import BookmakerType
from app.schemas.provider_models import ProviderOddsMarket, ProviderSignalCandidate

S13_CONTROLLED_STRATEGY_ID = "S13_LIVE_ANALYTIC_PROBABILITY_MODEL"
S13_CONTROLLED_STRATEGY_NAME = "S13 live analytic probability model"


@dataclass(frozen=True)
class S13ControlledDecision:
    passed: bool
    reasons: list[str]


def evaluate_s13_controlled_idea(row: dict[str, Any]) -> S13ControlledDecision:
    reasons: list[str] = []
    if not bool(row.get("is_usable")):
        reasons.append("not_usable")
    edge = _decimal(row.get("value_edge"))
    if edge is None or edge < Decimal("0.07"):
        reasons.append("edge_lt_0_07")
    confidence = _int(row.get("confidence_score"))
    if confidence is None or confidence < 65:
        reasons.append("confidence_lt_65")
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

    if kind == "match_total_over":
        if goals_needed != 1:
            reasons.append("match_total_goals_needed_not_1")
        if minute is None or not (35 <= minute <= 75):
            reasons.append("match_total_minute_window_35_75")
        if odds is None or not (Decimal("1.35") <= odds <= Decimal("2.40")):
            reasons.append("match_total_odds_window_1_35_2_40")
        if line is None or line > Decimal("3.5"):
            reasons.append("match_total_line_gt_3_5")
    elif kind == "team_total_over":
        if goals_needed != 1:
            reasons.append("team_total_goals_needed_not_1")
        if minute is None or not (35 <= minute <= 70):
            reasons.append("team_total_minute_window_35_70")
        if odds is None or not (Decimal("1.45") <= odds <= Decimal("2.40")):
            reasons.append("team_total_odds_window_1_45_2_40")
        if line is None or line > Decimal("2.5"):
            reasons.append("team_total_line_gt_2_5")
    elif kind == "ft_1x2":
        if not api_ok:
            reasons.append("1x2_without_api_intelligence")
    else:
        reasons.append("unsupported_market_kind")

    return S13ControlledDecision(passed=not reasons, reasons=reasons)


def select_s13_controlled_candidates(
    rows: list[dict[str, Any]],
    *,
    enabled: bool,
) -> tuple[list[ProviderSignalCandidate], dict[str, int]]:
    if not enabled or not rows:
        return [], {"evaluated": 0, "sent": 0, "blocked": 0}
    out: list[ProviderSignalCandidate] = []
    evaluated = 0
    blocked = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        evaluated += 1
        decision = evaluate_s13_controlled_idea(row)
        if not decision.passed:
            blocked += 1
            continue
        cand = _idea_to_candidate(row, decision.reasons)
        if cand is None:
            blocked += 1
            continue
        out.append(cand)
    return out, {"evaluated": evaluated, "sent": len(out), "blocked": blocked}


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
        signal_score=Decimal(str(row.get("confidence_score") or "65")),
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
