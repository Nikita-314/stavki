from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import re
from typing import Any

from app.schemas.provider_models import ProviderSignalCandidate
from app.services.football_bet_formatter_service import FootballBetFormatterService
from app.services.football_live_analytic_ranker_service import FootballLiveAnalyticRankerService
from app.services.football_signal_send_filter_service import FootballSignalSendFilterService

S12_CONTROLLED_STRATEGY_ID = "S12_LIVE_ANALYTIC_RANKER_CONTROLLED"
S12_CONTROLLED_STRATEGY_NAME = "S12 controlled: match total over need 1 (eligible only)"


@dataclass(frozen=True)
class S12ControlledDecision:
    passed: bool
    reasons: list[str]


def evaluate_s12_controlled_candidate(
    c: ProviderSignalCandidate,
    *,
    rank_row: dict[str, Any] | None,
) -> S12ControlledDecision:
    reasons: list[str] = []
    if not isinstance(rank_row, dict):
        return S12ControlledDecision(False, ["rank_row_missing"])
    if str(rank_row.get("preview_bucket") or "") != "eligible":
        reasons.append("rank_bucket_not_eligible")
    if str(rank_row.get("risk_level") or "") not in {"low", "medium"}:
        reasons.append("rank_risk_not_low_medium")
    if str(rank_row.get("market") or "") != "match_total_over":
        reasons.append("market_not_match_total_over")
    if str(rank_row.get("market_type") or "") != "match_total_over_need_1":
        reasons.append("market_type_not_need_1_match_total")
    if int(rank_row.get("goals_needed_to_win") or 0) != 1:
        reasons.append("goals_needed_not_1")

    minute = _int(rank_row.get("minute"))
    if minute is None or not (45 <= minute <= 70):
        reasons.append("minute_window_45_70")
    odds = _decimal(rank_row.get("odds"))
    if odds is None or not (Decimal("1.45") <= odds <= Decimal("2.10")):
        reasons.append("odds_window_1_45_2_10")

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
    if ctx is None:
        reasons.append("total_context_missing")
    else:
        if ctx.total_side != "ТБ":
            reasons.append("not_over")
        if str(ctx.target_scope or "") != "match":
            reasons.append("not_match_total_scope")
        if str(ctx.period_scope or "") != "match":
            reasons.append("not_match_period")
    if FootballSignalSendFilterService().is_corner_market(c):
        reasons.append("corner_market_blocked")
    if _competition_is_blocked(c):
        reasons.append("competition_blocked")

    return S12ControlledDecision(passed=not reasons, reasons=reasons)


def select_s12_controlled_candidates(
    candidates: list[ProviderSignalCandidate],
    *,
    enabled: bool,
) -> tuple[list[ProviderSignalCandidate], dict[str, int]]:
    if not enabled or not candidates:
        return [], {"evaluated": 0, "sent": 0, "blocked": 0}
    ranker = FootballLiveAnalyticRankerService()
    out: list[ProviderSignalCandidate] = []
    evaluated = 0
    blocked = 0
    for c in candidates:
        row = ranker.evaluate(c)
        if not isinstance(row, dict):
            continue
        evaluated += 1
        d = evaluate_s12_controlled_candidate(c, rank_row=row)
        if not d.passed:
            blocked += 1
            continue
        prev_expl = dict(c.explanation_json or {})
        prev_expl["football_live_strategy_id"] = S12_CONTROLLED_STRATEGY_ID
        prev_expl["football_live_strategy_name"] = S12_CONTROLLED_STRATEGY_NAME
        prev_expl["football_live_strategy_reasons"] = list(d.reasons)
        out.append(c.model_copy(update={"explanation_json": prev_expl}))
    return out, {"evaluated": evaluated, "sent": len(out), "blocked": blocked}


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


def _competition_is_blocked(c: ProviderSignalCandidate) -> bool:
    blob = " ".join(
        [
            str(c.match.tournament_name or ""),
            str(c.match.match_name or ""),
            str(c.match.home_team or ""),
            str(c.match.away_team or ""),
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
