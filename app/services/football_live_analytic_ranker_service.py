from __future__ import annotations

import math
import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from app.schemas.provider_models import ProviderSignalCandidate
from app.services.football_bet_formatter_service import FootballBetFormatterService
from app.services.football_signal_send_filter_service import FootballSignalSendFilterService


@dataclass(frozen=True)
class FootballLiveRankerResult:
    total_candidates: int
    opportunities: int
    top: list[dict[str, object]]
    eligible_top: list[dict[str, object]]
    watchlist_top: list[dict[str, object]]
    api_count: int
    blocked_count: int
    eligible_count: int
    watchlist_count: int
    blocked_breakdown: dict[str, int]


class FootballLiveAnalyticRankerService:
    """Preview-only ranker for live football opportunities.

    It does not publish, score production candidates, or bypass existing safety gates.
    """

    def __init__(self) -> None:
        self._family = FootballSignalSendFilterService()
        self._fmt = FootballBetFormatterService()

    def rank(self, candidates: list[ProviderSignalCandidate], *, limit: int = 10) -> FootballLiveRankerResult:
        rows = [row for c in candidates if (row := self.evaluate(c)) is not None]
        rows.sort(key=lambda r: (self._bucket_rank(str(r.get("preview_bucket") or "")), float(r.get("analytic_score") or 0.0)), reverse=True)
        eligible_top = [r for r in rows if r.get("preview_bucket") == "eligible"][: max(1, int(limit))]
        watchlist_top = [r for r in rows if r.get("preview_bucket") == "watchlist"][: max(1, int(limit))]
        blocked_top = [r for r in rows if r.get("preview_bucket") == "blocked"][: max(1, int(limit))]
        top = [*eligible_top, *watchlist_top, *blocked_top][: max(1, int(limit))]
        return FootballLiveRankerResult(
            total_candidates=len(candidates),
            opportunities=len(rows),
            top=top,
            eligible_top=eligible_top,
            watchlist_top=watchlist_top,
            api_count=sum(1 for r in rows if bool(r.get("api_intelligence"))),
            blocked_count=sum(1 for r in rows if r.get("preview_bucket") == "blocked"),
            eligible_count=sum(1 for r in rows if r.get("preview_bucket") == "eligible"),
            watchlist_count=sum(1 for r in rows if r.get("preview_bucket") == "watchlist"),
            blocked_breakdown=self._blocked_breakdown(rows),
        )

    def evaluate(self, c: ProviderSignalCandidate) -> dict[str, object] | None:
        minute, sh, sa = self._live_state(c)
        odds = self._odds(c)
        api = self._api_intelligence(c)
        missing: list[str] = []
        if minute is None:
            missing.append("minute")
        if sh is None or sa is None:
            missing.append("score")
        if odds is None:
            missing.append("odds")

        opportunity = self._opportunity(c, sh, sa)
        if opportunity is None:
            return None

        score = 0.0
        reasons: list[str] = []
        block_reasons: list[str] = []
        risk_points = 0

        if missing:
            block_reasons.append("missing_" + "_".join(missing))
        if odds is not None and not (Decimal("1.25") <= odds <= Decimal("4.50")):
            block_reasons.append("odds_outside_sane_range")
        if self._is_blocked_competition(c):
            block_reasons.append("competition_blocked")
            risk_points += 3

        kind = str(opportunity["kind"])
        goals_needed = opportunity.get("goals_needed_to_win")
        selection_side = self._selection_side(c)
        if kind == "ft_1x2":
            score += 42.0
            if self._is_exotic_result_like(c):
                block_reasons.append("blocked_exotic_result_like")
            if not api:
                block_reasons.append("blocked_1x2_without_api_intelligence")
            if (sh, sa) == (0, 0) and not self._has_api_pressure(c):
                block_reasons.append("blocked_1x2_00_without_pressure")
            if self._is_trailing_side(selection_side, sh, sa):
                block_reasons.append("blocked_trailing_side_1x2")
            if api:
                score += 8.0
                reasons.append("api_intelligence_available")
            risk_points += 2
        elif kind == "match_total_over_need_1":
            score += 58.0
            reasons.append("match_total_over_need_1")
            if opportunity.get("period_scope") != "match":
                block_reasons.append("blocked_period_total")
            if minute is not None and not (35 <= minute <= 70):
                block_reasons.append("blocked_total_minute_window")
                if minute > 70:
                    block_reasons.append("blocked_late_total_over")
            if odds is not None and not (Decimal("1.45") <= odds <= Decimal("2.40")):
                block_reasons.append("blocked_total_odds_window")
        elif kind == "team_total_over_need_1":
            score += 52.0
            reasons.append("team_total_over_need_1")
            risk_points += 1
            if opportunity.get("period_scope") != "match":
                block_reasons.append("blocked_period_total")
            if minute is not None and not (35 <= minute <= 70):
                block_reasons.append("blocked_total_minute_window")
                if minute > 70:
                    block_reasons.append("blocked_late_total_over")
            if odds is not None and odds < Decimal("1.45"):
                block_reasons.append("blocked_total_odds_window")
            if odds is not None and odds > Decimal("2.40"):
                block_reasons.append("blocked_team_total_high_odds")

        if goals_needed is not None and goals_needed != 1:
            block_reasons.append("goals_needed_not_1")
        if odds is not None and odds > Decimal("3.50"):
            block_reasons.append("blocked_watchlist_odds_gt_3_50")

        score += self._minute_score(minute, kind, reasons)
        score += self._odds_score(odds, reasons)
        score += self._score_state_bonus(sh, sa, kind, reasons)
        score += self._api_score(api, opportunity, reasons)

        if not api:
            reasons.append("winline_only")
            if kind == "ft_1x2":
                risk_points += 2

        risk_level = "high" if risk_points >= 3 or block_reasons else "medium" if risk_points else "low"
        if risk_level == "high" and "blocked_high_risk_preview" not in block_reasons:
            block_reasons.append("blocked_high_risk_preview")
        send_eligible = not block_reasons
        if not send_eligible:
            score = min(score, 49.0)
        preview_bucket = "eligible" if send_eligible else "watchlist" if self._is_watchlist_candidate(
            kind=kind,
            minute=minute,
            odds=odds,
            api=api,
            selection_side=selection_side,
            sh=sh,
            sa=sa,
            block_reasons=block_reasons,
        ) else "blocked"

        return {
            "match": str(c.match.match_name or ""),
            "event_id": str(c.match.external_event_id or ""),
            "minute": minute,
            "score": f"{sh}:{sa}" if sh is not None and sa is not None else None,
            "proposed_bet": self._bet_text(c),
            "odds": str(odds) if odds is not None else None,
            "market_type": opportunity["kind"],
            "analytic_score": round(max(0.0, min(100.0, score)), 1),
            "risk_level": risk_level,
            "api_intelligence": bool(api),
            "confidence_reason": "; ".join(reasons[:8]) or "no_positive_factors",
            "missing_data": missing,
            "send_eligible": bool(send_eligible),
            "preview_bucket": preview_bucket,
            "block_reason": ", ".join(block_reasons) if block_reasons else None,
            "block_reasons": block_reasons,
            "goals_needed_to_win": goals_needed,
        }

    def _bucket_rank(self, bucket: str) -> int:
        return {"eligible": 3, "watchlist": 2, "blocked": 1}.get(bucket, 0)

    def _is_watchlist_candidate(
        self,
        *,
        kind: str,
        minute: int | None,
        odds: Decimal | None,
        api: dict[str, Any],
        selection_side: str | None,
        sh: int | None,
        sa: int | None,
        block_reasons: list[str],
    ) -> bool:
        hard_no = {
            "missing_minute",
            "missing_score",
            "missing_odds",
            "missing_minute_score",
            "missing_minute_odds",
            "missing_score_odds",
            "missing_minute_score_odds",
            "odds_outside_sane_range",
            "blocked_watchlist_odds_gt_3_50",
            "blocked_exotic_result_like",
            "competition_blocked",
            "goals_needed_not_1",
            "blocked_period_total",
            "blocked_trailing_side_1x2",
        }
        if any(reason in hard_no for reason in block_reasons):
            return False
        if kind == "ft_1x2":
            return bool(api and not self._is_trailing_side(selection_side, sh, sa))
        if kind == "match_total_over_need_1":
            return bool(minute is not None and 35 <= minute <= 80 and odds is not None and odds <= Decimal("3.50"))
        if kind == "team_total_over_need_1":
            return bool(minute is not None and 35 <= minute <= 80 and odds is not None and odds <= Decimal("3.20"))
        return False

    def _blocked_breakdown(self, rows: list[dict[str, object]]) -> dict[str, int]:
        out: dict[str, int] = {}
        for row in rows:
            reasons = row.get("block_reasons")
            if not isinstance(reasons, list) or not reasons:
                continue
            for reason in reasons:
                key = str(reason)
                out[key] = int(out.get(key, 0) or 0) + 1
        return dict(sorted(out.items(), key=lambda kv: kv[1], reverse=True))

    def _opportunity(self, c: ProviderSignalCandidate, sh: int | None, sa: int | None) -> dict[str, object] | None:
        fam = self._family.get_market_family(c)
        mt = (c.market.market_type or "").strip().lower()
        if fam == "result" and mt in {"1x2", "match_winner"} and self._selection_side(c) in {"home", "away", "draw"}:
            return {"kind": "ft_1x2", "goals_needed_to_win": None}

        ctx = self._fmt.describe_total_context(
            market_type=c.market.market_type,
            market_label=c.market.market_label,
            selection=c.market.selection,
            home_team=c.match.home_team,
            away_team=c.match.away_team,
            section_name=c.market.section_name,
            subsection_name=c.market.subsection_name,
        )
        if ctx is None or ctx.total_side != "ТБ" or ctx.total_line is None:
            return None
        try:
            line = float(str(ctx.total_line).replace(",", "."))
        except (TypeError, ValueError):
            return None
        goals_needed = self._goals_needed(ctx.target_scope, line, sh, sa)
        if ctx.target_scope == "match":
            return {
                "kind": "match_total_over_need_1",
                "goals_needed_to_win": goals_needed,
                "period_scope": ctx.period_scope,
            }
        if ctx.target_scope in {"home_team", "away_team", "team_total"}:
            return {
                "kind": "team_total_over_need_1",
                "goals_needed_to_win": goals_needed,
                "period_scope": ctx.period_scope,
            }
        return None

    def _goals_needed(self, target_scope: str, line: float, sh: int | None, sa: int | None) -> int | None:
        if sh is None or sa is None:
            return None
        current = sh + sa
        if target_scope == "home_team":
            current = sh
        elif target_scope == "away_team":
            current = sa
        return int(math.floor(float(line))) + 1 - int(current)

    def _live_state(self, c: ProviderSignalCandidate) -> tuple[int | None, int | None, int | None]:
        fs = c.feature_snapshot_json if isinstance(c.feature_snapshot_json, dict) else {}
        fa = fs.get("football_analytics") if isinstance(fs.get("football_analytics"), dict) else {}
        minute_raw = fa.get("minute") if fa.get("minute") is not None else fs.get("minute")
        sh_raw = fa.get("score_home") if fa.get("score_home") is not None else fs.get("score_home")
        sa_raw = fa.get("score_away") if fa.get("score_away") is not None else fs.get("score_away")
        return self._int(minute_raw), self._int(sh_raw), self._int(sa_raw)

    def _api_intelligence(self, c: ProviderSignalCandidate) -> dict[str, Any]:
        fs = c.feature_snapshot_json if isinstance(c.feature_snapshot_json, dict) else {}
        api = fs.get("api_football_team_intelligence")
        return api if isinstance(api, dict) else {}

    def _api_score(self, api: dict[str, Any], opportunity: dict[str, object], reasons: list[str]) -> float:
        if not api:
            return 0.0
        score = min(10.0, float(api.get("confidence_score") or 0.0) / 10.0)
        st = api.get("standings_edge") if isinstance(api.get("standings_edge"), dict) else {}
        rank_edge = self._float(st.get("rank_edge_home_minus_away")) if st else None
        if rank_edge is not None:
            score += max(-4.0, min(4.0, rank_edge * 0.35))
            reasons.append(f"standings_edge={rank_edge:g}")
        gf_home = self._float(api.get("avg_goals_for_home"))
        gf_away = self._float(api.get("avg_goals_for_away"))
        if gf_home is not None and gf_away is not None:
            score += max(-3.0, min(3.0, (gf_home + gf_away - 2.0) * 1.5))
            reasons.append(f"avg_goals_sum={gf_home + gf_away:.2f}")
        h2h_home = self._float(api.get("h2h_home_wins")) or 0.0
        h2h_away = self._float(api.get("h2h_away_wins")) or 0.0
        h2h_matches = self._float(api.get("h2h_matches")) or 0.0
        if h2h_matches > 0:
            score += max(-2.0, min(2.0, (h2h_home - h2h_away) / max(1.0, h2h_matches) * 4.0))
            reasons.append(f"h2h={int(h2h_home)}-{int(h2h_away)}")
        common = api.get("common_opponent_edge") if isinstance(api.get("common_opponent_edge"), dict) else {}
        edge = self._float(common.get("edge_home_minus_away")) if common else None
        if edge is not None:
            score += max(-2.0, min(2.0, edge))
            reasons.append(f"common_edge={edge:g}")
        return score

    def _minute_score(self, minute: int | None, kind: str, reasons: list[str]) -> float:
        if minute is None:
            return 0.0
        if kind.endswith("need_1") and 35 <= minute <= 70:
            reasons.append("minute_window_good")
            return 12.0
        if 20 <= minute <= 80:
            return 5.0
        return -8.0

    def _odds_score(self, odds: Decimal | None, reasons: list[str]) -> float:
        if odds is None:
            return 0.0
        if Decimal("1.45") <= odds <= Decimal("2.40"):
            reasons.append("odds_window_good")
            return 10.0
        if Decimal("2.40") < odds <= Decimal("3.20"):
            return 3.0
        return -6.0

    def _score_state_bonus(self, sh: int | None, sa: int | None, kind: str, reasons: list[str]) -> float:
        if sh is None or sa is None:
            return 0.0
        total = sh + sa
        if kind.endswith("need_1") and total <= 2:
            reasons.append("low_total_goal_state")
            return 8.0
        if kind == "ft_1x2" and total > 0:
            return 4.0
        return 0.0

    def _bet_text(self, c: ProviderSignalCandidate) -> str:
        pres = self._fmt.format_bet(
            market_type=c.market.market_type,
            market_label=c.market.market_label,
            selection=c.market.selection,
            home_team=c.match.home_team,
            away_team=c.match.away_team,
            section_name=c.market.section_name,
            subsection_name=c.market.subsection_name,
        )
        return pres.main_label + (f" ({pres.detail_label})" if pres.detail_label else "")

    def _selection_side(self, c: ProviderSignalCandidate) -> str | None:
        sel = (c.market.selection or "").strip().lower().replace("х", "x")
        if sel in {"1", "п1", "p1", "home"}:
            return "home"
        if sel in {"2", "п2", "p2", "away"}:
            return "away"
        if sel in {"x", "н", "draw", "ничья"}:
            return "draw"
        return None

    def _is_trailing_side(self, side: str | None, sh: int | None, sa: int | None) -> bool:
        if side not in {"home", "away"} or sh is None or sa is None:
            return False
        return bool((side == "home" and sh < sa) or (side == "away" and sa < sh))

    def _is_exotic_result_like(self, c: ProviderSignalCandidate) -> bool:
        blob = " ".join(
            [
                c.market.market_type or "",
                c.market.market_label or "",
                c.market.selection or "",
                c.market.section_name or "",
                c.market.subsection_name or "",
            ]
        ).lower().replace("ё", "е")
        return bool(
            re.search(
                r"(?:european\s*handicap|handicap|гандикап|фора|interval|интервал|тайм|half|period|период|next\s*goal|следующ(?:ий|его)\s+гол|remainder|остат)",
                blob,
            )
        )

    def _has_api_pressure(self, c: ProviderSignalCandidate) -> bool:
        fs = c.feature_snapshot_json if isinstance(c.feature_snapshot_json, dict) else {}
        part = fs.get("football_live_context_participation")
        if isinstance(part, dict):
            score = self._int(part.get("api_football_pressure_score"))
            return bool(part.get("api_football_pressure_used") and score is not None and score >= 2)
        ctx = fs.get("football_live_context_filter")
        if isinstance(ctx, dict):
            score = self._int(ctx.get("selected_pressure_score"))
            return bool(ctx.get("fixture_id") and score is not None and score >= 2)
        return False

    def _is_blocked_competition(self, c: ProviderSignalCandidate) -> bool:
        blob = " ".join([c.match.tournament_name or "", c.match.match_name or "", c.match.home_team or "", c.match.away_team or ""]).lower().replace("ё", "е")
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

    def _odds(self, c: ProviderSignalCandidate) -> Decimal | None:
        try:
            return Decimal(str(c.market.odds_value))
        except Exception:
            return None

    def _int(self, value: object) -> int | None:
        if value is None or isinstance(value, bool):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _float(self, value: object) -> float | None:
        if value is None or isinstance(value, bool):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
