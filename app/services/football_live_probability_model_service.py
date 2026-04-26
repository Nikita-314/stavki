from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from app.schemas.provider_models import ProviderSignalCandidate
from app.services.football_bet_formatter_service import FootballBetFormatterService
from app.services.football_signal_send_filter_service import FootballSignalSendFilterService


@dataclass(frozen=True)
class FootballLiveProbabilityModelResult:
    total_matches: int
    with_api_intelligence: int
    without_api_intelligence: int
    ideas: list[dict[str, object]]
    top_raw: list[dict[str, object]]
    usable_top: list[dict[str, object]]
    value_edge_7_count: int
    confidence_60_count: int
    usable_count: int
    raw_high_risk_count: int


class FootballLiveProbabilityModelService:
    def __init__(self) -> None:
        self._fmt = FootballBetFormatterService()
        self._family = FootballSignalSendFilterService()

    def evaluate(self, candidates: list[ProviderSignalCandidate], *, limit: int = 15) -> FootballLiveProbabilityModelResult:
        grouped: dict[str, list[ProviderSignalCandidate]] = {}
        for c in candidates:
            eid = str(c.match.external_event_id or "")
            if not eid:
                continue
            grouped.setdefault(eid, []).append(c)

        ideas: list[dict[str, object]] = []
        with_api = 0
        for rows in grouped.values():
            idea = self._evaluate_match(rows)
            if idea is None:
                continue
            if bool(idea.get("api_intelligence_available")):
                with_api += 1
            ideas.append(idea)

        ideas.sort(key=lambda r: (float(r.get("value_edge") or -9.0), float(r.get("confidence_score") or 0.0)), reverse=True)
        raw_top = ideas[: max(1, int(limit))]
        usable = [r for r in ideas if bool(r.get("is_usable"))]
        usable.sort(
            key=lambda r: (
                float(r.get("usable_score") or -999.0),
                float(r.get("value_edge") or -9.0),
            ),
            reverse=True,
        )
        usable_top = usable[: max(1, int(limit))]
        return FootballLiveProbabilityModelResult(
            total_matches=len(ideas),
            with_api_intelligence=with_api,
            without_api_intelligence=max(0, len(ideas) - with_api),
            ideas=ideas,
            top_raw=raw_top,
            usable_top=usable_top,
            value_edge_7_count=sum(1 for r in ideas if float(r.get("value_edge") or 0.0) >= 0.05),
            confidence_60_count=sum(1 for r in ideas if float(r.get("confidence_score") or 0.0) >= 55.0),
            usable_count=len(usable),
            raw_high_risk_count=sum(1 for r in ideas if str(r.get("risk_level") or "") == "high"),
        )

    def _evaluate_match(self, rows: list[ProviderSignalCandidate]) -> dict[str, object] | None:
        if not rows:
            return None
        c0 = rows[0]
        minute, sh, sa = self._live_state(c0)
        if minute is None or sh is None or sa is None:
            return None
        api = self._api_intelligence(c0)
        api_ok = bool(api)
        competition_risk = self._competition_risk(c0)
        fixture_id = self._fixture_id(c0, api)
        odds_1x2 = self._extract_1x2_odds(rows)
        ph, pd, pa = self._base_1x2_probs(odds_1x2, minute=minute, sh=sh, sa=sa)
        ph, pd, pa, reasons_api = self._apply_api_adjustments(ph, pd, pa, api)
        over_p = self._over_next_goal_probability(rows, minute=minute, sh=sh, sa=sa, api=api)
        best = self._best_bet(rows, ph=ph, pd=pd, pa=pa, over_p=over_p)
        if not best.get("best_bet") or not best.get("best_bet_odds"):
            return None
        implied = float(best.get("implied_probability") or 0.0)
        model_p = float(best.get("model_probability") or 0.0)
        missing: list[str] = []
        if not api_ok:
            missing.append("api_intelligence")
        if not odds_1x2:
            missing.append("1x2_odds_snapshot")
        if not best.get("best_bet") or not best.get("best_bet_odds"):
            missing.append("best_bet_missing")
        total_goals = sh + sa
        penalties: list[str] = []
        model_mult = 1.0
        conf_delta = 0.0
        best_odds = self._float(best.get("best_bet_odds"))
        best_line = self._float(best.get("line"))
        best_kind = str(best.get("bet_kind") or "")
        best_goals_needed = self._int(best.get("goals_needed_to_win"))
        if competition_risk == "high":
            model_mult *= 0.76
            conf_delta -= 25.0
            penalties.append("penalty_competition_high")
        elif competition_risk == "medium":
            model_mult *= 0.92
            conf_delta -= 8.0
            penalties.append("penalty_competition_medium")
        if minute > 75:
            model_mult *= 0.82
            conf_delta -= 14.0
            penalties.append("penalty_late_minute")
        if best_odds is not None and best_odds > 3.00:
            model_mult *= 0.80
            conf_delta -= 14.0
            penalties.append("penalty_high_odds")
        if best_kind == "match_total_over" and best_line is not None and best_line > 3.5:
            model_mult *= 0.78
            conf_delta -= 18.0
            penalties.append("penalty_match_total_line_gt_3_5")
        if best_kind == "team_total_over" and best_line is not None and best_line > 2.5:
            model_mult *= 0.76
            conf_delta -= 18.0
            penalties.append("penalty_team_total_line_gt_2_5")
        if total_goals >= 4:
            model_mult *= 0.84
            conf_delta -= 12.0
            penalties.append("penalty_high_score_state")
        if best_kind == "ft_1x2" and not api_ok:
            model_mult *= 0.72
            conf_delta -= 22.0
            penalties.append("penalty_1x2_without_api")
        model_p = max(0.01, min(0.98, model_p * model_mult))
        edge = round(model_p - implied, 4)
        confidence = self._confidence_score(
            api_ok=api_ok,
            odds_1x2=bool(odds_1x2),
            minute=minute,
            sh=sh,
            sa=sa,
            best_odds=best_odds,
            best_kind=best_kind,
            competition_risk=competition_risk,
        )
        confidence = int(max(5, min(95, confidence + conf_delta)))
        risk = self._risk_level(
            minute=minute,
            odds=best_odds,
            critical_missing=bool(best.get("best_bet") is None or best.get("best_bet_odds") is None),
            competition_risk=competition_risk,
        )
        reasons = [
            *reasons_api,
            f"competition={competition_risk}",
            *self._live_reasons(minute=minute, sh=sh, sa=sa),
            *list(best.get("reasons") or []),
            *penalties,
        ]
        is_usable, usable_blockers = self._is_usable_idea(
            minute=minute,
            risk=risk,
            confidence=confidence,
            api_ok=api_ok,
            best=best,
            competition_risk=competition_risk,
            total_goals=total_goals,
            value_edge=edge,
        )
        usable_score = round(max(0.0, edge) * float(confidence), 4) if is_usable else 0.0

        return {
            "match": str(c0.match.match_name or ""),
            "tournament_name": str(c0.match.tournament_name or ""),
            "event_id": str(c0.match.external_event_id or ""),
            "fixture_id": fixture_id,
            "minute": minute,
            "score": f"{sh}:{sa}",
            "score_home": sh,
            "score_away": sa,
            "home": str(c0.match.home_team or ""),
            "away": str(c0.match.away_team or ""),
            "home_win_probability": round(ph, 4),
            "draw_probability": round(pd, 4),
            "away_win_probability": round(pa, 4),
            "over_next_goal_probability": round(over_p, 4),
            "best_bet": best.get("best_bet"),
            "best_bet_odds": best.get("best_bet_odds"),
            "bet_kind": best.get("bet_kind"),
            "line": best.get("line"),
            "goals_needed_to_win": best.get("goals_needed_to_win"),
            "is_exotic": bool(best.get("is_exotic")),
            "is_corner": bool(best.get("is_corner")),
            "is_period": bool(best.get("is_period")),
            "source_market_type": best.get("source_market_type"),
            "source_market_label": best.get("source_market_label"),
            "source_selection": best.get("source_selection"),
            "source_section_name": best.get("source_section_name"),
            "source_subsection_name": best.get("source_subsection_name"),
            "implied_probability": round(implied, 4),
            "model_probability": round(model_p, 4),
            "value_edge": edge,
            "confidence_score": int(confidence),
            "usable_score": usable_score,
            "is_usable": bool(is_usable),
            "usable_blockers": usable_blockers,
            "api_intelligence_available": api_ok,
            "reasons": reasons[:10],
            "missing_data": missing,
            "risk_level": risk,
        }

    def _extract_1x2_odds(self, rows: list[ProviderSignalCandidate]) -> dict[str, float]:
        out: dict[str, float] = {}
        for c in rows:
            fam = self._family.get_market_family(c)
            mt = (c.market.market_type or "").strip().lower()
            if fam != "result" or mt not in {"1x2", "match_winner"}:
                continue
            side = self._selection_side(c.market.selection, c.match.home_team, c.match.away_team)
            odd = self._float(c.market.odds_value)
            if side in {"home", "draw", "away"} and odd:
                cur = out.get(side)
                if cur is None or odd < cur:
                    out[side] = odd
        return out

    def _base_1x2_probs(self, odds: dict[str, float], *, minute: int, sh: int, sa: int) -> tuple[float, float, float]:
        if all(k in odds for k in ("home", "draw", "away")):
            ih = 1.0 / max(odds["home"], 1e-6)
            ix = 1.0 / max(odds["draw"], 1e-6)
            ia = 1.0 / max(odds["away"], 1e-6)
            s = ih + ix + ia
            if s > 0:
                return ih / s, ix / s, ia / s
        # fallback if 1x2 snapshot is missing
        ph, pd, pa = 0.42, 0.30, 0.28
        if sh > sa:
            ph += 0.10
            pa -= 0.08
            pd -= 0.02
        elif sa > sh:
            pa += 0.10
            ph -= 0.08
            pd -= 0.02
        if minute >= 70:
            pd += 0.05
            ph -= 0.025
            pa -= 0.025
        return self._norm3(ph, pd, pa)

    def _apply_api_adjustments(self, ph: float, pd: float, pa: float, api: dict[str, Any]) -> tuple[float, float, float, list[str]]:
        if not api:
            return ph, pd, pa, ["api_missing_winline_fallback"]
        reasons: list[str] = []
        adj = 0.0
        st = api.get("standings_edge") if isinstance(api.get("standings_edge"), dict) else {}
        rank_edge = self._float(st.get("rank_edge_home_minus_away")) if st else None
        if rank_edge is not None:
            adj += max(-0.06, min(0.06, rank_edge * 0.01))
            reasons.append(f"standings_edge={rank_edge:g}")
        gf_h = self._float(api.get("avg_goals_for_home"))
        gf_a = self._float(api.get("avg_goals_for_away"))
        ga_h = self._float(api.get("avg_goals_against_home"))
        ga_a = self._float(api.get("avg_goals_against_away"))
        if gf_h is not None and gf_a is not None and ga_h is not None and ga_a is not None:
            form_edge = (gf_h - ga_h) - (gf_a - ga_a)
            adj += max(-0.05, min(0.05, form_edge * 0.03))
            reasons.append(f"form_edge={form_edge:.2f}")
        h2h_home = self._float(api.get("h2h_home_wins")) or 0.0
        h2h_away = self._float(api.get("h2h_away_wins")) or 0.0
        h2h_matches = self._float(api.get("h2h_matches")) or 0.0
        if h2h_matches > 0:
            h2h_edge = (h2h_home - h2h_away) / max(1.0, h2h_matches)
            adj += max(-0.03, min(0.03, h2h_edge * 0.10))
            reasons.append(f"h2h_edge={h2h_edge:.2f}")
        common = api.get("common_opponent_edge") if isinstance(api.get("common_opponent_edge"), dict) else {}
        cedge = self._float(common.get("edge_home_minus_away")) if common else None
        if cedge is not None:
            adj += max(-0.03, min(0.03, cedge * 0.05))
            reasons.append(f"common_edge={cedge:.2f}")
        ph += adj
        pa -= adj
        return (*self._norm3(ph, pd, pa), reasons)

    def _over_next_goal_probability(
        self,
        rows: list[ProviderSignalCandidate],
        *,
        minute: int,
        sh: int,
        sa: int,
        api: dict[str, Any],
    ) -> float:
        # baseline from live state
        total = sh + sa
        p = 0.56 if 35 <= minute <= 70 else 0.48
        if total <= 1:
            p += 0.06
        if minute > 80:
            p -= 0.10
        if minute < 15:
            p -= 0.06

        # if we have match-total-over candidate with need1, anchor by market implied
        for c in rows:
            ctx = self._fmt.describe_total_context(
                market_type=c.market.market_type,
                market_label=c.market.market_label,
                selection=c.market.selection,
                home_team=c.match.home_team,
                away_team=c.match.away_team,
                section_name=c.market.section_name,
                subsection_name=c.market.subsection_name,
            )
            if not ctx or ctx.total_side != "ТБ" or ctx.target_scope != "match" or ctx.period_scope != "match" or ctx.total_line is None:
                continue
            line = self._float(ctx.total_line)
            odd = self._float(c.market.odds_value)
            if line is None or odd is None:
                continue
            need = int(math.floor(line)) + 1 - total
            if need == 1:
                implied = max(0.0, min(1.0, 1.0 / max(odd, 1e-6)))
                p = 0.65 * p + 0.35 * implied
                break

        if api:
            conf = self._float(api.get("confidence_score")) or 0.0
            p += max(-0.04, min(0.04, (conf - 50.0) / 1000.0))
        return max(0.02, min(0.98, p))

    def _best_bet(self, rows: list[ProviderSignalCandidate], *, ph: float, pd: float, pa: float, over_p: float) -> dict[str, object]:
        best = {
            "best_bet": None,
            "best_bet_odds": None,
            "implied_probability": 0.0,
            "model_probability": 0.0,
            "edge": -9.0,
            "reasons": [],
            "bet_kind": None,
            "line": None,
            "goals_needed_to_win": None,
            "is_exotic": False,
            "is_corner": False,
            "is_period": False,
        }
        for c in rows:
            odd = self._float(c.market.odds_value)
            if odd is None or odd <= 1e-6:
                continue
            if odd < 1.20 or odd > 5.50:
                continue
            implied = 1.0 / odd
            fam = self._family.get_market_family(c)
            mt = (c.market.market_type or "").strip().lower()
            model_p = None
            reason = ""
            bet_kind = None
            line: float | None = None
            goals_needed: int | None = None
            is_exotic = False
            is_corner = False
            is_period = False
            if fam == "result" and mt in {"1x2", "match_winner"}:
                if self._is_exotic_result_like(c):
                    continue
                side = self._selection_side(c.market.selection, c.match.home_team, c.match.away_team)
                if side == "home":
                    model_p, reason = ph, "model_1x2_home"
                elif side == "away":
                    model_p, reason = pa, "model_1x2_away"
                elif side == "draw":
                    model_p, reason = pd, "model_1x2_draw"
                bet_kind = "ft_1x2"
            else:
                if self._is_corner_market(c):
                    continue
                ctx = self._fmt.describe_total_context(
                    market_type=c.market.market_type,
                    market_label=c.market.market_label,
                    selection=c.market.selection,
                    home_team=c.match.home_team,
                    away_team=c.match.away_team,
                    section_name=c.market.section_name,
                    subsection_name=c.market.subsection_name,
                )
                if ctx and ctx.total_side == "ТБ" and ctx.period_scope == "match":
                    line = self._float(ctx.total_line) if ctx.total_line is not None else None
                    is_period = ctx.period_scope != "match"
                    is_exotic = bool(getattr(ctx, "is_exotic_total", False))
                    if line is not None:
                        goals_needed = self._goals_needed_to_win(c, ctx, line)
                        if ctx.target_scope == "match":
                            lam_total = self._expected_remaining_goals(
                                minute=self._int(c.feature_snapshot_json.get("football_analytics", {}).get("minute"))
                                if isinstance(c.feature_snapshot_json, dict)
                                else None,
                                score_home=self._int(c.feature_snapshot_json.get("football_analytics", {}).get("score_home"))
                                if isinstance(c.feature_snapshot_json, dict)
                                else None,
                                score_away=self._int(c.feature_snapshot_json.get("football_analytics", {}).get("score_away"))
                                if isinstance(c.feature_snapshot_json, dict)
                                else None,
                                api=self._api_intelligence(c),
                            )
                            model_p = self._poisson_prob_at_least(goals_needed, lam_total)
                            model_p = 0.75 * model_p + 0.25 * over_p
                            reason = "model_match_total_over_poisson"
                            bet_kind = "match_total_over"
                        elif ctx.target_scope in {"home_team", "away_team", "team_total"}:
                            lam_total = self._expected_remaining_goals(
                                minute=self._int(c.feature_snapshot_json.get("football_analytics", {}).get("minute"))
                                if isinstance(c.feature_snapshot_json, dict)
                                else None,
                                score_home=self._int(c.feature_snapshot_json.get("football_analytics", {}).get("score_home"))
                                if isinstance(c.feature_snapshot_json, dict)
                                else None,
                                score_away=self._int(c.feature_snapshot_json.get("football_analytics", {}).get("score_away"))
                                if isinstance(c.feature_snapshot_json, dict)
                                else None,
                                api=self._api_intelligence(c),
                            )
                            home_share = max(0.15, min(0.85, ph / max(1e-6, ph + pa)))
                            side_share = home_share if ctx.target_scope == "home_team" else (1.0 - home_share)
                            lam_team = max(0.02, lam_total * side_share)
                            model_p = self._poisson_prob_at_least(goals_needed, lam_team)
                            model_p = 0.70 * model_p + 0.30 * over_p
                            reason = "model_team_total_over_poisson"
                            bet_kind = "team_total_over"
            if model_p is None:
                continue
            edge = model_p - implied
            if edge > float(best["edge"]):
                best = {
                    "best_bet": self._fmt.format_bet(
                        market_type=c.market.market_type,
                        market_label=c.market.market_label,
                        selection=c.market.selection,
                        home_team=c.match.home_team,
                        away_team=c.match.away_team,
                        section_name=c.market.section_name,
                        subsection_name=c.market.subsection_name,
                    ).main_label,
                    "best_bet_odds": str(c.market.odds_value) if c.market.odds_value is not None else None,
                    "implied_probability": implied,
                    "model_probability": model_p,
                    "edge": edge,
                    "reasons": [reason],
                    "bet_kind": bet_kind,
                    "line": line,
                    "goals_needed_to_win": goals_needed,
                    "is_exotic": bool(is_exotic),
                    "is_corner": bool(is_corner),
                    "is_period": bool(is_period),
                    "source_market_type": str(c.market.market_type or ""),
                    "source_market_label": str(c.market.market_label or ""),
                    "source_selection": str(c.market.selection or ""),
                    "source_section_name": c.market.section_name,
                    "source_subsection_name": c.market.subsection_name,
                }
        return best

    def _expected_remaining_goals(
        self,
        *,
        minute: int | None,
        score_home: int | None,
        score_away: int | None,
        api: dict[str, Any],
    ) -> float:
        m = minute if minute is not None else 45
        m = max(0, min(90, m))
        rem_ratio = max(0.05, (90 - m) / 90.0)
        base_total = 2.45
        gf_h = self._float(api.get("avg_goals_for_home")) if api else None
        gf_a = self._float(api.get("avg_goals_for_away")) if api else None
        ga_h = self._float(api.get("avg_goals_against_home")) if api else None
        ga_a = self._float(api.get("avg_goals_against_away")) if api else None
        if None not in (gf_h, gf_a, ga_h, ga_a):
            base_total = max(1.40, min(4.20, ((gf_h or 0) + (gf_a or 0) + (ga_h or 0) + (ga_a or 0)) / 2.0))
        total_now = (score_home or 0) + (score_away or 0)
        tempo = 1.0
        if total_now >= 3:
            tempo *= 0.90
        elif total_now == 0 and m >= 50:
            tempo *= 0.85
        elif total_now <= 1 and 35 <= m <= 70:
            tempo *= 1.07
        return max(0.05, min(3.5, base_total * rem_ratio * tempo))

    def _poisson_prob_at_least(self, goals_needed: int | None, lam: float) -> float:
        if goals_needed is None:
            return 0.5
        k = int(goals_needed)
        if k <= 0:
            return 0.99
        # P(X >= k) = 1 - sum_{i=0}^{k-1} e^-lam * lam^i / i!
        cdf = 0.0
        for i in range(0, max(0, k)):
            cdf += math.exp(-lam) * (lam**i) / math.factorial(i)
        return max(0.01, min(0.99, 1.0 - cdf))

    def _goals_needed_to_win(self, c: ProviderSignalCandidate, ctx: Any, line: float) -> int | None:
        minute, sh, sa = self._live_state(c)
        if minute is None or sh is None or sa is None:
            return None
        if ctx.target_scope == "match":
            base = sh + sa
        elif ctx.target_scope == "home_team":
            base = sh
        elif ctx.target_scope == "away_team":
            base = sa
        else:
            return None
        return int(math.floor(line)) + 1 - int(base)

    def _is_usable_idea(
        self,
        *,
        minute: int,
        risk: str,
        confidence: int,
        api_ok: bool,
        best: dict[str, object],
        competition_risk: str,
        total_goals: int,
        value_edge: float,
    ) -> tuple[bool, list[str]]:
        blockers: list[str] = []
        if value_edge < 0.05:
            blockers.append("value_edge_lt_0_05")
        if risk == "high":
            blockers.append("risk_high")
        if confidence < 55:
            blockers.append("confidence_lt_55")
        if competition_risk == "high":
            blockers.append("competition_high_risk")
        if minute > 75:
            blockers.append("late_gt_75")
        best_odds = self._float(best.get("best_bet_odds"))
        if best_odds is None or best_odds > 3.00:
            blockers.append("odds_gt_3_00_or_missing")
        if bool(best.get("is_corner")):
            blockers.append("corners_blocked")
        if bool(best.get("is_exotic")):
            blockers.append("exotic_blocked")
        if bool(best.get("is_period")):
            blockers.append("period_blocked")
        if total_goals >= 4:
            blockers.append("score_total_goals_4plus")
        kind = str(best.get("bet_kind") or "")
        line = self._float(best.get("line"))
        goals_needed = self._int(best.get("goals_needed_to_win"))
        if kind == "ft_1x2":
            if not api_ok:
                blockers.append("1x2_without_api_blocked")
        elif kind == "match_total_over":
            if goals_needed != 1:
                blockers.append("match_total_goals_needed_not_1")
            if minute < 35 or minute > 75:
                blockers.append("match_total_minute_window")
            if best_odds is None or best_odds < 1.35 or best_odds > 2.60:
                blockers.append("match_total_odds_window")
            if line is None or line > 3.5:
                blockers.append("match_total_line_gt_3_5")
        elif kind == "team_total_over":
            if goals_needed != 1:
                blockers.append("team_total_goals_needed_not_1")
            if minute < 35 or minute > 70:
                blockers.append("team_total_minute_window")
            if best_odds is None or best_odds < 1.45 or best_odds > 2.60:
                blockers.append("team_total_odds_window")
            if line is None or line > 2.5:
                blockers.append("team_total_line_gt_2_5")
        else:
            blockers.append("unsupported_kind")
        return (len(blockers) == 0), blockers

    def _is_corner_market(self, c: ProviderSignalCandidate) -> bool:
        text = " ".join(
            [
                str(c.market.market_type or ""),
                str(c.market.market_label or ""),
                str(c.market.section_name or ""),
                str(c.market.subsection_name or ""),
            ]
        ).lower()
        return "corner" in text or "углов" in text

    def _is_exotic_result_like(self, c: ProviderSignalCandidate) -> bool:
        text = " ".join(
            [
                str(c.market.market_type or ""),
                str(c.market.market_label or ""),
                str(c.market.section_name or ""),
                str(c.market.subsection_name or ""),
            ]
        ).lower()
        markers = ("handicap", "гандикап", "next goal", "следующ", "interval", "тайм", "remainder", "остат", "точный счет")
        return any(m in text for m in markers)

    def _confidence_score(
        self,
        *,
        api_ok: bool,
        odds_1x2: bool,
        minute: int,
        sh: int,
        sa: int,
        best_odds: float | None,
        best_kind: str,
        competition_risk: str,
    ) -> int:
        score = 25.0
        if api_ok:
            score += 30.0
        if odds_1x2:
            score += 15.0
        if minute is not None and sh is not None and sa is not None:
            score += 20.0
        if best_odds is not None and 1.35 <= best_odds <= 3.20:
            score += 10.0
        if best_kind in {"match_total_over", "team_total_over"}:
            score += 10.0
        if competition_risk == "high":
            score -= 22.0
        elif competition_risk == "medium":
            score -= 8.0
        return int(max(5.0, min(95.0, score)))

    def _risk_level(self, *, minute: int, odds: float | None, critical_missing: bool, competition_risk: str) -> str:
        if competition_risk == "high":
            return "high"
        if critical_missing:
            return "high"
        if minute >= 85:
            return "high"
        if odds is not None and odds > 3.40:
            return "high"
        if minute < 20 or minute > 75:
            return "medium"
        if competition_risk == "medium":
            return "medium"
        return "low"

    def _competition_risk(self, c: ProviderSignalCandidate) -> str:
        tokens = " ".join(
            [
                str(c.match.tournament_name or ""),
                str(c.match.match_name or ""),
                str(c.match.home_team or ""),
                str(c.match.away_team or ""),
            ]
        ).lower()
        high_markers = ("u19", "u20", "u21", "u23", "women", " жен", "(ж", "reserve", "reserves", "рез", "дубл", "e-football", "esoccer", "amateur", "люб.")
        if any(m in tokens for m in high_markers):
            return "high"
        med_markers = ("friendly", "товарищ", "cup", "кубок")
        if any(m in tokens for m in med_markers):
            return "medium"
        return "low"

    def _live_reasons(self, *, minute: int, sh: int, sa: int) -> list[str]:
        out = [f"minute={minute}", f"score={sh}:{sa}"]
        total = sh + sa
        if total <= 1:
            out.append("low_total_state")
        if minute >= 70:
            out.append("late_phase")
        return out

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

    def _fixture_id(self, c: ProviderSignalCandidate, api: dict[str, Any]) -> int | None:
        fid = self._int(api.get("fixture_id")) if api else None
        if fid:
            return fid
        fs = c.feature_snapshot_json if isinstance(c.feature_snapshot_json, dict) else {}
        ctx = fs.get("football_live_context_filter") if isinstance(fs.get("football_live_context_filter"), dict) else {}
        return self._int(ctx.get("fixture_id")) if ctx else None

    def _selection_side(self, selection: object, home_team: object, away_team: object) -> str | None:
        sel = str(selection or "").strip().lower().replace("х", "x").replace("ё", "е")
        if sel in {"1", "п1", "p1", "home"}:
            return "home"
        if sel in {"2", "п2", "p2", "away"}:
            return "away"
        if sel in {"x", "draw", "ничья", "н"}:
            return "draw"
        home = str(home_team or "").strip().lower().replace("ё", "е")
        away = str(away_team or "").strip().lower().replace("ё", "е")
        if home and (sel == home or home in sel):
            return "home"
        if away and (sel == away or away in sel):
            return "away"
        return None

    def _norm3(self, a: float, b: float, c: float) -> tuple[float, float, float]:
        a = max(0.01, a)
        b = max(0.01, b)
        c = max(0.01, c)
        s = a + b + c
        return a / s, b / s, c / s

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
