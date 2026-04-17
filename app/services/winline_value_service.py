"""Demo rule-based value evaluation for Winline live-selected candidates.

Heuristic probabilities and EV — not a calibrated model. Layer sits on top of
`WinlineLiveSignalService` without changing selection logic.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field


class WinlineValueAssessment(BaseModel):
    event_external_id: str
    market_label: str
    selection: str
    odds_value: Decimal
    implied_prob: Decimal | None = None
    estimated_prob: Decimal | None = None
    edge: Decimal | None = None
    expected_value: Decimal | None = None
    is_value_bet: bool = False
    confidence_score: Decimal | None = None
    reason_codes: list[str] = Field(default_factory=list)
    raw_json: dict[str, Any] | None = None


class WinlineValueService:
    """Rule-based value / edge / EV for a single candidate after live selection."""

    _EDGE_OK = Decimal("0.05")
    _CONF_OK = Decimal("0.70")

    def calculate_edge(
        self,
        estimated_prob: Decimal | None,
        implied_prob: Decimal | None,
    ) -> Decimal | None:
        if estimated_prob is None or implied_prob is None:
            return None
        return estimated_prob - implied_prob

    def calculate_expected_value(
        self,
        estimated_prob: Decimal | None,
        odds_value: Decimal | None,
    ) -> Decimal | None:
        if estimated_prob is None or odds_value is None:
            return None
        return estimated_prob * odds_value - Decimal("1")

    def estimate_probability_from_live(
        self,
        evaluation: Any,
        features: Any,
        candidate: Any,
    ) -> Decimal | None:
        """Rough heuristic p̂; not a statistical model."""
        codes = set(getattr(evaluation, "reason_codes", None) or [])
        mk = getattr(candidate, "market_kind", None)
        sel = (getattr(candidate, "selection", None) or "").strip().upper()

        if mk != "MATCH_RESULT":
            return None

        # Football: late lead + red card on opponent → leading side
        if {"late_lead", "opponent_red_card"}.issubset(codes):
            if getattr(features, "is_home_leading", None) is True and sel == "HOME":
                return Decimal("0.86")
            if getattr(features, "is_away_leading", None) is True and sel == "AWAY":
                return Decimal("0.86")
            return None

        # Football: favorite in trouble
        if "favorite_collapsed" in codes:
            if sel == "AWAY":
                return Decimal("0.67")
            if sel == "DRAW":
                return Decimal("0.61")
            return None

        # CS2
        if codes & {"momentum_rounds", "win_streak"}:
            return Decimal("0.75")

        # Dota2 (and aliases used by live analysis)
        if codes & {
            "economy_advantage",
            "strong_advantage",
            "objective_control",
            "late_game_lock",
        }:
            return Decimal("0.77")

        return None

    def assess_candidate(
        self,
        *,
        evaluation: Any,
        features: Any,
        candidate: Any,
    ) -> WinlineValueAssessment:
        implied = getattr(candidate, "implied_prob", None)
        odds_value = getattr(candidate, "odds_value", None)
        if odds_value is None:
            odds_value = Decimal("1")

        est = self.estimate_probability_from_live(evaluation, features, candidate)
        edge = self.calculate_edge(est, implied)
        ev = self.calculate_expected_value(est, odds_value)

        live_conf = getattr(evaluation, "confidence_score", None)
        if live_conf is not None and not isinstance(live_conf, Decimal):
            try:
                live_conf = Decimal(str(live_conf))
            except Exception:
                live_conf = None

        tags: list[str] = []

        if est is None:
            tags.append("no_probability_estimate")
        if edge is None:
            pass
        elif edge >= self._EDGE_OK:
            tags.append("positive_edge")
        else:
            tags.append("weak_edge")

        if ev is None:
            tags.append("negative_ev")
        elif ev <= 0:
            tags.append("negative_ev")

        if live_conf is None or live_conf < self._CONF_OK:
            tags.append("low_confidence")

        value_ok = (
            est is not None
            and edge is not None
            and edge >= self._EDGE_OK
            and ev is not None
            and ev > 0
            and live_conf is not None
            and live_conf >= self._CONF_OK
        )

        return WinlineValueAssessment(
            event_external_id=str(getattr(candidate, "event_external_id", "")),
            market_label=str(getattr(candidate, "market_label", "")),
            selection=str(getattr(candidate, "selection", "")),
            odds_value=odds_value,
            implied_prob=implied,
            estimated_prob=est,
            edge=edge,
            expected_value=ev,
            is_value_bet=value_ok,
            confidence_score=live_conf,
            reason_codes=tags,
            raw_json={
                "live_reason_codes": list(getattr(evaluation, "reason_codes", []) or []),
                "heuristic": "rule_v0",
            },
        )

    def preview_value_demo(self) -> None:
        from app.services.winline_live_signal_service import WinlineLiveSignalService

        sig = WinlineLiveSignalService()
        for case_name, bundle in sig.build_live_demo_inputs().items():
            state = bundle["live_state"]
            feats = bundle["live_features"]
            ev = bundle["live_evaluation"]
            ar = bundle["line_adapter_result"]

            cands = sig.collect_live_candidates(ar.candidates, state.event_external_id)
            chosen = sig.choose_best_candidate(cands, ev, feats, state)

            print(f"=== VALUE CASE: {case_name} ===")
            if chosen is None:
                print("candidate: (none) — assessment skipped")
                print()
                continue

            a = self.assess_candidate(evaluation=ev, features=feats, candidate=chosen)

            print("candidate:")
            print(f"- market: {chosen.market_label}")
            print(f"- selection: {chosen.selection}")
            print(f"- odds: {chosen.odds_value}")
            print("probabilities:")
            ip = a.implied_prob
            print(f"- implied: {float(ip):.4f}" if ip is not None else "- implied: n/a")
            ep = a.estimated_prob
            print(f"- estimated: {float(ep):.4f}" if ep is not None else "- estimated: n/a")
            print("value:")
            print(f"- edge: {float(a.edge):.4f}" if a.edge is not None else "- edge: n/a")
            print(f"- ev: {float(a.expected_value):.4f}" if a.expected_value is not None else "- ev: n/a")
            print("decision:")
            print(f"- is_value_bet: {'yes' if a.is_value_bet else 'no'}")
            print(f"- reasons: {a.reason_codes}")
            print()


if __name__ == "__main__":
    WinlineValueService().preview_value_demo()
