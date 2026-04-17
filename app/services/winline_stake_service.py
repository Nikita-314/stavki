"""Demo stake sizing on top of Winline value assessment (flat / confidence / capped Kelly).

Not a bankroll or production staking engine — heuristic units only, no DB or runtime wiring.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field

from app.services.winline_value_service import WinlineValueAssessment, WinlineValueService


class WinlineStakeRecommendation(BaseModel):
    event_external_id: str
    market_label: str
    selection: str
    odds_value: Decimal
    implied_prob: Decimal | None = None
    estimated_prob: Decimal | None = None
    edge: Decimal | None = None
    expected_value: Decimal | None = None
    confidence_score: Decimal | None = None
    is_value_bet: bool = False
    recommended_stake_units: Decimal | None = None
    recommended_stake_fraction: Decimal | None = None
    sizing_method: str | None = None
    reason_codes: list[str] = Field(default_factory=list)
    raw_json: dict[str, Any] | None = None


_KELLY_CAP = Decimal("0.05")


class WinlineStakeService:
    """Rule-based stake units / Kelly fraction demo layer above `WinlineValueAssessment`."""

    def calculate_flat_stake(self, is_value_bet: bool) -> Decimal:
        if not is_value_bet:
            return Decimal("0")
        return Decimal("1.0")

    def calculate_confidence_adjusted_stake(
        self,
        confidence_score: Decimal | None,
        edge: Decimal | None,
        is_value_bet: bool,
    ) -> Decimal:
        if not is_value_bet:
            return Decimal("0")
        conf = confidence_score if confidence_score is not None else Decimal("0")
        e = edge if edge is not None else Decimal("0")
        if conf >= Decimal("0.85") and e >= Decimal("0.12"):
            return Decimal("1.50")
        if conf >= Decimal("0.80") and e >= Decimal("0.08"):
            return Decimal("1.25")
        if conf >= Decimal("0.70") and e >= Decimal("0.05"):
            return Decimal("1.00")
        return Decimal("0.50")

    def _kelly_fraction_uncapped(
        self,
        estimated_prob: Decimal | None,
        odds_value: Decimal | None,
    ) -> Decimal | None:
        if estimated_prob is None or odds_value is None:
            return None
        if odds_value <= Decimal("1"):
            return None
        p = estimated_prob
        q = Decimal("1") - p
        b = odds_value - Decimal("1")
        if b <= 0:
            return None
        kelly = (b * p - q) / b
        return kelly

    def calculate_capped_kelly_fraction(
        self,
        estimated_prob: Decimal | None,
        odds_value: Decimal | None,
    ) -> Decimal | None:
        """Heuristic Kelly fraction with max 5% — not a full bankroll Kelly implementation."""
        raw = self._kelly_fraction_uncapped(estimated_prob, odds_value)
        if raw is None:
            return None
        if raw <= 0:
            return Decimal("0")
        capped = min(raw, _KELLY_CAP)
        return capped.quantize(Decimal("0.0001"))

    def build_stake_recommendation(self, assessment: WinlineValueAssessment) -> WinlineStakeRecommendation:
        flat_u = self.calculate_flat_stake(assessment.is_value_bet)
        conf_u = self.calculate_confidence_adjusted_stake(
            assessment.confidence_score,
            assessment.edge,
            assessment.is_value_bet,
        )
        raw_kelly = self._kelly_fraction_uncapped(
            assessment.estimated_prob,
            assessment.odds_value,
        )
        kelly_frac = self.calculate_capped_kelly_fraction(
            assessment.estimated_prob,
            assessment.odds_value,
        )

        reasons: list[str] = []
        sizing_method: str | None = None
        rec_units: Decimal | None = None
        rec_fraction: Decimal | None = None

        extra_raw: dict[str, Any] = {
            "flat_stake_units": str(flat_u),
            "confidence_adjusted_units": str(conf_u),
            "kelly_fraction_raw": str(raw_kelly) if raw_kelly is not None else None,
            "kelly_fraction_capped": str(kelly_frac) if kelly_frac is not None else None,
            "heuristic": "stake_rule_v0",
        }

        if not assessment.is_value_bet:
            sizing_method = "no_bet"
            rec_units = Decimal("0")
            reasons.extend(["no_bet"])
        else:
            sizing_method = "confidence_adjusted"
            rec_units = conf_u
            reasons.extend(["confidence_adjusted", "flat_only"])

            if kelly_frac is not None:
                rec_fraction = kelly_frac
                if kelly_frac == Decimal("0"):
                    reasons.append("kelly_zero")
                else:
                    reasons.append("kelly_capped")
                    if raw_kelly is not None and raw_kelly > _KELLY_CAP:
                        reasons.append("kelly_hit_cap")

        return WinlineStakeRecommendation(
            event_external_id=assessment.event_external_id,
            market_label=assessment.market_label,
            selection=assessment.selection,
            odds_value=assessment.odds_value,
            implied_prob=assessment.implied_prob,
            estimated_prob=assessment.estimated_prob,
            edge=assessment.edge,
            expected_value=assessment.expected_value,
            confidence_score=assessment.confidence_score,
            is_value_bet=assessment.is_value_bet,
            recommended_stake_units=rec_units,
            recommended_stake_fraction=rec_fraction,
            sizing_method=sizing_method,
            reason_codes=reasons,
            raw_json=extra_raw,
        )

    def preview_stake_demo(self) -> None:
        from app.services.winline_live_signal_service import WinlineLiveSignalService

        sig = WinlineLiveSignalService()
        val = WinlineValueService()

        for case_name, bundle in sig.build_live_demo_inputs().items():
            state = bundle["live_state"]
            feats = bundle["live_features"]
            ev = bundle["live_evaluation"]
            ar = bundle["line_adapter_result"]

            cands = sig.collect_live_candidates(ar.candidates, state.event_external_id)
            chosen = sig.choose_best_candidate(cands, ev, feats, state)

            print(f"=== STAKE CASE: {case_name} ===")
            if chosen is None:
                print("candidate: (none) — assessment skipped")
                print()
                continue

            a = val.assess_candidate(evaluation=ev, features=feats, candidate=chosen)
            rec = self.build_stake_recommendation(a)

            print("candidate:")
            print(f"- market: {chosen.market_label}")
            print(f"- selection: {chosen.selection}")
            print(f"- odds: {chosen.odds_value}")
            print("value:")
            ip = a.implied_prob
            print(f"- implied: {float(ip):.4f}" if ip is not None else "- implied: n/a")
            ep = a.estimated_prob
            print(f"- estimated: {float(ep):.4f}" if ep is not None else "- estimated: n/a")
            print(f"- edge: {float(a.edge):.4f}" if a.edge is not None else "- edge: n/a")
            print(f"- ev: {float(a.expected_value):.4f}" if a.expected_value is not None else "- ev: n/a")
            print("stake:")
            print(f"- is_value_bet: {'yes' if a.is_value_bet else 'no'}")
            print(f"- sizing_method: {rec.sizing_method}")
            print(
                f"- recommended_stake_units: {float(rec.recommended_stake_units):.2f}"
                if rec.recommended_stake_units is not None
                else "- recommended_stake_units: n/a"
            )
            kf = rec.recommended_stake_fraction
            print(
                f"- kelly_fraction: {float(kf):.4f}"
                if kf is not None
                else "- kelly_fraction: n/a"
            )
            print(f"- reasons: {rec.reason_codes}")
            print()


if __name__ == "__main__":
    WinlineStakeService().preview_stake_demo()
