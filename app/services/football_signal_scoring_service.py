from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any

from app.schemas.provider_models import ProviderSignalCandidate


@dataclass
class FootballSignalScoreBreakdown:
    base_score: float
    market_score: float
    timing_score: float
    live_score: float
    confidence_score: float
    learning_factor: float
    final_score: float
    reason_codes: list[str]

    def as_dict(self) -> dict[str, Any]:
        d = asdict(self)
        return d


class FootballSignalScoringService:
    """Transparent rule-based score; no ML claims."""

    @staticmethod
    def humanize_reason_codes(codes: list[str]) -> list[str]:
        """Map internal reason_codes to short Russian lines for Telegram (no fake reasons)."""
        mapping = {
            "core_market_family": "• сильный базовый рынок",
            "combo_market_family": "• комбинированный рынок",
            "non_core_market_penalty": "• нишевый рынок (пониженный приоритет)",
            "corners_cards_like_penalty": "• углы / карточки / спецрынок",
            "near_prematch_bonus": "• матч скоро начнётся",
            "mid_horizon_neutral": "• средний горизонт до старта",
            "far_prematch_penalty": "• далеко до старта",
            "too_far_strong_penalty": "• слишком далеко по времени",
            "timing_unknown": "• время старта неизвестно",
            "prematch_started_or_clock_skew": "• старт/часы: возможное расхождение",
            "live_bonus": "• приоритет LIVE",
            "red_card_home_observed": "• зафиксирована красная карточка (хозяева)",
            "red_card_away_observed": "• зафиксирована красная карточка (гости)",
            "live_strength_signal_present": "• live-сила по счёту (ограниченная эвристика)",
            "model_prob_and_edge_present": "• есть оценка модели и edge",
            "implied_prob_only": "• оценка вероятности по коэффициенту",
        }
        lines: list[str] = []
        for code in codes or []:
            if code.startswith("learning_factor:"):
                lines.append("• корректировка по истории сигналов")
                continue
            lines.append(mapping.get(code, f"• {code}"))
        return lines

    def score(
        self,
        *,
        candidate: ProviderSignalCandidate,
        analytics: dict[str, Any],
        market_family: str,
        learning_factor: float = 1.0,
    ) -> FootballSignalScoreBreakdown:
        reasons: list[str] = []
        base = 50.0

        market_score = self._market_component(market_family, analytics, reasons)
        timing_score = self._timing_component(analytics, reasons)
        live_score = self._live_component(analytics, reasons)
        confidence = self._confidence_component(candidate, analytics, reasons)

        raw = base + market_score + timing_score + live_score + confidence
        clamped_pre = max(0.0, min(100.0, raw))
        lf = max(0.85, min(1.15, float(learning_factor)))
        final = max(0.0, min(100.0, clamped_pre * lf))
        if lf != 1.0:
            reasons.append(f"learning_factor:{lf:.3f}")

        return FootballSignalScoreBreakdown(
            base_score=round(base, 4),
            market_score=round(market_score, 4),
            timing_score=round(timing_score, 4),
            live_score=round(live_score, 4),
            confidence_score=round(confidence, 4),
            learning_factor=round(lf, 6),
            final_score=round(final, 4),
            reason_codes=reasons,
        )

    def _market_component(self, family: str, analytics: dict[str, Any], reasons: list[str]) -> float:
        score = 0.0
        if family in {"result", "totals", "btts", "handicap", "double_chance"}:
            score += 12.0
            reasons.append("core_market_family")
        elif family == "combo":
            score += 4.0
            reasons.append("combo_market_family")
        else:
            score -= 18.0
            reasons.append("non_core_market_penalty")

        if analytics.get("corner_or_cards_like_market"):
            score -= 10.0
            reasons.append("corners_cards_like_penalty")
        return score

    def _timing_component(self, analytics: dict[str, Any], reasons: list[str]) -> float:
        if analytics.get("is_live"):
            return 0.0
        h = analytics.get("hours_to_start")
        if h is None:
            reasons.append("timing_unknown")
            return 0.0
        if h < 0:
            reasons.append("prematch_started_or_clock_skew")
            return -5.0
        if h <= 6:
            reasons.append("near_prematch_bonus")
            return 8.0
        if h <= 18:
            reasons.append("mid_horizon_neutral")
            return 2.0
        if h <= 24:
            reasons.append("far_prematch_penalty")
            return -6.0
        reasons.append("too_far_strong_penalty")
        return -22.0

    def _live_component(self, analytics: dict[str, Any], reasons: list[str]) -> float:
        score = 0.0
        if analytics.get("is_live"):
            score += 6.0
            reasons.append("live_bonus")
        rh = analytics.get("red_cards_home")
        ra = analytics.get("red_cards_away")
        if isinstance(rh, int) and rh > 0:
            score += min(4.0, 2.0 * rh)
            reasons.append("red_card_home_observed")
        if isinstance(ra, int) and ra > 0:
            score += min(4.0, 2.0 * ra)
            reasons.append("red_card_away_observed")
        if analytics.get("current_side_strength_signal"):
            score += 3.0
            reasons.append("live_strength_signal_present")
        return score

    def _confidence_component(self, candidate: ProviderSignalCandidate, analytics: dict[str, Any], reasons: list[str]) -> float:
        score = 0.0
        if candidate.predicted_prob is not None and candidate.edge is not None:
            score += 4.0
            reasons.append("model_prob_and_edge_present")
        elif analytics.get("implied_prob") is not None:
            score += 1.0
            reasons.append("implied_prob_only")
        return score

    def to_signal_score_decimal(self, breakdown: FootballSignalScoreBreakdown) -> Decimal:
        return Decimal(str(breakdown.final_score)).quantize(Decimal("0.0001"))
