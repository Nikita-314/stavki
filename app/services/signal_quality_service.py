from __future__ import annotations

from decimal import Decimal

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import BetResult
from app.schemas.analytics import SignalAnalyticsReport
from app.schemas.signal_quality import SignalQualityMetrics, SignalQualityReport
from app.services.analytics_service import AnalyticsService


class SignalQualityService:
    async def build_signal_quality_report(self, session: AsyncSession, signal_id: int) -> SignalQualityReport:
        """Build on-demand quality report for a signal based on predicted vs actual outcome."""
        report = await AnalyticsService().get_signal_report(session, signal_id)
        metrics = self.build_signal_quality_metrics(report)

        return SignalQualityReport(
            signal_id=report.signal.id,
            match_name=report.signal.match_name,
            market_type=report.signal.market_type,
            selection=report.signal.selection,
            model_name=report.signal.model_name,
            model_version_name=report.signal.model_version_name,
            metrics=metrics,
        )

    def build_signal_quality_metrics(self, report: SignalAnalyticsReport) -> SignalQualityMetrics:
        """Compute quality metrics from an existing SignalAnalyticsReport."""
        predicted_prob = report.signal.predicted_prob
        implied_prob = report.signal.implied_prob
        edge = report.signal.edge

        actual_outcome: int | None
        if report.settlement is None:
            actual_outcome = None
        elif report.settlement.result == BetResult.WIN:
            actual_outcome = 1
        elif report.settlement.result == BetResult.LOSE:
            actual_outcome = 0
        else:
            actual_outcome = None

        prediction_error: Decimal | None
        if predicted_prob is not None and actual_outcome is not None:
            prediction_error = (predicted_prob - Decimal(actual_outcome)).copy_abs()
        else:
            prediction_error = None

        value_direction: str | None
        if predicted_prob is None or implied_prob is None:
            value_direction = None
        elif predicted_prob > implied_prob:
            value_direction = "positive"
        elif predicted_prob < implied_prob:
            value_direction = "negative"
        else:
            value_direction = "neutral"

        calibration_bucket: str | None
        if predicted_prob is None:
            calibration_bucket = None
        else:
            # 0.0-0.1, 0.1-0.2, ..., 0.9-1.0
            # clamp to [0, 1]
            p = predicted_prob
            if p < 0:
                p = Decimal("0")
            if p > 1:
                p = Decimal("1")
            idx = int((p * Decimal("10")).to_integral_value(rounding="ROUND_FLOOR"))
            if idx >= 10:
                idx = 9
            lo = Decimal(idx) / Decimal("10")
            hi = Decimal(idx + 1) / Decimal("10")
            calibration_bucket = f"{lo:.1f}-{hi:.1f}"

        is_overestimated: bool | None
        if predicted_prob is None or implied_prob is None or actual_outcome is None:
            is_overestimated = None
        else:
            is_overestimated = bool(predicted_prob > implied_prob and actual_outcome == 0)

        is_underestimated: bool | None
        if predicted_prob is None or implied_prob is None or actual_outcome is None:
            is_underestimated = None
        else:
            is_underestimated = bool(predicted_prob < implied_prob and actual_outcome == 1)

        quality_label: str
        if actual_outcome is None or predicted_prob is None:
            quality_label = "insufficient_data"
        elif implied_prob is None:
            quality_label = "market_aligned_win" if actual_outcome == 1 else "market_aligned_loss"
        elif predicted_prob > implied_prob:
            quality_label = "strong_value_win" if actual_outcome == 1 else "strong_value_loss"
        else:
            quality_label = "market_aligned_win" if actual_outcome == 1 else "market_aligned_loss"

        return SignalQualityMetrics(
            signal_id=report.signal.id,
            predicted_prob=predicted_prob,
            implied_prob=implied_prob,
            actual_outcome=actual_outcome,
            prediction_error=prediction_error,
            edge=edge,
            value_direction=value_direction,
            calibration_bucket=calibration_bucket,
            is_overestimated=is_overestimated,
            is_underestimated=is_underestimated,
            quality_label=quality_label,
        )

