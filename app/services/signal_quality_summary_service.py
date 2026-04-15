from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.analytics_summary import AnalyticsFilter
from app.schemas.signal_quality_summary import (
    CalibrationBucketStat,
    SignalQualitySummaryItem,
    SignalQualitySummaryReport,
)
from app.services.training_dataset_service import TrainingDatasetService


@dataclass
class _GroupAgg:
    total: int = 0
    with_outcome: int = 0
    error_sum: Decimal = Decimal("0")
    error_count: int = 0
    overestimated: int = 0
    underestimated: int = 0
    strong_value_win: int = 0
    strong_value_loss: int = 0


@dataclass
class _BucketAgg:
    total: int = 0
    wins: int = 0
    losses: int = 0
    error_sum: Decimal = Decimal("0")
    error_count: int = 0


class SignalQualitySummaryService:
    async def build_quality_summary(
        self, session: AsyncSession, filters: AnalyticsFilter | None = None
    ) -> SignalQualitySummaryReport:
        """Build aggregated on-demand quality summary based on TrainingDatasetService rows."""
        dataset = await TrainingDatasetService().build_dataset(session, filters)
        rows = dataset.rows

        total_signals = len(rows)

        # Overall accumulators
        signals_with_outcome = 0
        overall_error_sum = Decimal("0")
        overall_error_count = 0
        overestimated_count = 0
        underestimated_count = 0

        # Group accumulators
        by_sport: dict[str, _GroupAgg] = {}
        by_bookmaker: dict[str, _GroupAgg] = {}
        by_market_type: dict[str, _GroupAgg] = {}
        by_model_name: dict[str, _GroupAgg] = {}
        by_quality_label: dict[str, _GroupAgg] = {}

        # Calibration bucket accumulators
        by_bucket: dict[str, _BucketAgg] = {}

        for r in rows:
            actual_outcome = r.target_outcome_success  # 1/0/None
            predicted_prob = r.predicted_prob
            implied_prob = r.implied_prob

            prediction_error: Decimal | None
            if predicted_prob is not None and actual_outcome is not None:
                prediction_error = (predicted_prob - Decimal(actual_outcome)).copy_abs()
            else:
                prediction_error = None

            if actual_outcome is not None:
                signals_with_outcome += 1

            if prediction_error is not None:
                overall_error_sum += prediction_error
                overall_error_count += 1

            overestimated = None
            underestimated = None
            if predicted_prob is not None and implied_prob is not None and actual_outcome is not None:
                overestimated = bool(predicted_prob > implied_prob and actual_outcome == 0)
                underestimated = bool(predicted_prob < implied_prob and actual_outcome == 1)

            if overestimated is True:
                overestimated_count += 1
            if underestimated is True:
                underestimated_count += 1

            quality_label = self._quality_label(
                predicted_prob=predicted_prob,
                implied_prob=implied_prob,
                actual_outcome=actual_outcome,
            )

            bucket = self._calibration_bucket(predicted_prob)

            # Update groups
            self._update_group(
                by_sport,
                key=str(r.sport),
                actual_outcome=actual_outcome,
                prediction_error=prediction_error,
                overestimated=overestimated,
                underestimated=underestimated,
                quality_label=quality_label,
            )
            self._update_group(
                by_bookmaker,
                key=str(r.bookmaker),
                actual_outcome=actual_outcome,
                prediction_error=prediction_error,
                overestimated=overestimated,
                underestimated=underestimated,
                quality_label=quality_label,
            )
            self._update_group(
                by_market_type,
                key=r.market_type or "UNKNOWN",
                actual_outcome=actual_outcome,
                prediction_error=prediction_error,
                overestimated=overestimated,
                underestimated=underestimated,
                quality_label=quality_label,
            )
            self._update_group(
                by_model_name,
                key=(r.model_name or "UNKNOWN"),
                actual_outcome=actual_outcome,
                prediction_error=prediction_error,
                overestimated=overestimated,
                underestimated=underestimated,
                quality_label=quality_label,
            )
            self._update_group(
                by_quality_label,
                key=quality_label,
                actual_outcome=actual_outcome,
                prediction_error=prediction_error,
                overestimated=overestimated,
                underestimated=underestimated,
                quality_label=quality_label,
            )

            # Calibration bucket stats
            if bucket is not None:
                b = by_bucket.setdefault(bucket, _BucketAgg())
                b.total += 1
                if actual_outcome == 1:
                    b.wins += 1
                elif actual_outcome == 0:
                    b.losses += 1
                if prediction_error is not None:
                    b.error_sum += prediction_error
                    b.error_count += 1

        avg_prediction_error = (
            (overall_error_sum / Decimal(overall_error_count)) if overall_error_count > 0 else None
        )

        return SignalQualitySummaryReport(
            total_signals=total_signals,
            signals_with_outcome=signals_with_outcome,
            avg_prediction_error=avg_prediction_error,
            overestimated_count=overestimated_count,
            underestimated_count=underestimated_count,
            by_sport=self._to_items(by_sport),
            by_bookmaker=self._to_items(by_bookmaker),
            by_market_type=self._to_items(by_market_type),
            by_model_name=self._to_items(by_model_name),
            by_quality_label=self._to_items(by_quality_label),
            by_calibration_bucket=self._to_bucket_items(by_bucket),
        )

    def _quality_label(
        self,
        *,
        predicted_prob: Decimal | None,
        implied_prob: Decimal | None,
        actual_outcome: int | None,
    ) -> str:
        if actual_outcome is None or predicted_prob is None:
            return "insufficient_data"
        if implied_prob is None:
            return "market_aligned_win" if actual_outcome == 1 else "market_aligned_loss"
        if predicted_prob > implied_prob:
            return "strong_value_win" if actual_outcome == 1 else "strong_value_loss"
        return "market_aligned_win" if actual_outcome == 1 else "market_aligned_loss"

    def _calibration_bucket(self, predicted_prob: Decimal | None) -> str | None:
        if predicted_prob is None:
            return None
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
        return f"{lo:.1f}-{hi:.1f}"

    def _update_group(
        self,
        groups: dict[str, _GroupAgg],
        *,
        key: str,
        actual_outcome: int | None,
        prediction_error: Decimal | None,
        overestimated: bool | None,
        underestimated: bool | None,
        quality_label: str,
    ) -> None:
        g = groups.setdefault(key, _GroupAgg())
        g.total += 1

        if actual_outcome is not None:
            g.with_outcome += 1

        if prediction_error is not None:
            g.error_sum += prediction_error
            g.error_count += 1

        if overestimated is True:
            g.overestimated += 1
        if underestimated is True:
            g.underestimated += 1

        if quality_label == "strong_value_win":
            g.strong_value_win += 1
        elif quality_label == "strong_value_loss":
            g.strong_value_loss += 1

    def _to_items(self, groups: dict[str, _GroupAgg]) -> list[SignalQualitySummaryItem]:
        items: list[SignalQualitySummaryItem] = []
        for key, g in groups.items():
            avg_err = (g.error_sum / Decimal(g.error_count)) if g.error_count > 0 else None
            items.append(
                SignalQualitySummaryItem(
                    key=key,
                    total_signals=g.total,
                    with_outcome=g.with_outcome,
                    avg_prediction_error=avg_err,
                    overestimated_count=g.overestimated,
                    underestimated_count=g.underestimated,
                    strong_value_win_count=g.strong_value_win,
                    strong_value_loss_count=g.strong_value_loss,
                )
            )
        items.sort(key=lambda x: x.total_signals, reverse=True)
        return items

    def _to_bucket_items(self, buckets: dict[str, _BucketAgg]) -> list[CalibrationBucketStat]:
        items: list[CalibrationBucketStat] = []
        for bucket, b in buckets.items():
            denom = b.wins + b.losses
            actual_win_rate = (Decimal(b.wins) / Decimal(denom)) if denom > 0 else None
            avg_err = (b.error_sum / Decimal(b.error_count)) if b.error_count > 0 else None
            items.append(
                CalibrationBucketStat(
                    bucket=bucket,
                    total_signals=b.total,
                    wins=b.wins,
                    losses=b.losses,
                    actual_win_rate=actual_win_rate,
                    avg_prediction_error=avg_err,
                )
            )
        items.sort(key=lambda x: x.total_signals, reverse=True)
        return items

