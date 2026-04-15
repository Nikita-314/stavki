from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel


class SignalQualitySummaryItem(BaseModel):
    key: str
    total_signals: int
    with_outcome: int
    avg_prediction_error: Decimal | None
    overestimated_count: int
    underestimated_count: int
    strong_value_win_count: int
    strong_value_loss_count: int


class CalibrationBucketStat(BaseModel):
    bucket: str
    total_signals: int
    wins: int
    losses: int
    actual_win_rate: Decimal | None
    avg_prediction_error: Decimal | None


class SignalQualitySummaryReport(BaseModel):
    total_signals: int
    signals_with_outcome: int
    avg_prediction_error: Decimal | None
    overestimated_count: int
    underestimated_count: int
    by_sport: list[SignalQualitySummaryItem]
    by_bookmaker: list[SignalQualitySummaryItem]
    by_market_type: list[SignalQualitySummaryItem]
    by_model_name: list[SignalQualitySummaryItem]
    by_quality_label: list[SignalQualitySummaryItem]
    by_calibration_bucket: list[CalibrationBucketStat]

