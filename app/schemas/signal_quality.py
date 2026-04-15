from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel


class SignalQualityMetrics(BaseModel):
    signal_id: int
    predicted_prob: Decimal | None
    implied_prob: Decimal | None
    actual_outcome: int | None
    prediction_error: Decimal | None
    edge: Decimal | None
    value_direction: str | None
    calibration_bucket: str | None
    is_overestimated: bool | None
    is_underestimated: bool | None
    quality_label: str | None


class SignalQualityReport(BaseModel):
    signal_id: int
    match_name: str
    market_type: str
    selection: str
    model_name: str | None
    model_version_name: str | None
    metrics: SignalQualityMetrics

