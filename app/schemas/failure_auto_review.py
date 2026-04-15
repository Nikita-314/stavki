from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field

from app.core.enums import FailureCategory
from app.schemas.analytics import (
    EntryRead,
    FailureReviewRead,
    PredictionLogRead,
    SettlementRead,
    SignalRead,
)


class FailureAutoReviewInput(BaseModel):
    signal: SignalRead
    prediction_logs: list[PredictionLogRead]
    entries: list[EntryRead]
    settlement: SettlementRead | None
    failure_reviews: list[FailureReviewRead] = Field(default_factory=list)


class FailureAutoReviewResult(BaseModel):
    category: FailureCategory
    auto_reason: str
    failure_tags_json: dict[str, Any]
    confidence_score: Decimal

