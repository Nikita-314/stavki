from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel

from app.core.enums import (
    BetResult,
    BookmakerType,
    FailureCategory,
    SignalStatus,
    SportType,
)


class TrainingDatasetRow(BaseModel):
    signal_id: int
    sport: SportType
    bookmaker: BookmakerType
    market_type: str
    is_live: bool
    model_name: str | None
    model_version_name: str | None
    signal_status: SignalStatus
    signal_created_at: datetime
    event_start_at: datetime | None

    odds_at_signal: Decimal
    min_entry_odds: Decimal
    predicted_prob: Decimal | None
    implied_prob: Decimal | None
    edge: Decimal | None
    signal_score: Decimal | None

    entered: bool
    entered_odds: Decimal | None
    stake_amount: Decimal | None
    entry_delay_seconds: int | None
    was_found_in_bookmaker: bool | None
    missed_reason: str | None

    settled: bool
    settlement_result: BetResult | None
    profit_loss: Decimal | None
    bankroll_before: Decimal | None
    bankroll_after: Decimal | None

    auto_failure_category: FailureCategory | None
    auto_failure_reason: str | None
    manual_failure_reason: str | None
    failure_tags_json: dict[str, Any] | None

    feature_snapshot_json: dict[str, Any] | None
    raw_model_output_json: dict[str, Any] | None
    explanation_json: dict[str, Any] | None

    target_outcome_success: int | None
    target_entry_success: int | None
    target_is_value_kept: int | None


class TrainingDatasetBuildResult(BaseModel):
    rows: list[TrainingDatasetRow]
    total_rows: int

