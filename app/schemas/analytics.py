from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict

from app.core.enums import BetResult, BookmakerType, EntryStatus, FailureCategory, SignalStatus, SportType


class SignalRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    created_at: datetime
    updated_at: datetime

    sport: SportType
    bookmaker: BookmakerType

    event_external_id: str | None
    tournament_name: str
    match_name: str
    home_team: str
    away_team: str

    market_type: str
    market_label: str
    selection: str

    odds_at_signal: Decimal
    min_entry_odds: Decimal

    predicted_prob: Decimal | None
    implied_prob: Decimal | None
    edge: Decimal | None

    model_name: str | None
    model_version_name: str | None
    signal_score: Decimal | None

    status: SignalStatus

    section_name: str | None
    subsection_name: str | None
    search_hint: str | None

    is_live: bool
    event_start_at: datetime | None
    signaled_at: datetime
    notes: str | None


class PredictionLogRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    signal_id: int
    feature_snapshot_json: dict[str, Any]
    raw_model_output_json: dict[str, Any] | None
    explanation_json: dict[str, Any] | None
    created_at: datetime


class EntryRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    signal_id: int
    created_at: datetime
    updated_at: datetime

    status: EntryStatus
    entered_odds: Decimal | None
    stake_amount: Decimal | None
    entered_at: datetime | None
    is_manual: bool
    was_found_in_bookmaker: bool | None
    missed_reason: str | None
    delay_seconds: int | None
    notes: str | None


class SettlementRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    signal_id: int
    created_at: datetime
    updated_at: datetime

    result: BetResult
    profit_loss: Decimal
    settled_at: datetime | None
    result_details: str | None
    bankroll_before: Decimal | None
    bankroll_after: Decimal | None


class FailureReviewRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    signal_id: int
    created_at: datetime
    updated_at: datetime

    category: FailureCategory
    auto_reason: str | None
    manual_reason: str | None
    failure_tags_json: dict[str, Any] | None
    notes: str | None
    reviewed_at: datetime | None


class SignalAnalyticsReport(BaseModel):
    signal: SignalRead
    prediction_logs: list[PredictionLogRead]
    entries: list[EntryRead]
    settlement: SettlementRead | None
    failure_reviews: list[FailureReviewRead]

