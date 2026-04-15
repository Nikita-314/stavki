from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from app.core.enums import BookmakerType, SportType


class SignalCreate(BaseModel):
    sport: SportType
    bookmaker: BookmakerType

    event_external_id: str | None = None
    tournament_name: str
    match_name: str
    home_team: str
    away_team: str

    market_type: str
    market_label: str
    selection: str

    odds_at_signal: Decimal
    min_entry_odds: Decimal

    predicted_prob: Decimal | None = None
    implied_prob: Decimal | None = None
    edge: Decimal | None = None

    model_name: str | None = None
    model_version_name: str | None = None
    signal_score: Decimal | None = None

    section_name: str | None = None
    subsection_name: str | None = None
    search_hint: str | None = None

    is_live: bool = False
    event_start_at: datetime | None = None
    notes: str | None = None

    @field_validator("odds_at_signal", "min_entry_odds")
    @classmethod
    def _odds_gt_one(cls, v: Decimal) -> Decimal:
        if v <= 1:
            raise ValueError("odds must be > 1")
        return v

    @field_validator("predicted_prob", "implied_prob")
    @classmethod
    def _prob_in_unit_interval(cls, v: Decimal | None) -> Decimal | None:
        if v is None:
            return v
        if v < 0 or v > 1:
            raise ValueError("probability must be within 0..1")
        return v

    @field_validator("edge")
    @classmethod
    def _edge_reasonable(cls, v: Decimal | None) -> Decimal | None:
        if v is None:
            return v
        if v < Decimal("-1") or v > Decimal("10"):
            raise ValueError("edge must be within -1..10")
        return v


class PredictionLogCreate(BaseModel):
    feature_snapshot_json: dict[str, Any] = Field(default_factory=dict)
    raw_model_output_json: dict[str, Any] | None = None
    explanation_json: dict[str, Any] | None = None


class SignalCreateBundle(BaseModel):
    signal: SignalCreate
    prediction_log: PredictionLogCreate

    @model_validator(mode="after")
    def _model_version_todo(self) -> "SignalCreateBundle":
        # TODO: later verify provided model_version_name against ModelVersion in DB
        return self

