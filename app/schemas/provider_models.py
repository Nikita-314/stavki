from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field, field_validator

from app.core.enums import BookmakerType, SportType


class ProviderMatch(BaseModel):
    external_event_id: str
    sport: SportType
    tournament_name: str
    match_name: str
    home_team: str
    away_team: str
    event_start_at: datetime | None = None
    is_live: bool = False
    source_name: str


class ProviderOddsMarket(BaseModel):
    bookmaker: BookmakerType
    market_type: str
    market_label: str
    selection: str
    odds_value: Decimal
    section_name: str | None = None
    subsection_name: str | None = None
    search_hint: str | None = None

    @field_validator("odds_value")
    @classmethod
    def _odds_gt_one(cls, v: Decimal) -> Decimal:
        if v <= 1:
            raise ValueError("odds_value must be > 1")
        return v


class ProviderSignalCandidate(BaseModel):
    match: ProviderMatch
    market: ProviderOddsMarket

    min_entry_odds: Decimal
    predicted_prob: Decimal | None = None
    implied_prob: Decimal | None = None
    edge: Decimal | None = None
    model_name: str | None = None
    model_version_name: str | None = None
    signal_score: Decimal | None = None
    notes: str | None = None

    feature_snapshot_json: dict[str, Any] = Field(default_factory=dict)
    raw_model_output_json: dict[str, Any] | None = None
    explanation_json: dict[str, Any] | None = None


class ProviderBatchIngestResult(BaseModel):
    total_candidates: int
    created_signals: int
    skipped_candidates: int
    created_signal_ids: list[int]

