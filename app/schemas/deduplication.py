from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from app.core.enums import BookmakerType, SportType
from app.schemas.provider_models import ProviderSignalCandidate


class SignalDeduplicationKey(BaseModel):
    sport: SportType
    bookmaker: BookmakerType
    event_external_id: str | None
    home_team: str
    away_team: str
    market_type: str
    selection: str
    is_live: bool
    event_start_at: datetime | None


class DeduplicationDecision(BaseModel):
    is_duplicate: bool
    reason: str
    key: SignalDeduplicationKey


class DeduplicationBatchResult(BaseModel):
    unique_candidates: list[ProviderSignalCandidate]
    duplicate_count: int
    unique_count: int
    duplicate_reasons: dict[str, int]

