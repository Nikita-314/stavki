from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from app.core.enums import SportType


class EventResultInput(BaseModel):
    event_external_id: str
    sport: SportType
    winner_selection: str | None = None
    is_void: bool = False
    settled_at: datetime | None = None
    result_payload_json: dict[str, Any] | None = None


class EventResultProcessingResult(BaseModel):
    total_signals_found: int
    settled_signals: int
    skipped_signals: int
    created_failure_reviews: int
    processed_signal_ids: list[int] = Field(default_factory=list)

