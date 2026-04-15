from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, field_validator

from app.core.enums import EntryStatus


class EntryCreate(BaseModel):
    signal_id: int
    status: EntryStatus = EntryStatus.PENDING

    entered_odds: Decimal | None = None
    stake_amount: Decimal | None = None
    entered_at: datetime | None = None

    is_manual: bool = True
    was_found_in_bookmaker: bool | None = None
    missed_reason: str | None = None
    delay_seconds: int | None = None
    notes: str | None = None

    @field_validator("entered_odds")
    @classmethod
    def _entered_odds_gt_one(cls, v: Decimal | None) -> Decimal | None:
        if v is None:
            return v
        if v <= 1:
            raise ValueError("entered_odds must be > 1")
        return v

    @field_validator("stake_amount")
    @classmethod
    def _stake_amount_positive(cls, v: Decimal | None) -> Decimal | None:
        if v is None:
            return v
        if v <= 0:
            raise ValueError("stake_amount must be > 0")
        return v

    @field_validator("delay_seconds")
    @classmethod
    def _delay_seconds_non_negative(cls, v: int | None) -> int | None:
        if v is None:
            return v
        if v < 0:
            raise ValueError("delay_seconds must be >= 0")
        return v

