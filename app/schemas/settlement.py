from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, field_validator

from app.core.enums import BetResult


class SettlementCreate(BaseModel):
    signal_id: int
    result: BetResult = BetResult.UNKNOWN
    profit_loss: Decimal = Decimal("0")

    settled_at: datetime | None = None
    result_details: str | None = None
    bankroll_before: Decimal | None = None
    bankroll_after: Decimal | None = None

    @field_validator("bankroll_before", "bankroll_after")
    @classmethod
    def _bankroll_non_negative(cls, v: Decimal | None) -> Decimal | None:
        if v is None:
            return v
        if v < 0:
            raise ValueError("bankroll values must be non-negative")
        return v

