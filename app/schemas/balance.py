from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, field_validator


class BalanceSnapshotCreate(BaseModel):
    base_amount: Decimal
    label: str | None = None

    @field_validator("base_amount")
    @classmethod
    def _non_negative(cls, v: Decimal) -> Decimal:
        if v < 0:
            raise ValueError("base_amount must be non-negative")
        return v


class BalanceOverview(BaseModel):
    base_amount: Decimal
    base_snapshot_at: datetime | None
    base_label: str | None
    total_profit_loss_since_base: Decimal
    current_balance: Decimal
    settled_signals_count: int
    wins: int
    losses: int
    voids: int


class RealisticBalanceOverview(BaseModel):
    flat_stake_rub: Decimal
    base_amount: Decimal
    base_snapshot_at: datetime | None
    base_label: str | None
    total_profit_loss_rub: Decimal
    current_balance_rub: Decimal
    settled_signals_count: int
    wins: int
    losses: int
    voids: int


class BalanceHistoryItem(BaseModel):
    snapshot_id: int
    base_amount: Decimal
    label: str | None
    created_at: datetime

