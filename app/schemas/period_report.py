from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel


class PeriodReportGroupItem(BaseModel):
    key: str
    settled_signals_count: int
    wins: int
    losses: int
    voids: int
    total_profit_loss: Decimal
    avg_profit_loss: Decimal


class PeriodReportOverview(BaseModel):
    period_started_at: datetime | None
    period_label: str | None
    start_balance: Decimal
    total_profit_loss: Decimal
    current_balance: Decimal
    settled_signals_count: int
    wins: int
    losses: int
    voids: int


class PeriodReport(BaseModel):
    overview: PeriodReportOverview
    by_sport: list[PeriodReportGroupItem]
    by_bookmaker: list[PeriodReportGroupItem]
    by_market_type: list[PeriodReportGroupItem]

