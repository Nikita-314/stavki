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


class RealisticPeriodReportGroupItem(BaseModel):
    key: str
    settled_signals_count: int
    wins: int
    losses: int
    voids: int
    total_profit_loss_rub: Decimal
    avg_profit_loss_rub: Decimal


class RealisticPeriodReportOverview(BaseModel):
    period_started_at: datetime | None
    period_label: str | None
    start_balance_rub: Decimal
    flat_stake_rub: Decimal
    total_profit_loss_rub: Decimal
    current_balance_rub: Decimal
    settled_signals_count: int
    wins: int
    losses: int
    voids: int


class RealisticPeriodReport(BaseModel):
    overview: RealisticPeriodReportOverview
    by_sport: list[RealisticPeriodReportGroupItem]
    by_bookmaker: list[RealisticPeriodReportGroupItem]
    by_market_type: list[RealisticPeriodReportGroupItem]

