from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel

from app.core.enums import BookmakerType, FailureCategory, SportType


class AnalyticsFilter(BaseModel):
    sport: SportType | None = None
    bookmaker: BookmakerType | None = None
    market_type: str | None = None
    is_live: bool | None = None
    model_name: str | None = None
    model_version_name: str | None = None
    date_from: datetime | None = None
    date_to: datetime | None = None


class KPIStats(BaseModel):
    total_signals: int
    entered_signals: int
    missed_signals: int
    settled_signals: int

    wins: int
    losses: int
    voids: int
    unknown_results: int

    total_profit_loss: Decimal
    avg_profit_loss: Decimal
    win_rate: Decimal
    roi_percent: Decimal | None


class GroupedStatItem(BaseModel):
    key: str
    total_signals: int
    settled_signals: int
    total_profit_loss: Decimal
    win_rate: Decimal


class FailureCategoryStatItem(BaseModel):
    category: FailureCategory
    count: int


class AnalyticsSummaryReport(BaseModel):
    filters: AnalyticsFilter
    kpis: KPIStats
    by_sport: list[GroupedStatItem]
    by_bookmaker: list[GroupedStatItem]
    by_market_type: list[GroupedStatItem]
    by_failure_category: list[FailureCategoryStatItem]

