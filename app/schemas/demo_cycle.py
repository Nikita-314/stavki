from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel


class DemoCycleResult(BaseModel):
    scenario: str
    created_signal_id: int | None
    signal_notification_sent: bool
    result_processed: bool
    result_notification_sent_count: int
    total_signals_found: int
    settled_signals: int
    skipped_signals: int
    created_failure_reviews: int
    processed_signal_ids: list[int]
    balance_mode_unit_current: Decimal | None
    balance_mode_rub_current: Decimal | None
    message: str

