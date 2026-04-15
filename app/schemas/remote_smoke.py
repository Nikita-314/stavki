from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel


class RemoteSmokeResult(BaseModel):
    endpoint: str | None
    fetch_ok: bool
    preview_candidates: int
    preview_skipped_items: int
    ingested_created_signals: int
    ingested_skipped_candidates: int
    created_signal_ids: list[int]
    sanity_issues_count: int
    total_signals: int
    settled_signals: int
    current_balance_rub: Decimal | None
    message: str

