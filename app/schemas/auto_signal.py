from __future__ import annotations

from pydantic import BaseModel


class AutoSignalCycleResult(BaseModel):
    endpoint: str | None
    fetch_ok: bool
    preview_candidates: int
    preview_skipped_items: int
    created_signal_ids: list[int]
    created_signals_count: int
    skipped_candidates_count: int
    notifications_sent_count: int
    preview_only: bool
    message: str
    raw_events_count: int = 0
    normalized_markets_count: int = 0
    candidates_before_filter_count: int = 0
    candidates_after_filter_count: int = 0
    runtime_paused: bool = False
    runtime_active_sports: list[str] = []

