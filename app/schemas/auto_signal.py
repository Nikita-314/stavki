from __future__ import annotations

from pydantic import BaseModel, Field


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
    source_name: str | None = None
    live_auth_status: str | None = None
    last_live_http_status: int | None = None
    fallback_used: bool = False
    fallback_source_name: str | None = None
    rejection_reason: str | None = None
    dry_run: bool = False
    report_matches_found: int | None = None
    report_candidates: int | None = None
    report_after_filter: int | None = None
    report_after_integrity: int | None = None
    report_after_scoring: int | None = None
    report_final_signal: str | None = None
    report_selected_match: str | None = None
    report_selected_bet: str | None = None
    report_selected_odds: str | None = None
    report_selected_score: str | None = None
    report_selected_reason_codes: list[str] = Field(default_factory=list)
    report_human_reasons: list[str] = Field(default_factory=list)
    report_rejection_code: str | None = None
    report_dedup_skipped: int | None = None

