from __future__ import annotations

from pydantic import BaseModel


class SanityIssueItem(BaseModel):
    issue_type: str
    signal_id: int | None = None
    details: str


class SanityCheckReport(BaseModel):
    total_signals: int
    total_settlements: int
    total_failure_reviews: int
    total_entries: int
    issues_count: int
    issues: list[SanityIssueItem]

