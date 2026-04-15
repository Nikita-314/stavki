from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel

from app.core.enums import FailureCategory


class FailureReviewCreate(BaseModel):
    signal_id: int
    category: FailureCategory = FailureCategory.UNKNOWN

    auto_reason: str | None = None
    manual_reason: str | None = None
    failure_tags_json: dict[str, Any] | None = None
    notes: str | None = None
    reviewed_at: datetime | None = None

