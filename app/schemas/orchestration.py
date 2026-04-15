from __future__ import annotations

from pydantic import BaseModel

from app.schemas.event_result import EventResultProcessingResult


class CreateSignalOrchestrationResult(BaseModel):
    signal_id: int | None
    should_notify: bool
    skipped_reason: str | None = None


class ProcessEventResultOrchestrationResult(BaseModel):
    result: EventResultProcessingResult
    signal_ids_to_notify: list[int]

