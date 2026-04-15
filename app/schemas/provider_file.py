from __future__ import annotations

from pydantic import BaseModel


class JsonCandidateFileLoadResult(BaseModel):
    total_items: int
    loaded_candidates: int
    skipped_items: int

