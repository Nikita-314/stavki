from __future__ import annotations

from pydantic import BaseModel

from app.schemas.provider_models import ProviderSignalCandidate


class ProviderAdapterResult(BaseModel):
    source_name: str
    total_events: int
    total_markets: int
    created_candidates: int
    skipped_items: int
    candidates: list[ProviderSignalCandidate]

