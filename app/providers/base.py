from __future__ import annotations

from abc import ABC, abstractmethod

from app.schemas.provider_models import ProviderSignalCandidate


class BaseCandidateProvider(ABC):
    @abstractmethod
    async def fetch_candidates(self) -> list[ProviderSignalCandidate]:
        """Fetch signal candidates from an external source (no side effects)."""
        raise NotImplementedError

