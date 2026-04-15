from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from app.schemas.provider_adapter import ProviderAdapterResult
from app.schemas.provider_raw import RawProviderPayload


class BaseProviderAdapter(ABC):
    """Adapter contract for converting provider payloads into ProviderSignalCandidate list.

    - parse_payload(): raw JSON dict -> normalized RawProviderPayload
    - to_candidates(): RawProviderPayload -> ProviderAdapterResult (candidates + stats)
    """

    @abstractmethod
    def parse_payload(self, payload: dict[str, Any]) -> RawProviderPayload:
        raise NotImplementedError

    @abstractmethod
    def to_candidates(self, raw: RawProviderPayload) -> ProviderAdapterResult:
        raise NotImplementedError

