from __future__ import annotations

import json
from pathlib import Path

from pydantic import ValidationError

from app.providers.base import BaseCandidateProvider
from app.schemas.provider_file import JsonCandidateFileLoadResult
from app.schemas.provider_models import ProviderSignalCandidate


class JsonCandidateProvider(BaseCandidateProvider):
    def __init__(self, file_path: str) -> None:
        self._file_path = file_path

    async def fetch_candidates(self) -> list[ProviderSignalCandidate]:
        candidates, _stats = self.load_with_stats()
        return candidates

    def load_with_stats(self) -> tuple[list[ProviderSignalCandidate], JsonCandidateFileLoadResult]:
        path = Path(self._file_path)
        try:
            raw = path.read_text(encoding="utf-8")
            data = json.loads(raw)
        except Exception:
            return [], JsonCandidateFileLoadResult(total_items=0, loaded_candidates=0, skipped_items=0)

        if not isinstance(data, list):
            return [], JsonCandidateFileLoadResult(total_items=0, loaded_candidates=0, skipped_items=0)

        total_items = len(data)
        loaded: list[ProviderSignalCandidate] = []
        skipped = 0

        for item in data:
            try:
                loaded.append(ProviderSignalCandidate.model_validate(item))
            except (ValidationError, TypeError, ValueError):
                skipped += 1
                continue

        stats = JsonCandidateFileLoadResult(
            total_items=total_items,
            loaded_candidates=len(loaded),
            skipped_items=skipped,
        )
        return loaded, stats

