from __future__ import annotations

from app.providers.base import BaseCandidateProvider
from app.providers.json_candidate_provider import JsonCandidateProvider
from app.providers.mock_candidate_provider import MockCandidateProvider

__all__ = [
    "BaseCandidateProvider",
    "JsonCandidateProvider",
    "MockCandidateProvider",
]

