from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from app.providers.mock_candidate_provider import MockCandidateProvider
from app.schemas.provider_models import ProviderBatchIngestResult, ProviderSignalCandidate
from app.services.ingestion_service import IngestionService


class BootstrapService:
    async def preview_mock_candidates(self) -> list[ProviderSignalCandidate]:
        """Return mock candidates without writing anything to the DB."""
        return await MockCandidateProvider().fetch_candidates()

    async def run_mock_ingestion(self, session: AsyncSession) -> ProviderBatchIngestResult:
        """Run end-to-end mock ingestion: provider -> filter -> dedup -> ingestion (no commit)."""
        candidates = await MockCandidateProvider().fetch_candidates()
        return await IngestionService().ingest_candidates_with_filter_and_dedup(session, candidates)

