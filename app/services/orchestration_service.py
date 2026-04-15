from __future__ import annotations

from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.db.repositories.signal_repository import SignalRepository
from app.schemas.candidate_filter import CandidateFilterConfig
from app.schemas.event_result import EventResultInput
from app.schemas.orchestration import CreateSignalOrchestrationResult, ProcessEventResultOrchestrationResult
from app.schemas.provider_models import ProviderSignalCandidate
from app.schemas.signal import SignalCreateBundle
from app.services.analytics_service import AnalyticsService
from app.services.candidate_filter_service import CandidateFilterService
from app.services.deduplication_service import DeduplicationService
from app.services.ingestion_service import IngestionService
from app.services.notification_service import NotificationService
from app.services.result_ingestion_service import ResultIngestionService
from app.services.signal_quality_service import SignalQualityService
from app.services.signal_service import SignalService


class OrchestrationService:
    async def create_signal(self, session: AsyncSession, candidate: ProviderSignalCandidate) -> CreateSignalOrchestrationResult:
        """Create one Signal from a provider candidate (no commit, no notifications)."""
        config = CandidateFilterConfig.default_for_russian_manual_betting()
        filtered = CandidateFilterService().filter_candidates([candidate], config)
        if not filtered.accepted_candidates:
            return CreateSignalOrchestrationResult(signal_id=None, should_notify=False, skipped_reason="filtered")

        deduped = DeduplicationService().deduplicate_candidates(filtered.accepted_candidates)
        if not deduped.unique_candidates:
            return CreateSignalOrchestrationResult(signal_id=None, should_notify=False, skipped_reason="deduped_in_batch")

        accepted = deduped.unique_candidates[0]
        try:
            bundle: SignalCreateBundle = IngestionService().candidate_to_bundle(accepted)
        except (ValidationError, ValueError, TypeError):
            return CreateSignalOrchestrationResult(signal_id=None, should_notify=False, skipped_reason="invalid_candidate")

        # DB-level dedup (same logic as ingestion batch path).
        existing = await SignalRepository().find_existing_similar_signal(
            session,
            sport=bundle.signal.sport,
            bookmaker=bundle.signal.bookmaker,
            event_external_id=bundle.signal.event_external_id,
            home_team=bundle.signal.home_team,
            away_team=bundle.signal.away_team,
            market_type=bundle.signal.market_type,
            selection=bundle.signal.selection,
            is_live=bundle.signal.is_live,
        )
        if existing is not None:
            return CreateSignalOrchestrationResult(signal_id=None, should_notify=False, skipped_reason="duplicate_in_db")

        signal = await SignalService().create_signal_with_prediction_log(session, bundle)
        signal_id = int(signal.id)
        return CreateSignalOrchestrationResult(signal_id=signal_id, should_notify=True, skipped_reason=None)

    async def process_event_result(self, session: AsyncSession, data: EventResultInput) -> ProcessEventResultOrchestrationResult:
        """Process an event result (auto-settle) (no commit, no notifications)."""
        res = await ResultIngestionService().process_event_result(session, data)
        return ProcessEventResultOrchestrationResult(result=res, signal_ids_to_notify=list(res.processed_signal_ids))

    async def notify_signal_if_configured(self, session: AsyncSession, bot, signal_id: int) -> bool:
        settings = get_settings()
        if settings.signal_chat_id is None:
            return False
        report = await AnalyticsService().get_signal_report(session, signal_id)
        await NotificationService().send_signal_notification(bot, settings.signal_chat_id, report)
        return True

    async def notify_result_if_configured(self, session: AsyncSession, bot, signal_id: int) -> bool:
        settings = get_settings()
        if settings.result_chat_id is None:
            return False
        signal_report = await AnalyticsService().get_signal_report(session, signal_id)
        quality_report = await SignalQualityService().build_signal_quality_report(session, signal_id)
        await NotificationService().send_result_notification(
            bot,
            settings.result_chat_id,
            signal_report,
            quality_report,
        )
        return True

