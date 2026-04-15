from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.failure_review import FailureReview
from app.db.repositories.failure_review_repository import FailureReviewRepository
from app.db.repositories.signal_repository import SignalRepository
from app.schemas.failure_review import FailureReviewCreate
from app.schemas.failure_auto_review import FailureAutoReviewInput
from app.services.analytics_service import AnalyticsService
from app.services.failure_auto_review_service import FailureAutoReviewService


class FailureReviewService:
    def __init__(
        self,
        *,
        failure_review_repo: FailureReviewRepository | None = None,
        signal_repo: SignalRepository | None = None,
    ) -> None:
        self._failure_review_repo = failure_review_repo or FailureReviewRepository()
        self._signal_repo = signal_repo or SignalRepository()

    async def register_failure_review(self, session: AsyncSession, data: FailureReviewCreate) -> FailureReview:
        """Register a failure review for a Signal (no commit)."""
        signal = await self._signal_repo.get_signal_by_id(session, data.signal_id)
        if signal is None:
            raise ValueError(f"Signal with id={data.signal_id} not found")

        review = await self._failure_review_repo.create_failure_review(session, data)
        await session.flush()
        return review

    async def register_auto_failure_review(self, session: AsyncSession, signal_id: int) -> FailureReview:
        """Build and persist an automatic failure review for a signal (no commit)."""
        report = await AnalyticsService().get_signal_report(session, signal_id)

        auto_input = FailureAutoReviewInput(
            signal=report.signal,
            prediction_logs=report.prediction_logs,
            entries=report.entries,
            settlement=report.settlement,
            failure_reviews=report.failure_reviews,
        )
        auto_result = FailureAutoReviewService().build_auto_review(auto_input)

        data = FailureReviewCreate(
            signal_id=signal_id,
            category=auto_result.category,
            auto_reason=auto_result.auto_reason,
            failure_tags_json=auto_result.failure_tags_json,
        )
        review = await self._failure_review_repo.create_failure_review(session, data)
        await session.flush()
        return review

