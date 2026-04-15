from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.failure_review import FailureReview
from app.schemas.failure_review import FailureReviewCreate


class FailureReviewRepository:
    async def create_failure_review(self, session: AsyncSession, data: FailureReviewCreate) -> FailureReview:
        """Create a FailureReview row (no commit)."""
        review = FailureReview(**data.model_dump())
        session.add(review)
        return review

    async def list_by_signal_id(self, session: AsyncSession, signal_id: int) -> list[FailureReview]:
        """List all failure reviews for a signal (ascending by id)."""
        result = await session.execute(
            select(FailureReview).where(FailureReview.signal_id == signal_id).order_by(FailureReview.id.asc())
        )
        return list(result.scalars().all())

