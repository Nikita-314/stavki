from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.repositories.signal_repository import SignalRepository
from app.schemas.analytics import (
    EntryRead,
    FailureReviewRead,
    PredictionLogRead,
    SettlementRead,
    SignalAnalyticsReport,
    SignalRead,
)


class AnalyticsService:
    def __init__(self, signal_repo: SignalRepository | None = None) -> None:
        self._signal_repo = signal_repo or SignalRepository()

    async def get_signal_report(self, session: AsyncSession, signal_id: int) -> SignalAnalyticsReport:
        """Return full analytics report for a single signal_id.

        Loads the full ORM graph and converts it into Pydantic read-schemas.
        """
        signal = await self._signal_repo.get_signal_full_graph(session, signal_id)
        if signal is None:
            raise ValueError(f"Signal with id={signal_id} not found")

        return SignalAnalyticsReport(
            signal=SignalRead.model_validate(signal),
            prediction_logs=[PredictionLogRead.model_validate(x) for x in signal.prediction_logs],
            entries=[EntryRead.model_validate(x) for x in signal.entries],
            settlement=SettlementRead.model_validate(signal.settlement) if signal.settlement is not None else None,
            failure_reviews=[FailureReviewRead.model_validate(x) for x in signal.failure_reviews],
        )

