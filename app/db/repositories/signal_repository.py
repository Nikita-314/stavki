from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.prediction_log import PredictionLog
from app.db.models.signal import Signal
from app.core.enums import SignalStatus
from app.schemas.signal import PredictionLogCreate, SignalCreate


class SignalRepository:
    async def create_signal(self, session: AsyncSession, data: SignalCreate) -> Signal:
        """Create a Signal row (no commit).

        Note: flush/commit is intentionally left to the caller.
        """
        signal = Signal(**data.model_dump())
        session.add(signal)
        return signal

    async def add_prediction_log(
        self, session: AsyncSession, signal_id: int, data: PredictionLogCreate
    ) -> PredictionLog:
        """Attach a PredictionLog to an existing Signal (no commit)."""
        log = PredictionLog(signal_id=signal_id, **data.model_dump())
        session.add(log)
        return log

    async def get_signal_by_id(self, session: AsyncSession, signal_id: int) -> Signal | None:
        """Return Signal by id or None."""
        result = await session.execute(select(Signal).where(Signal.id == signal_id))
        return result.scalar_one_or_none()

    async def update_status(self, session: AsyncSession, signal: Signal, status: SignalStatus) -> None:
        """Update Signal.status (no commit)."""
        signal.status = status
        session.add(signal)

