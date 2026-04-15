from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.signal import Signal
from app.db.repositories.signal_repository import SignalRepository
from app.schemas.signal import SignalCreateBundle


class SignalService:
    def __init__(self, signal_repo: SignalRepository | None = None) -> None:
        self._signal_repo = signal_repo or SignalRepository()

    async def create_signal_with_prediction_log(self, session: AsyncSession, bundle: SignalCreateBundle) -> Signal:
        """Create Signal + PredictionLog in the same transaction (no commit).

        - creates Signal
        - flushes to obtain signal.id
        - creates PredictionLog bound to this signal
        - flushes again to persist FK row ordering

        Commit is intentionally not performed here: transaction ownership belongs to the caller.
        """
        # Pydantic already validates most invariants; keep this as a safety net for future changes.
        if bundle.signal.odds_at_signal <= 1 or bundle.signal.min_entry_odds <= 1:
            raise ValueError("odds_at_signal and min_entry_odds must be > 1")

        if bundle.signal.predicted_prob is not None and not (0 <= bundle.signal.predicted_prob <= 1):
            raise ValueError("predicted_prob must be within 0..1")
        if bundle.signal.implied_prob is not None and not (0 <= bundle.signal.implied_prob <= 1):
            raise ValueError("implied_prob must be within 0..1")
        if bundle.signal.edge is not None and not (-1 <= bundle.signal.edge <= 10):
            raise ValueError("edge must be within -1..10")

        # TODO: later verify provided model_version_name against ModelVersion in DB (non-fatal for now)

        signal = await self._signal_repo.create_signal(session, bundle.signal)
        await session.flush()  # assigns signal.id

        await self._signal_repo.add_prediction_log(session, signal.id, bundle.prediction_log)
        await session.flush()

        return signal

