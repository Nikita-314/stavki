from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import SignalStatus
from app.db.models.settlement import Settlement
from app.db.repositories.settlement_repository import SettlementRepository
from app.db.repositories.signal_repository import SignalRepository
from app.schemas.settlement import SettlementCreate


class SettlementService:
    def __init__(
        self,
        *,
        settlement_repo: SettlementRepository | None = None,
        signal_repo: SignalRepository | None = None,
    ) -> None:
        self._settlement_repo = settlement_repo or SettlementRepository()
        self._signal_repo = signal_repo or SignalRepository()

    async def register_settlement(self, session: AsyncSession, data: SettlementCreate) -> Settlement:
        """Register settlement for a Signal (no commit).

        Rules:
        - Signal must exist
        - Only one Settlement per signal_id is allowed
        - Create Settlement
        - Set Signal.status = SETTLED
        """
        signal = await self._signal_repo.get_signal_by_id(session, data.signal_id)
        if signal is None:
            raise ValueError(f"Signal with id={data.signal_id} not found")

        existing = await self._settlement_repo.get_by_signal_id(session, data.signal_id)
        if existing is not None:
            raise ValueError(f"Settlement for signal_id={data.signal_id} already exists")

        settlement = await self._settlement_repo.create_settlement(session, data)
        await session.flush()

        await self._signal_repo.update_status(session, signal, SignalStatus.SETTLED)
        await session.flush()

        return settlement

