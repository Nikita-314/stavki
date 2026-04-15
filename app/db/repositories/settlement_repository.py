from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.settlement import Settlement
from app.schemas.settlement import SettlementCreate


class SettlementRepository:
    async def create_settlement(self, session: AsyncSession, data: SettlementCreate) -> Settlement:
        """Create a Settlement row (no commit)."""
        settlement = Settlement(**data.model_dump())
        session.add(settlement)
        return settlement

    async def get_by_signal_id(self, session: AsyncSession, signal_id: int) -> Settlement | None:
        """Return Settlement by signal_id or None."""
        result = await session.execute(select(Settlement).where(Settlement.signal_id == signal_id))
        return result.scalar_one_or_none()

