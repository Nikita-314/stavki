from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.balance_snapshot import BalanceSnapshot
from app.schemas.balance import BalanceSnapshotCreate


class BalanceRepository:
    async def create_snapshot(self, session: AsyncSession, data: BalanceSnapshotCreate) -> BalanceSnapshot:
        """Create BalanceSnapshot (no commit)."""
        snapshot = BalanceSnapshot(**data.model_dump())
        session.add(snapshot)
        return snapshot

    async def get_latest_snapshot(self, session: AsyncSession) -> BalanceSnapshot | None:
        stmt = select(BalanceSnapshot).order_by(BalanceSnapshot.created_at.desc(), BalanceSnapshot.id.desc())
        result = await session.execute(stmt)
        return result.scalars().first()

    async def list_snapshots(self, session: AsyncSession) -> list[BalanceSnapshot]:
        stmt = select(BalanceSnapshot).order_by(BalanceSnapshot.created_at.desc(), BalanceSnapshot.id.desc())
        result = await session.execute(stmt)
        return list(result.scalars().all())

