from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.entry import Entry
from app.schemas.entry import EntryCreate


class EntryRepository:
    async def create_entry(self, session: AsyncSession, data: EntryCreate) -> Entry:
        """Create an Entry row (no commit)."""
        entry = Entry(**data.model_dump())
        session.add(entry)
        return entry

    async def list_by_signal_id(self, session: AsyncSession, signal_id: int) -> list[Entry]:
        """List all entries for a signal (ascending by id)."""
        result = await session.execute(select(Entry).where(Entry.signal_id == signal_id).order_by(Entry.id.asc()))
        return list(result.scalars().all())

