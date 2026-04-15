from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import EntryStatus, SignalStatus
from app.db.models.entry import Entry
from app.db.repositories.entry_repository import EntryRepository
from app.db.repositories.signal_repository import SignalRepository
from app.schemas.entry import EntryCreate


class EntryService:
    def __init__(
        self,
        *,
        entry_repo: EntryRepository | None = None,
        signal_repo: SignalRepository | None = None,
    ) -> None:
        self._entry_repo = entry_repo or EntryRepository()
        self._signal_repo = signal_repo or SignalRepository()

    async def register_entry(self, session: AsyncSession, data: EntryCreate) -> Entry:
        """Register a manual entry attempt for a Signal (no commit).

        Rules:
        - Signal must exist
        - Create Entry
        - If status == ENTERED -> set Signal.status = ENTERED
        - If status in {SKIPPED, REJECTED} -> set Signal.status = MISSED
        """
        signal = await self._signal_repo.get_signal_by_id(session, data.signal_id)
        if signal is None:
            raise ValueError(f"Signal with id={data.signal_id} not found")

        entry = await self._entry_repo.create_entry(session, data)
        await session.flush()

        if data.status == EntryStatus.ENTERED:
            await self._signal_repo.update_status(session, signal, SignalStatus.ENTERED)
        elif data.status in {EntryStatus.SKIPPED, EntryStatus.REJECTED}:
            await self._signal_repo.update_status(session, signal, SignalStatus.MISSED)

        await session.flush()
        return entry

