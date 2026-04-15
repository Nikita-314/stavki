from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.db.models.prediction_log import PredictionLog
from app.db.models.signal import Signal
from app.db.models.settlement import Settlement
from app.core.enums import BookmakerType, SignalStatus, SportType
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

    async def get_signal_full_graph(self, session: AsyncSession, signal_id: int) -> Signal | None:
        """Load Signal with prediction_logs, entries, settlement, failure_reviews."""
        stmt = (
            select(Signal)
            .where(Signal.id == signal_id)
            .options(
                selectinload(Signal.prediction_logs),
                selectinload(Signal.entries),
                selectinload(Signal.settlement),
                selectinload(Signal.failure_reviews),
            )
        )
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def find_existing_similar_signal(
        self,
        session: AsyncSession,
        *,
        sport: SportType,
        bookmaker: BookmakerType,
        event_external_id: str | None,
        home_team: str,
        away_team: str,
        market_type: str,
        selection: str,
        is_live: bool,
    ) -> Signal | None:
        """Best-effort exact duplicate lookup (no unique constraints, no fuzzy matching).

        Returns the first matching row or None.
        """
        stmt = (
            select(Signal)
            .where(Signal.sport == sport)
            .where(Signal.bookmaker == bookmaker)
            .where(Signal.market_type == market_type)
            .where(Signal.selection == selection)
            .where(Signal.is_live.is_(is_live))
            .where(Signal.home_team == home_team)
            .where(Signal.away_team == away_team)
        )
        if event_external_id is not None:
            stmt = stmt.where(Signal.event_external_id == event_external_id)
        result = await session.execute(stmt)
        return result.scalars().first()

    async def list_unsettled_by_event_external_id(
        self,
        session: AsyncSession,
        event_external_id: str,
        sport: SportType,
    ) -> list[Signal]:
        """List signals for an event that do not have a Settlement yet."""
        stmt = (
            select(Signal)
            .outerjoin(Settlement, Settlement.signal_id == Signal.id)
            .where(Signal.event_external_id == event_external_id)
            .where(Signal.sport == sport)
            .where(Settlement.id.is_(None))
            .order_by(Signal.id.asc())
        )
        result = await session.execute(stmt)
        return list(result.scalars().all())

    async def list_latest_signal_ids(self, session: AsyncSession, limit: int = 10) -> list[int]:
        """Return latest Signal ids ordered by id desc."""
        stmt = select(Signal.id).order_by(Signal.id.desc()).limit(int(limit))
        result = await session.execute(stmt)
        return [int(x) for x in result.scalars().all()]

    async def list_latest_signals(self, session: AsyncSession, limit: int = 10) -> list[Signal]:
        """Return latest Signal rows ordered by id desc, with settlement preloaded."""
        stmt = (
            select(Signal)
            .options(selectinload(Signal.settlement))
            .order_by(Signal.id.desc())
            .limit(int(limit))
        )
        result = await session.execute(stmt)
        return list(result.scalars().all())

