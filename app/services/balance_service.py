from __future__ import annotations

from decimal import Decimal

import sqlalchemy as sa
from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.enums import BetResult
from app.db.models.balance_snapshot import BalanceSnapshot
from app.db.models.settlement import Settlement
from app.db.models.signal import Signal
from app.db.repositories.balance_repository import BalanceRepository
from app.schemas.balance import BalanceHistoryItem, BalanceOverview, BalanceSnapshotCreate, RealisticBalanceOverview


class BalanceService:
    _repo = BalanceRepository()

    async def get_balance_overview(self, session: AsyncSession) -> BalanceOverview:
        snapshot = await self._repo.get_latest_snapshot(session)

        if snapshot is not None:
            base_amount = Decimal(snapshot.base_amount)
            base_snapshot_at = snapshot.created_at
            base_label = snapshot.label
            cutoff = snapshot.created_at
            effective_dt = func.coalesce(Settlement.settled_at, Settlement.created_at)
            where_clause = effective_dt >= cutoff
        else:
            base_amount = Decimal("50000.00")
            base_snapshot_at = None
            base_label = "default_start_balance"
            where_clause = sa.true()

        wins_case = case((Settlement.result == BetResult.WIN, 1), else_=0)
        losses_case = case((Settlement.result == BetResult.LOSE, 1), else_=0)
        voids_case = case((Settlement.result == BetResult.VOID, 1), else_=0)

        stmt = (
            select(
                func.coalesce(func.sum(Settlement.profit_loss), 0).label("total_pl"),
                func.count(Settlement.id).label("cnt"),
                func.coalesce(func.sum(wins_case), 0).label("wins"),
                func.coalesce(func.sum(losses_case), 0).label("losses"),
                func.coalesce(func.sum(voids_case), 0).label("voids"),
            )
            .where(where_clause)
        )

        row = (await session.execute(stmt)).one()
        total_pl = Decimal(row.total_pl)
        cnt = int(row.cnt)
        wins = int(row.wins)
        losses = int(row.losses)
        voids = int(row.voids)

        current_balance = base_amount + total_pl

        return BalanceOverview(
            base_amount=base_amount,
            base_snapshot_at=base_snapshot_at,
            base_label=base_label,
            total_profit_loss_since_base=total_pl,
            current_balance=current_balance,
            settled_signals_count=cnt,
            wins=wins,
            losses=losses,
            voids=voids,
        )

    async def get_realistic_balance_overview(self, session: AsyncSession) -> RealisticBalanceOverview:
        settings = get_settings()
        snapshot = await self._repo.get_latest_snapshot(session)

        flat_stake = settings.virtual_flat_stake_rub

        if snapshot is not None:
            base_amount = Decimal(snapshot.base_amount)
            base_snapshot_at = snapshot.created_at
            base_label = snapshot.label
            cutoff = snapshot.created_at
            effective_dt = func.coalesce(Settlement.settled_at, Settlement.created_at)
            where_clause = effective_dt >= cutoff
        else:
            base_amount = settings.virtual_start_balance_rub
            base_snapshot_at = None
            base_label = "default_start_balance_rub"
            where_clause = sa.true()

        # Realistic PnL in RUB (fixed stake) based on Signal.odds_at_signal and Settlement.result.
        pnl_rub_expr = case(
            (Settlement.result == BetResult.WIN, (Signal.odds_at_signal - 1) * flat_stake),
            (Settlement.result == BetResult.LOSE, -flat_stake),
            (Settlement.result == BetResult.VOID, 0),
            else_=0,
        )

        wins_case = case((Settlement.result == BetResult.WIN, 1), else_=0)
        losses_case = case((Settlement.result == BetResult.LOSE, 1), else_=0)
        voids_case = case((Settlement.result == BetResult.VOID, 1), else_=0)

        stmt = (
            select(
                func.coalesce(func.sum(pnl_rub_expr), 0).label("total_pl_rub"),
                func.count(Settlement.id).label("cnt"),
                func.coalesce(func.sum(wins_case), 0).label("wins"),
                func.coalesce(func.sum(losses_case), 0).label("losses"),
                func.coalesce(func.sum(voids_case), 0).label("voids"),
            )
            .select_from(Settlement)
            .join(Signal, Signal.id == Settlement.signal_id)
            .where(where_clause)
        )

        row = (await session.execute(stmt)).one()
        total_pl_rub = Decimal(row.total_pl_rub)
        cnt = int(row.cnt)
        wins = int(row.wins)
        losses = int(row.losses)
        voids = int(row.voids)

        return RealisticBalanceOverview(
            flat_stake_rub=flat_stake,
            base_amount=base_amount,
            base_snapshot_at=base_snapshot_at,
            base_label=base_label,
            total_profit_loss_rub=total_pl_rub,
            current_balance_rub=base_amount + total_pl_rub,
            settled_signals_count=cnt,
            wins=wins,
            losses=losses,
            voids=voids,
        )

    async def reset_balance(self, session: AsyncSession, base_amount: Decimal, label: str | None = None) -> BalanceSnapshot:
        data = BalanceSnapshotCreate(base_amount=base_amount, label=label)
        snapshot = await self._repo.create_snapshot(session, data)
        await session.flush()
        return snapshot

    async def list_balance_history(self, session: AsyncSession) -> list[BalanceHistoryItem]:
        snapshots = await self._repo.list_snapshots(session)
        return [
            BalanceHistoryItem(
                snapshot_id=int(s.id),
                base_amount=Decimal(s.base_amount),
                label=s.label,
                created_at=s.created_at,
            )
            for s in snapshots
        ]

