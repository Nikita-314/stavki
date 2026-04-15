from __future__ import annotations

from decimal import Decimal

import sqlalchemy as sa
from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import BetResult
from app.db.models.balance_snapshot import BalanceSnapshot
from app.db.models.settlement import Settlement
from app.db.models.signal import Signal
from app.db.repositories.balance_repository import BalanceRepository
from app.schemas.period_report import PeriodReport, PeriodReportGroupItem, PeriodReportOverview


class PeriodReportService:
    _balance_repo = BalanceRepository()

    async def get_period_report(self, session: AsyncSession) -> PeriodReport:
        # TODO: add a separate realistic period report (fixed stake in RUB) based on Settlement+Signal,
        # keeping this unit-based report for analytics.
        snapshot = await self._balance_repo.get_latest_snapshot(session)

        if snapshot is not None:
            start_balance = Decimal(snapshot.base_amount)
            period_started_at = snapshot.created_at
            period_label = snapshot.label
            cutoff = snapshot.created_at
            effective_dt = func.coalesce(Settlement.settled_at, Settlement.created_at)
            where_clause = effective_dt >= cutoff
        else:
            start_balance = Decimal("50000.00")
            period_started_at = None
            period_label = "default_start_balance"
            where_clause = sa.true()

        overview = await self._build_overview(session, where_clause, start_balance, period_started_at, period_label)

        by_sport = await self._group_by(session, where_clause, Signal.sport, "sport")
        by_bookmaker = await self._group_by(session, where_clause, Signal.bookmaker, "bookmaker")
        by_market_type = await self._group_by(session, where_clause, Signal.market_type, "market_type")

        return PeriodReport(
            overview=overview,
            by_sport=by_sport,
            by_bookmaker=by_bookmaker,
            by_market_type=by_market_type,
        )

    async def _build_overview(
        self,
        session: AsyncSession,
        where_clause,
        start_balance: Decimal,
        period_started_at,
        period_label,
    ) -> PeriodReportOverview:
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

        return PeriodReportOverview(
            period_started_at=period_started_at,
            period_label=period_label,
            start_balance=start_balance,
            total_profit_loss=total_pl,
            current_balance=start_balance + total_pl,
            settled_signals_count=cnt,
            wins=wins,
            losses=losses,
            voids=voids,
        )

    async def _group_by(self, session: AsyncSession, where_clause, group_col, group_name: str) -> list[PeriodReportGroupItem]:
        wins_case = case((Settlement.result == BetResult.WIN, 1), else_=0)
        losses_case = case((Settlement.result == BetResult.LOSE, 1), else_=0)
        voids_case = case((Settlement.result == BetResult.VOID, 1), else_=0)

        total_pl = func.coalesce(func.sum(Settlement.profit_loss), 0).label("total_pl")
        cnt = func.count(Settlement.id).label("cnt")
        avg_pl = (
            func.coalesce(func.sum(Settlement.profit_loss), 0) / func.nullif(func.count(Settlement.id), 0)
        ).label("avg_pl")

        stmt = (
            select(
                group_col.label("key"),
                total_pl,
                avg_pl,
                cnt,
                func.coalesce(func.sum(wins_case), 0).label("wins"),
                func.coalesce(func.sum(losses_case), 0).label("losses"),
                func.coalesce(func.sum(voids_case), 0).label("voids"),
            )
            .select_from(Settlement)
            .join(Signal, Signal.id == Settlement.signal_id)
            .where(where_clause)
            .group_by(group_col)
            .order_by(total_pl.desc())
        )

        result = await session.execute(stmt)
        items: list[PeriodReportGroupItem] = []
        for r in result.all():
            key_obj = r.key
            key_str = getattr(key_obj, "value", None) or (str(key_obj) if key_obj is not None else "None")
            items.append(
                PeriodReportGroupItem(
                    key=key_str,
                    settled_signals_count=int(r.cnt),
                    wins=int(r.wins),
                    losses=int(r.losses),
                    voids=int(r.voids),
                    total_profit_loss=Decimal(r.total_pl),
                    avg_profit_loss=Decimal(r.avg_pl) if r.avg_pl is not None else Decimal("0"),
                )
            )
        return items

