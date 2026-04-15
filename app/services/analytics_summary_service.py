from __future__ import annotations

from decimal import Decimal

from sqlalchemy import and_, case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import BetResult, EntryStatus, FailureCategory, SignalStatus
from app.db.models.entry import Entry
from app.db.models.failure_review import FailureReview
from app.db.models.settlement import Settlement
from app.db.models.signal import Signal
from app.schemas.analytics_summary import (
    AnalyticsFilter,
    AnalyticsSummaryReport,
    FailureCategoryStatItem,
    GroupedStatItem,
    KPIStats,
)


class AnalyticsSummaryService:
    async def get_summary(self, session: AsyncSession, filters: AnalyticsFilter | None = None) -> AnalyticsSummaryReport:
        """Build aggregated analytics summary report across many signals."""
        filters = filters or AnalyticsFilter()

        kpis = await self._calc_kpis(session, filters)
        by_sport = await self._group_by(session, filters, key_col=Signal.sport, key_label="sport")
        by_bookmaker = await self._group_by(session, filters, key_col=Signal.bookmaker, key_label="bookmaker")
        by_market_type = await self._group_by(session, filters, key_col=Signal.market_type, key_label="market_type")
        by_failure_category = await self._group_failure_category(session, filters)

        return AnalyticsSummaryReport(
            filters=filters,
            kpis=kpis,
            by_sport=by_sport,
            by_bookmaker=by_bookmaker,
            by_market_type=by_market_type,
            by_failure_category=by_failure_category,
        )

    def _signal_filters(self, f: AnalyticsFilter) -> list:
        clauses: list = []
        if f.sport is not None:
            clauses.append(Signal.sport == f.sport)
        if f.bookmaker is not None:
            clauses.append(Signal.bookmaker == f.bookmaker)
        if f.market_type is not None:
            clauses.append(Signal.market_type == f.market_type)
        if f.is_live is not None:
            clauses.append(Signal.is_live.is_(f.is_live))
        if f.model_name is not None:
            clauses.append(Signal.model_name == f.model_name)
        if f.model_version_name is not None:
            clauses.append(Signal.model_version_name == f.model_version_name)
        if f.date_from is not None:
            clauses.append(Signal.signaled_at >= f.date_from)
        if f.date_to is not None:
            clauses.append(Signal.signaled_at <= f.date_to)
        return clauses

    async def _calc_kpis(self, session: AsyncSession, f: AnalyticsFilter) -> KPIStats:
        where_clause = and_(*self._signal_filters(f)) if self._signal_filters(f) else None

        # total signals, missed signals
        total_stmt = select(
            func.count(Signal.id),
            func.sum(case((Signal.status == SignalStatus.MISSED, 1), else_=0)),
        ).select_from(Signal)
        if where_clause is not None:
            total_stmt = total_stmt.where(where_clause)
        total_signals, missed_signals = (await session.execute(total_stmt)).one()

        # entered signals: at least one ENTERED entry
        entered_subq = (
            select(func.distinct(Entry.signal_id).label("signal_id"))
            .join(Signal, Signal.id == Entry.signal_id)
            .where(Entry.status == EntryStatus.ENTERED)
        )
        if where_clause is not None:
            entered_subq = entered_subq.where(where_clause)
        entered_count_stmt = select(func.count()).select_from(entered_subq.subquery())
        entered_signals = (await session.execute(entered_count_stmt)).scalar_one()

        # settlement stats
        settled_stmt = (
            select(
                func.count(Settlement.id),
                func.sum(case((Settlement.result == BetResult.WIN, 1), else_=0)),
                func.sum(case((Settlement.result == BetResult.LOSE, 1), else_=0)),
                func.sum(case((Settlement.result == BetResult.VOID, 1), else_=0)),
                func.sum(case((Settlement.result == BetResult.UNKNOWN, 1), else_=0)),
                func.coalesce(func.sum(Settlement.profit_loss), 0),
                func.coalesce(func.avg(Settlement.profit_loss), 0),
            )
            .select_from(Signal)
            .outerjoin(Settlement, Settlement.signal_id == Signal.id)
        )
        if where_clause is not None:
            settled_stmt = settled_stmt.where(where_clause)
        (
            settled_signals,
            wins,
            losses,
            voids,
            unknown_results,
            total_profit_loss,
            avg_profit_loss,
        ) = (await session.execute(settled_stmt)).one()

        total_profit_loss = Decimal(total_profit_loss or 0)
        avg_profit_loss = Decimal(avg_profit_loss or 0)

        win_rate = Decimal("0")
        if settled_signals and settled_signals > 0:
            win_rate = Decimal(wins or 0) / Decimal(settled_signals)

        # ROI: sum stake_amount for ENTERED entries
        roi_den_stmt = (
            select(func.coalesce(func.sum(Entry.stake_amount), 0))
            .select_from(Entry)
            .join(Signal, Signal.id == Entry.signal_id)
            .where(Entry.status == EntryStatus.ENTERED)
            .where(Entry.stake_amount.is_not(None))
        )
        if where_clause is not None:
            roi_den_stmt = roi_den_stmt.where(where_clause)
        denom = (await session.execute(roi_den_stmt)).scalar_one()
        denom = Decimal(denom or 0)

        roi_percent: Decimal | None
        if denom == 0:
            roi_percent = None
        else:
            roi_percent = (total_profit_loss / denom) * Decimal("100")

        return KPIStats(
            total_signals=int(total_signals or 0),
            entered_signals=int(entered_signals or 0),
            missed_signals=int(missed_signals or 0),
            settled_signals=int(settled_signals or 0),
            wins=int(wins or 0),
            losses=int(losses or 0),
            voids=int(voids or 0),
            unknown_results=int(unknown_results or 0),
            total_profit_loss=total_profit_loss,
            avg_profit_loss=avg_profit_loss,
            win_rate=win_rate,
            roi_percent=roi_percent,
        )

    async def _group_by(
        self,
        session: AsyncSession,
        f: AnalyticsFilter,
        *,
        key_col,
        key_label: str,
    ) -> list[GroupedStatItem]:
        where_clause = and_(*self._signal_filters(f)) if self._signal_filters(f) else None

        stmt = (
            select(
                key_col.label(key_label),
                func.count(Signal.id).label("total_signals"),
                func.count(Settlement.id).label("settled_signals"),
                func.coalesce(func.sum(Settlement.profit_loss), 0).label("total_profit_loss"),
                func.coalesce(func.sum(case((Settlement.result == BetResult.WIN, 1), else_=0)), 0).label("wins"),
            )
            .select_from(Signal)
            .outerjoin(Settlement, Settlement.signal_id == Signal.id)
            .group_by(key_col)
            .order_by(func.count(Signal.id).desc())
        )
        if where_clause is not None:
            stmt = stmt.where(where_clause)

        rows = (await session.execute(stmt)).all()
        items: list[GroupedStatItem] = []
        for key, total_signals, settled_signals, total_profit_loss, wins in rows:
            settled_signals_i = int(settled_signals or 0)
            wins_i = int(wins or 0)
            win_rate = Decimal("0")
            if settled_signals_i > 0:
                win_rate = Decimal(wins_i) / Decimal(settled_signals_i)

            items.append(
                GroupedStatItem(
                    key=str(key) if key is not None else "UNKNOWN",
                    total_signals=int(total_signals or 0),
                    settled_signals=settled_signals_i,
                    total_profit_loss=Decimal(total_profit_loss or 0),
                    win_rate=win_rate,
                )
            )
        return items

    async def _group_failure_category(self, session: AsyncSession, f: AnalyticsFilter) -> list[FailureCategoryStatItem]:
        where_clause = and_(*self._signal_filters(f)) if self._signal_filters(f) else None

        stmt = (
            select(FailureReview.category, func.count(FailureReview.id))
            .select_from(FailureReview)
            .join(Signal, Signal.id == FailureReview.signal_id)
            .group_by(FailureReview.category)
            .order_by(func.count(FailureReview.id).desc())
        )
        if where_clause is not None:
            stmt = stmt.where(where_clause)

        rows = (await session.execute(stmt)).all()
        return [
            FailureCategoryStatItem(category=category, count=int(count or 0)) for category, count in rows
        ]

