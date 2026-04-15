from __future__ import annotations

from typing import Any

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import BetResult, EntryStatus
from app.db.models.signal import Signal
from app.schemas.analytics_summary import AnalyticsFilter
from app.schemas.training_dataset import TrainingDatasetBuildResult, TrainingDatasetRow
from app.schemas.analytics import SignalAnalyticsReport
from app.services.analytics_service import AnalyticsService


class TrainingDatasetService:
    async def build_dataset(
        self, session: AsyncSession, filters: AnalyticsFilter | None = None
    ) -> TrainingDatasetBuildResult:
        """Build a flat training dataset (one row per signal) using existing analytics read-layer.

        - base entity: Signal
        - applies filters to Signal fields only
        - for each signal_id loads full report and flattens it into TrainingDatasetRow
        """
        filters = filters or AnalyticsFilter()
        signal_ids = await self._list_signal_ids(session, filters)

        analytics = AnalyticsService()
        rows: list[TrainingDatasetRow] = []
        for signal_id in signal_ids:
            report = await analytics.get_signal_report(session, signal_id)
            rows.append(self._report_to_row(report))

        return TrainingDatasetBuildResult(rows=rows, total_rows=len(rows))

    async def _list_signal_ids(self, session: AsyncSession, f: AnalyticsFilter) -> list[int]:
        clauses = self._signal_filters(f)
        stmt = select(Signal.id).order_by(Signal.id.asc())
        if clauses:
            stmt = stmt.where(and_(*clauses))
        result = await session.execute(stmt)
        return [int(x) for x in result.scalars().all()]

    def _signal_filters(self, f: AnalyticsFilter) -> list[Any]:
        clauses: list[Any] = []
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

    def _report_to_row(self, report: SignalAnalyticsReport) -> TrainingDatasetRow:
        signal = report.signal

        # Entry selection:
        # - first ENTERED
        # - else first entry if exists
        entered_entry = next((e for e in report.entries if e.status == EntryStatus.ENTERED), None)
        primary_entry = entered_entry or (report.entries[0] if report.entries else None)

        # Prediction log selection: first if exists
        primary_log = report.prediction_logs[0] if report.prediction_logs else None

        # Failure review selection:
        # 1) first with manual_reason not empty
        # 2) else first review
        primary_review = next((r for r in report.failure_reviews if (r.manual_reason or "").strip()), None)
        if primary_review is None and report.failure_reviews:
            primary_review = report.failure_reviews[0]

        entered = entered_entry is not None
        settled = report.settlement is not None

        settlement_result = report.settlement.result if report.settlement is not None else None

        # Targets
        if not settled or settlement_result in {None, BetResult.UNKNOWN}:
            target_outcome_success = None
        elif settlement_result == BetResult.WIN:
            target_outcome_success = 1
        elif settlement_result == BetResult.LOSE:
            target_outcome_success = 0
        else:
            # VOID -> None
            target_outcome_success = None

        target_entry_success = 1 if entered else 0

        if primary_entry is None or primary_entry.entered_odds is None:
            target_is_value_kept = None
        else:
            target_is_value_kept = 1 if primary_entry.entered_odds >= signal.min_entry_odds else 0

        return TrainingDatasetRow(
            signal_id=signal.id,
            sport=signal.sport,
            bookmaker=signal.bookmaker,
            market_type=signal.market_type,
            is_live=signal.is_live,
            model_name=signal.model_name,
            model_version_name=signal.model_version_name,
            signal_status=signal.status,
            signal_created_at=signal.created_at,
            event_start_at=signal.event_start_at,
            odds_at_signal=signal.odds_at_signal,
            min_entry_odds=signal.min_entry_odds,
            predicted_prob=signal.predicted_prob,
            implied_prob=signal.implied_prob,
            edge=signal.edge,
            signal_score=signal.signal_score,
            entered=entered,
            entered_odds=primary_entry.entered_odds if primary_entry is not None else None,
            stake_amount=primary_entry.stake_amount if primary_entry is not None else None,
            entry_delay_seconds=primary_entry.delay_seconds if primary_entry is not None else None,
            was_found_in_bookmaker=primary_entry.was_found_in_bookmaker if primary_entry is not None else None,
            missed_reason=primary_entry.missed_reason if primary_entry is not None else None,
            settled=settled,
            settlement_result=settlement_result,
            profit_loss=report.settlement.profit_loss if report.settlement is not None else None,
            bankroll_before=report.settlement.bankroll_before if report.settlement is not None else None,
            bankroll_after=report.settlement.bankroll_after if report.settlement is not None else None,
            auto_failure_category=primary_review.category if primary_review is not None else None,
            auto_failure_reason=primary_review.auto_reason if primary_review is not None else None,
            manual_failure_reason=primary_review.manual_reason if primary_review is not None else None,
            failure_tags_json=primary_review.failure_tags_json if primary_review is not None else None,
            feature_snapshot_json=primary_log.feature_snapshot_json if primary_log is not None else None,
            raw_model_output_json=primary_log.raw_model_output_json if primary_log is not None else None,
            explanation_json=primary_log.explanation_json if primary_log is not None else None,
            target_outcome_success=target_outcome_success,
            target_entry_success=target_entry_success,
            target_is_value_kept=target_is_value_kept,
        )

