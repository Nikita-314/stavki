from __future__ import annotations

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import BetResult, EntryStatus, SignalStatus
from app.db.models.entry import Entry
from app.db.models.failure_review import FailureReview
from app.db.models.settlement import Settlement
from app.db.models.signal import Signal
from app.schemas.sanity_check import SanityCheckReport, SanityIssueItem


class SanityCheckService:
    async def run_sanity_check(self, session: AsyncSession) -> SanityCheckReport:
        total_signals = int((await session.execute(select(func.count(Signal.id)))).scalar_one() or 0)
        total_settlements = int((await session.execute(select(func.count(Settlement.id)))).scalar_one() or 0)
        total_failure_reviews = int((await session.execute(select(func.count(FailureReview.id)))).scalar_one() or 0)
        total_entries = int((await session.execute(select(func.count(Entry.id)))).scalar_one() or 0)

        issues: list[SanityIssueItem] = []

        # a) Settlement without Signal
        orphan_settlements = (
            await session.execute(
                select(Settlement.id, Settlement.signal_id)
                .outerjoin(Signal, Signal.id == Settlement.signal_id)
                .where(Signal.id.is_(None))
            )
        ).all()
        for settlement_id, signal_id in orphan_settlements:
            issues.append(
                SanityIssueItem(
                    issue_type="orphan_settlement",
                    signal_id=int(signal_id) if signal_id is not None else None,
                    details=f"settlement_id={settlement_id}",
                )
            )

        # b) FailureReview without Signal
        orphan_reviews = (
            await session.execute(
                select(FailureReview.id, FailureReview.signal_id)
                .outerjoin(Signal, Signal.id == FailureReview.signal_id)
                .where(Signal.id.is_(None))
            )
        ).all()
        for review_id, signal_id in orphan_reviews:
            issues.append(
                SanityIssueItem(
                    issue_type="orphan_failure_review",
                    signal_id=int(signal_id) if signal_id is not None else None,
                    details=f"failure_review_id={review_id}",
                )
            )

        # c) Signal.status == SETTLED but settlement missing
        missing_settlement = (
            await session.execute(
                select(Signal.id)
                .outerjoin(Settlement, Settlement.signal_id == Signal.id)
                .where(Signal.status == SignalStatus.SETTLED)
                .where(Settlement.id.is_(None))
            )
        ).scalars().all()
        for sid in missing_settlement:
            issues.append(
                SanityIssueItem(
                    issue_type="settled_status_without_settlement",
                    signal_id=int(sid),
                    details="signal.status=SETTLED but no settlement",
                )
            )

        # d) settlement exists but Signal.status != SETTLED
        settlement_without_status = (
            await session.execute(
                select(Signal.id, Signal.status)
                .join(Settlement, Settlement.signal_id == Signal.id)
                .where(Signal.status != SignalStatus.SETTLED)
            )
        ).all()
        for sid, status in settlement_without_status:
            issues.append(
                SanityIssueItem(
                    issue_type="settlement_without_settled_status",
                    signal_id=int(sid),
                    details=f"signal.status={getattr(status, 'value', status)} but has settlement",
                )
            )

        # e) failed settlement but no failure_reviews
        failed_no_reviews = (
            await session.execute(
                select(Signal.id)
                .join(Settlement, Settlement.signal_id == Signal.id)
                .outerjoin(FailureReview, FailureReview.signal_id == Signal.id)
                .where(Settlement.result.in_([BetResult.LOSE, BetResult.VOID]))
                .group_by(Signal.id)
                .having(func.count(FailureReview.id) == 0)
            )
        ).scalars().all()
        for sid in failed_no_reviews:
            issues.append(
                SanityIssueItem(
                    issue_type="missing_failure_review_for_failed_signal",
                    signal_id=int(sid),
                    details="settlement.result in (LOSE, VOID) but failure_reviews is empty",
                )
            )

        # f) failure_reviews exist but settlement missing
        reviews_no_settlement = (
            await session.execute(
                select(Signal.id)
                .join(FailureReview, FailureReview.signal_id == Signal.id)
                .outerjoin(Settlement, Settlement.signal_id == Signal.id)
                .where(Settlement.id.is_(None))
                .group_by(Signal.id)
            )
        ).scalars().all()
        for sid in reviews_no_settlement:
            issues.append(
                SanityIssueItem(
                    issue_type="failure_review_without_settlement",
                    signal_id=int(sid),
                    details="failure_reviews exist but settlement is missing",
                )
            )

        # g) Signal.status == ENTERED but no Entry with status ENTERED
        entered_without_entry = (
            await session.execute(
                select(Signal.id)
                .outerjoin(
                    Entry,
                    and_(Entry.signal_id == Signal.id, Entry.status == EntryStatus.ENTERED),
                )
                .where(Signal.status == SignalStatus.ENTERED)
                .where(Entry.id.is_(None))
            )
        ).scalars().all()
        for sid in entered_without_entry:
            issues.append(
                SanityIssueItem(
                    issue_type="entered_status_without_entered_entry",
                    signal_id=int(sid),
                    details="signal.status=ENTERED but no entry.status=ENTERED",
                )
            )

        # h) Signal.status == MISSED but no Entry with status SKIPPED/REJECTED
        missed_without_entry = (
            await session.execute(
                select(Signal.id)
                .outerjoin(
                    Entry,
                    and_(
                        Entry.signal_id == Signal.id,
                        Entry.status.in_([EntryStatus.SKIPPED, EntryStatus.REJECTED]),
                    ),
                )
                .where(Signal.status == SignalStatus.MISSED)
                .where(Entry.id.is_(None))
            )
        ).scalars().all()
        for sid in missed_without_entry:
            issues.append(
                SanityIssueItem(
                    issue_type="missed_status_without_missed_entry",
                    signal_id=int(sid),
                    details="signal.status=MISSED but no entry.status in (SKIPPED, REJECTED)",
                )
            )

        return SanityCheckReport(
            total_signals=total_signals,
            total_settlements=total_settlements,
            total_failure_reviews=total_failure_reviews,
            total_entries=total_entries,
            issues_count=len(issues),
            issues=issues,
        )

