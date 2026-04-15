from __future__ import annotations

from decimal import Decimal

from aiogram import Bot
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.enums import SportType
from app.providers.mock_candidate_provider import MockCandidateProvider
from app.schemas.candidate_filter import CandidateFilterConfig
from app.schemas.demo_cycle import DemoCycleResult
from app.schemas.event_result import EventResultInput
from app.services.balance_service import BalanceService
from app.services.candidate_filter_service import CandidateFilterService
from app.services.orchestration_service import OrchestrationService


class DemoCycleService:
    async def run_mock_demo_cycle(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        bot: Bot,
        *,
        sport: SportType | None = None,
    ) -> DemoCycleResult:
        # Stage 1: pick mock candidate
        candidates = await MockCandidateProvider().fetch_candidates()
        config = CandidateFilterConfig.default_for_russian_manual_betting()
        filtered = CandidateFilterService().filter_candidates(candidates, config)

        accepted = filtered.accepted_candidates
        if sport is not None:
            accepted = [c for c in accepted if c.match.sport == sport]

        if not accepted:
            return DemoCycleResult(
                created_signal_id=None,
                signal_notification_sent=False,
                result_processed=False,
                result_notification_sent_count=0,
                total_signals_found=0,
                settled_signals=0,
                skipped_signals=0,
                created_failure_reviews=0,
                processed_signal_ids=[],
                balance_mode_unit_current=None,
                balance_mode_rub_current=None,
                message="no suitable candidate",
            )

        candidate = accepted[0]
        orch = OrchestrationService()

        # Stage 2: create signal (commit), then notify post-commit
        async with sessionmaker() as session:
            create_res = await orch.create_signal(session, candidate)
            if create_res.signal_id is None:
                return DemoCycleResult(
                    created_signal_id=None,
                    signal_notification_sent=False,
                    result_processed=False,
                    result_notification_sent_count=0,
                    total_signals_found=0,
                    settled_signals=0,
                    skipped_signals=0,
                    created_failure_reviews=0,
                    processed_signal_ids=[],
                    balance_mode_unit_current=None,
                    balance_mode_rub_current=None,
                    message=f"candidate skipped: {create_res.skipped_reason}",
                )
            created_signal_id = int(create_res.signal_id)
            await session.commit()

        signal_notification_sent = False
        try:
            async with sessionmaker() as session2:
                signal_notification_sent = await orch.notify_signal_if_configured(session2, bot, created_signal_id)
        except Exception:
            signal_notification_sent = False

        # Stage 3: process result for the same event (commit)
        data = EventResultInput(
            event_external_id=candidate.match.external_event_id,
            sport=candidate.match.sport,
            winner_selection=candidate.market.selection,
        )

        async with sessionmaker() as session3:
            proc_res = await orch.process_event_result(session3, data)
            await session3.commit()

        # Stage 4: post-commit result notifications
        result_notification_sent_count = 0
        try:
            async with sessionmaker() as session4:
                for sid in proc_res.signal_ids_to_notify:
                    try:
                        sent = await orch.notify_result_if_configured(session4, bot, sid)
                        if sent:
                            result_notification_sent_count += 1
                    except Exception:
                        continue
        except Exception:
            result_notification_sent_count = result_notification_sent_count

        # Stage 5: balances
        balance_mode_unit_current: Decimal | None = None
        balance_mode_rub_current: Decimal | None = None
        try:
            async with sessionmaker() as session5:
                unit_overview = await BalanceService().get_balance_overview(session5)
                rub_overview = await BalanceService().get_realistic_balance_overview(session5)
                balance_mode_unit_current = unit_overview.current_balance
                balance_mode_rub_current = rub_overview.current_balance_rub
        except Exception:
            balance_mode_unit_current = None
            balance_mode_rub_current = None

        r = proc_res.result
        return DemoCycleResult(
            created_signal_id=created_signal_id,
            signal_notification_sent=bool(signal_notification_sent),
            result_processed=True,
            result_notification_sent_count=int(result_notification_sent_count),
            total_signals_found=int(r.total_signals_found),
            settled_signals=int(r.settled_signals),
            skipped_signals=int(r.skipped_signals),
            created_failure_reviews=int(r.created_failure_reviews),
            processed_signal_ids=list(r.processed_signal_ids),
            balance_mode_unit_current=balance_mode_unit_current,
            balance_mode_rub_current=balance_mode_rub_current,
            message="ok",
        )

