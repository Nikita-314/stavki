from __future__ import annotations

import logging
from decimal import Decimal

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import BetResult, SportType
from app.db.repositories.signal_repository import SignalRepository
from app.schemas.event_result import EventResultInput, EventResultProcessingResult
from app.schemas.settlement import SettlementCreate
from app.services.failure_review_service import FailureReviewService
from app.services.football_signal_outcome_reason_service import FootballSignalOutcomeReasonService
from app.services.openai_signal_analysis_service import OpenAISignalAnalysisService
from app.services.settlement_service import SettlementService

logger = logging.getLogger(__name__)


class ResultIngestionService:
    async def process_event_result(self, session: AsyncSession, data: EventResultInput) -> EventResultProcessingResult:
        """Process an external event result and auto-settle matching signals (no commit).

        This is an analytical auto-settlement (unit-based P/L), not an execution ledger.
        """
        signals = await SignalRepository().list_unsettled_by_event_external_id(session, data.event_external_id, data.sport)

        processed_ids: list[int] = []
        settled_count = 0
        skipped = 0
        created_reviews = 0

        supported_market_types = {"match_winner", "map_winner", "1x2"}

        for s in signals:
            if s.market_type not in supported_market_types:
                skipped += 1
                continue

            result = self._determine_result(
                is_void=data.is_void,
                winner_selection=data.winner_selection,
                signal_selection=s.selection,
            )
            if result is None:
                skipped += 1
                continue

            profit_loss = self._unit_profit_loss(result=result, odds_at_signal=s.odds_at_signal)

            await SettlementService().register_settlement(
                session,
                SettlementCreate(
                    signal_id=s.id,
                    result=result,
                    profit_loss=profit_loss,
                    settled_at=data.settled_at,
                    result_details=None,
                    bankroll_before=None,
                    bankroll_after=None,
                ),
            )
            if s.sport == SportType.FOOTBALL:
                try:
                    await FootballSignalOutcomeReasonService().apply_to_signal(session, s, result, data)
                except Exception:
                    logger.exception("football outcome reason apply failed (signal_id=%s)", s.id)
                # OpenAI analysis is strictly post-settlement and best-effort (must never break settlement pipeline).
                if bool(s.is_live):
                    try:
                        await OpenAISignalAnalysisService().analyze_settled_live_football_signal(
                            session, signal_id=int(s.id)
                        )
                    except Exception:
                        logger.exception("openai post-settlement analysis failed (signal_id=%s)", s.id)
            settled_count += 1
            processed_ids.append(int(s.id))

            if result in {BetResult.LOSE, BetResult.UNKNOWN, BetResult.VOID}:
                await FailureReviewService().register_auto_failure_review(session, s.id)
                created_reviews += 1

        return EventResultProcessingResult(
            total_signals_found=len(signals),
            settled_signals=settled_count,
            skipped_signals=skipped,
            created_failure_reviews=created_reviews,
            processed_signal_ids=processed_ids,
        )

    def _determine_result(
        self,
        *,
        is_void: bool,
        winner_selection: str | None,
        signal_selection: str,
    ) -> BetResult | None:
        if is_void:
            return BetResult.VOID
        if winner_selection is None:
            return None

        sel = (signal_selection or "").strip().lower()
        win = (winner_selection or "").strip().lower()
        return BetResult.WIN if sel == win else BetResult.LOSE

    def _unit_profit_loss(self, *, result: BetResult, odds_at_signal: Decimal) -> Decimal:
        if result == BetResult.WIN:
            return odds_at_signal - Decimal("1")
        if result == BetResult.LOSE:
            return Decimal("-1")
        if result == BetResult.VOID:
            return Decimal("0")
        return Decimal("0")

