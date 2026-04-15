from __future__ import annotations

import asyncio

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.enums import SportType
from app.providers.odds_http_client import OddsHttpClient
from app.schemas.event_result import EventResultInput, EventResultProcessingResult
from app.schemas.provider_client import ProviderClientConfig
from app.schemas.remote_smoke import RemoteSmokeResult
from app.services.adapter_ingestion_service import AdapterIngestionService
from app.services.analytics_summary_service import AnalyticsSummaryService
from app.services.balance_service import BalanceService
from app.services.orchestration_service import OrchestrationService
from app.services.sanity_check_service import SanityCheckService


class RemoteSmokeService:
    async def run_remote_smoke(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        *,
        config: ProviderClientConfig,
    ) -> RemoteSmokeResult:
        # Step 1: fetch (sync call in thread)
        fetch_res = await asyncio.to_thread(OddsHttpClient().fetch, config)
        if not fetch_res.ok:
            return RemoteSmokeResult(
                endpoint=getattr(fetch_res, "endpoint", None),
                fetch_ok=False,
                preview_candidates=0,
                preview_skipped_items=0,
                ingested_created_signals=0,
                ingested_skipped_candidates=0,
                created_signal_ids=[],
                sanity_issues_count=0,
                total_signals=0,
                settled_signals=0,
                current_balance_rub=None,
                message=f"fetch_error: {fetch_res.error}",
            )

        payload = fetch_res.payload
        if not isinstance(payload, dict):
            return RemoteSmokeResult(
                endpoint=getattr(fetch_res, "endpoint", None),
                fetch_ok=False,
                preview_candidates=0,
                preview_skipped_items=0,
                ingested_created_signals=0,
                ingested_skipped_candidates=0,
                created_signal_ids=[],
                sanity_issues_count=0,
                total_signals=0,
                settled_signals=0,
                current_balance_rub=None,
                message="fetch_error: payload_is_not_dict",
            )

        # Step 2: preview via OddsStyleAdapter
        preview = AdapterIngestionService().preview_odds_style_payload(payload)
        preview_candidates = int(len(preview.candidates))
        preview_skipped_items = int(preview.skipped_items)

        # Step 3: ingest + commit
        async with sessionmaker() as session:
            adapter_res, ing = await AdapterIngestionService().ingest_odds_style_payload(session, payload)
            await session.commit()

        # Step 4: post-checks in a new session
        async with sessionmaker() as session2:
            sanity = await SanityCheckService().run_sanity_check(session2)
            summary = await AnalyticsSummaryService().get_summary(session2)
            balance = await BalanceService().get_realistic_balance_overview(session2)

        return RemoteSmokeResult(
            endpoint=getattr(fetch_res, "endpoint", None),
            fetch_ok=True,
            preview_candidates=preview_candidates,
            preview_skipped_items=preview_skipped_items,
            ingested_created_signals=int(ing.created_signals),
            ingested_skipped_candidates=int(ing.skipped_candidates),
            created_signal_ids=list(ing.created_signal_ids),
            sanity_issues_count=int(sanity.issues_count),
            total_signals=int(summary.kpis.total_signals),
            settled_signals=int(summary.kpis.settled_signals),
            current_balance_rub=balance.current_balance_rub,
            message="ok",
        )

    async def settle_latest_remote_signal(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        *,
        winner_selection: str,
        sport: SportType,
        event_external_id: str,
    ) -> EventResultProcessingResult:
        data = EventResultInput(
            event_external_id=event_external_id,
            sport=sport,
            winner_selection=winner_selection,
            is_void=False,
        )

        async with sessionmaker() as session:
            res = await OrchestrationService().process_event_result(session, data)
            await session.commit()
            return res.result

