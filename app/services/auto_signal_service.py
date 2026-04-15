from __future__ import annotations

import asyncio
import logging

from aiogram import Bot
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings, get_settings
from app.providers.odds_http_client import OddsHttpClient
from app.schemas.auto_signal import AutoSignalCycleResult
from app.schemas.provider_client import ProviderClientConfig
from app.services.adapter_ingestion_service import AdapterIngestionService
from app.services.ingestion_service import IngestionService
from app.services.orchestration_service import OrchestrationService


logger = logging.getLogger(__name__)


class AutoSignalService:
    async def run_single_cycle(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        bot: Bot,
    ) -> AutoSignalCycleResult:
        settings = get_settings()
        config = self._build_provider_client_config(settings)
        if config is None:
            return AutoSignalCycleResult(
                endpoint=None,
                fetch_ok=False,
                preview_candidates=0,
                preview_skipped_items=0,
                created_signal_ids=[],
                created_signals_count=0,
                skipped_candidates_count=0,
                notifications_sent_count=0,
                preview_only=False,
                message="provider_not_configured",
            )

        fetch_res = await asyncio.to_thread(OddsHttpClient().fetch, config)
        if not fetch_res.ok:
            return AutoSignalCycleResult(
                endpoint=fetch_res.endpoint,
                fetch_ok=False,
                preview_candidates=0,
                preview_skipped_items=0,
                created_signal_ids=[],
                created_signals_count=0,
                skipped_candidates_count=0,
                notifications_sent_count=0,
                preview_only=False,
                message=str(fetch_res.error or "fetch_error"),
            )

        payload = fetch_res.payload
        if not isinstance(payload, dict):
            return AutoSignalCycleResult(
                endpoint=fetch_res.endpoint,
                fetch_ok=False,
                preview_candidates=0,
                preview_skipped_items=0,
                created_signal_ids=[],
                created_signals_count=0,
                skipped_candidates_count=0,
                notifications_sent_count=0,
                preview_only=False,
                message="payload_is_not_dict",
            )

        adapter_service = AdapterIngestionService()
        preview = adapter_service.preview_odds_style_payload(payload)
        preview_candidates = len(preview.candidates)
        preview_skipped_items = int(preview.skipped_items)

        if settings.auto_signal_preview_only:
            return AutoSignalCycleResult(
                endpoint=fetch_res.endpoint,
                fetch_ok=True,
                preview_candidates=preview_candidates,
                preview_skipped_items=preview_skipped_items,
                created_signal_ids=[],
                created_signals_count=0,
                skipped_candidates_count=0,
                notifications_sent_count=0,
                preview_only=True,
                message="preview_only",
            )

        candidates_to_ingest = list(preview.candidates)
        omitted_by_limit = 0
        limit = settings.auto_signal_max_created_per_cycle
        if limit is not None and limit > 0:
            candidates_to_ingest = candidates_to_ingest[:limit]
            omitted_by_limit = max(0, preview_candidates - len(candidates_to_ingest))

        async with sessionmaker() as session:
            ingest_res = await IngestionService().ingest_candidates_with_filter_and_dedup(session, candidates_to_ingest)
            await session.commit()

        notifications_sent_count = 0
        orch = OrchestrationService()
        for signal_id in ingest_res.created_signal_ids:
            try:
                async with sessionmaker() as session2:
                    sent = await orch.notify_signal_if_configured(session2, bot, signal_id)
                if sent:
                    notifications_sent_count += 1
            except Exception:
                logger.exception("Auto signal notification failed for signal_id=%s", signal_id)

        return AutoSignalCycleResult(
            endpoint=fetch_res.endpoint,
            fetch_ok=True,
            preview_candidates=preview_candidates,
            preview_skipped_items=preview_skipped_items,
            created_signal_ids=list(ingest_res.created_signal_ids),
            created_signals_count=int(ingest_res.created_signals),
            skipped_candidates_count=int(ingest_res.skipped_candidates + omitted_by_limit),
            notifications_sent_count=notifications_sent_count,
            preview_only=False,
            message="ok",
        )

    async def run_forever(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        bot: Bot,
    ) -> None:
        settings = get_settings()
        if not settings.auto_signal_polling_enabled:
            return

        while True:
            try:
                result = await self.run_single_cycle(sessionmaker, bot)
                logger.info(
                    "Auto signal cycle: endpoint=%s fetch_ok=%s preview_candidates=%s created=%s skipped=%s "
                    "notifications=%s preview_only=%s message=%s",
                    result.endpoint,
                    result.fetch_ok,
                    result.preview_candidates,
                    result.created_signals_count,
                    result.skipped_candidates_count,
                    result.notifications_sent_count,
                    result.preview_only,
                    result.message,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Auto signal cycle failed")

            interval = max(1, int(get_settings().auto_signal_polling_interval_seconds))
            await asyncio.sleep(interval)

    def _build_provider_client_config(self, settings: Settings) -> ProviderClientConfig | None:
        if not settings.odds_provider_base_url:
            return None
        return ProviderClientConfig(
            base_url=settings.odds_provider_base_url,
            api_key=settings.odds_provider_api_key,
            sport=settings.odds_provider_sport,
            regions=settings.odds_provider_regions,
            markets=settings.odds_provider_markets,
            bookmakers=settings.odds_provider_bookmakers,
            odds_format=settings.odds_provider_odds_format,
            date_format=settings.odds_provider_date_format,
            timeout_seconds=int(settings.odds_provider_timeout_seconds),
        )

