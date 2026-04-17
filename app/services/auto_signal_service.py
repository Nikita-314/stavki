from __future__ import annotations

import asyncio
import logging

from aiogram import Bot
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings, get_settings
from app.providers.odds_http_client import OddsHttpClient
from app.schemas.candidate_filter import CandidateFilterConfig
from app.schemas.auto_signal import AutoSignalCycleResult
from app.schemas.provider_client import ProviderClientConfig
from app.services.adapter_ingestion_service import AdapterIngestionService
from app.services.candidate_filter_service import CandidateFilterService
from app.services.deduplication_service import DeduplicationService
from app.services.ingestion_service import IngestionService
from app.services.orchestration_service import OrchestrationService
from app.services.signal_runtime_settings_service import SignalRuntimeSettingsService


logger = logging.getLogger(__name__)


class AutoSignalService:
    async def run_single_cycle(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        bot: Bot,
    ) -> AutoSignalCycleResult:
        settings = get_settings()
        runtime = SignalRuntimeSettingsService()
        active_sports = [sport.value for sport in runtime.active_sports()]
        if runtime.is_paused():
            logger.info("[WINLINE] cycle skipped: bot paused")
            return AutoSignalCycleResult(
                endpoint=None,
                fetch_ok=False,
                preview_candidates=0,
                preview_skipped_items=0,
                created_signal_ids=[],
                created_signals_count=0,
                skipped_candidates_count=0,
                notifications_sent_count=0,
                preview_only=settings.auto_signal_preview_only,
                message="paused",
                runtime_paused=True,
                runtime_active_sports=active_sports,
            )
        if not active_sports:
            logger.info("[WINLINE] cycle skipped: no active sports selected")
            return AutoSignalCycleResult(
                endpoint=None,
                fetch_ok=False,
                preview_candidates=0,
                preview_skipped_items=0,
                created_signal_ids=[],
                created_signals_count=0,
                skipped_candidates_count=0,
                notifications_sent_count=0,
                preview_only=settings.auto_signal_preview_only,
                message="no_active_sports",
                runtime_paused=False,
                runtime_active_sports=active_sports,
            )
        config = self._build_provider_client_config(settings)
        inferred_sport = self._infer_provider_sport(config) if config is not None else None
        if inferred_sport is not None and not runtime.is_sport_enabled(inferred_sport):
            logger.info(
                "[WINLINE] cycle skipped before fetch: configured source sport disabled sport=%s endpoint=%s",
                inferred_sport.value,
                getattr(config, "base_url", None),
            )
            return AutoSignalCycleResult(
                endpoint=getattr(config, "base_url", None),
                fetch_ok=False,
                preview_candidates=0,
                preview_skipped_items=0,
                created_signal_ids=[],
                created_signals_count=0,
                skipped_candidates_count=0,
                notifications_sent_count=0,
                preview_only=settings.auto_signal_preview_only,
                message=f"sport_disabled:{inferred_sport.value}",
                runtime_paused=False,
                runtime_active_sports=active_sports,
            )
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
                runtime_paused=False,
                runtime_active_sports=active_sports,
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
                runtime_paused=False,
                runtime_active_sports=active_sports,
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
                runtime_paused=False,
                runtime_active_sports=active_sports,
            )

        adapter_service = AdapterIngestionService()
        preview = adapter_service.preview_odds_style_payload(payload)
        raw_events_count = int(preview.total_events)
        normalized_markets_count = int(preview.total_markets)
        preview_candidates = len(preview.candidates)
        preview_skipped_items = int(preview.skipped_items)
        candidates_before_filter = list(preview.candidates)
        runtime_candidates = self._filter_candidates_by_runtime(candidates_before_filter, runtime)
        filtered = CandidateFilterService().filter_candidates(
            runtime_candidates,
            CandidateFilterConfig.default_for_russian_manual_betting(),
        )
        deduped = DeduplicationService().deduplicate_candidates(filtered.accepted_candidates)
        filtered_candidates = list(deduped.unique_candidates)

        logger.info("[WINLINE] raw events: %s", raw_events_count)
        logger.info("[WINLINE] normalized markets: %s", normalized_markets_count)
        logger.info("[WINLINE] candidates before filter: %s", len(candidates_before_filter))
        logger.info("[WINLINE] candidates after filter: %s", len(filtered_candidates))

        if settings.auto_signal_preview_only:
            logger.info("[WINLINE] cycle preview_only enabled; no signal creation")
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
                raw_events_count=raw_events_count,
                normalized_markets_count=normalized_markets_count,
                candidates_before_filter_count=len(candidates_before_filter),
                candidates_after_filter_count=len(filtered_candidates),
                runtime_paused=False,
                runtime_active_sports=active_sports,
            )

        candidates_to_ingest = filtered_candidates
        omitted_by_limit = 0
        limit = settings.auto_signal_max_created_per_cycle
        if limit is not None and limit > 0:
            candidates_to_ingest = candidates_to_ingest[:limit]
            omitted_by_limit = max(0, len(filtered_candidates) - len(candidates_to_ingest))

        async with sessionmaker() as session:
            ingest_res = await IngestionService().ingest_candidates(session, candidates_to_ingest)
            await session.commit()

        notifications_sent_count = 0
        orch = OrchestrationService()
        logger.info("[WINLINE] final signals: %s", ingest_res.created_signals)
        for signal_id in ingest_res.created_signal_ids:
            try:
                async with sessionmaker() as session2:
                    sent = await orch.notify_signal_if_configured(session2, bot, signal_id)
                if sent:
                    notifications_sent_count += 1
            except Exception:
                logger.exception("Auto signal notification failed for signal_id=%s", signal_id)
        logger.info("[WINLINE] messages sent: %s", notifications_sent_count)

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
            raw_events_count=raw_events_count,
            normalized_markets_count=normalized_markets_count,
            candidates_before_filter_count=len(candidates_before_filter),
            candidates_after_filter_count=len(filtered_candidates),
            runtime_paused=False,
            runtime_active_sports=active_sports,
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
                    "notifications=%s preview_only=%s paused=%s active_sports=%s message=%s",
                    result.endpoint,
                    result.fetch_ok,
                    result.preview_candidates,
                    result.created_signals_count,
                    result.skipped_candidates_count,
                    result.notifications_sent_count,
                    result.preview_only,
                    result.runtime_paused,
                    result.runtime_active_sports,
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

    def _filter_candidates_by_runtime(self, candidates, runtime: SignalRuntimeSettingsService):
        accepted = []
        for candidate in candidates:
            sport = getattr(getattr(candidate, "match", None), "sport", None)
            if sport is None:
                continue
            if runtime.is_sport_enabled(sport):
                accepted.append(candidate)
            else:
                logger.info(
                    "[WINLINE] candidate skipped by runtime sport filter: sport=%s event=%s match=%s",
                    getattr(sport, "value", sport),
                    getattr(getattr(candidate, "match", None), "external_event_id", None),
                    getattr(getattr(candidate, "match", None), "match_name", None),
                )
        return accepted

    def _infer_provider_sport(self, config: ProviderClientConfig | None):
        if config is None:
            return None
        joined = " ".join(
            [
                str(config.base_url or ""),
                str(config.sport or ""),
            ]
        ).lower()
        if any(token in joined for token in ("soccer", "football", "epl")):
            from app.core.enums import SportType

            return SportType.FOOTBALL
        if any(token in joined for token in ("counterstrike", "counter_strike", "cs2", "cs_")):
            from app.core.enums import SportType

            return SportType.CS2
        if any(token in joined for token in ("dota2", "dota 2", "dota")):
            from app.core.enums import SportType

            return SportType.DOTA2
        return None

