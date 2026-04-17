from __future__ import annotations

import asyncio
import logging

from aiogram import Bot
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.enums import SportType
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
from app.services.football_signal_send_filter_service import FootballSignalSendFilterService
from app.services.signal_runtime_diagnostics_service import SignalRuntimeDiagnosticsService
from app.services.signal_runtime_settings_service import SignalRuntimeSettingsService
from app.services.winline_manual_cycle_service import WinlineManualCycleService


logger = logging.getLogger(__name__)


class AutoSignalService:
    async def run_single_cycle(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        bot: Bot,
    ) -> AutoSignalCycleResult:
        settings = get_settings()
        runtime = SignalRuntimeSettingsService()
        diagnostics = SignalRuntimeDiagnosticsService()
        active_sports = [sport.value for sport in runtime.active_sports()]
        logger.info("[FOOTBALL] cycle started")
        logger.info("[FOOTBALL] paused state: %s", str(runtime.is_paused()).upper())
        diagnostics.update(
            active_mode="football" if SportType.FOOTBALL.value in active_sports else "inactive",
            football_source=self._detect_provider_name(settings),
            football_fallback_source="manual_winline_json",
            preview_only=bool(settings.auto_signal_preview_only),
            fallback_used=False,
            last_error=None,
            note=None,
            football_candidates_count=0,
            football_after_filter_count=0,
            football_sent_count=0,
        )
        if runtime.is_paused():
            logger.info("[FOOTBALL][BLOCK] skipped due to paused")
            diagnostics.update(
                last_fetch_status="paused",
                note="delivery skipped: paused",
            )
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
                source_name=self._detect_provider_name(settings),
                rejection_reason="delivery skipped: paused",
            )
        if not runtime.is_sport_enabled(SportType.FOOTBALL):
            logger.info("[FOOTBALL] fetch skipped: football disabled in runtime")
            diagnostics.update(
                last_fetch_status="football_disabled",
                note="filtered by runtime sport settings",
            )
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
                message="football_disabled",
                runtime_paused=False,
                runtime_active_sports=active_sports,
                source_name=self._detect_provider_name(settings),
                rejection_reason="filtered by runtime sport settings",
            )
        config = self._build_provider_client_config(settings)
        inferred_sport = self._infer_provider_sport(config) if config is not None else None
        if inferred_sport is not None and not runtime.is_sport_enabled(inferred_sport):
            logger.info("[FOOTBALL] fetch skipped: configured source sport disabled source=%s", inferred_sport.value)
            diagnostics.update(
                last_fetch_status=f"sport_disabled:{inferred_sport.value.lower()}",
                note="filtered by runtime sport settings",
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
                source_name=self._detect_provider_name(settings),
                rejection_reason="filtered by runtime sport settings",
            )
        if config is None:
            logger.info("[FOOTBALL] fetch skipped: provider not configured")
            diagnostics.update(
                last_fetch_status="provider_not_configured",
                last_error="provider_not_configured",
            )
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
                source_name=self._detect_provider_name(settings),
                rejection_reason="provider_not_configured",
            )

        logger.info("[FOOTBALL] fetch started")
        logger.info("[FOOTBALL] fetch source=%s endpoint=%s", self._detect_provider_name(settings), config.base_url)
        fetch_res = await asyncio.to_thread(OddsHttpClient().fetch, config)
        preview = None
        payload = None
        source_name = self._detect_provider_name(settings)
        fallback_used = False
        fallback_source_name = None
        source_kind = "live"

        if fetch_res.ok and isinstance(fetch_res.payload, dict):
            payload = fetch_res.payload
            source_name = str(fetch_res.source_name or source_name)
            diagnostics.update(last_fetch_status="ok")
        else:
            err = str(fetch_res.error or "fetch_error")
            diagnostics.update(
                last_fetch_status=err,
                last_error=err,
            )
            logger.info("[FOOTBALL] fetch source=%s failed: %s", source_name, err)
            if "Unauthorized" in err or "provider_not_configured" in err or "fetch_error" in err:
                fallback = self._build_manual_football_fallback_preview()
                if fallback is None:
                    logger.info("[FOOTBALL] provider unauthorized and no football fallback payload available")
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
                        message=err,
                        runtime_paused=False,
                        runtime_active_sports=active_sports,
                        source_name=source_name,
                        rejection_reason="provider unauthorized",
                    )
                preview = fallback["preview"]
                payload = fallback["payload"]
                fallback_used = True
                fallback_source_name = "manual_winline_json"
                source_kind = "fallback"
                diagnostics.update(
                    last_fetch_status="fallback_manual_payload",
                    fallback_used=True,
                    note="provider unauthorized; fallback to manual football payload",
                )
                logger.info("[FOOTBALL] fetch source=%s unauthorized; fallback source=%s", source_name, fallback_source_name)
            else:
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
                    message=err,
                    runtime_paused=False,
                    runtime_active_sports=active_sports,
                    source_name=source_name,
                    rejection_reason=err,
                )

        if payload is None or not isinstance(payload, dict):
            diagnostics.update(last_fetch_status="payload_is_not_dict", last_error="payload_is_not_dict")
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
                source_name=source_name,
                fallback_used=fallback_used,
                fallback_source_name=fallback_source_name,
                rejection_reason="payload_is_not_dict",
            )

        if preview is None:
            adapter_service = AdapterIngestionService()
            preview = adapter_service.preview_odds_style_payload(payload)
        logger.info("[FOOTBALL] source: %s", source_kind)
        raw_events_count = int(preview.total_events)
        normalized_markets_count = int(preview.total_markets)
        preview_candidates = len(preview.candidates)
        preview_skipped_items = int(preview.skipped_items)
        candidates_before_filter = list(preview.candidates)
        logger.info("[FOOTBALL] raw events fetched: %s", raw_events_count)
        if not candidates_before_filter:
            logger.info("[FOOTBALL] candidates before filter: 0 (no football events in payload)")
        logger.info("[FOOTBALL] candidates total: %s", len(candidates_before_filter))
        self._log_candidates_per_match(candidates_before_filter)
        runtime_candidates = self._filter_candidates_by_runtime(candidates_before_filter, runtime)
        filtered = CandidateFilterService().filter_candidates(
            runtime_candidates,
            CandidateFilterConfig.default_for_russian_manual_betting(),
        )
        deduped = DeduplicationService().deduplicate_candidates(filtered.accepted_candidates)
        filtered_candidates = list(deduped.unique_candidates)

        logger.info("[FOOTBALL] raw events: %s", raw_events_count)
        logger.info("[FOOTBALL] normalized markets: %s", normalized_markets_count)
        logger.info("[FOOTBALL] candidates before filter: %s", len(candidates_before_filter))
        logger.info("[FOOTBALL] candidates after filter: %s", len(filtered_candidates))
        diagnostics.update(
            raw_events_count=raw_events_count,
            normalized_markets_count=normalized_markets_count,
            candidates_before_filter_count=len(candidates_before_filter),
            candidates_after_filter_count=len(filtered_candidates),
            football_candidates_count=len(candidates_before_filter),
            football_source=source_name,
            football_fallback_source=fallback_source_name,
            fallback_used=fallback_used,
        )
        if not filtered_candidates:
            reject_reason = self._resolve_zero_candidate_reason(
                preview_candidates=preview_candidates,
                runtime_candidates_count=len(runtime_candidates),
                filtered_accepted_count=filtered.accepted_count,
                deduped_count=len(deduped.unique_candidates),
                filter_rejections=filtered.rejection_reasons,
            )
            logger.info("[FOOTBALL] candidates after filter: 0 (%s)", reject_reason)

        if settings.auto_signal_preview_only:
            logger.info("[FOOTBALL] final signals: 0 (preview_only enabled)")
            diagnostics.update(
                final_signals_count=0,
                messages_sent_count=0,
                note="preview_only enabled",
            )
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
                source_name=source_name,
                fallback_used=fallback_used,
                fallback_source_name=fallback_source_name,
                rejection_reason="preview_only enabled",
            )

        candidates_to_ingest = filtered_candidates
        if fallback_used:
            candidates_to_ingest = [
                c.model_copy(
                    update={
                        "notes": "fallback_json",
                        "feature_snapshot_json": {
                            **(c.feature_snapshot_json or {}),
                            "runtime_source_kind": "fallback_json",
                            "runtime_primary_source": source_name,
                        },
                    }
                )
                for c in candidates_to_ingest
            ]
        logger.info("[FOOTBALL] final before send filter: %s", len(candidates_to_ingest))
        if settings.football_debug_disable_filter:
            logger.info("[FOOTBALL][DEBUG] filter disabled, sending raw candidates")
            candidates_to_ingest = candidates_to_ingest[:3]
            diagnostics.update(football_after_filter_count=len(candidates_to_ingest))
        else:
            football_send_filter = FootballSignalSendFilterService()
            send_filter_result = football_send_filter.filter_auto_send_candidates(candidates_to_ingest)
            logger.info("[FOOTBALL] after family whitelist: %s", send_filter_result.stats.after_whitelist)
            logger.info("[FOOTBALL] after ranking: %s", send_filter_result.stats.after_ranking)
            logger.info("[FOOTBALL] after family dedup: %s", send_filter_result.stats.after_family_dedup)
            logger.info("[FOOTBALL] after per-match cap: %s", send_filter_result.stats.after_per_match_cap)
            candidates_to_ingest = send_filter_result.candidates
            diagnostics.update(football_after_filter_count=len(candidates_to_ingest))
        post_send_filter_count = len(candidates_to_ingest)
        if not candidates_to_ingest:
            diagnostics.update(
                final_signals_count=0,
                messages_sent_count=0,
                football_after_filter_count=0,
                football_sent_count=0,
                note="football send filter rejected all signals",
            )
            return AutoSignalCycleResult(
                endpoint=fetch_res.endpoint,
                fetch_ok=True,
                preview_candidates=preview_candidates,
                preview_skipped_items=preview_skipped_items,
                created_signal_ids=[],
                created_signals_count=0,
                skipped_candidates_count=max(0, len(filtered_candidates) - post_send_filter_count),
                notifications_sent_count=0,
                preview_only=False,
                message="ok",
                raw_events_count=raw_events_count,
                normalized_markets_count=normalized_markets_count,
                candidates_before_filter_count=len(candidates_before_filter),
                candidates_after_filter_count=len(filtered_candidates),
                runtime_paused=False,
                runtime_active_sports=active_sports,
                source_name=source_name,
                fallback_used=fallback_used,
                fallback_source_name=fallback_source_name,
                rejection_reason="football send filter rejected all signals",
            )
        omitted_by_limit = 0
        limit = settings.auto_signal_max_created_per_cycle
        if limit is not None and limit > 0:
            candidates_to_ingest = candidates_to_ingest[:limit]
            omitted_by_limit = max(0, post_send_filter_count - len(candidates_to_ingest))

        logger.info("[FOOTBALL] final signals to send: %s", len(candidates_to_ingest))
        self._log_final_candidates(candidates_to_ingest)

        async with sessionmaker() as session:
            ingest_res = await IngestionService().ingest_candidates(session, candidates_to_ingest)
            await session.commit()

        notifications_sent_count = 0
        orch = OrchestrationService()
        logger.info("[FOOTBALL] final signals: %s", ingest_res.created_signals)
        for signal_id in ingest_res.created_signal_ids:
            try:
                async with sessionmaker() as session2:
                    sent = await orch.notify_signal_if_configured(session2, bot, signal_id)
                if sent:
                    notifications_sent_count += 1
            except Exception:
                logger.exception("Auto signal notification failed for signal_id=%s", signal_id)
        logger.info("[FOOTBALL] messages sent: %s", notifications_sent_count)
        diagnostics.update(
            final_signals_count=int(ingest_res.created_signals),
            messages_sent_count=notifications_sent_count,
            football_sent_count=notifications_sent_count,
            note=None if ingest_res.created_signals else "no created football signals",
        )

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
            source_name=source_name,
            fallback_used=fallback_used,
            fallback_source_name=fallback_source_name,
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
                    "Football auto cycle: source=%s endpoint=%s fetch_ok=%s preview_candidates=%s created=%s skipped=%s "
                    "notifications=%s preview_only=%s paused=%s active_sports=%s fallback=%s message=%s",
                    result.source_name,
                    result.endpoint,
                    result.fetch_ok,
                    result.preview_candidates,
                    result.created_signals_count,
                    result.skipped_candidates_count,
                    result.notifications_sent_count,
                    result.preview_only,
                    result.runtime_paused,
                    result.runtime_active_sports,
                    result.fallback_used,
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
                    "[FOOTBALL] candidate skipped by runtime sport filter: sport=%s event=%s match=%s",
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

    def _detect_provider_name(self, settings: Settings) -> str:
        base = str(settings.odds_provider_base_url or "").lower()
        if "the-odds-api" in base:
            return "the_odds_api"
        return "odds_http"

    def _build_manual_football_fallback_preview(self):
        svc = WinlineManualCycleService()
        raw, err = svc._manual.load_line_payload()
        if raw is None or err:
            return None
        normalized, nerr = svc._normalize_line_or_error(raw)
        if normalized is None or nerr:
            return None
        preview = AdapterIngestionService().preview_payload(normalized)
        football_candidates = [
            c
            for c in preview.candidates
            if getattr(getattr(c, "match", None), "sport", None) == SportType.FOOTBALL
        ]
        football_candidates = [
            c.model_copy(
                update={
                    "notes": "fallback_json",
                    "feature_snapshot_json": {
                        **(c.feature_snapshot_json or {}),
                        "runtime_source_kind": "fallback_json",
                        "runtime_primary_source": "the_odds_api",
                    },
                }
            )
            for c in football_candidates
        ]
        preview = preview.model_copy(
            update={
                "total_events": len(normalized.get("events") or []),
                "total_markets": len(normalized.get("markets") or []),
                "created_candidates": len(football_candidates),
                "candidates": football_candidates,
            }
        )
        return {"payload": normalized, "preview": preview}

    def _resolve_zero_candidate_reason(
        self,
        *,
        preview_candidates: int,
        runtime_candidates_count: int,
        filtered_accepted_count: int,
        deduped_count: int,
        filter_rejections: dict[str, int],
    ) -> str:
        if preview_candidates == 0:
            return "no football events in payload"
        if runtime_candidates_count == 0:
            return "filtered by runtime sport settings"
        if filtered_accepted_count == 0:
            if filter_rejections:
                parts = ", ".join(f"{k}={v}" for k, v in sorted(filter_rejections.items()))
                return f"filtered by candidate rules: {parts}"
            return "filtered by candidate rules"
        if deduped_count == 0:
            return "deduplicated to zero candidates"
        return "unknown_zero_candidate_reason"

    def _log_candidates_per_match(self, candidates) -> None:
        counts: dict[tuple[str, str], int] = {}
        for candidate in candidates:
            match = getattr(candidate, "match", None)
            event_id = str(getattr(match, "external_event_id", "") or "—")
            match_name = str(getattr(match, "match_name", "") or "—")
            key = (event_id, match_name)
            counts[key] = counts.get(key, 0) + 1
        logger.info("[FOOTBALL] candidates per match:")
        for (event_id, match_name), count in sorted(counts.items()):
            logger.info("- event_id=%s, match=%s, count=%s", event_id, match_name, count)

    def _log_final_candidates(self, candidates) -> None:
        for candidate in candidates:
            match = getattr(candidate, "match", None)
            market = getattr(candidate, "market", None)
            logger.info(
                "- match=%s, market=%s, odds=%s, family=%s",
                getattr(match, "match_name", "—"),
                getattr(market, "market_label", "—"),
                getattr(market, "odds_value", "—"),
                FootballSignalSendFilterService().get_market_family(candidate),
            )

