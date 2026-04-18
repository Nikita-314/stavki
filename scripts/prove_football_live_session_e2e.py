from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone

import faulthandler
import signal
import os
import sys

from aiogram import Bot

# Ensure repo root is on sys.path when running as a script.
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from app.core.config import get_settings
from app.db.session import create_engine, create_sessionmaker
from app.services.auto_signal_service import AutoSignalService
from app.services.football_live_session_service import FootballLiveSessionService
from app.services.signal_runtime_diagnostics_service import SignalRuntimeDiagnosticsService
from app.services.signal_runtime_settings_service import SignalRuntimeSettingsService


logger = logging.getLogger(__name__)


def _pick(d: dict, keys: list[str]) -> dict:
    return {k: d.get(k) for k in keys}


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    faulthandler.register(signal.SIGUSR1)
    settings = get_settings()

    # Safety: do not print tokens or full endpoints.
    logger.info("[E2E] start ts=%s", datetime.now(tz=timezone.utc).isoformat())
    logger.info("[E2E] signal_chat_id=%s", getattr(settings, "signal_chat_id", None))
    logger.info("[E2E] odds_provider_configured=%s", bool(getattr(settings, "odds_provider_base_url", None)))
    logger.info("[E2E] preview_only=%s", bool(getattr(settings, "auto_signal_preview_only", False)))

    bot = Bot(token=settings.bot_token)
    engine = create_engine(settings.database_url, echo=bool(settings.debug))
    sessionmaker = create_sessionmaker(engine)

    # Ensure football runtime enabled and not paused.
    rt = SignalRuntimeSettingsService()
    rt.start()
    rt.enable_sport("football")

    # Start live session and run 2 cycles.
    FootballLiveSessionService().start_session(duration_minutes=int(settings.football_live_session_duration_minutes or 15))

    svc = AutoSignalService()
    diag_svc = SignalRuntimeDiagnosticsService()

    for idx in (1, 2):
        print(f"[E2E] cycle={idx} begin", flush=True)
        res = await asyncio.wait_for(svc.run_single_cycle(sessionmaker, bot, dry_run=False), timeout=60)
        svc.log_football_cycle_trace(res)
        diag = diag_svc.get_state()

        summary = _pick(
            diag,
            [
                "football_live_session_active",
                "football_live_session_started_at",
                "football_live_session_expires_at",
                "football_live_session_last_cycle_at",
                "football_live_session_remaining_minutes",
                "live_provider_name",
                "live_auth_status",
                "source_mode",
                "football_live_effective_source",
                "football_live_cycle_live_matches_found",
                "football_live_cycle_candidates_before_filter",
                "football_live_cycle_after_send_filter",
                "football_live_cycle_after_integrity",
                "football_live_cycle_after_score",
                "football_live_cycle_new_ideas_sendable",
                "football_live_cycle_duplicate_ideas_blocked",
                "football_live_signals_sent_session",
                "football_live_telegram_sent_session",
                "football_live_last_notify_path",
                "football_live_cycle_bottleneck",
            ],
        )
        logger.info("[E2E] cycle=%s diag=%s", idx, json.dumps(summary, ensure_ascii=False, default=str))

        # Small delay so provider/live state can change; not tied to polling loop.
        await asyncio.sleep(3)

    await bot.session.close()
    await engine.dispose()
    logger.info("[E2E] done")


if __name__ == "__main__":
    asyncio.run(main())

