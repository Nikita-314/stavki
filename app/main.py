from __future__ import annotations

import asyncio
import logging
from contextlib import suppress

from aiogram import Bot, Dispatcher
from aiogram.types import BotCommand

from app.core.config import get_settings
from app.bot.handlers import debug_router
from app.db.session import create_engine, create_sessionmaker
from app.services.auto_signal_service import AutoSignalService
from app.services.external_api_monitor_service import ExternalApiMonitorService
from app.services.football_finished_result_autosettlement_service import (
    FootballFinishedResultAutosettlementService,
)
from app.services.winline_result_autosettlement_service import WinlineResultAutoSettlementService


async def main() -> None:
    settings = get_settings()

    logging.basicConfig(
        level=logging.DEBUG if settings.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    bot = Bot(token=settings.bot_token)
    await bot.set_my_commands(
        [
            BotCommand(command="start", description="Показать меню и статус"),
            BotCommand(command="debug", description="Открыть debug-меню"),
            BotCommand(command="debug_help", description="Список команд"),
            BotCommand(command="quick_check", description="Быстрая сводка по системе"),
            BotCommand(command="system_status", description="Полный статус системы"),
            BotCommand(command="ping", description="Проверка связи с ботом"),
            BotCommand(command="auto_signal_status", description="Статус футбольного контура"),
            BotCommand(command="auto_signal_run_once", description="Запустить один футбольный прогон"),
            BotCommand(command="signal_status", description="Статус футбольных сигналов"),
            BotCommand(command="signal_pause", description="Остановить футбольный цикл"),
            BotCommand(command="signal_start", description="Запустить футбольный цикл"),
            BotCommand(
                command="football_live_debug",
                description="[admin] Полный debug последнего football live цикла",
            ),
            BotCommand(command="football_live_ranker_debug", description="[admin] S12 preview top-10 live идей"),
            BotCommand(command="football_live_probability_debug", description="[admin] S13 probability preview top-15"),
            BotCommand(command="football_prob_ideas_status", description="[admin] S13 ideas storage/settlement status"),
            BotCommand(command="signal_football", description="Включить рабочий режим футбола"),
            BotCommand(command="signal_cs2", description="Переключить CS2"),
            BotCommand(command="signal_dota", description="Переключить Dota"),
            BotCommand(command="winline_demo_status", description="Winline demo: статус контура"),
            BotCommand(command="winline_demo_preview", description="Winline demo: превью сигналов"),
            BotCommand(command="winline_demo_send", description="Winline demo: отправить в SIGNAL_CHAT"),
            BotCommand(command="winline_demo_settlement", description="Winline demo: settlement из примеров JSON"),
            BotCommand(command="winline_demo_full_cycle", description="Winline demo: send + settlement"),
            BotCommand(command="winline_manual_status", description="Winline manual: файлы JSON"),
            BotCommand(command="winline_manual_line_preview", description="Winline manual: превью line JSON"),
            BotCommand(command="winline_manual_line_ingest", description="Winline manual: ingest line JSON"),
            BotCommand(command="winline_manual_result_preview", description="Winline manual: превью result JSON"),
            BotCommand(command="winline_manual_result_process", description="Winline manual: обработка result JSON"),
            BotCommand(command="winline_manual_full_cycle", description="Winline manual: полный цикл"),
            BotCommand(command="winline_manual_upload_line", description="Winline: загрузить line JSON"),
            BotCommand(command="winline_manual_upload_result", description="Winline: загрузить result JSON"),
            BotCommand(command="winline_manual_clear_line", description="Winline: очистить line JSON"),
            BotCommand(command="winline_manual_clear_result", description="Winline: очистить result JSON"),
            BotCommand(command="winline_manual_file_status", description="Winline: статус файлов на диске"),
            BotCommand(command="winline_runtime_source", description="Winline: текущий runtime source"),
            BotCommand(command="openai_test", description="[admin] OpenAI: тест ключа"),
            BotCommand(
                command="openai_live_learning_stats",
                description="[admin] OpenAI: post-settlement learning stats",
            ),
            BotCommand(command="winline_clear_uploaded_line", description="Winline: удалить uploaded line runtime"),
            BotCommand(command="winline_manual_show_line", description="Winline: фрагмент line JSON"),
            BotCommand(command="winline_manual_show_result", description="Winline: фрагмент result JSON"),
            BotCommand(command="winline_manual_run_ready", description="Winline: умный следующий шаг"),
        ]
    )
    dp = Dispatcher()

    engine = create_engine(settings.database_url, echo=settings.debug)
    sessionmaker = create_sessionmaker(engine)

    dp.include_router(debug_router)

    async def _notify_admin_once(text: str) -> None:
        # Use first admin user id as an "admin chat" (private message).
        if not settings.admin_user_ids:
            logging.getLogger(__name__).info("[OPENAI] admin notify skipped (no ADMIN_USER_IDS)")
            return
        try:
            await bot.send_message(chat_id=int(settings.admin_user_ids[0]), text=text)
        except Exception:  # noqa: BLE001
            logging.getLogger(__name__).exception("[OPENAI] failed to send admin notification")

    async def _external_api_monitor_loop() -> None:
        """Extends the former OpenAI-only monitor to all external APIs."""
        await ExternalApiMonitorService().run_forever(settings, notify_admin=_notify_admin_once, interval_seconds=15 * 60)

    async def _football_live_loop_runner() -> None:
        # IMPORTANT: asyncio.create_task must receive a coroutine *function call* that returns a coroutine,
        # not an already-instantiated coroutine object.
        await AutoSignalService().run_football_live_forever(sessionmaker, bot)

    async def _winline_settlement_loop_runner() -> None:
        await WinlineResultAutoSettlementService().run_forever(sessionmaker, interval_seconds=120)

    async def _football_finished_result_loop_runner() -> None:
        await FootballFinishedResultAutosettlementService().run_forever(
            sessionmaker,
            interval_seconds=15 * 60,
            limit=50,
            lookback_days=3,
            older_than_hours=4,
        )

    football_live_task = asyncio.create_task(_football_live_loop_runner())
    settlement_task = asyncio.create_task(_winline_settlement_loop_runner())
    finished_result_task = asyncio.create_task(_football_finished_result_loop_runner())
    external_api_task = asyncio.create_task(_external_api_monitor_loop())
    try:
        await dp.start_polling(bot, sessionmaker=sessionmaker)
    finally:
        football_live_task.cancel()
        settlement_task.cancel()
        finished_result_task.cancel()
        external_api_task.cancel()
        with suppress(asyncio.CancelledError):
            await football_live_task
        with suppress(asyncio.CancelledError):
            await settlement_task
        with suppress(asyncio.CancelledError):
            await finished_result_task
        with suppress(asyncio.CancelledError):
            await external_api_task
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())

