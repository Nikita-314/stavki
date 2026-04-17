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
            BotCommand(command="signal_pause", description="Поставить футбол на паузу"),
            BotCommand(command="signal_start", description="Снять паузу футбола"),
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
            BotCommand(command="winline_manual_show_line", description="Winline: фрагмент line JSON"),
            BotCommand(command="winline_manual_show_result", description="Winline: фрагмент result JSON"),
            BotCommand(command="winline_manual_run_ready", description="Winline: умный следующий шаг"),
        ]
    )
    dp = Dispatcher()

    engine = create_engine(settings.database_url, echo=settings.debug)
    sessionmaker = create_sessionmaker(engine)
    auto_task: asyncio.Task | None = None

    dp.include_router(debug_router)
    if settings.auto_signal_polling_enabled:
        auto_task = asyncio.create_task(AutoSignalService().run_forever(sessionmaker, bot))
    try:
        await dp.start_polling(bot, sessionmaker=sessionmaker)
    finally:
        if auto_task is not None:
            auto_task.cancel()
            with suppress(asyncio.CancelledError):
                await auto_task
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())

