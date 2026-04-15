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
            BotCommand(command="start", description="Show bot status and menu"),
            BotCommand(command="debug", description="Open debug menu"),
            BotCommand(command="debug_help", description="Show command list"),
            BotCommand(command="quick_check", description="Show quick system summary"),
            BotCommand(command="system_status", description="Show full system status"),
            BotCommand(command="ping", description="Ping bot"),
            BotCommand(command="auto_signal_status", description="Show auto signal settings"),
            BotCommand(command="auto_signal_run_once", description="Run one auto signal cycle"),
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

