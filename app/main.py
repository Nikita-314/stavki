from __future__ import annotations

import asyncio
import logging

from aiogram import Bot, Dispatcher

from app.core.config import get_settings
from app.bot.handlers import debug_router
from app.db.session import create_engine, create_sessionmaker


async def main() -> None:
    settings = get_settings()

    logging.basicConfig(
        level=logging.DEBUG if settings.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    bot = Bot(token=settings.bot_token)
    dp = Dispatcher()

    engine = create_engine(settings.database_url, echo=settings.debug)
    sessionmaker = create_sessionmaker(engine)

    dp.include_router(debug_router)
    try:
        await dp.start_polling(bot, sessionmaker=sessionmaker)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())

