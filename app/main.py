from __future__ import annotations

import asyncio
import logging

from aiogram import Bot, Dispatcher

from app.core.config import get_settings


async def main() -> None:
    settings = get_settings()

    logging.basicConfig(
        level=logging.DEBUG if settings.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    bot = Bot(token=settings.bot_token)
    dp = Dispatcher()

    # Handlers will be registered later (app/bot/handlers).
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

