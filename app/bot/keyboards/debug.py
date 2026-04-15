from __future__ import annotations

from aiogram.types import KeyboardButton, ReplyKeyboardMarkup


def get_debug_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Mock candidates"), KeyboardButton(text="Run mock ingestion")],
            [KeyboardButton(text="Summary"), KeyboardButton(text="Signal report")],
        ],
        resize_keyboard=True,
        selective=True,
    )

