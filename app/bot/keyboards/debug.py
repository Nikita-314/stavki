from __future__ import annotations

from aiogram.types import KeyboardButton, ReplyKeyboardMarkup


def get_debug_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Проверка бота"), KeyboardButton(text="Статус системы")],
            [KeyboardButton(text="Последние сигналы"), KeyboardButton(text="Последние результаты")],
            [KeyboardButton(text="Баланс"), KeyboardButton(text="Баланс ₽")],
            [KeyboardButton(text="Отчёт за период"), KeyboardButton(text="Отчёт за период ₽")],
            [KeyboardButton(text="Быстрая проверка"), KeyboardButton(text="Проверка данных")],
            [KeyboardButton(text="Помощь"), KeyboardButton(text="Кто я")],
            [KeyboardButton(text="Автосигналы"), KeyboardButton(text="Запустить цикл")],
        ],
        resize_keyboard=True,
        selective=True,
    )

