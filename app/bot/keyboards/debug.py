from __future__ import annotations

from aiogram.types import KeyboardButton, ReplyKeyboardMarkup


def get_winline_manual_flow_keyboard() -> ReplyKeyboardMarkup:
    """Компактные кнопки для операторского manual Winline flow (reply)."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="Winline manual line"),
                KeyboardButton(text="Winline manual ingest"),
            ],
            [
                KeyboardButton(text="Winline manual result"),
                KeyboardButton(text="Winline manual process"),
            ],
            [
                KeyboardButton(text="Winline manual full"),
                KeyboardButton(text="Winline run ready"),
            ],
            [KeyboardButton(text="Winline file status")],
        ],
        resize_keyboard=True,
        selective=True,
    )


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
            [
                KeyboardButton(text="Winline статус"),
                KeyboardButton(text="Winline превью"),
            ],
            [
                KeyboardButton(text="Winline отправка"),
                KeyboardButton(text="Winline settlement"),
            ],
            [KeyboardButton(text="Winline full cycle")],
            [
                KeyboardButton(text="Winline manual статус"),
                KeyboardButton(text="Winline manual line"),
            ],
            [
                KeyboardButton(text="Winline manual ingest"),
                KeyboardButton(text="Winline manual result"),
            ],
            [
                KeyboardButton(text="Winline manual process"),
                KeyboardButton(text="Winline manual full"),
            ],
            [
                KeyboardButton(text="Winline upload line"),
                KeyboardButton(text="Winline upload result"),
            ],
            [
                KeyboardButton(text="Winline clear line"),
                KeyboardButton(text="Winline clear result"),
            ],
            [KeyboardButton(text="Winline file status")],
            [
                KeyboardButton(text="Winline show line"),
                KeyboardButton(text="Winline show result"),
            ],
            [KeyboardButton(text="Winline run ready")],
        ],
        resize_keyboard=True,
        selective=True,
    )

