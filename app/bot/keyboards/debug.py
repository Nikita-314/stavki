from __future__ import annotations

from aiogram.types import KeyboardButton, ReplyKeyboardMarkup


def get_winline_manual_flow_keyboard() -> ReplyKeyboardMarkup:
    """Компактные кнопки для операторского manual Winline flow (reply)."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="Winline превью линии"),
                KeyboardButton(text="Winline загрузить сигналы"),
            ],
            [
                KeyboardButton(text="Winline превью результата"),
                KeyboardButton(text="Winline обработать результат"),
            ],
            [
                KeyboardButton(text="Winline полный цикл"),
                KeyboardButton(text="Winline умный запуск"),
            ],
            [KeyboardButton(text="Winline статус файлов")],
        ],
        resize_keyboard=True,
        selective=True,
    )


def get_signal_control_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="⚽ Футбол"), KeyboardButton(text="🎮 CS2")],
            [KeyboardButton(text="🎮 Dota"), KeyboardButton(text="📊 Статус сигналов")],
            [KeyboardButton(text="▶️ Старт"), KeyboardButton(text="⏸ Пауза")],
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
            [KeyboardButton(text="⚽ Футбол"), KeyboardButton(text="🎮 CS2")],
            [KeyboardButton(text="🎮 Dota"), KeyboardButton(text="📊 Статус сигналов")],
            [KeyboardButton(text="▶️ Старт"), KeyboardButton(text="⏸ Пауза")],
            [
                KeyboardButton(text="Winline статус"),
                KeyboardButton(text="Winline превью"),
            ],
            [
                KeyboardButton(text="Winline отправка"),
                KeyboardButton(text="Winline расчёт"),
            ],
            [KeyboardButton(text="Winline полный демо-цикл")],
            [
                KeyboardButton(text="Winline ручной статус"),
                KeyboardButton(text="Winline превью линии"),
            ],
            [
                KeyboardButton(text="Winline загрузить сигналы"),
                KeyboardButton(text="Winline превью результата"),
            ],
            [
                KeyboardButton(text="Winline обработать результат"),
                KeyboardButton(text="Winline полный цикл"),
            ],
            [
                KeyboardButton(text="Winline загрузить JSON линии"),
                KeyboardButton(text="Winline загрузить JSON результата"),
            ],
            [
                KeyboardButton(text="Winline очистить линию"),
                KeyboardButton(text="Winline очистить результат"),
            ],
            [KeyboardButton(text="Winline статус файлов")],
            [
                KeyboardButton(text="Winline показать JSON линии"),
                KeyboardButton(text="Winline показать JSON результата"),
            ],
            [KeyboardButton(text="Winline умный запуск")],
        ],
        resize_keyboard=True,
        selective=True,
    )

