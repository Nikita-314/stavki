from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from aiogram.exceptions import TelegramNetworkError

from app.bot.handlers.debug import _answer_long_message, cmd_signal_start
from app.schemas.auto_signal import AutoSignalCycleResult
from app.services.auto_signal_service import AutoSignalService


def _successful_cycle_result() -> AutoSignalCycleResult:
    return AutoSignalCycleResult(
        endpoint="wss://test",
        fetch_ok=True,
        preview_candidates=0,
        preview_skipped_items=0,
        created_signal_ids=[],
        created_signals_count=2,
        skipped_candidates_count=0,
        notifications_sent_count=1,
        preview_only=False,
        message="ok",
        raw_events_count=77,
    )


def test_cmd_signal_start_telegram_delivery_failure_no_cycle_failed_message() -> None:
    """Успешный цикл + сбой доставки отчёта не должен показывать «live-cycle завершился с ошибкой»."""

    async def _run() -> list[str]:
        message = MagicMock()
        message.text = "▶️ Старт"
        message.chat.id = 1
        message.from_user = MagicMock()
        message.from_user.id = 1
        message.bot = MagicMock()
        message.answer = AsyncMock(return_value=MagicMock())
        sessionmaker = MagicMock()

        with (
            patch("app.bot.handlers.debug._is_allowed", return_value=True),
            patch(
                "app.services.football_live_session_service.FootballLiveSessionService"
            ) as sess_cls,
            patch("app.bot.handlers.debug.SignalRuntimeSettingsService") as rts_cls,
            patch(
                "app.services.football_live_runtime_pacing.get_football_live_runtime_pacing"
            ) as gfp,
            patch.object(
                AutoSignalService,
                "run_single_cycle",
                new_callable=AsyncMock,
                return_value=_successful_cycle_result(),
            ),
            patch.object(
                AutoSignalService,
                "update_football_live_session_diagnostics_with_pacing",
                MagicMock(),
            ),
            patch.object(AutoSignalService, "log_football_cycle_trace", MagicMock()),
            patch(
                "app.bot.handlers.debug._answer_long_message",
                new_callable=AsyncMock,
                return_value=False,
            ),
        ):
            sess_cls.return_value.start_session = MagicMock()
            rts_cls.return_value = MagicMock()
            gfp.return_value.reset_session = MagicMock()
            await cmd_signal_start(message, sessionmaker)

        texts = [str(c.args[0]) for c in message.answer.call_args_list if c.args]
        return texts

    texts = asyncio.run(_run())
    joined = "\n".join(texts)
    assert "таймаута Telegram" in joined
    assert "✅ Первый live-cycle выполнен" in joined
    assert "❌ Первый live-cycle завершился с ошибкой" not in joined


def test_answer_long_message_false_after_network_error_retry() -> None:
    async def _run() -> tuple[bool, int]:
        message = MagicMock()
        message.answer = AsyncMock(
            side_effect=[
                TelegramNetworkError(MagicMock(), "timeout"),
                TelegramNetworkError(MagicMock(), "timeout"),
            ]
        )
        ok = await _answer_long_message(message, "hello")
        return ok, message.answer.await_count

    ok, n = asyncio.run(_run())
    assert ok is False
    assert n == 2


def test_answer_long_message_true_after_retry_success() -> None:
    async def _run() -> tuple[bool, int]:
        message = MagicMock()
        message.answer = AsyncMock(
            side_effect=[TelegramNetworkError(MagicMock(), "timeout"), MagicMock()],
        )
        ok = await _answer_long_message(message, "short")
        return ok, message.answer.await_count

    ok, n = asyncio.run(_run())
    assert ok is True
    assert n == 2
