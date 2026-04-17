from __future__ import annotations

from aiogram import Bot

from app.core.constants import MAX_TELEGRAM_MESSAGE_LENGTH
from app.schemas.analytics import SignalAnalyticsReport
from app.schemas.signal_quality import SignalQualityReport
import logging


logger = logging.getLogger(__name__)


class NotificationService:
    def format_signal_message(self, report: SignalAnalyticsReport) -> str:
        s = report.signal
        lines = [
            "Новый сигнал",
            f"ID: {s.id}",
            f"Sport: {getattr(s.sport, 'value', s.sport)}",
            f"Bookmaker: {getattr(s.bookmaker, 'value', s.bookmaker)}",
            f"Match: {s.match_name}",
            f"Market: {s.market_type}",
            f"Selection: {s.selection}",
            f"Odds at signal: {s.odds_at_signal}",
            f"Min entry odds: {s.min_entry_odds}",
            f"Model: {s.model_name}/{s.model_version_name}",
            f"Predicted prob: {s.predicted_prob}",
            f"Implied prob: {s.implied_prob}",
            f"Edge: {s.edge}",
            f"Search hint: {s.search_hint}",
            f"Status: {getattr(s.status, 'value', s.status)}",
        ]
        return "\n".join(lines)

    def format_result_message(self, signal_report: SignalAnalyticsReport, quality_report: SignalQualityReport) -> str:
        s = signal_report.signal
        settlement = signal_report.settlement
        settlement_result = settlement.result.value if settlement is not None else None
        profit_loss = settlement.profit_loss if settlement is not None else None

        m = quality_report.metrics
        lines = [
            "Результат сигнала",
            f"ID: {s.id}",
            f"Match: {s.match_name}",
            f"Market: {s.market_type}",
            f"Selection: {s.selection}",
            f"Status: {getattr(s.status, 'value', s.status)}",
            f"Settlement result: {settlement_result}",
            f"Profit/loss: {profit_loss}",
            f"Predicted prob: {m.predicted_prob}",
            f"Implied prob: {m.implied_prob}",
            f"Prediction error: {m.prediction_error}",
            f"Quality label: {m.quality_label}",
            f"Overestimated: {m.is_overestimated}",
            f"Underestimated: {m.is_underestimated}",
            f"Failure reviews count: {len(signal_report.failure_reviews)}",
        ]
        return "\n".join(lines)

    async def send_signal_notification(self, bot: Bot, chat_id: int, report: SignalAnalyticsReport) -> None:
        text = self.format_signal_message(report)
        logger.info(
            "[WINLINE] send_message signal chat_id=%s signal_id=%s text_len=%s",
            chat_id,
            getattr(getattr(report, "signal", None), "id", None),
            len(text),
        )
        await bot.send_message(chat_id=chat_id, text=self._trim(text))

    async def send_result_notification(
        self,
        bot: Bot,
        chat_id: int,
        signal_report: SignalAnalyticsReport,
        quality_report: SignalQualityReport,
    ) -> None:
        text = self.format_result_message(signal_report, quality_report)
        logger.info(
            "[WINLINE] send_message result chat_id=%s signal_id=%s text_len=%s",
            chat_id,
            getattr(getattr(signal_report, "signal", None), "id", None),
            len(text),
        )
        await bot.send_message(chat_id=chat_id, text=self._trim(text))

    def _trim(self, text: str) -> str:
        if len(text) <= MAX_TELEGRAM_MESSAGE_LENGTH:
            return text
        return text[: MAX_TELEGRAM_MESSAGE_LENGTH - 3] + "..."

