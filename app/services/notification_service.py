from __future__ import annotations

import logging
from decimal import Decimal

from aiogram import Bot

from app.core.constants import MAX_TELEGRAM_MESSAGE_LENGTH
from app.schemas.analytics import SignalAnalyticsReport
from app.schemas.signal_quality import SignalQualityReport
from app.services.football_bet_formatter_service import FootballBetFormatterService


logger = logging.getLogger(__name__)


class NotificationService:
    _TEAM_TRANSLIT = {
        "Zenit": "Зенит",
        "Spartak": "Спартак",
        "Liverpool": "Ливерпуль",
        "Everton": "Эвертон",
    }

    def _humanize_team(self, value: str) -> str:
        text = (value or "").strip()
        return self._TEAM_TRANSLIT.get(text, text)

    def _humanize_match_name(self, match_name: str, home_team: str, away_team: str) -> str:
        home = self._humanize_team(home_team)
        away = self._humanize_team(away_team)
        if home and away:
            return f"{home} — {away}"
        text = (match_name or "").strip()
        if " vs " in text:
            left, right = text.split(" vs ", 1)
            return f"{self._humanize_team(left)} — {self._humanize_team(right)}"
        return text.replace(" vs ", " — ")

    def _fmt_decimal(self, value: Decimal | None, places: int = 2) -> str:
        if value is None:
            return ""
        q = Decimal("1").scaleb(-places)
        try:
            return str(value.quantize(q))
        except Exception:
            return str(value)

    def __init__(self) -> None:
        self._football_bet_formatter = FootballBetFormatterService()

    def _source_badge(self, report: SignalAnalyticsReport) -> str | None:
        notes = (report.signal.notes or "").strip().lower()
        if notes == "fallback_json":
            return "🧪 Режим: fallback JSON"
        prediction_logs = report.prediction_logs or []
        if prediction_logs:
            snapshot = prediction_logs[0].feature_snapshot_json or {}
            kind = str(snapshot.get("runtime_source_kind") or "").strip().lower()
            if kind == "fallback_json":
                return "🧪 Режим: fallback JSON"
        return None

    def _reason_line(self, report: SignalAnalyticsReport) -> str | None:
        prediction_logs = report.prediction_logs or []
        if not prediction_logs:
            return None
        payload = prediction_logs[0].feature_snapshot_json or {}
        raw_market_type = payload.get("raw_market_type")
        if raw_market_type:
            return None
        return None

    def format_signal_message(self, report: SignalAnalyticsReport) -> str:
        s = report.signal
        match_name = self._humanize_match_name(s.match_name, s.home_team, s.away_team)
        tournament = (s.tournament_name or "").strip() or "Не указан"
        bet_presentation = self._football_bet_formatter.format_bet(
            market_type=s.market_type,
            market_label=s.market_label,
            selection=s.selection,
            home_team=s.home_team,
            away_team=s.away_team,
            section_name=s.section_name,
            subsection_name=s.subsection_name,
        )
        odds = self._fmt_decimal(s.odds_at_signal, 2)
        bookmaker_raw = str(getattr(s.bookmaker, "value", s.bookmaker) or "").strip()
        bookmaker = "Winline" if bookmaker_raw.lower() == "winline" else bookmaker_raw
        source_badge = self._source_badge(report)
        lines = [
            "🚨 Футбольный сигнал",
        ]
        if source_badge:
            lines.extend(["", source_badge])
        lines.extend(
            [
                "",
                f"🏆 Турнир: {tournament}",
                f"⚽ Матч: {match_name}",
                f"🎯 Ставка: {bet_presentation.main_label}",
                f"💰 Коэффициент: {odds}",
                f"🏢 Букмекер: {bookmaker}",
            ]
        )
        if bet_presentation.detail_label:
            lines.insert(-2, f"🧾 Исход: {bet_presentation.detail_label}")
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
        signal = getattr(report, "signal", None)
        logger.info(
            "[FOOTBALL][SEND] sending message: match=%s market=%s odds=%s",
            getattr(signal, "match_name", None),
            getattr(signal, "market_label", None),
            getattr(signal, "odds_at_signal", None),
        )
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

