from __future__ import annotations

import html
import logging
from decimal import Decimal
from zoneinfo import ZoneInfo

from aiogram import Bot
from aiogram.enums import ParseMode

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

    def _match_line_for_telegram(self, match_name: str) -> str:
        """Render only team names as a Telegram HTML code fragment."""
        safe = html.escape((match_name or "").strip())
        return f"⚽ Матч: <code>{safe}</code>"

    def _html_text(self, value: str | None) -> str:
        return html.escape((value or "").strip())

    def _fmt_decimal(self, value: Decimal | None, places: int = 2) -> str:
        if value is None:
            return ""
        q = Decimal("1").scaleb(-places)
        try:
            return str(value.quantize(q))
        except Exception:
            return str(value)

    def _format_match_start(self, value) -> str:
        if value is None:
            return "неизвестно"
        try:
            dt = value.astimezone(ZoneInfo("Europe/Moscow")) if getattr(value, "tzinfo", None) else value
            return dt.strftime("%d.%m.%Y %H:%M")
        except Exception:
            return "неизвестно"

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

    def _football_analysis_lines(self, report: SignalAnalyticsReport) -> list[str]:
        # Explicitly disabled: analysis block is noisy and not needed in signal messages.
        return []

    def _football_live_minute_line(self, report: SignalAnalyticsReport) -> str | None:
        prediction_logs = report.prediction_logs or []
        if not prediction_logs:
            return None
        snap = prediction_logs[0].feature_snapshot_json or {}
        analytics = snap.get("football_analytics") or {}
        minute = analytics.get("minute")
        if minute is None:
            return None
        try:
            m = int(minute)
        except (TypeError, ValueError):
            return None
        if m <= 0:
            return "🕒 Идёт: live"
        return f"🕒 Идёт: {m} мин (live)"

    def _football_live_context_line(self, report: SignalAnalyticsReport) -> str:
        prediction_logs = report.prediction_logs or []
        if prediction_logs:
            snap = prediction_logs[0].feature_snapshot_json or {}
            ctx = (
                snap.get("football_live_context_participation")
                if isinstance(snap.get("football_live_context_participation"), dict)
                else {}
            )
            label = str(ctx.get("context_label") or "").strip()
            if label:
                return f"🧩 Контекст: {self._html_text(label)}"
        return "🧩 Контекст: Winline"

    def _football_strategy_lines(self, report: SignalAnalyticsReport) -> list[str]:
        prediction_logs = report.prediction_logs or []
        if not prediction_logs:
            return []
        log = prediction_logs[0]
        expl = log.explanation_json or {}
        snap = log.feature_snapshot_json or {}
        strategy_id = str(expl.get("football_live_strategy_id") or "").strip()
        if not strategy_id:
            return []
        lines = [f"🏷 Стратегия: {self._html_text(strategy_id)}"]
        s13 = snap.get("football_live_s13_probability") if isinstance(snap, dict) else None
        if isinstance(s13, dict) and strategy_id.startswith("S13"):
            model = self._pct(s13.get("model_probability"))
            implied = self._pct(s13.get("implied_probability"))
            edge = self._pct(s13.get("value_edge"), signed=True)
            confidence = s13.get("confidence_score")
            risk = str(s13.get("risk_level") or "—")
            api = "yes" if bool(s13.get("api_intelligence_available")) else "no"
            lines.extend(
                [
                    f"📊 model_prob: {model}",
                    f"📉 implied_prob: {implied}",
                    f"📈 edge: {edge}",
                    f"🧠 confidence: {self._html_text(str(confidence or '—'))}",
                    f"⚠️ risk: {self._html_text(risk)}",
                    f"🔗 API: {api}",
                ]
            )
        return lines

    def _pct(self, value: object, *, signed: bool = False) -> str:
        try:
            v = float(value)
        except (TypeError, ValueError):
            return "—"
        sign = "+" if signed and v >= 0 else ""
        return f"{sign}{v * 100:.1f}%"

    def format_signal_message(self, report: SignalAnalyticsReport) -> str:
        s = report.signal
        match_name = self._humanize_match_name(s.match_name, s.home_team, s.away_team)
        tournament = (s.tournament_name or "").strip() or "Не указан"
        match_start = self._format_match_start(getattr(s, "event_start_at", None))
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
        source_badge = self._source_badge(report)
        live_line = self._football_live_minute_line(report)
        title = "🚨 Live-сигнал" if bool(getattr(s, "is_live", False)) else "🚨 Футбольный сигнал"
        lines = [
            title,
        ]
        if source_badge:
            lines.extend(["", source_badge])
        lines.extend(
            [
                "",
                f"🏆 Турнир: {self._html_text(tournament)}",
                self._match_line_for_telegram(match_name),
                *( [live_line] if live_line else [] ),
                self._football_live_context_line(report),
                f"🗓 Начало матча: {self._html_text(match_start)}",
                f"🎯 Ставка: {self._html_text(bet_presentation.main_label)}",
                f"💰 Коэффициент: {self._html_text(odds)}",
                *self._football_strategy_lines(report),
            ]
        )
        if bet_presentation.detail_label and not bool(getattr(s, "is_live", False)):
            lines.insert(-2, f"🧾 Исход: {self._html_text(bet_presentation.detail_label)}")
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
        logger.info(
            "[FOOTBALL][NOTIFY] NotificationService.send_signal_notification -> bot.send_message "
            "(signal_id=%s chat_id=%s)",
            getattr(getattr(report, "signal", None), "id", None),
            chat_id,
        )
        await bot.send_message(chat_id=chat_id, text=self._trim(text), parse_mode=ParseMode.HTML)

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

