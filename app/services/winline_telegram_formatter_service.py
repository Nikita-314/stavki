"""Telegram-oriented text formatters for `WinlineFinalSignal` (demo / preview only).

No bot wiring, no HTTP — pure string building on top of the final signal object.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from app.services.winline_final_signal_service import WinlineFinalSignal, WinlineFinalSignalService


class WinlineTelegramFormatterService:
    """Format `WinlineFinalSignal` into short Telegram-friendly messages."""

    _TEAM_TRANSLIT = {
        "Zenit": "Зенит",
        "Spartak": "Спартак",
        "Liverpool": "Ливерпуль",
        "Everton": "Эвертон",
    }

    def _fmt_decimal(self, value: Decimal, *, places: int = 4) -> str:
        q = Decimal("1").scaleb(-places)
        return str(value.quantize(q))

    def _fmt_optional_decimal(self, value: Any, *, places: int = 4) -> str:
        if value is None:
            return "n/a"
        if not isinstance(value, Decimal):
            try:
                value = Decimal(str(value))
            except Exception:
                return "n/a"
        return self._fmt_decimal(value, places=places)

    def _sport_emoji(self, sport: str) -> str:
        key = (sport or "").strip().lower().replace("-", "").replace("_", "")
        if key in {"football", "soccer"}:
            return "⚽"
        if key in {"cs2", "cs 2"}:
            return "🎮"
        if key in {"dota2", "dota 2"}:
            return "🛡️"
        return "📌"

    def _selection_humanize(self, selection: str) -> str:
        s = (selection or "").strip().upper()
        if s == "HOME":
            return "Победа хозяев"
        if s == "AWAY":
            return "Победа гостей"
        if s == "DRAW":
            return "Ничья"
        if s == "YES":
            return "Да"
        if s == "NO":
            return "Нет"
        return (selection or "").strip() or "?"

    def _humanize_team(self, value: str | None) -> str:
        text = (value or "").strip()
        return self._TEAM_TRANSLIT.get(text, text)

    def _match_name(self, signal: WinlineFinalSignal) -> str:
        home = self._humanize_team(signal.home_team)
        away = self._humanize_team(signal.away_team)
        if home and away:
            return f"{home} — {away}"
        text = (signal.match_name or "").strip()
        return text.replace(" vs ", " — ")

    def _humanize_bet(self, signal: WinlineFinalSignal) -> str:
        mt = (signal.market_kind or "").strip().lower()
        ml = (signal.market_label or "").strip()
        sel = (signal.selection or "").strip()
        sel_l = sel.lower()
        number = None
        for token in f"{ml} {sel}".replace(",", ".").split():
            try:
                float(token)
                number = token
                break
            except Exception:
                continue
        if "total" in mt or "тотал" in ml.lower():
            if "under" in sel_l or "меньше" in sel_l:
                return f"Тотал меньше {number}".strip()
            if "over" in sel_l or "больше" in sel_l:
                return f"Тотал больше {number}".strip()
        if "match" in mt or "result" in ml.lower():
            return self._selection_humanize(sel)
        if "btts" in mt or "both" in ml.lower():
            return f"Обе забьют: {self._selection_humanize(sel).lower()}"
        return f"{ml}: {self._selection_humanize(sel)}".strip(": ")

    def _source_badge(self, signal: WinlineFinalSignal) -> str | None:
        source = (signal.source_kind or "").strip().lower()
        if source in {"fallback_json", "fallback"}:
            return "🧪 Режим: fallback JSON"
        if source == "manual":
            return "📂 Режим: manual JSON"
        if source == "demo":
            return "🧪 Источник: тестовый demo"
        return None

    def format_signal_text(self, signal: WinlineFinalSignal) -> str:
        """Full readable message with emojis; no markdown tables, no raw_json dump."""
        source_badge = self._source_badge(signal)
        lines: list[str] = [
            "🚨 Футбольный сигнал",
        ]
        if source_badge:
            lines.extend(["", source_badge])
        lines.extend(
            [
                "",
                f"🏆 Турнир: {(signal.tournament_name or 'Не указан').strip()}",
                f"⚽ Матч: {self._match_name(signal)}",
                f"🎯 Ставка: {self._humanize_bet(signal)}",
                f"💰 Коэффициент: {self._fmt_optional_decimal(signal.odds_value, places=2)}",
                "🏢 Букмекер: Winline",
            ]
        )

        expl = (signal.short_explanation or "").strip()
        if expl:
            lines.extend(["", "📌 Основание:", expl])

        return "\n".join(lines)

    def format_compact_signal_text(self, signal: WinlineFinalSignal) -> str:
        """One-screen alert: summary line + key metrics."""
        header = f"🚨 {self._match_name(signal)}"
        tournament = (signal.tournament_name or "").strip()
        bet = self._humanize_bet(signal)
        odds = self._fmt_optional_decimal(signal.odds_value, places=2)
        lines = [header]
        if tournament:
            lines.append(f"🏆 {tournament}")
        lines.append(f"🎯 {bet} @ {odds}")
        badge = self._source_badge(signal)
        if badge:
            lines.append(badge)
        return "\n".join(lines)

    def format_skip_text(self, case_name: str, skip_reason: str | None) -> str:
        reason = (skip_reason or "unknown").strip()
        return f"⚪ {case_name}\nСигнал не собран: {reason}"

    def preview_formatter_demo(self) -> None:
        from app.services.winline_live_signal_service import WinlineLiveSignalService

        final_svc = WinlineFinalSignalService()
        sig = WinlineLiveSignalService()

        for case_name in sig.build_live_demo_inputs().keys():
            prev = final_svc.build_preview_for_case(case_name)
            print(f"=== TELEGRAM FORMAT CASE: {case_name} ===")
            if prev.has_signal and prev.signal is not None:
                print("[FULL]")
                print(self.format_signal_text(prev.signal))
                print()
                print("[COMPACT]")
                print(self.format_compact_signal_text(prev.signal))
            else:
                print(self.format_skip_text(case_name, prev.skip_reason))
            print()


if __name__ == "__main__":
    WinlineTelegramFormatterService().preview_formatter_demo()
