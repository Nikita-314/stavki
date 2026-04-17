"""Telegram-oriented text formatters for `WinlineFinalSignal` (demo / preview only).

No bot wiring, no HTTP — pure string building on top of the final signal object.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from app.services.winline_final_signal_service import WinlineFinalSignal, WinlineFinalSignalService


class WinlineTelegramFormatterService:
    """Format `WinlineFinalSignal` into short Telegram-friendly messages."""

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
        if s in {"HOME", "AWAY", "DRAW", "OVER", "UNDER", "YES", "NO"}:
            return s
        return (selection or "").strip() or "?"

    def format_signal_text(self, signal: WinlineFinalSignal) -> str:
        """Full readable message with emojis; no markdown tables, no raw_json dump."""
        sp = (signal.sport or "unknown").strip()
        emoji = self._sport_emoji(sp)
        sel_h = self._selection_humanize(signal.selection)

        lines: list[str] = [
            "🚨 Live signal",
            "",
            f"{emoji} Матч: {signal.match_name}",
            f"🎯 Рынок: {signal.market_label}",
            f"✅ Выбор: {sel_h}",
            f"💸 Коэффициент: {self._fmt_optional_decimal(signal.odds_value, places=2)}",
            "",
            "📊 Оценка:",
            f"• Implied probability: {self._fmt_optional_decimal(signal.implied_prob)}",
            f"• Estimated probability: {self._fmt_optional_decimal(signal.estimated_prob)}",
            f"• Edge: {self._fmt_optional_decimal(signal.edge)}",
            f"• EV: {self._fmt_optional_decimal(signal.expected_value)}",
            f"• Confidence: {self._fmt_optional_decimal(signal.confidence_score)}",
            "",
            "💰 Ставка:",
        ]

        lines.append(f"• Units: {self._fmt_optional_decimal(signal.recommended_stake_units, places=2)}")
        kf = signal.recommended_stake_fraction
        if kf is not None:
            lines.append(f"• Kelly fraction: {self._fmt_optional_decimal(kf)}")
        else:
            lines.append("• Kelly fraction: n/a")
        sm = signal.sizing_method or "n/a"
        lines.append(f"• Sizing: {sm}")

        lines.extend(["", "🧠 Причина:"])
        expl = (signal.short_explanation or "").strip()
        lines.append(expl if expl else "—")

        return "\n".join(lines)

    def format_compact_signal_text(self, signal: WinlineFinalSignal) -> str:
        """One-screen alert: summary line + key metrics."""
        sp = (signal.sport or "unknown").strip().upper()
        emoji = self._sport_emoji(signal.sport or "")
        sel_h = self._selection_humanize(signal.selection)
        line1 = f"{emoji} {sp} | {signal.match_name}"
        line2 = f"{signal.market_label} {sel_h} @ {self._fmt_optional_decimal(signal.odds_value, places=2)}"
        edge = self._fmt_optional_decimal(signal.edge)
        ev = self._fmt_optional_decimal(signal.expected_value)
        stake = self._fmt_optional_decimal(signal.recommended_stake_units, places=2)
        line3 = f"Edge: {edge} | EV: {ev} | Stake: {stake}u"
        return "\n".join([line1, line2, line3])

    def format_skip_text(self, case_name: str, skip_reason: str | None) -> str:
        reason = (skip_reason or "unknown").strip()
        return f"⚪ {case_name}\nNo signal: {reason}"

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
