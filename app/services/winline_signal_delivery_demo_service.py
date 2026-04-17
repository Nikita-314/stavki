"""Manual/demo bridge: Winline final signal → Telegram text → optional send to SIGNAL_CHAT_ID.

**Manual/demo only:** not wired into polling, not auto-triggered, not a production dispatcher.
Safe for testing formatter + `bot.send_message` path without changing main.py or debug handlers.
"""

from __future__ import annotations

import logging
from typing import Any

from aiogram import Bot

from app.core.constants import MAX_TELEGRAM_MESSAGE_LENGTH
from app.services.winline_final_signal_service import WinlineFinalSignalPreview, WinlineFinalSignalService
from app.services.winline_telegram_formatter_service import WinlineTelegramFormatterService


logger = logging.getLogger(__name__)


class WinlineSignalDeliveryDemoService:
    """Build demo payloads from the final-signal stack; preview locally; send compact text manually."""

    def build_demo_messages(self) -> list[dict[str, Any]]:
        """One entry per demo case: full + compact text when `has_signal`, else skip metadata."""
        from app.services.winline_live_signal_service import WinlineLiveSignalService

        final_svc = WinlineFinalSignalService()
        fmt = WinlineTelegramFormatterService()
        sig = WinlineLiveSignalService()
        out: list[dict[str, Any]] = []

        for case_name in sig.build_live_demo_inputs().keys():
            prev = final_svc.build_preview_for_case(case_name)
            if prev.has_signal and prev.signal is not None:
                s = prev.signal
                out.append(
                    {
                        "case_name": case_name,
                        "has_signal": True,
                        "signal": s,
                        "full_text": fmt.format_signal_text(s),
                        "compact_text": fmt.format_compact_signal_text(s),
                        "skip_reason": None,
                    }
                )
            else:
                out.append(
                    {
                        "case_name": case_name,
                        "has_signal": False,
                        "signal": None,
                        "full_text": None,
                        "compact_text": None,
                        "skip_reason": prev.skip_reason,
                    }
                )
        return out

    def _get_sendable_previews(self, messages: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
        msgs = messages if messages is not None else self.build_demo_messages()
        sendable = [m for m in msgs if m.get("has_signal") and m.get("signal") is not None]
        logger.info("[WINLINE] final signals: %s", len(sendable))
        return sendable

    def preview_messages(self) -> None:
        """Print FULL + COMPACT for each sendable case (no Telegram I/O)."""
        sendable = self._get_sendable_previews()
        if not sendable:
            print("(no sendable demo messages — all cases skipped or not_value_bet)")
            print()
            return
        for m in sendable:
            print(f"=== DELIVERY DEMO CASE: {m['case_name']} ===")
            print("[FULL]")
            print(m["full_text"])
            print()
            print("[COMPACT]")
            print(m["compact_text"])
            print()

    async def send_demo_messages(self, bot: Bot) -> dict[str, Any]:
        """Send **COMPACT** text only to `signal_chat_id`. No parse_mode, no background retries."""
        from app.core.config import get_settings

        settings = get_settings()
        if settings.signal_chat_id is None:
            return {
                "status": "skipped_no_signal_chat",
                "sent": 0,
                "chat_id": None,
                "case_names_sent": [],
                "message": "SIGNAL_CHAT_ID / signal_chat_id not configured — nothing sent",
            }

        sendable = self._get_sendable_previews()
        if not sendable:
            return {
                "status": "no_sendable_messages",
                "sent": 0,
                "chat_id": settings.signal_chat_id,
                "case_names_sent": [],
                "message": "No cases with has_signal=True",
            }

        chat_id = settings.signal_chat_id
        sent = 0
        case_names_sent: list[str] = []
        for m in sendable:
            text = str(m["compact_text"] or "")
            if len(text) > MAX_TELEGRAM_MESSAGE_LENGTH:
                text = text[: MAX_TELEGRAM_MESSAGE_LENGTH - 3] + "..."
            logger.info("[WINLINE] send_message demo case=%s chat_id=%s", m.get("case_name"), chat_id)
            await bot.send_message(chat_id=chat_id, text=text)
            sent += 1
            case_names_sent.append(str(m.get("case_name", "")))
        logger.info("[WINLINE] messages sent: %s", sent)

        return {
            "status": "ok",
            "sent": sent,
            "chat_id": chat_id,
            "case_names_sent": case_names_sent,
            "message": None,
        }

    def build_messages_from_final_previews(
        self, previews: list[WinlineFinalSignalPreview]
    ) -> list[dict[str, Any]]:
        """Compact Telegram payloads from arbitrary final previews (manual line path)."""
        fmt = WinlineTelegramFormatterService()
        out: list[dict[str, Any]] = []
        for p in previews:
            if not p.has_signal or p.signal is None:
                continue
            s = p.signal
            out.append(
                {
                    "case_name": p.case_name,
                    "has_signal": True,
                    "signal": s,
                    "full_text": fmt.format_signal_text(s),
                    "compact_text": fmt.format_compact_signal_text(s),
                    "skip_reason": None,
                }
            )
        return out

    async def send_manual_previews(self, bot: Bot, previews: list[WinlineFinalSignalPreview]) -> dict[str, Any]:
        """Send compact text for `build_previews_from_normalized_line_payload` output."""
        from app.core.config import get_settings

        settings = get_settings()
        if settings.signal_chat_id is None:
            return {
                "status": "skipped_no_signal_chat",
                "sent": 0,
                "chat_id": None,
                "case_names_sent": [],
                "message": "SIGNAL_CHAT_ID / signal_chat_id not configured — nothing sent",
            }

        msgs = self.build_messages_from_final_previews(previews)
        sendable = [m for m in msgs if m.get("has_signal") and m.get("signal") is not None]
        if not sendable:
            return {
                "status": "no_sendable_messages",
                "sent": 0,
                "chat_id": settings.signal_chat_id,
                "case_names_sent": [],
                "message": "No manual previews with has_signal=True",
            }

        chat_id = settings.signal_chat_id
        sent = 0
        case_names_sent: list[str] = []
        for m in sendable:
            text = str(m["compact_text"] or "")
            if len(text) > MAX_TELEGRAM_MESSAGE_LENGTH:
                text = text[: MAX_TELEGRAM_MESSAGE_LENGTH - 3] + "..."
            logger.info("[WINLINE] send_message manual case=%s chat_id=%s", m.get("case_name"), chat_id)
            await bot.send_message(chat_id=chat_id, text=text)
            sent += 1
            case_names_sent.append(str(m.get("case_name", "")))
        logger.info("[WINLINE] messages sent: %s", sent)

        return {
            "status": "ok",
            "sent": sent,
            "chat_id": chat_id,
            "case_names_sent": case_names_sent,
            "message": None,
        }

    async def run_manual_demo_send(self) -> dict[str, Any]:
        """Create Bot from settings, send compact demo lines, close session. Manual entrypoint only."""
        from app.core.config import get_settings

        settings = get_settings()
        bot = Bot(token=settings.bot_token)
        try:
            return await self.send_demo_messages(bot)
        finally:
            await bot.session.close()


async def main() -> None:
    result = await WinlineSignalDeliveryDemoService().run_manual_demo_send()
    print(result)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
