from __future__ import annotations

import asyncio
import json
import logging
from io import BytesIO
from datetime import datetime, timezone
from decimal import Decimal

from aiogram import Bot, F, Router
from aiogram.filters import BaseFilter, Command
from aiogram.types import Message
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.bot.keyboards.debug import get_debug_keyboard, get_signal_control_keyboard, get_winline_manual_flow_keyboard
from app.core.enums import BetResult, EntryStatus
from app.core.config import Settings, get_settings
from app.schemas.entry import EntryCreate
from app.schemas.settlement import SettlementCreate
from app.schemas.candidate_filter import CandidateFilterConfig
from app.services.analytics_service import AnalyticsService
from app.services.analytics_summary_service import AnalyticsSummaryService
from app.services.bootstrap_service import BootstrapService
from app.services.candidate_filter_service import CandidateFilterService
from app.services.entry_service import EntryService
from app.services.failure_review_service import FailureReviewService
from app.services.signal_quality_service import SignalQualityService
from app.services.signal_quality_summary_service import SignalQualitySummaryService
from app.services.settlement_service import SettlementService
from app.services.result_ingestion_service import ResultIngestionService
from app.schemas.event_result import EventResultInput
from app.services.notification_service import NotificationService
from app.services.balance_service import BalanceService
from app.services.period_report_service import PeriodReportService
from app.services.orchestration_service import OrchestrationService
from app.providers.mock_candidate_provider import MockCandidateProvider
from app.services.demo_cycle_service import DemoCycleService
from app.db.repositories.signal_repository import SignalRepository
from app.services.sanity_check_service import SanityCheckService
from app.providers.json_candidate_provider import JsonCandidateProvider
from app.services.ingestion_service import IngestionService
from app.providers.generic_odds_adapter import GenericOddsAdapter
from app.services.http_fetch_service import HttpFetchService
from app.services.adapter_ingestion_service import AdapterIngestionService
from app.providers.odds_http_client import OddsHttpClient
from app.schemas.auto_signal import AutoSignalCycleResult
from app.services.auto_signal_service import AutoSignalService
from app.services.signal_runtime_diagnostics_service import SignalRuntimeDiagnosticsService
from app.services.signal_runtime_settings_service import SignalRuntimeSettingsService
from app.schemas.provider_client import ProviderClientConfig
from app.services.remote_smoke_service import RemoteSmokeService


router = Router(name="debug")
logger = logging.getLogger(__name__)

# user_id -> "line" | "result" — ожидание JSON-документа после /winline_manual_upload_*
_pending_manual_json_upload: dict[int, str] = {}

_MAX_MANUAL_JSON_UPLOAD_BYTES = 5 * 1024 * 1024
_SNIPPET_CHARS = 1200


class _PendingManualJsonUploadFilter(BaseFilter):
    """Только сообщения от пользователей, которым нужен приём manual JSON."""

    async def __call__(self, message: Message) -> bool:
        uid = message.from_user.id if message.from_user else None
        return bool(uid and uid in _pending_manual_json_upload)


def _format_manual_line_preview_lines() -> list[str]:
    from app.services.winline_manual_cycle_service import WinlineManualCycleService

    pv = WinlineManualCycleService().preview_manual_line()
    lp = pv.get("line_preview") or {}
    keys = lp.get("top_level_keys") or []
    keys_s = ", ".join(keys[:15]) + ("…" if len(keys) > 15 else "")
    lines = [
        "📄 Winline manual — line preview",
        f"- detected shape: {lp.get('detected_shape')}",
        f"- root: {lp.get('root_type')}",
        f"- keys: {keys_s or '—'}",
        f"- raw events: {lp.get('raw_events_count') if lp.get('raw_events_count') is not None else '—'}",
        f"- raw lines: {lp.get('lines_count') if lp.get('lines_count') is not None else '—'}",
        f"- championships: {lp.get('championships_count') if lp.get('championships_count') is not None else '—'}",
        f"- normalized events: {lp.get('normalized_events_count') if lp.get('normalized_events_count') is not None else '—'}",
        f"- normalized markets: {lp.get('normalized_markets_count') if lp.get('normalized_markets_count') is not None else '—'}",
        f"- ingestible: {_fmt_yes_no(bool(lp.get('ingestible_shape')))}",
        f"- adapter candidates: {lp.get('preview_candidates') if lp.get('preview_candidates') is not None else '—'}",
        f"- final signals (synth): {pv.get('final_signals_ready') if pv.get('final_signals_ready') is not None else '—'}",
    ]
    err = lp.get("error") or pv.get("error")
    if err:
        lines.append(f"- ошибка: {err}")
    samples = pv.get("sample_market_mappings") or []
    if samples:
        lines.append("sample market mappings:")
        for s in samples[:3]:
            dbg = s.get("mapping_debug") or {}
            lines.append(
                f"- idTipMarket={s.get('idTipMarket')} -> market_type={s.get('market_type')} | "
                f"selection={s.get('selection')} | src={dbg.get('market_type_source')}"
            )
    return lines


def _format_manual_result_preview_lines() -> list[str]:
    from app.services.winline_manual_cycle_service import WinlineManualCycleService

    pr = WinlineManualCycleService().preview_manual_result()
    rp = pr.get("result_preview") or {}
    keys = rp.get("top_level_keys") or []
    keys_s = ", ".join(keys[:15]) + ("…" if len(keys) > 15 else "")
    lines = [
        "📄 Winline manual — result preview",
        f"- detected shape: {rp.get('detected_shape')}",
        f"- root: {rp.get('root_type')}",
        f"- keys: {keys_s or '—'}",
        f"- raw result rows: {rp.get('raw_results_count') if rp.get('raw_results_count') is not None else '—'}",
        f"- normalized result rows: {rp.get('normalized_results_count') if rp.get('normalized_results_count') is not None else '—'}",
        f"- processible: {_fmt_yes_no(bool(rp.get('processible')))}",
        f"- event results: {rp.get('event_results_count') if rp.get('event_results_count') is not None else '—'}",
    ]
    if rp.get("error"):
        lines.append(f"- ошибка: {rp['error']}")
    samples = pr.get("sample_result_mappings") or []
    if samples:
        lines.append("sample result mappings:")
        for s in samples[:3]:
            dbg = s.get("mapping_debug") or {}
            lines.append(
                f"- winner={s.get('winner_raw')} -> {s.get('winner_selection')} | "
                f"src={dbg.get('winner_source')} | status={s.get('status_raw')} -> "
                f"is_void={s.get('is_void')} ({dbg.get('void_source')})"
            )
    return lines


def _format_winline_runtime_source_lines() -> list[str]:
    from app.services.winline_manual_payload_service import WinlineManualPayloadService

    settings = get_settings()
    diag = SignalRuntimeDiagnosticsService().get_state()
    truth = WinlineManualPayloadService().get_line_source_truth()
    provider_name = diag.get("live_provider_name") or (
        "the_odds_api" if getattr(settings, "odds_provider_base_url", None) else "—"
    )
    return [
        "📡 Winline runtime source",
        f"- provider: {provider_name}",
        f"- live auth status: {diag.get('live_auth_status') or '—'}",
        f"- source_mode: {truth.get('source_mode') or diag.get('source_mode') or '—'}",
        f"- is_real_source: {_fmt_yes_no(bool(truth.get('is_real_source')))}",
        f"- source_origin: {truth.get('source_origin') or '—'}",
        f"- upload_provenance_present: {_fmt_yes_no(bool(truth.get('provenance_present')))}",
        f"- uploaded_at: {truth.get('uploaded_at') or '—'}",
        f"- source_file_path: {truth.get('file_path') or '—'}",
        f"- checksum: {truth.get('checksum') or '—'}",
        f"- fixture_match: {_fmt_yes_no(bool(truth.get('fixture_match')))}",
        f"- raw_events_count: {diag.get('raw_events_count') if diag.get('raw_events_count') is not None else 0}",
        f"- normalized_markets_count: {diag.get('normalized_markets_count') if diag.get('normalized_markets_count') is not None else 0}",
    ]


def _json_snippet_messages(title: str, text: str | None) -> list[str]:
    if not text or not text.strip():
        return [f"{title}\n(пусто)"]
    t = text.strip()
    if len(t) <= _SNIPPET_CHARS:
        return [f"{title}\n{t}"]
    chunk2 = t[_SNIPPET_CHARS : _SNIPPET_CHARS * 2]
    more = len(t) > _SNIPPET_CHARS * 2
    return [
        f"{title}\n{t[:_SNIPPET_CHARS]}\n… (обрезано)",
        f"{title} (продолжение)\n{chunk2}" + ("\n… (обрезано)" if more else ""),
    ]


def _text_is(*values: str):
    expected = {v.strip() for v in values}
    return lambda m: (m.text or "").strip() in expected


def _is_allowed(message: Message) -> bool:
    settings = get_settings()
    if not settings.admin_user_ids:
        return True
    user_id = message.from_user.id if message.from_user else None
    return bool(user_id and user_id in settings.admin_user_ids)


async def _deny(message: Message) -> None:
    await message.answer("Доступ запрещён")


def _fmt_decimal(v: Decimal | None) -> str:
    if v is None:
        return "None"
    return str(v)


def _fmt_yes_no(v: bool) -> str:
    return "Да" if v else "Нет"


def _sport_toggle_label(key: str, enabled: bool) -> str:
    if key == "football":
        return f"⚽ Футбол: {'включён' if enabled else 'выключен'}"
    if key == "cs2":
        return f"🎮 CS2: {'включён' if enabled else 'выключен'}"
    return f"🎮 Dota: {'включена' if enabled else 'выключена'}"


def _format_signal_runtime_status_lines() -> list[str]:
    state = SignalRuntimeSettingsService().get_state()
    diag = SignalRuntimeDiagnosticsService().get_state()
    source = diag.get("football_source") or "—"
    if diag.get("fallback_used"):
        source = f"fallback ({diag.get('football_fallback_source') or 'manual'})"
    live_provider = diag.get("live_provider_name") or source
    live_auth_status = diag.get("live_auth_status") or "—"
    live_http_status = diag.get("last_live_http_status")
    live_http_status_text = str(live_http_status) if live_http_status is not None else "—"
    fallback_source_available = _fmt_yes_no(bool(diag.get("fallback_source_available")))
    manual_fallback_allowed = _fmt_yes_no(bool(diag.get("manual_production_fallback_allowed")))
    source_mode = diag.get("source_mode") or "—"
    is_real_source = _fmt_yes_no(bool(diag.get("is_real_source")))
    source_origin = diag.get("source_origin") or "—"
    upload_provenance_present = _fmt_yes_no(bool(diag.get("upload_provenance_present")))
    uploaded_at = diag.get("uploaded_at") or "—"
    last_fetch = diag.get("last_fetch_status") or "—"
    if last_fetch == "fallback_manual_payload" and diag.get("last_error"):
        last_fetch = f"{diag.get('last_error')} -> fallback"
    delivery_reason = diag.get("last_delivery_reason") or diag.get("note") or "—"
    return [
        "📊 Статус сигналов",
        f"▶️ Режим: {'запущен' if not state.get('paused') else 'остановлен'}",
        f"⚽ Футбол: {'включён' if state.get('football_enabled') else 'выключен'}",
        f"🎮 CS2: {'включён' if state.get('cs2_enabled') else 'выключен'}",
        f"🎮 Dota: {'включена' if state.get('dota_enabled') else 'выключена'}",
        f"👁 Preview-only: {_fmt_yes_no(bool(diag.get('preview_only')))}",
        f"📡 Источник футбола: {source}",
        f"🌐 Live provider: {live_provider}",
        f"🔐 Live auth: {live_auth_status}",
        f"📶 Live HTTP status: {live_http_status_text}",
        f"📦 Fallback source available: {fallback_source_available}",
        f"🛠 Manual production fallback: {manual_fallback_allowed}",
        f"🧭 Source mode: {source_mode}",
        f"✅ Real source: {is_real_source}",
        f"🧾 Source origin: {source_origin}",
        f"📎 Upload provenance: {upload_provenance_present}",
        f"⏱ Uploaded at: {uploaded_at}",
        f"📥 Последний fetch: {last_fetch}",
        f"📊 Raw events: {diag.get('raw_events_count') or 0}",
        f"🔴 Live matches: {diag.get('live_matches_count') or 0}",
        f"🟡 Near matches: {diag.get('near_matches_count') or 0}",
        f"⏭ Too far matches: {diag.get('too_far_matches_count') or 0}",
        f"🧠 Кандидатов: {diag.get('candidates_after_filter_count') or 0}",
        f"⚽ real_candidates: {diag.get('football_real_candidates_count') or 0}",
        f"⚽ football_candidates: {diag.get('football_candidates_count') or 0}",
        f"⚽ football_after_filter: {diag.get('football_after_filter_count') or 0}",
        f"⚽ football_after_integrity: {diag.get('football_after_integrity_count') or 0}",
        f"⚠ dropped_invalid_market_mapping: {diag.get('dropped_invalid_market_mapping_count') or 0}",
        f"⚠ dropped_invalid_total_scope: {diag.get('dropped_invalid_total_scope_count') or 0}",
        f"⚠ dropped_too_far_in_time: {diag.get('dropped_too_far_in_time_count') or 0}",
        f"🧭 Почему выбран матч: {diag.get('selected_match_reason') or '—'}",
        f"⚽ football_sent: {diag.get('football_sent_count') or 0}",
        f"🚨 Финальных сигналов: {diag.get('final_signals_count') or 0}",
        f"📨 Отправлено: {diag.get('messages_sent_count') or 0}",
        f"🛑 Причина без отправки: {delivery_reason}",
        "",
        "— Аналитика и обучение (флаги, без выдуманных данных) —",
        f"Сбор признаков в снимке: {_fmt_yes_no(bool(diag.get('football_analytics_enabled')))}",
        f"Правка score по истории: {_fmt_yes_no(bool(diag.get('football_learning_enabled')))}",
        f"Семейств в истории (последний цикл): {diag.get('football_learning_families_tracked') or 0}",
        f"Live-поля в снимке (счёт/минута): {_fmt_yes_no(bool(diag.get('football_live_fields_in_last_cycle')))}",
        f"Источник травм подключён: {_fmt_yes_no(bool(diag.get('football_injuries_data_available')))}",
        f"Движение линии подключено: {_fmt_yes_no(bool(diag.get('football_line_movement_available')))}",
    ]


def _chunk_answer_text(text: str, limit: int = 3800) -> list[str]:
    """Split long debug text for Telegram (no huge single messages)."""
    if len(text) <= limit:
        return [text]
    lines = text.split("\n")
    chunks: list[str] = []
    buf: list[str] = []
    size = 0
    for line in lines:
        line_len = len(line) + 1
        if buf and size + line_len > limit:
            chunks.append("\n".join(buf))
            buf = [line]
            size = line_len
        else:
            buf.append(line)
            size += line_len
    if buf:
        chunks.append("\n".join(buf))
    return chunks or [""]


async def _answer_long_message(message: Message, text: str) -> None:
    for part in _chunk_answer_text(text):
        await message.answer(part)


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _parse_signal_id(value: str) -> int:
    return int(value)


def _parse_decimal(value: str) -> Decimal:
    # Accept "1.87" style values; keep Decimal precision.
    return Decimal(value)


def _fmt_enum(v: object) -> str:
    return getattr(v, "value", str(v))


def _fmt_sport_ru(v: object) -> str:
    raw = _fmt_enum(v)
    mapping = {
        "CS2": "CS2",
        "DOTA2": "Dota 2",
        "FOOTBALL": "Футбол",
    }
    return mapping.get(raw, raw)


def _fmt_bookmaker_ru(v: object) -> str:
    raw = _fmt_enum(v)
    mapping = {
        "FONBET": "Fonbet",
        "WINLINE": "Winline",
        "BETBOOM": "BetBoom",
    }
    return mapping.get(raw, raw)


def _fmt_signal_status_ru(v: object) -> str:
    raw = _fmt_enum(v)
    mapping = {
        "NEW": "Новый",
        "SENT": "Отправлен",
        "ENTERED": "Введён",
        "MISSED": "Пропущен",
        "SETTLED": "Завершён",
        "CANCELED": "Отменён",
    }
    return mapping.get(raw, raw)


def _fmt_bet_result_ru(v: object) -> str:
    raw = _fmt_enum(v)
    mapping = {
        "WIN": "Победа",
        "LOSE": "Поражение",
        "VOID": "Возврат",
        "UNKNOWN": "Неизвестно",
        "-": "-",
    }
    return mapping.get(raw, raw)


def _fmt_market_type_ru(value: str | None) -> str:
    raw = (value or "").strip()
    mapping = {
        "match_winner": "Победитель матча",
        "moneyline": "Победитель матча",
        "h2h": "Победитель матча",
        "1x2": "Исход 1X2",
        "handicap": "Фора",
        "total_goals": "Тотал",
        "totals": "Тотал",
        "spreads": "Фора",
    }
    return mapping.get(raw.lower(), raw or "-")


def _fmt_quality_label_ru(value: str | None) -> str:
    raw = (value or "").strip()
    mapping = {
        "strong_value_win": "Сильный value, победа",
        "strong_value_loss": "Сильный value, проигрыш",
        "market_aligned_win": "По рынку, победа",
        "market_aligned_loss": "По рынку, проигрыш",
        "insufficient_data": "Недостаточно данных",
    }
    return mapping.get(raw, raw.replace("_", " ") if raw else "-")


def _fmt_failure_category_ru(value: str | None) -> str:
    raw = (value or "").strip()
    mapping = {
        "MODEL_ERROR": "Ошибка модели",
        "EXECUTION_ERROR": "Ошибка исполнения",
        "MARKET_UNAVAILABLE": "Рынок недоступен",
        "LINE_MOVEMENT": "Сдвиг линии",
        "VARIANCE": "Дисперсия",
        "DATA_ISSUE": "Проблема данных",
        "UNKNOWN": "Неизвестно",
    }
    return mapping.get(raw, raw.replace("_", " ") if raw else "-")


def _format_latest_signal_card(signal) -> str:
    sport = _fmt_sport_ru(signal.sport)
    bookmaker = _fmt_bookmaker_ru(signal.bookmaker)
    status = _fmt_signal_status_ru(signal.status)
    result = _fmt_bet_result_ru(signal.settlement.result if signal.settlement is not None else "-")
    market = _fmt_market_type_ru(signal.market_type)
    return "\n".join(
        [
            f"#{signal.id} • {sport} • {bookmaker}",
            f"Матч: {signal.match_name}",
            f"Рынок: {market} → {signal.selection}",
            f"Коэффициент: {signal.odds_at_signal} • Статус: {status} • Итог: {result}",
        ]
    )


def _format_latest_result_card(signal, quality_label: str) -> str:
    sport = _fmt_sport_ru(signal.sport)
    bookmaker = _fmt_bookmaker_ru(signal.bookmaker)
    result = _fmt_bet_result_ru(signal.settlement.result if signal.settlement is not None else "-")
    market = _fmt_market_type_ru(signal.market_type)
    pl = signal.settlement.profit_loss if signal.settlement is not None else None
    return "\n".join(
        [
            f"#{signal.id} • {sport} • {bookmaker}",
            f"Матч: {signal.match_name}",
            f"Рынок: {market} → {signal.selection}",
            f"Итог: {result} • P/L: {pl} • Качество: {_fmt_quality_label_ru(quality_label)}",
        ]
    )


def _format_latest_failure_card(signal, category: str, reason: str) -> str:
    sport = _fmt_sport_ru(signal.sport)
    bookmaker = _fmt_bookmaker_ru(signal.bookmaker)
    result = _fmt_bet_result_ru(signal.settlement.result if signal.settlement is not None else "-")
    market = _fmt_market_type_ru(signal.market_type)
    return "\n".join(
        [
            f"#{signal.id} • {sport} • {bookmaker}",
            f"Матч: {signal.match_name}",
            f"Рынок: {market} → {signal.selection}",
            f"Итог: {result} • Категория: {category}",
            f"Причина: {reason}",
        ]
    )


def _parse_sport(value: str):
    # SportType is a str Enum: accept "CS2", "DOTA2", "FOOTBALL" (case-insensitive).
    from app.core.enums import SportType

    return SportType(value.strip().upper())


def _parse_int(value: str) -> int:
    return int(value)


def _parse_required_parts(message: Message, *, min_parts: int, example: str) -> list[str] | None:
    parts = (message.text or "").strip().split()
    if len(parts) < min_parts:
        return None
    return parts


def _parse_winner_tail(message: Message, *, event_external_id: str) -> str | None:
    text = (message.text or "").strip()
    needle = f" {event_external_id} "
    idx = text.find(needle)
    if idx < 0:
        return None
    tail = text[idx + len(needle) :].strip()
    return tail or None


@router.message(Command("debug"))
async def cmd_debug(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    await message.answer(
        "🛠 Меню диагностики\nВыберите действие кнопками ниже.\nПолный список команд: /debug_help",
        reply_markup=get_debug_keyboard(),
    )


@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    await message.answer(
        "👋 Бот запущен и готов к работе.\n"
        "Это бот для сигналов и диагностики системы.\n\n"
        "Нажмите кнопки ниже\n"
        "или откройте список команд через /debug_help",
        reply_markup=get_signal_control_keyboard(),
    )


@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    await message.answer("ℹ️ Используйте /debug_help для списка команд.", reply_markup=get_signal_control_keyboard())


@router.message(_text_is("Проверка бота"))
@router.message(Command("ping"))
async def cmd_ping(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    await message.answer("🏓 Бот на связи: pong")


@router.message(_text_is("Кто я"))
@router.message(Command("whoami"))
async def cmd_whoami(message: Message) -> None:
    allowed = _is_allowed(message)
    if not allowed:
        await _deny(message)
        return

    user_id = message.from_user.id if message.from_user else None
    chat_id = message.chat.id if getattr(message, "chat", None) is not None else None
    chat_type = message.chat.type if getattr(message, "chat", None) is not None else None
    await message.answer(
        "\n".join(
            [
                "🪪 Кто вы для бота",
                f"- User ID: {user_id}",
                f"- Chat ID: {chat_id}",
                f"- Тип чата: {chat_type}",
                f"- Доступ: {_fmt_yes_no(allowed)}",
            ]
        )
    )


@router.message(_text_is("📊 Статус сигналов"))
@router.message(Command("signal_status"))
async def cmd_signal_status(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    await message.answer("\n".join(_format_signal_runtime_status_lines()), reply_markup=get_signal_control_keyboard())


@router.message(_text_is("⏸ Стоп"))
@router.message(Command("signal_pause"))
async def cmd_signal_pause(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    SignalRuntimeSettingsService().pause()
    await message.answer("⏸ Стоп: отправка и футбольный цикл остановлены", reply_markup=get_signal_control_keyboard())


@router.message(_text_is("▶️ Старт"))
@router.message(Command("signal_start"))
async def cmd_signal_start(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    SignalRuntimeSettingsService().start()
    await message.answer("▶️ Старт: футбольный цикл снова запущен", reply_markup=get_signal_control_keyboard())


@router.message(_text_is("⚽ Футбол"))
@router.message(Command("signal_football"))
async def cmd_signal_football(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    runtime = SignalRuntimeSettingsService()
    runtime.enable_sport("football")
    await message.answer("⚽ Футбол: основной рабочий режим включён", reply_markup=get_signal_control_keyboard())


@router.message(_text_is("🎮 CS2"))
@router.message(Command("signal_cs2"))
async def cmd_signal_cs2(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    state = SignalRuntimeSettingsService().toggle_sport("cs2")
    await message.answer(
        _sport_toggle_label("cs2", bool(state.get("cs2_enabled"))),
        reply_markup=get_signal_control_keyboard(),
    )


@router.message(_text_is("🎮 Dota"))
@router.message(Command("signal_dota"))
async def cmd_signal_dota(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    state = SignalRuntimeSettingsService().toggle_sport("dota2")
    await message.answer(
        _sport_toggle_label("dota", bool(state.get("dota_enabled"))),
        reply_markup=get_signal_control_keyboard(),
    )


def _format_football_prog_run_report(res: AutoSignalCycleResult) -> str:
    m = res.report_matches_found
    c = res.report_candidates
    af = res.report_after_filter
    ai = res.report_after_integrity
    asc = res.report_after_scoring
    fin = res.report_final_signal or "—"
    lines = [
        "⚽ Футбольный прогон завершён",
        "",
        f"📊 Матчей найдено: {m if m is not None else '—'}",
        f"📊 Кандидатов: {c if c is not None else '—'}",
        f"📊 После фильтра: {af if af is not None else '—'}",
        f"📊 После integrity: {ai if ai is not None else '—'}",
        f"📊 После аналитики (порог score): {asc if asc is not None else '—'}",
        f"📊 Финальный сигнал: {fin}",
        "",
    ]
    if fin == "ДА" and res.report_selected_match:
        lines.extend(
            [
                "🎯 Выбран сигнал:",
                f"Матч: {res.report_selected_match}",
                f"Ставка: {res.report_selected_bet or '—'}",
                f"Коэффициент: {res.report_selected_odds or '—'}",
                f"Score: {res.report_selected_score or '—'}",
                "Причины:",
            ]
        )
        for r in res.report_human_reasons or []:
            lines.append(r)
    elif fin == "НЕТ":
        lines.append("❌ Сигнал не выбран")
        rc = res.report_rejection_code or res.rejection_reason or "unknown"
        lines.append(f"Причина: {rc}")
        if res.report_dedup_skipped:
            lines.append(f"(dedup skipped: {res.report_dedup_skipped})")
    if res.dry_run:
        lines.extend(["", "ℹ️ Режим прогона: без записи в БД и без отправки в канал."])
    lines.extend(["", f"Сообщение цикла: {res.message}"])
    return "\n".join(lines)


def _format_auto_signal_env_lines(settings: Settings, runtime: SignalRuntimeSettingsService) -> list[str]:
    provider_configured = bool(settings.odds_provider_base_url)
    signal_chat_configured = settings.signal_chat_id is not None
    return [
        "",
        "— Настройки автоконтура (.env) —",
        f"Автопуллинг: {_fmt_yes_no(settings.auto_signal_polling_enabled)}",
        f"Интервал, сек: {settings.auto_signal_polling_interval_seconds}",
        f"Только preview: {_fmt_yes_no(settings.auto_signal_preview_only)}",
        f"Лимит сигналов за цикл: {settings.auto_signal_max_created_per_cycle or '—'}",
        f"HTTP provider настроен: {_fmt_yes_no(provider_configured)}",
        f"Чат сигналов настроен: {_fmt_yes_no(signal_chat_configured)}",
        f"Активные виды спорта: {', '.join(s.value.lower() for s in runtime.active_sports()) or '—'}",
    ]


@router.message(_text_is("🏠 Основная клавиатура"))
async def cmd_main_keyboard(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    await message.answer("Основная клавиатура.", reply_markup=get_signal_control_keyboard())


@router.message(_text_is("Автосигналы"))
@router.message(Command("auto_signal_status"))
async def cmd_auto_signal_status(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    settings = get_settings()
    runtime = SignalRuntimeSettingsService()
    lines = ["⚽ Футбольный контур и автосигналы", ""] + _format_signal_runtime_status_lines()
    lines += _format_auto_signal_env_lines(settings, runtime)
    await message.answer("\n".join(lines), reply_markup=get_signal_control_keyboard())


@router.message(_text_is("⚽ Прогон", "⚽ Футбольный прогон", "Запустить цикл"))
@router.message(Command("auto_signal_run_once"))
async def cmd_auto_signal_run_once(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    res = await AutoSignalService().run_single_cycle(sessionmaker, message.bot, dry_run=True)
    await message.answer(
        _format_football_prog_run_report(res),
        reply_markup=get_signal_control_keyboard(),
    )


@router.message(lambda m: (m.text or "").strip() in {"Mock candidates", "/mock_candidates"})
async def show_mock_candidates(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    candidates = await BootstrapService().preview_mock_candidates()
    shown = candidates[:10]
    lines = [f"Candidates: {len(candidates)} (showing {len(shown)})"]
    for c in shown:
        lines.append(
            f"- {c.match.sport} | {c.market.bookmaker} | {c.match.match_name} | "
            f"{c.market.market_type} | {c.market.selection} | {c.market.odds_value}"
        )
    await message.answer("\n".join(lines))


@router.message(lambda m: (m.text or "").strip() in {"Run mock ingestion", "/run_mock_ingestion"})
async def run_mock_ingestion(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    async with sessionmaker() as session:
        result = await BootstrapService().run_mock_ingestion(session)
        await session.commit()

    await message.answer(
        "\n".join(
            [
                f"total_candidates: {result.total_candidates}",
                f"created_signals: {result.created_signals}",
                f"skipped_candidates: {result.skipped_candidates}",
                f"created_signal_ids: {result.created_signal_ids}",
            ]
        )
    )


@router.message(lambda m: (m.text or "").strip() in {"Summary", "/summary"})
async def show_summary(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    async with sessionmaker() as session:
        report = await AnalyticsSummaryService().get_summary(session)

    k = report.kpis
    await message.answer(
        "\n".join(
            [
                f"total_signals: {k.total_signals}",
                f"entered_signals: {k.entered_signals}",
                f"missed_signals: {k.missed_signals}",
                f"settled_signals: {k.settled_signals}",
                f"wins/losses/voids: {k.wins}/{k.losses}/{k.voids}",
                f"total_profit_loss: {_fmt_decimal(k.total_profit_loss)}",
                f"win_rate: {_fmt_decimal(k.win_rate)}",
                f"roi_percent: {_fmt_decimal(k.roi_percent)}",
            ]
        )
    )


@router.message(lambda m: (m.text or "").strip() in {"Signal report"})
async def hint_signal_report(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    await message.answer("Use: /signal_report <signal_id>")


@router.message(Command("signal_report"))
async def cmd_signal_report(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Usage: /signal_report <signal_id>")
        return
    try:
        signal_id = int(parts[1])
    except ValueError:
        await message.answer("signal_id must be an integer. Example: /signal_report 5")
        return

    try:
        async with sessionmaker() as session:
            report = await AnalyticsService().get_signal_report(session, signal_id)
    except ValueError as e:
        await message.answer(str(e))
        return

    settlement_result = report.settlement.result.value if report.settlement is not None else None
    await message.answer(
        "\n".join(
            [
                f"id: {report.signal.id}",
                f"sport: {_fmt_enum(report.signal.sport)}",
                f"bookmaker: {_fmt_enum(report.signal.bookmaker)}",
                f"match: {report.signal.match_name}",
                f"market_type: {report.signal.market_type}",
                f"selection: {report.signal.selection}",
                f"odds_at_signal: {report.signal.odds_at_signal}",
                f"min_entry_odds: {report.signal.min_entry_odds}",
                f"status: {_fmt_enum(report.signal.status)}",
                f"entries: {len(report.entries)}",
                f"settlement_result: {settlement_result}",
                f"failure_reviews: {len(report.failure_reviews)}",
            ]
        )
    )


@router.message(Command("signal_quality"))
async def cmd_signal_quality(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Usage: /signal_quality <signal_id>")
        return
    try:
        signal_id = int(parts[1])
    except ValueError:
        await message.answer("signal_id must be an integer. Example: /signal_quality 5")
        return

    try:
        async with sessionmaker() as session:
            q = await SignalQualityService().build_signal_quality_report(session, signal_id)
    except ValueError as e:
        await message.answer(str(e))
        return

    m = q.metrics
    await message.answer(
        "\n".join(
            [
                f"signal_id: {q.signal_id}",
                f"match_name: {q.match_name}",
                f"market_type: {q.market_type}",
                f"selection: {q.selection}",
                f"model: {q.model_name}/{q.model_version_name}",
                f"predicted_prob: {_fmt_decimal(m.predicted_prob)}",
                f"implied_prob: {_fmt_decimal(m.implied_prob)}",
                f"actual_outcome: {m.actual_outcome}",
                f"prediction_error: {_fmt_decimal(m.prediction_error)}",
                f"edge: {_fmt_decimal(m.edge)}",
                f"value_direction: {m.value_direction}",
                f"calibration_bucket: {m.calibration_bucket}",
                f"is_overestimated: {m.is_overestimated}",
                f"is_underestimated: {m.is_underestimated}",
                f"quality_label: {m.quality_label}",
            ]
        )
    )


@router.message(Command("enter_signal"))
async def cmd_enter_signal(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    parts = (message.text or "").split()
    if len(parts) < 4:
        await message.answer("Usage: /enter_signal <signal_id> <entered_odds> <stake_amount>")
        return
    try:
        signal_id = _parse_signal_id(parts[1])
        entered_odds = _parse_decimal(parts[2])
        stake_amount = _parse_decimal(parts[3])
    except Exception:
        await message.answer("Example: /enter_signal 12 1.87 1000")
        return

    async with sessionmaker() as session:
        entry = await EntryService().register_entry(
            session,
            EntryCreate(
                signal_id=signal_id,
                status=EntryStatus.ENTERED,
                entered_odds=entered_odds,
                stake_amount=stake_amount,
                entered_at=_utc_now(),
                is_manual=True,
                delay_seconds=None,
            ),
        )
        await session.commit()

    await message.answer(
        "\n".join(
            [
                f"signal_id: {signal_id}",
                f"status: {entry.status.value}",
                f"entered_odds: {entry.entered_odds}",
                f"stake_amount: {entry.stake_amount}",
            ]
        )
    )


@router.message(Command("miss_signal"))
async def cmd_miss_signal(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.answer("Usage: /miss_signal <signal_id> <reason>")
        return
    try:
        signal_id = _parse_signal_id(parts[1])
    except Exception:
        await message.answer("Example: /miss_signal 12 market moved too fast")
        return

    reason = (message.text or "").split(None, 2)[2].strip()
    if not reason:
        await message.answer("Usage: /miss_signal <signal_id> <reason>")
        return

    async with sessionmaker() as session:
        entry = await EntryService().register_entry(
            session,
            EntryCreate(
                signal_id=signal_id,
                status=EntryStatus.SKIPPED,
                missed_reason=reason,
                is_manual=True,
            ),
        )
        await session.commit()

    await message.answer(
        "\n".join(
            [
                f"signal_id: {signal_id}",
                f"status: {entry.status.value}",
                f"missed_reason: {entry.missed_reason}",
            ]
        )
    )


@router.message(Command("settle_signal"))
async def cmd_settle_signal(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    parts = (message.text or "").split()
    if len(parts) < 4:
        await message.answer("Usage: /settle_signal <signal_id> <WIN|LOSE|VOID> <profit_loss>")
        return
    try:
        signal_id = _parse_signal_id(parts[1])
        result_raw = parts[2].upper().strip()
        profit_loss = _parse_decimal(parts[3])
        result = BetResult(result_raw)
    except Exception:
        await message.answer("Examples: /settle_signal 12 WIN 870 | /settle_signal 12 LOSE -1000 | /settle_signal 12 VOID 0")
        return

    async with sessionmaker() as session:
        settlement = await SettlementService().register_settlement(
            session,
            SettlementCreate(
                signal_id=signal_id,
                result=result,
                profit_loss=profit_loss,
                bankroll_before=None,
                bankroll_after=None,
            ),
        )
        await session.commit()

    await message.answer(
        "\n".join(
            [
                f"signal_id: {signal_id}",
                f"result: {settlement.result.value}",
                f"profit_loss: {settlement.profit_loss}",
            ]
        )
    )


@router.message(Command("auto_review"))
async def cmd_auto_review(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Usage: /auto_review <signal_id>")
        return
    try:
        signal_id = _parse_signal_id(parts[1])
    except Exception:
        await message.answer("Example: /auto_review 12")
        return

    async with sessionmaker() as session:
        review = await FailureReviewService().register_auto_failure_review(session, signal_id)
        await session.commit()

    await message.answer(
        "\n".join(
            [
                f"signal_id: {signal_id}",
                f"category: {review.category.value}",
                f"auto_reason: {review.auto_reason}",
            ]
        )
    )


@router.message(Command("full_signal_review"))
async def cmd_full_signal_review(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Usage: /full_signal_review <signal_id>")
        return
    try:
        signal_id = _parse_signal_id(parts[1])
    except Exception:
        await message.answer("Example: /full_signal_review 12")
        return

    try:
        async with sessionmaker() as session:
            report = await AnalyticsService().get_signal_report(session, signal_id)
            q = await SignalQualityService().build_signal_quality_report(session, signal_id)
    except ValueError as e:
        await message.answer(str(e))
        return

    settlement_result = report.settlement.result.value if report.settlement is not None else None
    m = q.metrics
    await message.answer(
        "\n".join(
            [
                f"id: {report.signal.id}",
                f"match: {report.signal.match_name}",
                f"bookmaker: {_fmt_enum(report.signal.bookmaker)}",
                f"market_type: {report.signal.market_type}",
                f"selection: {report.signal.selection}",
                f"status: {_fmt_enum(report.signal.status)}",
                f"entries: {len(report.entries)}",
                f"settlement_result: {settlement_result}",
                f"predicted_prob: {_fmt_decimal(m.predicted_prob)}",
                f"implied_prob: {_fmt_decimal(m.implied_prob)}",
                f"prediction_error: {_fmt_decimal(m.prediction_error)}",
                f"quality_label: {m.quality_label}",
                f"failure_reviews: {len(report.failure_reviews)}",
            ]
        )
    )


@router.message(Command("quality_summary"))
async def cmd_quality_summary(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    async with sessionmaker() as session:
        summary = await SignalQualitySummaryService().build_quality_summary(session)

    top_market = summary.by_market_type[:3]
    top_label = summary.by_quality_label[:3]
    top_buckets = summary.by_calibration_bucket[:5]

    lines: list[str] = [
        f"total_signals: {summary.total_signals}",
        f"signals_with_outcome: {summary.signals_with_outcome}",
        f"avg_prediction_error: {_fmt_decimal(summary.avg_prediction_error)}",
        f"overestimated_count: {summary.overestimated_count}",
        f"underestimated_count: {summary.underestimated_count}",
        "",
        "top by_market_type:",
    ]
    for it in top_market:
        lines.append(
            f"- {it.key}: total={it.total_signals}, with_outcome={it.with_outcome}, "
            f"avg_err={_fmt_decimal(it.avg_prediction_error)}"
        )

    lines.append("")
    lines.append("top by_quality_label:")
    for it in top_label:
        lines.append(
            f"- {it.key}: total={it.total_signals}, with_outcome={it.with_outcome}, "
            f"avg_err={_fmt_decimal(it.avg_prediction_error)}"
        )

    lines.append("")
    lines.append("top by_calibration_bucket:")
    for b in top_buckets:
        lines.append(
            f"- {b.bucket}: total={b.total_signals}, W/L={b.wins}/{b.losses}, "
            f"win_rate={_fmt_decimal(b.actual_win_rate)}, avg_err={_fmt_decimal(b.avg_prediction_error)}"
        )

    await message.answer("\n".join(lines))


@router.message(Command("ingest_result"))
async def cmd_ingest_result(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    parts = (message.text or "").split()
    if len(parts) < 4:
        await message.answer("Usage: /ingest_result <sport> <event_external_id> <winner_selection>")
        return
    try:
        sport = _parse_sport(parts[1])
        event_external_id = parts[2].strip()
        winner_selection = (message.text or "").split(None, 3)[3].strip()
        data = EventResultInput(event_external_id=event_external_id, sport=sport, winner_selection=winner_selection)
    except Exception:
        await message.answer("Examples: /ingest_result CS2 cs2_10001 Team Spirit | /ingest_result FOOTBALL football_30001 Зенит")
        return

    async with sessionmaker() as session:
        res = await ResultIngestionService().process_event_result(session, data)
        await session.commit()

    await message.answer(
        "\n".join(
            [
                f"total_signals_found: {res.total_signals_found}",
                f"settled_signals: {res.settled_signals}",
                f"skipped_signals: {res.skipped_signals}",
                f"created_failure_reviews: {res.created_failure_reviews}",
                f"processed_signal_ids: {res.processed_signal_ids}",
            ]
        )
    )


@router.message(Command("void_result"))
async def cmd_void_result(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.answer("Usage: /void_result <sport> <event_external_id>")
        return
    try:
        sport = _parse_sport(parts[1])
        event_external_id = parts[2].strip()
        data = EventResultInput(event_external_id=event_external_id, sport=sport, is_void=True)
    except Exception:
        await message.answer("Example: /void_result CS2 cs2_10001")
        return

    async with sessionmaker() as session:
        res = await ResultIngestionService().process_event_result(session, data)
        await session.commit()

    await message.answer(
        "\n".join(
            [
                f"total_signals_found: {res.total_signals_found}",
                f"settled_signals: {res.settled_signals}",
                f"skipped_signals: {res.skipped_signals}",
                f"created_failure_reviews: {res.created_failure_reviews}",
                f"processed_signal_ids: {res.processed_signal_ids}",
            ]
        )
    )


@router.message(Command("notify_signal"))
async def cmd_notify_signal(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Usage: /notify_signal <signal_id>")
        return
    try:
        signal_id = _parse_signal_id(parts[1])
    except Exception:
        await message.answer("Example: /notify_signal 12")
        return

    settings = get_settings()
    if settings.signal_chat_id is None:
        await message.answer("SIGNAL_CHAT_ID is not set")
        return

    async with sessionmaker() as session:
        report = await AnalyticsService().get_signal_report(session, signal_id)

    await NotificationService().send_signal_notification(message.bot, settings.signal_chat_id, report)
    await message.answer("signal notification sent")


@router.message(Command("notify_result"))
async def cmd_notify_result(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Usage: /notify_result <signal_id>")
        return
    try:
        signal_id = _parse_signal_id(parts[1])
    except Exception:
        await message.answer("Example: /notify_result 12")
        return

    settings = get_settings()
    if settings.result_chat_id is None:
        await message.answer("RESULT_CHAT_ID is not set")
        return

    async with sessionmaker() as session:
        signal_report = await AnalyticsService().get_signal_report(session, signal_id)
        quality_report = await SignalQualityService().build_signal_quality_report(session, signal_id)

    await NotificationService().send_result_notification(message.bot, settings.result_chat_id, signal_report, quality_report)
    await message.answer("result notification sent")


@router.message(_text_is("Баланс"))
@router.message(Command("balance"))
async def cmd_balance(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    async with sessionmaker() as session:
        overview = await BalanceService().get_balance_overview(session)

    await message.answer(
        "\n".join(
            [
                "💼 Баланс (условные единицы)",
                f"- Базовая точка: {overview.base_amount}",
                f"- Время точки отсчёта: {overview.base_snapshot_at}",
                f"- Метка точки отсчёта: {overview.base_label}",
                f"- Прибыль с точки отсчёта: {overview.total_profit_loss_since_base}",
                f"- Текущий баланс: {overview.current_balance}",
                f"- Завершённых сигналов: {overview.settled_signals_count}",
                f"- Побед / поражений / возвратов: {overview.wins}/{overview.losses}/{overview.voids}",
            ]
        )
    )


@router.message(_text_is("Баланс ₽"))
@router.message(Command("balance_rub"))
async def cmd_balance_rub(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    async with sessionmaker() as session:
        overview = await BalanceService().get_realistic_balance_overview(session)

    await message.answer(
        "\n".join(
            [
                "💰 Баланс (₽, фиксированная ставка)",
                f"- Фиксированная ставка: {overview.flat_stake_rub}",
                f"- Стартовый баланс: {overview.base_amount}",
                f"- Время точки отсчёта: {overview.base_snapshot_at}",
                f"- Метка точки отсчёта: {overview.base_label}",
                f"- Текущий результат: {overview.total_profit_loss_rub}",
                f"- Текущий баланс: {overview.current_balance_rub}",
                f"- Завершённых сигналов: {overview.settled_signals_count}",
                f"- Побед / поражений / возвратов: {overview.wins}/{overview.losses}/{overview.voids}",
            ]
        )
    )


@router.message(Command("reset_balance"))
async def cmd_reset_balance(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Usage: /reset_balance <amount> [label...]")
        return

    try:
        amount = _parse_decimal(parts[1])
    except Exception:
        await message.answer("Example: /reset_balance 50000 april test")
        return

    label = " ".join(parts[2:]).strip() if len(parts) > 2 else None
    if label == "":
        label = None

    async with sessionmaker() as session:
        snapshot = await BalanceService().reset_balance(session, amount, label=label)
        await session.commit()

    await message.answer(
        "\n".join(
            [
                "snapshot created",
                f"amount: {snapshot.base_amount}",
                f"label: {snapshot.label}",
                f"created_at: {snapshot.created_at}",
            ]
        )
    )


@router.message(Command("balance_history"))
async def cmd_balance_history(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    async with sessionmaker() as session:
        items = await BalanceService().list_balance_history(session)

    shown = items[:10]
    lines = [f"snapshots: {len(items)} (showing {len(shown)})"]
    for it in shown:
        lines.append(f"- id={it.snapshot_id} | base_amount={it.base_amount} | label={it.label} | created_at={it.created_at}")
    await message.answer("\n".join(lines))


@router.message(_text_is("Отчёт за период"))
@router.message(Command("period_report"))
async def cmd_period_report(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    async with sessionmaker() as session:
        report = await PeriodReportService().get_period_report(session)

    o = report.overview

    lines: list[str] = [
        "📈 Отчёт за период (условные единицы)",
        f"- Период с: {o.period_started_at}",
        f"- Метка периода: {o.period_label}",
        f"- Стартовый баланс: {o.start_balance}",
        f"- Результат за период: {o.total_profit_loss}",
        f"- Текущий баланс: {o.current_balance}",
        f"- Завершённых сигналов: {o.settled_signals_count}",
        f"- Побед / поражений / возвратов: {o.wins}/{o.losses}/{o.voids}",
    ]

    top_sport = report.by_sport[:5]
    if top_sport:
        lines.append("")
        lines.append("Топ-5 по видам спорта:")
        for it in top_sport:
            lines.append(
                f"- {it.key}: сигналов={it.settled_signals_count} побед/поражений/возвратов={it.wins}/{it.losses}/{it.voids} "
                f"результат={it.total_profit_loss} среднее={it.avg_profit_loss}"
            )

    top_market = report.by_market_type[:5]
    if top_market:
        lines.append("")
        lines.append("Топ-5 по рынкам:")
        for it in top_market:
            lines.append(
                f"- {it.key}: сигналов={it.settled_signals_count} побед/поражений/возвратов={it.wins}/{it.losses}/{it.voids} "
                f"результат={it.total_profit_loss} среднее={it.avg_profit_loss}"
            )

    await message.answer("\n".join(lines))


@router.message(_text_is("Отчёт за период ₽"))
@router.message(Command("period_report_rub"))
async def cmd_period_report_rub(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    async with sessionmaker() as session:
        report = await PeriodReportService().get_realistic_period_report(session)

    o = report.overview

    lines: list[str] = [
        "📊 Отчёт за период (₽)",
        f"- Период с: {o.period_started_at}",
        f"- Метка периода: {o.period_label}",
        f"- Стартовый баланс: {o.start_balance_rub}",
        f"- Фиксированная ставка: {o.flat_stake_rub}",
        f"- Результат за период: {o.total_profit_loss_rub}",
        f"- Текущий баланс: {o.current_balance_rub}",
        f"- Завершённых сигналов: {o.settled_signals_count}",
        f"- Побед / поражений / возвратов: {o.wins}/{o.losses}/{o.voids}",
    ]

    top_sport = report.by_sport[:5]
    if top_sport:
        lines.append("")
        lines.append("Топ-5 по видам спорта:")
        for it in top_sport:
            lines.append(
                f"- {it.key}: сигналов={it.settled_signals_count} побед/поражений/возвратов={it.wins}/{it.losses}/{it.voids} "
                f"результат={it.total_profit_loss_rub} среднее={it.avg_profit_loss_rub}"
            )

    top_market = report.by_market_type[:5]
    if top_market:
        lines.append("")
        lines.append("Топ-5 по рынкам:")
        for it in top_market:
            lines.append(
                f"- {it.key}: сигналов={it.settled_signals_count} побед/поражений/возвратов={it.wins}/{it.losses}/{it.voids} "
                f"результат={it.total_profit_loss_rub} среднее={it.avg_profit_loss_rub}"
            )

    await message.answer("\n".join(lines))


@router.message(Command("orchestrate_mock_signal"))
async def cmd_orchestrate_mock_signal(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    candidates = await MockCandidateProvider().fetch_candidates()
    config = CandidateFilterConfig.default_for_russian_manual_betting()
    filtered = CandidateFilterService().filter_candidates(candidates, config)
    if not filtered.accepted_candidates:
        await message.answer("candidate skipped")
        return

    candidate = filtered.accepted_candidates[0]

    orch = OrchestrationService()
    created_signal_id: int | None = None
    skipped_reason: str | None = None

    async with sessionmaker() as session:
        res = await orch.create_signal(session, candidate)
        created_signal_id = res.signal_id
        skipped_reason = res.skipped_reason
        if created_signal_id is not None:
            await session.commit()

    if created_signal_id is None:
        await message.answer(f"candidate skipped ({skipped_reason})")
        return

    notification_sent = "no"
    try:
        async with sessionmaker() as session2:
            sent = await orch.notify_signal_if_configured(session2, message.bot, created_signal_id)
            notification_sent = "yes" if sent else "no"
    except Exception:
        notification_sent = "no"

    await message.answer("\n".join([f"created signal id: {created_signal_id}", f"notification sent: {notification_sent}"]))


@router.message(Command("orchestrate_mock_result"))
async def cmd_orchestrate_mock_result(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    parts = (message.text or "").split()
    if len(parts) < 4:
        await message.answer("Usage: /orchestrate_mock_result <sport> <event_external_id> <winner_selection>")
        return
    try:
        sport = _parse_sport(parts[1])
        event_external_id = parts[2].strip()
        winner_selection = " ".join(parts[3:]).strip()
        data = EventResultInput(event_external_id=event_external_id, sport=sport, winner_selection=winner_selection)
    except Exception:
        await message.answer("Example: /orchestrate_mock_result CS2 cs2_10001 Team Spirit")
        return

    orch = OrchestrationService()
    async with sessionmaker() as session:
        orch_res = await orch.process_event_result(session, data)
        await session.commit()

    notifications_sent = 0
    try:
        async with sessionmaker() as session2:
            for sid in orch_res.signal_ids_to_notify:
                try:
                    sent = await orch.notify_result_if_configured(session2, message.bot, sid)
                    if sent:
                        notifications_sent += 1
                except Exception:
                    continue
    except Exception:
        notifications_sent = notifications_sent

    res = orch_res.result
    await message.answer(
        "\n".join(
            [
                f"total_signals_found: {res.total_signals_found}",
                f"settled_signals: {res.settled_signals}",
                f"skipped_signals: {res.skipped_signals}",
                f"created_failure_reviews: {res.created_failure_reviews}",
                f"processed_signal_ids: {res.processed_signal_ids}",
                f"notifications_sent_count: {notifications_sent}",
            ]
        )
    )


def _fmt_bool(v: bool) -> str:
    return "yes" if v else "no"


def _parse_tail_arg(message: Message) -> str | None:
    text = (message.text or "").strip()
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        return None
    return parts[1].strip()


def _is_http_url(value: str) -> bool:
    v = (value or "").strip().lower()
    return v.startswith("http://") or v.startswith("https://")


def _load_json_from_path(path: str) -> dict | list | None:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _build_provider_client_config_from_settings() -> ProviderClientConfig | None:
    s = get_settings()
    if not s.odds_provider_base_url:
        return None
    return ProviderClientConfig(
        base_url=s.odds_provider_base_url,
        api_key=s.odds_provider_api_key,
        sport=s.odds_provider_sport,
        regions=s.odds_provider_regions,
        markets=s.odds_provider_markets,
        bookmakers=s.odds_provider_bookmakers,
        odds_format=s.odds_provider_odds_format,
        date_format=s.odds_provider_date_format,
        timeout_seconds=int(s.odds_provider_timeout_seconds),
    )


def _require_url_or_settings_default(message: Message) -> str | None:
    url = _parse_tail_arg(message)
    if url:
        return url
    settings = get_settings()
    return settings.provider_test_url


@router.message(_text_is("Проверка данных"))
@router.message(Command("sanity_check"))
async def cmd_sanity_check(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    async with sessionmaker() as session:
        report = await SanityCheckService().run_sanity_check(session)

    shown = report.issues[:10]
    lines = [
        "🧪 Проверка данных",
        f"- Всего сигналов: {report.total_signals}",
        f"- Завершений: {report.total_settlements}",
        f"- Разборов ошибок: {report.total_failure_reviews}",
        f"- Записей входа: {report.total_entries}",
        f"- Найдено проблем: {report.issues_count}",
        "",
        "Первые проблемы:",
    ]
    if not shown:
        lines.append("- Проблем не найдено")
    else:
        for it in shown:
            sid = it.signal_id if it.signal_id is not None else "-"
            lines.append(f"- {it.issue_type} • signal_id={sid} • {it.details}")
    await message.answer("\n".join(lines))


@router.message(Command("file_preview"))
async def cmd_file_preview(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    path = _parse_tail_arg(message)
    if not path:
        await message.answer("Usage: /file_preview <path>")
        return

    provider = JsonCandidateProvider(path)
    candidates, stats = provider.load_with_stats()

    shown = candidates[:5]
    lines = [
        "FILE PREVIEW",
        f"- path: {path}",
        f"- total_items: {stats.total_items}",
        f"- loaded_candidates: {stats.loaded_candidates}",
        f"- skipped_items: {stats.skipped_items}",
        "",
        "candidates (first 5):",
    ]
    if not shown:
        lines.append("- none")
    else:
        for c in shown:
            lines.append(
                " | ".join(
                    [
                        str(getattr(c.match.sport, "value", c.match.sport)),
                        str(getattr(c.market.bookmaker, "value", c.market.bookmaker)),
                        c.match.match_name,
                        c.market.market_type,
                        c.market.selection,
                        f"odds={c.market.odds_value}",
                    ]
                )
            )
    await message.answer("\n".join(lines))


@router.message(Command("file_ingest"))
async def cmd_file_ingest(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    path = _parse_tail_arg(message)
    if not path:
        await message.answer("Usage: /file_ingest <path>")
        return

    provider = JsonCandidateProvider(path)
    candidates, stats = provider.load_with_stats()

    async with sessionmaker() as session:
        res = await IngestionService().ingest_candidates_with_filter_and_dedup(session, candidates)
        await session.commit()

    await message.answer(
        "\n".join(
            [
                "FILE INGEST",
                f"- total_items: {stats.total_items}",
                f"- loaded_candidates: {stats.loaded_candidates}",
                f"- skipped_items: {stats.skipped_items}",
                f"- created_signals: {res.created_signals}",
                f"- skipped_candidates: {res.skipped_candidates}",
                f"- created_signal_ids: {res.created_signal_ids}",
            ]
        )
    )


@router.message(Command("adapter_preview"))
async def cmd_adapter_preview(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    path = _parse_tail_arg(message)
    if not path:
        await message.answer("Usage: /adapter_preview <path>")
        return

    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if not isinstance(payload, dict):
            await message.answer("Payload must be a JSON object")
            return
    except Exception:
        await message.answer("Unable to read JSON file")
        return

    adapter = GenericOddsAdapter()
    raw = adapter.parse_payload(payload)
    res = adapter.to_candidates(raw)

    shown = res.candidates[:5]
    lines = [
        "ADAPTER PREVIEW",
        f"- source_name: {res.source_name}",
        f"- total_events: {res.total_events}",
        f"- total_markets: {res.total_markets}",
        f"- created_candidates: {res.created_candidates}",
        f"- skipped_items: {res.skipped_items}",
        "",
        "candidates (first 5):",
    ]
    if not shown:
        lines.append("- none")
    else:
        for c in shown:
            lines.append(
                " | ".join(
                    [
                        str(getattr(c.match.sport, "value", c.match.sport)),
                        str(getattr(c.market.bookmaker, "value", c.market.bookmaker)),
                        c.match.match_name,
                        c.market.market_type,
                        c.market.selection,
                        f"odds={c.market.odds_value}",
                    ]
                )
            )
    await message.answer("\n".join(lines))


@router.message(Command("adapter_ingest"))
async def cmd_adapter_ingest(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    path = _parse_tail_arg(message)
    if not path:
        await message.answer("Usage: /adapter_ingest <path>")
        return

    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if not isinstance(payload, dict):
            await message.answer("Payload must be a JSON object")
            return
    except Exception:
        await message.answer("Unable to read JSON file")
        return

    adapter = GenericOddsAdapter()
    raw = adapter.parse_payload(payload)
    adapter_res = adapter.to_candidates(raw)

    async with sessionmaker() as session:
        ing = await IngestionService().ingest_candidates_with_filter_and_dedup(session, adapter_res.candidates)
        await session.commit()

    await message.answer(
        "\n".join(
            [
                "ADAPTER INGEST",
                f"- source_name: {adapter_res.source_name}",
                f"- total_events: {adapter_res.total_events}",
                f"- total_markets: {adapter_res.total_markets}",
                f"- created_candidates: {adapter_res.created_candidates}",
                f"- skipped_items: {adapter_res.skipped_items}",
                f"- ingested_created_signals: {ing.created_signals}",
                f"- ingested_skipped_candidates: {ing.skipped_candidates}",
                f"- created_signal_ids: {ing.created_signal_ids}",
            ]
        )
    )


@router.message(Command("remote_preview"))
async def cmd_remote_preview(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    url = _parse_tail_arg(message)
    if not url:
        await message.answer("Usage: /remote_preview <url>")
        return

    settings = get_settings()
    fetch_res = await asyncio.to_thread(
        HttpFetchService().fetch_json,
        url,
        int(settings.provider_test_timeout_seconds),
    )
    if not fetch_res.ok:
        await message.answer(f"REMOTE PREVIEW\n- url: {url}\n- ok: false\n- error: {fetch_res.error}")
        return
    if not isinstance(fetch_res.payload, dict):
        await message.answer("REMOTE PREVIEW\n- ok: false\n- error: adapter expects JSON object payload")
        return

    adapter_res = AdapterIngestionService().preview_payload(fetch_res.payload)
    shown = adapter_res.candidates[:5]

    lines = [
        "REMOTE PREVIEW",
        f"- url: {url}",
        f"- ok: true",
        f"- source_name: {adapter_res.source_name}",
        f"- total_events: {adapter_res.total_events}",
        f"- total_markets: {adapter_res.total_markets}",
        f"- created_candidates: {adapter_res.created_candidates}",
        f"- skipped_items: {adapter_res.skipped_items}",
        "",
        "candidates (first 5):",
    ]
    if not shown:
        lines.append("- none")
    else:
        for c in shown:
            lines.append(
                " | ".join(
                    [
                        str(getattr(c.match.sport, "value", c.match.sport)),
                        str(getattr(c.market.bookmaker, "value", c.market.bookmaker)),
                        c.match.match_name,
                        c.market.market_type,
                        c.market.selection,
                        f"odds={c.market.odds_value}",
                    ]
                )
            )
    await message.answer("\n".join(lines))


@router.message(Command("remote_ingest"))
async def cmd_remote_ingest(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    url = _parse_tail_arg(message)
    if not url:
        await message.answer("Usage: /remote_ingest <url>")
        return

    settings = get_settings()
    fetch_res = await asyncio.to_thread(
        HttpFetchService().fetch_json,
        url,
        int(settings.provider_test_timeout_seconds),
    )
    if not fetch_res.ok:
        await message.answer(f"REMOTE INGEST\n- url: {url}\n- error: {fetch_res.error}")
        return
    if not isinstance(fetch_res.payload, dict):
        await message.answer("REMOTE INGEST\n- error: adapter expects JSON object payload")
        return

    async with sessionmaker() as session:
        adapter_res, ing = await AdapterIngestionService().ingest_payload(session, fetch_res.payload)
        await session.commit()

    await message.answer(
        "\n".join(
            [
                "REMOTE INGEST",
                f"- url: {url}",
                f"- source_name: {adapter_res.source_name}",
                f"- total_events: {adapter_res.total_events}",
                f"- total_markets: {adapter_res.total_markets}",
                f"- created_candidates: {adapter_res.created_candidates}",
                f"- skipped_items: {adapter_res.skipped_items}",
                f"- ingested_created_signals: {ing.created_signals}",
                f"- ingested_skipped_candidates: {ing.skipped_candidates}",
                f"- created_signal_ids: {ing.created_signal_ids}",
            ]
        )
    )


@router.message(Command("remote_preview_default"))
async def cmd_remote_preview_default(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    url = get_settings().provider_test_url
    if not url:
        await message.answer("PROVIDER_TEST_URL is not set")
        return

    # Reuse /remote_preview logic by calling fetch+preview inline
    settings = get_settings()
    fetch_res = await asyncio.to_thread(HttpFetchService().fetch_json, url, int(settings.provider_test_timeout_seconds))
    if not fetch_res.ok:
        await message.answer(f"REMOTE PREVIEW\n- url: {url}\n- ok: false\n- error: {fetch_res.error}")
        return
    if not isinstance(fetch_res.payload, dict):
        await message.answer("REMOTE PREVIEW\n- ok: false\n- error: adapter expects JSON object payload")
        return
    adapter_res = AdapterIngestionService().preview_payload(fetch_res.payload)
    shown = adapter_res.candidates[:5]
    lines = [
        "REMOTE PREVIEW",
        f"- url: {url}",
        f"- ok: true",
        f"- source_name: {adapter_res.source_name}",
        f"- total_events: {adapter_res.total_events}",
        f"- total_markets: {adapter_res.total_markets}",
        f"- created_candidates: {adapter_res.created_candidates}",
        f"- skipped_items: {adapter_res.skipped_items}",
        "",
        "candidates (first 5):",
    ]
    if not shown:
        lines.append("- none")
    else:
        for c in shown:
            lines.append(
                " | ".join(
                    [
                        str(getattr(c.match.sport, "value", c.match.sport)),
                        str(getattr(c.market.bookmaker, "value", c.market.bookmaker)),
                        c.match.match_name,
                        c.market.market_type,
                        c.market.selection,
                        f"odds={c.market.odds_value}",
                    ]
                )
            )
    await message.answer("\n".join(lines))


@router.message(Command("remote_ingest_default"))
async def cmd_remote_ingest_default(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    url = get_settings().provider_test_url
    if not url:
        await message.answer("PROVIDER_TEST_URL is not set")
        return

    settings = get_settings()
    fetch_res = await asyncio.to_thread(HttpFetchService().fetch_json, url, int(settings.provider_test_timeout_seconds))
    if not fetch_res.ok:
        await message.answer(f"REMOTE INGEST\n- url: {url}\n- error: {fetch_res.error}")
        return
    if not isinstance(fetch_res.payload, dict):
        await message.answer("REMOTE INGEST\n- error: adapter expects JSON object payload")
        return

    async with sessionmaker() as session:
        adapter_res, ing = await AdapterIngestionService().ingest_payload(session, fetch_res.payload)
        await session.commit()

    await message.answer(
        "\n".join(
            [
                "REMOTE INGEST",
                f"- url: {url}",
                f"- source_name: {adapter_res.source_name}",
                f"- total_events: {adapter_res.total_events}",
                f"- total_markets: {adapter_res.total_markets}",
                f"- created_candidates: {adapter_res.created_candidates}",
                f"- skipped_items: {adapter_res.skipped_items}",
                f"- ingested_created_signals: {ing.created_signals}",
                f"- ingested_skipped_candidates: {ing.skipped_candidates}",
                f"- created_signal_ids: {ing.created_signal_ids}",
            ]
        )
    )


@router.message(Command("odds_style_preview"))
async def cmd_odds_style_preview(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    arg = _parse_tail_arg(message)
    if not arg:
        await message.answer("Usage: /odds_style_preview <path_or_url>")
        return

    payload = None
    if _is_http_url(arg):
        settings = get_settings()
        fetch_res = await asyncio.to_thread(
            HttpFetchService().fetch_json, arg, int(settings.provider_test_timeout_seconds)
        )
        if not fetch_res.ok:
            await message.answer(f"ODDS STYLE PREVIEW\n- ok: false\n- error: {fetch_res.error}")
            return
        payload = fetch_res.payload
    else:
        payload = _load_json_from_path(arg)

    if not isinstance(payload, dict):
        await message.answer("ODDS STYLE PREVIEW\n- ok: false\n- error: adapter expects JSON object payload")
        return

    res = AdapterIngestionService().preview_odds_style_payload(payload)
    shown = res.candidates[:8]
    lines = [
        "ODDS STYLE PREVIEW",
        f"- source_name: {res.source_name}",
        f"- total_events: {res.total_events}",
        f"- total_markets: {res.total_markets}",
        f"- created_candidates: {res.created_candidates}",
        f"- skipped_items: {res.skipped_items}",
        "",
        "candidates (first 8):",
    ]
    if not shown:
        lines.append("- none")
    else:
        for c in shown:
            lines.append(
                " | ".join(
                    [
                        str(getattr(c.match.sport, "value", c.match.sport)),
                        str(getattr(c.market.bookmaker, "value", c.market.bookmaker)),
                        c.match.match_name,
                        c.market.market_type,
                        c.market.selection,
                        f"odds={c.market.odds_value}",
                    ]
                )
            )
    await message.answer("\n".join(lines))


@router.message(Command("odds_style_ingest"))
async def cmd_odds_style_ingest(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    arg = _parse_tail_arg(message)
    if not arg:
        await message.answer("Usage: /odds_style_ingest <path_or_url>")
        return

    payload = None
    if _is_http_url(arg):
        settings = get_settings()
        fetch_res = await asyncio.to_thread(
            HttpFetchService().fetch_json, arg, int(settings.provider_test_timeout_seconds)
        )
        if not fetch_res.ok:
            await message.answer(f"ODDS STYLE INGEST\n- error: {fetch_res.error}")
            return
        payload = fetch_res.payload
    else:
        payload = _load_json_from_path(arg)

    if not isinstance(payload, dict):
        await message.answer("ODDS STYLE INGEST\n- error: adapter expects JSON object payload")
        return

    async with sessionmaker() as session:
        adapter_res, ing = await AdapterIngestionService().ingest_odds_style_payload(session, payload)
        await session.commit()

    await message.answer(
        "\n".join(
            [
                "ODDS STYLE INGEST",
                f"- source_name: {adapter_res.source_name}",
                f"- total_events: {adapter_res.total_events}",
                f"- total_markets: {adapter_res.total_markets}",
                f"- created_candidates: {adapter_res.created_candidates}",
                f"- skipped_items: {adapter_res.skipped_items}",
                f"- ingested_created_signals: {ing.created_signals}",
                f"- ingested_skipped_candidates: {ing.skipped_candidates}",
                f"- created_signal_ids: {ing.created_signal_ids}",
            ]
        )
    )


@router.message(Command("odds_style_preview_sample"))
async def cmd_odds_style_preview_sample(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    payload = _load_json_from_path("examples/odds_style_sample.json")
    if not isinstance(payload, dict):
        await message.answer("ODDS STYLE PREVIEW\n- ok: false\n- error: unable to read sample payload")
        return
    res = AdapterIngestionService().preview_odds_style_payload(payload)
    shown = res.candidates[:8]
    lines = [
        "ODDS STYLE PREVIEW",
        f"- source_name: {res.source_name}",
        f"- total_events: {res.total_events}",
        f"- total_markets: {res.total_markets}",
        f"- created_candidates: {res.created_candidates}",
        f"- skipped_items: {res.skipped_items}",
        "",
        "candidates (first 8):",
    ]
    if not shown:
        lines.append("- none")
    else:
        for c in shown:
            lines.append(
                " | ".join(
                    [
                        str(getattr(c.match.sport, "value", c.match.sport)),
                        str(getattr(c.market.bookmaker, "value", c.market.bookmaker)),
                        c.match.match_name,
                        c.market.market_type,
                        c.market.selection,
                        f"odds={c.market.odds_value}",
                    ]
                )
            )
    await message.answer("\n".join(lines))


@router.message(Command("odds_style_ingest_sample"))
async def cmd_odds_style_ingest_sample(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    payload = _load_json_from_path("examples/odds_style_sample.json")
    if not isinstance(payload, dict):
        await message.answer("ODDS STYLE INGEST\n- error: unable to read sample payload")
        return

    async with sessionmaker() as session:
        adapter_res, ing = await AdapterIngestionService().ingest_odds_style_payload(session, payload)
        await session.commit()

    await message.answer(
        "\n".join(
            [
                "ODDS STYLE INGEST",
                f"- source_name: {adapter_res.source_name}",
                f"- total_events: {adapter_res.total_events}",
                f"- total_markets: {adapter_res.total_markets}",
                f"- created_candidates: {adapter_res.created_candidates}",
                f"- skipped_items: {adapter_res.skipped_items}",
                f"- ingested_created_signals: {ing.created_signals}",
                f"- ingested_skipped_candidates: {ing.skipped_candidates}",
                f"- created_signal_ids: {ing.created_signal_ids}",
            ]
        )
    )


@router.message(Command("odds_http_url"))
async def cmd_odds_http_url(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    config = _build_provider_client_config_from_settings()
    if config is None:
        await message.answer("ODDS_PROVIDER_BASE_URL is not set")
        return

    endpoint = OddsHttpClient().build_url(config)
    await message.answer("\n".join(["ODDS HTTP URL", endpoint]))


@router.message(Command("odds_http_preview"))
async def cmd_odds_http_preview(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    config = _build_provider_client_config_from_settings()
    if config is None:
        await message.answer("ODDS_PROVIDER_BASE_URL is not set")
        return

    fetch_res = await asyncio.to_thread(OddsHttpClient().fetch, config)
    if not fetch_res.ok or fetch_res.payload is None:
        await message.answer(
            "\n".join(
                [
                    "ODDS HTTP PREVIEW",
                    f"- endpoint: {fetch_res.endpoint}",
                    f"- ok: false",
                    f"- status_code: {fetch_res.status_code}",
                    f"- error: {fetch_res.error}",
                ]
            )
        )
        return

    adapter_res = AdapterIngestionService().preview_odds_style_payload(fetch_res.payload)
    shown = adapter_res.candidates[:8]

    lines = [
        "ODDS HTTP PREVIEW",
        f"- endpoint: {fetch_res.endpoint}",
        f"- source_name: {fetch_res.source_name}",
        f"- status_code: {fetch_res.status_code}",
        f"- total_events: {adapter_res.total_events}",
        f"- total_markets: {adapter_res.total_markets}",
        f"- created_candidates: {adapter_res.created_candidates}",
        f"- skipped_items: {adapter_res.skipped_items}",
        "",
        "candidates (first 8):",
    ]
    if not shown:
        lines.append("- none")
    else:
        for c in shown:
            lines.append(
                " | ".join(
                    [
                        str(getattr(c.match.sport, "value", c.match.sport)),
                        str(getattr(c.market.bookmaker, "value", c.market.bookmaker)),
                        c.match.match_name,
                        c.market.market_type,
                        c.market.selection,
                        f"odds={c.market.odds_value}",
                    ]
                )
            )
    await message.answer("\n".join(lines))


@router.message(Command("odds_http_ingest"))
async def cmd_odds_http_ingest(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    config = _build_provider_client_config_from_settings()
    if config is None:
        await message.answer("ODDS_PROVIDER_BASE_URL is not set")
        return

    fetch_res = await asyncio.to_thread(OddsHttpClient().fetch, config)
    if not fetch_res.ok or fetch_res.payload is None:
        await message.answer(
            "\n".join(
                [
                    "ODDS HTTP INGEST",
                    f"- endpoint: {fetch_res.endpoint}",
                    f"- ok: false",
                    f"- status_code: {fetch_res.status_code}",
                    f"- error: {fetch_res.error}",
                ]
            )
        )
        return

    async with sessionmaker() as session:
        adapter_res, ing = await AdapterIngestionService().ingest_odds_style_payload(session, fetch_res.payload)
        await session.commit()

    await message.answer(
        "\n".join(
            [
                "ODDS HTTP INGEST",
                f"- endpoint: {fetch_res.endpoint}",
                f"- source_name: {fetch_res.source_name}",
                f"- status_code: {fetch_res.status_code}",
                f"- total_events: {adapter_res.total_events}",
                f"- total_markets: {adapter_res.total_markets}",
                f"- created_candidates: {adapter_res.created_candidates}",
                f"- skipped_items: {adapter_res.skipped_items}",
                f"- ingested_created_signals: {ing.created_signals}",
                f"- ingested_skipped_candidates: {ing.skipped_candidates}",
                f"- created_signal_ids: {ing.created_signal_ids}",
            ]
        )
    )


@router.message(Command("remote_smoke"))
async def cmd_remote_smoke(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    config = _build_provider_client_config_from_settings()
    if config is None:
        await message.answer("REMOTE SMOKE\n- error: ODDS_PROVIDER_BASE_URL is not set")
        return

    res = await RemoteSmokeService().run_remote_smoke(sessionmaker, config=config)
    await message.answer(
        "\n".join(
            [
                "REMOTE SMOKE",
                f"- endpoint: {res.endpoint}",
                f"- fetch_ok: {res.fetch_ok}",
                f"- preview_candidates: {res.preview_candidates}",
                f"- preview_skipped_items: {res.preview_skipped_items}",
                f"- ingested_created_signals: {res.ingested_created_signals}",
                f"- ingested_skipped_candidates: {res.ingested_skipped_candidates}",
                f"- created_signal_ids: {res.created_signal_ids}",
                f"- sanity_issues_count: {res.sanity_issues_count}",
                f"- total_signals: {res.total_signals}",
                f"- settled_signals: {res.settled_signals}",
                f"- current_balance_rub: {res.current_balance_rub}",
                f"- message: {res.message}",
            ]
        )
    )


@router.message(Command("remote_settle"))
async def cmd_remote_settle(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    parts = _parse_required_parts(
        message,
        min_parts=4,
        example="/remote_settle FOOTBALL football_30001 Зенит",
    )
    if not parts:
        await message.answer(
            "\n".join(
                [
                    "Usage: /remote_settle <sport> <event_external_id> <winner_selection>",
                    "Examples:",
                    "- /remote_settle FOOTBALL football_30001 Зенит",
                    "- /remote_settle CS2 cs2_10001 Team Spirit",
                ]
            )
        )
        return

    try:
        sport = _parse_sport(parts[1])
    except Exception:
        await message.answer("REMOTE SETTLE\n- error: invalid sport (use FOOTBALL/CS2/DOTA2)")
        return

    event_external_id = parts[2]
    winner_selection = _parse_winner_tail(message, event_external_id=event_external_id)
    if not winner_selection:
        await message.answer("REMOTE SETTLE\n- error: winner_selection is required (it can contain spaces)")
        return

    proc = await RemoteSmokeService().settle_latest_remote_signal(
        sessionmaker,
        winner_selection=winner_selection,
        sport=sport,
        event_external_id=event_external_id,
    )
    await message.answer(
        "\n".join(
            [
                "REMOTE SETTLE",
                f"- total_signals_found: {proc.total_signals_found}",
                f"- settled_signals: {proc.settled_signals}",
                f"- skipped_signals: {proc.skipped_signals}",
                f"- created_failure_reviews: {proc.created_failure_reviews}",
                f"- processed_signal_ids: {proc.processed_signal_ids}",
            ]
        )
    )


@router.message(Command("remote_status"))
async def cmd_remote_status(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    async with sessionmaker() as session:
        summary = await AnalyticsSummaryService().get_summary(session)
        balance = await BalanceService().get_realistic_balance_overview(session)
        balance_unit = await BalanceService().get_balance_overview(session)
        snapshots = await BalanceService().list_balance_history(session)
        qsum = await SignalQualitySummaryService().build_quality_summary(session)
        sanity = await SanityCheckService().run_sanity_check(session)
        latest_ids = await SignalRepository().list_latest_signal_ids(session, limit=10)

    k = summary.kpis
    snapshots_count = len(snapshots)
    latest_snapshot_label = snapshots[0].label if snapshots else None
    await message.answer(
        "\n".join(
            [
                "🌐 Состояние remote-потока",
                f"- Всего сигналов: {k.total_signals}",
                f"- Завершённых: {k.settled_signals}",
                f"- Побед / поражений / возвратов: {k.wins}/{k.losses}/{k.voids}",
                f"- Текущий баланс (₽): {balance.current_balance_rub}",
                f"- Текущий баланс (unit): {balance_unit.current_balance}",
                f"- Итог в рублях: {balance.total_profit_loss_rub}",
                f"- Снимков баланса: {snapshots_count}",
                f"- Последняя метка: {latest_snapshot_label}",
                f"- Средняя ошибка прогноза: {qsum.avg_prediction_error}",
                f"- Переоценённых: {qsum.overestimated_count}",
                f"- Недооценённых: {qsum.underestimated_count}",
                f"- Проблем в данных: {sanity.issues_count}",
                f"- Последние ID сигналов: {latest_ids}",
            ]
        )
    )


@router.message(Command("server_checklist"))
async def cmd_server_checklist(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    s = get_settings()
    await message.answer(
        "\n".join(
            [
                "🖥 Проверка настроек сервера",
                f"- Токен бота задан: {_fmt_yes_no(bool(getattr(s, 'bot_token', '') or ''))}",
                f"- Подключение к БД задано: {_fmt_yes_no(bool(getattr(s, 'database_url', '') or ''))}",
                f"- Чат сигналов задан: {_fmt_yes_no(getattr(s, 'signal_chat_id', None) is not None)}",
                f"- Чат результатов задан: {_fmt_yes_no(getattr(s, 'result_chat_id', None) is not None)}",
                f"- URL remote provider задан: {_fmt_yes_no(bool(getattr(s, 'odds_provider_base_url', None)))}",
                f"- Вид спорта provider задан: {_fmt_yes_no(bool(getattr(s, 'odds_provider_sport', None)))}",
                f"- Рынки provider заданы: {_fmt_yes_no(bool(getattr(s, 'odds_provider_markets', None)))}",
                f"- Admin ID заданы: {_fmt_yes_no(bool(getattr(s, 'admin_user_ids', []) or []))}",
                f"- Таймаут provider: {getattr(s, 'odds_provider_timeout_seconds', None)} сек.",
                f"- Фиксированная ставка: {getattr(s, 'virtual_flat_stake_rub', None)} ₽",
                f"- Стартовый баланс: {getattr(s, 'virtual_start_balance_rub', None)} ₽",
            ]
        )
    )


@router.message(Command("remote_flow_demo"))
async def cmd_remote_flow_demo(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    config = _build_provider_client_config_from_settings()
    if config is None:
        await message.answer("REMOTE FLOW DEMO\n- error: ODDS_PROVIDER_BASE_URL is not set")
        return

    parts = _parse_required_parts(
        message,
        min_parts=4,
        example="/remote_flow_demo FOOTBALL football_30001 Зенит",
    )
    if not parts:
        await message.answer(
            "\n".join(
                [
                    "Usage: /remote_flow_demo <sport> <event_external_id> <winner_selection>",
                    "Examples:",
                    "- /remote_flow_demo FOOTBALL football_30001 Зенит",
                    "- /remote_flow_demo CS2 cs2_10001 Team Spirit",
                ]
            )
        )
        return

    try:
        sport = _parse_sport(parts[1])
    except Exception:
        await message.answer("REMOTE FLOW DEMO\n- error: invalid sport (use FOOTBALL/CS2/DOTA2)")
        return

    event_external_id = parts[2]
    winner_selection = _parse_winner_tail(message, event_external_id=event_external_id)
    if not winner_selection:
        await message.answer("REMOTE FLOW DEMO\n- error: winner_selection is required (it can contain spaces)")
        return

    smoke = await RemoteSmokeService().run_remote_smoke(sessionmaker, config=config)
    if not smoke.fetch_ok:
        await message.answer(
            "\n".join(
                [
                    "REMOTE FLOW DEMO",
                    f"- smoke_fetch_ok: false",
                    f"- endpoint: {smoke.endpoint}",
                    f"- message: {smoke.message}",
                ]
            )
        )
        return

    if not smoke.created_signal_ids:
        await message.answer(
            "\n".join(
                [
                    "REMOTE FLOW DEMO",
                    f"- smoke_created_signal_ids: {smoke.created_signal_ids}",
                    f"- message: no new signals created",
                ]
            )
        )
        return

    settle = await RemoteSmokeService().settle_latest_remote_signal(
        sessionmaker,
        winner_selection=winner_selection,
        sport=sport,
        event_external_id=event_external_id,
    )

    async with sessionmaker() as session:
        summary = await AnalyticsSummaryService().get_summary(session)
        balance = await BalanceService().get_realistic_balance_overview(session)
        qsum = await SignalQualitySummaryService().build_quality_summary(session)
        sanity = await SanityCheckService().run_sanity_check(session)

    k = summary.kpis
    await message.answer(
        "\n".join(
            [
                "REMOTE FLOW DEMO",
                f"- smoke_created_signal_ids: {smoke.created_signal_ids}",
                f"- settle_processed_signal_ids: {settle.processed_signal_ids}",
                f"- total_signals: {k.total_signals}",
                f"- settled_signals: {k.settled_signals}",
                f"- wins/losses/voids: {k.wins}/{k.losses}/{k.voids}",
                f"- current_balance_rub: {balance.current_balance_rub}",
                f"- avg_prediction_error: {qsum.avg_prediction_error}",
                f"- overestimated_count: {qsum.overestimated_count}",
                f"- underestimated_count: {qsum.underestimated_count}",
                f"- sanity_issues_count: {sanity.issues_count}",
            ]
        )
    )


@router.message(_text_is("Помощь"))
@router.message(Command("debug_help"))
async def cmd_debug_help(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    await message.answer(
        "\n".join(
            [
                "📚 Список команд",
                "",
                "Основное",
                "- /start — открыть меню",
                "- /ping — проверить, что бот жив",
                "- /debug_help — показать список команд",
                "",
                "Сигналы",
                "- /latest_signals",
                "- /latest_results",
                "- /latest_failures",
                "- /signal_report <id>",
                "- /signal_quality <id>",
                "",
                "Аналитика",
                "- /quick_check",
                "- /system_status",
                "- /quality_summary",
                "- /sanity_check",
                "",
                "Баланс",
                "- /balance",
                "- /balance_rub",
                "- /period_report",
                "- /period_report_rub",
                "- /reset_balance <сумма> [метка]",
                "- /balance_history",
                "",
                "Автосигналы",
                "- /auto_signal_status",
                "- /auto_signal_run_once",
                "",
                "Remote / ingest",
                "- /odds_http_url",
                "- /odds_http_preview",
                "- /odds_http_ingest",
                "- /remote_smoke",
                "- /remote_settle <sport> <event_external_id> <winner_selection>",
                "- /remote_flow_demo <sport> <event_external_id> <winner_selection>",
                "- /remote_status",
                "",
                "Debug",
                "- /whoami",
                "- /server_checklist",
                "- /regression_pack",
            ]
        ),
        reply_markup=get_debug_keyboard(),
    )


@router.message(Command("regression_pack"))
async def cmd_regression_pack(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    win_demo = await DemoCycleService().run_mock_demo_cycle(sessionmaker, message.bot, scenario="win")
    lose_demo = await DemoCycleService().run_mock_demo_cycle(sessionmaker, message.bot, scenario="lose")
    void_demo = await DemoCycleService().run_mock_demo_cycle(sessionmaker, message.bot, scenario="void")

    async with sessionmaker() as session:
        sanity = await SanityCheckService().run_sanity_check(session)
        summary = await AnalyticsSummaryService().get_summary(session)
        bal_rub = await BalanceService().get_realistic_balance_overview(session)
        qsum = await SignalQualitySummaryService().build_quality_summary(session)

    await message.answer(
        "\n".join(
            [
                "REGRESSION PACK",
                f"- win_demo_created_signal_id: {win_demo.created_signal_id} ({win_demo.message})",
                f"- lose_demo_created_signal_id: {lose_demo.created_signal_id} ({lose_demo.message})",
                f"- void_demo_created_signal_id: {void_demo.created_signal_id} ({void_demo.message})",
                f"- total_signals: {summary.kpis.total_signals}",
                f"- settled_signals: {summary.kpis.settled_signals}",
                f"- issues_count: {sanity.issues_count}",
                f"- current_balance_rub: {bal_rub.current_balance_rub}",
                f"- avg_prediction_error: {qsum.avg_prediction_error}",
                f"- overestimated_count: {qsum.overestimated_count}",
                f"- underestimated_count: {qsum.underestimated_count}",
            ]
        )
    )

@router.message(_text_is("Последние сигналы"))
@router.message(Command("latest_signals"))
async def cmd_latest_signals(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    parts = (message.text or "").split()
    limit = 10
    if len(parts) >= 2:
        try:
            limit = int(parts[1])
        except Exception:
            await message.answer("Usage: /latest_signals [limit] (example: /latest_signals 20)")
            return

    if limit <= 0:
        await message.answer("limit must be > 0 (max 30)")
        return
    if limit > 30:
        limit = 30

    async with sessionmaker() as session:
        signals = await SignalRepository().list_latest_signals(session, limit=limit)

    if not signals:
        await message.answer("📭 Пока сигналов нет.")
        return

    lines: list[str] = ["📌 Последние сигналы", ""]
    for s in signals:
        lines.append(_format_latest_signal_card(s))
        lines.append("")

    await message.answer("\n".join(lines).rstrip())


@router.message(_text_is("Последние результаты"))
@router.message(Command("latest_results"))
async def cmd_latest_results(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    parts = (message.text or "").split()
    limit = 10
    if len(parts) >= 2:
        try:
            limit = int(parts[1])
        except Exception:
            await message.answer("Usage: /latest_results [limit] (example: /latest_results 20)")
            return
    if limit <= 0:
        await message.answer("limit must be > 0 (max 30)")
        return
    if limit > 30:
        limit = 30

    async with sessionmaker() as session:
        signals = await SignalRepository().list_latest_settled_signals(session, limit=limit)

        if not signals:
            await message.answer("📭 Завершённых сигналов пока нет.")
            return

        lines: list[str] = ["✅ Последние результаты", ""]
        for s in signals:
            quality_label = "-"
            try:
                qr = await SignalQualityService().build_signal_quality_report(session, int(s.id))
                quality_label = qr.metrics.quality_label or "-"
            except Exception:
                quality_label = "-"

            lines.append(_format_latest_result_card(s, quality_label))
            lines.append("")

    await message.answer("\n".join(lines).rstrip())


@router.message(Command("latest_failures"))
async def cmd_latest_failures(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    parts = (message.text or "").split()
    limit = 10
    if len(parts) >= 2:
        try:
            limit = int(parts[1])
        except Exception:
            await message.answer("Usage: /latest_failures [limit] (example: /latest_failures 10)")
            return
    if limit <= 0:
        await message.answer("limit must be > 0 (max 30)")
        return
    if limit > 30:
        limit = 30

    async with sessionmaker() as session:
        signals = await SignalRepository().list_latest_failed_signals(session, limit=limit)

    if not signals:
        await message.answer("📭 Неудачных сигналов пока нет.")
        return

    lines: list[str] = ["⚠️ Последние неудачные сигналы", ""]
    for s in signals:
        category = "-"
        reason = "-"
        if getattr(s, "failure_reviews", None):
            r0 = s.failure_reviews[0]
            category = getattr(r0.category, "value", r0.category) if r0.category is not None else "-"
            reason = (r0.auto_reason or r0.manual_reason or "-")

        lines.append(_format_latest_failure_card(s, _fmt_failure_category_ru(str(category)), reason))
        lines.append("")

    await message.answer("\n".join(lines).rstrip())


@router.message(_text_is("Быстрая проверка"))
@router.message(Command("quick_check"))
async def cmd_quick_check(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    async with sessionmaker() as session:
        summary = await AnalyticsSummaryService().get_summary(session)
        balance_unit = await BalanceService().get_balance_overview(session)
        balance_rub = await BalanceService().get_realistic_balance_overview(session)
        latest_ids = await SignalRepository().list_latest_signal_ids(session, limit=5)

    k = summary.kpis
    latest_ids_str = ", ".join(str(x) for x in latest_ids) if latest_ids else "-"

    await message.answer(
        "\n".join(
            [
                "⚡ Быстрая проверка",
                f"- Всего сигналов: {k.total_signals}",
                f"- Завершённых: {k.settled_signals}",
                f"- Побед / поражений / возвратов: {k.wins}/{k.losses}/{k.voids}",
                f"- Баланс (unit): {balance_unit.current_balance}",
                f"- Баланс (₽): {balance_rub.current_balance_rub}",
                f"- Последние ID: {latest_ids_str}",
            ]
        )
    )


@router.message(Command("demo_smoke"))
async def cmd_demo_smoke(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    demo = await DemoCycleService().run_mock_demo_cycle(sessionmaker, message.bot, scenario="win")

    async with sessionmaker() as session:
        summary = await AnalyticsSummaryService().get_summary(session)
        bal_rub = await BalanceService().get_realistic_balance_overview(session)
        qsum = await SignalQualitySummaryService().build_quality_summary(session)

    await message.answer(
        "\n".join(
            [
                "DEMO SMOKE",
                f"- scenario: {demo.scenario}",
                f"- created_signal_id: {demo.created_signal_id}",
                f"- settled_signals: {summary.kpis.settled_signals}",
                f"- notifications_sent_result: {demo.result_notification_sent_count}",
                f"- current_balance_rub: {bal_rub.current_balance_rub}",
                f"- avg_prediction_error: {qsum.avg_prediction_error}",
                f"- overestimated_count: {qsum.overestimated_count}",
                f"- underestimated_count: {qsum.underestimated_count}",
                f"- message: {demo.message}",
            ]
        )
    )


@router.message(_text_is("Статус системы"))
@router.message(Command("system_status"))
async def cmd_system_status(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    async with sessionmaker() as session:
        summary = await AnalyticsSummaryService().get_summary(session)
        balance_unit = await BalanceService().get_balance_overview(session)
        balance_rub = await BalanceService().get_realistic_balance_overview(session)
        period_unit = await PeriodReportService().get_period_report(session)
        period_rub = await PeriodReportService().get_realistic_period_report(session)
        quality = await SignalQualitySummaryService().build_quality_summary(session)
        history = await BalanceService().list_balance_history(session)
        latest_ids = await SignalRepository().list_latest_signal_ids(session, limit=10)

    k = summary.kpis
    latest_snapshot_label = history[0].label if history else None
    latest_ids_str = ", ".join(str(x) for x in latest_ids) if latest_ids else "-"

    lines = [
        "📊 Состояние системы",
        "",
        "Сигналы:",
        f"- Всего: {k.total_signals}",
        f"- Завершённых: {k.settled_signals}",
        f"- Введённых: {k.entered_signals}",
        f"- Пропущенных: {k.missed_signals}",
        "",
        "Баланс (unit):",
        f"- Текущий баланс: {balance_unit.current_balance}",
        f"- Прибыль с точки отсчёта: {balance_unit.total_profit_loss_since_base}",
        "",
        "Баланс (₽):",
        f"- Фиксированная ставка: {balance_rub.flat_stake_rub}",
        f"- Текущий баланс (₽): {balance_rub.current_balance_rub}",
        f"- Итог в рублях: {balance_rub.total_profit_loss_rub}",
        "",
        "Период (unit):",
        f"- Метка периода: {period_unit.overview.period_label}",
        f"- Текущий баланс: {period_unit.overview.current_balance}",
        "",
        "Период (₽):",
        f"- Метка периода: {period_rub.overview.period_label}",
        f"- Текущий баланс (₽): {period_rub.overview.current_balance_rub}",
        "",
        "Качество:",
        f"- Средняя ошибка прогноза: {quality.avg_prediction_error}",
        f"- Переоценённых: {quality.overestimated_count}",
        f"- Недооценённых: {quality.underestimated_count}",
        "",
        "История баланса:",
        f"- Количество снимков: {len(history)}",
        f"- Последняя метка: {latest_snapshot_label or 'нет'}",
        "",
        "Последние сигналы:",
        f"- {latest_ids_str}",
    ]
    await message.answer("\n".join(lines))

@router.message(Command("demo_cycle"))
async def cmd_demo_cycle(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    res = await DemoCycleService().run_mock_demo_cycle(sessionmaker, message.bot, scenario="win")
    await message.answer(
        "\n".join(
            [
                f"scenario: {res.scenario}",
                f"created_signal_id: {res.created_signal_id}",
                f"signal_notification_sent: {_fmt_bool(res.signal_notification_sent)}",
                f"result_processed: {_fmt_bool(res.result_processed)}",
                f"result_notification_sent_count: {res.result_notification_sent_count}",
                f"total_signals_found: {res.total_signals_found}",
                f"settled_signals: {res.settled_signals}",
                f"skipped_signals: {res.skipped_signals}",
                f"created_failure_reviews: {res.created_failure_reviews}",
                f"balance_mode_unit_current: {res.balance_mode_unit_current}",
                f"balance_mode_rub_current: {res.balance_mode_rub_current}",
                f"message: {res.message}",
            ]
        )
    )


@router.message(Command("demo_cycle_sport"))
async def cmd_demo_cycle_sport(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Usage: /demo_cycle_sport <CS2|DOTA2|FOOTBALL>")
        return
    try:
        sport = _parse_sport(parts[1])
    except Exception:
        await message.answer("Example: /demo_cycle_sport CS2")
        return

    res = await DemoCycleService().run_mock_demo_cycle(sessionmaker, message.bot, sport=sport, scenario="win")
    await message.answer(
        "\n".join(
            [
                f"scenario: {res.scenario}",
                f"created_signal_id: {res.created_signal_id}",
                f"signal_notification_sent: {_fmt_bool(res.signal_notification_sent)}",
                f"result_processed: {_fmt_bool(res.result_processed)}",
                f"result_notification_sent_count: {res.result_notification_sent_count}",
                f"total_signals_found: {res.total_signals_found}",
                f"settled_signals: {res.settled_signals}",
                f"skipped_signals: {res.skipped_signals}",
                f"created_failure_reviews: {res.created_failure_reviews}",
                f"balance_mode_unit_current: {res.balance_mode_unit_current}",
                f"balance_mode_rub_current: {res.balance_mode_rub_current}",
                f"message: {res.message}",
            ]
        )
    )


@router.message(Command("demo_cycle_win"))
async def cmd_demo_cycle_win(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    res = await DemoCycleService().run_mock_demo_cycle(sessionmaker, message.bot, scenario="win")
    await message.answer(
        "\n".join(
            [
                f"scenario: {res.scenario}",
                f"created_signal_id: {res.created_signal_id}",
                f"signal_notification_sent: {_fmt_bool(res.signal_notification_sent)}",
                f"result_processed: {_fmt_bool(res.result_processed)}",
                f"result_notification_sent_count: {res.result_notification_sent_count}",
                f"total_signals_found: {res.total_signals_found}",
                f"settled_signals: {res.settled_signals}",
                f"skipped_signals: {res.skipped_signals}",
                f"created_failure_reviews: {res.created_failure_reviews}",
                f"balance_mode_unit_current: {res.balance_mode_unit_current}",
                f"balance_mode_rub_current: {res.balance_mode_rub_current}",
                f"message: {res.message}",
            ]
        )
    )


@router.message(Command("demo_cycle_lose"))
async def cmd_demo_cycle_lose(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    res = await DemoCycleService().run_mock_demo_cycle(sessionmaker, message.bot, scenario="lose")
    await message.answer(
        "\n".join(
            [
                f"scenario: {res.scenario}",
                f"created_signal_id: {res.created_signal_id}",
                f"signal_notification_sent: {_fmt_bool(res.signal_notification_sent)}",
                f"result_processed: {_fmt_bool(res.result_processed)}",
                f"result_notification_sent_count: {res.result_notification_sent_count}",
                f"total_signals_found: {res.total_signals_found}",
                f"settled_signals: {res.settled_signals}",
                f"skipped_signals: {res.skipped_signals}",
                f"created_failure_reviews: {res.created_failure_reviews}",
                f"balance_mode_unit_current: {res.balance_mode_unit_current}",
                f"balance_mode_rub_current: {res.balance_mode_rub_current}",
                f"message: {res.message}",
            ]
        )
    )


@router.message(Command("demo_cycle_void"))
async def cmd_demo_cycle_void(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    res = await DemoCycleService().run_mock_demo_cycle(sessionmaker, message.bot, scenario="void")
    await message.answer(
        "\n".join(
            [
                f"scenario: {res.scenario}",
                f"created_signal_id: {res.created_signal_id}",
                f"signal_notification_sent: {_fmt_bool(res.signal_notification_sent)}",
                f"result_processed: {_fmt_bool(res.result_processed)}",
                f"result_notification_sent_count: {res.result_notification_sent_count}",
                f"total_signals_found: {res.total_signals_found}",
                f"settled_signals: {res.settled_signals}",
                f"skipped_signals: {res.skipped_signals}",
                f"created_failure_reviews: {res.created_failure_reviews}",
                f"balance_mode_unit_current: {res.balance_mode_unit_current}",
                f"balance_mode_rub_current: {res.balance_mode_rub_current}",
                f"message: {res.message}",
            ]
        )
    )


@router.message(Command("demo_cycle_sport_scenario"))
async def cmd_demo_cycle_sport_scenario(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.answer("Usage: /demo_cycle_sport_scenario <CS2|DOTA2|FOOTBALL> <win|lose|void>")
        return
    try:
        sport = _parse_sport(parts[1])
        scenario = parts[2].strip().lower()
    except Exception:
        await message.answer("Example: /demo_cycle_sport_scenario CS2 lose")
        return

    res = await DemoCycleService().run_mock_demo_cycle(sessionmaker, message.bot, sport=sport, scenario=scenario)
    await message.answer(
        "\n".join(
            [
                f"scenario: {res.scenario}",
                f"created_signal_id: {res.created_signal_id}",
                f"signal_notification_sent: {_fmt_bool(res.signal_notification_sent)}",
                f"result_processed: {_fmt_bool(res.result_processed)}",
                f"result_notification_sent_count: {res.result_notification_sent_count}",
                f"total_signals_found: {res.total_signals_found}",
                f"settled_signals: {res.settled_signals}",
                f"skipped_signals: {res.skipped_signals}",
                f"created_failure_reviews: {res.created_failure_reviews}",
                f"balance_mode_unit_current: {res.balance_mode_unit_current}",
                f"balance_mode_rub_current: {res.balance_mode_rub_current}",
                f"message: {res.message}",
            ]
        )
    )


@router.message(Command("winline_manual_upload_line"))
@router.message(_text_is("Winline загрузить JSON линии", "Winline загрузить line", "Winline upload line"))
async def cmd_winline_manual_upload_line(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    uid = message.from_user.id if message.from_user else None
    if uid is None:
        return
    _pending_manual_json_upload[uid] = "line"
    await message.answer("📥 Пришлите JSON-файл line payload документом")


@router.message(Command("winline_runtime_source"))
@router.message(_text_is("Winline runtime source", "Winline runtime", "Winline источник runtime"))
async def cmd_winline_runtime_source(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    await message.answer("\n".join(_format_winline_runtime_source_lines()))


@router.message(Command("winline_manual_upload_result"))
@router.message(_text_is("Winline загрузить JSON результата", "Winline загрузить result", "Winline upload result"))
async def cmd_winline_manual_upload_result(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    uid = message.from_user.id if message.from_user else None
    if uid is None:
        return
    _pending_manual_json_upload[uid] = "result"
    await message.answer("📥 Пришлите JSON-файл result payload документом")


@router.message(Command("winline_clear_uploaded_line"))
@router.message(_text_is("Очистить загруженный line JSON", "Winline очистить загруженный line", "Winline clear uploaded line"))
async def cmd_winline_clear_uploaded_line(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    from app.services.winline_manual_file_storage_service import WinlineManualFileStorageService

    r = WinlineManualFileStorageService().clear_uploaded_line_payload()
    if r.get("ok"):
        await message.answer(
            "\n".join(
                [
                    "✅ Uploaded line runtime очищен",
                    f"- runtime path: {r.get('path')}",
                    f"- metadata path: {r.get('metadata_path')}",
                    f"- что-то удалено: {_fmt_yes_no(bool(r.get('deleted_any')))}",
                ]
            )
        )
    else:
        await message.answer(f"⚠️ Не удалось очистить uploaded line runtime.\n{r.get('error')}")


@router.message(Command("winline_manual_clear_line"))
@router.message(_text_is("Winline очистить линию", "Winline очистить line", "Winline clear line"))
async def cmd_winline_manual_clear_line(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    from app.services.winline_manual_file_storage_service import WinlineManualFileStorageService

    r = WinlineManualFileStorageService().clear_line_payload()
    if r.get("ok"):
        await message.answer("✅ line payload очищен")
    else:
        await message.answer(f"⚠️ Не удалось очистить line.\n{r.get('error')}")


@router.message(Command("winline_manual_clear_result"))
@router.message(_text_is("Winline очистить результат", "Winline очистить result", "Winline clear result"))
async def cmd_winline_manual_clear_result(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    from app.services.winline_manual_file_storage_service import WinlineManualFileStorageService

    r = WinlineManualFileStorageService().clear_result_payload()
    if r.get("ok"):
        await message.answer("✅ result payload очищен")
    else:
        await message.answer(f"⚠️ Не удалось очистить result.\n{r.get('error')}")


@router.message(Command("winline_manual_file_status"))
@router.message(_text_is("Winline статус файлов", "Winline file status"))
async def cmd_winline_manual_file_status(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    try:
        from app.services.winline_manual_cycle_service import WinlineManualCycleService
        from app.services.winline_manual_payload_service import WinlineManualPayloadService

        svc = WinlineManualCycleService()
        r = svc.get_operator_readiness()
        st = r.get("storage") or {}
        payloads = WinlineManualPayloadService()
        truth = payloads.get_line_source_truth()
        lines = [
            "📁 Winline manual files",
            f"- effective line path: {payloads.get_line_payload_path()}",
            f"- uploaded line path: {payloads.get_uploaded_line_payload_path()}",
            f"- example line path: {payloads.get_example_line_payload_path()}",
            f"- line exists: {_fmt_yes_no(bool(st.get('line_exists')))}",
            f"- line size: {st.get('line_size_bytes')} B",
            f"- line readable: {_fmt_yes_no(bool(st.get('line_readable')))}",
            f"- line keys: {', '.join(st.get('line_keys') or []) or '—'}",
            f"- line shape: {(r.get('line_preview_meta') or {}).get('detected_shape') or '—'}",
            f"- line ingestible: {_fmt_yes_no(bool(r.get('line_ready_for_ingest')))}",
            f"- source_mode: {truth.get('source_mode') or '—'}",
            f"- is_real_source: {_fmt_yes_no(bool(truth.get('is_real_source')))}",
            f"- source_origin: {truth.get('source_origin') or '—'}",
            f"- upload provenance present: {_fmt_yes_no(bool(truth.get('provenance_present')))}",
            f"- uploaded_at: {truth.get('uploaded_at') or '—'}",
            f"- checksum: {truth.get('checksum') or '—'}",
            f"- fixture match: {_fmt_yes_no(bool(truth.get('fixture_match')))}",
            f"- result exists: {_fmt_yes_no(bool(st.get('result_exists')))}",
            f"- result size: {st.get('result_size_bytes')} B",
            f"- result readable: {_fmt_yes_no(bool(st.get('result_readable')))}",
            f"- result keys: {', '.join(st.get('result_keys') or []) or '—'}",
            f"- result shape: {(r.get('result_preview_meta') or {}).get('detected_shape') or '—'}",
            f"- result processible: {_fmt_yes_no(bool(r.get('result_ready_for_process')))}",
            "- line mapping rules: active",
            "- result mapping rules: active",
            f"- line ready for preview: {_fmt_yes_no(bool(r.get('line_ready_for_preview')))}",
            f"- line ready for ingest: {_fmt_yes_no(bool(r.get('line_ready_for_ingest')))}",
            f"- result ready for preview: {_fmt_yes_no(bool(r.get('result_ready_for_preview')))}",
            f"- result ready for process: {_fmt_yes_no(bool(r.get('result_ready_for_process')))}",
            f"- recommended: {r.get('recommended_next_action') or '—'}",
        ]
        if st.get("line_error"):
            lines.append(f"- line err: {st['line_error']}")
        if st.get("result_error"):
            lines.append(f"- result err: {st['result_error']}")
        lines.append(svc.next_step_hint("run_ready"))
        await message.answer("\n".join(lines))
    except Exception as exc:
        await message.answer(f"⚠️ Статус файлов недоступен.\nКратко: {exc!s}")


@router.message(Command("winline_manual_show_line"))
@router.message(_text_is("Winline показать JSON линии", "Winline показать line", "Winline show line"))
async def cmd_winline_manual_show_line(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    from app.services.winline_manual_file_storage_service import WinlineManualFileStorageService

    text = WinlineManualFileStorageService().read_line_payload_text()
    for part in _json_snippet_messages("📄 line payload snippet", text):
        await message.answer(part)


@router.message(Command("winline_manual_show_result"))
@router.message(_text_is("Winline показать JSON результата", "Winline показать result", "Winline show result"))
async def cmd_winline_manual_show_result(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    from app.services.winline_manual_file_storage_service import WinlineManualFileStorageService

    text = WinlineManualFileStorageService().read_result_payload_text()
    for part in _json_snippet_messages("📄 result payload snippet", text):
        await message.answer(part)


@router.message(F.document, _PendingManualJsonUploadFilter())
async def handle_winline_manual_json_document(message: Message, bot: Bot) -> None:
    if not _is_allowed(message):
        await _deny(message)
        if message.from_user:
            _pending_manual_json_upload.pop(message.from_user.id, None)
        return
    uid = message.from_user.id if message.from_user else None
    if uid is None or not message.document:
        return
    kind = _pending_manual_json_upload.get(uid)
    if not kind:
        return

    doc = message.document
    if doc.file_size and doc.file_size > _MAX_MANUAL_JSON_UPLOAD_BYTES:
        await message.answer(
            f"⚠️ Файл слишком большой (лимит {_MAX_MANUAL_JSON_UPLOAD_BYTES // (1024 * 1024)} MB). Пришлите меньший JSON."
        )
        return

    try:
        buf = BytesIO()
        await bot.download(doc, destination=buf)
        data = buf.getvalue()
    except Exception as exc:
        await message.answer(f"⚠️ Не удалось скачать файл.\nКратко: {exc!s}")
        return

    if len(data) > _MAX_MANUAL_JSON_UPLOAD_BYTES:
        await message.answer("⚠️ После скачивания файл всё ещё слишком большой.")
        return

    fname = (doc.file_name or "").strip().lower()
    if fname and not fname.endswith(".json"):
        await message.answer(
            "ℹ️ Имя файла не .json — пробуем распарсить содержимое как JSON."
        )

    from app.services.winline_manual_cycle_service import WinlineManualCycleService
    from app.services.winline_manual_file_storage_service import WinlineManualFileStorageService
    from app.services.winline_manual_payload_service import WinlineManualPayloadService

    storage = WinlineManualFileStorageService()
    if kind == "line":
        res = storage.save_line_payload_bytes(data)
    else:
        res = storage.save_result_payload_bytes(data)

    if not res.get("ok"):
        await message.answer(
            "\n".join(
                [
                    "⚠️ JSON не сохранён (ошибка валидации). Старый файл не изменён.",
                    f"- причина: {res.get('error')}",
                    "Пришлите исправленный файл или начните заново командой загрузки.",
                ]
            )
        )
        return

    _pending_manual_json_upload.pop(uid, None)

    keys = res.get("top_level_keys") or []
    keys_s = ", ".join(keys[:20]) + ("…" if len(keys) > 20 else "")
    payloads = WinlineManualPayloadService()
    if kind == "line":
        truth = payloads.get_line_source_truth()
        preview = payloads.preview_line_payload()
        cycle_preview = WinlineManualCycleService().preview_manual_line()
        await message.answer(
            "\n".join(
                [
                    "✅ Line JSON сохранён",
                    f"- путь: {res.get('path')}",
                    f"- размер: {res.get('bytes')} B",
                    f"- top-level: {res.get('top_level_type')}",
                    f"- keys: {keys_s or '—'}",
                    f"- source_mode: {truth.get('source_mode') or '—'}",
                    f"- is_real_source: {_fmt_yes_no(bool(truth.get('is_real_source')))}",
                    f"- source_origin: {truth.get('source_origin') or '—'}",
                    f"- uploaded_at: {truth.get('uploaded_at') or '—'}",
                    f"- checksum: {truth.get('checksum') or '—'}",
                    f"- fixture_match: {_fmt_yes_no(bool(truth.get('fixture_match')))}",
                    f"- raw_events: {preview.get('raw_events_count') if preview.get('raw_events_count') is not None else '—'}",
                    f"- normalized_markets: {preview.get('normalized_markets_count') if preview.get('normalized_markets_count') is not None else '—'}",
                    f"- candidates_preview: {preview.get('preview_candidates') if preview.get('preview_candidates') is not None else '—'}",
                    f"- final_previews: {cycle_preview.get('final_signals_ready') if cycle_preview.get('final_signals_ready') is not None else '—'}",
                ]
            )
        )
        if truth.get("fixture_match"):
            await message.answer(
                "⚠️ Этот файл совпадает с bundled fixture/example. "
                "Он будет помечен как manual_example и не будет использован как боевой football source."
            )
        await message.answer("\n".join(_format_manual_line_preview_lines()))
        await message.answer(
            "Что дальше можно сделать:\n"
            "- /winline_runtime_source — проверить текущий runtime source\n"
            "- ⚽ Прогон — тестовый прогон (без БД и без канала)\n"
            "- /winline_clear_uploaded_line — убрать runtime uploaded line JSON",
            reply_markup=get_winline_manual_flow_keyboard(),
        )
    else:
        await message.answer(
            "\n".join(
                [
                    "✅ Result JSON сохранён",
                    f"- путь: {res.get('path')}",
                    f"- размер: {res.get('bytes')} B",
                    f"- top-level: {res.get('top_level_type')}",
                    f"- keys: {keys_s or '—'}",
                    f"- checksum: {res.get('checksum') or '—'}",
                ]
            )
        )
        await message.answer("\n".join(_format_manual_result_preview_lines()))
        await message.answer(
            "Что дальше можно сделать:\n"
            "- Winline превью результата — посмотреть preview result\n"
            "- Winline обработать результат — обработать результаты\n"
            "- Winline полный цикл — попробовать полный цикл",
            reply_markup=get_winline_manual_flow_keyboard(),
        )


@router.message(Command("winline_manual_status"))
@router.message(_text_is("Winline ручной статус", "Winline manual статус"))
async def cmd_winline_manual_status(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    try:
        from app.services.winline_manual_payload_service import WinlineManualPayloadService

        m = WinlineManualPayloadService()
        settings = get_settings()
        line_ex = m.line_payload_exists()
        res_ex = m.result_payload_exists()
        ld, le = m.load_line_payload()
        rd, re = m.load_result_payload()
        line_ok = ld is not None and le is None
        res_ok = rd is not None and re is None
        chat_ok = settings.signal_chat_id is not None
        lines = [
            "📁 Winline manual JSON",
            f"- line file exists: {_fmt_yes_no(line_ex)}",
            f"- result file exists: {_fmt_yes_no(res_ex)}",
            f"- line payload readable: {_fmt_yes_no(line_ok)}",
            f"- result payload readable: {_fmt_yes_no(res_ok)}",
            f"- signal chat configured: {_fmt_yes_no(chat_ok)}",
        ]
        if le:
            lines.append(f"- line load: {le}")
        if re:
            lines.append(f"- result load: {re}")
        await message.answer("\n".join(lines))
    except Exception as exc:
        await message.answer(f"⚠️ Статус manual недоступен.\nКратко: {exc!s}")


@router.message(Command("winline_manual_line_preview"))
@router.message(_text_is("Winline превью линии", "Winline line превью", "Winline manual line"))
async def cmd_winline_manual_line_preview(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    try:
        from app.services.winline_manual_cycle_service import WinlineManualCycleService

        lines = _format_manual_line_preview_lines()
        lines.append(WinlineManualCycleService().next_step_hint("line_preview"))
        await message.answer("\n".join(lines))
    except Exception as exc:
        await message.answer(f"⚠️ Превью line не получилось.\nКратко: {exc!s}")


@router.message(Command("winline_manual_line_ingest"))
@router.message(_text_is("Winline загрузить сигналы", "Winline ingest линии", "Winline ingest line", "Winline manual ingest"))
async def cmd_winline_manual_line_ingest(
    message: Message, sessionmaker: async_sessionmaker[AsyncSession]
) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    try:
        from app.services.winline_manual_cycle_service import WinlineManualCycleService

        svc = WinlineManualCycleService()
        r = await svc.ingest_manual_line(sessionmaker)
        lines = [
            "⬇️ Winline manual — line ingest",
            f"- status: {r.get('status')}",
            f"- created_signals: {r.get('created_signals')}",
            f"- skipped_candidates: {r.get('skipped_candidates')}",
            f"- created ids count: {len(r.get('created_signal_ids') or [])}",
        ]
        if r.get("error"):
            lines.append(f"- ошибка: {r['error']}")
        lines.append(svc.next_step_hint("line_ingest"))
        await message.answer("\n".join(lines))
    except Exception as exc:
        await message.answer(f"⚠️ Ingest line не выполнен.\nКратко: {exc!s}")


@router.message(Command("winline_manual_result_preview"))
@router.message(_text_is("Winline превью результата", "Winline result превью", "Winline manual result"))
async def cmd_winline_manual_result_preview(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    try:
        from app.services.winline_manual_cycle_service import WinlineManualCycleService

        lines = _format_manual_result_preview_lines()
        lines.append(WinlineManualCycleService().next_step_hint("result_preview"))
        await message.answer("\n".join(lines))
    except Exception as exc:
        await message.answer(f"⚠️ Превью result не получилось.\nКратко: {exc!s}")


@router.message(Command("winline_manual_result_process"))
@router.message(_text_is("Winline обработать результат", "Winline process result", "Winline manual process"))
async def cmd_winline_manual_result_process(
    message: Message, sessionmaker: async_sessionmaker[AsyncSession]
) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    try:
        from app.services.winline_manual_cycle_service import WinlineManualCycleService

        svc = WinlineManualCycleService()
        r = await svc.process_manual_result(sessionmaker)
        bal = r.get("current_balance_rub")
        bal_s = f"{bal:.2f} ₽" if isinstance(bal, Decimal) else (str(bal) if bal is not None else "—")
        lines = [
            "⚙️ Winline manual — result process",
            f"- status: {r.get('status')}",
            f"- raw_results rows: {r.get('raw_results')}",
            f"- settled signals count: {len(r.get('settled_signal_ids') or [])}",
            f"- wins / losses / voids: {r.get('wins')} / {r.get('losses')} / {r.get('voids')}",
            f"- balance: {bal_s}",
            f"- sanity issues: {r.get('sanity_issues_count')}",
        ]
        if r.get("error"):
            lines.append(f"- ошибка: {r['error']}")
        lines.append(svc.next_step_hint("result_process"))
        await message.answer("\n".join(lines))
    except Exception as exc:
        await message.answer(f"⚠️ Обработка result не выполнена.\nКратко: {exc!s}")


@router.message(Command("winline_manual_full_cycle"))
@router.message(_text_is("Winline полный цикл", "Winline manual full"))
async def cmd_winline_manual_full_cycle(
    message: Message, sessionmaker: async_sessionmaker[AsyncSession]
) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    try:
        from app.services.winline_manual_cycle_service import WinlineManualCycleService

        data = await WinlineManualCycleService().run_manual_full_cycle(sessionmaker, message.bot, send_signals=True)
        s = data.get("summary") or {}
        rp = data.get("result_processing") or {}
        bal = rp.get("current_balance_rub")
        bal_s = f"{bal:.2f} ₽" if isinstance(bal, Decimal) else (str(bal) if bal is not None else "—")
        send_r = data.get("send_result") or {}
        lines = [
            "🧪 Winline manual — full cycle",
            f"- line ingest created: {s.get('line_ingest_created')}",
            f"- final signals ready: {s.get('final_ready')}",
            f"- messages sent: {s.get('messages_sent')}",
            f"- result rows: {s.get('result_rows')}",
            f"- settled count: {s.get('settled_ids_count')}",
            f"- balance: {bal_s}",
            f"- sanity issues: {s.get('sanity_issues')}",
            f"- send status: {send_r.get('status')}",
        ]
        errs = data.get("errors") or []
        if errs:
            lines.append(f"- замечания: {'; '.join(errs)[:500]}")
        lines.append(WinlineManualCycleService().next_step_hint("full_cycle"))
        await message.answer("\n".join(lines))
    except Exception as exc:
        await message.answer(f"⚠️ Full cycle не завершён.\nКратко: {exc!s}")


@router.message(Command("winline_manual_run_ready"))
@router.message(_text_is("Winline умный запуск", "Winline run ready"))
async def cmd_winline_manual_run_ready(
    message: Message, sessionmaker: async_sessionmaker[AsyncSession]
) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    try:
        from app.services.winline_manual_cycle_service import WinlineManualCycleService

        svc = WinlineManualCycleService()
        data = await svc.run_ready_cycle(sessionmaker, message.bot)
        mode = str(data.get("mode") or "nothing")
        hint = svc.next_step_hint("run_ready")

        if mode == "nothing":
            await message.answer(data.get("message") or "Файлы не загружены. Сначала загрузите line и/или result JSON.")
            return

        if mode == "line_only":
            lp = data.get("line_preview") or {}
            li = data.get("line_ingest") or {}
            lines = [
                "🚀 Winline run ready — line_only",
                f"- preview err: {lp.get('error') or '—'}",
                f"- final signals (synth): {lp.get('final_signals_ready') if lp.get('final_signals_ready') is not None else '—'}",
                f"- ingest status: {li.get('status')}",
                f"- created_signals: {li.get('created_signals')}",
                f"- skipped: {li.get('skipped_candidates')}",
            ]
            if data.get("errors"):
                lines.append(f"- замечания: {'; '.join(data['errors'])[:400]}")
            lines.append(hint)
            await message.answer("\n".join(lines))
            return

        if mode == "result_only":
            rp = data.get("result_preview") or {}
            pr = data.get("result_processing") or {}
            lines = [
                "🚀 Winline run ready — result_only",
                f"- result rows: {pr.get('raw_results')}",
                f"- settled count: {len(pr.get('settled_signal_ids') or [])}",
                f"- balance: {pr.get('current_balance_rub')}",
                f"- process status: {pr.get('status')}",
            ]
            if pr.get("error"):
                lines.append(f"- ошибка: {pr['error']}")
            if data.get("errors"):
                lines.append(f"- замечания: {'; '.join(data['errors'])[:400]}")
            lines.append(hint)
            await message.answer("\n".join(lines))
            return

        # full
        full = data.get("full_cycle") or {}
        s = full.get("summary") or {}
        rp = full.get("result_processing") or {}
        bal = rp.get("current_balance_rub")
        bal_s = f"{bal:.2f} ₽" if isinstance(bal, Decimal) else (str(bal) if bal is not None else "—")
        send_r = full.get("send_result") or {}
        lines = [
            "🚀 Winline run ready — full cycle",
            f"- line ingest created: {s.get('line_ingest_created')}",
            f"- final ready: {s.get('final_ready')}",
            f"- messages sent: {s.get('messages_sent')}",
            f"- result rows: {s.get('result_rows')}",
            f"- settled: {s.get('settled_ids_count')}",
            f"- balance: {bal_s}",
            f"- sanity: {s.get('sanity_issues')}",
            f"- send: {send_r.get('status')}",
        ]
        fe = full.get("errors") or []
        if fe:
            lines.append(f"- замечания: {'; '.join(fe)[:400]}")
        lines.append(hint)
        await message.answer("\n".join(lines))
    except Exception as exc:
        await message.answer(f"⚠️ Run ready не выполнен.\nКратко: {exc!s}")


@router.message(Command("winline_demo_status"))
@router.message(_text_is("Winline статус"))
async def cmd_winline_demo_status(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    try:
        from app.services.winline_final_signal_service import WinlineFinalSignalService
        from app.services.winline_live_signal_service import WinlineLiveSignalService
        from app.services.winline_signal_delivery_demo_service import WinlineSignalDeliveryDemoService

        settings = get_settings()
        demo_cases = len(WinlineLiveSignalService().build_live_demo_inputs())
        previews = WinlineFinalSignalService().build_all_previews()
        final_ready = sum(1 for p in previews if p.has_signal)
        sendable = len(WinlineSignalDeliveryDemoService()._get_sendable_previews())
        signal_ok = settings.signal_chat_id is not None
        token_ok = bool(settings.bot_token and str(settings.bot_token).strip())
        lines = [
            "🤖 Winline demo status",
            f"- demo inputs cases: {demo_cases}",
            f"- final signals ready: {final_ready}",
            f"- sendable messages: {sendable}",
            f"- signal chat configured: {_fmt_yes_no(signal_ok)}",
            f"- bot token configured: {_fmt_yes_no(token_ok)}",
            "- settlement demo available: Да",
            "- manual delivery available: Да",
        ]
        await message.answer("\n".join(lines))
    except Exception as exc:
        await message.answer(f"⚠️ Не удалось собрать статус.\nКратко: {exc!s}")


@router.message(Command("winline_demo_preview"))
@router.message(_text_is("Winline превью"))
async def cmd_winline_demo_preview(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    try:
        from app.services.winline_final_signal_service import WinlineFinalSignalService
        from app.services.winline_telegram_formatter_service import WinlineTelegramFormatterService

        fmt = WinlineTelegramFormatterService()
        previews = WinlineFinalSignalService().build_all_previews()
        sendable = [p for p in previews if p.has_signal and p.signal is not None]
        n = len(sendable)
        lines: list[str] = [f"🔎 Найдено сигналов: {n}", ""]
        if n == 0:
            lines.append("Сигналы не собраны. Все кейсы отфильтрованы.")
            await _answer_long_message(message, "\n".join(lines))
            return
        for p in sendable:
            s = p.signal
            if s is None:
                continue
            lines.append(f"— {p.case_name}")
            lines.append(fmt.format_compact_signal_text(s))
            rc = ", ".join((s.live_reason_codes or [])[:3])
            if len(s.live_reason_codes or []) > 3:
                rc += "…"
            stake_u = s.recommended_stake_units
            su = f"{float(stake_u):.2f}" if stake_u is not None else "n/a"
            ev = s.expected_value
            evs = f"{float(ev):.4f}" if ev is not None else "n/a"
            lines.append(f"  live: {rc or '—'} | stake: {su}u | EV: {evs}")
            lines.append("")
        await _answer_long_message(message, "\n".join(lines).rstrip())
    except Exception as exc:
        await message.answer(f"⚠️ Превью недоступно.\nКратко: {exc!s}")


@router.message(Command("winline_demo_send"))
@router.message(_text_is("Winline отправка"))
async def cmd_winline_demo_send(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    try:
        from app.services.winline_signal_delivery_demo_service import WinlineSignalDeliveryDemoService

        res = await WinlineSignalDeliveryDemoService().send_demo_messages(message.bot)
        st = res.get("status")
        if st == "skipped_no_signal_chat":
            await message.answer(
                "\n".join(
                    [
                        "⚠️ Отправка пропущена",
                        "- причина: SIGNAL_CHAT_ID не настроен",
                    ]
                )
            )
            return
        if st == "no_sendable_messages":
            await message.answer(
                "\n".join(
                    [
                        "⚠️ Отправка пропущена",
                        "- причина: нет sendable-сообщений (все кейсы отфильтрованы)",
                    ]
                )
            )
            return
        chat_id = res.get("chat_id")
        sent = int(res.get("sent") or 0)
        await message.answer(
            "\n".join(
                [
                    "✅ Demo-сигналы отправлены",
                    f"- отправлено: {sent}",
                    f"- chat_id: {chat_id}",
                ]
            )
        )
    except Exception as exc:
        await message.answer(f"⚠️ Отправка не удалась.\nКратко: {exc!s}")


@router.message(Command("winline_demo_settlement"))
@router.message(_text_is("Winline расчёт", "Winline settlement"))
async def cmd_winline_demo_settlement(
    message: Message, sessionmaker: async_sessionmaker[AsyncSession]
) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    try:
        from app.services.winline_settlement_demo_service import WinlineSettlementDemoService

        data = await WinlineSettlementDemoService().run_demo_and_collect(sessionmaker)
        err = data.get("error")
        bal = data.get("current_balance_rub")
        bal_s = f"{bal:.2f} ₽" if isinstance(bal, Decimal) else (str(bal) if bal is not None else "n/a")
        lines = [
            "📦 Winline settlement demo",
            f"- preview candidates: {data.get('preview_candidates')}",
            f"- created signals: {data.get('created_signals')}",
            f"- settled signals: {len(data.get('settled_signal_ids') or [])}",
            f"- wins / losses / voids: {data.get('wins')} / {data.get('losses')} / {data.get('voids')}",
            f"- balance: {bal_s}",
            f"- sanity issues: {data.get('sanity_issues_count')}",
            f"- intersection event ids: {', '.join(data.get('intersection_event_ids') or []) or '—'}",
        ]
        if err:
            lines.insert(1, f"- ошибка: {err}")
        if data.get("ok") is False and not err:
            lines.insert(1, "- статус: частичный сбой (см. лог)")
        await message.answer("\n".join(lines))
    except Exception as exc:
        await message.answer(f"⚠️ Settlement demo не выполнен.\nКратко: {exc!s}")


@router.message(Command("winline_demo_full_cycle"))
@router.message(_text_is("Winline полный демо-цикл", "Winline полный demo цикл", "Winline full cycle"))
async def cmd_winline_demo_full_cycle(
    message: Message, sessionmaker: async_sessionmaker[AsyncSession]
) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    from app.services.winline_final_signal_service import WinlineFinalSignalService
    from app.services.winline_settlement_demo_service import WinlineSettlementDemoService
    from app.services.winline_signal_delivery_demo_service import WinlineSignalDeliveryDemoService

    steps: list[str] = []
    final_ready = 0
    sent_n = 0
    data: dict | None = None

    try:
        previews = WinlineFinalSignalService().build_all_previews()
        final_ready = sum(1 for p in previews if p.has_signal)
    except Exception as exc:
        steps.append(f"final: {exc!s}")

    try:
        send_res = await WinlineSignalDeliveryDemoService().send_demo_messages(message.bot)
        if send_res.get("status") == "ok":
            sent_n = int(send_res.get("sent") or 0)
        else:
            steps.append(f"send: {send_res.get('status')} — {send_res.get('message') or ''}".strip())
    except Exception as exc:
        steps.append(f"send: {exc!s}")

    try:
        data = await WinlineSettlementDemoService().run_demo_and_collect(sessionmaker)
    except Exception as exc:
        steps.append(f"settlement: {exc!s}")

    err_all = None
    if data:
        err_all = data.get("error")
    if steps:
        err_all = "; ".join(steps + ([err_all] if err_all else []))

    bal = (data or {}).get("current_balance_rub")
    bal_s = f"{bal:.2f} ₽" if isinstance(bal, Decimal) else (str(bal) if bal is not None else "n/a")

    lines = [
        "🚀 Winline full cycle completed",
        f"- final signals ready: {final_ready}",
        f"- messages sent: {sent_n}",
        f"- created signals: {(data or {}).get('created_signals')}",
        f"- settled signals: {len((data or {}).get('settled_signal_ids') or [])}",
        f"- wins / losses / voids: {(data or {}).get('wins')} / {(data or {}).get('losses')} / {(data or {}).get('voids')}",
        f"- balance: {bal_s}",
        f"- sanity issues: {(data or {}).get('sanity_issues_count')}",
    ]
    if err_all:
        lines.append(f"- этапы / ошибки: {err_all}")
    await message.answer("\n".join(lines))


@router.message()
async def fallback_unrecognized(message: Message) -> None:
    allowed = _is_allowed(message)
    if not allowed:
        await _deny(message)
        return

    user_id = message.from_user.id if message.from_user else None
    chat_id = message.chat.id if getattr(message, "chat", None) is not None else None
    chat_type = message.chat.type if getattr(message, "chat", None) is not None else None
    entities = [
        {
            "type": entity.type,
            "offset": entity.offset,
            "length": entity.length,
        }
        for entity in (message.entities or [])
    ]
    logger.info(
        "Unhandled message: chat_id=%s user_id=%s chat_type=%s text=%r entities=%s",
        chat_id,
        user_id,
        chat_type,
        message.text,
        entities,
    )
    await message.answer(
        "Не понял команду или сообщение.\nОткройте меню кнопкой /start\nИли посмотрите список команд через /debug_help",
        reply_markup=get_signal_control_keyboard(),
    )
