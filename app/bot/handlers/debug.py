from __future__ import annotations

import asyncio
import json
import logging
import time
from io import BytesIO
from datetime import datetime, timezone
from decimal import Decimal

from aiogram import Bot, F, Router
from aiogram.filters import BaseFilter, Command
from aiogram.types import Message
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.bot.keyboards.debug import get_debug_keyboard, get_signal_control_keyboard, get_winline_manual_flow_keyboard
from sqlalchemy import select

from app.core.enums import BetResult, EntryStatus, SportType
from app.db.models.signal import Signal
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
from app.services.auto_signal_service import (
    AutoSignalService,
    format_final_live_gate_summary_lines,
    format_football_session_start_user_message,
)
from app.services.football_signal_outcome_reason_service import (
    FootballSignalOutcomeReasonService,
    build_football_postmatch_verify_report,
)
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
from app.services.signal_runtime_diagnostics_service import SignalRuntimeDiagnosticsService
from app.services.signal_runtime_settings_service import SignalRuntimeSettingsService
from app.schemas.provider_client import ProviderClientConfig
from app.services.remote_smoke_service import RemoteSmokeService
from app.services.football_live_strategy_performance_service import FootballLiveStrategyPerformanceService


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


def _fmt_football_live_source_label_ru(diag: dict) -> str:
    """Понятная подпись вместо технических fresh/unknown в диагностике."""
    if bool(diag.get("football_live_stale_source")):
        return "Источник устарел"
    raw = (diag.get("football_live_source_freshness") or "").strip().lower()
    if raw == "fresh":
        return "Источник свежий"
    if raw == "unknown":
        return "Нет данных о свежести источника"
    if not raw or raw == "—":
        return "—"
    return str(diag.get("football_live_source_freshness") or "—")


def _fmt_source_age_for_ui(seconds: object) -> str:
    if seconds is None:
        return "—"
    try:
        s = float(seconds)
    except (TypeError, ValueError):
        return "—"
    if abs(s) < 180:
        return f"{s:.0f} с"
    return f"{s / 60.0:.1f} мин"


def _fmt_pacing_seconds(value: object) -> str:
    if value is None:
        return "—"
    try:
        s = float(value)
    except (TypeError, ValueError):
        return "—"
    return f"{s:.2f} с"


def _sport_toggle_label(key: str, enabled: bool) -> str:
    if key == "football":
        return f"⚽ Футбол: {'включён' if enabled else 'выключен'}"
    if key == "cs2":
        return f"🎮 CS2: {'включён' if enabled else 'выключен'}"
    return f"🎮 Dota: {'включена' if enabled else 'выключена'}"


def _format_external_api_status_lines(diag: dict[str, object] | None = None) -> list[str]:
    d = diag or SignalRuntimeDiagnosticsService().get_state()

    def _line(label: str, prefix: str) -> list[str]:
        st = str(d.get(f"external_api_{prefix}_status") or "unknown")
        err = str(d.get(f"external_api_{prefix}_last_error") or "—")
        ok = str(d.get(f"external_api_{prefix}_last_success") or "—")
        return [
            f"• {label}: {st}",
            f"  last_success: {ok}",
            f"  last_error: {err[:220]}",
        ]

    return [
        "",
        "— External API status —",
        *_line("OpenAI", "openai"),
        *_line("API-Football", "api_football"),
        *_line("Sportmonks", "sportmonks"),
    ]


def _format_signal_runtime_status_lines() -> list[str]:
    from app.services.football_live_session_service import FootballLiveSessionService

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
    rem_m = diag.get("football_live_session_remaining_minutes")
    _pers = bool(diag.get("football_live_session_persistent"))
    if _pers and bool(diag.get("football_live_session_active")):
        rem_txt = "до ⏸ Стоп (бессрочно)"
    else:
        rem_txt = f"{round(float(rem_m), 1)}" if rem_m is not None else "—"
    last_cy = diag.get("football_live_session_last_cycle_at") or "—"
    live_m = diag.get("football_live_cycle_live_matches_found")
    if live_m is None:
        live_m = diag.get("live_matches_count") or 0
    new_ideas = diag.get("football_live_cycle_new_ideas_sendable")
    if new_ideas is None:
        new_ideas = 0
    started_s = diag.get("football_live_session_started_at") or "—"
    if _pers and bool(diag.get("football_live_session_active")):
        expires_s = "— (бессрочно)"
    else:
        expires_s = diag.get("football_live_session_expires_at") or "—"
    bn = diag.get("football_live_cycle_bottleneck") or "—"
    bn_ru = diag.get("football_live_cycle_bottleneck_ru") or "—"
    src_age_txt = _fmt_source_age_for_ui(diag.get("football_live_source_age_seconds"))
    ff_ru = _fmt_football_live_source_label_ru(diag)
    # Strict last-cycle summary (single source of truth, no mixing).
    last_cycle_lines: list[str] = [
        "— Последний live-cycle (строго) —",
        f"• Последний цикл: {last_cy}",
        f"• Live matches: {live_m}",
        f"• After scoring pool: {int(diag.get('football_live_cycle_after_score') or 0)}",
        f"• Strategy matches: {int(diag.get('football_live_strategy_matches_last_cycle') or 0)} "
        f"(S1={int(diag.get('football_live_strategy_s1_matches_last_cycle') or 0)}, "
        f"S2={int(diag.get('football_live_strategy_s2_matches_last_cycle') or 0)})",
        f"• After final gate: {int(diag.get('football_live_cycle_new_ideas_sendable') or 0)}",
        f"• Created signals: {int(diag.get('football_last_combat_created_signals') or 0)}",
        f"• Telegram sent: {int(diag.get('football_last_combat_messages_sent') or 0)}",
        f"• Bottleneck: {bn_ru} ({bn})",
    ]
    csum = diag.get("football_live_combat_delivery_last_summary")
    e2e_lines: list[str] = [f"• E2E (ingest → TG): {csum}"] if csum else []
    _san_n = int(diag.get("football_live_sanity_blocked_last_cycle") or 0)
    _san_blk = diag.get("football_live_sanity_last_blocker") or "—"
    _san_br = (diag.get("football_live_sanity_last_best_rejected") or "").strip()
    sanity_status_lines: list[str] = [
        "— Pre-send live sanity —",
        f"• Заблокировано по sanity (последний цикл): {_san_n}",
        f"• Последний block_token: {_san_blk}",
    ]
    if _san_br:
        sanity_status_lines.append(f"• Сильный отсев: {_san_br[:400]}")
    n_pm = int(diag.get("football_postmatch_settled_count") or 0)
    tlr_loss = (diag.get("football_postmatch_top_loss_reasons") or "").strip()
    if n_pm or tlr_loss:
        postmatch_block: list[str] = [
            "",
            "— Football post-match (сеттл / объяснения) —",
            f"• Последняя выборка settled: n={n_pm} "
            f"(W {int(diag.get('football_postmatch_wins_last') or 0)} / "
            f"L {int(diag.get('football_postmatch_losses_last') or 0)} / "
            f"V {int(diag.get('football_postmatch_voids_last') or 0)})",
        ]
        if tlr_loss:
            postmatch_block.append(f"• Топ причин минуса: {tlr_loss[:600]}")
        _raj = diag.get("football_postmatch_rationale_aggregate_json")
        if isinstance(_raj, str) and _raj.strip():
            try:
                _rb = json.loads(_raj)
                postmatch_block.append(
                    "• Live rationale×outcome (последний refresh): "
                    f"wins_with_rationale={_rb.get('wins_with_rationale')} "
                    f"losses_with_rationale={_rb.get('losses_with_rationale')} "
                    f"late_warn_on_losses={_rb.get('losses_late_stage_warning_hits')} "
                    f"limited_ctx_on_losses={_rb.get('losses_limited_live_context_hits')}"
                )
            except (json.JSONDecodeError, TypeError):
                postmatch_block.append("• Live rationale×outcome: (не удалось разобрать JSON)")
    else:
        postmatch_block = []
    adaptive_block: list[str] = []
    _alj = diag.get("football_live_adaptive_learning_json")
    if isinstance(_alj, str) and _alj.strip():
        try:
            alb = json.loads(_alj)
            meta = alb.get("meta") or {}
            adaptive_block = [
                "",
                "— Football LIVE adaptive (settled → score delta) —",
                f"• lookback≤{meta.get('lookback_limit', '—')}  rationale_rows={meta.get('rows_with_rationale', '—')}",
                f"• активных penalty: {len(alb.get('penalties_active') or [])}  boost: {len(alb.get('boosts_active') or [])}",
            ]
            pen = alb.get("penalties_active") or []
            if pen:
                s = "; ".join(
                    f"{p.get('key')} ({p.get('delta')})" for p in pen[:6] if isinstance(p, dict)
                )
                adaptive_block.append(f"• примеры penalty: {s[:480]}")
            bst = alb.get("boosts_active") or []
            if bst:
                s = "; ".join(
                    f"{p.get('key')} ({p.get('delta')})" for p in bst[:6] if isinstance(p, dict)
                )
                adaptive_block.append(f"• примеры boost: {s[:480]}")
        except (json.JSONDecodeError, TypeError):
            adaptive_block = ["", "— Football LIVE adaptive —", "• (не удалось разобрать JSON)"]
    training_block: list[str] = []
    n_combat = int(diag.get("football_live_combat_signals_total") or 0)
    if n_combat or int(diag.get("adaptive_training_ready_signals_count") or 0):
        _tw = (diag.get("football_live_adaptive_training_warning_ru") or "").strip()
        training_block = [
            "",
            "— Football LIVE → adaptive (обучающая выборка) —",
            f"• combat `live_auto` всего (exact): {n_combat}",
            f"• с rationale (в последнем scan): {int(diag.get('football_live_with_any_rationale_count') or 0)}",
            f"• rationale полный (коды/path/context): {int(diag.get('football_live_with_training_ready_rationale_count') or 0)}",
            f"• с исходом WIN/LOSE: {int(diag.get('football_live_with_settlement_winlose_count') or 0)}",
            f"• с outcome_reason_code в audit: {int(diag.get('football_live_with_outcome_reason_code_count') or 0)}",
            f"• adaptive_training_ready: {int(diag.get('adaptive_training_ready_signals_count') or 0)}",
        ]
        if _tw:
            training_block.append(f"• ⚠ {_tw[:500]}")
    return [
        "📊 Статус сигналов",
        "",
        "— Football live-источник (runtime) —",
        f"• Приоритет: Winline WebSocket (primary), The Odds API — только при fallback (см. .env)",
        f"• Текущий primary: {diag.get('football_primary_live_source') or '—'}",
        f"• Winline live в последнем цикле: {_fmt_yes_no(bool(diag.get('football_winline_ws_active_last_cycle')))}",
        f"• Live-матчей (Winline, events): {diag.get('football_winline_football_event_count') or 0}",
        f"• Сырых линий (Winline, lines): {diag.get('football_winline_line_count_raw') or 0}",
        f"• Кандидатов футбол (Winline→bridge): {diag.get('football_winline_football_candidate_count') or 0}",
        f"• Ошибка Winline (токен): {diag.get('football_winline_error_last') or '—'}",
        "",
        "⚽ Football live-сессия",
        f"• Активна: {_fmt_yes_no(bool(diag.get('football_live_session_active')))}",
        f"• Старт: {started_s}",
        f"• Истекает: {expires_s}",
        f"• Осталось: {rem_txt}{' мин' if rem_txt != 'до ⏸ Стоп (бессрочно)' else ''}",
        "",
        "— Football LIVE runtime cadence —",
        f"• Сессия: {'активна' if diag.get('football_live_session_active') else 'нет'}",
        f"• Интервал цикла: {_fmt_pacing_seconds(diag.get('football_live_pacing_current_interval_seconds'))}",
        f"• Последний fetch (измерено): {_fmt_pacing_seconds(diag.get('football_live_pacing_last_fetch_seconds'))}",
        f"• Средний fetch (скользящее): {_fmt_pacing_seconds(diag.get('football_live_pacing_avg_fetch_seconds'))}",
        f"• Backoff: уровень {diag.get('football_live_pacing_backoff_level') if diag.get('football_live_pacing_backoff_level') is not None else '—'}",
        f"• Причина текущего интервала: {(diag.get('football_live_pacing_last_reason_ru') or '—')[:900]}",
        f"• Ошибки подряд (pacing): {int(diag.get('football_live_pacing_consecutive_errors') or 0)}",
        f"• Пустые снимки подряд: {int(diag.get('football_live_pacing_consecutive_empty_snapshots') or 0)}",
        f"• Wall последнего цикла: {_fmt_pacing_seconds(diag.get('football_live_last_cycle_wall_seconds'))}",
        f"• Live-матчей (последний цикл): {live_m}",
        "— Проверка свежести (live-only) —",
        f"• {ff_ru}",
        f"• Возраст источника: {src_age_txt}",
        f"• Кандидатов до проверки свежести: {diag.get('football_live_freshness_candidates_before') or 0}",
        f"• Live-матчей принято: {diag.get('football_live_freshness_live_events_accepted') or 0}",
        f"• Устаревших live-матчей отсеяно: {diag.get('football_live_freshness_stale_events_dropped') or 0}",
        f"• Рынков на устаревших матчах отсеяно: {diag.get('football_live_freshness_stale_markets_dropped') or 0}",
        "— Качество live-идей —",
        f"• Свежих live-матчей: {diag.get('football_live_quality_fresh_matches') or 0}",
        f"• С сильной идеей (лучший score ≥ порога): {diag.get('football_live_quality_strong_idea_matches') or 0}",
        f"• Без sendable-идеи в цикле: {diag.get('football_live_quality_no_sendable_matches') or 0}",
        f"• Главный блокер: {diag.get('football_live_quality_main_blocker_ru') or diag.get('football_live_quality_main_blocker') or '—'}",
        f"• Подсказка качества: {diag.get('football_live_quality_hint_ru') or '—'}",
        f"• Порог score (база): {diag.get('football_live_min_signal_score_base') or '—'}",
        f"• Отклонено в send-gate (reject): {diag.get('football_live_rejected_at_send_gate') or 0}",
        "• Safety-block S8 1X2 0:0 без API context: "
        f"{diag.get('football_live_rejected_s8_1x2_00_without_api_context') or 0}",
        "• Safety-block S8 1X2 0:0 без pressure_score>=2: "
        f"{diag.get('football_live_rejected_s8_1x2_00_no_pressure') or 0}",
        "• Safety-pass S8 1X2 0:0 с API pressure: "
        f"{diag.get('football_live_passed_s8_1x2_00_with_api_pressure') or 0}",
        f"• К пулу: обычных (≥ порога): {diag.get('football_live_normal_sendable_count') or 0}",
        f"• К пулу: мягких (soft) live: {diag.get('football_live_soft_sendable_count') or 0}",
        f"• К пулу: soft gap ≤1.5: {diag.get('football_live_soft_sendable_tight_count') or 0}",
        f"• К пулу: soft relief (main, gap до 2): {diag.get('football_live_soft_sendable_relief_single_count') or 0}",
        f"• Схема порога: {diag.get('football_live_score_relief_note') or '—'}",
        f"• Лучшие score: {diag.get('football_live_best_scores_distribution_hint') or '—'}",
        *last_cycle_lines,
        *e2e_lines,
        *sanity_status_lines,
        "— Последняя запись (не dry-run) —",
        f"• Обычных сигналов записано в БД: {diag.get('football_last_cycle_ingest_normal') or 0}",
        f"• Мягко допущенных (soft) live в БД: {diag.get('football_last_cycle_ingest_soft') or 0}",
        f"• Основной режим пачки: {diag.get('football_last_cycle_send_mode') or '—'} (normal / soft / mixed / none)",
        f"• DB dedup отсёк кандидатов (послед. ingest): {diag.get('football_last_cycle_db_dedup_skipped') or 0}",
        f"• Post-selection: {diag.get('football_live_post_selection_hint_ru') or '—'}",
        f"• Новых идей к отправке (последний цикл): {new_ideas}",
        f"• Повторов идей отсеяно (сессия): {diag.get('football_live_duplicate_ideas_blocked') or 0}",
        f"• Отправлено в Telegram (сессия): {diag.get('football_live_telegram_sent_session') or 0}",
        f"• Записано сигналов в БД (сессия): {diag.get('football_live_signals_sent_session') or 0}",
        f"• Последний цикл: {last_cy}",
        f"• Узкое место цикла: {bn}",
        "",
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
        # selected_match_reason is a send-filter ranking trace (priority_score includes +10000 for live),
        # and must not be shown as "chosen match / signal score" to avoid confusion.
        f"⚽ football_sent: {diag.get('football_sent_count') or 0}",
        f"🚨 Финальных сигналов: {diag.get('final_signals_count') or 0}",
        f"📨 Отправлено (цикл→канал): {diag.get('messages_sent_count') or 0}",
        f"🛑 Причина без отправки: {bn_ru} ({bn})",
        *_format_external_api_status_lines(diag),
        "— Участие внешнего контекста (runtime, накопительно) —",
        f"• signals_with_api_football: {int(diag.get('signals_with_api_football') or 0)}",
        f"• signals_without_api_football: {int(diag.get('signals_without_api_football') or 0)}",
        f"• signals_with_sportmonks: {int(diag.get('signals_with_sportmonks') or 0)}",
        f"• signals_with_external_context: {int(diag.get('signals_with_external_context') or 0)}",
        *postmatch_block,
        *training_block,
        *adaptive_block,
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


async def _answer_long_message(
    message: Message, text: str, *, reply_markup: object | None = None
) -> None:
    parts = _chunk_answer_text(text)
    for i, part in enumerate(parts):
        await message.answer(
            part,
            reply_markup=reply_markup if i == 0 and reply_markup is not None else None,
        )


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


@router.message(Command("openai_test"))
async def cmd_openai_test(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    from app.core.config import get_settings
    from app.services.openai_service import OpenAIService

    s = get_settings()
    if not getattr(s, "openai_enabled", False):
        await message.answer("⚠️ OpenAI отключён (OPENAI_API_KEY пустой)")
        return
    res = await OpenAIService().test_simple_request(s)
    if res.success:
        txt = (res.text_response or "OK").strip()
        await message.answer(f"✅ OpenAI работает\nОтвет: {txt}")
        return
    err = (res.error_text or "unknown_error").strip()
    await message.answer(f"⚠️ OpenAI ошибка\n{err}")


@router.message(Command("openai_live_learning_stats"))
async def cmd_openai_live_learning_stats(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    from sqlalchemy import select
    from sqlalchemy.orm import selectinload

    from app.core.enums import SportType
    from app.db.models.signal import Signal
    from app.db.models.settlement import Settlement
    from app.services.football_live_adaptive_learning_service import build_live_adaptive_snapshot

    async with sessionmaker() as session:
        # Last settled live football signals, best-effort scan
        q = (
            select(Signal, Settlement)
            .join(Settlement, Settlement.signal_id == Signal.id)
            .where(Signal.sport == SportType.FOOTBALL)
            .where(Signal.is_live.is_(True))
            .options(selectinload(Signal.prediction_logs))
            .order_by(Settlement.id.desc())
            .limit(500)
        )
        rows = list((await session.execute(q)).all())

        verdict_cnt = {"good_signal": 0, "bad_signal": 0, "neutral_signal": 0, "missing": 0}
        analyzed = 0
        total_settled = 0
        tag_stats: dict[str, dict[str, float]] = {}

        for sig, st in rows:
            total_settled += 1
            pl0 = min(sig.prediction_logs, key=lambda p: p.id) if sig.prediction_logs else None
            fs0 = dict(pl0.feature_snapshot_json or {}) if pl0 else {}
            oa = fs0.get("openai_analysis") if isinstance(fs0.get("openai_analysis"), dict) else None
            if not oa:
                verdict_cnt["missing"] += 1
                continue
            analyzed += 1
            v = str(oa.get("verdict") or "").strip()
            if v in verdict_cnt:
                verdict_cnt[v] += 1
            else:
                verdict_cnt["neutral_signal"] += 1

            try:
                conf = float(oa.get("confidence") or 0.0)
            except Exception:
                conf = 0.0
            if conf < 0.75:
                continue
            tags = oa.get("pattern_tags") if isinstance(oa.get("pattern_tags"), list) else []
            if not tags:
                continue
            try:
                pl = float(st.profit_loss) if st.profit_loss is not None else 0.0
            except Exception:
                pl = 0.0
            for t in tags[:30]:
                if not t:
                    continue
                k = str(t)[:60]
                s = tag_stats.get(k) or {"n": 0.0, "profit_sum": 0.0}
                s["n"] += 1.0
                s["profit_sum"] += float(pl)
                tag_stats[k] = s

        # rank tags by avg profit, require n>=10 per spec
        ranked = []
        for k, v in tag_stats.items():
            n = int(v.get("n") or 0)
            if n < 10:
                continue
            ps = float(v.get("profit_sum") or 0.0)
            ranked.append((k, n, ps / n))
        ranked_best = sorted(ranked, key=lambda x: x[2], reverse=True)[:5]
        ranked_worst = sorted(ranked, key=lambda x: x[2])[:5]

        snap = await build_live_adaptive_snapshot(session, lookback=400)
        pub = snap.to_public_dict()
        # only OpenAI keys are relevant here
        penalties = [r for r in (pub.get("penalties_active") or []) if str(r.get("key") or "").startswith("oa_")][:10]
        boosts = [r for r in (pub.get("boosts_active") or []) if str(r.get("key") or "").startswith("oa_")][:10]

    lines = [
        "🧠 OpenAI LIVE learning stats",
        f"- settled_live_scanned: {total_settled}",
        f"- openai_analyzed: {analyzed}",
        f"- verdicts: good={verdict_cnt['good_signal']} bad={verdict_cnt['bad_signal']} neutral={verdict_cnt['neutral_signal']} missing={verdict_cnt['missing']}",
        "",
        "— top 5 worst tags (n>=10) —",
        *([f"  {k}  n={n}  profit_avg={avg:.3f}" for k, n, avg in ranked_worst] or ["  (нет, выборка мала)"]),
        "",
        "— top 5 best tags (n>=10) —",
        *([f"  {k}  n={n}  profit_avg={avg:.3f}" for k, n, avg in ranked_best] or ["  (нет, выборка мала)"]),
        "",
        f"active_openai_penalties: {len(penalties)}",
        *(
            [
                f"  {r.get('key')}: delta={r.get('delta')} n={r.get('n')} W/L={r.get('wins')}/{r.get('losses')}"
                for r in penalties
            ]
            or []
        ),
        f"active_openai_boosts: {len(boosts)}",
        *(
            [
                f"  {r.get('key')}: delta={r.get('delta')} n={r.get('n')} W/L={r.get('wins')}/{r.get('losses')}"
                for r in boosts
            ]
            or []
        ),
    ]
    await _answer_long_message(message, "\n".join(lines))


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


@router.message(_text_is("📊 Статус сигналов", "Статус сигналов"))
@router.message(Command("signal_status"))
async def cmd_signal_status(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    logger.info(
        "Signal status requested: chat_id=%s user_id=%s text=%r",
        message.chat.id if getattr(message, "chat", None) is not None else None,
        message.from_user.id if message.from_user else None,
        message.text,
    )
    await _answer_long_message(
        message,
        "\n".join(_format_signal_runtime_status_lines()),
        reply_markup=get_signal_control_keyboard(),
    )


@router.message(_text_is("⏸ Стоп"))
@router.message(Command("signal_pause"))
async def cmd_signal_pause(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    from app.services.football_live_session_service import FootballLiveSessionService

    FootballLiveSessionService().stop_session(manual=True)
    SignalRuntimeSettingsService().pause()
    await message.answer(
        "⏸ Футбольная live-сессия остановлена.\n"
        "Live-only режим остаётся политикой контура, но новые live-циклы не запускаются, пока снова не нажмёте ▶️ Старт.",
        reply_markup=get_signal_control_keyboard(),
    )


@router.message(_text_is("▶️ Старт"))
@router.message(Command("signal_start"))
async def cmd_signal_start(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    from app.services.football_live_session_service import FootballLiveSessionService

    from app.services.football_live_runtime_pacing import get_football_live_runtime_pacing

    rts = SignalRuntimeSettingsService()
    rts.enable_sport("football")
    rts.start()
    get_football_live_runtime_pacing().reset_session()
    FootballLiveSessionService().start_session()
    t0 = time.perf_counter()
    cres = await AutoSignalService().run_single_cycle(sessionmaker, message.bot, dry_run=False)
    AutoSignalService().update_football_live_session_diagnostics_with_pacing(
        cres, cycle_wall_seconds=float(time.perf_counter() - t0)
    )
    AutoSignalService().log_football_cycle_trace(cres)
    text = format_football_session_start_user_message(cres, persistent=True)
    text = (
        text
        + "\n\n"
        + "—\n"
        + "Live-only: только матчи с признаком live. Повтор той же идеи в сессии блокируется."
    )
    await _answer_long_message(
        message, text, reply_markup=get_signal_control_keyboard()
    )


@router.message(Command("football_live_debug"))
async def cmd_football_live_debug(message: Message) -> None:
    """Admin-only: last full football live cycle breakdown (legacy verbose format)."""
    if not _is_allowed(message):
        await _deny(message)
        return
    diag = SignalRuntimeDiagnosticsService().get_state()
    txt = diag.get("football_live_last_combat_debug_telegram_text") or diag.get(
        "football_live_last_cycle_debug_telegram_text"
    )
    if not txt or not str(txt).strip():
        await message.answer(
            "Пока нет сохранённого технического отчёта. "
            "Запустите ▶️ Старт или дождитесь следующего live-цикла.",
            reply_markup=get_signal_control_keyboard(),
        )
        return
    await _answer_long_message(message, str(txt), reply_markup=get_signal_control_keyboard())


@router.message(Command("football_live_ranker_debug"))
async def cmd_football_live_ranker_debug(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    """Preview-only S12 ranker: one dry-run cycle, no DB writes and no channel sends."""
    if not _is_allowed(message):
        await _deny(message)
        return
    await message.answer("S12 preview: запускаю dry-run live-cycle, сигналы не создаются и не отправляются...")
    res = await AutoSignalService().run_single_cycle(sessionmaker, message.bot, dry_run=True)
    AutoSignalService().log_football_cycle_trace(res)
    diag = SignalRuntimeDiagnosticsService().get_state()
    try:
        top = json.loads(str(diag.get("football_live_ranker_top_json") or "[]"))
        if not isinstance(top, list):
            top = []
    except (json.JSONDecodeError, TypeError):
        top = []
    try:
        eligible_top = json.loads(str(diag.get("football_live_ranker_eligible_top_json") or "[]"))
        if not isinstance(eligible_top, list):
            eligible_top = []
    except (json.JSONDecodeError, TypeError):
        eligible_top = []
    try:
        watchlist_top = json.loads(str(diag.get("football_live_ranker_watchlist_top_json") or "[]"))
        if not isinstance(watchlist_top, list):
            watchlist_top = []
    except (json.JSONDecodeError, TypeError):
        watchlist_top = []
    try:
        blocked_breakdown = json.loads(str(diag.get("football_live_ranker_blocked_breakdown_json") or "{}"))
        if not isinstance(blocked_breakdown, dict):
            blocked_breakdown = {}
    except (json.JSONDecodeError, TypeError):
        blocked_breakdown = {}
    eligible_count = int(diag.get("football_live_ranker_eligible_count") or 0)
    watchlist_count = int(diag.get("football_live_ranker_watchlist_count") or 0)

    def _append_ranker_rows(lines_out: list[str], rows: list[object]) -> None:
        if not rows:
            lines_out.append("— нет")
            return
        for idx, row in enumerate(rows[:10], start=1):
            if not isinstance(row, dict):
                continue
            eligible = "yes" if row.get("send_eligible") else "no"
            api = "yes" if row.get("api_intelligence") else "no"
            lines_out.extend(
                [
                    f"{idx}. {row.get('match') or '—'}",
                    f"   {row.get('minute') or '—'}' {row.get('score') or '—'} | {row.get('proposed_bet') or '—'} | odds {row.get('odds') or '—'}",
                    f"   score={row.get('analytic_score')} risk={row.get('risk_level')} api={api} bucket={row.get('preview_bucket') or '—'} eligible={eligible}",
                    f"   why: {row.get('confidence_reason') or '—'}",
                    f"   block: {row.get('block_reason') or '—'}",
                ]
            )

    lines = [
        "🧪 S12_LIVE_ANALYTIC_RANKER preview-only",
        "",
        f"after_integrity: {res.report_after_integrity}",
        f"ranker opportunities: {int(diag.get('football_live_ranker_candidates') or 0)}",
        f"top_count: {int(diag.get('football_live_ranker_top_count') or 0)}",
        f"eligible_count: {eligible_count}",
        f"watchlist_count: {watchlist_count}",
        f"api_intelligence: {int(diag.get('football_live_ranker_api_count') or 0)}",
        f"blocked_preview: {int(diag.get('football_live_ranker_blocked_count') or 0)}",
        f"blocked_high_risk_count: {int(blocked_breakdown.get('blocked_high_risk_preview') or 0)}",
        f"blocked_exotic_count: {int(blocked_breakdown.get('blocked_exotic_result_like') or 0)}",
        f"blocked_no_api_1x2_count: {int(blocked_breakdown.get('blocked_1x2_without_api_intelligence') or 0)}",
        f"blocked_trailing_count: {int(blocked_breakdown.get('blocked_trailing_side_1x2') or 0)}",
        "",
        "Eligible ideas:",
    ]
    _append_ranker_rows(lines, eligible_top)
    lines.extend(["", "Watchlist ideas:"])
    _append_ranker_rows(lines, watchlist_top)
    lines.extend(["", "Blocked summary:"])
    if blocked_breakdown:
        lines.extend([f"• {k}: {v}" for k, v in list(blocked_breakdown.items())[:10]])
    else:
        lines.append("— нет")
    blocked_top = [row for row in top if isinstance(row, dict) and not bool(row.get("send_eligible"))]
    if eligible_count < 10 and blocked_top:
        lines.extend(["", "Top blocked ideas (preview):"])
        for idx, row in enumerate(blocked_top[:5], start=1):
            lines.append(
                f"{idx}. {row.get('match') or '—'} | {row.get('proposed_bet') or '—'} | "
                f"score={row.get('analytic_score')} | block={row.get('block_reason') or '—'}"
            )
    await _answer_long_message(message, "\n".join(lines), reply_markup=get_signal_control_keyboard())


@router.message(Command("football_live_strategy_stats"))
async def cmd_football_live_strategy_stats(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    async with sessionmaker() as session:
        svc = FootballLiveStrategyPerformanceService()
        epoch, epoch_meta = await svc.resolve_strategy_epoch_signaled_at_utc(session)
        rows = await svc.load_strategy_rows(session, since_signaled_at_utc=epoch, limit=5000)
        rep = svc.build_report(rows, epoch_meta=epoch_meta)

    def _pct(x: object) -> str:
        try:
            v = float(x)
        except (TypeError, ValueError):
            return "—"
        return f"{v * 100.0:.1f}%"

    lines: list[str] = []
    lines.append("⚽ Football LIVE — Strategy stats (live_auto)")
    lines.append("Фильтр: sport=FOOTBALL · is_live=true · notes=live_auto · strategy_id present")
    ep = rep.get("epoch") if isinstance(rep.get("epoch"), dict) else {}
    if isinstance(ep, dict) and ep.get("epoch_signaled_at_utc"):
        lines.append("")
        lines.append("— Эпоха стратегии (отсечка) —")
        lines.append(f"• Правило: {ep.get('epoch_rule')}")
        lines.append(f"• Коммит стратегий (UTC): {ep.get('commit_ts_utc')}")
        if ep.get("first_db_strategy_id_signal_signaled_at_utc"):
            lines.append(f"• Первый сигнал с strategy_id в БД (signaled_at UTC): {ep.get('first_db_strategy_id_signal_signaled_at_utc')}")
        lines.append(f"• Итоговая отсечка (signaled_at >=): {ep.get('epoch_signaled_at_utc')}")
    lines.append("")
    lines.append("— Общая статистика —")
    lines.append(f"• Всего strategy signals (после отсечки): {rep.get('total')}")
    sids = rep.get("strategy_ids_observed") or []
    if isinstance(sids, list) and sids:
        lines.append("• strategy_id наблюдались: " + ", ".join([str(x) for x in sids]))
    lines.append(
        f"• Settled: {rep.get('settled')}  · WIN: {rep.get('WIN')}  · LOSE: {rep.get('LOSE')}  · VOID: {rep.get('VOID')}"
    )
    lines.append(f"• Win rate (WIN/(WIN+LOSE)): {_pct(rep.get('win_rate'))}")
    avg_odds = rep.get("avg_odds")
    lines.append(f"• Avg odds: {avg_odds:.2f}" if isinstance(avg_odds, (int, float)) else "• Avg odds: —")
    ob = rep.get("odds_buckets") or {}
    if isinstance(ob, dict) and ob:
        parts = [f"{k}={int(v)}" for k, v in sorted(ob.items(), key=lambda kv: str(kv[0]))]
        lines.append("• Odds buckets: " + ", ".join(parts[:12]) + ("…" if len(parts) > 12 else ""))

    lines.append("")
    lines.append("— По стратегиям —")
    bys = rep.get("by_strategy") or {}
    if isinstance(bys, dict) and bys:
        for sid, st in sorted(
            bys.items(), key=lambda kv: (-int((kv[1] or {}).get("total") or 0), kv[0])
        ):
            if not isinstance(st, dict):
                continue
            nm = st.get("strategy_name") or ""
            avg2 = st.get("avg_odds")
            avg2s = f"{float(avg2):.2f}" if avg2 is not None else "—"
            lines.append(
                f"• {sid}{(' — ' + nm) if nm else ''}: total={st.get('total')} settled={st.get('settled')} "
                f"WIN={st.get('WIN')} LOSE={st.get('LOSE')} VOID={st.get('VOID')} "
                f"wr={_pct(st.get('win_rate'))} avg_odds={avg2s}"
            )
    else:
        lines.append("• (пока нет strategy signals в окне)")

    s1 = rep.get("s1_breakdown") or {}
    if isinstance(s1, dict) and (s1.get("minute_buckets") or s1.get("odds_buckets")):
        lines.append("")
        lines.append("— S1 разрезы —")
        mb = s1.get("minute_buckets") or {}
        if isinstance(mb, dict) and mb:
            lines.append("• Minute buckets: " + ", ".join([f"{k}={int(v)}" for k, v in mb.items()]))
        sb = s1.get("score_states_top") or {}
        if isinstance(sb, dict) and sb:
            lines.append("• Score states (top): " + ", ".join([f"{k}={int(v)}" for k, v in sb.items()]))
        ob2 = s1.get("odds_buckets") or {}
        if isinstance(ob2, dict) and ob2:
            lines.append("• Odds buckets: " + ", ".join([f"{k}={int(v)}" for k, v in ob2.items()]))

    latest = rep.get("latest_settled") or []
    lines.append("")
    lines.append("— Последние settled strategy signals (top-10) —")
    if isinstance(latest, list) and latest:
        for r in latest[:10]:
            if not isinstance(r, dict):
                continue
            lines.append(
                f"• id={r.get('signal_id')} {r.get('match')} | {r.get('strategy_id')} | "
                f"{r.get('bet')} | odds={r.get('odds')} | m={r.get('minute')} | score={r.get('score')} | "
                f"{r.get('result')} | outcome_reason_code={r.get('outcome_reason_code') or '—'}"
            )
    else:
        lines.append("• (пока нет settled сигналов в окне; вероятно матчи ещё не завершились)")

    short = rep.get("latest_strategy_signals_short") or []
    lines.append("")
    lines.append("— Short report: последние strategy-based сигналы (после отсечки) —")
    if isinstance(short, list) and short:
        for r in short[:15]:
            if not isinstance(r, dict):
                continue
            lines.append(
                f"• id={r.get('signal_id')} created={r.get('created_at')} sig={r.get('signaled_at')} | "
                f"{r.get('strategy_id')} | {r.get('match')} | m={r.get('minute')} score={r.get('score')} | "
                f"{r.get('bet_text')} | odds={r.get('odds')} | settle={r.get('settlement_status')} | "
                f"orc={r.get('outcome_reason_code') or '—'}"
            )
    else:
        lines.append("• (пока нет strategy signals после отсечки)")

    settled_n = int(rep.get("settled") or 0)
    winlose = int(rep.get("WIN") or 0) + int(rep.get("LOSE") or 0)
    lines.append("")
    if settled_n <= 5 or winlose < 20:
        lines.append(
            "Оценка качества стратегии: пока рано. Для первых осмысленных выводов обычно нужно "
            "хотя бы ~20–50 settled WIN/LOSE (а не просто settled), плюс стабильный источник результатов."
        )

    await _answer_long_message(message, "\n".join(lines), reply_markup=get_signal_control_keyboard())


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


def _prog_netloc(endpoint: str | None) -> str:
    if not endpoint:
        return "—"
    try:
        from urllib.parse import urlparse

        netloc = urlparse(endpoint).netloc
        return netloc or "—"
    except Exception:
        return "—"


def _humanize_live_auth_short(code: str | None) -> str:
    if not code or code == "—":
        return "нет данных"
    c = code.lower()
    if c == "ok":
        return "Ok"
    if c in {"unauthorized_quota", "out_of_usage_credits"}:
        return "квота Live API исчерпана"
    if "quota" in c or "usage" in c:
        return "квота Live API исчерпана"
    if c == "no_key":
        return "ключ API не задан"
    if c == "unauthorized":
        return "не авторизован"
    if c == "http_error":
        return "ошибка HTTP"
    if c == "request_error":
        return "ошибка запроса"
    return code


def _humanize_rejection_for_owner(raw: str | None) -> str:
    if not raw:
        return "нет данных"
    s = raw.lower()
    mapping = [
        ("non_live_source_blocked", "источник не в режиме live — автоматическая отправка отключена"),
        ("non_real_source_blocked", "источник не считается боевым"),
        ("preview_only enabled", "включён режим только preview в настройках"),
        ("paused", "контур на паузе"),
        ("football_disabled", "футбол выключен в runtime"),
        ("provider_not_configured", "Live API не настроен"),
        ("low_score", "score ниже порога"),
        ("dry_run_low_score", "score ниже порога"),
        ("blocked_by_dedup", "уже есть похожий сигнал в базе"),
        ("blocked_low_score", "score ниже порога"),
        ("too_far_in_time", "матч слишком далеко по времени"),
        ("football_send_filter_rejected_all", "все кандидаты отсеяны фильтром отправки"),
        ("dropped_invalid_market_mapping", "ставка не прошла проверку целостности"),
        ("dropped_invalid_total_scope", "несовпадение тотала по области действия"),
        ("payload_is_not_dict", "ошибка формата данных провайдера"),
        ("football_live_session_inactive", "live-сессия не запущена — нажмите ▶️ Старт"),
        (
            "blocked_s8_home_00_without_api_context",
            "временный safety-rule снял S8 П1 0:0 без API-Football context/pressure",
        ),
        (
            "blocked_s8_1x2_00_without_api_context",
            "временный safety-rule снял S8 1X2 0:0 без API-Football context",
        ),
        (
            "blocked_s8_1x2_00_no_pressure",
            "временный safety-rule снял S8 1X2 0:0 без pressure_score >= 2",
        ),
        ("blocked_stale_manual_live_source", "ручной live JSON слишком старый"),
        ("blocked_stale_live_source", "снимок live устарел по задержке обработки"),
        ("blocked_stale_live_events", "все live-матчи признаны протухшими"),
    ]
    for needle, nice in mapping:
        if needle.lower() in s:
            return nice
    return raw


def _humanize_live_bottleneck_ru(token: str | None) -> str:
    if not token or token == "—":
        return "нет данных"
    m = {
        "blocked_paused": "контур на паузе",
        "blocked_no_live_session": "live-сессия не запущена (для боя нужен ▶️ Старт)",
        "blocked_no_live_matches": "нет live-матчей футбола в выборке провайдера",
        "blocked_send_filter": "все отсеяны фильтром отправки (live/семья/время)",
        "blocked_integrity": "не прошли проверку целостности ставки",
        "blocked_no_strategy_match": "после integrity ни один рынок не прошёл strategy gate (S8/S9)",
        "blocked_context_filter": "после strategy gate кандидаты сняты context filter",
        "blocked_s8_1x2_00_without_api_context": "временный safety-rule снял S8 1X2 0:0 без API-Football context",
        "blocked_s8_1x2_00_no_pressure": "временный safety-rule снял S8 1X2 0:0 без pressure_score >= 2",
        "blocked_s8_home_00_without_api_context": "временный safety-rule снял S8 П1 0:0 без API-Football context/pressure",
        "blocked_value_filter": "после context filter кандидаты сняты value filter",
        "blocked_low_score": "score ниже порога",
        "blocked_duplicate_idea": "повтор той же идеи в рамках live-сессии",
        "blocked_dedup_db": "отсеяно дедупликацией в базе",
        "blocked_non_real_source": "источник не считается боевым live",
        "blocked_non_live_source": "источник не в режиме live",
        "blocked_notify_config": "сигнал создан, но уведомление не ушло (чат/пауза)",
        "blocked_fetch": "ошибка загрузки у провайдера",
        "blocked_live_provider_auth_or_quota": "Live API: авторизация или квота",
        "blocked_preview_only": "включён только preview в .env",
        "ok_sent_telegram": "сообщение ушло в Telegram",
        "dry_run_ok": "тестовый прогон: сигнал был бы выбран",
        "ok_no_signal_selected": "цикл завершён без выбранной ставки",
        "blocked_unknown": "причина не классифицирована",
        "blocked_stale_manual_live_source": "ручной live JSON слишком старый",
        "blocked_stale_live_source": "снимок live устарел по задержке обработки",
        "blocked_stale_live_events": "все live-матчи признаны протухшими",
        "blocked_winline_live_unavailable": "Winline live (WebSocket) недоступен",
    }
    return m.get(token, token.replace("_", " "))


def _humanize_status_token(token: str | None) -> str:
    if not token:
        return ""
    m = {
        "blocked_low_score": "score ниже порога",
        "blocked_send_filter": "рынок отсеян фильтром отправки",
        "blocked_integrity": "ставка не прошла проверку целостности",
        "blocked_too_far_in_time": "матч слишком далеко по времени",
        "blocked_dedup": "похожий сигнал уже есть",
        "blocked_unknown": "не удалось классифицировать причину",
        "blocked_duplicate_idea": "повтор идеи в сессии",
        "blocked_non_real_source": "источник не считается боевым",
        "no_candidates": "нет подходящих кандидатов",
        "selected": "выбран для отправки",
        "blocked_pre_send_pipeline": "отсев до send-фильтра (runtime / дедуп в батче)",
        "blocked_no_enriched_scored_row": "после integrity не нашлось кандидата под S8/S9",
        "blocked_no_strategy_match": "после integrity не нашлось кандидата под S8/S9",
        "blocked_context_filter": "после strategy gate матч снят context filter",
        "blocked_s8_1x2_00_without_api_context": "временный safety-rule снял S8 1X2 0:0 без API-Football context",
        "blocked_s8_1x2_00_no_pressure": "временный safety-rule снял S8 1X2 0:0 без pressure_score >= 2",
        "blocked_s8_home_00_without_api_context": "временный safety-rule снял S8 П1 0:0 без API-Football context/pressure",
        "blocked_value_filter": "после context filter матч снят value filter",
        "blocked_live_market_sanity": "pre-send live sanity (счёт, текст, plausibility)",
        "blocked_invalid_live_market_text": "некорректный live-текст / маппинг рынка",
        "blocked_impossible_live_outcome": "исход противоречит счёту/минуте",
        "blocked_low_live_plausibility": "низкая plausibility (поздно / слабая логика)",
        "blocked_suspicious_core_live_signal": "сомнительный core live (контекст/агрессивный тотал)",
        "blocked_missing_live_context_from_source": "нет счёта/минуты в данных провайдера (1X2)",
        "blocked_live_quality_gate": "не прошёл combat quality gate (шаблон/юниоры/лотерея)",
        "blocked_core_late_high_gap_total": "тотал: слишком много голов на поздней стадии",
        "blocked_late_live_market": "поздняя стадия / timing: сигнал запоздал для live",
    }
    return m.get(token, token.replace("_", " "))


def _format_football_prog_run_report(res: AutoSignalCycleResult) -> str:
    """Человекочитаемый отчёт для «⚽ Прогон». Без полных URL и сырых machine-кодов."""
    settings = get_settings()
    diag = SignalRuntimeDiagnosticsService().get_state()
    dbg = res.football_cycle_debug or {}

    matches_found = res.report_matches_found
    if matches_found is None:
        mlist = dbg.get("matches") or []
        matches_found = len(mlist) if mlist else 0

    cand_total = res.report_candidates if res.report_candidates is not None else res.candidates_before_filter_count
    after_filter = res.report_after_filter if res.report_after_filter is not None else diag.get("football_after_filter_count")
    after_integrity = res.report_after_integrity if res.report_after_integrity is not None else diag.get("football_after_integrity_count")
    after_score = res.report_after_scoring if res.report_after_scoring is not None else None

    fin = (res.report_final_signal or "").strip() or "НЕТ"
    signal_yes = fin == "ДА"

    if res.preview_only:
        mode_line = "Только preview (.env)"
    elif res.runtime_paused:
        mode_line = "Контур на паузе"
    elif res.message == "football_disabled":
        mode_line = "Футбол выключен"
    elif res.message == "provider_not_configured":
        mode_line = "Live API не настроен"
    elif res.fetch_ok is False and res.message not in {"paused"}:
        mode_line = "Ошибка загрузки данных"
    elif bool(res.fallback_used):
        mode_line = "Live API недоступен → использован Winline JSON"
    elif (diag.get("source_mode") or "").lower() in {"semi_live_manual", "manual_example"}:
        mode_line = "Winline JSON (semi-live)"
    elif diag.get("source_mode") == "live":
        mode_line = "Live API"
    else:
        mode_line = "Смешанный режим"

    live_api_human = _humanize_live_auth_short(res.live_auth_status)

    used_parts: list[str] = []
    if bool(res.fallback_used):
        used_parts.append(f"фолбэк {res.fallback_source_name or 'Winline JSON'}")
    if diag.get("football_source"):
        used_parts.append(str(diag["football_source"]))
    elif res.source_name:
        used_parts.append(str(res.source_name))
    used_source = ", ".join(dict.fromkeys(used_parts)) if used_parts else (res.source_name or "—")

    lines: list[str] = ["⚽ Футбольный прогон завершён", ""]

    dbg = res.football_cycle_debug or {}
    if dbg.get("pipeline_live_only"):
        lines.extend(["📍 Политика контура: только live-матчи (prematch исключён).", ""])

    live_payload_yes = bool(matches_found) or bool(cand_total)
    lq0 = (dbg or {}).get("live_quality_summary") or {}
    agg0 = (dbg or {}).get("football_pipeline_aggregate")
    if isinstance(agg0, dict) and agg0:
        agg = agg0
    else:
        agg = lq0.get("football_pipeline_aggregate") if isinstance(lq0, dict) else {}
    if isinstance(agg, dict) and "total_live_matches_tracked" in agg:
        m_trk = int(agg.get("total_live_matches_tracked") or 0)
    else:
        m_trk = 0
    m_send_idea: int | None
    if isinstance(agg, dict) and "matches_with_sendable_idea" in agg:
        m_send_idea = int(agg.get("matches_with_sendable_idea") or 0)
    else:
        m_send_idea = None
    lss0 = (dbg or {}).get("live_send_stats") or {}
    soft_n = int(lss0.get("soft_sendable_total") or 0) if lss0 else 0
    norm_n = int(lss0.get("normal_sendable") or 0) if lss0 else 0
    m_send_line = m_send_idea if m_send_idea is not None else (norm_n + soft_n)
    if isinstance(agg, dict) and "total_live_matches_tracked" in agg:
        display_trk: object = m_trk
    else:
        display_trk = matches_found if matches_found is not None else "—"
    lines.extend(
        [
            "📌 Live сейчас (прогон не зависит от ▶️ Старт):",
            f"• Live-матчи в данных: {'да' if live_payload_yes else 'нет'}",
            f"• Live-матчей в pipeline (после свежести, Winline): {display_trk}",
            f"• Матчей с идеей normal+soft (send-gate): {m_send_line}",
            f"• Кандидатов-строк после свежести: {cand_total if cand_total is not None else '—'}",
            "",
        ]
    )
    if isinstance(agg, dict) and agg.get("total_live_matches_tracked"):
        lines.append(
            f"• Воронка по матчам: pre-pipeline {agg.get('with_candidates_pre_send_pipeline', '—')} → "
            f"send-фильтр {agg.get('after_send_filter', '—')} → integrity {agg.get('after_integrity', '—')} → "
            f"строк в scoring {agg.get('after_scoring_pool', '—')}"
        )
        lines.append("")
    if isinstance(agg, dict) and (int(agg.get("live_sanity_dropped") or 0) > 0 or (dbg or {}).get("live_sanity_drops")):
        _ldn = int(agg.get("live_sanity_dropped") or 0)
        _drops0 = (dbg or {}).get("live_sanity_drops") or []
        lines.append("— Pre-send live sanity —")
        lines.append(f"• Снято перед отправкой (по воронке): {_ldn}")
        if _drops0 and isinstance(_drops0, list) and str((_drops0[0] or {}).get("reason", "")).strip():
            _d0 = _drops0[0] or {}
            lines.append(
                f"• Пример: eid={_d0.get('eid', '—')} — {str(_d0.get('reason', ''))[:300]}"
            )
        # если прогон dry-run, в памяти теста нет best_rejected
        d_diag = SignalRuntimeDiagnosticsService().get_state()
        if (d_diag.get("football_live_sanity_last_best_rejected") or "") and (not _drops0):
            lines.append("• (из памяти) " + str(d_diag.get("football_live_sanity_last_best_rejected"))[:300])
        lines.append("")
    top10 = (dbg or {}).get("top_10_live_pipeline_lines") or []
    if top10:
        lines.append("— ТОП-10 live-идей по score (Winline → pipeline) —")
        for tln in top10[:10]:
            lines.append("• " + tln[:420])
        lines.append("")
    send_lines = (dbg or {}).get("sendable_live_idea_lines") or []
    bpipe = (dbg or {}).get("bottleneck_no_sendable_pipeline_ru") or (
        (lq0 or {}).get("bottleneck_no_sendable_pipeline_ru") if isinstance(lq0, dict) else None
    )
    if send_lines and m_send_idea is not None and m_send_idea > 0:
        lines.append("— Сейчас sendable (normal+soft) —")
        lines.append("• Пример: " + str(send_lines[0])[:380])
        for tln in send_lines[1:8]:
            lines.append("• " + tln[:400])
        lines.append("")
    elif bpipe and m_trk and m_send_idea == 0:
        lines.append("— Почему нет sendable-идеи (факт по воронке) —")
        lines.append("• " + str(bpipe)[:450])
        lines.append("")

    from app.services.football_live_session_service import FootballLiveSessionService as _Fls

    _snap = _Fls().snapshot()
    _rem = _Fls().remaining_seconds()
    if not _snap.active:
        lines.extend(
            [
                "ℹ️ Подсказка:",
                "• Чтобы бот начал слать live-сигналы, нажмите ▶️ Старт (сессия до ⏸ Стоп, cadence см. статус).",
                "",
            ]
        )
    else:
        lines.extend(
            [
                "ℹ️ Подсказка:",
                "• Live-сессия активна — новые идеи будут отправляться автоматически.",
                "",
            ]
        )
    d_combat = SignalRuntimeDiagnosticsService().get_state()
    if d_combat.get("football_last_combat_cycle_at"):
        _rsm = str(d_combat.get("football_last_combat_send_mode") or "none")
        _rsm_ru = {
            "normal": "обычный (normal)",
            "soft": "мягкий (soft)",
            "mixed": "смешанный",
            "none": "нет",
        }.get(_rsm, _rsm)
        lines.extend(
            [
                "— Последний боевой цикл (не этот тест) —",
                f"• Время: {d_combat.get('football_last_combat_cycle_at')}",
                f"• Отправлено: {int(d_combat.get('football_last_combat_messages_sent') or 0)}",
                f"• Режим: {_rsm_ru}",
                f"• Главный блокер: {d_combat.get('football_last_combat_bottleneck_ru') or '—'}",
                "",
            ]
        )
    else:
        lines.extend(
            [
                "— Последний боевой цикл —",
                "• Пока нет зафиксированного боя после рестарта процесса.",
                "",
            ]
        )
    lines.extend(
        [
            "🎚 Память live-сессии (только если жмёте ▶️ Старт для боя):",
            f"• Активна: {'да' if _snap.active else 'нет'}",
            f"• Осталось мин: {round(_rem / 60.0, 1) if _rem is not None and _snap.active else '—'}",
            f"• В БД за сессию: {_snap.signals_sent_in_session}",
            f"• В Telegram за сессию: {_snap.telegram_messages_sent_in_session}",
            f"• Повтор идей отсеяно (сессия): {_snap.duplicate_ideas_blocked_session}",
            f"• Уникальных идей в памяти сессии: {_snap.sent_idea_keys_count}",
            "",
        ]
    )

    eff = diag.get("football_live_effective_source") or used_source
    sm = (diag.get("source_mode") or "").lower()
    if sm == "semi_live_manual":
        lines.append("⚠️ Эффективный источник: semi_live_manual (контролируемый JSON, не маскируем под боевой live).")
        lines.append("")
    elif sm and sm != "live" and not res.fallback_used:
        lines.append(f"⚠️ Режим источника: {sm} (не чистый live).")
        lines.append("")

    src_age_line = _fmt_source_age_for_ui(diag.get("football_live_source_age_seconds"))
    ff_ru = _fmt_football_live_source_label_ru(diag)

    lines.extend(
        [
            "— Winline live (последний цикл в памяти) —",
            f"• primary: {diag.get('football_primary_live_source') or '—'}",
            f"• Winline активен: {_fmt_yes_no(bool(diag.get('football_winline_ws_active_last_cycle')))}",
            f"• матчей/ивентов: {diag.get('football_winline_football_event_count') or 0} • линий: {diag.get('football_winline_line_count_raw') or 0} • кандидатов: {diag.get('football_winline_football_candidate_count') or 0}",
            f"• ошибка WS: {diag.get('football_winline_error_last') or '—'}",
            "",
            "📡 Источник:",
            f"• Режим: {mode_line}",
            f"• Статус Live API: {live_api_human}",
            f"• Эффективный источник данных: {eff}",
            "",
            "— Проверка свежести (live-only) —",
            f"• {ff_ru}",
            f"• Возраст источника: {src_age_line}",
            f"• Кандидатов до проверки свежести: {diag.get('football_live_freshness_candidates_before') or 0}",
            f"• Live-матчей принято: {diag.get('football_live_freshness_live_events_accepted') or 0}",
            f"• Устаревших live-матчей отсеяно: {diag.get('football_live_freshness_stale_events_dropped') or 0}",
            f"• Рынков на устаревших матчах отсеяно: {diag.get('football_live_freshness_stale_markets_dropped') or 0}",
            "",
        ]
    )
    best_live_lines = dbg.get("best_live_ideas_for_prog") or []
    lq_sum = dbg.get("live_quality_summary") or {}
    if best_live_lines:
        lines.extend(
            [
                "— Лучшие live-идеи —",
                *[f"• {ln}" for ln in best_live_lines[:5]],
                "",
            ]
        )
    dist_scores = lq_sum.get("fresh_live_best_scores_distribution") or []
    if dist_scores:
        lines.append(
            "• Лучшие score по свежим матчам: "
            + ", ".join(str(x) for x in dist_scores[:12])
        )
        lines.append("")

    fac_ct = int(lq_sum.get("fresh_live_accepted_count") or 0)
    fb_br = lq_sum.get("fresh_live_send_breakdown") or dbg.get("fresh_live_send_breakdown") or {}
    if fac_ct > 0 and fb_br:
        lines.extend(
            [
                "— Разрез отсева (свежие live, есть кандидаты после проверки свежести) —",
                f"• К отправке (selected): {fb_br.get('selected', 0)}",
                f"• Ниже порога score: {fb_br.get('blocked_low_score', 0)}",
                f"• Повтор идеи (сессия): {fb_br.get('blocked_duplicate_idea', 0)}",
                f"• Фильтр отправки: {fb_br.get('blocked_send_filter', 0)}",
                f"• Integrity: {fb_br.get('blocked_integrity', 0)}",
                f"• Dedup БД (цикл): {fb_br.get('blocked_dedup_db', 0)}",
                "",
            ]
        )

    why_agg = lq_sum.get("why_no_signal_lines") or dbg.get("why_no_signal_lines") or []
    if (not signal_yes) and why_agg:
        lines.append("— Почему нет сигнала —")
        lines.extend([f"• {ln}" for ln in why_agg[:10]])
        lines.append("")
    gap_dist = lq_sum.get("gap_to_sendable_fresh_low_score") or []
    if (not signal_yes) and gap_dist:
        lines.append(
            "• Gap до порога (свежие live, ниже порога): "
            + ", ".join(str(x) for x in gap_dist[:15])
        )
        lines.append("")
    lss = lq_sum.get("live_send_stats") or dbg.get("live_send_stats") or {}
    lines.extend(
        [
            "📊 Сводка (live-only цепочка):",
            f"• Матчей найдено: {matches_found}",
            f"• Кандидатов до фильтра отправки: {cand_total}",
            f"• После фильтра отправки: {after_filter if after_filter is not None else '—'}",
            f"• После проверки целостности: {after_integrity if after_integrity is not None else '—'}",
            f"• После порога score: {after_score if after_score is not None else '—'}",
        ]
    )
    if lss:
        lines.extend(
            [
                "— Live send (отбор кандидатов) —",
                f"• Нормальных (≥ базового порога): {lss.get('normal_sendable', 0)}",
                f"• Мягких (soft) live: {lss.get('soft_sendable_total', 0)}",
                f"• Soft gap ≤1.5: {lss.get('soft_sendable_tight', 0)}",
                f"• Soft relief (main, gap до 2): {lss.get('soft_sendable_relief_single', 0)}",
                f"• S8 1X2 0:0 reject без API context: {lss.get('rejected_s8_1x2_00_without_api_context', 0)}",
                f"• S8 1X2 0:0 reject без pressure_score>=2: {lss.get('rejected_s8_1x2_00_no_pressure', 0)}",
                f"• S8 1X2 0:0 passed с API pressure: {lss.get('passed_s8_1x2_00_with_api_pressure', 0)}",
                f"• Отсеклось (reject) в send-gate: {lss.get('rejected_total', 0)}",
            ]
        )
    d_last = SignalRuntimeDiagnosticsService().get_state()
    _m = (d_last.get("football_last_cycle_send_mode") or "—") or "—"
    if _m in ("normal", "soft", "mixed", "none"):
        _m_ru = {"normal": "обычный (normal)", "soft": "мягкий (soft)", "mixed": "смешанный", "none": "нет"}.get(
            _m, _m
        )
    else:
        _m_ru = str(_m)
    _sd = int(dbg.get("session_idea_dedup_this_cycle") or 0)
    lines.extend(
        [
            "— Итог конверсии (текущий прогон) —",
            f"• Обычных сигналов к пулу: {lss.get('normal_sendable', 0) if lss else 0}",
            f"• Мягко допущенных (soft) к пулу: {lss.get('soft_sendable_total', 0) if lss else 0}",
            f"• Повторов идей (сессия) в этом цикле: {_sd}",
        ]
    )
    if res.dry_run:
        lines.append(f"• Лучший кандидат: режим {_m_ru}")
    else:
        lines.append(f"• Последняя запись в БД: режим пачки {_m_ru}")
    lines.append(
        f"• Счётчик в БД (посл. бою): обычных {d_last.get('football_last_cycle_ingest_normal') or 0}, "
        f"мягких {d_last.get('football_last_cycle_ingest_soft') or 0}"
    )
    fg_lines = format_final_live_gate_summary_lines(dbg.get("final_live_send_gate") or {})
    if fg_lines:
        lines.extend(fg_lines)

    lines.extend(
        [
            "",
            "🎯 Итог:",
            f"• Финальный сигнал: {'Да' if signal_yes else 'Нет'}",
        ]
    )

    chat_ok = settings.signal_chat_id is not None
    if res.dry_run:
        lines.append("• Отправка в канал сейчас: нет (тестовый прогон)")
        if signal_yes:
            lines.append(
                "• В бою ушло бы в канал: "
                + ("да — чат сигналов настроен" if chat_ok else "нет — не задан signal_chat_id")
            )
        else:
            lines.append("• В бою ушло бы в канал: нет — сигнал не выбран")
    else:
        lines.append(f"• Отправлено в канал: {res.notifications_sent_count}")

    raw_code = res.report_rejection_code or res.rejection_reason or res.message
    primary_reason = _humanize_rejection_for_owner(raw_code)
    if primary_reason == "нет данных" or not raw_code:
        bh = dbg.get("bottleneck_hint")
        if bh:
            primary_reason = _humanize_status_token(str(bh))
    lines.append(f"• Комментарий: {primary_reason}")
    bn = diag.get("football_live_cycle_bottleneck")
    if bn:
        lines.append(f"• Узкое место (цикл): {_humanize_live_bottleneck_ru(str(bn))}")

    min_thr = dbg.get("min_signal_score") or dbg.get("min_signal_score_base")
    relief_nt = dbg.get("score_relief_note") or lq_sum.get("score_relief_note")
    if min_thr is not None:
        lines.append("")
        lines.append(f"📐 Базовый порог score: {min_thr}")
        if relief_nt:
            lines.append(f"• Режим отбора: {relief_nt}")

    status_counts = dbg.get("final_status_counts") or {}
    total_m = sum(status_counts.values()) if status_counts else 0
    if status_counts and total_m > 1:
        lines.extend(["", "📎 Сводка по матчам:"])
        for st, cnt in sorted(status_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:5]:
            lines.append(f"• {_humanize_status_token(st)} — {cnt}")

    top_rows = dbg.get("matches_top_for_message") or dbg.get("matches") or []
    if top_rows and total_m <= 8:
        lines.extend(["", "📎 Примеры (до 5):"])
        for row in top_rows[:5]:
            nm = row.get("match_name") or "—"
            st = row.get("final_status")
            sc = row.get("best_candidate_score")
            hs = _humanize_status_token(st) if st else ""
            if sc is not None and (st == "blocked_low_score" or "score" in hs.lower()):
                lines.append(f"• {nm}: {hs} ({sc})")
            else:
                lines.append(f"• {nm}: {hs}")

    sel_match = res.report_selected_match
    sel_bet = res.report_selected_bet
    sel_odds = res.report_selected_odds
    sel_score = res.report_selected_score

    selected_row = None
    for row in dbg.get("matches") or []:
        if row.get("final_status") == "selected":
            selected_row = row
            break
    best_kind = None
    if selected_row is not None:
        if bool(selected_row.get("best_candidate_is_corner_like")):
            best_kind = "corners"
        else:
            best_kind = "main market"

    swd = dbg.get("selected_winner_detail") or {}
    tournament_display = swd.get("tournament_name") or (selected_row or {}).get("tournament_name")
    minute_display = swd.get("minute")
    event_start_display = None
    esa = (selected_row or {}).get("event_start_at")
    if esa:
        event_start_display = str(esa).replace("T", " ").replace("+00:00", " UTC")

    lines.append("")

    if signal_yes and (sel_match or swd):
        bet_disp = swd.get("bet_line") or sel_bet
        odds_disp = swd.get("odds") or sel_odds
        sc_disp = swd.get("score") or sel_score
        match_disp = swd.get("match_name") or sel_match
        fam_disp = swd.get("market_family")
        lines.extend(
            [
                "🏆 Лучший кандидат:",
                f"• Лучшая идея: {best_kind or fam_disp or '—'}",
                f"• Матч: {match_disp}",
                f"• Турнир: {tournament_display or '—'}",
                f"• Минута: {minute_display if minute_display is not None else '—'}",
                f"• Начало: {event_start_display or '—'}",
                f"• Ставка: {bet_disp or '—'}",
                f"• Коэффициент: {odds_disp or '—'}",
                f"• Score: {sc_disp or '—'}",
            ]
        )
        reasons = swd.get("why_selected_lines") or res.report_human_reasons or []
        if reasons:
            lines.append("• Почему выбран (скоринг):")
            for r in reasons[:8]:
                lines.append(f"  — {r}")
        if swd.get("send_path") == "soft":
            lines.append(
                f"• Тип: soft_sendable ({swd.get('soft_label') or '—'})"
            )
            lines.append(
                swd.get("live_note")
                or "Live-сигнал допущен по мягкому порогу (недобор score компенсирован live-контекстом)"
            )
        if res.dry_run:
            lines.extend(
                [
                    "",
                    "ℹ️ Тестовый прогон: БД и канал не трогаем. При таких же данных в бою была бы выбрана эта ставка.",
                ]
            )
    elif signal_yes:
        lines.extend(["🏆 Финальный сигнал: да", "• Детали матча недоступны в отчёте — см. логи цикла.", ""])
    elif not signal_yes:
        lines.append("❌ Сигнал не выбран")
        cfm = dbg.get("closest_fresh_live_miss") or lq_sum.get("closest_fresh_live_miss")
        if cfm and cfm.get("match_name"):
            lines.append("• Ближайший по gap (свежий live, ниже порога):")
            lines.append(f"  — матч: {cfm.get('match_name')}")
            lines.append(f"  — ставка: {cfm.get('best_candidate_market') or '—'}")
            lines.append(f"  — score: {cfm.get('best_candidate_score')}")
            if cfm.get("gap_to_sendable") is not None:
                lines.append(f"  — до порога не хватило: ~{cfm.get('gap_to_sendable')}")
        nearest = None
        best_sc = None
        for row in dbg.get("matches") or []:
            sc = row.get("best_candidate_score")
            if sc is not None:
                if best_sc is None or float(sc) > float(best_sc):
                    best_sc = float(sc)
                    nearest = row
        if (not cfm or not cfm.get("match_name")) and nearest and nearest.get("match_name"):
            lines.append(f"• Лучший матч по score: {nearest.get('match_name')}")
            lines.append(f"• Его score: {nearest.get('best_candidate_score')}")
            st = nearest.get("final_status")
            if st:
                lines.append(f"• Почему не ушёл: {_humanize_status_token(str(st))}")
            eff_thr = dbg.get("min_signal_score") or dbg.get("min_signal_score_base")
            if eff_thr is not None and best_sc is not None:
                gap = float(eff_thr) - float(best_sc)
                if gap > 0:
                    lines.append(f"• До порога не хватило: ~{gap:.1f}")
        elif (cand_total == 0 or cand_total is None) and not dbg.get("matches"):
            lines.append("• Лучший кандидат: не сформирован (нет live-данных по футболу)")
        if res.report_dedup_skipped:
            lines.append(f"• Dedup в БД отклонил кандидатов: {res.report_dedup_skipped}")
        if res.dry_run:
            lines.append("")
            lines.append("ℹ️ Прогон тестовый: канал и БД не затрагиваются.")

    lines.extend(
        [
            "",
            "🛠 Диагностика:",
            f"• Provider: {res.source_name or '—'}",
            f"• HTTP: {res.last_live_http_status if res.last_live_http_status is not None else '—'}",
            f"• Auth (сырой код): {res.live_auth_status or '—'}",
            f"• Тестовый dry-run: {'да' if res.dry_run else 'нет'}",
            f"• Хост API: {_prog_netloc(res.endpoint)}",
        ]
    )

    text = "\n".join(lines)
    if len(text) > 3900:
        return text[:3870] + "\n… (обрезано; полный вывод — в логах сервера или /football_live_debug для админов)"
    return text


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
    AutoSignalService().log_football_cycle_trace(res)
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


@router.message(Command("football_postmatch_verify"))
async def cmd_football_postmatch_verify(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    parts = (message.text or "").split()
    limit = 200
    if len(parts) > 1:
        try:
            limit = int(parts[1])
        except Exception:
            await message.answer("Usage: /football_postmatch_verify [limit]\n limit 1..500, default 200")
            return
    async with sessionmaker() as session:
        text = await build_football_postmatch_verify_report(session, limit=limit, detail_count=10)
    await _answer_long_message(message, text)


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
        sig_row = (await session.execute(select(Signal).where(Signal.id == int(signal_id)))).scalar_one_or_none()
        if sig_row and sig_row.sport == SportType.FOOTBALL:
            try:
                await FootballSignalOutcomeReasonService().apply_to_signal(session, sig_row, result, None)
            except Exception:
                logger.exception("football outcome reason after /settle_signal (signal_id=%s)", signal_id)
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
    diag = SignalRuntimeDiagnosticsService().get_state()

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
        *_format_external_api_status_lines(diag),
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
