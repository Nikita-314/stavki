from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.bot.keyboards.debug import get_debug_keyboard
from app.core.enums import BetResult, EntryStatus
from app.core.config import get_settings
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


router = Router(name="debug")


def _is_allowed(message: Message) -> bool:
    settings = get_settings()
    if not settings.admin_user_ids:
        return True
    user_id = message.from_user.id if message.from_user else None
    return bool(user_id and user_id in settings.admin_user_ids)


async def _deny(message: Message) -> None:
    await message.answer("Access denied")


def _fmt_decimal(v: Decimal | None) -> str:
    if v is None:
        return "None"
    return str(v)


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _parse_signal_id(value: str) -> int:
    return int(value)


def _parse_decimal(value: str) -> Decimal:
    # Accept "1.87" style values; keep Decimal precision.
    return Decimal(value)


def _fmt_enum(v: object) -> str:
    return getattr(v, "value", str(v))


def _parse_sport(value: str):
    # SportType is a str Enum: accept "CS2", "DOTA2", "FOOTBALL" (case-insensitive).
    from app.core.enums import SportType

    return SportType(value.strip().upper())


def _parse_int(value: str) -> int:
    return int(value)


@router.message(Command("debug"))
async def cmd_debug(message: Message) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return
    await message.answer("Debug menu", reply_markup=get_debug_keyboard())


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
                "mode: unit_based",
                f"base_amount: {overview.base_amount}",
                f"base_snapshot_at: {overview.base_snapshot_at}",
                f"base_label: {overview.base_label}",
                f"total_profit_loss_since_base: {overview.total_profit_loss_since_base}",
                f"current_balance: {overview.current_balance}",
                f"settled_signals_count: {overview.settled_signals_count}",
                f"wins/losses/voids: {overview.wins}/{overview.losses}/{overview.voids}",
            ]
        )
    )


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
                "mode: realistic_fixed_stake",
                f"flat_stake_rub: {overview.flat_stake_rub}",
                f"base_amount: {overview.base_amount}",
                f"base_snapshot_at: {overview.base_snapshot_at}",
                f"base_label: {overview.base_label}",
                f"total_profit_loss_rub: {overview.total_profit_loss_rub}",
                f"current_balance_rub: {overview.current_balance_rub}",
                f"settled_signals_count: {overview.settled_signals_count}",
                f"wins/losses/voids: {overview.wins}/{overview.losses}/{overview.voids}",
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


@router.message(Command("period_report"))
async def cmd_period_report(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    async with sessionmaker() as session:
        report = await PeriodReportService().get_period_report(session)

    o = report.overview

    lines: list[str] = [
        "mode: unit_based",
        f"period_started_at: {o.period_started_at}",
        f"period_label: {o.period_label}",
        f"start_balance: {o.start_balance}",
        f"total_profit_loss: {o.total_profit_loss}",
        f"current_balance: {o.current_balance}",
        f"settled_signals_count: {o.settled_signals_count}",
        f"wins/losses/voids: {o.wins}/{o.losses}/{o.voids}",
    ]

    top_sport = report.by_sport[:5]
    if top_sport:
        lines.append("")
        lines.append("top 5 by_sport:")
        for it in top_sport:
            lines.append(
                f"- {it.key}: cnt={it.settled_signals_count} w/l/v={it.wins}/{it.losses}/{it.voids} "
                f"pl={it.total_profit_loss} avg={it.avg_profit_loss}"
            )

    top_market = report.by_market_type[:5]
    if top_market:
        lines.append("")
        lines.append("top 5 by_market_type:")
        for it in top_market:
            lines.append(
                f"- {it.key}: cnt={it.settled_signals_count} w/l/v={it.wins}/{it.losses}/{it.voids} "
                f"pl={it.total_profit_loss} avg={it.avg_profit_loss}"
            )

    await message.answer("\n".join(lines))


@router.message(Command("period_report_rub"))
async def cmd_period_report_rub(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    async with sessionmaker() as session:
        report = await PeriodReportService().get_realistic_period_report(session)

    o = report.overview

    lines: list[str] = [
        "mode: realistic_fixed_stake",
        f"period_started_at: {o.period_started_at}",
        f"period_label: {o.period_label}",
        f"start_balance_rub: {o.start_balance_rub}",
        f"flat_stake_rub: {o.flat_stake_rub}",
        f"total_profit_loss_rub: {o.total_profit_loss_rub}",
        f"current_balance_rub: {o.current_balance_rub}",
        f"settled_signals_count: {o.settled_signals_count}",
        f"wins/losses/voids: {o.wins}/{o.losses}/{o.voids}",
    ]

    top_sport = report.by_sport[:5]
    if top_sport:
        lines.append("")
        lines.append("top 5 by_sport:")
        for it in top_sport:
            lines.append(
                f"- {it.key}: cnt={it.settled_signals_count} w/l/v={it.wins}/{it.losses}/{it.voids} "
                f"pl_rub={it.total_profit_loss_rub} avg_rub={it.avg_profit_loss_rub}"
            )

    top_market = report.by_market_type[:5]
    if top_market:
        lines.append("")
        lines.append("top 5 by_market_type:")
        for it in top_market:
            lines.append(
                f"- {it.key}: cnt={it.settled_signals_count} w/l/v={it.wins}/{it.losses}/{it.voids} "
                f"pl_rub={it.total_profit_loss_rub} avg_rub={it.avg_profit_loss_rub}"
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


def _require_url_or_settings_default(message: Message) -> str | None:
    url = _parse_tail_arg(message)
    if url:
        return url
    settings = get_settings()
    return settings.provider_test_url


@router.message(Command("sanity_check"))
async def cmd_sanity_check(message: Message, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    if not _is_allowed(message):
        await _deny(message)
        return

    async with sessionmaker() as session:
        report = await SanityCheckService().run_sanity_check(session)

    shown = report.issues[:10]
    lines = [
        "SANITY CHECK",
        f"- total_signals: {report.total_signals}",
        f"- total_settlements: {report.total_settlements}",
        f"- total_failure_reviews: {report.total_failure_reviews}",
        f"- total_entries: {report.total_entries}",
        f"- issues_count: {report.issues_count}",
        "",
        "issues (first 10):",
    ]
    if not shown:
        lines.append("- none")
    else:
        for it in shown:
            sid = it.signal_id if it.signal_id is not None else "-"
            lines.append(f"- {it.issue_type} | signal_id={sid} | {it.details}")
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
        await message.answer("No signals found")
        return

    lines: list[str] = ["LATEST SIGNALS", ""]
    for s in signals:
        sport = getattr(s.sport, "value", s.sport)
        bookmaker = getattr(s.bookmaker, "value", s.bookmaker)
        status = getattr(s.status, "value", s.status)
        result = s.settlement.result.value if s.settlement is not None else "-"
        lines.append(
            " | ".join(
                [
                    f"#{s.id}",
                    str(sport),
                    str(bookmaker),
                    s.match_name,
                    s.market_type,
                    s.selection,
                    f"odds={s.odds_at_signal}",
                    f"status={status}",
                    f"result={result}",
                ]
            )
        )

    await message.answer("\n".join(lines))


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
            await message.answer("No settled signals found")
            return

        lines: list[str] = ["LATEST RESULTS", ""]
        for s in signals:
            sport = getattr(s.sport, "value", s.sport)
            bookmaker = getattr(s.bookmaker, "value", s.bookmaker)
            result = s.settlement.result.value if s.settlement is not None else "-"
            pl = s.settlement.profit_loss if s.settlement is not None else None

            quality_label = "-"
            try:
                qr = await SignalQualityService().build_signal_quality_report(session, int(s.id))
                quality_label = qr.metrics.quality_label or "-"
            except Exception:
                quality_label = "-"

            lines.append(
                " | ".join(
                    [
                        f"#{s.id}",
                        str(sport),
                        str(bookmaker),
                        s.match_name,
                        s.market_type,
                        s.selection,
                        f"result={result}",
                        f"pl={pl}",
                        f"quality={quality_label}",
                    ]
                )
            )

    await message.answer("\n".join(lines))


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
        await message.answer("No failed signals found")
        return

    lines: list[str] = ["LATEST FAILURES", ""]
    for s in signals:
        sport = getattr(s.sport, "value", s.sport)
        bookmaker = getattr(s.bookmaker, "value", s.bookmaker)
        result = s.settlement.result.value if s.settlement is not None else "-"

        category = "-"
        reason = "-"
        if getattr(s, "failure_reviews", None):
            r0 = s.failure_reviews[0]
            category = getattr(r0.category, "value", r0.category) if r0.category is not None else "-"
            reason = (r0.auto_reason or r0.manual_reason or "-")

        lines.append(
            " | ".join(
                [
                    f"#{s.id}",
                    str(sport),
                    str(bookmaker),
                    s.match_name,
                    s.market_type,
                    s.selection,
                    f"result={result}",
                    f"category={category}",
                    f"reason={reason}",
                ]
            )
        )

    await message.answer("\n".join(lines))


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
                "QUICK CHECK",
                f"- total_signals: {k.total_signals}",
                f"- settled_signals: {k.settled_signals}",
                f"- wins/losses/voids: {k.wins}/{k.losses}/{k.voids}",
                f"- unit_balance: {balance_unit.current_balance}",
                f"- rub_balance: {balance_rub.current_balance_rub}",
                f"- latest_ids: {latest_ids_str}",
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
        "SYSTEM STATUS",
        "",
        "Signals:",
        f"- total_signals: {k.total_signals}",
        f"- settled_signals: {k.settled_signals}",
        f"- entered_signals: {k.entered_signals}",
        f"- missed_signals: {k.missed_signals}",
        "",
        "Balance unit:",
        f"- current_balance: {balance_unit.current_balance}",
        f"- total_profit_loss_since_base: {balance_unit.total_profit_loss_since_base}",
        "",
        "Balance RUB:",
        f"- flat_stake_rub: {balance_rub.flat_stake_rub}",
        f"- current_balance_rub: {balance_rub.current_balance_rub}",
        f"- total_profit_loss_rub: {balance_rub.total_profit_loss_rub}",
        "",
        "Period unit:",
        f"- period_label: {period_unit.overview.period_label}",
        f"- current_balance: {period_unit.overview.current_balance}",
        "",
        "Period RUB:",
        f"- period_label: {period_rub.overview.period_label}",
        f"- current_balance_rub: {period_rub.overview.current_balance_rub}",
        "",
        "Quality:",
        f"- avg_prediction_error: {quality.avg_prediction_error}",
        f"- overestimated_count: {quality.overestimated_count}",
        f"- underestimated_count: {quality.underestimated_count}",
        "",
        "History:",
        f"- snapshots count: {len(history)}",
        f"- latest snapshot label: {latest_snapshot_label or 'none'}",
        "",
        "Latest signals:",
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
