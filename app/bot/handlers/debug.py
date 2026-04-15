from __future__ import annotations

from decimal import Decimal

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.bot.keyboards.debug import get_debug_keyboard
from app.core.config import get_settings
from app.services.analytics_service import AnalyticsService
from app.services.analytics_summary_service import AnalyticsSummaryService
from app.services.bootstrap_service import BootstrapService
from app.services.signal_quality_service import SignalQualityService
from app.services.signal_quality_summary_service import SignalQualitySummaryService


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

    settlement_result = report.settlement.result if report.settlement is not None else None
    await message.answer(
        "\n".join(
            [
                f"id: {report.signal.id}",
                f"sport: {report.signal.sport}",
                f"bookmaker: {report.signal.bookmaker}",
                f"match: {report.signal.match_name}",
                f"market_type: {report.signal.market_type}",
                f"selection: {report.signal.selection}",
                f"odds_at_signal: {report.signal.odds_at_signal}",
                f"min_entry_odds: {report.signal.min_entry_odds}",
                f"status: {report.signal.status}",
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
