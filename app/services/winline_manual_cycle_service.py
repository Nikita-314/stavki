"""Semi-real manual workflow: JSON files in `examples/manual_winline/` → ingestion → final → send → results."""

from __future__ import annotations

from typing import Any

from aiogram import Bot
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.schemas.event_result import EventResultInput
from app.services.adapter_ingestion_service import AdapterIngestionService
from app.services.balance_service import BalanceService
from app.services.result_ingestion_service import ResultIngestionService
from app.services.sanity_check_service import SanityCheckService
from app.services.winline_final_signal_service import WinlineFinalSignalService
from app.services.winline_manual_payload_service import WinlineManualPayloadService
from app.services.winline_settlement_demo_service import _map_sport, _parse_dt, normalize_winline_line_payload
from app.services.winline_signal_delivery_demo_service import WinlineSignalDeliveryDemoService


class WinlineManualCycleService:
    """Bridge: manual files → same adapters/services as demo settlement (no new ingestion core)."""

    def __init__(self) -> None:
        self._manual = WinlineManualPayloadService()
        self._final = WinlineFinalSignalService()
        self._delivery = WinlineSignalDeliveryDemoService()
        self._adapter = AdapterIngestionService()

    def _normalize_line_or_error(self, raw: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
        if "events" not in raw or "markets" not in raw:
            return None, "manual_line_payload_not_supported_shape"
        try:
            return normalize_winline_line_payload(raw), None
        except Exception as exc:  # noqa: BLE001
            return None, f"normalize_line: {exc!s}"

    def preview_manual_line(self) -> dict[str, Any]:
        summary = self._manual.preview_line_payload()
        raw, load_err = self._manual.load_line_payload()
        out: dict[str, Any] = {
            "line_preview": summary,
            "preview_candidates": summary.get("preview_candidates"),
            "final_signals_ready": None,
            "sendable_previews": None,
            "error": summary.get("error") or load_err,
        }
        if raw is None:
            return out

        normalized, nerr = self._normalize_line_or_error(raw)
        if normalized is None:
            out["error"] = nerr
            return out

        try:
            previews = self._final.build_previews_from_normalized_line_payload(normalized)
        except Exception as exc:  # noqa: BLE001
            out["error"] = f"final_previews: {exc!s}"
            return out

        out["final_signals_ready"] = sum(1 for p in previews if p.has_signal)
        out["sendable_previews"] = sum(1 for p in previews if p.has_signal and p.signal is not None)
        return out

    async def ingest_manual_line(self, sessionmaker: async_sessionmaker[AsyncSession]) -> dict[str, Any]:
        raw, load_err = self._manual.load_line_payload()
        if raw is None:
            return {
                "status": "skipped",
                "created_signals": 0,
                "skipped_candidates": 0,
                "created_signal_ids": [],
                "error": load_err or "no_payload",
            }

        normalized, nerr = self._normalize_line_or_error(raw)
        if normalized is None:
            return {
                "status": "manual_line_payload_not_supported_shape",
                "created_signals": 0,
                "skipped_candidates": 0,
                "created_signal_ids": [],
                "error": nerr,
            }

        try:
            async with sessionmaker() as session:
                _ar, ingest = await self._adapter.ingest_payload(session, normalized)
                await session.commit()
            return {
                "status": "ok",
                "created_signals": ingest.created_signals,
                "skipped_candidates": ingest.skipped_candidates,
                "created_signal_ids": list(ingest.created_signal_ids),
                "error": None,
            }
        except Exception as exc:  # noqa: BLE001
            return {
                "status": "error",
                "created_signals": 0,
                "skipped_candidates": 0,
                "created_signal_ids": [],
                "error": str(exc),
            }

    def preview_manual_result(self) -> dict[str, Any]:
        return {"result_preview": self._manual.preview_result_payload()}

    async def process_manual_result(self, sessionmaker: async_sessionmaker[AsyncSession]) -> dict[str, Any]:
        raw, load_err = self._manual.load_result_payload()
        if raw is None:
            return {
                "status": "skipped",
                "raw_results": 0,
                "processed_event_results": [],
                "settled_signal_ids": [],
                "wins": None,
                "losses": None,
                "voids": None,
                "current_balance_rub": None,
                "sanity_issues_count": None,
                "error": load_err or "no_payload",
            }

        rows = raw.get("results")
        if not isinstance(rows, list):
            return {
                "status": "manual_result_payload_not_supported_shape",
                "raw_results": 0,
                "processed_event_results": [],
                "settled_signal_ids": [],
                "wins": None,
                "losses": None,
                "voids": None,
                "current_balance_rub": None,
                "sanity_issues_count": None,
                "error": "results_not_a_list",
            }

        settled_all: list[int] = []
        processed: list[dict[str, Any]] = []
        last_err: str | None = None

        for row in rows:
            if not isinstance(row, dict):
                continue
            eid = row.get("event_external_id")
            if not eid:
                continue
            try:
                sport = _map_sport(row.get("sport"))
                inp = EventResultInput(
                    event_external_id=str(eid),
                    sport=sport,
                    winner_selection=row.get("winner_selection"),
                    is_void=bool(row.get("is_void", False)),
                    settled_at=_parse_dt(row.get("settled_at")),
                    result_payload_json=row,
                )
                async with sessionmaker() as session:
                    pr = await ResultIngestionService().process_event_result(session, inp)
                    await session.commit()
                    settled_all.extend(pr.processed_signal_ids)
                    processed.append(
                        {
                            "event_external_id": str(eid),
                            "total_signals_found": pr.total_signals_found,
                            "settled_signals": pr.settled_signals,
                            "skipped_signals": pr.skipped_signals,
                            "processed_signal_ids": list(pr.processed_signal_ids),
                        }
                    )
            except Exception as exc:  # noqa: BLE001
                last_err = f"result[{eid}]: {exc!s}"

        out: dict[str, Any] = {
            "status": "ok" if last_err is None else "partial",
            "raw_results": len(rows),
            "processed_event_results": processed,
            "settled_signal_ids": sorted(set(settled_all)),
            "wins": None,
            "losses": None,
            "voids": None,
            "current_balance_rub": None,
            "sanity_issues_count": None,
            "error": last_err,
        }

        try:
            async with sessionmaker() as session:
                realistic = await BalanceService().get_realistic_balance_overview(session)
                sanity = await SanityCheckService().run_sanity_check(session)
                out["wins"] = int(realistic.wins)
                out["losses"] = int(realistic.losses)
                out["voids"] = int(realistic.voids)
                out["current_balance_rub"] = realistic.current_balance_rub
                out["sanity_issues_count"] = len(sanity.issues)
        except Exception as exc:  # noqa: BLE001
            out["error"] = (out["error"] + "; " if out["error"] else "") + f"summary: {exc!s}"

        return out

    def build_manual_final_signals(self) -> dict[str, Any]:
        raw, load_err = self._manual.load_line_payload()
        if raw is None:
            return {
                "status": "skipped",
                "previews": [],
                "error": load_err or "no_payload",
            }
        normalized, nerr = self._normalize_line_or_error(raw)
        if normalized is None:
            return {
                "status": "manual_line_payload_not_supported_shape",
                "previews": [],
                "error": nerr,
            }
        try:
            previews = self._final.build_previews_from_normalized_line_payload(normalized)
        except Exception as exc:  # noqa: BLE001
            return {
                "status": "error",
                "previews": [],
                "error": str(exc),
            }
        return {
            "status": "ok",
            "previews": previews,
            "ready_count": sum(1 for p in previews if p.has_signal),
            "error": None,
        }

    async def send_manual_signals(self, bot: Bot) -> dict[str, Any]:
        built = self.build_manual_final_signals()
        if built.get("status") != "ok":
            return {
                "status": built.get("status") or "skipped",
                "sent": 0,
                "detail": built,
                "error": built.get("error"),
            }
        previews = built.get("previews") or []
        return await self._delivery.send_manual_previews(bot, previews)

    async def run_manual_full_cycle(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        bot: Bot | None = None,
        *,
        send_signals: bool = True,
    ) -> dict[str, Any]:
        errors: list[str] = []
        line_pv = self.preview_manual_line()
        if line_pv.get("error"):
            errors.append(str(line_pv["error"]))

        line_ing = await self.ingest_manual_line(sessionmaker)
        if line_ing.get("error"):
            errors.append(str(line_ing["error"]))
        elif line_ing.get("status") not in {"ok", "skipped"}:
            errors.append(str(line_ing.get("status")))

        fs = self.build_manual_final_signals()
        fs_compact = {
            "status": fs.get("status"),
            "ready_count": fs.get("ready_count"),
            "preview_count": len(fs.get("previews") or []),
            "error": fs.get("error"),
        }
        if fs.get("error"):
            errors.append(str(fs["error"]))

        send_result: dict[str, Any] | None = None
        if send_signals and bot is not None:
            send_result = await self.send_manual_signals(bot)
            if send_result.get("status") not in {"ok", "skipped_no_signal_chat"} and send_result.get(
                "message"
            ):
                errors.append(str(send_result.get("message")))
        elif send_signals and bot is None:
            send_result = {"status": "skipped_no_bot", "message": "bot not passed"}

        res_pv = self.preview_manual_result()
        res_proc = await self.process_manual_result(sessionmaker)
        if res_proc.get("error"):
            errors.append(str(res_proc["error"]))

        summary = {
            "line_ingest_created": line_ing.get("created_signals"),
            "final_ready": fs_compact.get("ready_count"),
            "messages_sent": (send_result or {}).get("sent"),
            "result_rows": res_proc.get("raw_results"),
            "settled_ids_count": len(res_proc.get("settled_signal_ids") or []),
            "balance_rub": res_proc.get("current_balance_rub"),
            "sanity_issues": res_proc.get("sanity_issues_count"),
        }

        return {
            "line_preview": line_pv,
            "line_ingest": line_ing,
            "final_signals": fs_compact,
            "send_result": send_result,
            "result_preview": res_pv,
            "result_processing": res_proc,
            "summary": summary,
            "errors": errors,
        }
