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
from app.services.winline_manual_file_storage_service import WinlineManualFileStorageService
from app.services.winline_manual_payload_service import WinlineManualPayloadService
from app.services.winline_raw_line_bridge_service import WinlineRawLineBridgeService
from app.services.winline_raw_result_bridge_service import WinlineRawResultBridgeService
from app.services.winline_settlement_demo_service import _map_sport, _parse_dt
from app.services.winline_signal_delivery_demo_service import WinlineSignalDeliveryDemoService


class WinlineManualCycleService:
    """Bridge: manual files → same adapters/services as demo settlement (no new ingestion core)."""

    def __init__(self) -> None:
        self._manual = WinlineManualPayloadService()
        self._storage = WinlineManualFileStorageService()
        self._bridge = WinlineRawLineBridgeService()
        self._result_bridge = WinlineRawResultBridgeService()
        self._final = WinlineFinalSignalService()
        self._delivery = WinlineSignalDeliveryDemoService()
        self._adapter = AdapterIngestionService()

    def _has_readable_line_payload(self) -> bool:
        raw, err = self._manual.load_line_payload()
        return raw is not None and err is None

    def _has_readable_result_payload(self) -> bool:
        raw, err = self._manual.load_result_payload()
        if raw is None or err:
            return False
        try:
            normalized = self._result_bridge.normalize_raw_winline_result_payload(raw)
        except Exception:
            return False
        return isinstance(normalized.get("results"), list) and bool(normalized.get("results"))

    def get_operator_readiness(self) -> dict[str, Any]:
        """Флаги для операторского UI: что уже можно делать с файлами."""
        st = self._storage.get_file_status()
        lp = self._manual.preview_line_payload()
        rp = self._manual.preview_result_payload()

        line_readable = bool(st.get("line_exists") and st.get("line_readable"))
        result_readable = bool(st.get("result_exists") and st.get("result_readable"))

        line_ready_for_preview = line_readable
        line_ready_for_ingest = bool(line_readable and bool(lp.get("ingestible_shape")))

        result_ready_for_preview = result_readable
        raw_r, _e = self._manual.load_result_payload()
        result_ready_for_process = bool(
            result_readable and raw_r is not None and isinstance(raw_r.get("results"), list)
        )

        has_line = self._has_readable_line_payload()
        has_result = self._has_readable_result_payload()

        if not has_line and not has_result:
            mode = "nothing"
            rec = "Загрузите line и/или result JSON (кнопки upload или файлы на сервере)."
        elif has_line and not has_result:
            mode = "line_only"
            rec = "Сделайте ingest line, затем загрузите result JSON или используйте run ready."
        elif has_result and not has_line:
            mode = "result_only"
            rec = "Обработайте result или загрузите line JSON для полного цикла."
        else:
            mode = "full"
            rec = "Можно Winline manual full или Winline run ready."

        return {
            "mode": mode,
            "has_line": has_line,
            "has_result": has_result,
            "line_ready_for_preview": line_ready_for_preview,
            "line_ready_for_ingest": line_ready_for_ingest,
            "result_ready_for_preview": result_ready_for_preview,
            "result_ready_for_process": result_ready_for_process,
            "recommended_next_action": rec,
            "storage": st,
            "line_preview_meta": lp,
            "result_preview_meta": rp,
        }

    def next_step_hint(self, after: str) -> str:
        """Одна короткая строка подсказки после manual-команды."""
        r = self.get_operator_readiness()
        hl, hr = r["has_line"], r["has_result"]
        ing = r["line_ready_for_ingest"]
        rp_ok = r["result_ready_for_process"]

        if after == "line_preview":
            if ing and not hr:
                return "Следующий шаг: Winline manual ingest или загрузите result JSON."
            if hr:
                return "Следующий шаг: Winline manual full или обработайте result."
            return "Следующий шаг: исправьте line JSON или загрузите заново."

        if after == "line_ingest":
            if not hr:
                return "Следующий шаг: загрузите result JSON, затем process или full."
            return "Следующий шаг: Winline manual process или Winline manual full."

        if after == "result_preview":
            if rp_ok and not hl:
                return "Следующий шаг: Winline manual process или загрузите line JSON."
            if hl and hr:
                return "Следующий шаг: Winline manual process или full cycle."
            return "Следующий шаг: проверьте result JSON или загрузите заново."

        if after == "result_process":
            if hl and hr:
                return "Следующий шаг: при необходимости Winline manual full или проверьте баланс."
            if not hl:
                return "Следующий шаг: загрузите line JSON для связки с сигналами."
            return "Следующий шаг: загрузите result или проверьте статус файлов."

        if after == "full_cycle":
            return "Следующий шаг: при новых JSON — upload; иначе Winline file status."

        if after == "run_ready":
            return "Следующий шаг: смотрите итог выше или Winline file status."

        return "Следующий шаг: Winline file status или загрузите JSON."

    def _normalize_line_or_error(self, raw: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
        try:
            normalized = self._bridge.normalize_raw_winline_line_payload(raw)
            if not (normalized.get("events") or []):
                return None, "no_valid_events"
            if not (normalized.get("markets") or []):
                return None, "no_supported_markets"
            return normalized, None
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

        try:
            normalized = self._result_bridge.normalize_raw_winline_result_payload(raw)
        except Exception as exc:
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
                "error": f"normalize_result: {exc!s}",
            }
        rows = normalized.get("results")
        if not isinstance(rows, list) or not rows:
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
                "error": "no_normalized_results",
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

    async def run_ready_cycle(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        bot: Bot | None = None,
    ) -> dict[str, Any]:
        """Выбрать сценарий по загруженным файлам и выполнить максимально полезный шаг."""
        r = self.get_operator_readiness()
        mode = str(r.get("mode") or "nothing")
        errors: list[str] = []

        base: dict[str, Any] = {
            "mode": mode,
            "line_preview": None,
            "line_ingest": None,
            "result_preview": None,
            "result_processing": None,
            "send_result": None,
            "full_cycle": None,
            "errors": errors,
            "message": None,
        }

        if mode == "nothing":
            base["message"] = "Файлы не загружены. Сначала загрузите line и/или result JSON."
            return base

        if mode == "line_only":
            lp = self.preview_manual_line()
            base["line_preview"] = lp
            if lp.get("error"):
                errors.append(str(lp["error"]))
            li = await self.ingest_manual_line(sessionmaker)
            base["line_ingest"] = li
            if li.get("error"):
                errors.append(str(li["error"]))
            base["errors"] = errors
            return base

        if mode == "result_only":
            rp = self.preview_manual_result()
            base["result_preview"] = rp
            pr = await self.process_manual_result(sessionmaker)
            base["result_processing"] = pr
            if pr.get("error"):
                errors.append(str(pr["error"]))
            base["errors"] = errors
            return base

        # full — оба файла
        full = await self.run_manual_full_cycle(sessionmaker, bot, send_signals=True)
        base["full_cycle"] = full
        base["line_preview"] = full.get("line_preview")
        base["line_ingest"] = full.get("line_ingest")
        base["result_preview"] = full.get("result_preview")
        base["result_processing"] = full.get("result_processing")
        base["send_result"] = full.get("send_result")
        base["errors"] = list(full.get("errors") or [])
        return base
