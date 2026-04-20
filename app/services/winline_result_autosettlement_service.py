from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.enums import SportType
from app.schemas.event_result import EventResultInput
from app.services.result_ingestion_service import ResultIngestionService
from app.services.winline_manual_payload_service import WinlineManualPayloadService
from app.services.winline_raw_result_bridge_service import WinlineRawResultBridgeService

logger = logging.getLogger(__name__)


@dataclass
class WinlineAutoSettlementTick:
    attempted_rows: int = 0
    processed_events: int = 0
    total_signals_found: int = 0
    settled_signals: int = 0
    skipped_signals: int = 0
    created_failure_reviews: int = 0
    processed_signal_ids: list[int] | None = None
    error: str | None = None


class WinlineResultAutoSettlementService:
    """Background helper: process manual/ingested result payload → write settlements for matching signals.

    Minimal goal: make the settlement pipeline work for combat `notes='live_auto'` signals
    once event result rows exist (manual upload or any other producer writing result_payload.json).
    """

    def __init__(self) -> None:
        self._manual = WinlineManualPayloadService()
        self._bridge = WinlineRawResultBridgeService()

    def _map_sport(self, raw: Any) -> SportType | None:
        key = (str(raw) if raw is not None else "").strip().lower()
        if key in {"football", "soccer"}:
            return SportType.FOOTBALL
        if key == "cs2":
            return SportType.CS2
        if key in {"dota2", "dota 2"}:
            return SportType.DOTA2
        return None

    async def tick(self, sessionmaker: async_sessionmaker[AsyncSession]) -> WinlineAutoSettlementTick:
        raw, err = self._manual.load_result_payload()
        if raw is None:
            return WinlineAutoSettlementTick(error=err or "no_result_payload")

        try:
            normalized = self._bridge.normalize_raw_winline_result_payload(raw)
        except Exception as exc:  # noqa: BLE001
            return WinlineAutoSettlementTick(error=f"normalize_result_payload: {exc!s}")

        rows = list(normalized.get("results") or [])
        out = WinlineAutoSettlementTick(attempted_rows=len(rows), processed_signal_ids=[])
        if not rows:
            return out

        for row in rows:
            if not isinstance(row, dict):
                continue
            eid = row.get("event_external_id")
            sport = self._map_sport(row.get("sport"))
            if not eid or not sport:
                continue
            try:
                inp = EventResultInput(
                    event_external_id=str(eid),
                    sport=sport,
                    winner_selection=row.get("winner_selection"),
                    is_void=bool(row.get("is_void", False)),
                    settled_at=None,
                    result_payload_json=row,
                )
            except Exception as exc:  # noqa: BLE001
                logger.info("[SETTLEMENT][AUTO] skip bad result row eid=%s err=%s", eid, exc)
                continue

            async with sessionmaker() as session:
                res = await ResultIngestionService().process_event_result(session, inp)
                await session.commit()

            out.processed_events += 1
            out.total_signals_found += int(res.total_signals_found)
            out.settled_signals += int(res.settled_signals)
            out.skipped_signals += int(res.skipped_signals)
            out.created_failure_reviews += int(res.created_failure_reviews)
            out.processed_signal_ids.extend(list(res.processed_signal_ids))

        return out

    async def run_forever(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        *,
        interval_seconds: int = 120,
    ) -> None:
        """Never-ending loop, safe to run as a background task."""
        iv = max(30, int(interval_seconds))
        while True:
            try:
                t = await self.tick(sessionmaker)
                logger.info(
                    "[SETTLEMENT][AUTO] tick rows=%s events=%s found=%s settled=%s skipped=%s err=%s",
                    t.attempted_rows,
                    t.processed_events,
                    t.total_signals_found,
                    t.settled_signals,
                    t.skipped_signals,
                    t.error,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[SETTLEMENT][AUTO] tick failed")
            await asyncio.sleep(iv)

