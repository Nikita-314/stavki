from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import selectinload

from app.core.config import get_settings
from app.core.enums import BetResult, SportType
from app.db.models.signal import Signal
from app.db.models.settlement import Settlement
from app.db.repositories.signal_repository import SignalRepository
from app.schemas.event_result import EventResultInput
from app.services.api_football_service import ApiFootballFixtureLite, ApiFootballService
from app.services.result_ingestion_service import ResultIngestionService
from app.services.sportmonks_service import SportmonksFixtureLite, SportmonksService

logger = logging.getLogger(__name__)


@dataclass
class FootballFinishedResultLookup:
    source: str
    fixture_found: bool = False
    final_score_found: bool = False
    final_score: str | None = None
    match_status: str | None = None
    winner_selection: str | None = None
    is_void: bool = False
    reason: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass
class FootballFinishedBackfillReport:
    checked_signals: int = 0
    checked_events: int = 0
    fixture_found: int = 0
    final_score_found: int = 0
    settlement_created: int = 0
    win_count: int = 0
    lose_count: int = 0
    void_count: int = 0
    total_profit_loss: Decimal = Decimal("0")
    football_outcome_audit_count: int = 0
    openai_analysis_count: int = 0
    source_counts: dict[str, int] = field(default_factory=dict)
    unknown_reasons: dict[str, int] = field(default_factory=dict)
    rows: list[dict[str, Any]] = field(default_factory=list)


class FootballFinishedResultAutosettlementService:
    _SPORTMONKS_FINISHED_STATE_IDS = frozenset({5})
    _API_FOOTBALL_FINISHED_SHORT = frozenset({"FT", "AET", "PEN"})
    _API_FOOTBALL_VOID_SHORT = frozenset({"PST", "CANC", "ABD", "AWD", "WO"})

    async def audit_or_backfill_recent_unsettled(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        *,
        limit: int = 50,
        lookback_days: int = 3,
        older_than_hours: int = 4,
        dry_run: bool = True,
    ) -> FootballFinishedBackfillReport:
        signals = await self._load_recent_unsettled_signals(
            sessionmaker,
            limit=limit,
            lookback_days=lookback_days,
            older_than_hours=older_than_hours,
        )
        report = FootballFinishedBackfillReport(checked_signals=len(signals))
        if not signals:
            return report

        grouped: dict[str, list[Signal]] = {}
        for sig in signals:
            key = str(sig.event_external_id or f"signal:{sig.id}")
            grouped.setdefault(key, []).append(sig)
        report.checked_events = len(grouped)

        sportmonks_pool, api_football_pool = await self._build_fixture_pools(signals)
        result_ingestion = ResultIngestionService()

        for event_key, event_signals in grouped.items():
            rep_signal = event_signals[0]
            lookup = self._lookup_finished_result(rep_signal, sportmonks_pool=sportmonks_pool, api_football_pool=api_football_pool)
            if lookup.fixture_found:
                report.fixture_found += len(event_signals)
            if lookup.final_score_found:
                report.final_score_found += len(event_signals)
            report.source_counts[lookup.source] = int(report.source_counts.get(lookup.source, 0) or 0) + len(event_signals)
            if lookup.reason:
                report.unknown_reasons[lookup.reason] = int(report.unknown_reasons.get(lookup.reason, 0) or 0) + len(event_signals)

            for sig in event_signals:
                preview_result = result_ingestion._determine_result(
                    signal=sig,
                    is_void=lookup.is_void,
                    winner_selection=lookup.winner_selection,
                    result_payload_json=lookup.payload,
                )
                report.rows.append(
                    {
                        "signal_id": int(sig.id),
                        "match": sig.match_name,
                        "signaled_at": self._iso(sig.signaled_at),
                        "event_start_at": self._iso(sig.event_start_at),
                        "bet_text": f"{sig.market_label}: {sig.selection}",
                        "selection": sig.selection,
                        "odds": self._to_float(sig.odds_at_signal),
                        "source": lookup.source,
                        "fixture_found": lookup.fixture_found,
                        "final_score_found": lookup.final_score_found,
                        "final_score": lookup.final_score,
                        "match_status": lookup.match_status,
                        "computed_settlement": preview_result.value if preview_result is not None else "UNKNOWN",
                        "reason": lookup.reason,
                    }
                )

            if dry_run or not (lookup.final_score_found or lookup.is_void):
                continue

            async with sessionmaker() as session:
                res = await ResultIngestionService().process_event_result(
                    session,
                    EventResultInput(
                        event_external_id=str(rep_signal.event_external_id or ""),
                        sport=SportType.FOOTBALL,
                        winner_selection=lookup.winner_selection,
                        is_void=lookup.is_void,
                        settled_at=datetime.now(timezone.utc),
                        result_payload_json=lookup.payload,
                    ),
                )
                await session.commit()
                report.settlement_created += int(res.settled_signals)
                await self._collect_post_settlement_stats(session, res.processed_signal_ids, report)

        return report

    async def run_forever(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        *,
        interval_seconds: int = 15 * 60,
        limit: int = 50,
        lookback_days: int = 3,
        older_than_hours: int = 4,
    ) -> None:
        iv = max(300, int(interval_seconds))
        while True:
            try:
                rep = await self.audit_or_backfill_recent_unsettled(
                    sessionmaker,
                    limit=limit,
                    lookback_days=lookback_days,
                    older_than_hours=older_than_hours,
                    dry_run=False,
                )
                logger.info(
                    "[FOOTBALL][RESULT_BACKFILL] checked=%s events=%s fixture_found=%s score_found=%s settled=%s W=%s L=%s V=%s pl=%s reasons=%s",
                    rep.checked_signals,
                    rep.checked_events,
                    rep.fixture_found,
                    rep.final_score_found,
                    rep.settlement_created,
                    rep.win_count,
                    rep.lose_count,
                    rep.void_count,
                    float(rep.total_profit_loss),
                    rep.unknown_reasons,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[FOOTBALL][RESULT_BACKFILL] loop failed")
            await asyncio.sleep(iv)

    async def _load_recent_unsettled_signals(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        *,
        limit: int,
        lookback_days: int,
        older_than_hours: int,
    ) -> list[Signal]:
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=max(1, int(older_than_hours)))
        horizon = now - timedelta(days=max(1, int(lookback_days)))
        async with sessionmaker() as session:
            stmt = (
                select(Signal)
                .outerjoin(Settlement, Settlement.signal_id == Signal.id)
                .options(selectinload(Signal.prediction_logs), selectinload(Signal.settlement))
                .where(Signal.sport == SportType.FOOTBALL)
                .where(Signal.is_live.is_(True))
                .where(Signal.notes == "live_auto")
                .where(Signal.event_external_id.is_not(None))
                .where(Signal.event_start_at.is_not(None))
                .where(Signal.event_start_at >= horizon)
                .where(Signal.event_start_at <= cutoff)
                .where(Settlement.id.is_(None))
                .order_by(Signal.signaled_at.desc())
                .limit(int(limit))
            )
            rows = (await session.execute(stmt)).scalars().all()
        return list(rows)

    async def _build_fixture_pools(
        self,
        signals: list[Signal],
    ) -> tuple[list[SportmonksFixtureLite], list[ApiFootballFixtureLite]]:
        dates = sorted({sig.event_start_at.date().isoformat() for sig in signals if sig.event_start_at})
        if not dates:
            return [], []

        sm_pool: list[SportmonksFixtureLite] = []
        af_pool: list[ApiFootballFixtureLite] = []
        settings = get_settings()

        if bool(getattr(settings, "sportmonks_enabled", False)):
            svc_sm = SportmonksService()
            sm_pool = await asyncio.to_thread(
                svc_sm.get_fixtures_between,
                start_date=min(dates),
                end_date=max(dates),
            )

        if bool(getattr(settings, "api_football_enabled", False)):
            svc_af = ApiFootballService()
            date_rows = await asyncio.gather(
                *(asyncio.to_thread(svc_af.get_fixtures_by_date, d) for d in dates),
                return_exceptions=True,
            )
            for item in date_rows:
                if isinstance(item, list):
                    af_pool.extend(item)
        return sm_pool, af_pool

    def _lookup_finished_result(
        self,
        signal: Signal,
        *,
        sportmonks_pool: list[SportmonksFixtureLite],
        api_football_pool: list[ApiFootballFixtureLite],
    ) -> FootballFinishedResultLookup:
        sm = self._lookup_from_sportmonks(signal, sportmonks_pool)
        if sm.final_score_found or sm.is_void:
            return sm
        af = self._lookup_from_api_football(signal, api_football_pool)
        if af.final_score_found or af.is_void:
            return af
        if sm.fixture_found:
            return sm
        return af if af.fixture_found else sm

    def _lookup_from_sportmonks(
        self,
        signal: Signal,
        fixtures: list[SportmonksFixtureLite],
    ) -> FootballFinishedResultLookup:
        if not fixtures:
            return FootballFinishedResultLookup(source="sportmonks", reason="sportmonks_no_fixtures")
        fx = SportmonksService.map_winline_match_to_fixture_from_window(
            winline_home=str(signal.home_team or ""),
            winline_away=str(signal.away_team or ""),
            fixtures=fixtures,
        )
        if fx is None:
            return FootballFinishedResultLookup(source="sportmonks", reason="sportmonks_fixture_not_found")
        if not self._kickoff_matches(signal.event_start_at, fx.starting_at):
            return FootballFinishedResultLookup(
                source="sportmonks",
                fixture_found=False,
                reason="sportmonks_kickoff_mismatch",
            )
        status = fx.result_info or (f"state_id={fx.state_id}" if fx.state_id is not None else None)
        final_score_found = fx.score_home is not None and fx.score_away is not None
        scoreline = f"{fx.score_home}:{fx.score_away}" if final_score_found else None
        if self._sportmonks_is_void(fx):
            return FootballFinishedResultLookup(
                source="sportmonks",
                fixture_found=True,
                final_score_found=False,
                final_score=None,
                match_status=status,
                winner_selection=None,
                is_void=True,
                reason=None,
                payload=self._base_payload(signal, "sportmonks", fx.fixture_id, None, None, status),
            )
        if final_score_found and self._sportmonks_is_finished(fx):
            winner = self._winner_from_score(int(fx.score_home), int(fx.score_away))
            payload = self._base_payload(
                signal,
                "sportmonks",
                fx.fixture_id,
                int(fx.score_home),
                int(fx.score_away),
                status,
            )
            return FootballFinishedResultLookup(
                source="sportmonks",
                fixture_found=True,
                final_score_found=True,
                final_score=scoreline,
                match_status=status,
                winner_selection=winner,
                is_void=False,
                reason=None,
                payload=payload,
            )
        return FootballFinishedResultLookup(
            source="sportmonks",
            fixture_found=True,
            final_score_found=False,
            final_score=scoreline,
            match_status=status,
            reason="sportmonks_not_finished_or_no_final_score",
        )

    def _lookup_from_api_football(
        self,
        signal: Signal,
        fixtures: list[ApiFootballFixtureLite],
    ) -> FootballFinishedResultLookup:
        if not fixtures:
            return FootballFinishedResultLookup(source="api_football", reason="api_football_no_fixtures")
        fx = ApiFootballService.map_winline_match_to_fixture(
            winline_home=str(signal.home_team or ""),
            winline_away=str(signal.away_team or ""),
            fixtures=fixtures,
        )
        if fx is None:
            return FootballFinishedResultLookup(source="api_football", reason="api_football_fixture_not_found")
        if not self._kickoff_matches(signal.event_start_at, fx.starting_at):
            return FootballFinishedResultLookup(
                source="api_football",
                fixture_found=False,
                reason="api_football_kickoff_mismatch",
            )
        status = fx.status_short or fx.status_long
        final_score_found = fx.score_home is not None and fx.score_away is not None
        scoreline = f"{fx.score_home}:{fx.score_away}" if final_score_found else None
        if self._api_football_is_void(fx):
            return FootballFinishedResultLookup(
                source="api_football",
                fixture_found=True,
                final_score_found=False,
                final_score=None,
                match_status=status,
                is_void=True,
                payload=self._base_payload(signal, "api_football", fx.fixture_id, None, None, status),
            )
        if final_score_found and self._api_football_is_finished(fx):
            winner = self._winner_from_score(int(fx.score_home), int(fx.score_away))
            payload = self._base_payload(
                signal,
                "api_football",
                fx.fixture_id,
                int(fx.score_home),
                int(fx.score_away),
                status,
            )
            return FootballFinishedResultLookup(
                source="api_football",
                fixture_found=True,
                final_score_found=True,
                final_score=scoreline,
                match_status=status,
                winner_selection=winner,
                payload=payload,
            )
        return FootballFinishedResultLookup(
            source="api_football",
            fixture_found=True,
            final_score_found=False,
            final_score=scoreline,
            match_status=status,
            reason="api_football_not_finished_or_no_final_score",
        )

    async def _collect_post_settlement_stats(
        self,
        session: AsyncSession,
        signal_ids: list[int],
        report: FootballFinishedBackfillReport,
    ) -> None:
        repo = SignalRepository()
        for sid in signal_ids:
            sig = await repo.get_signal_full_graph(session, int(sid))
            if sig is None or sig.settlement is None:
                continue
            st = sig.settlement
            if st.result == BetResult.WIN:
                report.win_count += 1
            elif st.result == BetResult.LOSE:
                report.lose_count += 1
            elif st.result == BetResult.VOID:
                report.void_count += 1
            report.total_profit_loss += Decimal(st.profit_loss or 0)
            pl = self._pick_prediction_log(sig)
            snap = (pl.feature_snapshot_json or {}) if pl else {}
            if isinstance(snap.get("football_outcome_audit"), dict):
                report.football_outcome_audit_count += 1
            if isinstance(snap.get("openai_analysis"), dict):
                report.openai_analysis_count += 1

    def _pick_prediction_log(self, signal: Signal):
        logs = list(signal.prediction_logs or [])
        if not logs:
            return None
        logs.sort(key=lambda x: ((x.created_at.isoformat() if x.created_at else ""), int(x.id or 0)), reverse=True)
        return logs[0]

    def _sportmonks_is_finished(self, fx: SportmonksFixtureLite) -> bool:
        if fx.result_info:
            return True
        return bool(fx.state_id in self._SPORTMONKS_FINISHED_STATE_IDS)

    def _sportmonks_is_void(self, fx: SportmonksFixtureLite) -> bool:
        info = str(fx.result_info or "").lower()
        return any(token in info for token in ("cancel", "abandon", "postpon", "awarded", "walkover"))

    def _api_football_is_finished(self, fx: ApiFootballFixtureLite) -> bool:
        return str(fx.status_short or "").upper() in self._API_FOOTBALL_FINISHED_SHORT

    def _api_football_is_void(self, fx: ApiFootballFixtureLite) -> bool:
        return str(fx.status_short or "").upper() in self._API_FOOTBALL_VOID_SHORT

    def _winner_from_score(self, sh: int, sa: int) -> str:
        if sh > sa:
            return "home"
        if sa > sh:
            return "away"
        return "draw"

    def _base_payload(
        self,
        signal: Signal,
        source: str,
        fixture_id: int | None,
        score_home: int | None,
        score_away: int | None,
        match_status: str | None,
    ) -> dict[str, Any]:
        out = {
            "source": source,
            "fixture_id": fixture_id,
            "event_external_id": signal.event_external_id,
            "home_team": signal.home_team,
            "away_team": signal.away_team,
            "match_status": match_status,
            "score_home": score_home,
            "score_away": score_away,
            "final_scoreline": (
                f"{score_home}:{score_away}" if score_home is not None and score_away is not None else None
            ),
        }
        return out

    def _kickoff_matches(self, signal_start: datetime | None, fixture_start: str | None) -> bool:
        if signal_start is None or not fixture_start:
            return True
        fx_dt = self._parse_dt(fixture_start)
        if fx_dt is None:
            return True
        sig_dt = signal_start if signal_start.tzinfo is not None else signal_start.replace(tzinfo=timezone.utc)
        sig_dt = sig_dt.astimezone(timezone.utc)
        return abs((sig_dt - fx_dt).total_seconds()) <= 12 * 3600

    def _parse_dt(self, value: str) -> datetime | None:
        s = (value or "").strip()
        if not s:
            return None
        try:
            if "T" in s:
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            else:
                dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
                dt = dt.replace(tzinfo=timezone.utc)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None

    def _iso(self, value: datetime | None) -> str | None:
        if value is None:
            return None
        dt = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()

    def _to_float(self, value: object) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except Exception:
            return None
