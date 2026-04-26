from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.enums import BetResult
from app.db.models.football_live_ranker_idea import FootballLiveRankerIdea
from app.services.api_football_service import ApiFootballFixtureLite, ApiFootballService


@dataclass
class FootballLiveRankerSettlementReport:
    checked: int = 0
    fixture_found: int = 0
    finished: int = 0
    settled: int = 0
    win: int = 0
    lose: int = 0
    void: int = 0
    unknown: int = 0
    total_profit_loss: Decimal = Decimal("0")


class FootballLiveRankerSettlementService:
    def __init__(self) -> None:
        self._api = ApiFootballService()

    async def settle_pending(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        *,
        limit: int = 500,
        dry_run: bool = True,
    ) -> FootballLiveRankerSettlementReport:
        rep = FootballLiveRankerSettlementReport()
        async with sessionmaker() as session:
            ideas = await self._load_pending(session, limit=limit)
        rep.checked = len(ideas)
        if not ideas:
            return rep

        dates = sorted({i.event_start_at.date().isoformat() for i in ideas if i.event_start_at is not None})
        fixtures: list[ApiFootballFixtureLite] = []
        if dates:
            rows = await asyncio.gather(*(asyncio.to_thread(self._api.get_fixtures_by_date, d) for d in dates), return_exceptions=True)
            for row in rows:
                if isinstance(row, list):
                    fixtures.extend(row)

        to_update: list[FootballLiveRankerIdea] = []
        for idea in ideas:
            lookup = self._lookup_fixture(idea, fixtures)
            if lookup is None:
                rep.unknown += 1
                continue
            rep.fixture_found += 1
            if not self._is_finished(lookup):
                rep.unknown += 1
                continue
            sh = lookup.score_home
            sa = lookup.score_away
            if sh is None or sa is None:
                rep.unknown += 1
                continue
            rep.finished += 1
            result = self._determine_result(idea, int(sh), int(sa))
            if result == BetResult.UNKNOWN:
                rep.unknown += 1
                continue
            idea.result = result.value
            idea.final_score_home = int(sh)
            idea.final_score_away = int(sa)
            idea.settled_at = datetime.now(timezone.utc)
            idea.result_payload_json = {
                "source": "api_football",
                "fixture_id": int(lookup.fixture_id),
                "status_short": lookup.status_short,
                "status_long": lookup.status_long,
                "score_home": int(sh),
                "score_away": int(sa),
            }
            idea.profit_loss = self._pl_for_result(result, idea.odds)
            to_update.append(idea)
            rep.settled += 1
            if result == BetResult.WIN:
                rep.win += 1
            elif result == BetResult.LOSE:
                rep.lose += 1
            elif result == BetResult.VOID:
                rep.void += 1
            rep.total_profit_loss += Decimal(idea.profit_loss or 0)

        if dry_run or not to_update:
            return rep
        async with sessionmaker() as session:
            for idea in to_update:
                session.add(idea)
            await session.commit()
        return rep

    async def _load_pending(self, session: AsyncSession, *, limit: int) -> list[FootballLiveRankerIdea]:
        stmt = (
            select(FootballLiveRankerIdea)
            .where(FootballLiveRankerIdea.result.is_(None))
            .order_by(FootballLiveRankerIdea.created_at.asc())
            .limit(max(1, int(limit)))
        )
        return list((await session.execute(stmt)).scalars().all())

    def _lookup_fixture(self, idea: FootballLiveRankerIdea, fixtures: list[ApiFootballFixtureLite]) -> ApiFootballFixtureLite | None:
        if not fixtures:
            return None
        return self._api.map_winline_match_to_fixture(
            winline_home=str(idea.home_team or ""),
            winline_away=str(idea.away_team or ""),
            fixtures=fixtures,
        )

    def _is_finished(self, fx: ApiFootballFixtureLite) -> bool:
        return str(fx.status_short or "").upper() in {"FT", "AET", "PEN"}

    def _determine_result(self, idea: FootballLiveRankerIdea, score_home: int, score_away: int) -> BetResult:
        market = str(idea.market or "").strip().lower()
        line = idea.line
        if line is None:
            return BetResult.UNKNOWN
        if market == "match_total_over":
            total = Decimal(score_home + score_away)
            if total > line:
                return BetResult.WIN
            if total == line:
                return BetResult.VOID
            return BetResult.LOSE
        if market == "team_total_over":
            if idea.team_side == "home":
                total = Decimal(score_home)
            elif idea.team_side == "away":
                total = Decimal(score_away)
            else:
                return BetResult.UNKNOWN
            if total > line:
                return BetResult.WIN
            if total == line:
                return BetResult.VOID
            return BetResult.LOSE
        if market == "1x2":
            side = str(idea.selection_side or "").lower()
            if score_home > score_away:
                winner = "home"
            elif score_away > score_home:
                winner = "away"
            else:
                winner = "draw"
            return BetResult.WIN if side == winner else BetResult.LOSE
        return BetResult.UNKNOWN

    def _pl_for_result(self, result: BetResult, odds: Decimal | None) -> Decimal:
        if result == BetResult.WIN:
            return (odds or Decimal("1")) - Decimal("1")
        if result == BetResult.LOSE:
            return Decimal("-1")
        return Decimal("0")
