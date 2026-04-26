from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.enums import BetResult
from app.db.models.football_live_probability_idea import FootballLiveProbabilityIdea
from app.services.api_football_service import ApiFootballFixtureLite, ApiFootballService


@dataclass
class FootballLiveProbabilityIdeasSettlementReport:
    checked: int = 0
    fixture_found: int = 0
    finished: int = 0
    settled: int = 0
    win: int = 0
    lose: int = 0
    void: int = 0
    unknown: int = 0
    total_profit_loss: Decimal = Decimal("0")


@dataclass
class FootballLiveProbabilityIdeasStatus:
    total: int = 0
    unsettled: int = 0
    settled: int = 0
    win: int = 0
    lose: int = 0
    void: int = 0
    total_profit_loss: Decimal = Decimal("0")
    roi: Decimal = Decimal("0")
    market_breakdown: list[dict[str, object]] | None = None


class FootballLiveProbabilityIdeasSettlementService:
    def __init__(self) -> None:
        self._api = ApiFootballService()

    async def settle_pending(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        *,
        limit: int = 500,
        dry_run: bool = True,
    ) -> FootballLiveProbabilityIdeasSettlementReport:
        rep = FootballLiveProbabilityIdeasSettlementReport()
        async with sessionmaker() as session:
            ideas = await self._load_pending(session, limit=limit)
        rep.checked = len(ideas)
        if not ideas:
            return rep

        dates = sorted({i.created_at.date().isoformat() for i in ideas if i.created_at is not None})
        fixtures: list[ApiFootballFixtureLite] = []
        if dates:
            rows = await asyncio.gather(*(asyncio.to_thread(self._api.get_fixtures_by_date, d) for d in dates), return_exceptions=True)
            for row in rows:
                if isinstance(row, list):
                    fixtures.extend(row)

        to_update: list[FootballLiveProbabilityIdea] = []
        for idea in ideas:
            lookup = self._lookup_fixture(idea, fixtures)
            if lookup is None:
                idea.settlement_note = "fixture_not_found"
                rep.unknown += 1
                continue
            rep.fixture_found += 1
            if not self._is_finished(lookup):
                idea.settlement_note = f"fixture_not_finished:{lookup.status_short or '-'}"
                rep.unknown += 1
                continue
            sh = lookup.score_home
            sa = lookup.score_away
            if sh is None or sa is None:
                idea.settlement_note = "score_missing"
                rep.unknown += 1
                continue
            rep.finished += 1
            result = self._determine_result(idea, int(sh), int(sa))
            if result == BetResult.UNKNOWN:
                idea.settlement_note = "unknown_market_or_selection"
                rep.unknown += 1
                continue

            idea.result = result.value
            idea.final_score_home = int(sh)
            idea.final_score_away = int(sa)
            idea.settled_at = datetime.now(timezone.utc)
            idea.settlement_note = "settled_api_football"
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

    async def status(self, sessionmaker: async_sessionmaker[AsyncSession]) -> FootballLiveProbabilityIdeasStatus:
        out = FootballLiveProbabilityIdeasStatus(market_breakdown=[])
        async with sessionmaker() as session:
            out.total = int(
                await session.scalar(select(func.count()).select_from(FootballLiveProbabilityIdea))
                or 0
            )
            out.unsettled = int(
                await session.scalar(
                    select(func.count()).select_from(FootballLiveProbabilityIdea).where(FootballLiveProbabilityIdea.result.is_(None))
                )
                or 0
            )
            out.settled = max(0, out.total - out.unsettled)
            out.win = int(
                await session.scalar(
                    select(func.count()).select_from(FootballLiveProbabilityIdea).where(FootballLiveProbabilityIdea.result == BetResult.WIN.value)
                )
                or 0
            )
            out.lose = int(
                await session.scalar(
                    select(func.count()).select_from(FootballLiveProbabilityIdea).where(FootballLiveProbabilityIdea.result == BetResult.LOSE.value)
                )
                or 0
            )
            out.void = int(
                await session.scalar(
                    select(func.count()).select_from(FootballLiveProbabilityIdea).where(FootballLiveProbabilityIdea.result == BetResult.VOID.value)
                )
                or 0
            )
            out.total_profit_loss = Decimal(
                await session.scalar(
                    select(func.coalesce(func.sum(FootballLiveProbabilityIdea.profit_loss), 0)).where(FootballLiveProbabilityIdea.result.is_not(None))
                )
                or 0
            )
            base = Decimal(out.win + out.lose)
            out.roi = (out.total_profit_loss / base) if base > 0 else Decimal("0")
            rows = (
                await session.execute(
                    select(
                        FootballLiveProbabilityIdea.market,
                        func.count().label("cnt"),
                        func.coalesce(func.sum(FootballLiveProbabilityIdea.profit_loss), 0).label("pl"),
                    )
                    .where(FootballLiveProbabilityIdea.result.is_not(None))
                    .group_by(FootballLiveProbabilityIdea.market)
                    .order_by(func.count().desc())
                )
            ).all()
            out.market_breakdown = [{"market": str(m), "count": int(c or 0), "profit_loss": str(pl or 0)} for m, c, pl in rows]
        return out

    async def sample_latest(self, sessionmaker: async_sessionmaker[AsyncSession], *, limit: int = 3) -> list[FootballLiveProbabilityIdea]:
        async with sessionmaker() as session:
            stmt = select(FootballLiveProbabilityIdea).order_by(FootballLiveProbabilityIdea.created_at.desc()).limit(max(1, int(limit)))
            return list((await session.execute(stmt)).scalars().all())

    async def _load_pending(self, session: AsyncSession, *, limit: int) -> list[FootballLiveProbabilityIdea]:
        stmt = (
            select(FootballLiveProbabilityIdea)
            .where(FootballLiveProbabilityIdea.result.is_(None))
            .order_by(FootballLiveProbabilityIdea.created_at.asc())
            .limit(max(1, int(limit)))
        )
        return list((await session.execute(stmt)).scalars().all())

    def _lookup_fixture(self, idea: FootballLiveProbabilityIdea, fixtures: list[ApiFootballFixtureLite]) -> ApiFootballFixtureLite | None:
        if not fixtures:
            return None
        return self._api.map_winline_match_to_fixture(
            winline_home=str(idea.home_team or ""),
            winline_away=str(idea.away_team or ""),
            fixtures=fixtures,
        )

    def _is_finished(self, fx: ApiFootballFixtureLite) -> bool:
        return str(fx.status_short or "").upper() in {"FT", "AET", "PEN"}

    def _determine_result(self, idea: FootballLiveProbabilityIdea, score_home: int, score_away: int) -> BetResult:
        market = str(idea.market or "").strip().lower()
        line = idea.line
        if market == "match_total_over":
            if line is None:
                return BetResult.UNKNOWN
            total = Decimal(score_home + score_away)
            if total > line:
                return BetResult.WIN
            if total == line:
                return BetResult.VOID
            return BetResult.LOSE
        if market == "team_total_over":
            if line is None:
                return BetResult.UNKNOWN
            side = self._team_side_from_selection(idea.selection, str(idea.home_team or ""), str(idea.away_team or ""))
            if side == "home":
                team_total = Decimal(score_home)
            elif side == "away":
                team_total = Decimal(score_away)
            else:
                return BetResult.UNKNOWN
            if team_total > line:
                return BetResult.WIN
            if team_total == line:
                return BetResult.VOID
            return BetResult.LOSE
        if market == "ft_1x2" or market == "1x2":
            side = self._selection_side_1x2(idea.selection)
            if not side:
                return BetResult.UNKNOWN
            winner = "draw"
            if score_home > score_away:
                winner = "home"
            elif score_away > score_home:
                winner = "away"
            return BetResult.WIN if side == winner else BetResult.LOSE
        return BetResult.UNKNOWN

    def _selection_side_1x2(self, selection: str | None) -> str | None:
        s = str(selection or "").lower().strip().replace("х", "x")
        if "п1" in s or s.startswith("1") or "home" in s:
            return "home"
        if "п2" in s or s.startswith("2") or "away" in s:
            return "away"
        if "нич" in s or "draw" in s or s == "x":
            return "draw"
        return None

    def _team_side_from_selection(self, selection: str | None, home: str, away: str) -> str | None:
        s = str(selection or "").lower()
        home_l = home.strip().lower()
        away_l = away.strip().lower()
        if home_l and home_l in s:
            return "home"
        if away_l and away_l in s:
            return "away"
        # fallback for IT1/IT2 notation
        if re.search(r"\bит1\b", s):
            return "home"
        if re.search(r"\bит2\b", s):
            return "away"
        return None

    def _pl_for_result(self, result: BetResult, odds: Decimal | None) -> Decimal:
        if result == BetResult.WIN:
            return (odds or Decimal("1")) - Decimal("1")
        if result == BetResult.LOSE:
            return Decimal("-1")
        return Decimal("0")
