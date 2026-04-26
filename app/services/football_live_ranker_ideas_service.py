from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from app.core.config import get_settings
from app.db.models.football_live_ranker_idea import FootballLiveRankerIdea
from app.db.session import get_sessionmaker

logger = logging.getLogger(__name__)

_SESSIONMAKER: async_sessionmaker | None = None


def _get_sessionmaker() -> async_sessionmaker:
    global _SESSIONMAKER
    if _SESSIONMAKER is None:
        settings = get_settings()
        _SESSIONMAKER = get_sessionmaker(settings.database_url)
    return _SESSIONMAKER


class FootballLiveRankerIdeasService:
    def schedule_persist_preview(self, rows: list[dict[str, Any]]) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        payload = [dict(r) for r in rows]
        loop.create_task(self.persist_preview_rows(payload))

    async def persist_preview_rows(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        ideas = [self._to_model(row) for row in rows if str(row.get("preview_bucket") or "") in {"eligible", "watchlist"}]
        if not ideas:
            return 0
        sm = _get_sessionmaker()
        try:
            async with sm() as session:
                session.add_all(ideas)
                await session.commit()
            return len(ideas)
        except Exception:
            logger.exception("[FOOTBALL][S12_IDEAS] failed to persist preview rows")
            return 0

    def _to_model(self, row: dict[str, Any]) -> FootballLiveRankerIdea:
        return FootballLiveRankerIdea(
            preview_run_at=datetime.now(timezone.utc),
            event_id=str(row.get("event_id") or ""),
            fixture_id=self._int(row.get("fixture_id")),
            event_start_at=self._dt(row.get("event_start_at")),
            match_name=str(row.get("match") or ""),
            home_team=self._str(row.get("home_team")),
            away_team=self._str(row.get("away_team")),
            minute=self._int(row.get("minute")),
            score_home=self._int(row.get("score_home")),
            score_away=self._int(row.get("score_away")),
            market=str(row.get("market") or ""),
            selection=str(row.get("selection") or ""),
            line=self._decimal(row.get("line")),
            odds=self._decimal(row.get("odds")),
            goals_needed_to_win=self._int(row.get("goals_needed_to_win")),
            team_side=self._str(row.get("team_side")),
            selection_side=self._str(row.get("selection_side")),
            bucket=str(row.get("preview_bucket") or "watchlist"),
            risk_level=str(row.get("risk_level") or "high"),
            api_intelligence_available=bool(row.get("api_intelligence")),
        )

    def _int(self, value: object) -> int | None:
        if value is None or isinstance(value, bool):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _decimal(self, value: object) -> Decimal | None:
        if value is None or isinstance(value, bool):
            return None
        try:
            return Decimal(str(value).replace(",", "."))
        except Exception:
            return None

    def _dt(self, value: object) -> datetime | None:
        if not isinstance(value, str) or not value.strip():
            return None
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None

    def _str(self, value: object) -> str | None:
        if value is None:
            return None
        s = str(value).strip()
        return s or None
