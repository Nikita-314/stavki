from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from app.core.config import get_settings
from app.db.models.football_live_probability_idea import FootballLiveProbabilityIdea
from app.db.session import get_sessionmaker

logger = logging.getLogger(__name__)

_SESSIONMAKER: async_sessionmaker | None = None


def _get_sessionmaker() -> async_sessionmaker:
    global _SESSIONMAKER
    if _SESSIONMAKER is None:
        settings = get_settings()
        _SESSIONMAKER = get_sessionmaker(settings.database_url)
    return _SESSIONMAKER


class FootballLiveProbabilityIdeasService:
    def schedule_persist_usable(self, rows: list[dict[str, Any]]) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        payload = [dict(r) for r in rows]
        loop.create_task(self.persist_usable_rows(payload))

    async def persist_usable_rows(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        ideas = [self._to_model(r) for r in rows if bool(r.get("is_usable")) and str(r.get("best_bet") or "").strip()]
        if not ideas:
            return 0
        sm = _get_sessionmaker()
        try:
            async with sm() as session:
                session.add_all(ideas)
                await session.commit()
            return len(ideas)
        except Exception:
            logger.exception("[FOOTBALL][S13_IDEAS] failed to persist usable rows")
            return 0

    def _to_model(self, row: dict[str, Any]) -> FootballLiveProbabilityIdea:
        return FootballLiveProbabilityIdea(
            event_id=str(row.get("event_id") or ""),
            fixture_id=self._int(row.get("fixture_id")),
            match_name=str(row.get("match") or ""),
            home_team=self._str(row.get("home")),
            away_team=self._str(row.get("away")),
            minute=self._int(row.get("minute")),
            score_home=self._int(row.get("score_home")),
            score_away=self._int(row.get("score_away")),
            market=str(row.get("bet_kind") or ""),
            selection=str(row.get("best_bet") or ""),
            line=self._decimal(row.get("line")),
            odds=self._decimal(row.get("best_bet_odds")),
            implied_probability=self._decimal(row.get("implied_probability")),
            model_probability=self._decimal(row.get("model_probability")),
            value_edge=self._decimal(row.get("value_edge")),
            confidence_score=self._int(row.get("confidence_score")),
            risk_level=str(row.get("risk_level") or "high"),
            api_intelligence_available=bool(row.get("api_intelligence_available")),
            reasons_json=(row.get("reasons") if isinstance(row.get("reasons"), list) else None),
            missing_data_json=(row.get("missing_data") if isinstance(row.get("missing_data"), list) else None),
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

    def _str(self, value: object) -> str | None:
        if value is None:
            return None
        s = str(value).strip()
        return s or None
