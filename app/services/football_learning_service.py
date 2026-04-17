from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.enums import BetResult, SportType
from app.db.models.settlement import Settlement
from app.db.models.signal import Signal
from app.schemas.provider_models import ProviderMatch, ProviderOddsMarket, ProviderSignalCandidate
from app.services.football_signal_send_filter_service import FootballSignalSendFilterService


@dataclass(frozen=True)
class FootballLearningAggregate:
    family: str
    sample_size: int
    wins: int
    losses: int
    win_rate: float | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "family": self.family,
            "sample_size": self.sample_size,
            "wins": self.wins,
            "losses": self.losses,
            "win_rate": self.win_rate,
        }


class FootballLearningService:
    """Reads settled football history from DB; emits conservative multipliers (no ML)."""

    _MIN_SAMPLES = 8
    _MAX_ADJUST = 0.06

    def __init__(self) -> None:
        self._family = FootballSignalSendFilterService()

    async def compute_family_multipliers(
        self,
        session: AsyncSession,
        *,
        lookback: int = 250,
    ) -> tuple[dict[str, float], list[FootballLearningAggregate]]:
        """Returns (multiplier_by_family, aggregates for logging)."""
        stmt = (
            select(Signal, Settlement)
            .join(Settlement, Settlement.signal_id == Signal.id)
            .where(Signal.sport == SportType.FOOTBALL)
            .where(Settlement.result.in_([BetResult.WIN, BetResult.LOSE]))
            .options(selectinload(Signal.prediction_logs))
            .order_by(Settlement.id.desc())
            .limit(int(lookback))
        )
        result = await session.execute(stmt)
        rows = list(result.all())

        bucket_wins: dict[str, int] = defaultdict(int)
        bucket_losses: dict[str, int] = defaultdict(int)

        for signal, st in rows:
            if st.result not in (BetResult.WIN, BetResult.LOSE):
                continue
            cand = self._candidate_from_signal(signal)
            fam = self._family.get_market_family(cand)
            if st.result == BetResult.WIN:
                bucket_wins[fam] += 1
            else:
                bucket_losses[fam] += 1

        aggregates: list[FootballLearningAggregate] = []
        multipliers: dict[str, float] = {}
        all_families = set(bucket_wins) | set(bucket_losses)
        for fam in sorted(all_families):
            w = bucket_wins.get(fam, 0)
            l = bucket_losses.get(fam, 0)
            n = w + l
            rate = (w / n) if n else None
            aggregates.append(FootballLearningAggregate(family=fam, sample_size=n, wins=w, losses=l, win_rate=rate))
            multipliers[fam] = self._multiplier_from_rate(rate, n)

        return multipliers, aggregates

    def multiplier_for_family(self, multipliers: dict[str, float], family: str) -> float:
        return float(multipliers.get(family, 1.0))

    def _multiplier_from_rate(self, win_rate: float | None, n: int) -> float:
        if win_rate is None or n < self._MIN_SAMPLES:
            return 1.0
        # Centered at 0.5: slightly down if historically weak, slightly up if strong.
        delta = (win_rate - 0.5) * 0.18
        delta = max(-self._MAX_ADJUST, min(self._MAX_ADJUST, delta))
        if math.isnan(delta):
            return 1.0
        return round(1.0 + delta, 6)

    def _candidate_from_signal(self, signal: Signal) -> ProviderSignalCandidate:
        snap: dict[str, Any] = {}
        if signal.prediction_logs:
            snap = dict(signal.prediction_logs[0].feature_snapshot_json or {})
        match = ProviderMatch(
            external_event_id=str(signal.event_external_id or ""),
            sport=SportType.FOOTBALL,
            tournament_name=signal.tournament_name,
            match_name=signal.match_name,
            home_team=signal.home_team,
            away_team=signal.away_team,
            event_start_at=signal.event_start_at,
            is_live=bool(signal.is_live),
            source_name="db_history",
        )
        market = ProviderOddsMarket(
            bookmaker=signal.bookmaker,
            market_type=signal.market_type,
            market_label=signal.market_label,
            selection=signal.selection,
            odds_value=signal.odds_at_signal,
            section_name=signal.section_name,
            subsection_name=signal.subsection_name,
        )
        return ProviderSignalCandidate(
            match=match,
            market=market,
            min_entry_odds=signal.min_entry_odds,
            predicted_prob=signal.predicted_prob,
            implied_prob=signal.implied_prob,
            edge=signal.edge,
            model_name=signal.model_name,
            model_version_name=signal.model_version_name,
            signal_score=signal.signal_score,
            feature_snapshot_json=snap,
        )
