from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from app.services.sportmonks_service import SportmonksService


def _safe_float(v: object) -> float | None:
    try:
        if v is None:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class TeamBaseline:
    team_id: int | None
    team_name: str
    played: int
    ppg: float | None
    avg_gf: float | None
    avg_ga: float | None
    score: float | None
    factors: dict[str, Any]


class FootballSportmonksBaselineService:
    """Baseline scoring from Sportmonks last fixtures (best-effort, cached)."""

    _CACHE: dict[str, tuple[float, TeamBaseline]] = {}
    _TTL_SECONDS: float = 15 * 60.0

    def build_team_baseline(self, team_name: str) -> TeamBaseline:
        key = (team_name or "").strip().lower()
        now = time.time()
        hit = self._CACHE.get(key)
        if hit and (now - hit[0]) <= self._TTL_SECONDS:
            return hit[1]

        sm = SportmonksService()
        tid = sm.find_team_id_by_name(team_name)
        summ = sm.get_team_last_fixtures_summary(tid, lookback_days=60, limit=5) if tid else None
        s = (summ or {}).get("summary") if isinstance(summ, dict) else {}
        played = int(s.get("played") or 0) if isinstance(s, dict) else 0
        ppg = _safe_float(s.get("ppg")) if isinstance(s, dict) else None
        avg_gf = _safe_float(s.get("avg_gf")) if isinstance(s, dict) else None
        avg_ga = _safe_float(s.get("avg_ga")) if isinstance(s, dict) else None

        # Simple weighted score in [0..1.5] range roughly (no magic):
        # - points-per-game (0..3) dominates
        # - goal diff proxy (avg_gf - avg_ga) small weight
        score = None
        if played >= 3 and ppg is not None:
            gd = 0.0
            if avg_gf is not None and avg_ga is not None:
                gd = float(avg_gf) - float(avg_ga)
            score = (ppg / 3.0) * 1.0 + max(-1.0, min(1.0, gd / 2.0)) * 0.25
        factors = {
            "team_id": tid,
            "played": played,
            "ppg": ppg,
            "avg_gf": avg_gf,
            "avg_ga": avg_ga,
        }
        tb = TeamBaseline(
            team_id=tid,
            team_name=team_name,
            played=played,
            ppg=ppg,
            avg_gf=avg_gf,
            avg_ga=avg_ga,
            score=score,
            factors=factors,
        )
        self._CACHE[key] = (now, tb)
        return tb

