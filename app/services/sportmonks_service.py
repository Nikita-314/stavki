from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import httpx

from app.core.config import get_settings

logger = logging.getLogger(__name__)


def _norm_team(s: str) -> str:
    return (s or "").strip().lower().replace("ё", "е")


def _safe_int(v: object) -> int | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class SportmonksFixtureLite:
    fixture_id: int
    home_team: str
    away_team: str
    home_team_id: int | None
    away_team_id: int | None
    minute: int | None
    score_home: int | None
    score_away: int | None


@dataclass(frozen=True)
class SportmonksFixtureStats:
    fixture_id: int
    shots_total: int | None
    shots_on_target: int | None
    corners: int | None
    cards_total: int | None
    raw: dict[str, Any] | None = None


class SportmonksService:
    """Minimal Sportmonks Football API v3 client (read-only, required fields only)."""

    BASE_URL = "https://api.sportmonks.com/v3/football"

    # Common statistic type ids from Sportmonks docs (API v3):
    _TYPE_CORNERS = 34
    _TYPE_SHOTS_ON_TARGET = 86
    _TYPE_SHOTS_OFF_TARGET = 41
    # Cards are often split; we keep best-effort sum if present.
    _TYPE_YELLOW_CARDS = 2
    _TYPE_RED_CARDS = 3

    def __init__(self) -> None:
        s = get_settings()
        self._token = (s.sportmonks_api_key or "").strip()
        self._timeout = 12.0

    def _client(self) -> httpx.Client:
        return httpx.Client(timeout=self._timeout, headers={"User-Agent": "stavki-bot/football-live"})

    def _require_token(self) -> bool:
        if not self._token:
            logger.info("[SPORTMONKS] api key missing (SPORTMONKS_API_KEY not set)")
            return False
        return True

    def get_live_fixtures(self) -> list[SportmonksFixtureLite]:
        """Returns in-play fixtures (best-effort)."""
        if not self._require_token():
            return []
        url = f"{self.BASE_URL}/livescores/inplay"
        params = {
            "api_token": self._token,
            # Participants include team names/ids
            "include": "participants;scores",
        }
        try:
            with self._client() as c:
                r = c.get(url, params=params)
                r.raise_for_status()
                payload = r.json()
        except Exception as e:
            logger.info("[SPORTMONKS] live fixtures fetch failed: %s", e)
            return []

        if isinstance(payload, dict) and payload.get("message") and not payload.get("data"):
            logger.info("[SPORTMONKS] inplay empty: %s", str(payload.get("message"))[:280])
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, list):
            return []

        out: list[SportmonksFixtureLite] = []
        for row in data:
            if not isinstance(row, dict):
                continue
            fid = _safe_int(row.get("id"))
            if fid is None:
                continue

            # participants: list with location home/away
            home_name = away_name = ""
            home_id = away_id = None
            parts = row.get("participants")
            if isinstance(parts, list):
                for p in parts:
                    if not isinstance(p, dict):
                        continue
                    loc = str(p.get("meta", {}).get("location") if isinstance(p.get("meta"), dict) else p.get("location") or "").strip().lower()
                    nm = str(p.get("name") or "")
                    pid = _safe_int(p.get("id"))
                    if loc == "home":
                        home_name, home_id = nm, pid
                    elif loc == "away":
                        away_name, away_id = nm, pid

            # minute + score: best-effort from scores array
            minute = _safe_int(row.get("minute")) or _safe_int(row.get("time"))
            sh = sa = None
            scores = row.get("scores")
            if isinstance(scores, list):
                # Try current score entries
                for sc in scores:
                    if not isinstance(sc, dict):
                        continue
                    if str(sc.get("description") or "").lower() in {"current", "ft", "ht", "live"} or sc.get("score") or sc.get("participant_id"):
                        # Format can vary; take last known home/away totals
                        h = sc.get("score", {}).get("home") if isinstance(sc.get("score"), dict) else sc.get("home_score")
                        a = sc.get("score", {}).get("away") if isinstance(sc.get("score"), dict) else sc.get("away_score")
                        sh = sh if sh is not None else _safe_int(h)
                        sa = sa if sa is not None else _safe_int(a)
            out.append(
                SportmonksFixtureLite(
                    fixture_id=fid,
                    home_team=home_name,
                    away_team=away_name,
                    home_team_id=home_id,
                    away_team_id=away_id,
                    minute=minute,
                    score_home=sh,
                    score_away=sa,
                )
            )
        return out

    def get_fixture_stats(self, fixture_id: int) -> SportmonksFixtureStats | None:
        """Return shots/corners/cards for fixture (best-effort)."""
        if not self._require_token():
            return None
        fid = int(fixture_id)
        url = f"{self.BASE_URL}/fixtures/{fid}"
        # Filter only needed stat types to keep payload small.
        type_ids = [
            self._TYPE_CORNERS,
            self._TYPE_SHOTS_ON_TARGET,
            self._TYPE_SHOTS_OFF_TARGET,
            self._TYPE_YELLOW_CARDS,
            self._TYPE_RED_CARDS,
        ]
        params = {
            "api_token": self._token,
            "include": "statistics",
            "filters": "fixtureStatisticTypes:" + ",".join(str(x) for x in type_ids),
        }
        try:
            with self._client() as c:
                r = c.get(url, params=params)
                r.raise_for_status()
                payload = r.json()
        except Exception as e:
            logger.info("[SPORTMONKS] fixture stats fetch failed fixture_id=%s: %s", fid, e)
            return None

        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            if isinstance(payload, dict) and payload.get("message"):
                logger.info(
                    "[SPORTMONKS] fixture stats empty fixture_id=%s msg=%s",
                    fid,
                    str(payload.get("message"))[:280],
                )
            return None
        stats = data.get("statistics")
        if not isinstance(stats, list):
            return SportmonksFixtureStats(
                fixture_id=fid,
                shots_total=None,
                shots_on_target=None,
                corners=None,
                cards_total=None,
                raw=None,
            )

        # Sportmonks returns one row per participant and type_id. We sum home+away.
        sums: dict[int, int] = {}
        for st in stats:
            if not isinstance(st, dict):
                continue
            tid = _safe_int(st.get("type_id"))
            if tid is None:
                continue
            dv = st.get("data")
            val = None
            if isinstance(dv, dict):
                val = dv.get("value")
            vi = _safe_int(val)
            if vi is None:
                continue
            sums[tid] = int(sums.get(tid, 0) or 0) + int(vi)

        corners = sums.get(self._TYPE_CORNERS)
        sot = sums.get(self._TYPE_SHOTS_ON_TARGET)
        soff = sums.get(self._TYPE_SHOTS_OFF_TARGET)
        shots_total = None
        if sot is not None or soff is not None:
            shots_total = int((sot or 0) + (soff or 0))

        cards_total = None
        yc = sums.get(self._TYPE_YELLOW_CARDS)
        rc = sums.get(self._TYPE_RED_CARDS)
        if yc is not None or rc is not None:
            cards_total = int((yc or 0) + (rc or 0))

        return SportmonksFixtureStats(
            fixture_id=fid,
            shots_total=shots_total,
            shots_on_target=sot,
            corners=corners,
            cards_total=cards_total,
            raw={"type_sums": sums},
        )

    def get_team_recent_form(self, team_id: int) -> dict[str, Any] | None:
        """Minimal recent form (best-effort). Returns last 5 fixtures results if available."""
        if not self._require_token():
            return None
        tid = int(team_id)
        # Use fixtures by team endpoint if available.
        url = f"{self.BASE_URL}/teams/{tid}"
        params = {
            "api_token": self._token,
            # Keep light: includes are plan-dependent; this is best-effort.
            "include": "latest",
        }
        try:
            with self._client() as c:
                r = c.get(url, params=params)
                r.raise_for_status()
                payload = r.json()
        except Exception as e:
            logger.info("[SPORTMONKS] team form fetch failed team_id=%s: %s", tid, e)
            return None
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            return None
        return {"team_id": tid, "data": data}

    @staticmethod
    def map_winline_match_to_fixture(
        *,
        winline_home: str,
        winline_away: str,
        live_fixtures: list[SportmonksFixtureLite],
    ) -> SportmonksFixtureLite | None:
        """Best-effort mapping by normalized home/away names."""
        h = _norm_team(winline_home)
        a = _norm_team(winline_away)
        if not h or not a:
            return None
        for fx in live_fixtures:
            if _norm_team(fx.home_team) == h and _norm_team(fx.away_team) == a:
                return fx
        # fallback: contains match (kept conservative)
        for fx in live_fixtures:
            if h in _norm_team(fx.home_team) and a in _norm_team(fx.away_team):
                return fx
        return None

