from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import httpx

from app.core.config import get_settings
from app.services.external_api_monitor_service import ExternalApiHealthCheckResult, ExternalApiMonitorService

logger = logging.getLogger(__name__)


def _norm(s: str) -> str:
    return (s or "").strip().lower().replace("ё", "е")


def _safe_int(v: object) -> int | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _safe_percent(v: object) -> int | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        s = str(v).strip().replace("%", "").replace(",", ".")
        if not s:
            return None
        return int(round(float(s)))
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class ApiFootballFixtureLite:
    fixture_id: int
    home_team: str
    away_team: str
    home_team_id: int | None
    away_team_id: int | None
    minute: int | None
    score_home: int | None
    score_away: int | None
    league: str | None = None


@dataclass(frozen=True)
class ApiFootballFixtureStats:
    fixture_id: int
    shots_total: int | None
    shots_on_target: int | None
    corners: int | None
    cards_yellow: int | None
    cards_red: int | None
    home_shots_total: int | None = None
    away_shots_total: int | None = None
    home_shots_on_target: int | None = None
    away_shots_on_target: int | None = None
    home_possession: int | None = None
    away_possession: int | None = None
    home_attacks: int | None = None
    away_attacks: int | None = None
    home_dangerous_attacks: int | None = None
    away_dangerous_attacks: int | None = None
    raw: dict[str, Any] | None = None


class ApiFootballService:
    """Minimal API-Football v3 (API-SPORTS) client for live analytics enrichment."""

    def __init__(self) -> None:
        s = get_settings()
        self._key = (s.api_football_api_key or "").strip()
        self._base = (s.api_football_base_url or "").strip().rstrip("/")
        self._timeout = 12.0

    def _require_key(self) -> bool:
        if not self._key:
            logger.info("[API_FOOTBALL] api key missing (API_FOOTBALL_API_KEY not set)")
            return False
        if not ExternalApiMonitorService().is_runtime_enabled("api_football", configured_enabled=bool(self._key)):
            logger.info("[API_FOOTBALL] runtime disabled by external API monitor")
            return False
        return True

    async def health_check(self) -> ExternalApiHealthCheckResult:
        if not self._key:
            return ExternalApiHealthCheckResult(ok=False, error_text="disabled_no_api_key", http_status=None)
        timeout = httpx.Timeout(connect=5.0, read=8.0, write=5.0, pool=5.0)
        try:
            async with httpx.AsyncClient(
                base_url=self._base,
                timeout=timeout,
                headers={
                    "User-Agent": "stavki-bot/football-live",
                    "x-apisports-key": self._key,
                },
            ) as c:
                r = await c.get("/status")
            if r.status_code == 200:
                return ExternalApiHealthCheckResult(ok=True, error_text=None, http_status=200)
            body = (r.text or "").strip()
            if len(body) > 400:
                body = body[:400] + "..."
            return ExternalApiHealthCheckResult(
                ok=False,
                error_text=f"http_{r.status_code}: {body or 'empty_body'}",
                http_status=int(r.status_code),
            )
        except Exception as exc:  # noqa: BLE001
            return ExternalApiHealthCheckResult(ok=False, error_text=f"request_error: {exc!s}", http_status=None)

    def _client(self) -> httpx.Client:
        return httpx.Client(
            base_url=self._base,
            timeout=self._timeout,
            headers={
                "User-Agent": "stavki-bot/football-live",
                "x-apisports-key": self._key,
            },
        )

    def get_live_fixtures(self) -> list[ApiFootballFixtureLite]:
        if not self._require_key():
            return []
        try:
            with self._client() as c:
                r = c.get("/fixtures", params={"live": "all"})
                r.raise_for_status()
                payload = r.json()
        except Exception as e:
            logger.info("[API_FOOTBALL] live fixtures fetch failed: %s", e)
            return []
        resp = payload.get("response") if isinstance(payload, dict) else None
        if not isinstance(resp, list):
            return []

        out: list[ApiFootballFixtureLite] = []
        for row in resp:
            if not isinstance(row, dict):
                continue
            fx = row.get("fixture") if isinstance(row.get("fixture"), dict) else {}
            teams = row.get("teams") if isinstance(row.get("teams"), dict) else {}
            goals = row.get("goals") if isinstance(row.get("goals"), dict) else {}
            league = row.get("league") if isinstance(row.get("league"), dict) else {}

            fid = _safe_int(fx.get("id"))
            if fid is None:
                continue
            status = fx.get("status") if isinstance(fx.get("status"), dict) else {}
            minute = _safe_int(status.get("elapsed"))

            home = teams.get("home") if isinstance(teams.get("home"), dict) else {}
            away = teams.get("away") if isinstance(teams.get("away"), dict) else {}
            out.append(
                ApiFootballFixtureLite(
                    fixture_id=fid,
                    home_team=str(home.get("name") or ""),
                    away_team=str(away.get("name") or ""),
                    home_team_id=_safe_int(home.get("id")),
                    away_team_id=_safe_int(away.get("id")),
                    minute=minute,
                    score_home=_safe_int(goals.get("home")),
                    score_away=_safe_int(goals.get("away")),
                    league=str(league.get("name") or "") or None,
                )
            )
        return out

    def get_fixture_statistics(self, fixture_id: int) -> ApiFootballFixtureStats | None:
        if not self._require_key():
            return None
        fid = int(fixture_id)
        try:
            with self._client() as c:
                r = c.get("/fixtures/statistics", params={"fixture": fid})
                r.raise_for_status()
                payload = r.json()
        except Exception as e:
            logger.info("[API_FOOTBALL] stats fetch failed fixture_id=%s: %s", fid, e)
            return None
        resp = payload.get("response") if isinstance(payload, dict) else None
        if not isinstance(resp, list) or not resp:
            return ApiFootballFixtureStats(fid, None, None, None, None, None, raw=None)

        # response: list per team with statistics list
        team_maps: list[dict[str, int | None]] = []
        for team_row in resp:
            if not isinstance(team_row, dict):
                continue
            stats = team_row.get("statistics")
            if not isinstance(stats, list):
                continue
            stat_map: dict[str, int | None] = {}
            for it in stats:
                if not isinstance(it, dict):
                    continue
                key = _norm(str(it.get("type") or ""))
                value = it.get("value")
                if key == _norm("Ball Possession"):
                    stat_map[key] = _safe_percent(value)
                else:
                    stat_map[key] = _safe_int(value)
            team_maps.append(stat_map)

        def _team_val(idx: int, name: str) -> int | None:
            if idx >= len(team_maps):
                return None
            return team_maps[idx].get(_norm(name))

        home_shots_total = _team_val(0, "Total Shots")
        away_shots_total = _team_val(1, "Total Shots")
        home_shots_on = _team_val(0, "Shots on Goal")
        away_shots_on = _team_val(1, "Shots on Goal")
        home_possession = _team_val(0, "Ball Possession")
        away_possession = _team_val(1, "Ball Possession")
        home_attacks = _team_val(0, "Attacks")
        away_attacks = _team_val(1, "Attacks")
        home_dangerous_attacks = _team_val(0, "Dangerous Attacks")
        away_dangerous_attacks = _team_val(1, "Dangerous Attacks")
        home_corners = _team_val(0, "Corner Kicks")
        away_corners = _team_val(1, "Corner Kicks")
        home_yellow = _team_val(0, "Yellow Cards")
        away_yellow = _team_val(1, "Yellow Cards")
        home_red = _team_val(0, "Red Cards")
        away_red = _team_val(1, "Red Cards")

        shots_total = (
            (home_shots_total or 0) + (away_shots_total or 0)
            if (home_shots_total is not None or away_shots_total is not None)
            else None
        )
        shots_on = (
            (home_shots_on or 0) + (away_shots_on or 0)
            if (home_shots_on is not None or away_shots_on is not None)
            else None
        )
        corners = (
            (home_corners or 0) + (away_corners or 0)
            if (home_corners is not None or away_corners is not None)
            else None
        )
        yellow = (
            (home_yellow or 0) + (away_yellow or 0)
            if (home_yellow is not None or away_yellow is not None)
            else None
        )
        red = (
            (home_red or 0) + (away_red or 0)
            if (home_red is not None or away_red is not None)
            else None
        )
        return ApiFootballFixtureStats(
            fixture_id=fid,
            shots_total=shots_total,
            shots_on_target=shots_on,
            corners=corners,
            cards_yellow=yellow,
            cards_red=red,
            home_shots_total=home_shots_total,
            away_shots_total=away_shots_total,
            home_shots_on_target=home_shots_on,
            away_shots_on_target=away_shots_on,
            home_possession=home_possession,
            away_possession=away_possession,
            home_attacks=home_attacks,
            away_attacks=away_attacks,
            home_dangerous_attacks=home_dangerous_attacks,
            away_dangerous_attacks=away_dangerous_attacks,
            raw={"teams": team_maps},
        )

    def get_fixture_events(self, fixture_id: int) -> dict[str, Any] | None:
        if not self._require_key():
            return None
        fid = int(fixture_id)
        try:
            with self._client() as c:
                r = c.get("/fixtures/events", params={"fixture": fid})
                r.raise_for_status()
                payload = r.json()
        except Exception as e:
            logger.info("[API_FOOTBALL] events fetch failed fixture_id=%s: %s", fid, e)
            return None
        return payload if isinstance(payload, dict) else None

    def get_fixture_lineups(self, fixture_id: int) -> dict[str, Any] | None:
        if not self._require_key():
            return None
        fid = int(fixture_id)
        try:
            with self._client() as c:
                r = c.get("/fixtures/lineups", params={"fixture": fid})
                r.raise_for_status()
                payload = r.json()
        except Exception as e:
            logger.info("[API_FOOTBALL] lineups fetch failed fixture_id=%s: %s", fid, e)
            return None
        return payload if isinstance(payload, dict) else None

    def get_team_recent_form(self, team_id: int) -> dict[str, Any] | None:
        # Optional; keep minimal and best-effort.
        if not self._require_key():
            return None
        tid = int(team_id)
        try:
            with self._client() as c:
                r = c.get("/fixtures", params={"team": tid, "last": 5})
                r.raise_for_status()
                payload = r.json()
        except Exception as e:
            logger.info("[API_FOOTBALL] team form fetch failed team_id=%s: %s", tid, e)
            return None
        return payload if isinstance(payload, dict) else None

    @staticmethod
    def map_winline_match_to_fixture(
        *,
        winline_home: str,
        winline_away: str,
        fixtures: list[ApiFootballFixtureLite],
    ) -> ApiFootballFixtureLite | None:
        h = _norm(winline_home)
        a = _norm(winline_away)
        if not h or not a:
            return None
        for fx in fixtures:
            if _norm(fx.home_team) == h and _norm(fx.away_team) == a:
                return fx
        for fx in fixtures:
            if h in _norm(fx.home_team) and a in _norm(fx.away_team):
                return fx
        return None

