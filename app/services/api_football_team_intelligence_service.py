from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

import httpx

from app.core.config import get_settings
from app.schemas.provider_models import ProviderSignalCandidate
from app.services.api_football_service import ApiFootballFixtureLite, ApiFootballService

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ApiFootballTeamIntelligenceBatchResult:
    candidates: list[ProviderSignalCandidate]
    attempted: int
    mapped: int
    loaded: int
    missing: int
    requests_used: int
    cache_hits: int
    examples: list[dict[str, object]]


@dataclass
class _CacheEntry:
    expires_at: float
    value: dict[str, Any] | None


class ApiFootballTeamIntelligenceService:
    """Cached API-Football team intelligence enrichment for mapped live fixtures.

    This service intentionally does not score or filter candidates. It only attaches
    a compact analytics snapshot built from economical endpoints.
    """

    _CACHE: dict[tuple[object, ...], _CacheEntry] = {}

    def __init__(self, *, ttl_seconds: int = 4 * 60 * 60, live_ttl_seconds: int = 60) -> None:
        settings = get_settings()
        self._key = (settings.api_football_api_key or "").strip()
        self._base = (settings.api_football_base_url or "").strip().rstrip("/")
        self._ttl_seconds = int(ttl_seconds)
        self._live_ttl_seconds = int(live_ttl_seconds)
        self.requests_used = 0
        self.cache_hits = 0

    def enrich_candidates(
        self,
        candidates: list[ProviderSignalCandidate],
        *,
        max_matches: int = 5,
    ) -> ApiFootballTeamIntelligenceBatchResult:
        if not candidates or not self._key or not self._base:
            return ApiFootballTeamIntelligenceBatchResult(
                candidates=candidates,
                attempted=0,
                mapped=0,
                loaded=0,
                missing=len({str(c.match.external_event_id or "") for c in candidates}),
                requests_used=0,
                cache_hits=0,
                examples=[],
            )

        live_fixtures = self._get_live_fixtures()
        if not live_fixtures:
            return ApiFootballTeamIntelligenceBatchResult(
                candidates=candidates,
                attempted=len({str(c.match.external_event_id or "") for c in candidates}),
                mapped=0,
                loaded=0,
                missing=len({str(c.match.external_event_id or "") for c in candidates}),
                requests_used=self.requests_used,
                cache_hits=self.cache_hits,
                examples=[],
            )

        svc = ApiFootballService()
        enriched: list[ProviderSignalCandidate] = []
        intelligence_by_event: dict[str, dict[str, object] | None] = {}
        attempted = 0
        mapped = 0
        examples: list[dict[str, object]] = []

        for cand in candidates:
            event_id = str(cand.match.external_event_id or "")
            if event_id in intelligence_by_event:
                intelligence = intelligence_by_event[event_id]
            else:
                attempted += 1
                intelligence = None
                if mapped < max_matches:
                    fx = svc.map_winline_match_to_fixture(
                        winline_home=str(cand.match.home_team or ""),
                        winline_away=str(cand.match.away_team or ""),
                        fixtures=live_fixtures,
                    )
                    if fx is not None:
                        mapped += 1
                        intelligence = self._build_intelligence(fx)
                intelligence_by_event[event_id] = intelligence

            if intelligence:
                fs = dict(cand.feature_snapshot_json or {})
                fs["api_football_team_intelligence"] = intelligence
                cand = cand.model_copy(update={"feature_snapshot_json": fs})
                if len(examples) < 3:
                    examples.append(
                        {
                            "match": str(cand.match.match_name or ""),
                            "fixture_id": intelligence.get("fixture_id"),
                            "recent_form_home": intelligence.get("recent_form_home"),
                            "recent_form_away": intelligence.get("recent_form_away"),
                            "avg_goals_for_home": intelligence.get("avg_goals_for_home"),
                            "avg_goals_for_away": intelligence.get("avg_goals_for_away"),
                            "standings_edge": intelligence.get("standings_edge"),
                            "h2h_summary": intelligence.get("h2h_summary"),
                            "common_opponent_edge": intelligence.get("common_opponent_edge"),
                            "confidence_score": intelligence.get("confidence_score"),
                            "requests_used": intelligence.get("requests_used"),
                            "cache_hits": intelligence.get("cache_hits"),
                        }
                    )
            enriched.append(cand)

        loaded = sum(1 for item in intelligence_by_event.values() if item)
        missing = max(0, attempted - loaded)
        return ApiFootballTeamIntelligenceBatchResult(
            candidates=enriched,
            attempted=attempted,
            mapped=mapped,
            loaded=loaded,
            missing=missing,
            requests_used=self.requests_used,
            cache_hits=self.cache_hits,
            examples=examples,
        )

    def _client(self) -> httpx.Client:
        return httpx.Client(
            base_url=self._base,
            timeout=12.0,
            headers={
                "User-Agent": "stavki-bot/api-football-team-intelligence",
                "x-apisports-key": self._key,
            },
        )

    def _get_json(
        self,
        endpoint: str,
        params: dict[str, object],
        *,
        cache_key: tuple[object, ...],
        ttl_seconds: int | None = None,
    ) -> dict[str, Any] | None:
        now = time.time()
        cached = self._CACHE.get(cache_key)
        if cached and cached.expires_at > now:
            self.cache_hits += 1
            return cached.value

        self.requests_used += 1
        try:
            with self._client() as client:
                resp = client.get(endpoint, params=params)
                resp.raise_for_status()
                payload = resp.json()
        except Exception as exc:  # noqa: BLE001
            logger.info("[API_FOOTBALL][INTELLIGENCE] request failed endpoint=%s params=%s error=%s", endpoint, params, exc)
            payload = None

        if isinstance(payload, dict) and isinstance(payload.get("errors"), dict) and payload.get("errors"):
            logger.info(
                "[API_FOOTBALL][INTELLIGENCE] endpoint returned errors endpoint=%s errors=%s",
                endpoint,
                payload.get("errors"),
            )

        ttl = self._ttl_seconds if ttl_seconds is None else int(ttl_seconds)
        self._CACHE[cache_key] = _CacheEntry(expires_at=now + ttl, value=payload if isinstance(payload, dict) else None)
        return payload if isinstance(payload, dict) else None

    def _response_list(self, payload: dict[str, Any] | None) -> list[dict[str, Any]]:
        resp = payload.get("response") if isinstance(payload, dict) else None
        return [row for row in resp if isinstance(row, dict)] if isinstance(resp, list) else []

    def _get_live_fixtures(self) -> list[ApiFootballFixtureLite]:
        payload = self._get_json(
            "/fixtures",
            {"live": "all"},
            cache_key=("live_fixtures", "all"),
            ttl_seconds=self._live_ttl_seconds,
        )
        fixtures: list[ApiFootballFixtureLite] = []
        for row in self._response_list(payload):
            fx = row.get("fixture") if isinstance(row.get("fixture"), dict) else {}
            teams = row.get("teams") if isinstance(row.get("teams"), dict) else {}
            goals = row.get("goals") if isinstance(row.get("goals"), dict) else {}
            league = row.get("league") if isinstance(row.get("league"), dict) else {}
            status = fx.get("status") if isinstance(fx.get("status"), dict) else {}
            fid = self._safe_int(fx.get("id"))
            if fid is None:
                continue
            home = teams.get("home") if isinstance(teams.get("home"), dict) else {}
            away = teams.get("away") if isinstance(teams.get("away"), dict) else {}
            fixtures.append(
                ApiFootballFixtureLite(
                    fixture_id=fid,
                    home_team=str(home.get("name") or ""),
                    away_team=str(away.get("name") or ""),
                    home_team_id=self._safe_int(home.get("id")),
                    away_team_id=self._safe_int(away.get("id")),
                    minute=self._safe_int(status.get("elapsed")),
                    score_home=self._safe_int(goals.get("home")),
                    score_away=self._safe_int(goals.get("away")),
                    league=str(league.get("name") or "") or None,
                    starting_at=str(fx.get("date") or "") or None,
                    status_short=str(status.get("short") or "") or None,
                    status_long=str(status.get("long") or "") or None,
                    league_id=self._safe_int(league.get("id")),
                    league_country=str(league.get("country") or "") or None,
                    season=self._safe_int(league.get("season")),
                )
            )
        return fixtures

    def _build_intelligence(self, fx: ApiFootballFixtureLite) -> dict[str, object] | None:
        if not (fx.home_team_id and fx.away_team_id):
            return None

        before_req = self.requests_used
        before_cache = self.cache_hits
        home_last = self._get_team_last_fixtures(int(fx.home_team_id))
        away_last = self._get_team_last_fixtures(int(fx.away_team_id))
        standings = self._get_standings(int(fx.league_id), int(fx.season)) if fx.league_id and fx.season else []
        h2h = self._get_h2h(int(fx.home_team_id), int(fx.away_team_id))

        home_form = self._recent_form_summary(int(fx.home_team_id), home_last)
        away_form = self._recent_form_summary(int(fx.away_team_id), away_last)
        standings_edge = self._standings_edge(standings, int(fx.home_team_id), int(fx.away_team_id))
        h2h_summary = self._h2h_summary(h2h, int(fx.home_team_id), int(fx.away_team_id))
        common_edge = self._common_opponent_edge(home_form.get("opponent_results", {}), away_form.get("opponent_results", {}))

        available_inputs = [
            home_form.get("matches", 0) > 0,
            away_form.get("matches", 0) > 0,
            bool(standings_edge.get("available")),
            bool(h2h_summary.get("matches")),
            bool(common_edge.get("count")),
        ]
        confidence_score = round(100.0 * sum(1 for item in available_inputs if item) / len(available_inputs), 1)

        return {
            "fixture_id": int(fx.fixture_id),
            "league_id": fx.league_id,
            "season": fx.season,
            "recent_form_home": home_form.get("form"),
            "recent_form_away": away_form.get("form"),
            "avg_goals_for_home": home_form.get("avg_goals_for"),
            "avg_goals_for_away": away_form.get("avg_goals_for"),
            "avg_goals_against_home": home_form.get("avg_goals_against"),
            "avg_goals_against_away": away_form.get("avg_goals_against"),
            "standings_home_rank": standings_edge.get("home_rank"),
            "standings_away_rank": standings_edge.get("away_rank"),
            "standings_edge": standings_edge,
            "h2h_matches": h2h_summary.get("matches"),
            "h2h_home_wins": h2h_summary.get("home_wins"),
            "h2h_away_wins": h2h_summary.get("away_wins"),
            "h2h_draws": h2h_summary.get("draws"),
            "h2h_summary": h2h_summary,
            "common_opponent_edge": common_edge,
            "confidence_score": confidence_score,
            "requests_used": self.requests_used - before_req,
            "cache_hits": self.cache_hits - before_cache,
        }

    def _get_team_last_fixtures(self, team_id: int) -> list[dict[str, Any]]:
        payload = self._get_json(
            "/fixtures",
            {"team": int(team_id), "last": 10},
            cache_key=("team_last", int(team_id), 10),
        )
        return self._response_list(payload)

    def _get_standings(self, league_id: int, season: int) -> list[dict[str, Any]]:
        payload = self._get_json(
            "/standings",
            {"league": int(league_id), "season": int(season)},
            cache_key=("standings", int(league_id), int(season)),
        )
        return self._response_list(payload)

    def _get_h2h(self, home_team_id: int, away_team_id: int) -> list[dict[str, Any]]:
        payload = self._get_json(
            "/fixtures/headtohead",
            {"h2h": f"{int(home_team_id)}-{int(away_team_id)}", "last": 10},
            cache_key=("h2h", int(home_team_id), int(away_team_id), 10),
        )
        return self._response_list(payload)

    def _recent_form_summary(self, team_id: int, rows: list[dict[str, Any]]) -> dict[str, object]:
        finished: list[dict[str, object]] = []
        opponent_results: dict[int, list[int]] = {}
        for row in rows:
            fixture = row.get("fixture") if isinstance(row.get("fixture"), dict) else {}
            status = fixture.get("status") if isinstance(fixture.get("status"), dict) else {}
            if str(status.get("short") or "") not in {"FT", "AET", "PEN"}:
                continue
            teams = row.get("teams") if isinstance(row.get("teams"), dict) else {}
            goals = row.get("goals") if isinstance(row.get("goals"), dict) else {}
            home = teams.get("home") if isinstance(teams.get("home"), dict) else {}
            away = teams.get("away") if isinstance(teams.get("away"), dict) else {}
            gh = self._safe_int(goals.get("home"))
            ga = self._safe_int(goals.get("away"))
            if gh is None or ga is None:
                continue
            is_home = self._safe_int(home.get("id")) == int(team_id)
            gf = gh if is_home else ga
            against = ga if is_home else gh
            result = "W" if gf > against else "L" if gf < against else "D"
            opponent = away if is_home else home
            opp_id = self._safe_int(opponent.get("id"))
            gd = int(gf) - int(against)
            if opp_id is not None:
                opponent_results.setdefault(int(opp_id), []).append(gd)
            finished.append({"gf": gf, "ga": against, "result": result})
        if not finished:
            return {
                "matches": 0,
                "form": "",
                "avg_goals_for": None,
                "avg_goals_against": None,
                "opponent_results": opponent_results,
            }
        return {
            "matches": len(finished),
            "form": " ".join(str(item["result"]) for item in finished[:10]),
            "avg_goals_for": round(sum(float(item["gf"]) for item in finished) / len(finished), 2),
            "avg_goals_against": round(sum(float(item["ga"]) for item in finished) / len(finished), 2),
            "opponent_results": opponent_results,
        }

    def _standings_edge(self, rows: list[dict[str, Any]], home_team_id: int, away_team_id: int) -> dict[str, object]:
        if not rows:
            return {"available": False}
        league = rows[0].get("league") if isinstance(rows[0].get("league"), dict) else {}
        standings = league.get("standings") if isinstance(league, dict) else None
        flat = standings[0] if isinstance(standings, list) and standings and isinstance(standings[0], list) else []
        by_team = {
            self._safe_int((row.get("team") or {}).get("id")): row
            for row in flat
            if isinstance(row, dict) and isinstance(row.get("team"), dict)
        }
        home = by_team.get(int(home_team_id))
        away = by_team.get(int(away_team_id))
        if not home or not away:
            return {"available": False}
        home_rank = self._safe_int(home.get("rank"))
        away_rank = self._safe_int(away.get("rank"))
        return {
            "available": True,
            "home_rank": home_rank,
            "away_rank": away_rank,
            "rank_edge_home_minus_away": (away_rank - home_rank) if home_rank is not None and away_rank is not None else None,
            "home_points": self._safe_int(home.get("points")),
            "away_points": self._safe_int(away.get("points")),
        }

    def _h2h_summary(self, rows: list[dict[str, Any]], home_team_id: int, away_team_id: int) -> dict[str, object]:
        matches = home_wins = away_wins = draws = 0
        for row in rows:
            fixture = row.get("fixture") if isinstance(row.get("fixture"), dict) else {}
            status = fixture.get("status") if isinstance(fixture.get("status"), dict) else {}
            if str(status.get("short") or "") not in {"FT", "AET", "PEN"}:
                continue
            teams = row.get("teams") if isinstance(row.get("teams"), dict) else {}
            goals = row.get("goals") if isinstance(row.get("goals"), dict) else {}
            api_home = teams.get("home") if isinstance(teams.get("home"), dict) else {}
            api_away = teams.get("away") if isinstance(teams.get("away"), dict) else {}
            gh = self._safe_int(goals.get("home"))
            ga = self._safe_int(goals.get("away"))
            if gh is None or ga is None:
                continue
            matches += 1
            if gh == ga:
                draws += 1
                continue
            winner_id = self._safe_int(api_home.get("id")) if gh > ga else self._safe_int(api_away.get("id"))
            if winner_id == int(home_team_id):
                home_wins += 1
            elif winner_id == int(away_team_id):
                away_wins += 1
        return {
            "matches": matches,
            "home_wins": home_wins,
            "away_wins": away_wins,
            "draws": draws,
        }

    def _common_opponent_edge(
        self,
        home_results: object,
        away_results: object,
    ) -> dict[str, object]:
        if not isinstance(home_results, dict) or not isinstance(away_results, dict):
            return {"count": 0, "edge_home_minus_away": None}
        common = set(home_results) & set(away_results)
        if not common:
            return {"count": 0, "edge_home_minus_away": None}
        home_avg = []
        away_avg = []
        for opp_id in common:
            h_vals = home_results.get(opp_id) or []
            a_vals = away_results.get(opp_id) or []
            if h_vals:
                home_avg.append(sum(h_vals) / len(h_vals))
            if a_vals:
                away_avg.append(sum(a_vals) / len(a_vals))
        if not home_avg or not away_avg:
            return {"count": len(common), "edge_home_minus_away": None}
        return {
            "count": len(common),
            "edge_home_minus_away": round((sum(home_avg) / len(home_avg)) - (sum(away_avg) / len(away_avg)), 3),
        }

    def _safe_int(self, value: object) -> int | None:
        if value is None or isinstance(value, bool):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
