from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import httpx

from app.core.config import get_settings

logger = logging.getLogger(__name__)


def _norm_team(s: str) -> str:
    s0 = (s or "").strip().lower().replace("ё", "е")
    if not s0:
        return ""
    # Cheap cyrillic -> latin transliteration (best-effort, for matching only).
    tr = {
        "а": "a",
        "б": "b",
        "в": "v",
        "г": "g",
        "д": "d",
        "е": "e",
        "ж": "zh",
        "з": "z",
        "и": "i",
        "й": "y",
        "к": "k",
        "л": "l",
        "м": "m",
        "н": "n",
        "о": "o",
        "п": "p",
        "р": "r",
        "с": "s",
        "т": "t",
        "у": "u",
        "ф": "f",
        "х": "h",
        "ц": "ts",
        "ч": "ch",
        "ш": "sh",
        "щ": "sch",
        "ъ": "",
        "ы": "y",
        "ь": "",
        "э": "e",
        "ю": "yu",
        "я": "ya",
    }
    s1 = "".join(tr.get(ch, ch) for ch in s0)
    # Strip punctuation and stopwords.
    buf: list[str] = []
    for ch in s1:
        if ch.isalnum() or ch.isspace():
            buf.append(ch)
        else:
            buf.append(" ")
    s2 = " ".join("".join(buf).split())
    stop = {"fc", "fk", "sc", "cf", "cd", "ac", "kc", "bk", "u19", "u20", "u21", "u23", "women", "w", "reserves"}
    tokens = [t for t in s2.split() if t and t not in stop]
    return " ".join(tokens)


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
    minute: int | None = None
    score_home: int | None = None
    score_away: int | None = None
    starting_at: str | None = None
    league_id: int | None = None


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
                    starting_at=row.get("starting_at"),
                    league_id=_safe_int(row.get("league_id")),
                    minute=minute,
                    score_home=sh,
                    score_away=sa,
                )
            )
        return out

    def get_fixtures_between(
        self,
        *,
        start_date: str,
        end_date: str,
    ) -> list[SportmonksFixtureLite]:
        """List fixtures for a date window (baseline mapping anchor)."""
        if not self._require_token():
            return []
        url = f"{self.BASE_URL}/fixtures/between/{start_date}/{end_date}"
        params = {"api_token": self._token, "include": "participants;scores"}
        try:
            with self._client() as c:
                r = c.get(url, params=params)
                r.raise_for_status()
                payload = r.json()
        except Exception as e:
            logger.info("[SPORTMONKS] fixtures between fetch failed: %s", e)
            return []
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, list):
            if isinstance(payload, dict) and payload.get("message"):
                logger.info("[SPORTMONKS] fixtures between empty: %s", str(payload.get("message"))[:280])
            return []

        out: list[SportmonksFixtureLite] = []
        for row in data:
            if not isinstance(row, dict):
                continue
            fid = _safe_int(row.get("id"))
            if fid is None:
                continue
            home_name = away_name = ""
            home_id = away_id = None
            parts = row.get("participants")
            if isinstance(parts, list):
                for p in parts:
                    if not isinstance(p, dict):
                        continue
                    meta = p.get("meta") if isinstance(p.get("meta"), dict) else {}
                    loc = str(meta.get("location") or p.get("location") or "").strip().lower()
                    nm = str(p.get("name") or "")
                    pid = _safe_int(p.get("id"))
                    if loc == "home":
                        home_name, home_id = nm, pid
                    elif loc == "away":
                        away_name, away_id = nm, pid
            # scores: best-effort final/current
            sh = sa = None
            scores = row.get("scores")
            if isinstance(scores, list):
                for sc in scores:
                    if not isinstance(sc, dict):
                        continue
                    scr = sc.get("score") if isinstance(sc.get("score"), dict) else {}
                    hh = scr.get("home") if isinstance(scr, dict) else sc.get("home_score")
                    aa = scr.get("away") if isinstance(scr, dict) else sc.get("away_score")
                    hi = _safe_int(hh)
                    ai = _safe_int(aa)
                    if hi is None or ai is None:
                        continue
                    sh, sa = hi, ai
            out.append(
                SportmonksFixtureLite(
                    fixture_id=fid,
                    home_team=home_name,
                    away_team=away_name,
                    home_team_id=home_id,
                    away_team_id=away_id,
                    starting_at=row.get("starting_at"),
                    league_id=_safe_int(row.get("league_id")),
                    minute=_safe_int(row.get("minute")) or _safe_int(row.get("time")),
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
        """Legacy shim: kept for callers. Prefer `get_team_last_fixtures_summary`."""
        return self.get_team_last_fixtures_summary(team_id, lookback_days=60, limit=5)

    def list_teams(self, *, page: int = 1, per_page: int = 100) -> list[dict[str, Any]]:
        if not self._require_token():
            return []
        url = f"{self.BASE_URL}/teams"
        params = {"api_token": self._token, "page": int(page), "per_page": int(per_page)}
        try:
            with self._client() as c:
                r = c.get(url, params=params)
                r.raise_for_status()
                payload = r.json()
        except Exception as e:
            logger.info("[SPORTMONKS] list teams failed: %s", e)
            return []
        data = payload.get("data") if isinstance(payload, dict) else None
        return [x for x in data if isinstance(x, dict)] if isinstance(data, list) else []

    def find_team_id_by_name(self, team_name: str) -> int | None:
        """Best-effort mapping using the teams list available to current subscription."""
        name_n = _norm_team(team_name)
        if not name_n:
            return None
        for page in range(1, 6):
            rows = self.list_teams(page=page, per_page=100)
            if not rows:
                break
            for t in rows:
                nm = _norm_team(str(t.get("name") or ""))
                if not nm:
                    continue
                if nm == name_n or (name_n in nm) or (nm in name_n):
                    tid = _safe_int(t.get("id"))
                    if tid is not None:
                        return tid
        return None

    def get_team_last_fixtures_summary(
        self,
        team_id: int,
        *,
        lookback_days: int = 60,
        limit: int = 5,
    ) -> dict[str, Any] | None:
        """Return minimal last-N fixtures summary for baseline scoring."""
        if not self._require_token():
            return None
        tid = int(team_id)
        from datetime import datetime, timedelta, timezone

        end = datetime.now(timezone.utc).date().isoformat()
        start = (datetime.now(timezone.utc) - timedelta(days=max(7, int(lookback_days)))).date().isoformat()
        url = f"{self.BASE_URL}/fixtures/between/{start}/{end}/{tid}"
        params = {"api_token": self._token, "include": "participants;scores"}
        try:
            with self._client() as c:
                r = c.get(url, params=params)
                r.raise_for_status()
                payload = r.json()
        except Exception as e:
            logger.info("[SPORTMONKS] team fixtures failed team_id=%s: %s", tid, e)
            return None
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, list) or not data:
            return {"team_id": tid, "fixtures": [], "summary": {"played": 0}}

        fixtures = [x for x in data if isinstance(x, dict)]
        fixtures.sort(key=lambda x: str(x.get("starting_at") or ""), reverse=True)

        out_rows: list[dict[str, Any]] = []
        gf = ga = w = d = l = 0
        played = 0
        for fx in fixtures:
            if played >= int(limit):
                break
            state_id = _safe_int(fx.get("state_id"))
            if state_id not in (5, 6, 7, 8, None):
                continue
            parts = fx.get("participants") if isinstance(fx.get("participants"), list) else []
            home_id = away_id = None
            for p in parts:
                if not isinstance(p, dict):
                    continue
                meta = p.get("meta") if isinstance(p.get("meta"), dict) else {}
                loc = str(meta.get("location") or p.get("location") or "").strip().lower()
                pid = _safe_int(p.get("id"))
                if loc == "home":
                    home_id = pid
                elif loc == "away":
                    away_id = pid

            sh = sa = None
            scores = fx.get("scores") if isinstance(fx.get("scores"), list) else []
            for sc in scores:
                if not isinstance(sc, dict):
                    continue
                scr = sc.get("score") if isinstance(sc.get("score"), dict) else {}
                hh = scr.get("home") if isinstance(scr, dict) else sc.get("home_score")
                aa = scr.get("away") if isinstance(scr, dict) else sc.get("away_score")
                hi = _safe_int(hh)
                ai = _safe_int(aa)
                if hi is None or ai is None:
                    continue
                sh, sa = hi, ai
            if sh is None or sa is None:
                continue

            is_home = home_id == tid
            is_away = away_id == tid
            if not is_home and not is_away:
                continue
            played += 1
            if is_home:
                gf += sh
                ga += sa
                if sh > sa:
                    w += 1
                elif sh == sa:
                    d += 1
                else:
                    l += 1
            else:
                gf += sa
                ga += sh
                if sa > sh:
                    w += 1
                elif sa == sh:
                    d += 1
                else:
                    l += 1
            out_rows.append(
                {
                    "fixture_id": _safe_int(fx.get("id")),
                    "starting_at": fx.get("starting_at"),
                    "result_info": fx.get("result_info"),
                    "is_home": is_home,
                    "goals_for": (sh if is_home else sa),
                    "goals_against": (sa if is_home else sh),
                }
            )

        avg_gf = round(gf / played, 3) if played else None
        avg_ga = round(ga / played, 3) if played else None
        points = w * 3 + d
        ppg = round(points / played, 3) if played else None
        return {
            "team_id": tid,
            "fixtures": out_rows,
            "summary": {
                "played": played,
                "w": w,
                "d": d,
                "l": l,
                "points": points,
                "ppg": ppg,
                "gf": gf,
                "ga": ga,
                "avg_gf": avg_gf,
                "avg_ga": avg_ga,
            },
        }

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

    @staticmethod
    def map_winline_match_to_fixture_from_window(
        *,
        winline_home: str,
        winline_away: str,
        fixtures: list[SportmonksFixtureLite],
    ) -> SportmonksFixtureLite | None:
        """Match by normalized team names within a fixture window (date-based)."""
        h = _norm_team(winline_home)
        a = _norm_team(winline_away)
        if not h or not a:
            return None
        for fx in fixtures:
            if _norm_team(fx.home_team) == h and _norm_team(fx.away_team) == a:
                return fx
        for fx in fixtures:
            if h in _norm_team(fx.home_team) and a in _norm_team(fx.away_team):
                return fx
        return None

