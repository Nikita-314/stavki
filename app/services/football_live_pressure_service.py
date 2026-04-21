"""Lightweight live pressure stats provider (shots/corners) for football live strategies.

Goal: enable a strict LIVE TOTALS (over) strategy based on in-play pressure.
No new dependencies: uses stdlib urllib + asyncio.to_thread. Cached in-process.
"""

from __future__ import annotations

import asyncio
import json
import time
import urllib.request
import urllib.error
from dataclasses import dataclass
from typing import Any


def _norm(s: str) -> str:
    return (s or "").strip().lower().replace("ё", "е")


def _safe_int(v: object) -> int | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class FootballLivePressureSnapshot:
    source: str
    fetched_at_epoch: float
    home_team: str
    away_team: str
    shots_total: int | None
    shots_on_target: int | None
    corners: int | None
    raw: dict[str, Any] | None = None


class FootballLivePressureService:
    """Fetches pressure stats from Sofascore public endpoints (best-effort)."""

    _CACHE: dict[str, tuple[float, FootballLivePressureSnapshot]] = {}
    _TTL_SECONDS: float = 25.0

    async def get_pressure(
        self,
        *,
        home_team: str,
        away_team: str,
    ) -> FootballLivePressureSnapshot | None:
        key = f"{_norm(home_team)}|{_norm(away_team)}"
        now = time.time()
        hit = self._CACHE.get(key)
        if hit and (now - hit[0]) <= self._TTL_SECONDS:
            return hit[1]

        snap = await self._fetch_sofascore(home_team=home_team, away_team=away_team)
        if snap is None:
            return None
        self._CACHE[key] = (now, snap)
        return snap

    async def _fetch_sofascore(
        self,
        *,
        home_team: str,
        away_team: str,
    ) -> FootballLivePressureSnapshot | None:
        # 1) list live football events
        live_url = "https://api.sofascore.com/api/v1/sport/football/events/live"
        live_payload = await _get_json(live_url)
        if not isinstance(live_payload, dict):
            return None
        events = live_payload.get("events")
        if not isinstance(events, list) or not events:
            return None

        h0 = _norm(home_team)
        a0 = _norm(away_team)

        match_event_id: int | None = None
        for ev in events:
            if not isinstance(ev, dict):
                continue
            ht = ev.get("homeTeam") if isinstance(ev.get("homeTeam"), dict) else {}
            at = ev.get("awayTeam") if isinstance(ev.get("awayTeam"), dict) else {}
            hn = _norm(str(ht.get("name") or ""))
            an = _norm(str(at.get("name") or ""))
            # strict name match first; then a loose contains match (kept conservative)
            if (hn == h0 and an == a0) or (h0 and a0 and (h0 in hn and a0 in an)):
                match_event_id = _safe_int(ev.get("id"))
                break
        if match_event_id is None:
            return None

        # 2) fetch event statistics (endpoint variants exist; try a couple)
        stat_urls = [
            f"https://api.sofascore.com/api/v1/event/{match_event_id}/statistics",
            f"https://api.sofascore.com/api/v1/event/{match_event_id}/statistics/0",
        ]
        stat_payload: dict[str, Any] | None = None
        for u in stat_urls:
            p = await _get_json(u)
            if isinstance(p, dict) and (p.get("statistics") or p.get("statisticsItems")):
                stat_payload = p
                break
        if stat_payload is None:
            # No stats available right now
            return FootballLivePressureSnapshot(
                source="sofascore",
                fetched_at_epoch=time.time(),
                home_team=home_team,
                away_team=away_team,
                shots_total=None,
                shots_on_target=None,
                corners=None,
                raw=None,
            )

        shots_total, shots_on_target, corners = _extract_pressure_from_sofascore_stats(stat_payload)
        return FootballLivePressureSnapshot(
            source="sofascore",
            fetched_at_epoch=time.time(),
            home_team=home_team,
            away_team=away_team,
            shots_total=shots_total,
            shots_on_target=shots_on_target,
            corners=corners,
            raw=stat_payload,
        )


async def _get_json(url: str) -> Any:
    def _do() -> Any:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "stavki-bot/1.0 (+football-live-pressure)",
                "Accept": "application/json,text/plain,*/*",
                "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
                "Referer": "https://www.sofascore.com/",
                "Origin": "https://www.sofascore.com",
            },
            method="GET",
        )
        try:
            with urllib.request.urlopen(req, timeout=8) as resp:  # noqa: S310 (public endpoint)
                raw = resp.read()
        except urllib.error.HTTPError:
            return None
        except OSError:
            return None
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return None

    return await asyncio.to_thread(_do)


def _extract_pressure_from_sofascore_stats(payload: dict[str, Any]) -> tuple[int | None, int | None, int | None]:
    """Best-effort extractor. Sofascore stats shape varies; scan for known names/keys."""
    shots_total = None
    shots_on_target = None
    corners = None

    # Common shape: payload["statistics"] -> list of groups -> items with name + home/away values
    stats = payload.get("statistics")
    items: list[dict[str, Any]] = []
    if isinstance(stats, list):
        for g in stats:
            if not isinstance(g, dict):
                continue
            its = g.get("statisticsItems") or g.get("items") or g.get("statistics")
            if isinstance(its, list):
                for it in its:
                    if isinstance(it, dict):
                        items.append(it)
    if not items and isinstance(payload.get("statisticsItems"), list):
        items = [x for x in payload.get("statisticsItems") if isinstance(x, dict)]

    def _value_sum(it: dict[str, Any]) -> int | None:
        hv = it.get("home") if it.get("home") is not None else it.get("homeValue")
        av = it.get("away") if it.get("away") is not None else it.get("awayValue")
        hi = _safe_int(hv)
        ai = _safe_int(av)
        if hi is None and ai is None:
            return None
        return int((hi or 0) + (ai or 0))

    for it in items:
        nm = _norm(str(it.get("name") or it.get("key") or it.get("title") or ""))
        if not nm:
            continue
        if shots_on_target is None and ("shots on target" in nm or "on target" in nm):
            shots_on_target = _value_sum(it)
            continue
        if corners is None and ("corner" in nm or "углов" in nm):
            corners = _value_sum(it)
            continue
        if shots_total is None and (nm == "shots" or "total shots" in nm):
            shots_total = _value_sum(it)
            continue

    return shots_total, shots_on_target, corners

