"""Winline live WebSocket → raw events+lines payload (same shape as manual capture).

Reuses the binary decoder from `winline_ws_live_binary_codec` and mirrors
`scripts/capture_winline_manual_line_payload.py` (prescan, TipLine, LIVE step 4).
I/O: network only; no file upload.
"""

from __future__ import annotations

import asyncio
import base64
import gzip
import logging
import struct
import time
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import websockets
from websockets.exceptions import WebSocketException

from app.core.config import Settings, get_settings
from app.services.winline_ws_live_binary_codec import (
    LiveChunk,
    attach_tip_templates,
    parse_live_step4_body,
    parse_menu_step16_tippeline,
)

logger = logging.getLogger(__name__)

_FOOTBALL_SPORT = 1


@dataclass
class _Accum:
    tips: dict[int, dict[str, Any]] = field(default_factory=dict)
    champs: dict[int, dict[str, Any]] = field(default_factory=dict)
    events: dict[int, dict[str, Any]] = field(default_factory=dict)
    lines: list[dict[str, Any]] = field(default_factory=list)


def _enc_event(event_id: int, mode: int = 0) -> str:
    buf = bytearray()
    buf.extend(struct.pack("<I", event_id))
    buf.append(mode)
    return base64.b64encode(bytes(buf)).decode("ascii")


def _step_raw(raw: bytes) -> int:
    return raw[0] + 256 * raw[1]


def _is_football_event(ev: dict[str, Any], champs: dict[int, dict[str, Any]]) -> bool:
    cid = ev.get("idChampionship")
    if cid is None:
        return False
    c = champs.get(int(cid))
    if not c:
        return False
    try:
        return int(c.get("idSport", 0)) == _FOOTBALL_SPORT
    except (TypeError, ValueError):
        return False


def _build_multi_event_raw_payload(
    acc: _Accum,
    event_ids: list[int],
) -> dict[str, Any] | None:
    out_events: list[dict[str, Any]] = []
    for eid in event_ids:
        ev = acc.events.get(int(eid))
        if not ev:
            continue
        rec = enrich_winline_event_for_ingest(ev, acc.champs)
        if not rec:
            continue
        out_events.append(rec)
    if not out_events:
        return None
    eid_set = {int(x) for x in event_ids}
    line_rows = [ln for ln in acc.lines if int(ln.get("idEvent") or 0) in eid_set]
    if not acc.tips:
        return None
    merged, missing = attach_tip_templates(line_rows, acc.tips)
    if missing and not merged:
        return None
    if not merged and line_rows:
        return None
    if not merged:
        return None
    ch_out: list[dict[str, Any]] = []
    for e in out_events:
        cid = int(e.get("idChampionship") or 0)
        c = acc.champs.get(cid)
        if c:
            ch_out.append({"id": int(c["id"]), "name": str(c.get("name", ""))})
    if not ch_out and out_events and acc.champs:
        cid0 = int(out_events[0].get("idChampionship") or 0)
        c0 = acc.champs.get(cid0)
        if c0:
            ch_out = [{"id": int(c0["id"]), "name": str(c0.get("name", ""))}]

    return {
        "source_name": "winline_live",
        "events": out_events,
        "championships": ch_out,
        "lines": merged,
    }


async def _recv_gzip_step(ws: websockets.WebSocketClientProtocol, timeout: float) -> tuple[int, bytes]:
    raw = gzip.decompress(await asyncio.wait_for(ws.recv(), timeout=timeout))
    st = _step_raw(raw)
    return st, raw[2:]


def _ingest_step4_body(acc: _Accum, body: bytes) -> None:
    chunk: LiveChunk = parse_live_step4_body(body)
    for c in chunk.championships:
        acc.champs[int(c["id"])] = c
    for _eid, ev in chunk.events.items():
        acc.events[int(_eid)] = ev
    for ln in chunk.lines:
        acc.lines.append(ln)


def enrich_winline_event_for_ingest(ev: dict[str, Any], champs: dict[int, dict[str, Any]]) -> dict[str, Any] | None:
    """Event dict from live step-4 has no `idSport`; add it from the championship (same as fetch payload)."""
    eid = int(ev.get("id") or 0)
    if eid <= 0:
        return None
    if not ev.get("members") or not isinstance(ev.get("members"), list) or len(ev["members"]) < 2:
        return None
    c = champs.get(int(ev.get("idChampionship") or 0))
    id_sport = int(c.get("idSport", _FOOTBALL_SPORT)) if c else _FOOTBALL_SPORT
    src_time = str(ev.get("sourceTime") or ev.get("time") or "").strip()
    # Parse minute from Winline clock strings like "1Т 12'", "2Т 57'", "90'" etc.
    minute: int | None = None
    period: str | None = None
    nums = re.findall(r"(\d{1,3})", src_time)
    if nums:
        try:
            # Winline strings often look like "2Т 72'" or "1Т 45'"; take the last number as minute.
            minute = int(nums[-1])
        except ValueError:
            minute = None
    if "2т" in src_time.lower() or "2h" in src_time.lower():
        period = "2H"
        if minute is not None and minute < 45:
            minute = 45 + minute
    elif "1т" in src_time.lower() or "1h" in src_time.lower():
        period = "1H"
    # Try to parse current score from tail UTF fields if present.
    score_home: int | None = None
    score_away: int | None = None
    tail_utf = ev.get("tail_utf") if isinstance(ev.get("tail_utf"), list) else []
    for t in tail_utf:
        if not isinstance(t, str):
            continue
        mm = re.search(r"\b(\d{1,2})\s*[:\-–]\s*(\d{1,2})\b", t)
        if mm:
            score_home, score_away = int(mm.group(1)), int(mm.group(2))
            break
    return {
        "id": eid,
        "idSport": id_sport,
        "idChampionship": int(ev.get("idChampionship") or 0),
        "date": ev.get("date")
        or datetime.now(tz=timezone.utc).isoformat(),
        "isLive": int(ev.get("isLive") or 0) or 1,
        "members": [str(ev["members"][0]), str(ev["members"][1])],
        "time": ev.get("time"),
        "sourceTime": ev.get("sourceTime"),
        "numer": ev.get("numer"),
        "score_home": score_home,
        "score_away": score_away,
        "minute": minute,
        "period": period,
        "live_state": ev.get("state"),
    }


async def winline_websocket_scan_accumulator(
    s: Settings,
    *,
    prescan: int | None = None,
) -> tuple[_Accum | None, str | None]:
    """Read WS prescan (step 16 tipline + coalesced step-4). Used by live feed and debug catalog."""
    tmo = max(5.0, float(s.winline_live_recv_timeout_seconds or 30))
    nscan = int(prescan if prescan is not None else (s.winline_live_max_prescan or 200))
    acc = _Accum()
    total_cap = max(45.0, float(s.winline_live_total_timeout_seconds or 180))
    # Leave headroom for event.plus + bridge inside the outer wait_for(cap).
    scan_wall = max(25.0, min(95.0, total_cap * 0.42))
    t_wall0 = time.monotonic()
    try:
        async with websockets.connect(  # type: ignore[call-overload]
            s.winline_live_ws_url,
            ping_interval=None,
            close_timeout=5,
            open_timeout=float(s.winline_live_connect_timeout_seconds or 25),
        ) as ws:
            for cmd in ("lang", "RU", "data", "WINLINE", "getdate"):
                await ws.send(str(cmd))
            for i in range(nscan):
                if time.monotonic() - t_wall0 > scan_wall:
                    logger.info(
                        "[FOOTBALL][WINLINE_LIVE] prescan_wall_budget_exhausted iter=%s budget_s=%.1f",
                        i,
                        scan_wall,
                    )
                    break
                step, body = await _recv_gzip_step(ws, tmo)
                if step == 16 and not acc.tips:
                    acc.tips = parse_menu_step16_tippeline(body)
                if step == 4:
                    _ingest_step4_body(acc, body)
    except (TimeoutError, asyncio.TimeoutError, OSError) as e:
        logger.info("[FOOTBALL][WINLINE_LIVE] connect_or_scan_failed: %s", e)
        return None, f"winline_connect_or_read_failed: {e!s}"
    except WebSocketException as e:
        logger.info("[FOOTBALL][WINLINE_LIVE] ws_error: %s", e)
        return None, f"winline_websocket: {e!s}"
    except Exception as e:  # noqa: BLE001
        logger.exception("[FOOTBALL][WINLINE_LIVE] unexpected: %s", e)
        return None, f"winline_unexpected: {e!s}"
    return acc, None


async def _event_plus_lines_for_event(
    s: Settings,
    event_id: int,
    tmo: float,
    postscan: int,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    async with websockets.connect(  # type: ignore[call-overload]
        s.winline_live_ws_url,
        ping_interval=None,
        close_timeout=5,
        open_timeout=float(s.winline_live_connect_timeout_seconds or 25),
    ) as ws:
        for cmd in ("lang", "RU", "data", "WINLINE", "getdate"):
            await ws.send(str(cmd))
        await ws.send("event.plus")
        await ws.send(_enc_event(int(event_id), 0))
        for _ in range(max(1, postscan)):
            step, body = await _recv_gzip_step(ws, tmo)
            if step != 4:
                continue
            chunk2: LiveChunk = parse_live_step4_body(body)
            for ln in chunk2.lines:
                if int(ln.get("idEvent") or 0) == int(event_id):
                    out.append(ln)
            if len(out) >= 50:
                break
    return out


class WinlineLiveFeedService:
    """Fetch a raw Winline `events+lines+championships` dict for current football live."""

    def __init__(self) -> None:
        self._settings: Settings = get_settings()

    async def fetch_football_live_raw_payload(
        self,
        settings: Settings | None = None,
    ) -> tuple[dict[str, Any] | None, str | None]:
        """Return (raw_payload, error) — raw is ingestible by `WinlineRawLineBridgeService`."""
        s = settings or self._settings
        if not s.football_live_winline_primary or not s.winline_live_ws_url.strip():
            return None, "winline_disabled"
        cap = max(25.0, float(s.winline_live_total_timeout_seconds or 90))
        try:
            return await asyncio.wait_for(self._do_fetch_football(s), timeout=cap)
        except asyncio.TimeoutError:
            return None, "winline_fetch_timeout"

    async def _do_fetch_football(self, s: Settings) -> tuple[dict[str, Any] | None, str | None]:
        tmo = max(5.0, float(s.winline_live_recv_timeout_seconds or 30))
        acc, err0 = await winline_websocket_scan_accumulator(s)
        if acc is None:
            return None, err0
        if not acc.tips:
            return None, "winline_tipline_missing"
        fball_ids = [
            int(eid) for eid, ev in acc.events.items() if _is_football_event(ev, acc.champs)
        ]
        fball_ids.sort()
        if not fball_ids:
            return None, "winline_football_live_not_seen"

        max_ev = int(s.winline_live_max_football_events or 0)
        pick = fball_ids if max_ev <= 0 else fball_ids[:max_ev]
        need = int(s.winline_live_event_plus_min_lines or 2)
        rounds = int(s.winline_live_event_plus_rounds or 2)
        if int(s.winline_live_event_plus_postscan or 0) > 0:
            ev_plus_cap = max(14.0, min(36.0, 0.18 * float(s.winline_live_total_timeout_seconds or 180)))
            post_n = min(int(s.winline_live_event_plus_postscan or 80), 45)
            for eid in pick[:rounds]:
                n_here = sum(1 for x in acc.lines if int(x.get("idEvent") or 0) == int(eid))
                if n_here >= need:
                    continue
                try:
                    extra = await asyncio.wait_for(
                        _event_plus_lines_for_event(
                            s,
                            int(eid),
                            tmo,
                            post_n,
                        ),
                        timeout=ev_plus_cap,
                    )
                    acc.lines.extend(extra)
                except asyncio.TimeoutError:
                    logger.info(
                        "[FOOTBALL][WINLINE_LIVE] event_plus_timeout eid=%s cap_s=%.1f",
                        eid,
                        ev_plus_cap,
                    )
                except (TimeoutError, OSError) as e:
                    logger.info("[FOOTBALL][WINLINE_LIVE] event_plus failed eid=%s: %s", eid, e)
                except Exception:  # noqa: BLE001
                    logger.info("[FOOTBALL][WINLINE_LIVE] event_plus other_error eid=%s", eid, exc_info=True)
        eid_set = {int(x) for x in pick}
        f_lines = [ln for ln in acc.lines if int(ln.get("idEvent") or 0) in eid_set]
        if not f_lines:
            return None, "winline_no_lines_for_football"
        raw = _build_multi_event_raw_payload(acc, pick)
        if raw is None or not raw.get("lines"):
            return None, "winline_tipline_incomplete"
        return raw, None
