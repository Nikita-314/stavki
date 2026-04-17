"""Decode Winline websocket binary payloads used for TipLine menu + LIVE feed.

Mirrors `main.*.js` parsers:
- Step **16** (`GET_MENU`): sports + TipLine rows (`getTipLine` / `Je`) + TVs + variables.
- Step **4** (`STEPS.LIVE.ID`): `ho.parse` after a fixed **12-byte** prefix; inner records use
  `STEPS.LIVE.*` substeps (`fn` / `qn`).

This module is **bytes-in / structures-out** only (no network I/O).
"""

from __future__ import annotations

import re
import struct
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


class _Reader:
    __slots__ = ("_b", "o")

    def __init__(self, data: bytes, offset: int = 0) -> None:
        self._b = data
        self.o = offset

    def u8(self) -> int:
        v = self._b[self.o]
        self.o += 1
        return v

    def u16(self) -> int:
        v = self._b[self.o] + 256 * self._b[self.o + 1]
        self.o += 2
        return v

    def u32(self) -> int:
        v = struct.unpack_from("<I", self._b, self.o)[0]
        self.o += 4
        return v

    def i32(self) -> int:
        v = struct.unpack_from("<i", self._b, self.o)[0]
        self.o += 4
        return v

    def utf(self) -> str:
        n = self.u16()
        raw = self._b[self.o : self.o + n]
        self.o += n
        if 27 in raw:
            raw = raw[: raw.index(27)]
        return raw.decode("utf-8", "replace")


def _parse_mn_tv_block(r: _Reader) -> None:
    """`Mn($, g)` TV/widget TLV block before live event numerics."""
    span = r.u16()
    end = r.o + span
    while r.o < end:
        rt = r.u8()
        lt = r.u8()
        if rt == 1:
            sub_end = r.o + lt
            while r.o < sub_end:
                r.u16()
        elif rt == 2:
            r.u8()
            r.u8()
        elif rt == 3:
            r.u8()
        else:
            r.o += lt


def _map_id_tip_event(id_tip_event_src: int) -> int:
    if id_tip_event_src == 51:
        return 5
    if id_tip_event_src == 61:
        return 6
    if id_tip_event_src == 71:
        return 7
    return id_tip_event_src


def parse_menu_step16_tippeline(body: bytes) -> dict[int, dict[str, Any]]:
    """Parse GET_MENU (step 16) payload: TipLine dictionary keyed by TipRadar id (`Je.id`)."""
    r = _Reader(body)
    n_sports = r.u32()
    for _ in range(n_sports):
        sid = r.i32()
        sort = r.i32()
        _name = r.utf()
        if sort < 100:
            sort *= 100
        if sid == 110:
            sort *= 1_000_000
        for _j in range(9):
            r.utf()
    n_tips = r.u32()
    tips: dict[int, dict[str, Any]] = {}
    for _ in range(n_tips):
        tid = r.u32()
        r.utf()
        r.u32()
        r.u32()
        id_tip_event_src = r.u32()
        free_text_r = r.utf()
        id_tip_event = _map_id_tip_event(id_tip_event_src)
        r_row = [r.utf() for _ in range(30)]
        tips[tid] = {
            "id": tid,
            "idTipEventSrc": id_tip_event_src,
            "idTipEvent": id_tip_event,
            "freeTextR": free_text_r,
            "R": r_row,
        }
    n_tv = r.u32()
    for _ in range(n_tv):
        r.u32()
        r.utf()
    n_vars = r.u32()
    for _ in range(n_vars):
        r.u8()
        r.u32()
    return tips


LIVE_CHAMPIONSHIP_GET = 2
LIVE_EVENT_GET = 3
LIVE_EVENT_UPDATE = 4
LIVE_LINE_GET = 5
LIVE_COUNT_LINE_TO_EVENT = 6

LINE_DELETE_STATE = 5
EVENT_STATE_DELETE = 3


@dataclass
class LiveChunk:
    championships: list[dict[str, Any]] = field(default_factory=list)
    events: dict[int, dict[str, Any]] = field(default_factory=dict)
    lines: list[dict[str, Any]] = field(default_factory=list)


def _read_event_get(r: _Reader) -> dict[str, Any] | None:
    ev: dict[str, Any] = {}
    ev["id"] = r.i32()
    ev["idRadar"] = r.u32()
    ev["idNative"] = r.u32()
    ev["provider"] = r.u8()
    ev["category"] = r.u8()
    _parse_mn_tv_block(r)
    ev["numer"] = r.u16()
    ev["duration"] = r.u8()
    state = r.i32()
    ev["state"] = state
    if state > EVENT_STATE_DELETE:
        return None
    ev["idChampionship"] = r.u32()
    m1 = r.utf()
    m2 = r.utf()
    ev["members"] = [m1, m2]
    ts = r.u32()
    ev["date"] = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    src = r.utf()
    ev["sourceTime"] = src
    time_s = re.sub(r"[-:][0-9]+", "", src)
    if time_s and time_s[0].isdigit() and time_s[-1].isdigit():
        time_s = time_s + "'"
    ev["time"] = time_s
    r.u8()
    r.u8()
    r.u8()
    r.u8()
    r.u8()
    r.utf()
    r.utf()
    r.utf()
    r.u8()
    ev["isLive"] = 1
    return ev


def _read_event_update(r: _Reader) -> dict[str, Any] | None:
    ev: dict[str, Any] = {"id": r.i32()}
    _parse_mn_tv_block(r)
    ev["numer"] = r.u16()
    ev["duration"] = r.u8()
    state = r.i32()
    ev["state"] = state
    if state > EVENT_STATE_DELETE:
        return None
    src = r.utf()
    ev["sourceTime"] = src
    z = re.sub(r"[-:][0-9]+", "", src)
    if z and (z[0].isdigit() or "OT" in z or "\u041e\u0422" in z) and z[-1].isdigit():
        z = z + "'"
    ev["time"] = z
    r.u8()
    r.u8()
    r.u8()
    r.u8()
    r.u8()
    r.utf()
    r.utf()
    r.utf()
    r.u8()
    ev["isLive"] = 1
    return ev


def _read_line_live(r: _Reader) -> tuple[str, int | dict[str, Any]]:
    lid = r.u32()
    st = r.u8()
    if st == LINE_DELETE_STATE:
        return "del", lid
    line: dict[str, Any] = {
        "id": lid,
        "state": st,
        "idEvent": r.u32(),
        "countV": r.u8(),
    }
    v: list[float] = []
    for _ in range(min(int(line["countV"]), 31)):
        v.append(r.u16() / 100.0)
    line["V"] = v
    line["idTipMarket"] = r.u16()
    line["koef"] = r.utf()
    line["favorite"] = r.u8()
    line["isLive"] = 1
    return "line", line


def _read_championship(r: _Reader) -> dict[str, Any]:
    return {
        "id": r.u32(),
        "idSport": r.u32(),
        "idSort": r.i32(),
        "idCountry": r.u32(),
        "code": r.u8(),
        "name": r.utf(),
        "levelOnSite": r.u8(),
        "idSortLevel": r.u32(),
        "idSortNewLevel": r.u32(),
    }


def parse_live_step4_body(body: bytes, *, header_skip: int = 12) -> LiveChunk:
    """Parse LIVE (`ho`) payload **without** outer u16 step prefix (caller strips first 2 bytes)."""
    r = _Reader(body, header_skip)
    out = LiveChunk()
    while r.o < len(r._b):
        step = r.u8()
        if step == LIVE_CHAMPIONSHIP_GET:
            out.championships.append(_read_championship(r))
        elif step == LIVE_EVENT_GET:
            ev = _read_event_get(r)
            if ev:
                out.events[ev["id"]] = ev
        elif step == LIVE_EVENT_UPDATE:
            ev = _read_event_update(r)
            if ev:
                merged = out.events.get(ev["id"], {})
                merged.update(ev)
                out.events[ev["id"]] = merged
        elif step == LIVE_LINE_GET:
            kind, obj = _read_line_live(r)
            if kind == "line":
                out.lines.append(obj)
        elif step == LIVE_COUNT_LINE_TO_EVENT:
            r.u32()
            r.u8()
        else:
            break
    return out


def attach_tip_templates(
    lines: list[dict[str, Any]],
    tips: dict[int, dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[int]]:
    """Merge decoded line rows with TipLine templates (real metadata only)."""
    attached: list[dict[str, Any]] = []
    missing: list[int] = []
    for ln in lines:
        mid = int(ln["idTipMarket"])
        tip = tips.get(mid)
        if tip is None:
            missing.append(mid)
            continue
        row = dict(ln)
        row["freeTextR"] = tip["freeTextR"]
        row["R"] = tip["R"]
        row["idTipEvent"] = tip["idTipEvent"]
        row["countV"] = len(row.get("V") or [])
        attached.append(row)
    return attached, missing
