#!/usr/bin/env python3
"""Capture real Winline line JSON via websocket (TipLine dictionary + LIVE `fn` rows).

Writes `runtime/manual_winline/line_payload.json` + `.meta.json` checksum.

Requires: `websockets`, network access to `wss://wss.winline.ru`.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import gzip
import hashlib
import json
import struct
from datetime import datetime, timezone
from pathlib import Path

import websockets

from app.services.winline_ws_live_binary_codec import (
    attach_tip_templates,
    parse_live_step4_body,
    parse_menu_step16_tippeline,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _enc_event(event_id: int, mode: int = 0) -> str:
    buf = bytearray()
    buf.extend(struct.pack("<I", event_id))
    buf.append(mode)
    return base64.b64encode(bytes(buf)).decode("ascii")


def _step(raw: bytes) -> int:
    return raw[0] + 256 * raw[1]


async def _recv_msg(ws: websockets.WebSocketClientProtocol) -> tuple[int, bytes]:
    raw = gzip.decompress(await asyncio.wait_for(ws.recv(), timeout=60))
    return _step(raw), raw


async def capture_payload(
    *,
    event_id: int,
    max_prescan: int,
    max_postscan: int,
    min_raw_lines: int,
) -> dict:
    tips: dict[int, dict] = {}
    champs: dict[int, dict] = {}
    events: dict[int, dict] = {}
    lines_by_id: dict[int, dict] = {}
    cached_target_event: dict | None = None

    uri = "wss://wss.winline.ru/data_ng?client=newsite&nb=true"
    async with websockets.connect(uri, ping_interval=None, close_timeout=5, open_timeout=25) as ws:
        for cmd in ("lang", "RU", "data", "WINLINE", "getdate"):
            await ws.send(cmd)

        target_ok = False
        for _ in range(max_prescan):
            step, raw = await _recv_msg(ws)
            body = raw[2:]
            if step == 16 and not tips:
                tips = parse_menu_step16_tippeline(body)
            elif step == 4:
                chunk = parse_live_step4_body(body)
                for c in chunk.championships:
                    champs[int(c["id"])] = c
                events.update(chunk.events)
                te = events.get(event_id)
                if (
                    te
                    and isinstance(te.get("members"), list)
                    and len(te["members"]) >= 2
                ):
                    cached_target_event = dict(te)
                    target_ok = True
                    break

        if not tips:
            raise RuntimeError("menu_step16_missing: no TipLine dictionary captured")
        if not target_ok:
            raise RuntimeError(f"event_get_missing: id={event_id} not seen in LIVE prescan burst")

        await ws.send("event.plus")
        await ws.send(_enc_event(event_id, 0))

        for _ in range(max_postscan):
            step, raw = await _recv_msg(ws)
            if step != 4:
                continue
            body = raw[2:]
            chunk = parse_live_step4_body(body)
            events.update(chunk.events)
            for ln in chunk.lines:
                if int(ln["idEvent"]) != event_id:
                    continue
                lines_by_id[int(ln["id"])] = ln
            if len(lines_by_id) >= min_raw_lines:
                break

    ev = cached_target_event or events.get(event_id)
    if not ev or not isinstance(ev.get("members"), list) or len(ev["members"]) < 2:
        raise RuntimeError("event_record_incomplete")

    champ = champs.get(int(ev["idChampionship"])) if ev.get("idChampionship") is not None else None
    id_sport = int(champ["idSport"]) if champ else 1

    raw_lines = list(lines_by_id.values())
    merged, missing = attach_tip_templates(raw_lines, tips)
    if missing:
        raise RuntimeError(f"tip_templates_missing_for_markets: {sorted(set(missing))[:20]}")

    championships_out = (
        [{"id": int(champ["id"]), "name": str(champ["name"])}] if champ else []
    )

    payload = {
        "source_name": "winline",
        "events": [
            {
                "id": int(ev["id"]),
                "idSport": id_sport,
                "idChampionship": int(ev["idChampionship"]),
                "date": ev["date"],
                "isLive": 1,
                "members": [str(ev["members"][0]), str(ev["members"][1])],
            }
        ],
        "championships": championships_out,
        "lines": merged,
    }
    return payload


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--event-id", type=int, default=15547811)
    ap.add_argument("--max-prescan", type=int, default=120)
    ap.add_argument("--max-postscan", type=int, default=250)
    ap.add_argument("--min-raw-lines", type=int, default=80)
    args = ap.parse_args()

    payload = asyncio.run(
        capture_payload(
            event_id=args.event_id,
            max_prescan=args.max_prescan,
            max_postscan=args.max_postscan,
            min_raw_lines=args.min_raw_lines,
        )
    )

    out_dir = _repo_root() / "runtime" / "manual_winline"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "line_payload.json"
    text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    path.write_text(text, encoding="utf-8")

    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    meta = {
        "origin": "manual_runtime_seed",
        "source_origin": "operator_uploaded_json",
        "source_mode": "semi_live_manual",
        "is_real_source": True,
        "fixture_match": False,
        "uploaded_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "checksum": digest,
        "file_path": str(path),
        "event_id": args.event_id,
    }
    (out_dir / "line_payload.meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"wrote {path} lines={len(payload['lines'])} sha256={digest}")


if __name__ == "__main__":
    main()
