#!/usr/bin/env python3
"""One dry-run cycle: print Winline live → signal pipeline aggregate (no channel/DB writes)."""

from __future__ import annotations

import asyncio
import json
import os
import sys

REPO = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from aiogram import Bot

from app.core.config import get_settings
from app.db.session import create_engine, create_sessionmaker
from app.services.auto_signal_service import AutoSignalService


async def _main() -> int:
    s = get_settings()
    sm = create_sessionmaker(create_engine(s.database_url, echo=False))
    bot = Bot(token=s.bot_token)
    res = await AutoSignalService().run_single_cycle(sm, bot, dry_run=True)
    dbg = res.football_cycle_debug or {}
    agg = dbg.get("football_pipeline_aggregate") or {}
    print("=== football_pipeline_aggregate ===")
    print(json.dumps(agg, ensure_ascii=False, indent=2))
    print()
    print("=== top_10_live_pipeline_lines ===")
    for ln in (dbg.get("top_10_live_pipeline_lines") or [])[:10]:
        print(ln)
    print()
    print("=== sendable_live_idea_lines (normal+soft) ===")
    for ln in (dbg.get("sendable_live_idea_lines") or []):
        print(ln)
    print()
    bp = dbg.get("bottleneck_no_sendable_pipeline_ru")
    if bp:
        print("bottleneck_no_sendable:", bp)
    return 0 if res.fetch_ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
