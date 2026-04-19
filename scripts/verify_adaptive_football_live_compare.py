#!/usr/bin/env python3
"""One live fetch → same post-integrity candidates → OFF vs ON adaptive scoring (dry, no Telegram/ingest)."""

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
    res = await AutoSignalService().run_single_cycle(sm, bot, dry_run=True, adaptive_compare_only=True)
    cmp = res.football_adaptive_compare or {}
    print("=== assessment ===")
    print(json.dumps(cmp.get("assessment"), ensure_ascii=False, indent=2))
    print()
    print("=== aggregate OFF ===")
    print(json.dumps(cmp.get("aggregate_off"), ensure_ascii=False, indent=2))
    print()
    print("=== aggregate ON ===")
    print(json.dumps(cmp.get("aggregate_on"), ensure_ascii=False, indent=2))
    print()
    print("=== max_abs_adjustment_observed ===")
    print(cmp.get("max_abs_adjustment_observed"))
    print()
    print("=== changed_cases (material diffs only) ===")
    print(json.dumps(cmp.get("changed_cases") or [], ensure_ascii=False, indent=2))
    print()
    print("=== per_match (truncated to 40 rows) ===")
    for row in (cmp.get("per_match") or [])[:40]:
        print(json.dumps(row, ensure_ascii=False))
    return 0 if res.fetch_ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
