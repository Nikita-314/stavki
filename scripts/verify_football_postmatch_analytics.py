#!/usr/bin/env python3
"""Run against production .env: E2E check of post-match reason analytics (football, settled)."""

from __future__ import annotations

import asyncio
import os
import sys

# Project root
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
os.chdir(_ROOT)


async def _main() -> int:
    from app.core.config import get_settings
    from app.db.session import get_sessionmaker
    from app.services.football_signal_outcome_reason_service import build_football_postmatch_verify_report

    limit = 200
    if len(sys.argv) > 1:
        try:
            limit = int(sys.argv[1])
        except ValueError:
            print("Usage: python3 scripts/verify_football_postmatch_analytics.py [limit]", file=sys.stderr)
            return 1

    settings = get_settings()
    sm = get_sessionmaker(settings.database_url)
    async with sm() as session:
        text = await build_football_postmatch_verify_report(session, limit=limit, detail_count=10)
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
