#!/usr/bin/env python3
"""Print A/B/C Winline live football match catalog (raw → norm → freshness). No bot/signals."""

from __future__ import annotations

import asyncio
import os
import sys

# Optional: same env as production
if __name__ == "__main__":
    root = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
    if root not in sys.path:
        sys.path.insert(0, root)

from app.core.config import get_settings
from app.services.winline_live_catalog_report import build_winline_football_live_catalog_report


def _print_block(title: str, rows: list) -> None:
    print()
    print("=" * 80)
    print(title)
    print("=" * 80)
    for r in rows:
        print(r.one_line)


async def _main() -> int:
    get_settings()  # load .env
    rep = await build_winline_football_live_catalog_report()
    if rep.error:
        print("ERROR:", rep.error)
        if not rep.group_a:
            return 1
    print("prescan", rep.prescan, "| tipline", rep.tipline_ok, "| meta", rep.scan_meta)
    print()
    print("COUNTS: raw (football) =", rep.football_raw_count)
    print("        after normalization    =", rep.after_norm_count)
    print("        after freshness (fresh)  =", rep.after_freshness_ok_count)
    _print_block("A) live_matches_detected_raw (all football in WS snapshot)", rep.group_a)
    _print_block("B) live_matches_after_normalization (bridge accepted event row)", rep.group_b)
    _print_block("C) live_matches_after_freshness (not stale)", rep.group_c)
    print()
    print("— Expected (screenshot hints) found:")
    for x in rep.expected_matched:
        print("  +", x)
    print("— Not found in raw list (hint):")
    for x in rep.expected_unmatched:
        print("  −", x)
    return 0 if not rep.error else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
