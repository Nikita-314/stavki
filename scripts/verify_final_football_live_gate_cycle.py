#!/usr/bin/env python3
"""One combat football live cycle + JSON report for final_live_send_gate (same path as ▶️ Старт)."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from collections import Counter
from datetime import datetime, timezone

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from aiogram import Bot

from app.core.config import get_settings
from app.db.session import create_engine, create_sessionmaker
from app.services.auto_signal_service import AutoSignalService
from app.services.football_live_session_service import FootballLiveSessionService
from app.services.signal_runtime_settings_service import SignalRuntimeSettingsService


def _cards_in_send_path(trace: list[dict]) -> list[dict]:
    out = []
    for tr in trace or []:
        if not isinstance(tr, dict):
            continue
        bet = str(tr.get("bet") or "").lower()
        if tr.get("final_outcome") != "sent":
            continue
        if "карточ" in bet or "card" in bet and "booking" in bet:
            out.append(tr)
    return out


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = get_settings()
    print("[verify] ts=", datetime.now(tz=timezone.utc).isoformat(), flush=True)

    bot = Bot(token=settings.bot_token)
    engine = create_engine(settings.database_url, echo=False)
    sessionmaker = create_sessionmaker(engine)

    rts = SignalRuntimeSettingsService()
    rts.enable_sport("football")
    rts.start()
    dm = int(settings.football_live_session_duration_minutes or 15)
    FootballLiveSessionService().start_session(duration_minutes=dm)

    res = await asyncio.wait_for(
        AutoSignalService().run_single_cycle(sessionmaker, bot, dry_run=False),
        timeout=240,
    )
    AutoSignalService().log_football_cycle_trace(res)

    d = res.football_cycle_debug or {}
    fg = d.get("final_live_send_gate") or {}
    per = fg.get("per_match") or []
    trace = d.get("combat_delivery_trace") or []

    agg = d.get("football_pipeline_aggregate") or {}
    matches_after_freshness = int(agg.get("matches_after_freshness") or 0)
    matches_scored = [m for m in per if float(m.get("best_scored_candidate_score") or 0) > 0 or m.get("finalists_found_before_gate")]

    # One signal per match in trace (created rows)
    eids_created = [str(t.get("event_id")) for t in trace if t.get("created_in_db") == "yes"]
    dup_eids = [e for e, n in Counter(eids_created).items() if n > 1]

    report = {
        "created_signals_count": res.created_signals_count,
        "notifications_sent_count": res.notifications_sent_count,
        "message": res.message,
        "live_matches_total": fg.get("live_matches_total"),
        "matches_after_freshness": matches_after_freshness,
        "matches_reaching_final_gate": fg.get("matches_reaching_final_gate"),
        "matches_blocked_by_final_gate": fg.get("matches_blocked_by_final_gate"),
        "matches_sent_after_final_gate": fg.get("matches_sent_after_final_gate"),
        "matches_with_scored_candidates": len(matches_scored),
        "blocked_cards_or_special_hits": fg.get("blocked_cards_or_special_hits"),
        "main_market_token_hits": fg.get("main_market_token_hits"),
        "suspicious_core_signals_blocked": fg.get("suspicious_core_signals_blocked"),
        "core_live_extra_sanity_blocked": fg.get("core_live_extra_sanity_blocked"),
        "late_game_live_sanity_blocked": fg.get("late_game_live_sanity_blocked"),
        "matches_sent_after_timing_sanity": fg.get("matches_sent_after_timing_sanity"),
        "per_match_table": per,
        "combat_delivery_trace": trace,
        "max_one_signal_per_match_in_trace": len(dup_eids) == 0,
        "duplicate_event_ids_if_any": dup_eids,
        "cards_markets_in_sent_path_count": len(_cards_in_send_path(trace)),
    }
    print(json.dumps(report, ensure_ascii=False, default=str, indent=2))
    await bot.session.close()
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
