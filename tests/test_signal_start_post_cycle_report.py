from __future__ import annotations

from unittest.mock import patch

from app.bot.handlers.debug import _format_live_session_start_post_cycle_report
from app.schemas.auto_signal import AutoSignalCycleResult
from app.services.signal_runtime_diagnostics_service import SignalRuntimeDiagnosticsService


def test_format_live_session_start_post_cycle_report_uses_diag_and_cres() -> None:
    cres = AutoSignalCycleResult(
        endpoint="wss://test",
        fetch_ok=True,
        preview_candidates=0,
        preview_skipped_items=0,
        created_signal_ids=[],
        created_signals_count=2,
        skipped_candidates_count=0,
        notifications_sent_count=1,
        preview_only=False,
        message="ok",
        raw_events_count=77,
    )
    fake_state = {
        "football_winline_football_event_count": 79,
        "football_live_freshness_live_events_accepted": 72,
        "football_live_cycle_after_integrity": 120,
        "football_live_s13_candidates": 35,
        "football_live_pacing_current_interval_seconds": 60,
    }
    with patch.object(SignalRuntimeDiagnosticsService, "get_state", return_value=fake_state):
        text = _format_live_session_start_post_cycle_report(cres)
    assert "79" in text
    assert "72" in text
    assert "120" in text
    assert "35" in text
    assert "2" in text
    assert "1" in text
    assert "Первый live-cycle завершён" in text


def test_format_live_session_start_post_cycle_report_fetch_not_ok_line() -> None:
    cres = AutoSignalCycleResult(
        endpoint=None,
        fetch_ok=False,
        preview_candidates=0,
        preview_skipped_items=0,
        created_signal_ids=[],
        created_signals_count=0,
        skipped_candidates_count=0,
        notifications_sent_count=0,
        preview_only=False,
        message="blocked_winline_live_unavailable",
        raw_events_count=0,
        rejection_reason="ws_timeout",
    )
    with patch.object(
        SignalRuntimeDiagnosticsService,
        "get_state",
        return_value={"football_live_pacing_current_interval_seconds": 60},
    ):
        text = _format_live_session_start_post_cycle_report(cres)
    assert "⚠️" in text
    assert "ws_timeout" in text
