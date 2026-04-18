from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from threading import Lock
from typing import Any


@dataclass
class SignalRuntimeDiagnosticsState:
    updated_at: str | None = None
    active_mode: str = "football"
    football_source: str | None = None
    football_fallback_source: str | None = None
    live_provider_name: str | None = None
    live_auth_status: str | None = None
    last_live_http_status: int | None = None
    last_live_endpoint: str | None = None
    last_live_error_body: str | None = None
    fallback_source_available: bool = False
    manual_production_fallback_allowed: bool = False
    source_mode: str | None = None
    is_real_source: bool = False
    source_origin: str | None = None
    upload_provenance_present: bool = False
    uploaded_at: str | None = None
    source_file_path: str | None = None
    source_checksum: str | None = None
    last_fetch_status: str | None = None
    last_error: str | None = None
    last_delivery_reason: str | None = None
    raw_events_count: int = 0
    normalized_markets_count: int = 0
    candidates_before_filter_count: int = 0
    candidates_after_filter_count: int = 0
    football_candidates_count: int = 0
    football_real_candidates_count: int = 0
    football_after_filter_count: int = 0
    football_after_integrity_count: int = 0
    dropped_invalid_market_mapping_count: int = 0
    dropped_invalid_total_scope_count: int = 0
    dropped_too_far_in_time_count: int = 0
    live_matches_count: int = 0
    near_matches_count: int = 0
    too_far_matches_count: int = 0
    selected_match_reason: str | None = None
    football_sent_count: int = 0
    final_signals_count: int = 0
    messages_sent_count: int = 0
    preview_only: bool = False
    fallback_used: bool = False
    note: str | None = None
    football_analytics_enabled: bool = True
    football_learning_enabled: bool = True
    football_learning_families_tracked: int = 0
    football_live_fields_in_last_cycle: bool = False
    football_injuries_data_available: bool = False
    football_line_movement_available: bool = False
    football_live_session_active: bool = False
    football_live_session_started_at: str | None = None
    football_live_session_expires_at: str | None = None
    football_live_session_last_cycle_at: str | None = None
    football_live_session_remaining_minutes: float | None = None
    football_live_signals_sent_session: int = 0
    football_live_telegram_sent_session: int = 0
    football_live_duplicate_ideas_blocked: int = 0
    football_live_sent_ideas_count: int = 0
    football_live_cycle_live_matches_found: int = 0
    football_live_cycle_candidates_before_filter: int = 0
    football_live_cycle_after_send_filter: int = 0
    football_live_cycle_after_integrity: int = 0
    football_live_cycle_after_score: int = 0
    football_live_cycle_new_ideas_sendable: int = 0
    football_live_cycle_duplicate_ideas_blocked: int = 0
    football_live_cycle_bottleneck: str | None = None
    football_live_effective_source: str | None = None
    football_live_last_notify_path: str | None = None
    football_live_source_timestamp: str | None = None
    football_live_source_age_seconds: float | None = None
    football_live_stale_source: bool = False
    football_live_source_freshness: str | None = None
    football_live_freshness_candidates_before: int = 0
    football_live_freshness_live_events_accepted: int = 0
    football_live_freshness_stale_events_dropped: int = 0
    football_live_freshness_stale_markets_dropped: int = 0


_STATE = SignalRuntimeDiagnosticsState()
_LOCK = Lock()


class SignalRuntimeDiagnosticsService:
    def _snapshot(self) -> dict[str, Any]:
        return dict(asdict(_STATE))

    def get_state(self) -> dict[str, Any]:
        with _LOCK:
            return self._snapshot()

    def update(self, **values: Any) -> dict[str, Any]:
        with _LOCK:
            for key, value in values.items():
                if hasattr(_STATE, key):
                    setattr(_STATE, key, value)
            _STATE.updated_at = datetime.now(timezone.utc).isoformat()
            return self._snapshot()

    def reset(self) -> dict[str, Any]:
        with _LOCK:
            global _STATE
            _STATE = SignalRuntimeDiagnosticsState()
            _STATE.updated_at = datetime.now(timezone.utc).isoformat()
            return self._snapshot()
