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
    source_mode: str | None = None
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
    football_sent_count: int = 0
    final_signals_count: int = 0
    messages_sent_count: int = 0
    preview_only: bool = False
    fallback_used: bool = False
    note: str | None = None


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
