from __future__ import annotations

from collections import deque
from dataclasses import dataclass

from app.core.config import Settings


@dataclass(frozen=True)
class FootballLivePacingCycleSnapshot:
    """Telemetry from the last completed football live cycle (facts from diagnostics + wall clock)."""

    cycle_wall_seconds: float
    primary_fetch_seconds: float | None
    """Wall time for the live data fetch leg(s): Winline WS attempt, plus HTTP if it ran in the same cycle."""
    fetch_ok: bool
    last_fetch_status: str | None
    winline_ws_active: bool
    winline_error: str | None
    winline_attempted: bool
    events_count: int
    lines_count: int
    candidates_count: int
    new_ideas_sendable: int
    created_signals_count: int
    snapshot_fingerprint: str
    """Coarse signature of the Winline-side snapshot for change detection."""


def build_football_live_pacing_cycle_snapshot(
    diag: dict, *, cycle_wall_seconds: float
) -> FootballLivePacingCycleSnapshot:
    ev = int(diag.get("football_winline_football_event_count") or 0)
    lines = int(diag.get("football_winline_line_count_raw") or 0)
    cands = int(diag.get("football_winline_football_candidate_count") or 0)
    fp = f"{ev}|{lines}|{cands}"
    werr = diag.get("football_winline_error_last")
    if isinstance(werr, str):
        werr_s = werr.strip() or None
    else:
        werr_s = None
    raw_ok = diag.get("football_live_last_cycle_fetch_ok")
    if raw_ok is None:
        fetch_ok = str(diag.get("last_fetch_status") or "") == "ok"
    else:
        fetch_ok = bool(raw_ok)
    st = diag.get("last_fetch_status")
    st_s = str(st) if st is not None else None
    wf = diag.get("football_live_winline_fetch_seconds_last")
    hf = diag.get("football_live_http_fetch_seconds_last")
    total_fetch: float | None = None
    if isinstance(wf, (int, float)) and wf > 0:
        total_fetch = float(wf)
        if isinstance(hf, (int, float)) and hf > 0:
            total_fetch += float(hf)
    elif isinstance(hf, (int, float)) and hf > 0:
        total_fetch = float(hf)
    return FootballLivePacingCycleSnapshot(
        cycle_wall_seconds=float(cycle_wall_seconds),
        primary_fetch_seconds=total_fetch,
        fetch_ok=fetch_ok,
        last_fetch_status=st_s,
        winline_ws_active=bool(diag.get("football_winline_ws_active_last_cycle")),
        winline_error=werr_s,
        winline_attempted=bool(diag.get("football_live_winline_attempted_last_cycle")),
        events_count=ev,
        lines_count=lines,
        candidates_count=cands,
        new_ideas_sendable=int(diag.get("football_live_cycle_new_ideas_sendable") or 0),
        created_signals_count=int(diag.get("football_live_last_cycle_created_signals") or 0),
        snapshot_fingerprint=fp,
    )


class FootballLiveRuntimePacing:
    """Process-local adaptive sleep between football live cycles (telemetry-driven, bounded)."""

    def __init__(self) -> None:
        self._fetch_samples: deque[float] = deque(maxlen=16)
        self._consecutive_errors: int = 0
        self._consecutive_empty: int = 0
        self._last_fingerprint: str | None = None
        self._last_interval: float = 0.0
        self._last_reason_ru: str = ""

    def reset_session(self) -> None:
        self._fetch_samples.clear()
        self._consecutive_errors = 0
        self._consecutive_empty = 0
        self._last_fingerprint = None
        self._last_interval = 0.0
        self._last_reason_ru = ""

    def compute_sleep_seconds(
        self, settings: Settings, snap: FootballLivePacingCycleSnapshot
    ) -> tuple[float, dict[str, object]]:
        min_s = float(max(15, int(settings.football_live_pacing_min_interval_seconds)))
        max_s = float(max(min_s, int(settings.football_live_pacing_max_interval_seconds)))
        base = float(
            max(
                min_s,
                min(max_s, int(settings.football_live_pacing_base_interval_seconds)),
            )
        )
        step = float(settings.football_live_pacing_backoff_step)
        max_bl = float(max(0.0, float(settings.football_live_pacing_max_backoff_level)))

        parts: list[str] = []

        if snap.primary_fetch_seconds is not None and snap.primary_fetch_seconds > 0:
            self._fetch_samples.append(snap.primary_fetch_seconds)
        avg_fetch = (
            sum(self._fetch_samples) / len(self._fetch_samples) if self._fetch_samples else None
        )

        interval = base

        err_token = (snap.winline_error or "").lower()
        st_token = (snap.last_fetch_status or "").lower()
        looks_timeout = any(
            x in err_token or x in st_token for x in ("timeout", "timed out", "time out")
        )
        looks_reconnect = "reconnect" in err_token or "reconnect" in st_token
        looks_fetch_fail = not snap.fetch_ok or (snap.winline_error is not None)

        empty_snapshot = (
            snap.winline_attempted
            and snap.winline_ws_active
            and snap.events_count == 0
            and snap.lines_count == 0
        )
        if snap.winline_attempted and not snap.winline_ws_active and looks_fetch_fail:
            empty_snapshot = True

        unchanged = (
            self._last_fingerprint is not None
            and snap.snapshot_fingerprint == self._last_fingerprint
            and (snap.events_count + snap.lines_count) > 0
        )

        if looks_fetch_fail:
            self._consecutive_errors += 1
        else:
            self._consecutive_errors = max(0, self._consecutive_errors - 1)

        if empty_snapshot:
            self._consecutive_empty += 1
        else:
            self._consecutive_empty = 0

        if snap.primary_fetch_seconds is not None:
            heavy = snap.primary_fetch_seconds >= float(
                settings.football_live_pacing_fetch_heavy_seconds
            )
            if heavy:
                interval += float(settings.football_live_pacing_fetch_heavy_extra_seconds)
                parts.append(
                    f"тяжёлый fetch (~{snap.primary_fetch_seconds:.1f}s ≥ порога "
                    f"{int(settings.football_live_pacing_fetch_heavy_seconds)}s)"
                )
            elif avg_fetch is not None and snap.primary_fetch_seconds > avg_fetch * 1.85:
                interval += float(settings.football_live_pacing_fetch_above_avg_extra_seconds)
                parts.append(
                    f"fetch дольше недавнего среднего (~{snap.primary_fetch_seconds:.1f}s vs avg ~{avg_fetch:.1f}s)"
                )

        if snap.cycle_wall_seconds >= float(settings.football_live_pacing_cycle_heavy_seconds):
            interval += float(settings.football_live_pacing_cycle_heavy_extra_seconds)
            parts.append(
                f"долгий целиком цикл (wall ~{snap.cycle_wall_seconds:.1f}s ≥ "
                f"{int(settings.football_live_pacing_cycle_heavy_seconds)}s)"
            )

        if looks_timeout or looks_reconnect:
            interval += float(settings.football_live_pacing_network_stress_extra_seconds)
            parts.append("таймаут/reconnect (по токенам статуса/ошибки)")

        if empty_snapshot:
            interval += float(settings.football_live_pacing_empty_snapshot_extra_seconds)
            parts.append("пустой/почти пустой live-снимок")

        if unchanged:
            interval += float(settings.football_live_pacing_unchanged_snapshot_extra_seconds)
            parts.append("снимок почти не изменился vs прошлый цикл (сигнатура events|lines|cands)")

        if looks_fetch_fail:
            interval += float(settings.football_live_pacing_error_extra_seconds)
            parts.append(f"ошибка источника (status={snap.last_fetch_status or '—'})")

        err_level = min(max_bl, float(self._consecutive_errors))
        empty_level = min(max_bl, max(0.0, float(self._consecutive_empty - 2)) * 0.75)
        backoff_level = min(max_bl, err_level + empty_level)
        mult = 1.0 + step * backoff_level
        interval *= mult
        if mult > 1.01:
            parts.append(
                f"backoff ×{mult:.2f} (уровень {backoff_level:.2f}: ошибки {self._consecutive_errors}, "
                f"пустые подряд {self._consecutive_empty})"
            )

        if (
            snap.fetch_ok
            and not empty_snapshot
            and self._consecutive_errors == 0
            and snap.cycle_wall_seconds < float(settings.football_live_pacing_cycle_light_seconds)
        ):
            if snap.primary_fetch_seconds is not None and snap.primary_fetch_seconds <= float(
                settings.football_live_pacing_fetch_light_seconds
            ):
                interval -= float(settings.football_live_pacing_light_cycle_relief_seconds)
                parts.append(
                    f"лёгкий цикл (fetch ~{snap.primary_fetch_seconds:.1f}s, wall ~{snap.cycle_wall_seconds:.1f}s) → чуть чаще"
                )

        interval = round(max(min_s, min(max_s, interval)), 2)
        self._last_interval = interval
        self._last_fingerprint = snap.snapshot_fingerprint

        if not parts:
            reason = (
                f"база {base:.0f}s, телеметрия без стресс-сигналов → "
                f"интервал {interval:.0f}s (диапазон {min_s:.0f}–{max_s:.0f}s)"
            )
        else:
            reason = "; ".join(parts) + f" → интервал {interval:.0f}s (диапазон {min_s:.0f}–{max_s:.0f}s)"

        self._last_reason_ru = reason

        updates: dict[str, object] = {
            "football_live_pacing_current_interval_seconds": round(interval, 3),
            "football_live_pacing_last_fetch_seconds": (
                round(snap.primary_fetch_seconds, 3) if snap.primary_fetch_seconds is not None else None
            ),
            "football_live_pacing_avg_fetch_seconds": (
                round(avg_fetch, 3) if avg_fetch is not None else None
            ),
            "football_live_pacing_backoff_level": round(backoff_level, 3),
            "football_live_pacing_last_reason_ru": reason,
            "football_live_pacing_consecutive_errors": int(self._consecutive_errors),
            "football_live_pacing_consecutive_empty_snapshots": int(self._consecutive_empty),
        }
        return interval, updates


_PACING: FootballLiveRuntimePacing | None = None


def get_football_live_runtime_pacing() -> FootballLiveRuntimePacing:
    global _PACING
    if _PACING is None:
        _PACING = FootballLiveRuntimePacing()
    return _PACING


def reset_football_live_runtime_pacing_for_tests() -> None:
    global _PACING
    _PACING = None
