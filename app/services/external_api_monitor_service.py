from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Awaitable, Callable

from app.core.config import Settings
from app.services.signal_runtime_diagnostics_service import SignalRuntimeDiagnosticsService

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExternalApiHealthCheckResult:
    ok: bool
    error_text: str | None = None
    http_status: int | None = None


@dataclass
class ExternalApiRuntimeState:
    status: str = "unknown"  # unknown | ok | fail | disabled
    runtime_enabled: bool = False
    last_error: str | None = None
    last_success: str | None = None
    last_checked_at: str | None = None
    last_notified_at: str | None = None
    http_status: int | None = None


@dataclass(frozen=True)
class _ExternalApiSource:
    name: str
    label: str
    configured_enabled: bool
    health_check: Callable[[], Awaitable[ExternalApiHealthCheckResult]]


_STATE: dict[str, ExternalApiRuntimeState] = {}
_LOCK = Lock()


class ExternalApiMonitorService:
    _INTERVAL_SECONDS = 15 * 60

    def get_all_states(self) -> dict[str, dict[str, Any]]:
        with _LOCK:
            return {k: asdict(v) for k, v in _STATE.items()}

    def get_source_state(self, source: str) -> dict[str, Any]:
        with _LOCK:
            st = _STATE.get(source) or ExternalApiRuntimeState()
            return asdict(st)

    def is_runtime_enabled(self, source: str, *, configured_enabled: bool) -> bool:
        if not configured_enabled:
            return False
        with _LOCK:
            st = _STATE.get(source)
            if st is None:
                return True
            return bool(st.runtime_enabled)

    async def run_forever(
        self,
        settings: Settings,
        *,
        notify_admin: Callable[[str], Awaitable[None]],
        interval_seconds: int | None = None,
    ) -> None:
        interval = int(interval_seconds or self._INTERVAL_SECONDS)
        while True:
            await self.run_check_once(settings, notify_admin=notify_admin, interval_seconds=interval)
            await asyncio.sleep(interval)

    async def run_check_once(
        self,
        settings: Settings,
        *,
        notify_admin: Callable[[str], Awaitable[None]],
        interval_seconds: int | None = None,
    ) -> None:
        interval = int(interval_seconds or self._INTERVAL_SECONDS)
        for source in self._build_sources(settings):
            try:
                if not source.configured_enabled:
                    await self._apply_disabled(source)
                    continue
                res = await source.health_check()
            except Exception as exc:  # noqa: BLE001
                res = ExternalApiHealthCheckResult(ok=False, error_text=f"request_error: {exc!s}", http_status=None)
            await self._apply_result(source, res, notify_admin=notify_admin, interval_seconds=interval)

    def _build_sources(self, settings: Settings) -> list[_ExternalApiSource]:
        from app.services.api_football_service import ApiFootballService
        from app.services.openai_service import OpenAIService
        from app.services.sportmonks_service import SportmonksService

        return [
            _ExternalApiSource(
                name="openai",
                label="OpenAI",
                configured_enabled=bool(getattr(settings, "openai_enabled", False)),
                health_check=lambda: OpenAIService().health_check(settings),
            ),
            _ExternalApiSource(
                name="api_football",
                label="API-Football",
                configured_enabled=bool(getattr(settings, "api_football_enabled", False)),
                health_check=lambda: ApiFootballService().health_check(),
            ),
            _ExternalApiSource(
                name="sportmonks",
                label="Sportmonks",
                configured_enabled=bool(getattr(settings, "sportmonks_enabled", False)),
                health_check=lambda: SportmonksService().health_check(),
            ),
        ]

    async def _apply_disabled(self, source: _ExternalApiSource) -> None:
        now_iso = datetime.now(timezone.utc).isoformat()
        with _LOCK:
            prev = _STATE.get(source.name) or ExternalApiRuntimeState()
            st = ExternalApiRuntimeState(
                status="disabled",
                runtime_enabled=False,
                last_error=None,
                last_success=prev.last_success,
                last_checked_at=now_iso,
                last_notified_at=prev.last_notified_at,
                http_status=None,
            )
            _STATE[source.name] = st
        if prev.status != "disabled":
            logger.info("[%s] disabled (no api key)", source.label.upper())
        self._sync_diagnostics()

    async def _apply_result(
        self,
        source: _ExternalApiSource,
        result: ExternalApiHealthCheckResult,
        *,
        notify_admin: Callable[[str], Awaitable[None]],
        interval_seconds: int,
    ) -> None:
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        notify_text: str | None = None
        with _LOCK:
            prev = _STATE.get(source.name) or ExternalApiRuntimeState()
            last_success = prev.last_success
            last_notified_at = prev.last_notified_at
            if result.ok:
                st = ExternalApiRuntimeState(
                    status="ok",
                    runtime_enabled=True,
                    last_error=None,
                    last_success=now_iso,
                    last_checked_at=now_iso,
                    last_notified_at=last_notified_at,
                    http_status=result.http_status,
                )
                _STATE[source.name] = st
                if prev.status == "fail" and self._can_notify(last_notified_at, now, interval_seconds):
                    notify_text = (
                        f"✅ {source.label} снова доступен\n"
                        "- источник снова используется ботом"
                    )
                    st.last_notified_at = now_iso
            else:
                err = (result.error_text or "unknown_error").strip()[:900]
                st = ExternalApiRuntimeState(
                    status="fail",
                    runtime_enabled=False,
                    last_error=err,
                    last_success=last_success,
                    last_checked_at=now_iso,
                    last_notified_at=last_notified_at,
                    http_status=result.http_status,
                )
                _STATE[source.name] = st
                if prev.status != "fail" and self._can_notify(last_notified_at, now, interval_seconds):
                    notify_text = (
                        f"⚠️ {source.label} недоступен\n"
                        f"- ошибка: {err}\n"
                        "- источник временно отключён\n"
                        "- бот продолжает работать"
                    )
                    st.last_notified_at = now_iso

        if result.ok:
            logger.info("[%s] ok", source.label.upper())
        else:
            logger.warning("[%s] error: %s", source.label.upper(), result.error_text or "unknown_error")

        self._sync_diagnostics()
        if notify_text:
            await notify_admin(notify_text)

    def _can_notify(self, last_notified_at: str | None, now: datetime, interval_seconds: int) -> bool:
        if not last_notified_at:
            return True
        try:
            last = datetime.fromisoformat(last_notified_at)
        except Exception:
            return True
        return (now - last).total_seconds() >= float(interval_seconds)

    def _sync_diagnostics(self) -> None:
        diag = SignalRuntimeDiagnosticsService()
        all_states = self.get_all_states()

        def _get(source: str, key: str) -> Any:
            return (all_states.get(source) or {}).get(key)

        diag.update(
            external_api_openai_status=_get("openai", "status"),
            external_api_openai_last_error=_get("openai", "last_error"),
            external_api_openai_last_success=_get("openai", "last_success"),
            external_api_api_football_status=_get("api_football", "status"),
            external_api_api_football_last_error=_get("api_football", "last_error"),
            external_api_api_football_last_success=_get("api_football", "last_success"),
            external_api_sportmonks_status=_get("sportmonks", "status"),
            external_api_sportmonks_last_error=_get("sportmonks", "last_error"),
            external_api_sportmonks_last_success=_get("sportmonks", "last_success"),
        )
