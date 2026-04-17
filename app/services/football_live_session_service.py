from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import Lock

logger = logging.getLogger(__name__)


@dataclass
class FootballLiveSessionSnapshot:
    active: bool = False
    started_at: datetime | None = None
    expires_at: datetime | None = None
    duration_minutes: int = 15
    stopped_manually: bool = False
    last_cycle_at: datetime | None = None
    signals_sent_in_session: int = 0
    duplicate_ideas_blocked_session: int = 0
    sent_idea_keys_count: int = 0


_LOCK = Lock()
_ACTIVE = False
_STARTED_AT: datetime | None = None
_EXPIRES_AT: datetime | None = None
_DURATION_MINUTES = 15
_STOPPED_MANUALLY = False
_LAST_CYCLE_AT: datetime | None = None
_SIGNALS_SENT = 0
_DUP_BLOCKED = 0
_SENT_IDEA_KEYS: set[str] = set()


class FootballLiveSessionService:
    """Процесс-local live-сессия футбола (~15 минут): только после «▶️ Старт».

    После перезапуска бота состояние не восстанавливается — сессия считается завершённой.
    """

    def snapshot(self) -> FootballLiveSessionSnapshot:
        with _LOCK:
            self._expire_locked()
            return FootballLiveSessionSnapshot(
                active=_ACTIVE,
                started_at=_STARTED_AT,
                expires_at=_EXPIRES_AT,
                duration_minutes=_DURATION_MINUTES,
                stopped_manually=_STOPPED_MANUALLY,
                last_cycle_at=_LAST_CYCLE_AT,
                signals_sent_in_session=_SIGNALS_SENT,
                duplicate_ideas_blocked_session=_DUP_BLOCKED,
                sent_idea_keys_count=len(_SENT_IDEA_KEYS),
            )

    def _expire_locked(self) -> None:
        global _ACTIVE, _STOPPED_MANUALLY
        if not _ACTIVE:
            return
        if _EXPIRES_AT is None:
            return
        now = datetime.now(timezone.utc)
        exp = _EXPIRES_AT
        if getattr(exp, "tzinfo", None) is None:
            exp = exp.replace(tzinfo=timezone.utc)
        if now >= exp:
            _ACTIVE = False
            logger.info("[FOOTBALL][LIVE_SESSION] expired automatically")

    def expire_if_needed(self) -> None:
        with _LOCK:
            self._expire_locked()

    def is_active(self) -> bool:
        with _LOCK:
            self._expire_locked()
            return bool(_ACTIVE)

    def remaining_seconds(self) -> float | None:
        with _LOCK:
            self._expire_locked()
            if not _ACTIVE or _EXPIRES_AT is None:
                return None
            now = datetime.now(timezone.utc)
            exp = _EXPIRES_AT
            if getattr(exp, "tzinfo", None) is None:
                exp = exp.replace(tzinfo=timezone.utc)
            return max(0.0, (exp - now).total_seconds())

    def start_session(self, *, duration_minutes: int | None = None) -> FootballLiveSessionSnapshot:
        global _ACTIVE, _STARTED_AT, _EXPIRES_AT, _DURATION_MINUTES
        global _STOPPED_MANUALLY, _LAST_CYCLE_AT
        global _SIGNALS_SENT, _DUP_BLOCKED
        global _SENT_IDEA_KEYS
        with _LOCK:
            dm = int(duration_minutes if duration_minutes is not None else _DURATION_MINUTES)
            dm = max(1, min(dm, 180))
            _DURATION_MINUTES = dm
            now = datetime.now(timezone.utc)
            _ACTIVE = True
            _STOPPED_MANUALLY = False
            _STARTED_AT = now
            _EXPIRES_AT = now + timedelta(minutes=dm)
            _LAST_CYCLE_AT = None
            _SIGNALS_SENT = 0
            _DUP_BLOCKED = 0
            _SENT_IDEA_KEYS = set()
            logger.info("[FOOTBALL][LIVE_SESSION] started expires_at=%s duration_min=%s", _EXPIRES_AT.isoformat(), dm)
            return self.snapshot()

    def stop_session(self, *, manual: bool = True) -> FootballLiveSessionSnapshot:
        global _ACTIVE, _STOPPED_MANUALLY
        with _LOCK:
            _ACTIVE = False
            _STOPPED_MANUALLY = manual
            logger.info("[FOOTBALL][LIVE_SESSION] stopped manual=%s", str(manual).lower())
            return self.snapshot()

    def touch_cycle(self) -> None:
        global _LAST_CYCLE_AT
        with _LOCK:
            _LAST_CYCLE_AT = datetime.now(timezone.utc)

    def record_duplicate_idea_blocked(self, n: int = 1) -> None:
        global _DUP_BLOCKED
        with _LOCK:
            _DUP_BLOCKED += max(0, int(n))

    def register_idea_sent(self, idea_key: str) -> None:
        global _SENT_IDEA_KEYS
        with _LOCK:
            _SENT_IDEA_KEYS.add(idea_key)

    def has_idea(self, idea_key: str) -> bool:
        with _LOCK:
            return idea_key in _SENT_IDEA_KEYS

    def record_notification_sent(self, n: int = 1) -> None:
        global _SIGNALS_SENT
        with _LOCK:
            _SIGNALS_SENT += max(0, int(n))

    def record_signals_created(self, n: int) -> None:
        """Сигналов записано в БД за текущую live-сессию (устойчивее чем только notify)."""
        global _SIGNALS_SENT
        with _LOCK:
            _SIGNALS_SENT += max(0, int(n))


def build_live_idea_key(candidate) -> str:
    """Уникальный ключ «матч + семья идеи + нормализованная ставка» для анти-спама."""
    from app.services.football_signal_send_filter_service import FootballSignalSendFilterService

    svc = FootballSignalSendFilterService()
    match = getattr(candidate, "match", None)
    market = getattr(candidate, "market", None)
    eid = str(getattr(match, "external_event_id", "") or "")
    idea_family = svc.get_signal_idea_family(candidate)
    mt = str(getattr(market, "market_type", "") or "").strip().lower()
    ml = str(getattr(market, "market_label", "") or "").strip().lower()
    sel = str(getattr(market, "selection", "") or "").strip().lower()
    blob = "|".join(x for x in (mt, ml, sel) if x)
    norm = blob.replace(" ", "").replace("ё", "е")
    return f"{eid}|{idea_family}|{norm}"
