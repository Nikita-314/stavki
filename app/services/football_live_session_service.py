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
    persistent: bool = False
    """No wall-clock expiry; run until manual stop."""
    duration_minutes: int = 15
    stopped_manually: bool = False
    last_cycle_at: datetime | None = None
    signals_sent_in_session: int = 0
    """Сколько сигналов записано в БД за сессию (ingest)."""
    telegram_messages_sent_in_session: int = 0
    """Сколько раз реально ушло сообщение в Telegram за сессию."""
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
_TELEGRAM_SENT = 0
_DUP_BLOCKED = 0
# Smarter session-local dedup: remember last sent time and live-state fingerprint per idea.
# Used to allow repeats after a cooldown or after live-state change.
_SENT_IDEA_KEYS: dict[str, tuple[datetime, str | None]] = {}
# Per-event direction guard (combat safety): block result-side flips within a live session.
_SENT_EVENT_DIRECTION: dict[str, tuple[datetime, str]] = {}


class FootballLiveSessionService:
    """Процесс-local live-сессия футбола: только после «▶️ Старт» (по умолчанию без авто-таймера).

    После перезапуска бота состояние не восстанавливается — сессия считается завершённой.
    """

    def snapshot(self) -> FootballLiveSessionSnapshot:
        with _LOCK:
            self._expire_locked()
            return FootballLiveSessionSnapshot(
                active=_ACTIVE,
                started_at=_STARTED_AT,
                expires_at=_EXPIRES_AT,
                persistent=bool(_ACTIVE and _EXPIRES_AT is None),
                duration_minutes=_DURATION_MINUTES,
                stopped_manually=_STOPPED_MANUALLY,
                last_cycle_at=_LAST_CYCLE_AT,
                signals_sent_in_session=_SIGNALS_SENT,
                telegram_messages_sent_in_session=_TELEGRAM_SENT,
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
            if not _ACTIVE:
                return None
            if _EXPIRES_AT is None:
                return None
            now = datetime.now(timezone.utc)
            exp = _EXPIRES_AT
            if getattr(exp, "tzinfo", None) is None:
                exp = exp.replace(tzinfo=timezone.utc)
            return max(0.0, (exp - now).total_seconds())

    def start_session(
        self,
        *,
        duration_minutes: int | None = None,
        persistent: bool = True,
    ) -> FootballLiveSessionSnapshot:
        global _ACTIVE, _STARTED_AT, _EXPIRES_AT, _DURATION_MINUTES
        global _STOPPED_MANUALLY, _LAST_CYCLE_AT
        global _SIGNALS_SENT, _TELEGRAM_SENT, _DUP_BLOCKED
        global _SENT_IDEA_KEYS
        global _SENT_EVENT_DIRECTION
        with _LOCK:
            now = datetime.now(timezone.utc)
            _ACTIVE = True
            _STOPPED_MANUALLY = False
            _STARTED_AT = now
            _LAST_CYCLE_AT = None
            _SIGNALS_SENT = 0
            _TELEGRAM_SENT = 0
            _DUP_BLOCKED = 0
            _SENT_IDEA_KEYS = {}
            _SENT_EVENT_DIRECTION = {}
            if persistent:
                _EXPIRES_AT = None
                _DURATION_MINUTES = 0
                logger.info("[FOOTBALL][LIVE_SESSION] started persistent (no auto-expiry)")
            else:
                dm = int(duration_minutes if duration_minutes is not None else _DURATION_MINUTES)
                dm = max(1, min(dm, 180))
                _DURATION_MINUTES = dm
                _EXPIRES_AT = now + timedelta(minutes=dm)
                logger.info(
                    "[FOOTBALL][LIVE_SESSION] started expires_at=%s duration_min=%s",
                    _EXPIRES_AT.isoformat(),
                    dm,
                )
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
            # Backwards compatible path: register as sent with no live-state fingerprint.
            _SENT_IDEA_KEYS[str(idea_key or "")] = (datetime.now(timezone.utc), None)

    def register_idea_sent_with_state(
        self,
        idea_key: str,
        *,
        state_fingerprint: str | None,
        sent_at_utc: datetime | None = None,
    ) -> None:
        global _SENT_IDEA_KEYS
        with _LOCK:
            ts = sent_at_utc or datetime.now(timezone.utc)
            if getattr(ts, "tzinfo", None) is None:
                ts = ts.replace(tzinfo=timezone.utc)
            _SENT_IDEA_KEYS[str(idea_key or "")] = (ts, str(state_fingerprint) if state_fingerprint else None)

    def should_block_event_direction_flip(
        self,
        *,
        event_external_id: str | None,
        direction_key: str,
        now_utc: datetime | None = None,
    ) -> tuple[bool, str]:
        """Block contradictory direction for the same event in a single live session.

        Example: 1X2 side=home was already sent for this event; sending side=away later is blocked.
        """
        eid = str(event_external_id or "").strip()
        dkey = str(direction_key or "").strip().lower()
        if not eid or not dkey:
            return False, "no_event_or_direction"
        with _LOCK:
            prev = _SENT_EVENT_DIRECTION.get(eid)
        if prev is None:
            return False, "no_previous_direction"
        _prev_ts, prev_key = prev
        if prev_key == dkey:
            return False, "same_direction_ok"
        return True, f"blocked_flip prev={prev_key} new={dkey}"

    def register_event_direction_sent(
        self,
        *,
        event_external_id: str | None,
        direction_key: str,
        sent_at_utc: datetime | None = None,
    ) -> None:
        eid = str(event_external_id or "").strip()
        dkey = str(direction_key or "").strip().lower()
        if not eid or not dkey:
            return
        ts = sent_at_utc or datetime.now(timezone.utc)
        if getattr(ts, "tzinfo", None) is None:
            ts = ts.replace(tzinfo=timezone.utc)
        with _LOCK:
            _SENT_EVENT_DIRECTION[eid] = (ts, dkey)

    def has_idea(self, idea_key: str) -> bool:
        with _LOCK:
            return str(idea_key or "") in _SENT_IDEA_KEYS

    def should_block_duplicate_idea(
        self,
        idea_key: str,
        *,
        min_repeat_minutes: int = 10,
        state_fingerprint: str | None = None,
        now_utc: datetime | None = None,
    ) -> tuple[bool, str]:
        """Session-local dedup that allows repeats after cooldown or live-state change."""
        key = str(idea_key or "")
        if not key:
            return False, "empty_key"
        with _LOCK:
            prev = _SENT_IDEA_KEYS.get(key)
        if prev is None:
            return False, "new_key"
        prev_ts, prev_state = prev
        now = now_utc or datetime.now(timezone.utc)
        if getattr(now, "tzinfo", None) is None:
            now = now.replace(tzinfo=timezone.utc)
        if getattr(prev_ts, "tzinfo", None) is None:
            prev_ts = prev_ts.replace(tzinfo=timezone.utc)
        age_s = (now - prev_ts).total_seconds()
        cooldown_s = max(60.0, float(int(min_repeat_minutes) * 60))
        if age_s >= cooldown_s:
            return False, "cooldown_elapsed"
        sfp = str(state_fingerprint) if state_fingerprint else None
        if sfp and prev_state and sfp != prev_state:
            return False, "live_state_changed"
        return True, "blocked_recent_duplicate"

    def record_telegram_message_sent(self, n: int = 1) -> None:
        global _TELEGRAM_SENT
        with _LOCK:
            _TELEGRAM_SENT += max(0, int(n))

    def record_signals_created(self, n: int) -> None:
        """Сигналов записано в БД за текущую live-сессию (устойчивее чем только notify)."""
        global _SIGNALS_SENT
        with _LOCK:
            _SIGNALS_SENT += max(0, int(n))


def reset_live_session_for_tests() -> None:
    """Сброс процесс-local состояния (только тесты / локальные демо)."""
    global _ACTIVE, _STARTED_AT, _EXPIRES_AT, _DURATION_MINUTES
    global _STOPPED_MANUALLY, _LAST_CYCLE_AT, _SIGNALS_SENT, _TELEGRAM_SENT, _DUP_BLOCKED, _SENT_IDEA_KEYS
    global _SENT_EVENT_DIRECTION
    with _LOCK:
        _ACTIVE = False
        _STARTED_AT = None
        _EXPIRES_AT = None
        _DURATION_MINUTES = 15
        _STOPPED_MANUALLY = False
        _LAST_CYCLE_AT = None
        _SIGNALS_SENT = 0
        _TELEGRAM_SENT = 0
        _DUP_BLOCKED = 0
        _SENT_IDEA_KEYS = {}
        _SENT_EVENT_DIRECTION = {}


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
