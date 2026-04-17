from __future__ import annotations

from dataclasses import asdict, dataclass
from threading import Lock

from app.core.enums import SportType


@dataclass
class SignalRuntimeState:
    football_enabled: bool = True
    cs2_enabled: bool = False
    dota_enabled: bool = False
    paused: bool = False


_STATE = SignalRuntimeState()
_LOCK = Lock()


class SignalRuntimeSettingsService:
    """Simple process-local runtime flags for active signal directions and pause."""

    def _snapshot(self) -> dict[str, bool]:
        return dict(asdict(_STATE))

    def _normalize_sport(self, sport: SportType | str) -> SportType:
        if isinstance(sport, SportType):
            return sport
        key = str(sport).strip().lower()
        if key in {"football", "soccer", "1", "футбол"}:
            return SportType.FOOTBALL
        if key in {"cs2", "cs", "counter_strike", "counter-strike", "2"}:
            return SportType.CS2
        if key in {"dota2", "dota", "dota 2", "3"}:
            return SportType.DOTA2
        raise ValueError(f"unsupported sport: {sport!r}")

    def get_state(self) -> dict[str, bool]:
        with _LOCK:
            return self._snapshot()

    def enable_sport(self, sport: SportType | str) -> dict[str, bool]:
        s = self._normalize_sport(sport)
        with _LOCK:
            if s == SportType.FOOTBALL:
                _STATE.football_enabled = True
            elif s == SportType.CS2:
                _STATE.cs2_enabled = True
            elif s == SportType.DOTA2:
                _STATE.dota_enabled = True
            return self._snapshot()

    def disable_sport(self, sport: SportType | str) -> dict[str, bool]:
        s = self._normalize_sport(sport)
        with _LOCK:
            if s == SportType.FOOTBALL:
                _STATE.football_enabled = False
            elif s == SportType.CS2:
                _STATE.cs2_enabled = False
            elif s == SportType.DOTA2:
                _STATE.dota_enabled = False
            return self._snapshot()

    def toggle_sport(self, sport: SportType | str) -> dict[str, bool]:
        s = self._normalize_sport(sport)
        with _LOCK:
            if s == SportType.FOOTBALL:
                _STATE.football_enabled = not _STATE.football_enabled
            elif s == SportType.CS2:
                _STATE.cs2_enabled = not _STATE.cs2_enabled
            elif s == SportType.DOTA2:
                _STATE.dota_enabled = not _STATE.dota_enabled
            return self._snapshot()

    def pause(self) -> dict[str, bool]:
        with _LOCK:
            _STATE.paused = True
            return self._snapshot()

    def start(self) -> dict[str, bool]:
        with _LOCK:
            _STATE.paused = False
            return self._snapshot()

    def is_paused(self) -> bool:
        with _LOCK:
            return bool(_STATE.paused)

    def is_sport_enabled(self, sport: SportType | str) -> bool:
        s = self._normalize_sport(sport)
        with _LOCK:
            if s == SportType.FOOTBALL:
                return bool(_STATE.football_enabled)
            if s == SportType.CS2:
                return bool(_STATE.cs2_enabled)
            return bool(_STATE.dota_enabled)

    def active_sports(self) -> list[SportType]:
        with _LOCK:
            out: list[SportType] = []
            if _STATE.football_enabled:
                out.append(SportType.FOOTBALL)
            if _STATE.cs2_enabled:
                out.append(SportType.CS2)
            if _STATE.dota_enabled:
                out.append(SportType.DOTA2)
            return out
