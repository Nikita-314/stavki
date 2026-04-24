"""Freshness / staleness guards for football live-only pipeline."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from app.core.config import Settings
from app.schemas.provider_models import ProviderMatch, ProviderSignalCandidate

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso_dt(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
        if getattr(dt, "tzinfo", None) is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    s = str(value).strip()
    if not s:
        return None
    try:
        s2 = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s2)
        if getattr(dt, "tzinfo", None) is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError, OSError):
        return None


def _file_mtime_seconds(path: str | None) -> float | None:
    if not path:
        return None
    try:
        p = Path(path)
        if not p.is_file():
            return None
        return float(p.stat().st_mtime)
    except OSError:
        return None


@dataclass(frozen=True)
class ManualSourceFreshness:
    stale: bool
    reason: str
    age_seconds: float | None
    uploaded_at_raw: str | None
    file_path: str | None
    reference: str  # uploaded_at | mtime | unknown


def evaluate_manual_live_source_freshness(
    *,
    uploaded_at: str | None,
    file_path: str | None,
    settings: Settings,
) -> ManualSourceFreshness:
    """semi_live_manual: snapshot must be recently written or have fresh uploaded_at metadata."""
    max_min = max(5, int(settings.football_live_manual_max_age_minutes or 45))
    max_age = timedelta(minutes=max_min)
    now = _utc_now()
    uploaded_dt = _parse_iso_dt(uploaded_at)
    if uploaded_dt is not None:
        age = (now - uploaded_dt).total_seconds()
        if age > max_age.total_seconds():
            return ManualSourceFreshness(
                stale=True,
                reason="uploaded_at_too_old",
                age_seconds=age,
                uploaded_at_raw=uploaded_at,
                file_path=file_path,
                reference="uploaded_at",
            )
        return ManualSourceFreshness(
            stale=False,
            reason="ok",
            age_seconds=age,
            uploaded_at_raw=uploaded_at,
            file_path=file_path,
            reference="uploaded_at",
        )
    mtime_s = _file_mtime_seconds(file_path)
    if mtime_s is not None:
        mtime_dt = datetime.fromtimestamp(mtime_s, tz=timezone.utc)
        age = (now - mtime_dt).total_seconds()
        if age > max_age.total_seconds():
            return ManualSourceFreshness(
                stale=True,
                reason="file_mtime_too_old",
                age_seconds=age,
                uploaded_at_raw=None,
                file_path=file_path,
                reference="file_mtime",
            )
        return ManualSourceFreshness(
            stale=False,
            reason="ok",
            age_seconds=age,
            uploaded_at_raw=None,
            file_path=file_path,
            reference="file_mtime",
        )
    return ManualSourceFreshness(
        stale=True,
        reason="no_timestamp_for_manual_source",
        age_seconds=None,
        uploaded_at_raw=None,
        file_path=file_path,
        reference="unknown",
    )


@dataclass(frozen=True)
class LiveEventFreshnessRow:
    event_id: str
    match_name: str
    event_start_at: str | None
    source_mode: str
    source_timestamp: str | None
    source_age_seconds: float | None
    is_live: bool
    minute: int | None
    stale: bool
    reason: str


def _winline_shows_in_play(time_str: str | None, numer: int | None) -> bool:
    """Winline sends separate scheduled kickoff (`date`) and live match clock (`time` / `sourceTime` / `numer`).

    If the clock looks like a live minute / break, the event must not be rejected
    as `kickoff_in_future` based on scheduled time alone.
    """
    if numer is not None and 1 <= int(numer) <= 200:
        return True
    s = (time_str or "").strip()
    if not s:
        return False
    if re.search(
        r"(?i)перер|(?:^|\b)\d?\s*пер\.?\d*|ht\b|half|int\.|инт\.|меж|ot\b|aet|экстр",
        s,
    ):
        return True
    if re.search(
        r"(?:^|[^\d])(\d{1,3}\s*[''′'″\+]|\d{1,2}\+?\d?\s*[''′])",
        s,
    ):
        return True
    if re.search(r"^\d{1,2}\+?\d?\s*[''′]?\s*$", s):
        return True
    return False


def _candidate_live_minute(candidate: ProviderSignalCandidate) -> int | None:
    fs = getattr(candidate, "feature_snapshot_json", None) or {}
    if not isinstance(fs, dict):
        return None
    for key in ("minute", "match_minute", "time"):
        v = fs.get(key)
        if v is None:
            continue
        try:
            return int(v)
        except (TypeError, ValueError):
            continue
    return None


def evaluate_live_event_staleness(
    *,
    candidate: ProviderSignalCandidate,
    source_mode: str,
    source_age_seconds: float | None,
    settings: Settings,
) -> tuple[bool, str]:
    """Returns (stale, reason). Not stale if OK to treat as current live."""
    match = getattr(candidate, "match", None)
    if match is None:
        return True, "missing_match"
    m = match if isinstance(match, ProviderMatch) else None
    if m is None:
        return True, "invalid_match"
    if not bool(getattr(m, "is_live", False)):
        return True, "not_marked_live"

    start = getattr(m, "event_start_at", None)
    kickoff = _parse_iso_dt(start) if start is not None else None
    if kickoff is None:
        # Winline live snapshots can be missing / inconsistent on scheduled kickoff (`date`) even while
        # the event is clearly in-play (minute/clock present). Treat such events as fresh to avoid
        # killing all real live matches due to a null kickoff timestamp.
        mx = max(95, int(settings.football_live_max_declared_live_minute or 130))
        minute = _candidate_live_minute(candidate)
        if minute is not None and 0 <= int(minute) <= mx:
            return False, "ok_missing_kickoff_but_live_minute_present"
        fs = getattr(candidate, "feature_snapshot_json", None) or {}
        wcl = fs.get("winline_time")
        wsrc = fs.get("winline_source_time")
        wclock = (wcl or wsrc) if (wcl or wsrc) else None
        wn = fs.get("winline_numer")
        if wn is not None and not isinstance(wn, int):
            try:
                wn = int(wn)
            except (TypeError, ValueError):
                wn = None
        wclock_s = wclock if isinstance(wclock, str) else (str(wclock) if wclock is not None else None)
        if _winline_shows_in_play(wclock_s, wn):
            return False, "ok_missing_kickoff_but_winline_clock_in_play"
        return True, "missing_kickoff_time"

    now = _utc_now()
    max_hours = float(settings.football_live_event_max_kickoff_age_hours or 4.0)
    hours_since = (now - kickoff).total_seconds() / 3600.0
    if hours_since < -0.02:
        fs = getattr(candidate, "feature_snapshot_json", None) or {}
        wcl = fs.get("winline_time")
        wsrc = fs.get("winline_source_time")
        wclock = (wcl or wsrc) if (wcl or wsrc) else None
        wn = fs.get("winline_numer")
        if wn is not None and not isinstance(wn, int):
            try:
                wn = int(wn)
            except (TypeError, ValueError):
                wn = None
        wclock_s = wclock if isinstance(wclock, str) else (str(wclock) if wclock is not None else None)
        if _winline_shows_in_play(wclock_s, wn):
            return False, "ok_winline_in_play_overrides_future_scheduled_time"
        return True, "kickoff_in_future_while_marked_live"
    if hours_since > max_hours:
        return True, f"kickoff_too_old_for_live hours={hours_since:.2f} max={max_hours}"

    # Absurd clock / finished-but-still-live in JSON
    mx = max(95, int(settings.football_live_max_declared_live_minute or 130))
    minute = _candidate_live_minute(candidate)
    if minute is not None and minute > mx:
        return True, f"declared_minute_unrealistic minute={minute} max={mx}"

    # Optional: manual source global age (handled earlier); per-event still checked above
    if source_mode == "semi_live_manual" and source_age_seconds is not None:
        if source_age_seconds > max(300.0, float((settings.football_live_manual_max_age_minutes or 45) * 60)):
            return True, "manual_source_age_propagated_stale"

    return False, "ok"


def filter_stale_live_football_candidates(
    candidates: list[ProviderSignalCandidate],
    *,
    source_mode: str,
    source_age_seconds: float | None,
    source_timestamp_iso: str | None,
    settings: Settings,
) -> tuple[list[ProviderSignalCandidate], list[LiveEventFreshnessRow], int, int, int]:
    """Drop stale candidates; one freshness row per distinct event_id.

    Returns: kept, rows, fresh_event_groups, stale_event_groups, dropped_market_rows
    """
    src_ts = source_timestamp_iso or _utc_now().isoformat()
    by_eid: dict[str, list[ProviderSignalCandidate]] = {}
    for c in candidates:
        m = getattr(c, "match", None)
        eid = str(getattr(m, "external_event_id", "") or "") if m else ""
        key = eid or f"_nm_{id(c)}"
        by_eid.setdefault(key, []).append(c)

    kept: list[ProviderSignalCandidate] = []
    rows: list[LiveEventFreshnessRow] = []
    stale_ev = 0
    fresh_ev = 0

    for _key, group in by_eid.items():
        c0 = group[0]
        m = getattr(c0, "match", None)
        eid = str(getattr(m, "external_event_id", "") or "") if m else ""
        stale, reason = evaluate_live_event_staleness(
            candidate=c0,
            source_mode=source_mode,
            source_age_seconds=source_age_seconds,
            settings=settings,
        )
        es = None
        if m is not None and getattr(m, "event_start_at", None) is not None:
            try:
                es = m.event_start_at.isoformat() if hasattr(m.event_start_at, "isoformat") else str(m.event_start_at)
            except Exception:
                es = str(getattr(m, "event_start_at", None))
        rows.append(
            LiveEventFreshnessRow(
                event_id=eid or "—",
                match_name=str(getattr(m, "match_name", "") or "—") if m else "—",
                event_start_at=es,
                source_mode=source_mode,
                source_timestamp=src_ts,
                source_age_seconds=source_age_seconds,
                is_live=bool(getattr(m, "is_live", False)) if m else False,
                minute=_candidate_live_minute(c0),
                stale=stale,
                reason=reason,
            )
        )
        if stale:
            stale_ev += 1
        else:
            fresh_ev += 1
            kept.extend(group)

    dropped_markets = max(0, len(candidates) - len(kept))
    return kept, rows, fresh_ev, stale_ev, dropped_markets


def http_fetch_processing_delay_is_stale(
    live_fetch_at: datetime | None,
    *,
    settings: Settings,
) -> tuple[bool, float | None]:
    """True if processing started unreasonably long after the live HTTP fetch (stuck worker / replay)."""
    if live_fetch_at is None:
        return False, None
    max_sec = max(120.0, float(settings.football_live_runtime_snapshot_max_age_minutes or 30) * 60.0)
    age = (_utc_now() - live_fetch_at).total_seconds()
    return age > max_sec, age


def log_live_freshness_block(rows: list[LiveEventFreshnessRow]) -> None:
    if not rows:
        return
    try:
        payload = [r.__dict__ for r in rows]
        logger.info("[FOOTBALL][LIVE_FRESHNESS] %s", json.dumps(payload, ensure_ascii=False, default=str)[:48000])
    except Exception:
        logger.info("[FOOTBALL][LIVE_FRESHNESS] (serialization failed)")
