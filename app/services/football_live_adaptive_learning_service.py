"""Football LIVE-only additive score adjustments from settled rationale/outcome history.

Not ML: bounded win-rate deltas per observable tags (why codes, warnings, family).
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.enums import BetResult, SportType
from app.db.models.settlement import Settlement
from app.db.models.signal import Signal
from app.schemas.provider_models import ProviderMatch, ProviderOddsMarket, ProviderSignalCandidate
from app.services.football_live_signal_rationale_service import (
    CODE_SELECTED_BEST_SCORE_ON_MATCH,
    CODE_SELECTED_CORE_MARKET,
    CODE_SELECTED_LIMITED_LIVE_CONTEXT,
    CODE_SELECTED_HIGH_PLAUSIBILITY,
    CODE_SELECTED_LOW_PLAUSIBILITY,
    CODE_SELECTED_MEDIUM_PLAUSIBILITY,
)
from app.services.football_signal_send_filter_service import FootballSignalSendFilterService

# Win-rate centered at 0.5; keep per-feature impact small.
# Calibrated after OFF vs ON harness: stronger bite when history exists; still bounded.
_MIN_SAMPLES_TAG = 4
_MIN_SAMPLES_FAMILY = 8
_MAX_ABS_PER_KEY = 1.15
_MAX_ABS_TOTAL = 3.5
_TAG_RATE_SCALE = 5.0  # (rate - 0.5) * scale before capping


@dataclass
class FootballLiveAdaptiveSnapshot:
    """Computed from settled football LIVE rows with `football_live_signal_rationale`."""

    deltas: dict[str, float] = field(default_factory=dict)
    stats: dict[str, dict[str, int]] = field(default_factory=dict)
    meta: dict[str, Any] = field(default_factory=dict)

    def to_public_dict(self) -> dict[str, Any]:
        penalties: list[dict[str, Any]] = []
        boosts: list[dict[str, Any]] = []
        for k, d in sorted(self.deltas.items(), key=lambda kv: kv[0]):
            row = {"key": k, "delta": round(d, 4), **(self.stats.get(k) or {})}
            if d < -1e-6:
                penalties.append(row)
            elif d > 1e-6:
                boosts.append(row)
        return {
            "meta": dict(self.meta),
            "penalties_active": penalties,
            "boosts_active": boosts,
            "deltas": {k: round(v, 4) for k, v in sorted(self.deltas.items())},
        }


def _minute_from_analytics(analytics: dict[str, Any]) -> int | None:
    for key in ("minute", "match_minute", "time"):
        v = analytics.get(key)
        if v is None:
            continue
        try:
            return int(v)
        except (TypeError, ValueError):
            continue
    return None


def preview_live_adaptive_tag_keys(
    candidate: ProviderSignalCandidate,
    analytics: dict[str, Any],
    market_family: str,
) -> tuple[list[str], dict[str, Any]]:
    """Keys aligned with `build_live_adaptive_snapshot` (historical aggregate prefixes)."""
    fam_svc = FootballSignalSendFilterService()
    fam = market_family or fam_svc.get_market_family(candidate)
    tags: list[str] = []
    meta: dict[str, Any] = {"market_family_resolved": fam}

    tags.append(f"fam:{fam}")

    fa = analytics or {}
    h, a = fa.get("score_home"), fa.get("score_away")
    try:
        hi = int(h) if h is not None and not isinstance(h, bool) else None
    except (TypeError, ValueError):
        hi = None
    try:
        ai = int(a) if a is not None and not isinstance(a, bool) else None
    except (TypeError, ValueError):
        ai = None
    minute = _minute_from_analytics(fa)
    limited = minute is None or hi is None or ai is None

    if fam in ("result", "totals", "double_chance", "handicap", "btts"):
        tags.append(f"why:{CODE_SELECTED_CORE_MARKET}")
    tags.append(f"why:{CODE_SELECTED_BEST_SCORE_ON_MATCH}")

    if limited:
        tags.append(f"why:{CODE_SELECTED_LIMITED_LIVE_CONTEXT}")
        tags.append("warn:limited_live_context")
        tags.append(f"fam_warn:{fam}:limited_live_context")

    expl = dict(candidate.explanation_json or {})
    if str(expl.get("football_live_late_stage_warning_ru") or "").strip():
        tags.append("warn:late_stage_signal")
        tags.append(f"fam_warn:{fam}:late_stage_signal")
    elif minute is not None and minute >= 80:
        tags.append("warn:late_stage_signal")
        tags.append(f"fam_warn:{fam}:late_stage_signal")

    ls = expl.get("live_sanity") if isinstance(expl.get("live_sanity"), dict) else {}
    if ls:
        pl_raw = ls.get("plausibility_score")
        try:
            pl_score = int(pl_raw) if pl_raw is not None else 100
        except (TypeError, ValueError):
            pl_score = 100
        if pl_score >= 80:
            tags.append(f"why:{CODE_SELECTED_HIGH_PLAUSIBILITY}")
        elif pl_score >= 50:
            tags.append(f"why:{CODE_SELECTED_MEDIUM_PLAUSIBILITY}")
        else:
            tags.append(f"why:{CODE_SELECTED_LOW_PLAUSIBILITY}")
    else:
        meta["preview_note"] = "plausibility_tags_skipped_no_live_sanity_yet"

    # Dedupe, stable order
    seen: set[str] = set()
    out: list[str] = []
    for t in tags:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out, meta


def _delta_from_counts(wins: int, losses: int, *, min_samples: int) -> float:
    n = wins + losses
    if n < min_samples:
        return 0.0
    rate = wins / n
    raw = (rate - 0.5) * _TAG_RATE_SCALE
    return max(-_MAX_ABS_PER_KEY, min(_MAX_ABS_PER_KEY, raw))


async def build_live_adaptive_snapshot(
    session: AsyncSession,
    *,
    lookback: int = 400,
) -> FootballLiveAdaptiveSnapshot:
    lim = max(40, min(2000, int(lookback)))
    stmt = (
        select(Signal, Settlement)
        .join(Settlement, Settlement.signal_id == Signal.id)
        .where(Signal.sport == SportType.FOOTBALL)
        .where(Signal.is_live.is_(True))
        .where(Settlement.result.in_([BetResult.WIN, BetResult.LOSE]))
        .options(selectinload(Signal.prediction_logs))
        .order_by(Settlement.id.desc())
        .limit(lim)
    )
    rows = list((await session.execute(stmt)).all())
    fam_svc = FootballSignalSendFilterService()

    wins_map: dict[str, int] = defaultdict(int)
    losses_map: dict[str, int] = defaultdict(int)

    used_rationale = 0
    for signal, st in rows:
        if not signal.prediction_logs:
            continue
        pl0 = min(signal.prediction_logs, key=lambda p: p.id)
        ex0 = dict(pl0.explanation_json or {})
        rat = ex0.get("football_live_signal_rationale")
        if not isinstance(rat, dict):
            continue
        used_rationale += 1
        codes = [str(x) for x in (rat.get("why_selected_codes") or []) if x]
        warns = rat.get("warnings") if isinstance(rat.get("warnings"), dict) else {}

        cand = ProviderSignalCandidate(
            match=ProviderMatch(
                external_event_id=str(signal.event_external_id or ""),
                sport=SportType.FOOTBALL,
                tournament_name=signal.tournament_name,
                match_name=signal.match_name,
                home_team=signal.home_team,
                away_team=signal.away_team,
                event_start_at=signal.event_start_at,
                is_live=bool(signal.is_live),
                source_name="db",
            ),
            market=ProviderOddsMarket(
                bookmaker=signal.bookmaker,
                market_type=signal.market_type,
                market_label=signal.market_label,
                selection=signal.selection,
                odds_value=signal.odds_at_signal,
                section_name=signal.section_name,
                subsection_name=signal.subsection_name,
            ),
            feature_snapshot_json=dict(pl0.feature_snapshot_json or {}),
            explanation_json=ex0,
        )
        fam = fam_svc.get_market_family(cand)
        is_win = st.result == BetResult.WIN

        def bump(key: str) -> None:
            if is_win:
                wins_map[key] += 1
            else:
                losses_map[key] += 1

        bump(f"fam:{fam}")
        for code in codes:
            bump(f"why:{code}")
        for wk, wv in warns.items():
            if wv:
                bump(f"warn:{wk}")
                bump(f"fam_warn:{fam}:{wk}")

    all_keys = set(wins_map) | set(losses_map)
    deltas: dict[str, float] = {}
    stats: dict[str, dict[str, int]] = {}
    for key in sorted(all_keys):
        w = wins_map.get(key, 0)
        l = losses_map.get(key, 0)
        if key.startswith("fam:") and not key.startswith("fam_warn:"):
            min_s = _MIN_SAMPLES_FAMILY
        else:
            min_s = _MIN_SAMPLES_TAG
        d = _delta_from_counts(w, l, min_samples=min_s)
        stats[key] = {"wins": w, "losses": l, "n": w + l, "delta": round(d, 4)}
        if abs(d) > 1e-12:
            deltas[key] = d

    return FootballLiveAdaptiveSnapshot(
        deltas=deltas,
        stats=stats,
        meta={
            "lookback_limit": lim,
            "rows_scanned": len(rows),
            "rows_with_rationale": used_rationale,
            "min_samples_tag": _MIN_SAMPLES_TAG,
            "min_samples_family": _MIN_SAMPLES_FAMILY,
            "max_abs_per_key": _MAX_ABS_PER_KEY,
            "max_abs_total": _MAX_ABS_TOTAL,
        },
    )


def apply_live_adaptive_adjustment(
    *,
    base_signal_score: Decimal,
    tag_keys: list[str],
    snapshot: FootballLiveAdaptiveSnapshot | None,
) -> tuple[Decimal, float, list[str], dict[str, Any]]:
    """Returns (effective_score, total_adjustment_float, reason_lines, detail)."""
    if snapshot is None:
        z = Decimal(str(base_signal_score)).quantize(Decimal("0.0001"))
        return z, 0.0, [], {"skipped": True, "reason": "no_snapshot"}

    total = 0.0
    applied: list[tuple[str, float]] = []
    for k in tag_keys:
        d = snapshot.deltas.get(k)
        if d is None:
            continue
        total += float(d)
        applied.append((k, float(d)))

    total_before_cap = total
    total = max(-_MAX_ABS_TOTAL, min(_MAX_ABS_TOTAL, total))
    eff = float(base_signal_score) + total
    eff = max(0.0, min(100.0, eff))
    eff_d = Decimal(str(round(eff, 4))).quantize(Decimal("0.0001"))

    reasons = [f"{k}:{v:+.3f}" for k, v in applied][:40]
    if abs(total_before_cap - total) > 1e-6:
        reasons.append(f"cap_total:{total_before_cap:+.3f}->{total:+.3f}")

    detail = {
        "tag_keys_used": list(tag_keys),
        "per_tag_applied": [{"key": k, "delta": round(v, 4)} for k, v in applied],
        "adjustment_before_cap": round(total_before_cap, 4),
        "learning_adjustment_total": round(total, 4),
        "base_signal_score": float(base_signal_score),
        "effective_live_score": float(eff_d),
    }
    return eff_d, float(total), reasons, detail


def snapshot_json_for_diagnostics(snapshot: FootballLiveAdaptiveSnapshot | None) -> str | None:
    if snapshot is None:
        return None
    try:
        return json.dumps(snapshot.to_public_dict(), ensure_ascii=False)[:20000]
    except (TypeError, ValueError):
        return None


def base_signal_score_for_threshold(candidate: ProviderSignalCandidate) -> float:
    """Soft/normal gate compares base score, not effective (post-adaptive)."""
    fs = candidate.feature_snapshot_json or {}
    adj = fs.get("football_live_adaptive_learning")
    if isinstance(adj, dict) and adj.get("base_signal_score") is not None:
        try:
            return float(adj["base_signal_score"])
        except (TypeError, ValueError):
            pass
    return float(candidate.signal_score or 0.0)
