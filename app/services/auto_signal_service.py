from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from urllib.parse import parse_qsl, urlparse

from pydantic import ValidationError

from aiogram import Bot
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.enums import SportType
from app.core.config import Settings, get_settings
from app.providers.odds_http_client import OddsHttpClient
from app.schemas.candidate_filter import CandidateFilterConfig
from app.schemas.auto_signal import AutoSignalCycleResult
from app.schemas.provider_models import ProviderSignalCandidate
from app.schemas.provider_client import ProviderClientConfig
from app.services.adapter_ingestion_service import AdapterIngestionService
from app.services.candidate_filter_service import CandidateFilterService
from app.services.deduplication_service import DeduplicationService
from app.services.football_live_freshness_service import (
    filter_stale_live_football_candidates,
    evaluate_manual_live_source_freshness,
    http_fetch_processing_delay_is_stale,
    log_live_freshness_block,
)
from app.services.football_live_runtime_pacing import (
    build_football_live_pacing_cycle_snapshot,
    get_football_live_runtime_pacing,
)
from app.services.football_live_session_service import FootballLiveSessionService, build_live_idea_key
from app.services.football_analytics_service import FootballAnalyticsService
from app.services.football_learning_service import FootballLearningService
from app.services.football_live_adaptive_learning_service import (
    apply_live_adaptive_adjustment,
    base_signal_score_for_threshold,
    build_live_adaptive_snapshot,
    preview_live_adaptive_tag_keys,
    snapshot_json_for_diagnostics,
)
from app.services.api_football_team_intelligence_service import ApiFootballTeamIntelligenceService
from app.services.football_live_analytic_ranker_service import FootballLiveAnalyticRankerService
from app.services.football_live_strategy_service import (
    evaluate_football_live_strategies,
    evaluate_football_live_strategies_async,
    evaluate_s1_live_1x2_controlled,
    evaluate_s2_live_total_over_need_1_2,
)
from app.services.football_signal_integrity_service import FootballSignalIntegrityService
from app.services.football_signal_scoring_service import FootballSignalScoringService
from app.services.football_signal_send_filter_service import FootballSignalSendFilterService
from app.services.ingestion_service import IngestionService
from app.services.orchestration_service import OrchestrationService
from app.services.signal_runtime_diagnostics_service import SignalRuntimeDiagnosticsService
from app.services.signal_runtime_settings_service import SignalRuntimeSettingsService
from app.services.winline_live_feed_service import WinlineLiveFeedService
from app.services.winline_manual_cycle_service import WinlineManualCycleService
from app.services.winline_manual_payload_service import WinlineManualPayloadService
from app.services.winline_raw_line_bridge_service import WinlineRawLineBridgeService
from app.db.repositories.signal_repository import SignalRepository


logger = logging.getLogger(__name__)


def _football_event_id(candidate: ProviderSignalCandidate) -> str:
    return str(getattr(getattr(candidate, "match", None), "external_event_id", "") or "")


def _football_only(candidates: list[ProviderSignalCandidate]) -> list[ProviderSignalCandidate]:
    return [c for c in candidates if getattr(getattr(c, "match", None), "sport", None) == SportType.FOOTBALL]


def _default_live_context_participation() -> dict[str, object]:
    return {
        "used_api_football": False,
        "used_sportmonks": False,
        "used_openai_analysis": False,
        "api_football_mapping_ok": False,
        "api_football_stats_ok": False,
        "api_football_pressure_used": False,
        "api_football_pressure_score": None,
        "api_football_pressure_reason": None,
        "external_context_sources": [],
        "context_sources": ["Winline"],
        "context_label": "Winline",
    }


def _extract_api_football_context_flags(ctx: dict[str, object] | None) -> dict[str, object]:
    payload = ctx if isinstance(ctx, dict) else {}
    stats = payload.get("stats") if isinstance(payload.get("stats"), dict) else {}
    stats_ok = any(v is not None for v in stats.values())
    mapping_ok = bool(payload.get("fixture_id"))
    pressure_score_raw = payload.get("selected_pressure_score")
    try:
        pressure_score = int(pressure_score_raw) if pressure_score_raw is not None else None
    except Exception:
        pressure_score = None
    pressure_used = bool(mapping_ok and stats_ok and pressure_score is not None)
    pressure_reason = payload.get("selected_pressure_reason")
    if not pressure_reason:
        pressure_reason = payload.get("skip_reason") or payload.get("reject_reason")
    return {
        "api_football_mapping_ok": mapping_ok,
        "api_football_stats_ok": stats_ok,
        "api_football_pressure_used": pressure_used,
        "api_football_pressure_score": pressure_score,
        "api_football_pressure_reason": pressure_reason,
    }


def _build_live_context_participation(candidate: ProviderSignalCandidate) -> dict[str, object]:
    fs = getattr(candidate, "feature_snapshot_json", None) or {}
    if not isinstance(fs, dict):
        fs = {}
    ex = getattr(candidate, "explanation_json", None) or {}
    if not isinstance(ex, dict):
        ex = {}

    out = _default_live_context_participation()

    ctx = fs.get("football_live_context_filter") if isinstance(fs.get("football_live_context_filter"), dict) else {}
    api_flags = _extract_api_football_context_flags(ctx)
    stats_ok = bool(api_flags.get("api_football_stats_ok"))
    mapping_ok = bool(api_flags.get("api_football_mapping_ok"))
    pressure_used = bool(api_flags.get("api_football_pressure_used"))
    used_api_football = bool(mapping_ok and stats_ok)

    sportmonks_used = bool(
        ex.get("sportmonks_baseline_home") is not None
        or ex.get("sportmonks_baseline_away") is not None
        or ex.get("sportmonks_baseline_gap_home_minus_away") is not None
        or fs.get("sportmonks_baseline") is not None
    )

    live_adaptive = (
        fs.get("football_live_adaptive_learning")
        if isinstance(fs.get("football_live_adaptive_learning"), dict)
        else {}
    )
    preview_tag_keys = (
        live_adaptive.get("preview_tag_keys") if isinstance(live_adaptive.get("preview_tag_keys"), list) else []
    )
    detail = live_adaptive.get("detail") if isinstance(live_adaptive.get("detail"), dict) else {}
    per_tag_applied = detail.get("per_tag_applied") if isinstance(detail.get("per_tag_applied"), list) else []
    used_openai_analysis = any(str(k).startswith(("oa_pen:", "oa_boost:")) for k in preview_tag_keys)
    if not used_openai_analysis:
        used_openai_analysis = any(
            str((row or {}).get("key") or "").startswith(("oa_pen:", "oa_boost:"))
            for row in per_tag_applied
            if isinstance(row, dict)
        )

    out["used_api_football"] = used_api_football
    out["used_sportmonks"] = sportmonks_used
    out["used_openai_analysis"] = used_openai_analysis
    out["api_football_mapping_ok"] = mapping_ok
    out["api_football_stats_ok"] = stats_ok
    out["api_football_pressure_used"] = pressure_used
    out["api_football_pressure_score"] = api_flags.get("api_football_pressure_score")
    out["api_football_pressure_reason"] = api_flags.get("api_football_pressure_reason")

    external_sources: list[str] = []
    if used_api_football:
        external_sources.append("API-Football")
    if sportmonks_used:
        external_sources.append("Sportmonks")
    if used_openai_analysis:
        external_sources.append("OpenAI")

    context_sources = ["Winline", *external_sources]
    out["external_context_sources"] = external_sources
    out["context_sources"] = context_sources
    out["context_label"] = " + ".join(context_sources)
    return out


def _attach_api_football_context(
    candidate: ProviderSignalCandidate,
    ctx_payload: dict[str, object],
) -> ProviderSignalCandidate:
    fs = dict(candidate.feature_snapshot_json or {})
    ex = dict(candidate.explanation_json or {})
    api_flags = _extract_api_football_context_flags(ctx_payload)
    fs["football_live_context_filter"] = ctx_payload
    ex["football_live_context_filter"] = ctx_payload
    for key, value in api_flags.items():
        fs[key] = value
        ex[key] = value
    return candidate.model_copy(update={"feature_snapshot_json": fs, "explanation_json": ex})


def _attach_live_context_participation(candidate: ProviderSignalCandidate) -> ProviderSignalCandidate:
    fs = dict(candidate.feature_snapshot_json or {})
    ex = dict(candidate.explanation_json or {})
    participation = _build_live_context_participation(candidate)
    fs["football_live_context_participation"] = participation
    ex["football_live_context_participation"] = participation
    for key in (
        "api_football_mapping_ok",
        "api_football_stats_ok",
        "api_football_pressure_used",
        "api_football_pressure_score",
        "api_football_pressure_reason",
    ):
        fs[key] = participation.get(key)
        ex[key] = participation.get(key)
    return candidate.model_copy(update={"feature_snapshot_json": fs, "explanation_json": ex})


_LIVE_MAIN_SINGLE_RELIEF = frozenset({"result", "totals", "btts", "handicap"})
_LIVE_MAIN_SOFT = frozenset({"result", "double_chance", "totals", "btts", "handicap"})
_LIVE_ABS_SCORE_FLOOR = 48.0
_SOFT_GAP_LABEL = 1.5
_SOFT_GAP_PRIORITY = 1.0


def classify_live_sendable_candidate(
    c: ProviderSignalCandidate,
    base: float,
    family_svc: FootballSignalSendFilterService,
    *,
    single_relief_max_gap: float,
) -> tuple[str, str | None]:
    """normal | soft | reject — soft only for main markets with reason_codes, not exotic/corners."""
    sc = base_signal_score_for_threshold(c)
    if sc >= base:
        return "normal", None
    floor = max(_LIVE_ABS_SCORE_FLOOR, base - 3.0)
    if sc < floor:
        return "reject", None
    if family_svc.is_corner_market(c):
        return "reject", None
    fam = family_svc.get_market_family(c)
    if fam == "exotic":
        return "reject", None
    codes = (c.explanation_json or {}).get("football_scoring_reason_codes") or []
    if not codes:
        return "reject", None
    if fam not in _LIVE_MAIN_SOFT:
        return "reject", None
    gap = base - sc
    if fam in _LIVE_MAIN_SINGLE_RELIEF:
        if sc < base - float(single_relief_max_gap):
            return "reject", None
        if gap <= _SOFT_GAP_LABEL:
            return "soft", "soft_sendable"
        return "soft", "soft_sendable_relief_single"
    if fam == "double_chance" and gap <= _SOFT_GAP_LABEL:
        return "soft", "soft_sendable_dc"
    return "reject", None


def order_live_finalist_tuples(
    items: list[tuple[ProviderSignalCandidate, str, str | None]],
    base: float,
    family_svc: FootballSignalSendFilterService,
) -> list[tuple[ProviderSignalCandidate, str, str | None]]:
    """Normals first; among softs prefer gap ≤1.0, then tighter gap, then main-market preference."""
    if not items:
        return []
    normals = [x for x in items if x[1] == "normal"]
    softs = [x for x in items if x[1] == "soft"]
    out: list[tuple[ProviderSignalCandidate, str, str | None]] = []
    if normals:
        nc = [c for c, _, _ in normals]
        ns = _sort_finalists_main_market_first(nc, family_svc)
        id_order = {id(c): i for i, c in enumerate(ns)}
        normals.sort(key=lambda x: id_order.get(id(x[0]), 999))
        out.extend(normals)
    if softs:

        def _sk(item: tuple[ProviderSignalCandidate, str, str | None]) -> tuple[int, float, float, float]:
            c, _, _ = item
            sc = float(c.signal_score or 0)
            gap = base - sc
            band = 0 if gap <= _SOFT_GAP_PRIORITY else (1 if gap <= _SOFT_GAP_LABEL else 2)
            if family_svc.is_corner_market(c):
                mb = -10.0
            else:
                fam = family_svc.get_market_family(c)
                if fam in ("result", "double_chance"):
                    mb = 18.0
                elif fam in ("totals", "btts", "handicap"):
                    mb = 14.0
                else:
                    mb = 0.0
            return (band, gap, -(mb + sc), sc)

        softs.sort(key=_sk)
        out.extend(softs)
    return out


def _sort_finalists_main_market_first(
    finalists: list[ProviderSignalCandidate],
    family_svc: FootballSignalSendFilterService,
) -> list[ProviderSignalCandidate]:
    """Corners lose to main markets at comparable raw score (final live selection)."""

    def _key(c: ProviderSignalCandidate) -> tuple[float, float]:
        fam = family_svc.get_market_family(c)
        sc = float(c.signal_score or 0.0)
        if family_svc.is_corner_market(c):
            return (sc - 8.0, sc)
        if fam in ("result", "double_chance"):
            return (sc + 14.0, sc)
        if fam in ("totals", "btts", "handicap"):
            return (sc + 12.0, sc)
        return (sc, sc)

    return sorted(finalists, key=_key, reverse=True)


def _assert_finalist_safe_for_live_send(
    c: ProviderSignalCandidate,
    base: float,
    fam_svc: FootballSignalSendFilterService,
) -> bool:
    """Defense: corners/exotic/below floor/missing reason_codes must never ship."""
    sc = float(c.signal_score or 0.0)
    if sc < max(_LIVE_ABS_SCORE_FLOOR, base - 3.0):
        return False
    if fam_svc.is_corner_market(c) or fam_svc.get_market_family(c) == "exotic":
        return False
    if not (c.explanation_json or {}).get("football_scoring_reason_codes"):
        return False
    return True


def _build_live_ingest_traces(
    created: list[ProviderSignalCandidate],
    min_score_base: float,
    family_svc: FootballSignalSendFilterService,
    send_meta: dict[int, tuple[str, str | None]],
) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for c in created:
        path = (c.explanation_json or {}).get("football_live_send_path")
        if not path:
            t0 = send_meta.get(id(c), ("normal", None))
            path = t0[0] if t0 else "normal"
        fam = family_svc.get_market_family(c)
        is_corner = bool(family_svc.is_corner_market(c))
        is_main = (not is_corner) and (fam in _LIVE_MAIN_SOFT)
        sc = float(c.signal_score or 0.0)
        gap = max(0.0, round(min_score_base - sc, 2)) if sc < min_score_base else 0.0
        codes = list((c.explanation_json or {}).get("football_scoring_reason_codes") or [])
        out.append(
            {
                "match": c.match.match_name,
                "tournament": (getattr(c.match, "tournament_name", None) or None),
                "minute": _football_match_minute_from_candidate(c),
                "market_family": fam,
                "bet_text": _football_format_bet_line(c),
                "odds": str(c.market.odds_value) if c.market.odds_value is not None else None,
                "score": round(sc, 2),
                "send_path": str(path),
                "gap_to_base_threshold": float(gap),
                "reason_codes": codes,
                "was_main_market": "yes" if is_main else "no",
            }
        )
    return out


def _post_selection_bottleneck_ru(
    *,
    session_dup_blocked: int,
    db_dedup_skipped: int,
    created_n: int,
) -> str | None:
    if created_n:
        return None
    if db_dedup_skipped and db_dedup_skipped > 0 and session_dup_blocked == 0:
        return (
            "После score сигналы в БД не созданы: дедуп (похожий сигнал уже есть). "
            "Порог score/soft здесь не при чём — упёрлись в post-selection."
        )
    if session_dup_blocked and session_dup_blocked > 0:
        return (
            "После score сигналы отсекались: повтор той же live-идеи в сессии. "
            "Сначала смотрите сессию/dedup идеи, а не качество оценки."
        )
    return None


def _football_format_bet_line(candidate: ProviderSignalCandidate) -> str:
    from app.services.football_bet_formatter_service import FootballBetFormatterService

    pres = FootballBetFormatterService().format_bet(
        market_type=candidate.market.market_type,
        market_label=candidate.market.market_label,
        selection=candidate.market.selection,
        home_team=candidate.match.home_team,
        away_team=candidate.match.away_team,
        section_name=candidate.market.section_name,
        subsection_name=candidate.market.subsection_name,
    )
    if pres.detail_label:
        return f"{pres.main_label} ({pres.detail_label})"
    return pres.main_label


def _football_match_minute_from_candidate(candidate: ProviderSignalCandidate | None) -> int | None:
    if candidate is None:
        return None
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
    fa = fs.get("football_analytics")
    if isinstance(fa, dict) and fa.get("minute") is not None:
        try:
            return int(fa.get("minute"))
        except (TypeError, ValueError):
            pass
    return None


def _resolve_football_live_send_meta(
    cand: ProviderSignalCandidate,
    ordered: list[tuple[ProviderSignalCandidate, str, str | None]],
    send_meta_final: dict[int, tuple[str, str | None]],
) -> tuple[str, str | None] | None:
    """Match tier/sub after final gate: gate returns model_copy rows with new object ids."""
    m = send_meta_final.get(id(cand))
    if m:
        return m
    fk = _combat_finalist_dedup_key(cand)
    for c, t, s in ordered:
        if _combat_finalist_dedup_key(c) == fk:
            return (t, s)
    return None


def _attach_football_live_send_meta(
    c: ProviderSignalCandidate,
    meta: tuple[str, str | None] | None,
) -> ProviderSignalCandidate:
    if not meta:
        return c
    tier, sub = meta
    expl = dict(c.explanation_json or {})
    expl["football_live_send_path"] = tier
    if sub:
        expl["football_live_send_soft_label"] = sub
    from app.services.football_live_signal_rationale_service import build_football_live_signal_rationale

    rationale = build_football_live_signal_rationale(c, send_path=tier, send_soft_label=sub)
    if rationale:
        expl["football_live_signal_rationale"] = rationale
    elif getattr(c.match, "is_live", False) and getattr(c.match, "sport", None) == SportType.FOOTBALL:
        logger.warning(
            "[FOOTBALL][RATIONALE] missing rationale after send_meta attach match=%s event_id=%s",
            c.match.match_name,
            getattr(c.match, "external_event_id", None),
        )
    expl.pop("football_live_signal_why", None)
    return c.model_copy(update={"explanation_json": expl})


def _ru_why_reject_at_soft_send_gate(
    c: ProviderSignalCandidate,
    tier: str,
    *,
    min_base: float,
    family_svc: FootballSignalSendFilterService,
) -> str:
    if tier in ("normal", "soft"):
        return ""
    sc = float(c.signal_score or 0.0)
    floor = max(_LIVE_ABS_SCORE_FLOOR, min_base - 3.0)
    if sc < floor:
        return f"мягкий send-gate: score {sc:.1f} < пол {floor:.0f}"
    if family_svc.is_corner_market(c):
        return "мягкий send-gate: углы не берём в live-auto"
    if family_svc.get_market_family(c) == "exotic":
        return "мягкий send-gate: экзот"
    if not (c.explanation_json or {}).get("football_scoring_reason_codes"):
        return "мягкий send-gate: нет reason_codes в скоринге"
    if sc < min_base:
        return f"мягкий send-gate: gap {min_base - sc:.1f} (семейство/порог soft)"
    return "мягкий send-gate: reject"


def _combat_finalist_dedup_key(c: ProviderSignalCandidate) -> str:
    m, mk = c.match, c.market
    return "|".join(
        (
            str(m.external_event_id or ""),
            str(mk.bookmaker or ""),
            str(mk.market_type or ""),
            str(mk.selection or ""),
            str(mk.market_label or ""),
        )
    )


async def _combat_e2e_delivery_rows(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    *,
    delivery_scope: str,
    relaxed_dedup: bool,
    dedup_relaxed_minutes: int,
    candidates_to_ingest: list[ProviderSignalCandidate],
    ingest_res: Any,
    per_signal_notified: dict[int, bool],
    runtime_paused: bool,
) -> list[dict[str, Any]]:
    """Per-finalist trace: DB ingest result, dedup, notify (no DB schema changes)."""
    chat_ok = bool(getattr(settings, "signal_chat_id", None))
    key_to_sid: dict[str, int] = {}
    for i, cc in enumerate(ingest_res.created_from_candidates):
        key_to_sid[_combat_finalist_dedup_key(cc)] = int(ingest_res.created_signal_ids[i])

    rows: list[dict[str, Any]] = []
    ing = IngestionService()
    for c in candidates_to_ingest:
        eid = str(c.match.external_event_id or "—")
        mname = str(c.match.match_name or "—")
        bet = _football_format_bet_line(c)
        sc = float(c.signal_score or 0.0)
        odds = str(c.market.odds_value) if c.market.odds_value is not None else "—"
        st_path = (c.explanation_json or {}).get("football_live_send_path") or "—"
        k = _combat_finalist_dedup_key(c)
        if k in key_to_sid:
            sid = key_to_sid[k]
            n_ok = bool(per_signal_notified.get(sid, False))
            if n_ok:
                final = "sent"
            elif not chat_ok:
                final = "blocked_notify_signal_chat"
            elif runtime_paused:
                final = "blocked_notify_runtime_paused"
            else:
                final = "blocked_notify"
            rows.append(
                {
                    "event_id": eid,
                    "match": mname,
                    "bet": bet,
                    "odds": odds,
                    "score": round(sc, 2),
                    "sendable_status": st_path,
                    "created_in_db": "yes",
                    "signal_id": sid,
                    "blocked_before_db": None,
                    "blocked_by_db_dedup": "no",
                    "notify_attempted": "yes",
                    "bot_send_message_effective": "yes" if n_ok else "no",
                    "final_outcome": final,
                }
            )
            continue
        try:
            b = ing.candidate_to_bundle(c)
        except (ValidationError, ValueError, TypeError) as e:
            rows.append(
                {
                    "event_id": eid,
                    "match": mname,
                    "bet": bet,
                    "odds": odds,
                    "score": round(sc, 2),
                    "sendable_status": st_path,
                    "created_in_db": "no",
                    "signal_id": None,
                    "blocked_before_db": f"bundle:{e!s}"[:180],
                    "blocked_by_db_dedup": "n/a",
                    "notify_attempted": "no",
                    "bot_send_message_effective": "no",
                    "final_outcome": "blocked_before_ingest",
                }
            )
            continue
        async with sessionmaker() as session:
            ex = await SignalRepository().find_existing_similar_signal(
                session,
                sport=b.signal.sport,
                bookmaker=b.signal.bookmaker,
                event_external_id=b.signal.event_external_id,
                home_team=b.signal.home_team,
                away_team=b.signal.away_team,
                market_type=b.signal.market_type,
                selection=b.signal.selection,
                is_live=b.signal.is_live,
                exclude_notes=("fallback_json", "manual_json", "demo"),
                required_notes=(delivery_scope,),
                relaxed_semi_manual=relaxed_dedup,
                candidate_odds=Decimal(b.signal.odds_at_signal) if b.signal.odds_at_signal is not None else None,
                candidate_event_start_at=b.signal.event_start_at,
                relaxed_interval_minutes=int(dedup_relaxed_minutes),
            )
        if ex is not None:
            sa = ex.signaled_at.isoformat() if ex.signaled_at else None
            rows.append(
                {
                    "event_id": eid,
                    "match": mname,
                    "bet": bet,
                    "odds": odds,
                    "score": round(sc, 2),
                    "sendable_status": st_path,
                    "created_in_db": "no",
                    "signal_id": None,
                    "blocked_before_db": None,
                    "blocked_by_db_dedup": "yes",
                    "existing_signal_id": int(ex.id),
                    "existing_event_id": ex.event_external_id,
                    "existing_bet": f"{ex.market_type} / {ex.selection}"[:200],
                    "existing_signaled_at": sa,
                    "notify_attempted": "no",
                    "bot_send_message_effective": "no",
                    "final_outcome": "blocked_db_dedup",
                }
            )
        else:
            rows.append(
                {
                    "event_id": eid,
                    "match": mname,
                    "bet": bet,
                    "odds": odds,
                    "score": round(sc, 2),
                    "sendable_status": st_path,
                    "created_in_db": "no",
                    "signal_id": None,
                    "blocked_before_db": "ingest_skipped (no duplicate found — validation or other)",
                    "blocked_by_db_dedup": "no",
                    "notify_attempted": "no",
                    "bot_send_message_effective": "no",
                    "final_outcome": "blocked_before_ingest",
                }
            )
    return rows


def _enrich_final_live_gate_with_delivery(
    final_live_gate: dict[str, Any], trace_rows: list[dict[str, Any]]
) -> None:
    """Attach combat delivery outcome per event_id; full_pipeline_decision includes post-gate DB/notify."""
    if not isinstance(final_live_gate, dict):
        return
    by_eid: dict[str, dict[str, Any]] = {}
    for tr in trace_rows or []:
        if not isinstance(tr, dict):
            continue
        eid = str(tr.get("event_id") or "").strip()
        if eid and eid != "—":
            by_eid[eid] = tr
    for row in final_live_gate.get("per_match") or []:
        if not isinstance(row, dict):
            continue
        eid = str(row.get("event_id") or "").strip()
        tr = by_eid.get(eid) if eid else None
        row["delivery_trace_row"] = tr
        gate_d = str(row.get("final_gate_decision") or "")
        if gate_d != "sent":
            row["full_pipeline_decision"] = gate_d
            continue
        if not tr:
            row["full_pipeline_decision"] = "sent_gate_no_delivery_trace"
            continue
        fo = str(tr.get("final_outcome") or "")
        if fo == "sent":
            row["full_pipeline_decision"] = "sent"
        elif fo == "blocked_db_dedup":
            row["full_pipeline_decision"] = "blocked_db_dedup"
        elif fo in {"blocked_notify", "blocked_notify_signal_chat", "blocked_notify_runtime_paused"}:
            row["full_pipeline_decision"] = "blocked_notify"
        elif fo == "blocked_before_ingest":
            row["full_pipeline_decision"] = "blocked_before_ingest"
        else:
            row["full_pipeline_decision"] = f"post_gate:{fo}"


def compile_football_cycle_debug(
    *,
    fb_preview: list[ProviderSignalCandidate],
    fb_cvf: list[ProviderSignalCandidate],
    fb_post_send: list[ProviderSignalCandidate],
    fb_post_integrity: list[ProviderSignalCandidate],
    enriched_scored: list[ProviderSignalCandidate] | None,
    finalists: list[ProviderSignalCandidate] | None,
    finalists_pre_session: list[ProviderSignalCandidate] | None = None,
    min_score: float,
    family_svc: FootballSignalSendFilterService,
    send_filter_stats,
    integrity_dropped_checks: list,
    dry_run: bool,
    global_block: str | None = None,
    min_score_base: float | None = None,
    score_relief_note: str = "none",
    db_dedup_blocked_count: int = 0,
    live_send_stats: dict | None = None,
    finalist_send_meta: dict[int, tuple[str, str | None]] | None = None,
    single_relief_max_gap: float = 2.0,
    live_sanity_drop_by_eid: dict[str, str] | None = None,
    live_sanity_drop_reasons: dict[str, str] | None = None,
) -> dict:
    """Aggregated per-match football pipeline diagnostics for dry_run Telegram + logs."""
    lsd: dict[str, str] = dict(live_sanity_drop_by_eid or {})
    lsr: dict[str, str] = dict(live_sanity_drop_reasons or {})
    base_for_display = float(min_score_base) if min_score_base is not None else float(min_score)
    thr_eff = float(min_score)
    send_surviving_eids = {_football_event_id(c) for c in fb_post_send if _football_event_id(c)}
    send_fail_by_eid = family_svc.per_event_send_filter_failure(fb_cvf, surviving_event_ids=send_surviving_eids)

    def _count_by_event(cands: list[ProviderSignalCandidate]) -> Counter[str]:
        ctr: Counter[str] = Counter()
        for c in cands:
            eid = _football_event_id(c)
            if eid:
                ctr[eid] += 1
        return ctr

    preview_counts = _count_by_event(fb_preview)
    cvf_counts = _count_by_event(fb_cvf)
    post_send_counts = _count_by_event(fb_post_send)
    post_int_counts = _count_by_event(fb_post_integrity)
    finalist_counts = _count_by_event(finalists or [])
    scored_eid_counts = _count_by_event(enriched_scored or [])
    strat_by_eid: dict[str, str] = {}
    if enriched_scored:
        for c in enriched_scored:
            eid = _football_event_id(c)
            if not eid or eid in strat_by_eid:
                continue
            sid = (c.explanation_json or {}).get("football_live_strategy_id")
            if isinstance(sid, str) and sid.strip():
                strat_by_eid[eid] = sid.strip()

    preview_by_eid: dict[str, ProviderSignalCandidate] = {}
    for c in fb_preview:
        eid = _football_event_id(c)
        if eid and eid not in preview_by_eid:
            preview_by_eid[eid] = c

    best_by_eid: dict[str, tuple[float, ProviderSignalCandidate]] = {}
    if enriched_scored:
        for c in enriched_scored:
            eid = _football_event_id(c)
            if not eid:
                continue
            sc = float(c.signal_score or 0.0)
            prev = best_by_eid.get(eid)
            if prev is None or sc > prev[0]:
                best_by_eid[eid] = (sc, c)

    # If strategy id is not present on enriched candidates (tagging happens later in the pipeline),
    # infer it from the best candidate per match for diagnostics only.
    if best_by_eid:
        try:
            from app.services.football_live_strategy_service import evaluate_football_live_strategies

            for eid, (_sc, best_c) in best_by_eid.items():
                if not eid or eid in strat_by_eid:
                    continue
                d0 = evaluate_football_live_strategies(best_c)
                if d0.passed and d0.strategy_id:
                    strat_by_eid[eid] = d0.strategy_id
        except Exception:
            pass

    pre_session_eids: set[str] = set()
    if finalists_pre_session:
        for c in finalists_pre_session:
            pe = _football_event_id(c)
            if pe:
                pre_session_eids.add(pe)
    finalists_post_eids: set[str] = {
        _football_event_id(c) for c in (finalists or []) if _football_event_id(c)
    }

    rows: list[dict] = []
    for eid, rep in sorted(preview_by_eid.items(), key=lambda kv: (kv[1].match.match_name or "")):
        match = rep.match
        is_live, hours_to_start = family_svc._time_window_info(rep)
        is_corner_like = bool(family_svc.is_corner_market(rep))
        raw_keys = {
            (
                str(c.market.market_type or ""),
                str(c.market.market_label or ""),
                str(c.market.selection or ""),
            )
            for c in fb_preview
            if _football_event_id(c) == eid
        }
        n_preview = preview_counts.get(eid, 0)
        n_cvf = cvf_counts.get(eid, 0)
        n_send = post_send_counts.get(eid, 0)
        n_int = post_int_counts.get(eid, 0)
        n_final = finalist_counts.get(eid, 0)

        best_sc, best_c = best_by_eid.get(eid, (0.0, None))
        best_market = None
        best_odds = None
        best_is_corner_like = None
        best_market_family = None
        if best_c is not None:
            best_market = str(best_c.market.market_label or best_c.market.market_type or "")
            best_odds = str(best_c.market.odds_value) if best_c.market.odds_value is not None else None
            best_is_corner_like = bool(family_svc.is_corner_market(best_c))
            best_market_family = family_svc.get_market_family(best_c)

        minute_val: int | None = None
        for c in fb_preview:
            if _football_event_id(c) == eid:
                minute_val = _football_match_minute_from_candidate(c)
                if minute_val is not None:
                    break

        final_status = "blocked_unknown"
        if n_preview == 0:
            final_status = "no_candidates"
        elif n_cvf == 0:
            final_status = "blocked_pre_send_pipeline"
        elif n_send == 0:
            if send_fail_by_eid.get(eid) == "too_far_in_time":
                final_status = "blocked_too_far_in_time"
            else:
                final_status = "blocked_send_filter"
        elif n_int == 0:
            final_status = "blocked_integrity"
        elif n_int > 0 and best_c is None:
            final_status = "blocked_no_strategy_match"
        elif n_final > 0:
            final_status = "selected"
        elif (
            best_c is not None
            and eid in pre_session_eids
            and eid not in finalists_post_eids
        ):
            final_status = "blocked_duplicate_idea"
        elif best_c is not None:
            final_status = "blocked_low_score"
        else:
            final_status = "blocked_integrity"

        if eid in lsd:
            final_status = lsd[eid]

        why_code = "other"
        why_ru = ""
        if n_preview == 0:
            why_code, why_ru = "no_candidates", "Нет кандидатов после проверки свежести"
        elif n_cvf == 0:
            why_code, why_ru = "pre_send_pipeline", "Нет кандидатов перед фильтром отправки"
        elif n_send == 0:
            why_code, why_ru = "send_filter", "Отсеяно фильтром отправки (рынок/время)"
        elif n_int == 0:
            why_code, why_ru = "integrity", "Не прошла проверку целостности ставки"
        elif best_c is None:
            why_code, why_ru = (
                "no_strategy_match",
                "После integrity ни один кандидат не прошёл explicit strategy gate (S8/S9)",
            )
        elif n_final > 0:
            why_code, why_ru = "sendable", "Готова к рассмотрению на отправку"
        elif eid in pre_session_eids and eid not in finalists_post_eids:
            why_code, why_ru = "duplicate_idea", "Та же идея по матчу уже отправлялась в этой сессии"
        elif best_sc < base_for_display:
            why_code, why_ru = "low_score", f"Score {best_sc:.1f} ниже порога {base_for_display:.0f}"
        else:
            why_ru = "См. final_status"
        if eid in lsd:
            why_code, why_ru = "live_sanity", lsr.get(eid) or "Отсеяно pre-send live sanity (счёт/текст рынка)"

        gap_to_sendable = None
        if best_c is not None and final_status == "blocked_low_score":
            gap_to_sendable = round(float(base_for_display) - float(best_sc), 2)

        learning_extra: dict[str, Any] = {}
        if best_c is not None:
            fsb = best_c.feature_snapshot_json or {}
            la = fsb.get("football_live_adaptive_learning")
            if isinstance(la, dict) and la.get("enabled"):
                learning_extra = {
                    "best_candidate_score_base": la.get("base_signal_score"),
                    "best_candidate_learning_adjustment_total": la.get("learning_adjustment_total"),
                    "best_candidate_learning_reasons_sample": ", ".join(
                        (la.get("learning_adjustment_reasons") or [])[:10]
                    )[:500],
                }

        n_scored = int(scored_eid_counts.get(eid, 0))
        best_bet_text: str | None
        if best_c is not None:
            best_bet_text = _football_format_bet_line(best_c)
        else:
            best_bet_text = None
        best_tier, soft_sub = ("reject", None)
        if n_int > 0 and best_c is not None:
            best_tier, soft_sub = classify_live_sendable_candidate(
                best_c, base_for_display, family_svc, single_relief_max_gap=float(single_relief_max_gap)
            )
        soft_reject_ru = ""
        if n_int > 0 and best_c is not None and best_tier == "reject":
            soft_reject_ru = _ru_why_reject_at_soft_send_gate(
                best_c, best_tier, min_base=base_for_display, family_svc=family_svc
            )
        reject_reason_ru = why_ru
        if n_send == 0:
            reject_reason_ru = "send-filter: " + (why_ru or "")
        elif n_int == 0 and n_send > 0:
            reject_reason_ru = "integrity: " + (why_ru or "")
        elif n_int > 0 and best_c and best_tier == "reject":
            reject_reason_ru = (soft_reject_ru or why_ru) if final_status in {"blocked_low_score", "blocked_unknown"} else (why_ru or soft_reject_ru)
        sendable_path_ru: str = str(reject_reason_ru)
        if final_status == "blocked_duplicate_idea":
            sendable_path_ru = "сессия: идея уже в памяти live (не ушла в пул) — " + (why_ru or "")
        elif n_final > 0:
            sendable_path_ru = "ok (есть в финалистах после live send-gate)"
        if eid in lsd:
            sendable_path_ru = "live sanity: " + lsr.get(eid, (why_ru or "—")[:200])

        tn = str(getattr(match, "tournament_name", "") or "").strip()
        rows.append(
            {
                "event_id": eid,
                "league": tn or "—",
                "match_name": str(match.match_name or ""),
                "tournament_name": tn or None,
                "minute": minute_val,
                "is_live": bool(getattr(match, "is_live", False)),
                "is_corner_like": is_corner_like,
                "event_start_at": match.event_start_at.isoformat() if getattr(match, "event_start_at", None) else None,
                "hours_to_start": None if hours_to_start is None else round(float(hours_to_start), 3),
                "raw_markets_count": len(raw_keys),
                "freshness_accepted": True,
                "candidates_after_freshness": n_preview,
                "candidates_before_filter": n_preview,
                "candidates_after_cvf": n_cvf,
                "candidates_after_send_filter": n_send,
                "candidates_after_integrity": n_int,
                "candidates_after_scoring": n_scored,
                "strategy_id": strat_by_eid.get(eid),
                "candidates_after_score_threshold": n_final,
                "best_market_family": best_market_family,
                "best_candidate_market": best_market,
                "best_bet_text": best_bet_text,
                "best_candidate_odds": best_odds,
                "best_candidate_score": round(best_sc, 2) if best_c is not None else None,
                **learning_extra,
                "best_candidate_is_corner_like": best_is_corner_like,
                "sendable_status": best_tier,
                "soft_subreason": soft_sub,
                "min_threshold_base": round(base_for_display, 2),
                "min_threshold_effective": round(base_for_display, 2),
                "gap_to_sendable": gap_to_sendable,
                "final_status": final_status,
                "why_not_sendable_code": why_code,
                "why_not_sendable_ru": why_ru,
                "if_not_sendable": sendable_path_ru,
            }
        )

    if global_block:
        gb = global_block.lower()
        force_map = {
            "preview_only_enabled": "blocked_preview_only",
            "blocked_stale_live_events": "blocked_stale_live_events",
        }
        if "non_live" in gb or "non_real" in gb:
            forced = "blocked_non_real_source"
        elif gb in force_map:
            forced = force_map.get(gb, gb)
            if not forced.startswith("blocked_"):
                forced = "blocked_unknown"
        else:
            forced = None
        if forced:
            for r in rows:
                r["final_status"] = forced

    status_counts = Counter(str(r["final_status"]) for r in rows)
    fresh_accepted = [
        r
        for r in rows
        if int(r.get("candidates_after_freshness") or 0) > 0 and bool(r.get("is_live"))
    ]
    status_counts_fresh = Counter(str(r["final_status"]) for r in fresh_accepted) if fresh_accepted else Counter()
    _sany_toks = {
        "blocked_invalid_live_market_text",
        "blocked_impossible_live_outcome",
        "blocked_low_live_plausibility",
        "blocked_live_market_sanity",
        "blocked_live_quality_gate",
    }
    n_sanity_fresh = sum(1 for r in fresh_accepted if str(r.get("final_status")) in _sany_toks)
    fresh_live_send_breakdown = {
        "blocked_send_filter": int(status_counts_fresh.get("blocked_send_filter", 0)),
        "blocked_integrity": int(status_counts_fresh.get("blocked_integrity", 0)),
        "blocked_low_score": int(status_counts_fresh.get("blocked_low_score", 0)),
        "blocked_duplicate_idea": int(status_counts_fresh.get("blocked_duplicate_idea", 0)),
        "blocked_dedup_db": int(db_dedup_blocked_count),
        "blocked_live_market_sanity": n_sanity_fresh,
        "selected": int(status_counts_fresh.get("selected", 0)),
    }
    best_scores_sorted = sorted(
        (float(r["best_candidate_score"]) for r in rows if r.get("best_candidate_score") is not None),
        reverse=True,
    )
    integrity_samples: list[dict] = []
    for check in (integrity_dropped_checks or [])[:5]:
        integrity_samples.append(
            {
                "source_market_label": getattr(check, "source_market_label", ""),
                "selection": getattr(check, "source_selection", ""),
                "family": getattr(check, "source_family", ""),
                "reason": getattr(check, "integrity_check_reason", ""),
            }
        )

    live_m = near_m = too_m = 0
    if send_filter_stats is not None:
        live_m = int(send_filter_stats.live_matches)
        near_m = int(send_filter_stats.near_matches)
        too_m = int(send_filter_stats.too_far_matches_dropped)
    else:
        live_m, near_m, too_m = family_svc._summarize_match_timing(fb_preview)

    families_after_send = dict(send_filter_stats.families_left) if send_filter_stats else {}
    hist_send = dict(send_filter_stats.family_histogram_input) if send_filter_stats else {}
    exotic_in = int(send_filter_stats.exotic_count_input) if send_filter_stats else 0
    exotic_after = int(send_filter_stats.exotic_count_after_filter) if send_filter_stats else 0

    fam_after_score: dict[str, int] = {}
    if finalists:
        fam_after_score, _ = family_svc.broad_family_histogram(finalists)
    fam_scored_integrity_pool: dict[str, int] = {}
    if enriched_scored:
        fam_scored_integrity_pool, _ = family_svc.broad_family_histogram(enriched_scored)

    bottleneck_hint: str | None = None
    if status_counts:
        bottleneck_hint = max(status_counts.items(), key=lambda kv: kv[1])[0]

    fresh_live_best_scores = sorted(
        (float(r["best_candidate_score"]) for r in fresh_accepted if r.get("best_candidate_score") is not None),
        reverse=True,
    )
    matches_strong_idea = sum(1 for r in rows if (r.get("best_candidate_score") or 0) >= base_for_display)
    matches_selected_n = sum(1 for r in rows if r.get("final_status") == "selected")
    matches_without_sendable = max(0, len(rows) - matches_selected_n)
    problem_status_rows = [r for r in rows if str(r.get("final_status")) not in {"selected", "no_candidates"}]
    problem_status_rows_fresh = (
        [
            r
            for r in fresh_accepted
            if str(r.get("final_status")) not in {"selected", "no_candidates"}
        ]
        if fresh_accepted
        else []
    )
    blocker_priority = [
        "blocked_no_strategy_match",
        "blocked_live_quality_gate",
        "blocked_invalid_live_market_text",
        "blocked_impossible_live_outcome",
        "blocked_low_live_plausibility",
        "blocked_live_market_sanity",
        "blocked_context_filter",
        "blocked_value_filter",
        "blocked_duplicate_idea",
        "blocked_low_score",
        "blocked_send_filter",
        "blocked_integrity",
        "blocked_too_far_in_time",
    ]
    blocker_rank = {token: idx for idx, token in enumerate(blocker_priority)}
    main_blocker_status = "unknown"
    def _dominant_blocker(counter: Counter[str]) -> str:
        return max(counter.items(), key=lambda kv: (int(kv[1]), -int(blocker_rank.get(str(kv[0]), 9999))))[0]
    if fresh_accepted:
        if not problem_status_rows_fresh:
            main_blocker_status = "none"
        else:
            main_blocker_status = _dominant_blocker(
                Counter(str(r.get("final_status")) for r in problem_status_rows_fresh)
            )
    elif rows:
        main_blocker_status = bottleneck_hint or "unknown"
        if not problem_status_rows:
            main_blocker_status = "none"
        else:
            main_blocker_status = _dominant_blocker(
                Counter(str(r.get("final_status")) for r in problem_status_rows)
            )

    m_with_send = sum(1 for r in rows if str(r.get("sendable_status") or "") in ("normal", "soft"))
    m_norm_m = sum(1 for r in rows if r.get("sendable_status") == "normal")
    m_soft_m = sum(1 for r in rows if r.get("sendable_status") == "soft")
    m_rej_gate = sum(
        1
        for r in rows
        if (r.get("candidates_after_integrity") or 0) > 0 and str(r.get("sendable_status") or "") == "reject"
    )
    football_pipeline_aggregate = {
        "total_live_matches_tracked": len(rows),
        "matches_after_freshness": len(rows),
        "with_candidates_pre_send_pipeline": sum(1 for r in rows if (r.get("candidates_after_cvf") or 0) > 0),
        "after_send_filter": sum(1 for r in rows if (r.get("candidates_after_send_filter") or 0) > 0),
        "after_integrity": sum(1 for r in rows if (r.get("candidates_after_integrity") or 0) > 0),
        "after_scoring_pool": sum(1 for r in rows if (r.get("candidates_after_scoring") or 0) > 0),
        "matches_with_sendable_idea": m_with_send,
        "normal_sendable_matches": m_norm_m,
        "soft_sendable_matches": m_soft_m,
        "rejected_at_soft_send_gate": m_rej_gate,
        "funnel_by_final_status": dict(status_counts),
        "live_sanity_dropped": len(lsd),
    }

    def _fmt_top10_line(r: dict) -> str:
        bet = (r.get("best_bet_text") or r.get("best_candidate_market") or "—")
        if len(bet) > 90:
            bet = bet[:87] + "…"  # noqa: RUF001
        fam = r.get("best_market_family") or "—"
        st0 = r.get("sendable_status") or "—"
        if st0 in ("normal", "soft"):
            rsn = (r.get("soft_subreason") or "ok")[:80]
        else:
            rsn = (r.get("if_not_sendable") or "")[:100]
        return (
            f"[{r.get('event_id')}] {str(r.get('match_name', '—'))[:50]} | {fam} | {bet} | "
            f"{r.get('best_candidate_odds', '—')} | {r.get('best_candidate_score', '—')} | {st0} | {rsn}"
        )

    top_cands = sorted(
        (r for r in rows if r.get("best_candidate_score") is not None),
        key=lambda r: float(r.get("best_candidate_score") or -1.0),
        reverse=True,
    )[:10]
    top_10_live_pipeline_lines = [_fmt_top10_line(r) for r in top_cands]
    sendable_only = [
        r
        for r in rows
        if str(r.get("sendable_status") or "") in ("normal", "soft") and (r.get("candidates_after_integrity") or 0) > 0
    ]
    sendable_live_idea_lines = [_fmt_top10_line(r) for r in sendable_only[:20]]
    bottleneck_no_sendable_ru: str | None = None
    if m_with_send == 0 and len(rows) > 0:
        ex2 = {k: int(v) for k, v in status_counts.items() if k not in ("selected", "no_candidates") and v > 0}
        st_ru_map = {
            "blocked_send_filter": "фильтр отправки (рынок/время/семейство)",
            "blocked_integrity": "integrity (тотал/маппинг/линия)",
            "blocked_no_strategy_match": "strategy gate (S8/S9)",
            "blocked_context_filter": "context filter",
            "blocked_s8_1x2_00_without_api_context": "S8 1X2 0:0 without API context",
            "blocked_s8_1x2_00_no_pressure": "S8 1X2 0:0 no pressure",
            "blocked_s8_home_00_without_api_context": "temporary S8 safety gate",
            "blocked_value_filter": "value filter",
            "blocked_low_score": "порог score + мягкий live send-gate",
            "blocked_live_quality_gate": "combat quality gate",
            "blocked_duplicate_idea": "сессия: эта идея уже в памяти",
            "blocked_too_far_in_time": "слишком далеко по времени",
            "blocked_unknown": "не классифицировано",
        }
        if ex2:
            dom0 = max(ex2.items(), key=lambda kv: kv[1])
            bottleneck_no_sendable_ru = (
                f"главный поток уходит в «{st_ru_map.get(str(dom0[0]), str(dom0[0]))}» — "
                f"{dom0[1]} из {len(rows)} live-матч."
            )
        else:
            bottleneck_no_sendable_ru = "нет кандидатов с разобранной причиной — см. final_status по матчам"
    _bl_map = {
        "blocked_no_strategy_match": ("strategy", "strategy gate (S8/S9)"),
        "blocked_context_filter": ("context", "context filter"),
        "blocked_s8_1x2_00_without_api_context": (
            "context",
            "temporary S8 safety gate (1X2 0:0 without API-Football context)",
        ),
        "blocked_s8_1x2_00_no_pressure": (
            "context",
            "temporary S8 safety gate (1X2 0:0 pressure_score < 2)",
        ),
        "blocked_s8_home_00_without_api_context": (
            "context",
            "temporary S8 safety gate (home 0:0 without API-Football context)",
        ),
        "blocked_value_filter": ("value", "value filter"),
        "blocked_low_score": ("score", "порог score"),
        "blocked_live_quality_gate": ("quality", "combat quality gate"),
        "blocked_duplicate_idea": ("duplicate_idea", "повтор идеи в сессии"),
        "blocked_send_filter": ("send_filter", "фильтр отправки"),
        "blocked_integrity": ("integrity", "проверка целостности"),
        "blocked_too_far_in_time": ("time", "слишком далеко по времени"),
        "blocked_no_enriched_scored_row": ("strategy", "strategy gate (S8/S9)"),
        "blocked_unknown": ("unknown", "не классифицировано"),
        "no_candidates": ("none", "нет кандидатов"),
        "none": ("ok", "нет блокирующих причин по матчам"),
        "selected": ("ok", "есть выбранные матчи"),
    }
    main_blocker_code_short, mb_ru = _bl_map.get(
        str(main_blocker_status) or "",
        ("cycle", "см. узкое место цикла"),
    )

    _agg_why = {
        "blocked_no_strategy_match": "после integrity не нашлось кандидата под S8/S9",
        "blocked_context_filter": "после стратегии матч отсеян context filter",
        "blocked_s8_1x2_00_without_api_context": "S8 1X2 при 0:0 временно запрещён без API-Football context",
        "blocked_s8_1x2_00_no_pressure": "S8 1X2 при 0:0 временно запрещён без pressure_score >= 2",
        "blocked_s8_home_00_without_api_context": "опасный S8-паттерн П1 0:0 без API-Football context временно запрещён",
        "blocked_value_filter": "после context filter не пройден value filter",
        "blocked_low_score": "score чуть ниже порога",
        "blocked_live_quality_gate": "не пройден combat quality gate",
        "blocked_duplicate_idea": "идея уже отправлялась в сессии",
        "blocked_send_filter": "рынок отсеян фильтром отправки",
        "blocked_integrity": "рынок не прошёл integrity",
        "blocked_too_far_in_time": "слишком далеко по времени",
        "blocked_no_enriched_scored_row": "после integrity не нашлось кандидата под S8/S9",
        "blocked_unknown": "причина не классифицирована",
    }
    non_sel_fresh = [r for r in fresh_accepted if str(r.get("final_status")) != "selected"]
    _cnt_nf = Counter(str(r.get("final_status")) for r in non_sel_fresh)
    _why_order = (
        "blocked_no_strategy_match",
        "blocked_context_filter",
        "blocked_s8_1x2_00_without_api_context",
        "blocked_s8_1x2_00_no_pressure",
        "blocked_s8_home_00_without_api_context",
        "blocked_value_filter",
        "blocked_live_quality_gate",
        "blocked_low_score",
        "blocked_duplicate_idea",
        "blocked_integrity",
        "blocked_send_filter",
        "blocked_too_far_in_time",
        "blocked_unknown",
    )
    why_no_signal_lines: list[str] = []
    for tok in _why_order:
        n = int(_cnt_nf.get(tok, 0))
        if n:
            why_no_signal_lines.append(f"{n} матч(а): {_agg_why.get(tok, tok)}")

    gap_distr_fresh = sorted(
        float(r["gap_to_sendable"])
        for r in fresh_accepted
        if str(r.get("final_status")) == "blocked_low_score" and r.get("gap_to_sendable") is not None
    )
    _candidates_gap = [
        r
        for r in fresh_accepted
        if str(r.get("final_status")) == "blocked_low_score"
        and r.get("gap_to_sendable") is not None
        and float(r.get("gap_to_sendable") or 0) > 0
    ]
    closest_fresh_live_miss: dict | None = None
    if _candidates_gap:
        hit = min(_candidates_gap, key=lambda rr: float(rr.get("gap_to_sendable") or 999))
        closest_fresh_live_miss = {
            "match_name": hit.get("match_name"),
            "best_candidate_market": hit.get("best_candidate_market"),
            "best_candidate_score": hit.get("best_candidate_score"),
            "gap_to_sendable": hit.get("gap_to_sendable"),
            "tournament_name": hit.get("tournament_name"),
            "minute": hit.get("minute"),
        }

    _qh_map = {
        "strategy": "Матчи доходят до integrity, но ни один рынок не проходит explicit strategy gate (S8/S9)",
        "context": "Стратегия что-то находит, но context filter снимает эти идеи",
        "value": "После context filter кандидаты не проходят value filter",
        "score": "Сигналы есть, но лучшим live-идеям не хватает score до порога",
        "quality": "Есть сильные идеи, но финальный combat quality gate снимает их перед отправкой",
        "duplicate_idea": "Повторные идеи блокируются, ждём новые live-сценарии",
        "send_filter": "Идеи отсекаются фильтром рынков/времени — посмотрите состав live-линии",
        "integrity": "Идеи есть, но рынок не проходит проверку целостности ставки",
        "time": "Матчи отсекаются как слишком далёкие по времени",
        "unknown": "См. сводку прогона и логи [FOOTBALL][CYCLE_DEBUG_JSON]",
        "none": "По свежим live-матчам нет блокирующей причины",
        "ok": "—",
        "cycle": "См. узкое место цикла",
    }
    football_live_quality_hint_ru = _qh_map.get(main_blocker_code_short, _qh_map["unknown"])

    selected_winner_detail: dict | None = None
    if finalists:
        sel = finalists[0]
        codes = list((sel.explanation_json or {}).get("football_scoring_reason_codes") or [])
        human_sel_rs = FootballSignalScoringService.humanize_reason_codes(codes)
        selected_winner_detail = {
            "match_name": str(sel.match.match_name or ""),
            "tournament_name": str(getattr(sel.match, "tournament_name", "") or "").strip() or None,
            "minute": _football_match_minute_from_candidate(sel),
            "bet_line": _football_format_bet_line(sel),
            "odds": str(sel.market.odds_value) if sel.market.odds_value is not None else None,
            "score": round(float(sel.signal_score or 0), 2),
            "market_family": family_svc.get_market_family(sel),
            "why_selected_lines": human_sel_rs[:10],
        }
        if finalist_send_meta and id(sel) in finalist_send_meta:
            stier, slabel = finalist_send_meta[id(sel)]
            selected_winner_detail["send_path"] = stier
            selected_winner_detail["soft_label"] = slabel
            if stier == "soft":
                selected_winner_detail["live_note"] = (
                    "Live-сигнал допущен по мягкому порогу (недобор score компенсирован live-контекстом)"
                )
        _ls0 = (sel.explanation_json or {}).get("live_sanity")
        if isinstance(_ls0, dict) and _ls0:
            if _ls0.get("passed") and _ls0.get("skipped") != "not_is_live":
                selected_winner_detail["live_sanity"] = (
                    f"ok ({_ls0.get('plausibility', 'ok')}, pscore={_ls0.get('plausibility_score', 100)})"
                )
            elif not _ls0.get("passed"):
                selected_winner_detail["live_sanity"] = f"blocked: {_ls0.get('block_token', '—')}"
            else:
                selected_winner_detail["live_sanity"] = "ok (not live or skipped check)"

    def _prog_best_line(r: dict) -> str:
        nm = str(r.get("match_name") or "—")
        bet = str(r.get("best_bet_text") or r.get("best_candidate_market") or "—")
        sc = r.get("best_candidate_score")
        st = str(r.get("final_status") or "")
        wr = str(r.get("why_not_sendable_ru") or "").strip()
        if st == "selected":
            return f"{nm} — {bet} — score {sc} — к отправке"
        if st == "blocked_low_score" and r.get("gap_to_sendable") is not None:
            return f"{nm} — {bet} — score {sc} — до порога не хватает ~{r['gap_to_sendable']}"
        if sc is not None and wr and wr != "См. final_status":
            return f"{nm} — {bet} — score {sc} — не прошла: {wr}"
        return f"{nm} — {bet} — {wr or st}"

    ranked_for_prog = sorted(
        rows,
        key=lambda r: float(r.get("best_candidate_score") or -1.0),
        reverse=True,
    )
    best_live_ideas_for_prog = [_prog_best_line(r) for r in ranked_for_prog[:5]]

    bottleneck_hint_fresh = None
    if fresh_accepted and status_counts_fresh:
        bottleneck_hint_fresh = max(status_counts_fresh.items(), key=lambda kv: kv[1])[0]

    live_quality_summary = {
        "fresh_live_matches": len(rows),
        "fresh_live_accepted_count": len(fresh_accepted),
        "matches_with_strong_idea": matches_strong_idea,
        "matches_without_sendable": matches_without_sendable,
        "matches_marked_selected": matches_selected_n,
        "main_blocker_code": main_blocker_code_short,
        "main_blocker_status": main_blocker_status,
        "main_blocker_ru": mb_ru,
        "min_signal_score": min_score,
        "min_signal_score_base": round(base_for_display, 2),
        "min_signal_score_effective": round(base_for_display, 2),
        "score_relief_note": score_relief_note,
        "live_send_stats": live_send_stats or {},
        "football_live_quality_hint_ru": football_live_quality_hint_ru,
        "fresh_live_send_breakdown": fresh_live_send_breakdown,
        "why_no_signal_lines": why_no_signal_lines,
        "gap_to_sendable_fresh_low_score": [round(g, 2) for g in gap_distr_fresh[:25]],
        "closest_fresh_live_miss": closest_fresh_live_miss,
        "fresh_live_best_scores_distribution": [round(x, 2) for x in fresh_live_best_scores[:20]],
        "best_live_ideas_lines": best_live_ideas_for_prog,
        "bottleneck_hint_fresh_live": bottleneck_hint_fresh,
    }

    live_quality_summary["football_pipeline_aggregate"] = football_pipeline_aggregate
    if bottleneck_no_sendable_ru and m_with_send == 0:
        live_quality_summary["bottleneck_no_sendable_pipeline_ru"] = bottleneck_no_sendable_ru

    debug = {
        "global_block": global_block,
        "dry_run": dry_run,
        "min_signal_score": min_score,
        "min_signal_score_base": round(base_for_display, 2),
        "min_signal_score_effective": round(base_for_display, 2),
        "score_relief_note": score_relief_note,
        "live_send_stats": live_send_stats or {},
        "live_quality_summary": live_quality_summary,
        "best_live_ideas_for_prog": best_live_ideas_for_prog,
        "why_no_signal_lines": why_no_signal_lines,
        "fresh_live_send_breakdown": fresh_live_send_breakdown,
        "selected_winner_detail": selected_winner_detail,
        "closest_fresh_live_miss": closest_fresh_live_miss,
        "football_live_quality_hint_ru": football_live_quality_hint_ru,
        "time_buckets_unique_matches": {"live": live_m, "near": near_m, "too_far": too_m},
        "final_status_counts": dict(status_counts),
        "final_status_counts_fresh_live_accepted": dict(status_counts_fresh),
        "bottleneck_hint_fresh_live": bottleneck_hint_fresh,
        "best_scores_all_matches": [round(x, 2) for x in best_scores_sorted[:25]],
        "send_filter_drop_reasons": dict(send_filter_stats.drop_reasons) if send_filter_stats else {},
        "family_histogram_before_send_filter": hist_send,
        "exotic_count_before_send_filter": exotic_in,
        "exotic_count_after_send_filter": exotic_after,
        "families_left_after_send_filter": families_after_send,
        "family_buckets_after_scoring_integrity_pool": fam_scored_integrity_pool,
        "family_buckets_after_scoring_finalists": fam_after_score,
        "bottleneck_hint": bottleneck_hint,
        "integrity_fail_samples": integrity_samples,
        "football_pipeline_aggregate": football_pipeline_aggregate,
        "top_10_live_pipeline_lines": top_10_live_pipeline_lines,
        "sendable_live_idea_lines": sendable_live_idea_lines,
        "bottleneck_no_sendable_pipeline_ru": bottleneck_no_sendable_ru,
        "matches": rows,
        "matches_top_for_message": _football_top_matches_for_telegram(rows, limit=10),
        "blocked_dedup_note": "dedup runs only on live ingest; dry_run does not evaluate DB dedup per match",
        "pipeline_live_only": True,
        "live_sanity_drops": [
            {"eid": e, "block_token": lsd.get(e, ""), "reason": lsr.get(e, "")} for e in lsd
        ],
    }
    try:
        logger.info("[FOOTBALL][LIVE_QUALITY] %s", json.dumps(live_quality_summary, default=str, ensure_ascii=False)[:12000])
    except Exception:
        logger.info("[FOOTBALL][LIVE_QUALITY] (serialization failed)")
    try:
        logger.info("[FOOTBALL][CYCLE_DEBUG_JSON] %s", json.dumps(debug, default=str, ensure_ascii=False)[:24000])
    except Exception:
        logger.info("[FOOTBALL][CYCLE_DEBUG_JSON] (serialization failed)")
    return debug


def _infer_football_live_cycle_bottleneck(res: AutoSignalCycleResult, diag: dict | None = None) -> str:
    """Один доминирующий bottleneck за цикл (для статуса и логов)."""
    msg = (res.message or "").strip().lower()
    rr = (res.rejection_reason or "").strip().lower()
    diag = diag or {}
    dbg = res.football_cycle_debug or {}
    if isinstance(dbg, dict):
        lq = dbg.get("live_quality_summary") or {}
        main_blocker_status = str(lq.get("main_blocker_status") or "").strip().lower()
        if main_blocker_status and main_blocker_status not in {"unknown", "none", "ok"}:
            return main_blocker_status
        gb = str(dbg.get("global_block") or "").strip().lower()
        if gb in {
            "blocked_no_strategy_match",
            "blocked_context_filter",
            "blocked_s8_1x2_00_without_api_context",
            "blocked_s8_1x2_00_no_pressure",
            "blocked_s8_home_00_without_api_context",
            "blocked_value_filter",
        }:
            return gb
    if msg == "paused" or "paused" in rr:
        return "blocked_paused"
    if msg == "football_disabled" or "football_disabled" in rr:
        return "blocked_football_disabled"
    if msg.startswith("sport_disabled"):
        return "blocked_sport_disabled"
    if msg == "provider_not_configured":
        return "blocked_provider_not_configured"
    if msg == "football_live_session_inactive":
        return "blocked_no_live_session"
    if msg == "blocked_winline_live_unavailable":
        return "blocked_winline_live_unavailable"
    if msg == "blocked_stale_manual_live_source" or rr == "blocked_stale_manual_live_source":
        # If live API already failed (e.g. quota), that is the dominant operator-facing reason.
        lauth = str((diag or {}).get("live_auth_status") or "").lower()
        if lauth in ("unauthorized_quota", "out_of_usage_credits", "unauthorized"):
            return "blocked_live_provider_auth_or_quota"
        return "blocked_stale_manual_live_source"
    if msg == "blocked_stale_live_source" or rr == "blocked_stale_live_source":
        return "blocked_stale_live_source"
    if msg == "blocked_stale_live_events" or rr == "blocked_stale_live_events":
        return "blocked_stale_live_events"
    if msg == "blocked_no_strategy_match" or rr == "blocked_no_strategy_match":
        return "blocked_no_strategy_match"
    if msg == "blocked_context_filter" or rr == "blocked_context_filter":
        return "blocked_context_filter"
    if msg == "blocked_s8_1x2_00_without_api_context" or rr == "blocked_s8_1x2_00_without_api_context":
        return "blocked_s8_1x2_00_without_api_context"
    if msg == "blocked_s8_1x2_00_no_pressure" or rr == "blocked_s8_1x2_00_no_pressure":
        return "blocked_s8_1x2_00_no_pressure"
    if msg == "blocked_s8_home_00_without_api_context" or rr == "blocked_s8_home_00_without_api_context":
        return "blocked_s8_home_00_without_api_context"
    if msg == "blocked_value_filter" or rr == "blocked_value_filter":
        return "blocked_value_filter"
    if msg == "blocked_live_quality_gate" or rr == "blocked_live_quality_gate":
        return "blocked_live_quality_gate"
    if not res.fetch_ok and ("unauthorized" in rr or "quota" in rr or "live_unavailable" in rr):
        return "blocked_live_provider_auth_or_quota"
    if not res.fetch_ok:
        return "blocked_fetch"
    if msg == "non_live_source_blocked" or "non_real_source" in rr:
        return "blocked_non_real_source"
    if "non_live_source" in rr:
        return "blocked_non_live_source"
    if msg == "preview_only" or "preview_only" in rr:
        return "blocked_preview_only"
    if msg == "payload_is_not_dict":
        return "blocked_bad_payload"
    if res.preview_candidates == 0 and res.fetch_ok:
        return "blocked_no_live_matches"
    if (res.report_after_filter or 0) == 0 and (res.preview_candidates or 0) > 0:
        return "blocked_send_filter"
    if (res.report_after_integrity or 0) == 0 and (res.report_after_filter or 0) > 0:
        return "blocked_integrity"
    if (
        int(diag.get("football_live_cycle_after_score") or 0) > 0
        and int(diag.get("football_live_cycle_new_ideas_sendable") or 0) == 0
        and int(diag.get("football_live_cycle_duplicate_ideas_blocked") or 0) > 0
    ):
        return "blocked_duplicate_idea"
    if (msg in ("low_score", "dry_run_low_score") or "low_score" in rr) and int(
        diag.get("football_live_sanity_blocked_last_cycle") or 0
    ) > 0 and int(diag.get("football_live_cycle_after_score") or 0) > 0:
        return "blocked_live_market_sanity"
    low = msg in {"low_score", "dry_run_low_score"} or "low_score" in rr
    if low:
        return "blocked_low_score"
    if msg == "dedup_blocked" or res.report_rejection_code == "blocked_by_dedup" or "dedup" in rr:
        return "blocked_dedup_db"
    if res.created_signals_count > 0 and res.notifications_sent_count == 0:
        return "blocked_notify_config"
    if msg == "ok" and res.notifications_sent_count > 0:
        return "ok_sent_telegram"
    if msg == "dry_run_ok":
        return "dry_run_ok"
    if msg == "ok":
        return "ok_no_signal_selected"
    return "blocked_unknown"


def _combat_bottleneck_ru(token: str | None) -> str:
    if not token or token == "—":
        return "нет данных"
    m = {
        "blocked_paused": "контур на паузе",
        "blocked_no_live_session": "live-сессия не запущена (нужен ▶️ Старт)",
        "blocked_no_live_matches": "нет live-матчей футбола в выборке провайдера",
        "blocked_send_filter": "все отсеяны фильтром отправки (live/семья/время)",
        "blocked_integrity": "не прошли проверку целостности ставки",
        "blocked_no_strategy_match": "после integrity ни один рынок не прошёл strategy gate (S8/S9)",
        "blocked_context_filter": "после strategy gate кандидаты сняты context filter",
        "blocked_s8_1x2_00_without_api_context": "временный safety-rule снял S8 1X2 0:0 без API-Football context",
        "blocked_s8_1x2_00_no_pressure": "временный safety-rule снял S8 1X2 0:0 без pressure_score >= 2",
        "blocked_s8_home_00_without_api_context": "временный safety-rule снял S8 П1 0:0 без API-Football context/pressure",
        "blocked_value_filter": "после context filter кандидаты сняты value filter",
        "blocked_low_score": "score ниже порога",
        "blocked_duplicate_idea": "повтор той же идеи в рамках live-сессии",
        "blocked_dedup_db": "отсеяно дедупликацией в базе",
        "blocked_football_disabled": "футбол выключен в настройках",
        "blocked_sport_disabled": "источник отключён",
        "blocked_provider_not_configured": "провайдер не настроен",
        "blocked_non_real_source": "источник не считается боевым live",
        "blocked_non_live_source": "источник не в режиме live",
        "blocked_notify_config": "сигнал создан, но уведомление не ушло (чат/пауза)",
        "blocked_fetch": "ошибка загрузки у провайдера",
        "blocked_live_provider_auth_or_quota": "Live API: авторизация или квота",
        "blocked_preview_only": "включён только preview в .env",
        "ok_sent_telegram": "сообщение ушло в Telegram",
        "dry_run_ok": "тестовый прогон",
        "ok_no_signal_selected": "цикл завершён без выбранной ставки",
        "blocked_unknown": "причина не классифицирована",
        "blocked_stale_manual_live_source": "ручной live JSON слишком старый",
        "blocked_stale_live_source": "снимок live устарел по задержке обработки",
        "blocked_stale_live_events": "все live-матчи признаны протухшими",
        "blocked_winline_live_unavailable": "Winline live feed недоступен (WS/данные/тип-лайн)",
        "blocked_live_market_sanity": "все кандидаты сняты pre-send live sanity",
        "blocked_invalid_live_market_text": "некорректный маппинг/текст live-рынка",
        "blocked_impossible_live_outcome": "исход несовместим с текущим счётом (невозможен)",
        "blocked_low_live_plausibility": "низкая plausibility (поздний тайм / счёт / тотал)",
        "blocked_suspicious_core_live_signal": "сомнительный core live-сигнал (контекст/линия)",
        "blocked_missing_live_context_from_source": "нет счёта/минуты в снимке провайдера (1X2)",
        "blocked_live_quality_gate": "не прошёл combat quality gate",
        "blocked_core_late_high_gap_total": "тотал: слишком много голов нужно на поздней стадии",
        "blocked_late_live_market": "поздняя стадия / timing: рынок уже неадекватен для live-сигнала",
        "blocked_no_enriched_scored_row": "после integrity ни один рынок не прошёл strategy gate (S8/S9)",
    }
    return m.get(str(token), str(token).replace("_", " "))


def format_final_live_gate_summary_lines(fg: dict, *, max_rows: int = 24) -> list[str]:
    """Shared UI block: final live send gate (core main markets only)."""
    if not isinstance(fg, dict) or fg.get("per_match") is None:
        return []
    lines: list[str] = [
        "🧱 Final live send gate (только core main markets, макс. 1 сигнал на матч за цикл):",
    ]
    chk = int(fg.get("live_matches_total") or len(fg.get("per_match") or []) or 0)
    mskip = int(fg.get("matches_skipped") or 0)
    mpass = int(fg.get("matches_sent_after_final_gate") or fg.get("matches_with_send") or 0)
    bnmain = int(fg.get("blocked_non_main_live_market_hits") or 0)
    bexo = int(fg.get("blocked_exotic_result_market_hits") or 0)
    smain = int(fg.get("sent_main_markets_count") or mpass or 0)
    msan = int(fg.get("matches_blocked_live_sanity") or 0)
    lines.append(
        f"   Матчей проверено: {chk} · отсеяно gate: {mskip} · к отправке после timing sanity: {mpass}"
    )
    lines.append(
        f"   blocked_non_main_live_market (хиты): {bnmain} · "
        f"blocked_exotic_result_market (хиты): {bexo} · sent_main_markets_count: {smain}"
    )
    if msan:
        lines.append(f"   Матчей отсеяно live sanity (все core-кандидаты): {msan}")
    s_core = int(fg.get("suspicious_core_signals_blocked") or 0)
    x_core = int(fg.get("core_live_extra_sanity_blocked") or 0)
    lg = int(fg.get("late_game_live_sanity_blocked") or 0)
    if s_core or x_core or lg:
        lines.append(
            f"   core live quality + timing: suspicious totals/context={s_core} · "
            f"late-gap / plausibility={x_core} · late-game timing cutoffs={lg}"
        )
    bc = int(fg.get("blocked_cards_or_special_hits") or 0)
    if bc:
        lines.append(f"   Карточки/мусор (whitelist): {bc}")
    lines.append(
        f"   Итого после gate (кандидатов): {fg.get('matches_with_send')} · "
        f"отсеяно gate: {fg.get('matches_skipped')}"
    )
    af = fg.get("allowed_families")
    if isinstance(af, list) and af:
        lines.append(f"   семей классификатора: {', '.join(af)}")
    lines.append("")
    for row in (fg.get("per_match") or [])[:max_rows]:
        if not isinstance(row, dict):
            continue
        sk = row.get("match_send_skipped")
        br = row.get("blocked_reason")
        tail = row.get("skip_reason") or row.get("chosen_reason") or "—"
        ch = (row.get("chosen_final_candidate") or row.get("chosen_allowed_candidate") or "—")[:88]
        fpd = row.get("full_pipeline_decision")
        extra = f" | → {fpd}" if fpd else ""
        forb = row.get("forbidden_finalists_count")
        forb_s = f" · forb={forb}" if forb is not None else ""
        br_s = f" · {str(br)[:48]}" if br else ""
        lines.append(
            f"  • {row.get('event_id')} {str(row.get('match_name') or '')[:34]} | "
            f"{'SKIP' if sk else 'OK'}{forb_s}{br_s} | {ch} | {str(tail)[:92]}{extra}"
        )
    lines.append("")
    return lines


def _format_football_live_cadence_head_lines() -> list[str]:
    """Lines for ▶️ Старт / status: bounded adaptive interval policy (telemetry-driven)."""
    from app.core.config import get_settings
    from app.services.signal_runtime_diagnostics_service import SignalRuntimeDiagnosticsService

    settings = get_settings()
    diag = SignalRuntimeDiagnosticsService().get_state()
    lines = [
        "",
        "— Football LIVE cadence (между циклами) —",
        f"• Режим: адаптивная пауза по телеметрии (база {int(settings.football_live_pacing_base_interval_seconds)}s, "
        f"границы {int(settings.football_live_pacing_min_interval_seconds)}–{int(settings.football_live_pacing_max_interval_seconds)}s).",
    ]
    iv = diag.get("football_live_pacing_current_interval_seconds")
    rs = diag.get("football_live_pacing_last_reason_ru")
    if iv is not None:
        lines.append(f"• Текущий интервал до следующего цикла: ~{float(iv):.0f} s")
    if isinstance(rs, str) and rs.strip():
        lines.append(f"• Обоснование интервала: {rs[:900]}")
    else:
        lines.append("• Интервал после первого цикла подставится из метрик fetch/total cycle.")
    lines.append(
        "• Правила: тяжёлый fetch / ошибка / пустой снимок / не меняющийся snapshot → дольше пауза; "
        "лёгкий успешный цикл → короче; серия сбоев наращивает backoff (с потолком)."
    )
    return lines


def _format_football_live_cadence_user_short_lines() -> list[str]:
    """Short cadence lines for normal users (no verbose pacing reason blob)."""
    from app.core.config import get_settings
    from app.services.signal_runtime_diagnostics_service import SignalRuntimeDiagnosticsService

    settings = get_settings()
    diag = SignalRuntimeDiagnosticsService().get_state()
    lines = [
        "",
        "⏱ Между циклами — адаптивная пауза "
        f"({int(settings.football_live_pacing_min_interval_seconds)}–{int(settings.football_live_pacing_max_interval_seconds)} с, "
        f"ориентир {int(settings.football_live_pacing_base_interval_seconds)} с).",
    ]
    iv = diag.get("football_live_pacing_current_interval_seconds")
    if iv is not None:
        lines.append(f"Следующий цикл примерно через {float(iv):.0f} с.")
    return lines


def _football_user_friendly_cycle_message(msg: str | None, rejection: str | None) -> str | None:
    """Map internal cycle messages to short RU lines (no raw tokens in user UI)."""
    m = (msg or "").strip()
    r = (rejection or "").strip()
    if not m and not r:
        return None
    low = f"{m} {r}".lower()
    if "session_inactive" in low or "football_live_session_inactive" in low:
        return "Сессия была неактивна в начале цикла — при необходимости повторите ▶️ Старт."
    if "winline" in low and ("unavailable" in low or "blocked" in low):
        return "Не удалось стабильно получить live-данные с основного источника."
    if "paused" in low:
        return "Контур был на паузе."
    if "provider_not_configured" in low:
        return "Провайдер odds не настроен в окружении."
    return None


def format_football_session_start_user_message(
    cres: AutoSignalCycleResult,
    *,
    duration_minutes: int | None = None,
    persistent: bool = True,
) -> str:
    """Short user-facing text after «▶️ Старт» (no internal tokens, no per-match dump)."""
    from app.services.signal_runtime_diagnostics_service import SignalRuntimeDiagnosticsService

    if persistent:
        head = "⚽ Live-сессия запущена"
        sub = "Работает до остановки ⏸ Стоп."
    else:
        head = f"⚽ Live-сессия запущена на {int(duration_minutes or 15)} мин"
        sub = ""
    cadence = _format_football_live_cadence_user_short_lines()
    d = cres.football_cycle_debug
    diag = SignalRuntimeDiagnosticsService().get_state()
    if not isinstance(d, dict) or not d:
        lines = [head]
        if sub:
            lines.append(sub)
        lines.extend([*cadence, ""])
        lines.append("Первый цикл завершён. Подробная техническая сводка доступна админам: /football_live_debug")
        uf = _football_user_friendly_cycle_message(cres.message, cres.rejection_reason)
        if uf:
            lines.append(uf)
        diag0 = SignalRuntimeDiagnosticsService().get_state()
        bn0 = str(
            diag0.get("football_last_combat_bottleneck")
            or _infer_football_live_cycle_bottleneck(cres, diag0)
        )
        lines.append(f"Главная причина: {_combat_bottleneck_ru(bn0)}")
        return "\n".join(lines)

    agg = d.get("football_pipeline_aggregate") or {}
    lq = d.get("live_quality_summary") or {}
    live_n = int(agg.get("total_live_matches_tracked") or 0)
    source_live_n = int(cres.raw_events_count or diag.get("football_winline_football_event_count") or live_n)
    s_norm = int(agg.get("normal_sendable_matches") or 0)
    s_soft = int(agg.get("soft_sendable_matches") or 0)
    s_total = s_norm + s_soft
    after_sanity = int(diag.get("football_live_sanity_blocked_last_cycle") or 0)
    n_sent = int(cres.notifications_sent_count or 0)
    n_db = int(cres.created_signals_count or 0)

    lines_u: list[str] = [head]
    if sub:
        lines_u.append(sub)
    lines_u.extend([*cadence, ""])
    lines_u.append(f"📊 Live-матчей в источнике: {source_live_n}")
    lines_u.append(f"🧹 После bridge/freshness в контуре: {live_n}")
    lines_u.append(f"🎯 Подходящих сигналов (готовых к отправке): {s_total}")
    lines_u.append(f"💾 Записано в базу: {n_db}  ·  📨 Отправлено в Telegram: {n_sent}")
    lines_u.append("")

    if n_sent > 0:
        lines_u.append("✅ Сигналы отправлены в канал (если настроен чат сигналов).")
    elif s_total == 0 and live_n > 0:
        lines_u.append("❌ Сейчас сигналов нет.")
    elif s_total > 0 and n_db == 0:
        lines_u.append("❌ Сигналы не записаны в базу (ограничения или дедупликация).")
    elif s_total > 0 and n_db > 0 and n_sent == 0:
        lines_u.append("❌ Сигналы в базе есть, в Telegram не ушли (проверьте чат и настройки).")
    else:
        lines_u.append("ℹ️ В этом цикле сигналов нет.")

    bn = str(
        diag.get("football_last_combat_bottleneck")
        or diag.get("football_live_cycle_bottleneck")
        or _infer_football_live_cycle_bottleneck(cres, diag)
    )
    lines_u.append(f"Главная причина: {_combat_bottleneck_ru(bn)}")

    wns = lq.get("why_no_signal_lines") or []
    if isinstance(wns, list) and wns and n_sent == 0:
        lines_u.append("")
        lines_u.append("Дополнительно:")
        for row in wns[:5]:
            if isinstance(row, str) and row.strip():
                lines_u.append(f"• {row.strip()}")
    if after_sanity and n_sent == 0:
        lines_u.append(f"• Перед отправкой отсеяно проверок честности рынка: {after_sanity}")

    mb_ru = lq.get("main_blocker_ru")
    if (
        isinstance(mb_ru, str)
        and mb_ru.strip()
        and n_sent == 0
        and "см." not in mb_ru.lower()
        and "cycle_debug" not in mb_ru.lower()
    ):
        lines_u.append(f"• {mb_ru.strip()[:280]}")

    lines_u.append("")
    lines_u.append("Подробный технический разбор (админы): /football_live_debug")
    return "\n".join(lines_u).strip()


def format_football_session_start_debug_message(
    cres: AutoSignalCycleResult,
    *,
    duration_minutes: int | None = None,
    persistent: bool = True,
) -> str:
    """Full legacy breakdown: per-match rows, final gate, internal statuses (admin / logs)."""
    from app.services.signal_runtime_diagnostics_service import SignalRuntimeDiagnosticsService

    if persistent:
        head = "⚽ Live-сессия запущена (до ручной остановки ⏸ Стоп) [debug]"
    else:
        head = f"⚽ Live-сессия запущена на {int(duration_minutes or 15)} мин [debug]"
    cadence = _format_football_live_cadence_head_lines()
    d = cres.football_cycle_debug
    if not isinstance(d, dict) or not d:
        lines = [head, *cadence, ""]
        lines.append("Первый боевой live-цикл завершён, подробной разбивки по матчам в ответе нет.")
        if cres.message:
            lines.append(f"Статус: {cres.message}")
        if cres.rejection_reason:
            lines.append(f"Деталь: {cres.rejection_reason}")
        diag0 = SignalRuntimeDiagnosticsService().get_state()
        bn0 = str(diag0.get("football_last_combat_bottleneck") or _infer_football_live_cycle_bottleneck(cres, diag0))
        lines.append(f"Узкое место: {_combat_bottleneck_ru(bn0)}  ({bn0})")
        return "\n".join(lines)

    diag = SignalRuntimeDiagnosticsService().get_state()
    agg = d.get("football_pipeline_aggregate") or {}
    lq = d.get("live_quality_summary") or {}
    matches: list[dict] = list(d.get("matches") or [])

    live_n = int(agg.get("total_live_matches_tracked") or 0)
    source_live_n = int(cres.raw_events_count or diag.get("football_winline_football_event_count") or live_n)
    w_c = int(agg.get("with_candidates_pre_send_pipeline") or 0)
    af = int(agg.get("after_send_filter") or 0)
    ai = int(agg.get("after_integrity") or 0)
    asc = int(agg.get("after_scoring_pool") or 0)
    s_norm = int(agg.get("normal_sendable_matches") or 0)
    s_soft = int(agg.get("soft_sendable_matches") or 0)
    s_total = s_norm + s_soft
    strong = int(lq.get("matches_with_strong_idea") or 0)
    after_sanity = int(diag.get("football_live_sanity_blocked_last_cycle") or 0)

    n_sent = int(cres.notifications_sent_count or 0)
    n_db = int(cres.created_signals_count or 0)

    lines: list[str] = [
        head,
        *cadence,
        "",
        f"📊 Live-матчей в источнике: {source_live_n}",
        f"🧹 После bridge/freshness (в контуре): {live_n}",
        f"🧩 С кандидатами (после препроцессинга): {w_c}",
        f"⬇️ После send filter: {af} матч(ей) с кандидатами",
        f"⬇️ После integrity: {ai}",
        f"⬇️ После scoring (пул): {asc}",
        f"🎯 Сильных идей (score ≥ порога): {strong}",
        f"✅ Sendable: normal {s_norm} · soft {s_soft} (всего {s_total})",
    ]
    if after_sanity:
        lines.append(f"🛡 Live sanity: отсеяно кандидатов на pre-send: {after_sanity}")
    lines.append(f"💾 Создано в БД: {n_db}  ·  📨 Ушло в Telegram: {n_sent}")
    lines.append("")

    if n_sent > 0:
        lines.append("✅ Сигналы ушли в канал (см. chat_id сигналов).")
    elif s_total == 0 and live_n > 0:
        lines.append("❌ Сейчас сигналов нет (ни одна live-идея не прошла sendable-gate).")
    elif s_total > 0 and n_db == 0:
        lines.append("❌ Сигналы не записаны: отсеяны на этапе БД (dedup) или лимиты.")
    elif s_total > 0 and n_db > 0 and n_sent == 0:
        lines.append("❌ Сигналы в БД есть, но в Telegram не ушли (чат/оркестрация).")
    else:
        lines.append("ℹ️ Сигналов в этом прогоне нет — см. причины по матчам ниже.")
    lines.append("")

    bn = str(
        diag.get("football_last_combat_bottleneck")
        or diag.get("football_live_cycle_bottleneck")
        or _infer_football_live_cycle_bottleneck(cres, diag)
    )
    lines.append("🔎 Главный bottleneck (цикл):")
    lines.append(f"   {_combat_bottleneck_ru(bn)}  ({bn})")
    bnp = d.get("bottleneck_no_sendable_pipeline_ru")
    if isinstance(bnp, str) and bnp.strip() and s_total == 0:
        lines.append(f"   {bnp.strip()}")
    hint = lq.get("football_live_quality_hint_ru")
    if isinstance(hint, str) and hint.strip() and "—" not in hint and "см. узкое" not in hint.lower():
        lines.append(f"   Подсказка: {hint.strip()[:400]}")
    lines.append("")

    # Delivery trace (when ingest ran)
    cdt = d.get("combat_delivery_trace")
    if isinstance(cdt, list) and cdt:
        lines.append("🧾 Доставка (ingest → notify):")
        for tr in cdt[:5]:
            if not isinstance(tr, dict):
                continue
            sid = tr.get("signal_id")
            fin = tr.get("final_outcome")
            ddb = tr.get("blocked_by_db_dedup")
            ntf = tr.get("notify_attempted")
            sent = tr.get("bot_send_message_effective")
            lines.append(
                f"  • id={sid}  outcome={fin}  db_dedup={ddb}  notify={ntf}  tg={sent}"
            )
        if len(cdt) > 5:
            lines.append(f"  … +{len(cdt) - 5} ещё")
        lines.append("")

    fg = d.get("final_live_send_gate") or {}
    lines.extend(format_final_live_gate_summary_lines(fg))

    if matches:
        lines.append("— Все live-матчи (лучшая идея на матч) —")
    max_rows = 40
    ranked = sorted(
        matches,
        key=lambda r: float(r.get("best_candidate_score") or -1.0),
        reverse=True,
    )
    for r in ranked[:max_rows]:
        mname = str(r.get("match_name") or "—")[:56]
        bet = (str(r.get("best_bet_text") or r.get("best_candidate_market") or "—"))[:64]
        sc = r.get("best_candidate_score")
        scs = f"{sc:.1f}" if sc is not None else "—"
        fst = str(r.get("final_status") or "—")
        eid = str(r.get("event_id") or "—")[:20]
        why = (str(r.get("if_not_sendable") or r.get("why_not_sendable_ru") or "") or fst)[:120]
        le = (str(r.get("league") or "—"))[:32]
        mn = r.get("minute")
        mpart = f"  {mn}'" if mn is not None else ""
        lines.append(
            f"• [{eid}] {mname}{mpart} ({le})"
        )
        lines.append(f"  {bet}  score={scs}  →  {fst}")
        if why and why != fst:
            lines.append(f"  {why}")
    if len(matches) > max_rows:
        lines.append(f"… и ещё {len(matches) - max_rows} матч(ей) (см. логи сервера)")

    lsani = d.get("live_sanity_drops")
    if isinstance(lsani, list) and lsani:
        lines.append("")
        lines.append("🛡 Live sanity (топ):")
        for s in lsani[:5]:
            if isinstance(s, dict):
                lines.append(f"  • eid {s.get('eid', '—')[:24]}: {s.get('reason', '')[:180]}")

    return "\n".join(lines).strip()


def _apply_last_combat_cycle_diagnostics(res: AutoSignalCycleResult) -> None:
    if res.dry_run:
        return
    diag = SignalRuntimeDiagnosticsService().get_state()
    bn = str(
        diag.get("football_live_cycle_bottleneck")
        or _infer_football_live_cycle_bottleneck(res, diag)
    )
    dbg = res.football_cycle_debug or {}
    lq = dbg.get("live_quality_summary") or {}
    lss = lq.get("live_send_stats") or {}
    sm = str(diag.get("football_last_cycle_send_mode") or "none")
    SignalRuntimeDiagnosticsService().update(
        football_last_combat_cycle_at=datetime.now(timezone.utc).isoformat(),
        football_last_combat_messages_sent=int(res.notifications_sent_count or 0),
        football_last_combat_created_signals=int(res.created_signals_count or 0),
        football_last_combat_bottleneck=bn,
        football_last_combat_bottleneck_ru=_combat_bottleneck_ru(bn),
        football_last_combat_send_mode=sm,
        football_last_combat_fresh_live_matches=int(
            lq.get("fresh_live_matches") or diag.get("football_live_quality_fresh_matches") or 0
        ),
        football_last_combat_normal_sendable=int(lss.get("normal_sendable") or 0),
        football_last_combat_soft_sendable_total=int(lss.get("soft_sendable_total") or 0),
        football_last_combat_rejected_total=int(lss.get("rejected_total") or 0),
        football_last_combat_session_idea_dedup=int(dbg.get("session_idea_dedup_this_cycle") or 0),
        football_last_combat_db_dedup_skipped=int(diag.get("football_last_cycle_db_dedup_skipped") or 0),
    )


def _football_log_live_session_report(*, res: AutoSignalCycleResult, diag: dict) -> None:
    """Расширенный отчёт одного цикла live-контура (JSON в лог)."""
    snap = FootballLiveSessionService().snapshot()
    rem = FootballLiveSessionService().remaining_seconds()
    bn = diag.get("football_live_cycle_bottleneck") or _infer_football_live_cycle_bottleneck(res, diag)
    dbg = res.football_cycle_debug or {}
    lq = dbg.get("live_quality_summary") or {}
    lss = lq.get("live_send_stats") or {}
    payload = {
        "session_active": snap.active,
        "session_started_at": snap.started_at.isoformat() if snap.started_at else None,
        "session_expires_at": snap.expires_at.isoformat() if snap.expires_at else None,
        "remaining_minutes": round(rem / 60.0, 2) if rem is not None else None,
        "last_cycle_at": snap.last_cycle_at.isoformat() if snap.last_cycle_at else None,
        "source_mode": diag.get("source_mode"),
        "source_age_seconds": diag.get("football_live_source_age_seconds"),
        "source_timestamp": diag.get("football_live_source_timestamp"),
        "source_freshness_label": diag.get("football_live_source_freshness"),
        "stale_source": diag.get("football_live_stale_source"),
        "live_freshness_candidates_before": diag.get("football_live_freshness_candidates_before"),
        "live_freshness_events_accepted": diag.get("football_live_freshness_live_events_accepted"),
        "live_freshness_stale_events_dropped": diag.get("football_live_freshness_stale_events_dropped"),
        "live_freshness_stale_markets_dropped": diag.get("football_live_freshness_stale_markets_dropped"),
        "live_matches_found": diag.get("football_live_cycle_live_matches_found"),
        "candidates_before_filter": diag.get("football_live_cycle_candidates_before_filter"),
        "candidates_after_send_filter": diag.get("football_live_cycle_after_send_filter"),
        "candidates_after_integrity": diag.get("football_live_cycle_after_integrity"),
        "candidates_after_score": diag.get("football_live_cycle_after_score"),
        "new_ideas_sendable": diag.get("football_live_cycle_new_ideas_sendable"),
        "duplicate_ideas_blocked_session_total": snap.duplicate_ideas_blocked_session,
        "duplicate_ideas_blocked_last_cycle": diag.get("football_live_cycle_duplicate_ideas_blocked"),
        "db_signals_created_session": snap.signals_sent_in_session,
        "telegram_messages_sent_session": snap.telegram_messages_sent_in_session,
        "bottleneck": bn,
        "live_quality_fresh_matches": diag.get("football_live_quality_fresh_matches"),
        "live_quality_strong_idea_matches": diag.get("football_live_quality_strong_idea_matches"),
        "live_quality_no_sendable_matches": diag.get("football_live_quality_no_sendable_matches"),
        "live_quality_main_blocker": diag.get("football_live_quality_main_blocker"),
        "live_quality_main_blocker_ru": diag.get("football_live_quality_main_blocker_ru"),
        "best_scores_distribution_hint": diag.get("football_live_best_scores_distribution_hint"),
        "dry_run": res.dry_run,
        "provider": res.source_name,
        "live_auth_status": res.live_auth_status,
        "effective_source": diag.get("football_live_effective_source"),
        "is_real_source": diag.get("is_real_source"),
        "fetch_ok": res.fetch_ok,
        "notifications_sent_count": res.notifications_sent_count,
        "created_signals_count": res.created_signals_count,
        "last_notify_path": diag.get("football_live_last_notify_path"),
        "last_delivery_reason": diag.get("last_delivery_reason"),
        "rejected_at_send_gate": diag.get("football_live_rejected_at_send_gate"),
        "ingest_normal_last": diag.get("football_last_cycle_ingest_normal"),
        "ingest_soft_last": diag.get("football_last_cycle_ingest_soft"),
        "last_send_mode": diag.get("football_last_cycle_send_mode"),
        "db_dedup_skipped_last_ingest": diag.get("football_last_cycle_db_dedup_skipped"),
        "post_selection_hint_ru": diag.get("football_live_post_selection_hint_ru"),
        "fresh_live_matches": lq.get("fresh_live_matches"),
        "live_send_stats": lss,
        "session_idea_dedup_this_cycle": dbg.get("session_idea_dedup_this_cycle"),
        "football_primary_source": diag.get("football_primary_live_source"),
        "winline_football_events": diag.get("football_winline_football_event_count"),
        "winline_football_candidates": diag.get("football_winline_football_candidate_count"),
    }
    try:
        logger.info("[FOOTBALL][LIVE_SESSION_REPORT] %s", json.dumps(payload, default=str, ensure_ascii=False)[:32000])
    except Exception:
        logger.info("[FOOTBALL][LIVE_SESSION_REPORT] (serialization failed)")


def _football_top_matches_for_telegram(rows: list[dict], *, limit: int) -> list[dict]:
    def sort_key(r: dict) -> tuple[int, float, str]:
        status = str(r.get("final_status") or "")
        if status == "selected":
            pri = 0
        elif status == "blocked_duplicate_idea":
            pri = 1
        elif status == "blocked_low_score":
            pri = 2
        else:
            pri = 3
        sc = float(r["best_candidate_score"] or -1.0)
        return (pri, -sc, r.get("match_name") or "")

    return sorted(rows, key=sort_key)[:limit]


class AutoSignalService:
    def _clean_optional_str(self, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = str(value).strip()
        return cleaned or None

    def _render_live_auth_status(self, auth_status: str | None, body_snippet: str | None) -> str:
        if auth_status == "ok":
            return "ok"
        if auth_status == "no_key":
            return "no_key"
        if auth_status == "out_of_usage_credits":
            return "unauthorized_quota"
        if auth_status == "unauthorized":
            return "unauthorized"
        if auth_status == "http_error":
            return "http_error"
        if auth_status == "request_error":
            return "request_error"
        return str(body_snippet or "").strip() or "unknown"

    def _provider_query_params(self, endpoint: str | None) -> dict[str, str]:
        if not endpoint:
            return {}
        return dict(parse_qsl(urlparse(endpoint).query, keep_blank_values=False))

    async def run_single_cycle(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        bot: Bot,
        *,
        dry_run: bool = False,
        adaptive_compare_only: bool = False,
    ) -> AutoSignalCycleResult:
        settings = get_settings()
        runtime = SignalRuntimeSettingsService()
        diagnostics = SignalRuntimeDiagnosticsService()
        active_sports = [sport.value for sport in runtime.active_sports()]
        logger.info("[FOOTBALL] cycle started")
        logger.info("[FOOTBALL] paused state: %s", str(runtime.is_paused()).upper())
        diagnostics.update(
            active_mode="football" if SportType.FOOTBALL.value in active_sports else "inactive",
            football_source=self._detect_provider_name(settings),
            football_fallback_source="manual_winline_json",
            live_provider_name=self._detect_provider_name(settings),
            live_auth_status=None,
            last_live_http_status=None,
            last_live_endpoint=None,
            last_live_error_body=None,
            fallback_source_available=False,
            manual_production_fallback_allowed=bool(settings.football_allow_manual_production_fallback),
            source_mode="unknown",
            is_real_source=False,
            source_origin=None,
            upload_provenance_present=False,
            uploaded_at=None,
            source_file_path=None,
            source_checksum=None,
            preview_only=bool(settings.auto_signal_preview_only),
            fallback_used=False,
            last_error=None,
            last_delivery_reason=None,
            note=None,
            football_candidates_count=0,
            football_real_candidates_count=0,
            football_after_filter_count=0,
            football_after_integrity_count=0,
            dropped_invalid_market_mapping_count=0,
            dropped_invalid_total_scope_count=0,
            dropped_too_far_in_time_count=0,
            live_matches_count=0,
            near_matches_count=0,
            too_far_matches_count=0,
            selected_match_reason=None,
            football_sent_count=0,
            football_analytics_enabled=bool(settings.football_analytics_enabled),
            football_learning_enabled=bool(settings.football_learning_enabled),
            football_learning_families_tracked=0,
            football_live_fields_in_last_cycle=False,
            football_injuries_data_available=False,
            football_line_movement_available=False,
            football_live_source_timestamp=None,
            football_live_source_age_seconds=None,
            football_live_stale_source=False,
            football_live_source_freshness=None,
            football_live_freshness_candidates_before=0,
            football_live_freshness_live_events_accepted=0,
            football_live_freshness_stale_events_dropped=0,
            football_live_freshness_stale_markets_dropped=0,
            football_live_quality_fresh_matches=0,
            football_live_quality_strong_idea_matches=0,
            football_live_quality_no_sendable_matches=0,
            football_live_quality_main_blocker="—",
            football_live_quality_main_blocker_ru="—",
            football_live_best_scores_distribution_hint="—",
            football_live_cycle_after_s8=0,
            football_live_cycle_after_s9=0,
            football_live_cycle_after_s10=0,
            football_live_cycle_after_s11=0,
            football_live_rejected_s8_home_00_without_api_context=0,
            football_live_rejected_s8_1x2_00_without_api_context=0,
            football_live_rejected_s8_1x2_00_no_pressure=0,
            football_live_passed_s8_1x2_00_with_api_pressure=0,
        )
        if runtime.is_paused():
            logger.info("[FOOTBALL][BLOCK] skipped due to paused")
            diagnostics.update(
                last_fetch_status="paused",
                last_delivery_reason="paused",
                note="delivery skipped: paused",
            )
            return AutoSignalCycleResult(
                endpoint=None,
                fetch_ok=False,
                preview_candidates=0,
                preview_skipped_items=0,
                created_signal_ids=[],
                created_signals_count=0,
                skipped_candidates_count=0,
                notifications_sent_count=0,
                preview_only=settings.auto_signal_preview_only,
                message="paused",
                runtime_paused=True,
                runtime_active_sports=active_sports,
                source_name=self._detect_provider_name(settings),
                rejection_reason="delivery skipped: paused",
            )
        if not runtime.is_sport_enabled(SportType.FOOTBALL):
            logger.info("[FOOTBALL] fetch skipped: football disabled in runtime")
            diagnostics.update(
                last_fetch_status="football_disabled",
                last_delivery_reason="football_disabled",
                note="filtered by runtime sport settings",
            )
            return AutoSignalCycleResult(
                endpoint=None,
                fetch_ok=False,
                preview_candidates=0,
                preview_skipped_items=0,
                created_signal_ids=[],
                created_signals_count=0,
                skipped_candidates_count=0,
                notifications_sent_count=0,
                preview_only=settings.auto_signal_preview_only,
                message="football_disabled",
                runtime_paused=False,
                runtime_active_sports=active_sports,
                source_name=self._detect_provider_name(settings),
                rejection_reason="filtered by runtime sport settings",
            )
        config = self._build_provider_client_config(settings)
        inferred_sport = self._infer_provider_sport(config) if config is not None else None
        if inferred_sport is not None and not runtime.is_sport_enabled(inferred_sport):
            logger.info("[FOOTBALL] fetch skipped: configured source sport disabled source=%s", inferred_sport.value)
            diagnostics.update(
                last_fetch_status=f"sport_disabled:{inferred_sport.value.lower()}",
                last_delivery_reason="sport_disabled",
                note="filtered by runtime sport settings",
            )
            return AutoSignalCycleResult(
                endpoint=getattr(config, "base_url", None),
                fetch_ok=False,
                preview_candidates=0,
                preview_skipped_items=0,
                created_signal_ids=[],
                created_signals_count=0,
                skipped_candidates_count=0,
                notifications_sent_count=0,
                preview_only=settings.auto_signal_preview_only,
                message=f"sport_disabled:{inferred_sport.value}",
                runtime_paused=False,
                runtime_active_sports=active_sports,
                source_name=self._detect_provider_name(settings),
                rejection_reason="filtered by runtime sport settings",
            )
        if config is None:
            logger.info("[FOOTBALL] fetch skipped: provider not configured")
            diagnostics.update(
                last_fetch_status="provider_not_configured",
                last_error="provider_not_configured",
                last_delivery_reason="provider_not_configured",
            )
            return AutoSignalCycleResult(
                endpoint=None,
                fetch_ok=False,
                preview_candidates=0,
                preview_skipped_items=0,
                created_signal_ids=[],
                created_signals_count=0,
                skipped_candidates_count=0,
                notifications_sent_count=0,
                preview_only=False,
                message="provider_not_configured",
                runtime_paused=False,
                runtime_active_sports=active_sports,
                source_name=self._detect_provider_name(settings),
                rejection_reason="provider_not_configured",
            )

        if not dry_run:
            ls_gate = FootballLiveSessionService()
            ls_gate.expire_if_needed()
            if not ls_gate.is_active():
                logger.info("[FOOTBALL][BLOCK] football live session inactive — skipping HTTP fetch")
                diagnostics.update(
                    last_fetch_status="football_live_session_inactive",
                    last_delivery_reason="football_live_session_inactive",
                    note="Нажмите ▶️ Старт для запуска football live-сессии",
                    football_live_cycle_bottleneck="blocked_no_live_session",
                    football_live_cycle_candidates_before_filter=0,
                    football_live_cycle_after_send_filter=0,
                    football_live_cycle_after_integrity=0,
                    football_live_cycle_after_s8=0,
                    football_live_cycle_after_s9=0,
                    football_live_cycle_after_s10=0,
                    football_live_cycle_after_s11=0,
                    football_live_rejected_s8_home_00_without_api_context=0,
                    football_live_rejected_s8_1x2_00_without_api_context=0,
                    football_live_rejected_s8_1x2_00_no_pressure=0,
                    football_live_passed_s8_1x2_00_with_api_pressure=0,
                    football_live_cycle_after_context_filter=0,
                    football_live_rejected_invalid_selection=0,
                    football_live_rejected_no_pressure=0,
                    football_live_passed_pressure=0,
                    football_live_context_filter_last_cycle_json=None,
                    football_live_cycle_after_score=0,
                    football_live_cycle_new_ideas_sendable=0,
                    football_live_cycle_duplicate_ideas_blocked=0,
                    football_live_cycle_live_matches_found=0,
                )
                return AutoSignalCycleResult(
                    endpoint=getattr(config, "base_url", None),
                    fetch_ok=False,
                    preview_candidates=0,
                    preview_skipped_items=0,
                    created_signal_ids=[],
                    created_signals_count=0,
                    skipped_candidates_count=0,
                    notifications_sent_count=0,
                    preview_only=False,
                    message="football_live_session_inactive",
                    runtime_paused=False,
                    runtime_active_sports=active_sports,
                    source_name=self._detect_provider_name(settings),
                    rejection_reason="football_live_session_inactive",
                )

        logger.info("[FOOTBALL] fetch started")
        logger.info(
            "[FOOTBALL] fetch order winline_primary=%s odds_fb=%s",
            str(settings.football_live_winline_primary).lower(),
            str(bool(settings.football_live_odds_api_fallback)).lower(),
        )
        diagnostics.update(
            football_live_cycle_live_matches_found=0,
            football_live_cycle_candidates_before_filter=0,
            football_live_cycle_after_send_filter=0,
            football_live_cycle_after_integrity=0,
            football_live_cycle_after_s8=0,
            football_live_cycle_after_s9=0,
            football_live_cycle_after_s10=0,
            football_live_cycle_after_s11=0,
            football_live_rejected_s8_home_00_without_api_context=0,
            football_live_rejected_s8_1x2_00_without_api_context=0,
            football_live_rejected_s8_1x2_00_no_pressure=0,
            football_live_passed_s8_1x2_00_with_api_pressure=0,
            football_live_cycle_after_context_filter=0,
            football_live_rejected_invalid_selection=0,
            football_live_rejected_no_pressure=0,
            football_live_passed_pressure=0,
            football_live_context_filter_last_cycle_json=None,
            football_live_cycle_after_score=0,
            football_live_cycle_new_ideas_sendable=0,
            football_live_cycle_duplicate_ideas_blocked=0,
            football_live_cycle_bottleneck=None,
            football_live_last_notify_path=None,
            football_live_effective_source=None,
            football_winline_ws_active_last_cycle=False,
            football_winline_football_event_count=0,
            football_winline_line_count_raw=0,
            football_winline_football_candidate_count=0,
            football_winline_error_last=None,
            football_primary_live_source=None,
            football_live_winline_attempted_last_cycle=False,
            football_live_winline_fetch_seconds_last=None,
            football_live_http_fetch_seconds_last=None,
        )
        preview = None
        payload = None
        source_name = self._detect_provider_name(settings)
        fallback_used = False
        fallback_source_name = None
        source_kind = "live"
        live_payload_fetched_at_utc: datetime | None = None
        line_manual_uploaded_at: str | None = None
        manual_freshness_result = None
        fetch_res: Any = None
        live_auth_status: str = "ok"
        winline_werr: str | None = None
        if settings.football_live_winline_primary and (settings.winline_live_ws_url or "").strip():
            diagnostics.update(football_live_winline_attempted_last_cycle=True)
            t_winline = time.perf_counter()
            wr, winline_werr = await WinlineLiveFeedService().fetch_football_live_raw_payload(settings)
            diagnostics.update(
                football_live_winline_fetch_seconds_last=float(time.perf_counter() - t_winline)
            )
            if wr and not winline_werr:
                try:
                    norm = WinlineRawLineBridgeService().normalize_raw_winline_line_payload(wr)
                    pvw = AdapterIngestionService().preview_payload(norm)
                except Exception as wbe:
                    norm, pvw, winline_werr = None, None, f"winline_bridge:{wbe!s}"
            else:
                norm, pvw = None, None
            if norm is not None and pvw is not None and wr is not None:
                preview = pvw
                payload = norm
                source_name = "winline_live"
                source_kind = "live"
                live_payload_fetched_at_utc = datetime.now(tz=timezone.utc)
                live_auth_status = self._render_live_auth_status("ok", None)
                fetch_res = SimpleNamespace(
                    ok=True,
                    payload=norm,
                    endpoint=str(settings.winline_live_ws_url),
                    error=None,
                    status_code=101,
                    source_name="winline_live",
                    key_present=False,
                    key_length=0,
                    key_masked="—",
                    response_body_snippet=None,
                    auth_status="ok",
                )
                diagnostics.update(
                    last_fetch_status="ok",
                    source_mode="live",
                    is_real_source=True,
                    source_origin="winline_websocket",
                    upload_provenance_present=True,
                    uploaded_at=live_payload_fetched_at_utc.isoformat(),
                    source_file_path=None,
                    source_checksum=None,
                    live_provider_name="winline_live",
                    live_auth_status=live_auth_status,
                    last_live_http_status=101,
                    last_live_endpoint=str(settings.winline_live_ws_url),
                    last_live_error_body=None,
                    football_winline_ws_active_last_cycle=True,
                    football_winline_football_event_count=len(norm.get("events") or []),
                    football_winline_line_count_raw=len(wr.get("lines") or []),
                    football_winline_football_candidate_count=len(
                        [
                            c
                            for c in pvw.candidates
                            if getattr(getattr(c, "match", None), "sport", None) == SportType.FOOTBALL
                        ]
                    ),
                    football_primary_live_source="winline_live",
                    football_live_effective_source="winline_live",
                )
                logger.info(
                    "[FOOTBALL][WINLINE_LIVE] source=winline_live events=%s lines_raw=%s markets_norm=%s football_cand=%s",
                    len(norm.get("events") or []),
                    len(wr.get("lines") or []),
                    len(norm.get("markets") or []),
                    len(
                        [
                            c
                            for c in pvw.candidates
                            if getattr(getattr(c, "match", None), "sport", None) == SportType.FOOTBALL
                        ]
                    ),
                )
            elif winline_werr and winline_werr != "winline_disabled" and not settings.football_live_odds_api_fallback:
                diagnostics.update(
                    last_fetch_status=str(winline_werr or "winline_failed"),
                    last_error=winline_werr,
                    last_delivery_reason="blocked_winline_live_unavailable",
                    football_winline_error_last=winline_werr,
                    # Ensure source diagnostics are truthful: Winline was the primary attempted source.
                    football_source="winline_live",
                    football_primary_live_source="winline_live",
                    football_live_effective_source="winline_live",
                    source_mode="blocked",
                    is_real_source=False,
                    source_origin="winline_websocket",
                    live_provider_name="winline_live",
                )
                return AutoSignalCycleResult(
                    endpoint=str(settings.winline_live_ws_url or ""),
                    fetch_ok=False,
                    preview_candidates=0,
                    preview_skipped_items=0,
                    created_signal_ids=[],
                    created_signals_count=0,
                    skipped_candidates_count=0,
                    notifications_sent_count=0,
                    preview_only=False,
                    message="blocked_winline_live_unavailable",
                    runtime_paused=False,
                    runtime_active_sports=active_sports,
                    source_name="winline_live",
                    live_auth_status="winline_unavailable",
                    last_live_http_status=101,
                    last_live_error_body=winline_werr,
                    rejection_reason=str(winline_werr or "winline_unavailable"),
                )
            elif winline_werr and winline_werr != "winline_disabled" and settings.football_live_odds_api_fallback:
                diagnostics.update(football_winline_error_last=winline_werr, last_error=winline_werr)
                logger.info(
                    "[FOOTBALL][WINLINE_LIVE] failed (fallback to odds) err=%s",
                    winline_werr,
                )

        if preview is None:
            t_http = time.perf_counter()
            fetch_res = await asyncio.to_thread(OddsHttpClient().fetch, config)
            diagnostics.update(
                football_live_http_fetch_seconds_last=float(time.perf_counter() - t_http)
            )
            live_auth_status = self._render_live_auth_status(
                fetch_res.auth_status, fetch_res.response_body_snippet
            )
            diagnostics.update(
                live_provider_name=self._detect_provider_name(settings),
                live_auth_status=live_auth_status,
                last_live_http_status=fetch_res.status_code,
                last_live_endpoint=fetch_res.endpoint,
                last_live_error_body=fetch_res.response_body_snippet,
            )
            if fetch_res and fetch_res.__class__.__name__ != "SimpleNamespace":
                diagnostics.update(
                    football_primary_live_source=self._detect_provider_name(settings),
                )
            logger.info(
                "[FOOTBALL][LIVE] provider=%s endpoint=%s key_present=%s key_length=%s key_masked=%s http_status=%s auth_status=%s params=%s",
                self._detect_provider_name(settings),
                fetch_res.endpoint,
                "yes" if fetch_res.key_present else "no",
                fetch_res.key_length,
                fetch_res.key_masked or "—",
                fetch_res.status_code,
                live_auth_status,
                self._provider_query_params(fetch_res.endpoint),
            )

        if preview is None and fetch_res is not None and fetch_res.ok and isinstance(fetch_res.payload, dict):
            payload = fetch_res.payload
            live_payload_fetched_at_utc = datetime.now(tz=timezone.utc)
            source_name = str(fetch_res.source_name or source_name)
            source_kind = "live"
            diagnostics.update(
                last_fetch_status="ok",
                source_mode="live",
                is_real_source=True,
                source_origin="live_provider",
                upload_provenance_present=False,
                uploaded_at=None,
                source_file_path=None,
                source_checksum=None,
                football_primary_live_source=source_name,
                football_live_effective_source=source_name,
            )
        elif preview is None and fetch_res is not None:
            err = str(fetch_res.error or "fetch_error")
            diagnostics.update(
                last_fetch_status=err,
                last_error=err,
            )
            logger.info("[FOOTBALL] fetch source=%s failed: %s", source_name, err)
            if "Unauthorized" in err or "provider_not_configured" in err or "fetch_error" in err:
                fallback = self._build_manual_football_fallback_preview()
                fallback_available = fallback is not None
                diagnostics.update(fallback_source_available=fallback_available)
                if fallback_available and settings.football_allow_manual_production_fallback:
                    manual_source_mode = str(fallback.get("source_mode") or "manual_example")
                    manual_is_real = bool(fallback.get("is_real_source", False))
                    if not manual_is_real:
                        diagnostics.update(
                            source_mode=manual_source_mode,
                            is_real_source=False,
                            source_origin=str(
                                fallback.get("source_origin") or fallback.get("source_reason") or "manual"
                            ),
                            upload_provenance_present=bool(fallback.get("provenance_present")),
                            uploaded_at=fallback.get("uploaded_at"),
                            source_file_path=fallback.get("file_path"),
                            source_checksum=fallback.get("checksum"),
                            last_delivery_reason=f"non_real_source_blocked: {manual_source_mode}",
                            note=str(fallback.get("source_reason") or "manual source is not real"),
                        )
                        return AutoSignalCycleResult(
                            endpoint=fetch_res.endpoint,
                            fetch_ok=False,
                            preview_candidates=0,
                            preview_skipped_items=0,
                            created_signal_ids=[],
                            created_signals_count=0,
                            skipped_candidates_count=0,
                            notifications_sent_count=0,
                            preview_only=False,
                            message=err,
                            runtime_paused=False,
                            runtime_active_sports=active_sports,
                            source_name=source_name,
                            live_auth_status=live_auth_status,
                            last_live_http_status=fetch_res.status_code,
                            rejection_reason=f"non_real_source_blocked: {manual_source_mode}",
                        )
                    if str(manual_source_mode).lower() == "semi_live_manual":
                        mf = evaluate_manual_live_source_freshness(
                            uploaded_at=fallback.get("uploaded_at"),
                            file_path=str(fallback.get("file_path") or "") or None,
                            settings=settings,
                        )
                        if mf.stale:
                            diagnostics.update(
                                last_fetch_status="blocked_stale_manual_live_source",
                                last_delivery_reason="blocked_stale_manual_live_source",
                                note=f"stale_manual_live_source:{mf.reason}",
                                football_live_stale_source=True,
                                football_live_source_age_seconds=mf.age_seconds,
                                football_live_source_freshness="stale",
                            )
                            return AutoSignalCycleResult(
                                endpoint=fetch_res.endpoint,
                                fetch_ok=True,
                                preview_candidates=0,
                                preview_skipped_items=0,
                                created_signal_ids=[],
                                created_signals_count=0,
                                skipped_candidates_count=0,
                                notifications_sent_count=0,
                                preview_only=False,
                                message="blocked_stale_manual_live_source",
                                runtime_paused=False,
                                runtime_active_sports=active_sports,
                                source_name=source_name,
                                live_auth_status=live_auth_status,
                                last_live_http_status=fetch_res.status_code,
                                fallback_used=True,
                                fallback_source_name="manual_winline_json",
                                rejection_reason="blocked_stale_manual_live_source",
                            )
                        manual_freshness_result = mf
                    line_manual_uploaded_at = fallback.get("uploaded_at")
                    preview = fallback["preview"]
                    payload = fallback["payload"]
                    fallback_used = True
                    fallback_source_name = "manual_winline_json"
                    source_kind = manual_source_mode
                    diagnostics.update(
                        last_fetch_status="manual_production_fallback",
                        fallback_used=True,
                        source_mode=manual_source_mode,
                        is_real_source=manual_is_real,
                        source_origin=str(
                            fallback.get("source_origin") or fallback.get("source_reason") or "manual"
                        ),
                        upload_provenance_present=bool(fallback.get("provenance_present")),
                        uploaded_at=fallback.get("uploaded_at"),
                        source_file_path=fallback.get("file_path"),
                        source_checksum=fallback.get("checksum"),
                        last_delivery_reason=None,
                        note=str(
                            fallback.get("source_reason")
                            or "temporary production fallback enabled: Winline JSON"
                        ),
                        football_primary_live_source="manual_winline_json",
                        football_live_effective_source="manual_winline_json",
                    )
                    logger.info(
                        "[FOOTBALL] live source unavailable; temporary production fallback source=%s",
                        fallback_source_name,
                    )
                elif fallback_available:
                    diagnostics.update(
                        source_mode="blocked",
                        is_real_source=False,
                        source_origin="live_provider_unavailable",
                        last_delivery_reason=f"live_unavailable_manual_fallback_disabled: {live_auth_status}",
                        note="manual production fallback disabled",
                    )
                    return AutoSignalCycleResult(
                        endpoint=fetch_res.endpoint,
                        fetch_ok=False,
                        preview_candidates=0,
                        preview_skipped_items=0,
                        created_signal_ids=[],
                        created_signals_count=0,
                        skipped_candidates_count=0,
                        notifications_sent_count=0,
                        preview_only=False,
                        message=err,
                        runtime_paused=False,
                        runtime_active_sports=active_sports,
                        source_name=source_name,
                        live_auth_status=live_auth_status,
                        last_live_http_status=fetch_res.status_code,
                        rejection_reason=f"live_unavailable_manual_fallback_disabled: {live_auth_status}",
                    )
                if fallback is None:
                    diagnostics.update(
                        source_mode="blocked",
                        is_real_source=False,
                        source_origin="live_provider_unavailable",
                        last_delivery_reason=f"live_unavailable_no_manual_fallback: {live_auth_status}",
                    )
                    logger.info("[FOOTBALL] provider unauthorized and no football fallback payload available")
                    return AutoSignalCycleResult(
                        endpoint=fetch_res.endpoint,
                        fetch_ok=False,
                        preview_candidates=0,
                        preview_skipped_items=0,
                        created_signal_ids=[],
                        created_signals_count=0,
                        skipped_candidates_count=0,
                        notifications_sent_count=0,
                        preview_only=False,
                        message=err,
                        runtime_paused=False,
                        runtime_active_sports=active_sports,
                        source_name=source_name,
                        live_auth_status=live_auth_status,
                        last_live_http_status=fetch_res.status_code,
                        rejection_reason=f"live_unavailable_no_manual_fallback: {live_auth_status}",
                    )
                logger.info(
                    "[FOOTBALL] fetch source=%s unauthorized; fallback source=%s", source_name, fallback_source_name
                )
            else:
                diagnostics.update(
                    source_mode="blocked", fallback_source_available=False, source_origin="live_provider_error"
                )
                return AutoSignalCycleResult(
                    endpoint=fetch_res.endpoint,
                    fetch_ok=False,
                    preview_candidates=0,
                    preview_skipped_items=0,
                    created_signal_ids=[],
                    created_signals_count=0,
                    skipped_candidates_count=0,
                    notifications_sent_count=0,
                    preview_only=False,
                    message=err,
                    runtime_paused=False,
                    runtime_active_sports=active_sports,
                    source_name=source_name,
                    live_auth_status=live_auth_status,
                    last_live_http_status=fetch_res.status_code,
                    rejection_reason=err,
                )

        if payload is None or not isinstance(payload, dict):
            diagnostics.update(last_fetch_status="payload_is_not_dict", last_error="payload_is_not_dict")
            return AutoSignalCycleResult(
                endpoint=fetch_res.endpoint,
                fetch_ok=False,
                preview_candidates=0,
                preview_skipped_items=0,
                created_signal_ids=[],
                created_signals_count=0,
                skipped_candidates_count=0,
                notifications_sent_count=0,
                preview_only=False,
                message="payload_is_not_dict",
                runtime_paused=False,
                runtime_active_sports=active_sports,
                source_name=source_name,
                live_auth_status=live_auth_status,
                last_live_http_status=fetch_res.status_code,
                fallback_used=fallback_used,
                fallback_source_name=fallback_source_name,
                rejection_reason="payload_is_not_dict",
            )

        if preview is None:
            adapter_service = AdapterIngestionService()
            preview = adapter_service.preview_odds_style_payload(payload)
        logger.info("[FOOTBALL] source: %s", source_kind)
        if not dry_run:
            FootballLiveSessionService().touch_cycle()
        raw_events_count = int(preview.total_events)
        normalized_markets_count = int(preview.total_markets)
        preview_skipped_items = int(preview.skipped_items)

        live_only_pool = self._filter_football_live_only(list(preview.candidates))
        pre_fresh_len = len(live_only_pool)

        delay_stale = False
        delay_age: float | None = None
        if source_kind == "live" and live_payload_fetched_at_utc is not None:
            delay_stale, delay_age = http_fetch_processing_delay_is_stale(
                live_payload_fetched_at_utc,
                settings=settings,
            )
        if delay_stale:
            diagnostics.update(
                last_delivery_reason="blocked_stale_live_source",
                last_fetch_status="blocked_stale_live_source",
                note="blocked_stale_live_source:http_processing_delay",
                football_live_stale_source=True,
                football_live_source_age_seconds=delay_age,
                football_live_source_freshness="stale",
            )
            logger.info(
                "[FOOTBALL][BLOCK] blocked_stale_live_source processing_delay_seconds=%s max_minutes=%s",
                delay_age,
                settings.football_live_runtime_snapshot_max_age_minutes,
            )
            return AutoSignalCycleResult(
                endpoint=fetch_res.endpoint,
                fetch_ok=True,
                preview_candidates=0,
                preview_skipped_items=preview_skipped_items,
                created_signal_ids=[],
                created_signals_count=0,
                skipped_candidates_count=0,
                notifications_sent_count=0,
                preview_only=False,
                message="blocked_stale_live_source",
                runtime_paused=False,
                runtime_active_sports=active_sports,
                source_name=source_name,
                live_auth_status=live_auth_status,
                last_live_http_status=fetch_res.status_code,
                fallback_used=fallback_used,
                fallback_source_name=fallback_source_name,
                rejection_reason="blocked_stale_live_source",
            )

        source_age_seconds: float | None = None
        source_ts_iso: str | None = None
        if source_kind == "live" and live_payload_fetched_at_utc is not None:
            source_ts_iso = live_payload_fetched_at_utc.isoformat()
            source_age_seconds = (datetime.now(timezone.utc) - live_payload_fetched_at_utc).total_seconds()
        elif manual_freshness_result is not None:
            source_age_seconds = manual_freshness_result.age_seconds
            source_ts_iso = str(line_manual_uploaded_at) if line_manual_uploaded_at else None
        elif fallback_used:
            source_ts_iso = str(line_manual_uploaded_at) if line_manual_uploaded_at else None

        (
            candidates_before_filter,
            freshness_rows,
            fresh_ev_ct,
            stale_ev_ct,
            dropped_stale_markets,
        ) = filter_stale_live_football_candidates(
            live_only_pool,
            source_mode=source_kind,
            source_age_seconds=source_age_seconds,
            source_timestamp_iso=source_ts_iso,
            settings=settings,
        )
        log_live_freshness_block(freshness_rows)
        preview_candidates = len(candidates_before_filter)

        if source_kind == "live":
            src_fresh_lbl = "fresh"
        elif manual_freshness_result is not None:
            src_fresh_lbl = "fresh"
        else:
            src_fresh_lbl = "unknown"

        diagnostics.update(
            football_live_source_timestamp=source_ts_iso,
            football_live_source_age_seconds=source_age_seconds,
            football_live_stale_source=False,
            football_live_source_freshness=src_fresh_lbl,
            football_live_freshness_candidates_before=pre_fresh_len,
            football_live_freshness_live_events_accepted=fresh_ev_ct,
            football_live_freshness_stale_events_dropped=stale_ev_ct,
            football_live_freshness_stale_markets_dropped=dropped_stale_markets,
        )

        if pre_fresh_len > 0 and len(candidates_before_filter) == 0:
            diagnostics.update(
                last_delivery_reason="blocked_stale_live_events",
                football_live_cycle_bottleneck="blocked_stale_live_events",
            )
            _dbg_stale = compile_football_cycle_debug(
                fb_preview=_football_only(live_only_pool),
                fb_cvf=[],
                fb_post_send=[],
                fb_post_integrity=[],
                enriched_scored=None,
                finalists=None,
                min_score=float(settings.football_min_signal_score or 60.0),
                family_svc=FootballSignalSendFilterService(),
                send_filter_stats=None,
                integrity_dropped_checks=[],
                dry_run=dry_run,
                global_block="blocked_stale_live_events",
            )
            if isinstance(_dbg_stale, dict):
                _dbg_stale["football_live_freshness_rows"] = [r.__dict__ for r in freshness_rows]
            return AutoSignalCycleResult(
                endpoint=fetch_res.endpoint,
                fetch_ok=True,
                preview_candidates=0,
                preview_skipped_items=preview_skipped_items,
                created_signal_ids=[],
                created_signals_count=0,
                skipped_candidates_count=0,
                notifications_sent_count=0,
                preview_only=False,
                message="blocked_stale_live_events",
                raw_events_count=raw_events_count,
                normalized_markets_count=normalized_markets_count,
                candidates_before_filter_count=0,
                candidates_after_filter_count=0,
                runtime_paused=False,
                runtime_active_sports=active_sports,
                source_name=source_name,
                live_auth_status=live_auth_status,
                last_live_http_status=fetch_res.status_code,
                fallback_used=fallback_used,
                fallback_source_name=fallback_source_name,
                rejection_reason="blocked_stale_live_events",
                football_cycle_debug=_dbg_stale,
            )

        logger.info("[FOOTBALL] raw events fetched: %s", raw_events_count)
        if not candidates_before_filter:
            logger.info("[FOOTBALL] candidates before filter: 0 (no live football matches)")
        logger.info("[FOOTBALL] candidates total: %s", len(candidates_before_filter))
        self._log_candidates_per_match(candidates_before_filter)
        runtime_candidates = self._filter_candidates_by_runtime(candidates_before_filter, runtime)
        filtered = CandidateFilterService().filter_candidates(
            runtime_candidates,
            CandidateFilterConfig.default_for_russian_manual_betting(),
        )
        deduped = DeduplicationService().deduplicate_candidates(filtered.accepted_candidates)
        filtered_candidates = list(deduped.unique_candidates)
        fb_preview = _football_only(candidates_before_filter)
        fb_cvf = _football_only(filtered_candidates)

        logger.info("[FOOTBALL] raw events: %s", raw_events_count)
        logger.info("[FOOTBALL] normalized markets: %s", normalized_markets_count)
        logger.info("[FOOTBALL] candidates before filter: %s", len(candidates_before_filter))
        logger.info("[FOOTBALL] candidates after filter: %s", len(filtered_candidates))
        diagnostics.update(
            raw_events_count=raw_events_count,
            normalized_markets_count=normalized_markets_count,
            candidates_before_filter_count=len(candidates_before_filter),
            candidates_after_filter_count=len(filtered_candidates),
            football_candidates_count=len(candidates_before_filter),
            football_real_candidates_count=len(candidates_before_filter) if source_kind == "live" else 0,
            football_source=source_name,
            football_fallback_source=fallback_source_name,
            fallback_used=fallback_used,
            source_mode=source_kind,
            is_real_source=(source_kind == "live" or source_kind == "semi_live_manual"),
        )
        if not filtered_candidates:
            reject_reason = self._resolve_zero_candidate_reason(
                preview_candidates=preview_candidates,
                runtime_candidates_count=len(runtime_candidates),
                filtered_accepted_count=filtered.accepted_count,
                deduped_count=len(deduped.unique_candidates),
                filter_rejections=filtered.rejection_reasons,
            )
            logger.info("[FOOTBALL] candidates after filter: 0 (%s)", reject_reason)

        if source_kind not in {"live", "semi_live_manual"}:
            logger.info("[FOOTBALL][BLOCK] auto-send disabled for non-live source=%s", source_kind)
            block_reason = "non_live_source_blocked"
            if live_auth_status and live_auth_status != "ok":
                block_reason = f"non_live_source_blocked: {live_auth_status}"
            diagnostics.update(
                final_signals_count=0,
                messages_sent_count=0,
                football_after_filter_count=0,
                football_sent_count=0,
                last_delivery_reason=block_reason,
                note=f"auto-send blocked for non-live source: {source_kind}",
            )
            _dbg = compile_football_cycle_debug(
                fb_preview=fb_preview,
                fb_cvf=fb_cvf,
                fb_post_send=[],
                fb_post_integrity=[],
                enriched_scored=None,
                finalists=None,
                min_score=float(settings.football_min_signal_score or 60.0),
                family_svc=FootballSignalSendFilterService(),
                send_filter_stats=None,
                integrity_dropped_checks=[],
                dry_run=dry_run,
                global_block=block_reason,
            )
            return AutoSignalCycleResult(
                endpoint=fetch_res.endpoint,
                fetch_ok=True,
                preview_candidates=preview_candidates,
                preview_skipped_items=preview_skipped_items,
                created_signal_ids=[],
                created_signals_count=0,
                skipped_candidates_count=0,
                notifications_sent_count=0,
                preview_only=settings.auto_signal_preview_only,
                message="non_live_source_blocked",
                raw_events_count=raw_events_count,
                normalized_markets_count=normalized_markets_count,
                candidates_before_filter_count=len(candidates_before_filter),
                candidates_after_filter_count=len(filtered_candidates),
                runtime_paused=False,
                runtime_active_sports=active_sports,
                source_name=source_name,
                live_auth_status=live_auth_status,
                last_live_http_status=fetch_res.status_code,
                fallback_used=fallback_used,
                fallback_source_name=fallback_source_name,
                rejection_reason=block_reason,
                football_cycle_debug=_dbg,
            )

        if settings.auto_signal_preview_only:
            logger.info("[FOOTBALL] final signals: 0 (preview_only enabled)")
            diagnostics.update(
                final_signals_count=0,
                messages_sent_count=0,
                last_delivery_reason="preview_only",
                note="preview_only enabled",
            )
            _dbg_po = compile_football_cycle_debug(
                fb_preview=fb_preview,
                fb_cvf=fb_cvf,
                fb_post_send=[],
                fb_post_integrity=[],
                enriched_scored=None,
                finalists=None,
                min_score=float(settings.football_min_signal_score or 60.0),
                family_svc=FootballSignalSendFilterService(),
                send_filter_stats=None,
                integrity_dropped_checks=[],
                dry_run=dry_run,
                global_block="preview_only_enabled",
            )
            return AutoSignalCycleResult(
                endpoint=fetch_res.endpoint,
                fetch_ok=True,
                preview_candidates=preview_candidates,
                preview_skipped_items=preview_skipped_items,
                created_signal_ids=[],
                created_signals_count=0,
                skipped_candidates_count=0,
                notifications_sent_count=0,
                preview_only=True,
                message="preview_only",
                raw_events_count=raw_events_count,
                normalized_markets_count=normalized_markets_count,
                candidates_before_filter_count=len(candidates_before_filter),
                candidates_after_filter_count=len(filtered_candidates),
                runtime_paused=False,
                runtime_active_sports=active_sports,
                source_name=source_name,
                live_auth_status=live_auth_status,
                last_live_http_status=fetch_res.status_code,
                fallback_used=fallback_used,
                fallback_source_name=fallback_source_name,
                rejection_reason="preview_only enabled",
                football_cycle_debug=_dbg_po,
            )

        delivery_scope = "live_auto" if source_kind == "live" else "football_manual_auto"
        runtime_source_kind = "live" if source_kind == "live" else "semi_live_manual"
        candidates_to_ingest = [
            c.model_copy(
                update={
                    "notes": delivery_scope,
                    "feature_snapshot_json": {
                        **(c.feature_snapshot_json or {}),
                        "runtime_source_kind": runtime_source_kind,
                        "runtime_primary_source": source_name if source_kind == "live" else "manual_winline_json",
                        "delivery_scope": delivery_scope,
                    },
                }
            )
            for c in filtered_candidates
        ]

        # --- Sportmonks integration (disabled) ---
        # Sportmonks is kept as an optional integration but is NOT in the critical path for live signals.
        diagnostics.update(
            football_live_sportmonks_baseline_enriched_last_cycle=0,
            football_live_sportmonks_baseline_missing_last_cycle=0,
            football_live_sportmonks_fixture_mapped_last_cycle=0,
            football_live_sportmonks_fixture_not_mapped_last_cycle=0,
        )

        logger.info("[FOOTBALL] final before send filter: %s", len(candidates_to_ingest))
        send_filter_result = None
        if settings.football_debug_disable_filter:
            logger.info("[FOOTBALL][DEBUG] filter disabled, sending raw candidates")
            candidates_to_ingest = candidates_to_ingest[:3]
            diagnostics.update(football_after_filter_count=len(candidates_to_ingest))
        elif candidates_to_ingest:
            football_send_filter = FootballSignalSendFilterService()
            max_pm = max(1, int(settings.football_live_max_signals_per_match or 12))
            send_filter_result = football_send_filter.filter_auto_send_candidates(
                candidates_to_ingest,
                live_only=True,
                max_signals_per_match=max_pm,
            )
            logger.info("[FOOTBALL] after family whitelist: %s", send_filter_result.stats.after_whitelist)
            logger.info("[FOOTBALL] after ranking: %s", send_filter_result.stats.after_ranking)
            logger.info("[FOOTBALL] after family dedup: %s", send_filter_result.stats.after_family_dedup)
            logger.info("[FOOTBALL] after per-match cap: %s", send_filter_result.stats.after_per_match_cap)
            candidates_to_ingest = send_filter_result.candidates
            diagnostics.update(
                football_after_filter_count=len(candidates_to_ingest),
                live_matches_count=send_filter_result.stats.live_matches,
                near_matches_count=send_filter_result.stats.near_matches,
                too_far_matches_count=send_filter_result.stats.too_far_matches_dropped,
                dropped_too_far_in_time_count=send_filter_result.stats.drop_reasons.get("too_far_in_time", 0),
                selected_match_reason=(send_filter_result.stats.selected_per_match[0] if send_filter_result.stats.selected_per_match else None),
            )
        fb_post_send_saved = _football_only(candidates_to_ingest)
        post_send_filter_count = len(candidates_to_ingest)
        integrity_result = FootballSignalIntegrityService().validate_candidates(candidates_to_ingest)
        candidates_to_ingest = integrity_result.valid_candidates
        fb_post_integrity_saved = _football_only(candidates_to_ingest)
        invalid_market_drops = len(
            [
                check
                for check in integrity_result.dropped_checks
                if check.integrity_check_reason not in {"invalid_total_scope", "invalid_total_line"}
            ]
        )
        invalid_total_scope_drops = len(
            [
                check
                for check in integrity_result.dropped_checks
                if check.integrity_check_reason in {"invalid_total_scope", "invalid_total_line"}
            ]
        )
        diagnostics.update(
            football_after_filter_count=post_send_filter_count,
            football_after_integrity_count=len(candidates_to_ingest),
            dropped_invalid_market_mapping_count=invalid_market_drops,
            dropped_invalid_total_scope_count=invalid_total_scope_drops,
            football_live_cycle_candidates_before_filter=preview_candidates,
            football_live_cycle_after_send_filter=post_send_filter_count,
            football_live_cycle_after_integrity=len(candidates_to_ingest),
            football_live_cycle_live_matches_found=(
                int(send_filter_result.stats.live_matches) if send_filter_result is not None else 0
            ),
            football_live_effective_source=f"{source_name}:{source_kind}",
        )
        if invalid_market_drops:
            logger.info("[FOOTBALL][INTEGRITY] dropped_invalid_market_mapping=%s", invalid_market_drops)
        if invalid_total_scope_drops:
            logger.info("[FOOTBALL][INTEGRITY] dropped_invalid_total_scope=%s", invalid_total_scope_drops)
        post_integrity_count = len(candidates_to_ingest)
        api_intelligence_examples: list[dict[str, object]] = []
        try:
            intelligence = ApiFootballTeamIntelligenceService()
            intelligence_res = intelligence.enrich_candidates(list(candidates_to_ingest), max_matches=5)
            candidates_to_ingest = intelligence_res.candidates
            fb_post_integrity_saved = _football_only(candidates_to_ingest)
            api_intelligence_examples = intelligence_res.examples
            diagnostics.update(
                api_football_intelligence_attempted=int(intelligence_res.attempted),
                api_football_intelligence_mapped=int(intelligence_res.mapped),
                api_football_intelligence_loaded=int(intelligence_res.loaded),
                api_football_intelligence_missing=int(intelligence_res.missing),
                api_football_intelligence_cache_hits=int(intelligence_res.cache_hits),
                api_football_intelligence_requests_used=int(intelligence_res.requests_used),
                api_football_intelligence_examples_json=(
                    json.dumps(api_intelligence_examples, default=str, ensure_ascii=False)[:20000]
                    if api_intelligence_examples
                    else None
                ),
            )
            if intelligence_res.attempted:
                logger.info(
                    "[API_FOOTBALL][INTELLIGENCE] attempted=%s mapped=%s loaded=%s missing=%s requests=%s cache_hits=%s",
                    intelligence_res.attempted,
                    intelligence_res.mapped,
                    intelligence_res.loaded,
                    intelligence_res.missing,
                    intelligence_res.requests_used,
                    intelligence_res.cache_hits,
                )
        except Exception as exc:  # noqa: BLE001
            logger.info("[API_FOOTBALL][INTELLIGENCE] enrichment skipped: %s", exc)
            diagnostics.update(
                api_football_intelligence_attempted=0,
                api_football_intelligence_mapped=0,
                api_football_intelligence_loaded=0,
                api_football_intelligence_missing=int(len(candidates_to_ingest)),
                api_football_intelligence_cache_hits=0,
                api_football_intelligence_requests_used=0,
                api_football_intelligence_examples_json=None,
            )
        try:
            ranker_res = FootballLiveAnalyticRankerService().rank(list(candidates_to_ingest), limit=10)
            diagnostics.update(
                football_live_ranker_candidates=int(ranker_res.opportunities),
                football_live_ranker_top_count=int(len(ranker_res.top)),
                football_live_ranker_api_count=int(ranker_res.api_count),
                football_live_ranker_blocked_count=int(ranker_res.blocked_count),
                football_live_ranker_eligible_count=int(ranker_res.eligible_count),
                football_live_ranker_blocked_breakdown_json=json.dumps(
                    ranker_res.blocked_breakdown,
                    default=str,
                    ensure_ascii=False,
                )[:12000],
                football_live_ranker_top_json=json.dumps(ranker_res.top, default=str, ensure_ascii=False)[:30000],
            )
            logger.info(
                "[FOOTBALL][S12_RANKER] opportunities=%s top=%s eligible=%s api=%s blocked=%s",
                ranker_res.opportunities,
                len(ranker_res.top),
                ranker_res.eligible_count,
                ranker_res.api_count,
                ranker_res.blocked_count,
            )
        except Exception as exc:  # noqa: BLE001
            logger.info("[FOOTBALL][S12_RANKER] preview skipped: %s", exc)
            diagnostics.update(
                football_live_ranker_candidates=0,
                football_live_ranker_top_count=0,
                football_live_ranker_api_count=0,
                football_live_ranker_blocked_count=0,
                football_live_ranker_eligible_count=0,
                football_live_ranker_blocked_breakdown_json=None,
                football_live_ranker_top_json=None,
            )
        if not candidates_to_ingest:
            if adaptive_compare_only:
                from app.services.football_live_adaptive_compare_service import run_adaptive_compare_report

                _lm = int(send_filter_result.stats.live_matches) if send_filter_result else 0
                _cmp = await run_adaptive_compare_report(
                    sessionmaker,
                    [],
                    settings,
                    dry_run=True,
                    pipeline_meta={
                        "live_matches_total": _lm,
                        "matches_after_freshness": len(
                            {_football_event_id(c) for c in candidates_before_filter if _football_event_id(c)}
                        ),
                        "preview_candidates": preview_candidates,
                    },
                )
                return AutoSignalCycleResult(
                    endpoint=fetch_res.endpoint,
                    fetch_ok=True,
                    preview_candidates=preview_candidates,
                    preview_skipped_items=preview_skipped_items,
                    created_signal_ids=[],
                    created_signals_count=0,
                    skipped_candidates_count=max(0, len(filtered_candidates) - post_integrity_count),
                    notifications_sent_count=0,
                    preview_only=False,
                    message="adaptive_compare_only",
                    raw_events_count=raw_events_count,
                    normalized_markets_count=normalized_markets_count,
                    candidates_before_filter_count=len(candidates_before_filter),
                    candidates_after_filter_count=len(filtered_candidates),
                    runtime_paused=False,
                    runtime_active_sports=active_sports,
                    source_name=source_name,
                    live_auth_status=live_auth_status,
                    last_live_http_status=fetch_res.status_code,
                    fallback_used=fallback_used,
                    fallback_source_name=fallback_source_name,
                    rejection_reason="no_post_integrity_candidates",
                    dry_run=True,
                    football_adaptive_compare=_cmp,
                )
            too_far_drops = (
                0
                if send_filter_result is None
                else int(send_filter_result.stats.drop_reasons.get("too_far_in_time", 0))
            )
            diagnostics.update(
                final_signals_count=0,
                messages_sent_count=0,
                football_after_filter_count=post_send_filter_count,
                football_after_integrity_count=0,
                football_sent_count=0,
                last_delivery_reason=(
                    "dropped_invalid_total_scope"
                    if invalid_total_scope_drops
                    else (
                        "dropped_invalid_market_mapping"
                        if invalid_market_drops
                        else ("too_far_in_time" if too_far_drops else "football_send_filter_rejected_all")
                    )
                ),
                note=(
                    "selected football signal lost exact total scope"
                    if invalid_total_scope_drops
                    else (
                        "all selected football signals failed integrity check"
                        if invalid_market_drops
                        else (
                            "all football matches are outside allowed prematch window"
                            if too_far_drops
                            else "football send filter rejected all signals"
                        )
                    )
                ),
            )
            _dbg_empty = compile_football_cycle_debug(
                fb_preview=fb_preview,
                fb_cvf=fb_cvf,
                fb_post_send=fb_post_send_saved,
                fb_post_integrity=fb_post_integrity_saved,
                enriched_scored=None,
                finalists=None,
                min_score=float(settings.football_min_signal_score or 60.0),
                family_svc=FootballSignalSendFilterService(),
                send_filter_stats=send_filter_result.stats if send_filter_result else None,
                integrity_dropped_checks=list(integrity_result.dropped_checks),
                dry_run=dry_run,
            )
            return AutoSignalCycleResult(
                endpoint=fetch_res.endpoint,
                fetch_ok=True,
                preview_candidates=preview_candidates,
                preview_skipped_items=preview_skipped_items,
                created_signal_ids=[],
                created_signals_count=0,
                skipped_candidates_count=max(0, len(filtered_candidates) - post_integrity_count),
                notifications_sent_count=0,
                preview_only=False,
                message="ok",
                raw_events_count=raw_events_count,
                normalized_markets_count=normalized_markets_count,
                candidates_before_filter_count=len(candidates_before_filter),
                candidates_after_filter_count=len(filtered_candidates),
                runtime_paused=False,
                runtime_active_sports=active_sports,
                source_name=source_name,
                live_auth_status=live_auth_status,
                last_live_http_status=fetch_res.status_code,
                fallback_used=fallback_used,
                fallback_source_name=fallback_source_name,
                rejection_reason=(
                    "dropped_invalid_total_scope"
                    if invalid_total_scope_drops
                    else (
                        "dropped_invalid_market_mapping"
                        if invalid_market_drops
                        else ("too_far_in_time" if too_far_drops else "football send filter rejected all signals")
                    )
                ),
                football_cycle_debug=_dbg_empty,
            )
        omitted_by_limit = 0
        limit = settings.auto_signal_max_created_per_cycle
        if limit is not None and limit > 0:
            candidates_to_ingest = candidates_to_ingest[:limit]
            omitted_by_limit = max(0, post_integrity_count - len(candidates_to_ingest))

        logger.info("[FOOTBALL] final signals to send: %s", len(candidates_to_ingest))
        self._log_final_candidates(candidates_to_ingest)

        if adaptive_compare_only:
            from app.services.football_live_adaptive_compare_service import run_adaptive_compare_report

            _lm = int(send_filter_result.stats.live_matches) if send_filter_result else 0
            _cmp = await run_adaptive_compare_report(
                sessionmaker,
                list(candidates_to_ingest),
                settings,
                dry_run=True,
                pipeline_meta={
                    "live_matches_total": _lm,
                    "matches_after_freshness": len(
                        {_football_event_id(c) for c in candidates_before_filter if _football_event_id(c)}
                    ),
                    "preview_candidates": preview_candidates,
                },
            )
            return AutoSignalCycleResult(
                endpoint=fetch_res.endpoint,
                fetch_ok=True,
                preview_candidates=preview_candidates,
                preview_skipped_items=preview_skipped_items,
                created_signal_ids=[],
                created_signals_count=0,
                skipped_candidates_count=int(omitted_by_limit),
                notifications_sent_count=0,
                preview_only=False,
                message="adaptive_compare_only",
                raw_events_count=raw_events_count,
                normalized_markets_count=normalized_markets_count,
                candidates_before_filter_count=len(candidates_before_filter),
                candidates_after_filter_count=len(filtered_candidates),
                report_after_filter=post_send_filter_count,
                report_after_integrity=len(candidates_to_ingest),
                runtime_paused=False,
                runtime_active_sports=active_sports,
                source_name=source_name,
                live_auth_status=live_auth_status,
                last_live_http_status=fetch_res.status_code,
                fallback_used=fallback_used,
                fallback_source_name=fallback_source_name,
                rejection_reason=None,
                dry_run=True,
                football_adaptive_compare=_cmp,
            )

        analytics_enabled = bool(settings.football_analytics_enabled)
        learning_enabled = bool(settings.football_learning_enabled)
        live_adaptive_enabled = bool(getattr(settings, "football_live_adaptive_learning_enabled", True))
        learning_multipliers: dict[str, float] = {}
        learning_aggregates: list = []
        live_adaptive_snapshot = None
        enriched: list[ProviderSignalCandidate] | None = None
        if candidates_to_ingest and (learning_enabled or live_adaptive_enabled):
            async with sessionmaker() as learn_session:
                if learning_enabled:
                    learning_multipliers, learning_aggregates = await FootballLearningService().compute_family_multipliers(
                        learn_session
                    )
                if live_adaptive_enabled:
                    live_adaptive_snapshot = await build_live_adaptive_snapshot(learn_session)

        fb_pre = [
            x
            for x in candidates_before_filter
            if getattr(getattr(x, "match", None), "sport", None) == SportType.FOOTBALL
        ]
        report_matches_found = len({(x.match.external_event_id, x.match.home_team, x.match.away_team) for x in fb_pre})
        report_candidates = len(candidates_before_filter)
        report_after_filter = int(post_send_filter_count)
        report_after_integrity = int(post_integrity_count)

        min_score_base = float(settings.football_min_signal_score or 60.0)
        single_gap_max = float(getattr(settings, "football_live_single_relief_max_gap", 2.0) or 2.0)

        # --- Strategy-first (S8 must see real live context BEFORE scoring filters) ---
        # Build minimal analytics snapshot (minute/score/live_state) pre-strategy.
        analytics_svc = FootballAnalyticsService()
        family_svc = FootballSignalSendFilterService()
        # --- Side funnel diagnostics (1X2 side distribution through the funnel) ---
        def _norm_side(s: str) -> str:
            return (s or "").strip().lower().replace("ё", "е")

        def _side_from_selection_1x2(cand: ProviderSignalCandidate) -> str:
            """home|away|draw|unknown for 1X2-like markets; unknown for others."""
            try:
                mt = _norm_side(str(cand.market.market_type or ""))
                if mt not in {"1x2", "match_winner"}:
                    return "unknown"
                sel = _norm_side(str(cand.market.selection or ""))
                if not sel:
                    return "unknown"
                tok = sel.replace("х", "x").replace(" ", "")
                # Strict tokens only (no substring "нич": team names can contain it)
                if tok in {"x", "draw", "н", "ничья"}:
                    return "draw"
                if tok in {"1", "p1", "п1", "home"}:
                    return "home"
                if tok in {"2", "p2", "п2", "away"}:
                    return "away"
                home = _norm_side(str(cand.match.home_team or ""))
                away = _norm_side(str(cand.match.away_team or ""))
                if home and (sel == home or home in sel or sel in home):
                    return "home"
                if away and (sel == away or away in sel or sel in away):
                    return "away"
                return "unknown"
            except Exception:
                return "unknown"

        def _side_breakdown(pool: list[ProviderSignalCandidate]) -> dict[str, int]:
            out = {"home": 0, "away": 0, "draw": 0, "unknown": 0, "total": 0}
            for c in pool:
                out["total"] += 1
                s = _side_from_selection_1x2(c)
                if s not in ("home", "away", "draw", "unknown"):
                    s = "unknown"
                out[s] += 1
            return out
        # --- Market flow diagnostics (after integrity) ---
        flow_family: dict[str, int] = {}
        flow_market_type: dict[str, int] = {}
        flow_norm_bet: dict[str, int] = {}
        flow_result_subtype: dict[str, int] = {}
        flow_total_scope: dict[str, int] = {}
        flow_ou: dict[str, int] = {}
        flow_line: dict[str, int] = {}
        flow_minute_bucket: dict[str, int] = {}
        flow_score_bucket: dict[str, int] = {}
        flow_odds_bucket: dict[str, int] = {}
        from app.services.football_bet_formatter_service import FootballBetFormatterService

        def _bump(d: dict[str, int], k: str) -> None:
            if not k:
                k = "—"
            d[k] = int(d.get(k, 0) or 0) + 1

        def _bucket_minute(m: int | None) -> str:
            if m is None:
                return "minute:missing"
            if m < 0:
                return "minute:<0"
            if m <= 10:
                return "minute:0-10"
            if m <= 20:
                return "minute:11-20"
            if m <= 30:
                return "minute:21-30"
            if m <= 40:
                return "minute:31-40"
            if m <= 50:
                return "minute:41-50"
            if m <= 60:
                return "minute:51-60"
            if m <= 70:
                return "minute:61-70"
            if m <= 80:
                return "minute:71-80"
            return "minute:81+"

        def _bucket_odds(o: float | None) -> str:
            if o is None:
                return "odds:missing"
            if o < 1.50:
                return "odds:<1.50"
            if o <= 2.20:
                return "odds:1.50-2.20"
            if o <= 3.40:
                return "odds:2.21-3.40"
            if o <= 5.00:
                return "odds:3.41-5.00"
            return "odds:5.01+"

        def _bucket_score(sh: int | None, sa: int | None) -> str:
            if sh is None or sa is None:
                return "score:missing"
            if (sh, sa) == (0, 0):
                return "score:0-0"
            if (sh, sa) == (1, 0):
                return "score:1-0"
            if (sh, sa) == (0, 1):
                return "score:0-1"
            if (sh, sa) == (1, 1):
                return "score:1-1"
            return "score:other"

        def _safe_odds_float(v: object) -> float | None:
            if v is None or isinstance(v, bool):
                return None
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        def _is_exotic_result_like(cand: ProviderSignalCandidate) -> bool:
            txt = " ".join(
                [
                    str(cand.market.section_name or ""),
                    str(cand.market.subsection_name or ""),
                    str(cand.market.market_label or ""),
                    str(cand.market.market_type or ""),
                ]
            ).lower()
            bad = ["remainder", "remain", "остат", "оставш", "handicap", "european", "европ", "фора", "hcp"]
            return any(b in txt for b in bad)

        def _norm_bet_type(cand: ProviderSignalCandidate, family: str) -> str:
            mt = str(cand.market.market_type or "").strip().lower()
            sel = str(cand.market.selection or "").strip().lower()
            lbl = " ".join([str(cand.market.market_label or ""), str(cand.market.section_name or ""), str(cand.market.subsection_name or "")]).lower()
            if family == "result":
                if _is_exotic_result_like(cand):
                    return "result:exotic_or_remainder_or_handicap"
                if mt in {"1x2", "match_winner"}:
                    return "result:1x2"
                if "следующий" in lbl or "next goal" in lbl:
                    return "result:next_goal"
                return f"result:{mt or 'other'}"
            if family == "totals":
                ou = "over" if ("over" in sel or "больше" in sel or sel.startswith("тб")) else ("under" if ("under" in sel or "меньше" in sel or sel.startswith("тм")) else "unknown")
                return f"totals:{ou}:{mt or 'other'}"
            return f"{family}:{mt or 'other'}"

        def _result_subtype_local(cand: ProviderSignalCandidate) -> str:
            mt0 = str(cand.market.market_type or "").strip().lower()
            lbl = " ".join(
                [
                    str(cand.market.section_name or ""),
                    str(cand.market.subsection_name or ""),
                    str(cand.market.market_label or ""),
                    str(cand.market.selection or ""),
                ]
            ).lower()
            if "следующий гол" in lbl or "next goal" in lbl:
                return "next_goal"
            if "остат" in lbl or "remainder" in lbl or "win the rest" in lbl or "выиграет остаток" in lbl:
                return "remainder"
            if "европ" in lbl or "european" in lbl or "фора" in lbl or "handicap" in lbl or "hcp" in lbl:
                return "european_handicap_or_handicap"
            if "интервал" in lbl or "с минут" in lbl or "interval" in lbl:
                return "interval_result"
            if "1-й тайм" in lbl or "2-й тайм" in lbl or "тайм" in lbl or "half" in lbl:
                return "period_result"
            if _is_exotic_result_like(cand):
                return "exotic_result_like"
            if mt0 in {"1x2", "match_winner"}:
                return "ft_1x2_candidate"
            return "other_result_like"

        pre_strategy: list[ProviderSignalCandidate] = []
        for cand in candidates_to_ingest:
            try:
                family = family_svc.get_market_family(cand)
                analytics = analytics_svc.build_snapshot(cand, market_family=family)
                prev_fs = dict(cand.feature_snapshot_json or {})
                fs_out = dict(prev_fs)
                fs_out["football_analytics"] = analytics
                pre_strategy.append(cand.model_copy(update={"feature_snapshot_json": fs_out}))

                # Flow stats (after integrity)
                _bump(flow_family, str(family or "—"))
                _bump(flow_market_type, str(cand.market.market_type or "—"))
                _bump(flow_norm_bet, _norm_bet_type(cand, str(family or "")))
                if str(family or "") == "result":
                    _bump(flow_result_subtype, _result_subtype_local(cand))
                minute0 = None
                sh0 = None
                sa0 = None
                if isinstance(analytics, dict):
                    try:
                        minute0 = int(analytics.get("minute")) if analytics.get("minute") is not None else None
                    except Exception:
                        minute0 = None
                    try:
                        sh0 = int(analytics.get("score_home")) if analytics.get("score_home") is not None else None
                    except Exception:
                        sh0 = None
                    try:
                        sa0 = int(analytics.get("score_away")) if analytics.get("score_away") is not None else None
                    except Exception:
                        sa0 = None
                _bump(flow_minute_bucket, _bucket_minute(minute0))
                _bump(flow_score_bucket, _bucket_score(sh0, sa0))
                _bump(flow_odds_bucket, _bucket_odds(_safe_odds_float(getattr(cand.market, "odds_value", None))))

                # Totals-specific: over/under, line, scope
                if str(family or "") == "totals":
                    sel0 = str(cand.market.selection or "")
                    if "over" in sel0.lower() or "больше" in sel0.lower() or sel0.lower().startswith("тб"):
                        _bump(flow_ou, "over")
                    elif "under" in sel0.lower() or "меньше" in sel0.lower() or sel0.lower().startswith("тм"):
                        _bump(flow_ou, "under")
                    else:
                        _bump(flow_ou, "unknown")
                    try:
                        fmt0 = FootballBetFormatterService()
                        ctx0 = fmt0.describe_total_context(
                            market_type=cand.market.market_type,
                            market_label=cand.market.market_label,
                            selection=cand.market.selection,
                            home_team=cand.match.home_team,
                            away_team=cand.match.away_team,
                            section_name=cand.market.section_name,
                            subsection_name=cand.market.subsection_name,
                        )
                        if ctx0 and ctx0.total_line:
                            _bump(flow_line, str(ctx0.total_line))
                        else:
                            _bump(flow_line, "line:missing")
                        ts0 = str(ctx0.target_scope or "")
                        if ts0:
                            tsn = ts0.strip().lower()
                            if "match" in tsn or "общ" in tsn or "total" in tsn:
                                _bump(flow_total_scope, "scope:match")
                            else:
                                _bump(flow_total_scope, f"scope:{tsn[:24]}")
                        else:
                            _bump(flow_total_scope, "scope:missing")
                    except Exception:
                        _bump(flow_line, "line:error")
                        _bump(flow_total_scope, "scope:error")
            except Exception:
                pre_strategy.append(cand)
        candidates_to_ingest = pre_strategy

        # --- Strict 1X2 selection validation (no fallbacks like "да") ---
        # For result/1X2-like markets we ONLY accept selection tokens 1/2/X.
        def _strict_1x2_token(raw_sel: str) -> str | None:
            tok = (raw_sel or "").strip()
            if not tok:
                return None
            t = tok.upper().replace("Х", "X").replace(" ", "")
            if t in {"1", "2", "X"}:
                return t
            return None

        validated_ingest: list[ProviderSignalCandidate] = []
        rejected_invalid_selection = 0
        for cand in candidates_to_ingest:
            try:
                fam = family_svc.get_market_family(cand)
                mt = _norm_side(str(cand.market.market_type or ""))
                if fam == "result" and mt in {"1x2", "match_winner"}:
                    tok = _strict_1x2_token(str(cand.market.selection or ""))
                    if tok is None:
                        rejected_invalid_selection += 1
                        continue
                    try:
                        m2 = cand.market.model_copy(update={"selection": tok})
                        cand = cand.model_copy(update={"market": m2})
                    except Exception:
                        pass
                validated_ingest.append(cand)
            except Exception:
                validated_ingest.append(cand)

        if rejected_invalid_selection:
            prev_bad = int(diagnostics.get_state().get("football_live_rejected_invalid_selection") or 0)
            diagnostics.update(football_live_rejected_invalid_selection=prev_bad + int(rejected_invalid_selection))

        candidates_to_ingest = validated_ingest
        side_after_integrity = _side_breakdown(list(candidates_to_ingest))

        # --- Explicit strategy gate (primary signal definition) ---
        # Pipeline: after integrity -> S8 first -> S9 second -> scoring -> final gate.
        strategy_passed: list[ProviderSignalCandidate] = []
        strategy_stats: dict[str, int] = {}
        strategy_by_eid: dict[str, str] = {}
        s1_fail: dict[str, int] = {}
        s2_fail: dict[str, int] = {}
        s8_fail: dict[str, int] = {}
        s9_fail: dict[str, int] = {}
        s10_fail: dict[str, int] = {}
        s8_ft_fail: dict[str, int] = {}
        s8_evaluated_count = 0
        s8_result_1x2_candidate_count = 0
        s8_result_1x2_pass_count = 0
        strategy_rejected_samples: list[dict[str, object]] = []
        strategy_rejected_limit = 5
        ft_1x2_rejected_samples: list[dict[str, object]] = []
        ft_1x2_rejected_limit = 5
        strategy_gate_debug: dict[str, object] = {}
        post_integrity_result_like = 0
        post_integrity_result_like_1x2 = 0
        from app.services.football_bet_formatter_service import FootballBetFormatterService

        fmt = FootballBetFormatterService()
        s8_passed: list[ProviderSignalCandidate] = []
        s9_passed: list[ProviderSignalCandidate] = []
        s10_passed: list[ProviderSignalCandidate] = []
        s11_passed: list[ProviderSignalCandidate] = []
        s9_candidates: list[ProviderSignalCandidate] = []
        s10_candidates: list[ProviderSignalCandidate] = []
        s11_candidates: list[ProviderSignalCandidate] = []
        s10_team_total_count = 0
        from app.services.football_live_strategy_service import (
            evaluate_s8_live_1x2_winline_strict,
            evaluate_s9_live_totals_over_controlled,
            evaluate_s10_live_team_total_over_controlled,
            evaluate_s11_live_match_total_over_need_1_controlled,
        )
        away_draw_samples: list[dict[str, object]] = []
        away_draw_samples_limit = 10

        def _persist_side_funnel_once(*, stages: dict[str, dict[str, int]], samples: list[dict[str, object]]) -> None:
            """Persist last-cycle funnel + rolling window aggregate (caps at 20 cycles)."""
            try:
                import json

                now_iso = datetime.now(timezone.utc).isoformat()
                funnel = {"cycle_at": now_iso, "stages": stages}

                prev = SignalRuntimeDiagnosticsService().get_state()
                prev_cycles = int(prev.get("football_live_side_funnel_window_cycles") or 0)
                started_at = prev.get("football_live_side_funnel_window_started_at") or now_iso
                agg: dict[str, dict[str, int]] = {}
                try:
                    agg0 = json.loads(prev.get("football_live_side_funnel_window_agg_json") or "{}")
                    if isinstance(agg0, dict):
                        agg = agg0  # type: ignore[assignment]
                except Exception:
                    agg = {}

                # merge
                for stg, cnt in stages.items():
                    if not isinstance(cnt, dict):
                        continue
                    base = agg.get(stg)
                    if not isinstance(base, dict):
                        base = {"home": 0, "away": 0, "draw": 0, "unknown": 0, "total": 0}
                    for k in ("home", "away", "draw", "unknown", "total"):
                        base[k] = int(base.get(k, 0) or 0) + int(cnt.get(k, 0) or 0)
                    agg[stg] = base

                next_cycles = prev_cycles + 1
                if next_cycles > 20:
                    agg = {stg: {k: int(cnt.get(k, 0) or 0) for k in ("home", "away", "draw", "unknown", "total")} for stg, cnt in stages.items()}
                    next_cycles = 1
                    started_at = now_iso

                diagnostics.update(
                    football_live_side_funnel_last_cycle_json=json.dumps(funnel, ensure_ascii=False)[:12000],
                    football_live_side_funnel_window_agg_json=json.dumps(agg, ensure_ascii=False)[:12000],
                    football_live_side_funnel_window_cycles=int(next_cycles),
                    football_live_side_funnel_window_started_at=str(started_at),
                    football_live_side_away_draw_samples_last_cycle_json=json.dumps(samples, ensure_ascii=False)[:12000],
                )
            except Exception:
                pass

        for c in candidates_to_ingest:
            try:
                fam0 = family_svc.get_market_family(c)
                if fam0 == "result":
                    post_integrity_result_like += 1
                    mt0 = _norm(str(c.market.market_type or ""))
                    if mt0 in {"1x2", "match_winner"}:
                        post_integrity_result_like_1x2 += 1
            except Exception:
                pass
            # Always compute breakdown on the same candidate pool we gate on (post scoring/adaptive).
            d1 = evaluate_s1_live_1x2_controlled(c)
            if not d1.passed:
                for r in (d1.reasons or [])[:12]:
                    s1_fail[str(r)] = int(s1_fail.get(str(r), 0) or 0) + 1
            d2 = evaluate_s2_live_total_over_need_1_2(c)
            if not d2.passed:
                for r in (d2.reasons or [])[:12]:
                    s2_fail[str(r)] = int(s2_fail.get(str(r), 0) or 0) + 1

            # --- S8 first ---
            fam_for_s8 = ""
            mt_for_s8 = ""
            st_for_s8 = ""
            raw_sel_for_s8 = ""
            sel_tok_for_s8 = None
            fs_raw_minute = None
            fs_raw_sh = None
            fs_raw_sa = None
            fa_exists = False
            fa_minute = None
            fa_sh = None
            fa_sa = None
            try:
                fam_for_s8 = str(family_svc.get_market_family(c) or "")
                mt_for_s8 = _norm_side(str(c.market.market_type or ""))
                st_for_s8 = _result_subtype_local(c)
                raw_sel_for_s8 = str(c.market.selection or "")
                sel_tok_for_s8 = _strict_1x2_token(raw_sel_for_s8)
                fs0 = c.feature_snapshot_json if isinstance(c.feature_snapshot_json, dict) else {}
                fs_raw_minute = fs0.get("minute")
                fs_raw_sh = fs0.get("score_home")
                fs_raw_sa = fs0.get("score_away")
                fa0 = fs0.get("football_analytics") if isinstance(fs0.get("football_analytics"), dict) else None
                fa_exists = isinstance(fa0, dict)
                if fa_exists:
                    fa_minute = fa0.get("minute")
                    fa_sh = fa0.get("score_home")
                    fa_sa = fa0.get("score_away")
                if fam_for_s8 == "result" and mt_for_s8 in {"1x2", "match_winner"}:
                    s8_evaluated_count += 1
                    if st_for_s8 == "ft_1x2_candidate":
                        s8_result_1x2_candidate_count += 1
            except Exception:
                pass
            d8 = await evaluate_s8_live_1x2_winline_strict(c)
            try:
                if fam_for_s8 == "result" and mt_for_s8 in {"1x2", "match_winner"}:
                    if d8.passed and st_for_s8 == "ft_1x2_candidate":
                        s8_result_1x2_pass_count += 1
                    logger.info(
                        "[FOOTBALL][S8_GATE] event_id=%s target=%s match=%s market_family=%s market_type=%s "
                        "result_subtype=%s selection=%s selection_token=%s odds=%s football_analytics_exists=%s "
                        "feature_snapshot_minute=%s feature_snapshot_score=%s:%s football_analytics_minute=%s "
                        "football_analytics_score=%s:%s s8_passed=%s s8_reasons=%s",
                        str(_football_event_id(c) or ""),
                        "yes" if str(_football_event_id(c) or "") in {"15630480", "15644954"} else "no",
                        str(c.match.match_name or "")[:160],
                        fam_for_s8,
                        str(c.market.market_type or ""),
                        st_for_s8,
                        raw_sel_for_s8,
                        sel_tok_for_s8 or "None",
                        str(c.market.odds_value) if c.market.odds_value is not None else "None",
                        "yes" if fa_exists else "no",
                        fs_raw_minute,
                        fs_raw_sh,
                        fs_raw_sa,
                        fa_minute,
                        fa_sh,
                        fa_sa,
                        str(bool(d8.passed)).lower(),
                        list(d8.reasons or [])[:12],
                    )
            except Exception:
                logger.exception("[FOOTBALL][S8_GATE] candidate debug serialization failed")
            if not d8.passed:
                for r in (d8.reasons or [])[:12]:
                    s8_fail[str(r)] = int(s8_fail.get(str(r), 0) or 0) + 1
                # If this is a clean FT 1X2 candidate, track S8 rejects separately.
                try:
                    if family_svc.get_market_family(c) == "result" and _result_subtype_local(c) == "ft_1x2_candidate":
                        for r in (d8.reasons or [])[:12]:
                            s8_ft_fail[str(r)] = int(s8_ft_fail.get(str(r), 0) or 0) + 1
                        if len(ft_1x2_rejected_samples) < ft_1x2_rejected_limit:
                            try:
                                eid0 = _football_event_id(c) or ""
                                lc = (
                                    (c.feature_snapshot_json or {}).get("football_analytics")
                                    if isinstance(c.feature_snapshot_json, dict)
                                    else None
                                )
                                minute0 = None
                                sh0 = None
                                sa0 = None
                                if isinstance(lc, dict):
                                    minute0 = lc.get("minute")
                                    sh0 = lc.get("score_home")
                                    sa0 = lc.get("score_away")
                                bet = fmt.format_bet(
                                    market_type=c.market.market_type,
                                    market_label=c.market.market_label,
                                    selection=c.market.selection,
                                    home_team=c.match.home_team,
                                    away_team=c.match.away_team,
                                    section_name=c.market.section_name,
                                    subsection_name=c.market.subsection_name,
                                )
                                ft_1x2_rejected_samples.append(
                                    {
                                        "event_id": eid0,
                                        "match": str(c.match.match_name or ""),
                                        "minute": minute0,
                                        "score": f"{sh0}:{sa0}" if sh0 is not None and sa0 is not None else None,
                                        "bet_text": bet.main_label + (f" ({bet.detail_label})" if bet.detail_label else ""),
                                        "odds": (str(c.market.odds_value) if c.market.odds_value is not None else None),
                                        "selection": str(c.market.selection or ""),
                                        "s8_reasons": list(d8.reasons or [])[:12],
                                    }
                                )
                            except Exception:
                                pass
                except Exception:
                    pass
                # Collect draw candidates (after_integrity) and their S8 reject reasons.
                # IMPORTANT: include non-FT subtypes too (interval/period/etc) to prove whether draw flow is mostly "junk".
                try:
                    if len(away_draw_samples) < away_draw_samples_limit:
                        famx = family_svc.get_market_family(c)
                        mt0 = _norm_side(str(c.market.market_type or ""))
                        if famx == "result" and mt0 in {"1x2", "match_winner"}:
                            sside = _side_from_selection_1x2(c)
                            if sside == "draw":
                                lc = (
                                    (c.feature_snapshot_json or {}).get("football_analytics")
                                    if isinstance(c.feature_snapshot_json, dict)
                                    else None
                                )
                                minute0 = (lc.get("minute") if isinstance(lc, dict) else None)
                                sh0 = (lc.get("score_home") if isinstance(lc, dict) else None)
                                sa0 = (lc.get("score_away") if isinstance(lc, dict) else None)
                                bet = fmt.format_bet(
                                    market_type=c.market.market_type,
                                    market_label=c.market.market_label,
                                    selection=c.market.selection,
                                    home_team=c.match.home_team,
                                    away_team=c.match.away_team,
                                    section_name=c.market.section_name,
                                    subsection_name=c.market.subsection_name,
                                )
                                away_draw_samples.append(
                                    {
                                        "event_id": str(_football_event_id(c) or ""),
                                        "match": str(c.match.match_name or ""),
                                        "minute": minute0,
                                        "score": f"{sh0}:{sa0}" if sh0 is not None and sa0 is not None else None,
                                        "market_type": str(c.market.market_type or ""),
                                        "result_subtype": _result_subtype_local(c),
                                        "bet_text": bet.main_label + (f" ({bet.detail_label})" if bet.detail_label else ""),
                                        "odds": (str(c.market.odds_value) if c.market.odds_value is not None else None),
                                        "side": sside,
                                        "fate_stage": "rejected_in_s8",
                                        "s8_reject_reasons": list(d8.reasons or [])[:12],
                                    }
                                )
                except Exception:
                    pass
                # Only totals candidates can ever match S9 by definition.
                try:
                    if family_svc.get_market_family(c) == "totals":
                        s9_candidates.append(c)
                except Exception:
                    pass
            else:
                eid = _football_event_id(c)
                if eid and eid not in strategy_by_eid:
                    strategy_by_eid[eid] = d8.strategy_id or ""
                strategy_stats[str(d8.strategy_id or "S8_LIVE_1X2_WINLINE_STRICT")] = int(
                    strategy_stats.get(str(d8.strategy_id or "S8_LIVE_1X2_WINLINE_STRICT"), 0) or 0
                ) + 1
                prev_expl = dict(c.explanation_json or {})
                prev_expl["football_live_strategy_id"] = d8.strategy_id
                prev_expl["football_live_strategy_name"] = d8.strategy_name
                prev_expl["football_live_strategy_reasons"] = list(d8.reasons or [])
                s8_passed.append(c.model_copy(update={"explanation_json": prev_expl}))

        # --- S9 second (only for candidates that failed S8) ---
        for c in s9_candidates:
            d9 = await evaluate_s9_live_totals_over_controlled(c)
            if not d9.passed:
                for r in (d9.reasons or [])[:12]:
                    s9_fail[str(r)] = int(s9_fail.get(str(r), 0) or 0) + 1
                try:
                    if family_svc.get_market_family(c) == "totals":
                        s10_candidates.append(c)
                except Exception:
                    pass
                if len(strategy_rejected_samples) < strategy_rejected_limit:
                    try:
                        eid0 = _football_event_id(c) or ""
                        fam0 = family_svc.get_market_family(c)
                        lc = (
                            (c.feature_snapshot_json or {}).get("football_analytics")
                            if isinstance(c.feature_snapshot_json, dict)
                            else None
                        )
                        minute0 = None
                        sh0 = None
                        sa0 = None
                        if isinstance(lc, dict):
                            minute0 = lc.get("minute")
                            sh0 = lc.get("score_home")
                            sa0 = lc.get("score_away")
                        bet = fmt.format_bet(
                            market_type=c.market.market_type,
                            market_label=c.market.market_label,
                            selection=c.market.selection,
                            home_team=c.match.home_team,
                            away_team=c.match.away_team,
                            section_name=c.market.section_name,
                            subsection_name=c.market.subsection_name,
                        )
                        strategy_rejected_samples.append(
                            {
                                "event_id": eid0,
                                "match": str(c.match.match_name or ""),
                                "minute": minute0,
                                "score": f"{sh0}:{sa0}" if sh0 is not None and sa0 is not None else None,
                                "market_type": str(c.market.market_type or ""),
                                "market_family": str(fam0 or ""),
                                "bet_text": bet.main_label + (f" ({bet.detail_label})" if bet.detail_label else ""),
                                "odds": (str(c.market.odds_value) if c.market.odds_value is not None else None),
                                "strategy": "S9_LIVE_TOTALS_OVER_CONTROLLED",
                                "reasons": list(d9.reasons or [])[:12],
                            }
                        )
                    except Exception:
                        pass
                continue

            eid = _football_event_id(c)
            if eid and eid not in strategy_by_eid:
                strategy_by_eid[eid] = d9.strategy_id or ""
            strategy_stats[str(d9.strategy_id or "S9_LIVE_TOTALS_OVER_CONTROLLED")] = int(
                strategy_stats.get(str(d9.strategy_id or "S9_LIVE_TOTALS_OVER_CONTROLLED"), 0) or 0
            ) + 1
            prev_expl = dict(c.explanation_json or {})
            prev_expl["football_live_strategy_id"] = d9.strategy_id
            prev_expl["football_live_strategy_name"] = d9.strategy_name
            prev_expl["football_live_strategy_reasons"] = list(d9.reasons or [])
            s9_passed.append(c.model_copy(update={"explanation_json": prev_expl}))

        # --- S10 third (only for candidates that failed S9) ---
        for c in s10_candidates:
            try:
                from app.services.football_bet_formatter_service import FootballBetFormatterService as _FmtTT

                _ctx = _FmtTT().describe_total_context(
                    market_type=c.market.market_type,
                    market_label=c.market.market_label,
                    selection=c.market.selection,
                    home_team=c.match.home_team,
                    away_team=c.match.away_team,
                    section_name=c.market.section_name,
                    subsection_name=c.market.subsection_name,
                )
                if _ctx and str(_ctx.target_scope or "").strip().lower() in {"home_team", "away_team", "team_total"}:
                    s10_team_total_count += 1
            except Exception:
                pass
            d10 = await evaluate_s10_live_team_total_over_controlled(c)
            if not d10.passed:
                for r in (d10.reasons or [])[:12]:
                    s10_fail[str(r)] = int(s10_fail.get(str(r), 0) or 0) + 1
                try:
                    if family_svc.get_market_family(c) == "totals":
                        s11_candidates.append(c)
                except Exception:
                    pass
                if len(strategy_rejected_samples) < strategy_rejected_limit:
                    try:
                        eid0 = _football_event_id(c) or ""
                        fam0 = family_svc.get_market_family(c)
                        lc = (
                            (c.feature_snapshot_json or {}).get("football_analytics")
                            if isinstance(c.feature_snapshot_json, dict)
                            else None
                        )
                        minute0 = None
                        sh0 = None
                        sa0 = None
                        if isinstance(lc, dict):
                            minute0 = lc.get("minute")
                            sh0 = lc.get("score_home")
                            sa0 = lc.get("score_away")
                        bet = fmt.format_bet(
                            market_type=c.market.market_type,
                            market_label=c.market.market_label,
                            selection=c.market.selection,
                            home_team=c.match.home_team,
                            away_team=c.match.away_team,
                            section_name=c.market.section_name,
                            subsection_name=c.market.subsection_name,
                        )
                        strategy_rejected_samples.append(
                            {
                                "event_id": eid0,
                                "match": str(c.match.match_name or ""),
                                "minute": minute0,
                                "score": f"{sh0}:{sa0}" if sh0 is not None and sa0 is not None else None,
                                "market_type": str(c.market.market_type or ""),
                                "market_family": str(fam0 or ""),
                                "bet_text": bet.main_label + (f" ({bet.detail_label})" if bet.detail_label else ""),
                                "odds": (str(c.market.odds_value) if c.market.odds_value is not None else None),
                                "strategy": "S10_LIVE_TEAM_TOTAL_OVER_CONTROLLED",
                                "reasons": list(d10.reasons or [])[:12],
                            }
                        )
                    except Exception:
                        pass
                continue

            eid = _football_event_id(c)
            if eid and eid not in strategy_by_eid:
                strategy_by_eid[eid] = d10.strategy_id or ""
            strategy_stats[str(d10.strategy_id or "S10_LIVE_TEAM_TOTAL_OVER_CONTROLLED")] = int(
                strategy_stats.get(str(d10.strategy_id or "S10_LIVE_TEAM_TOTAL_OVER_CONTROLLED"), 0) or 0
            ) + 1
            prev_expl = dict(c.explanation_json or {})
            prev_expl["football_live_strategy_id"] = d10.strategy_id
            prev_expl["football_live_strategy_name"] = d10.strategy_name
            prev_expl["football_live_strategy_reasons"] = list(d10.reasons or [])
            s10_passed.append(c.model_copy(update={"explanation_json": prev_expl}))

        # --- S11 fourth (only for totals that failed S9/S10) ---
        s11_fail: dict[str, int] = {}
        s11_pass_samples: list[dict[str, object]] = []
        s11_pass_sample_limit = 5
        for c in s11_candidates:
            d11 = await evaluate_s11_live_match_total_over_need_1_controlled(c)
            if not d11.passed:
                for r in (d11.reasons or [])[:12]:
                    s11_fail[str(r)] = int(s11_fail.get(str(r), 0) or 0) + 1
                continue

            eid = _football_event_id(c)
            if eid and eid not in strategy_by_eid:
                strategy_by_eid[eid] = d11.strategy_id or ""
            strategy_stats[str(d11.strategy_id or "S11_LIVE_MATCH_TOTAL_OVER_NEED_1_CONTROLLED")] = int(
                strategy_stats.get(str(d11.strategy_id or "S11_LIVE_MATCH_TOTAL_OVER_NEED_1_CONTROLLED"), 0) or 0
            ) + 1
            prev_expl = dict(c.explanation_json or {})
            prev_expl["football_live_strategy_id"] = d11.strategy_id
            prev_expl["football_live_strategy_name"] = d11.strategy_name
            prev_expl["football_live_strategy_reasons"] = list(d11.reasons or [])
            cand_s11 = c.model_copy(update={"explanation_json": prev_expl})
            s11_passed.append(cand_s11)
            if len(s11_pass_samples) < s11_pass_sample_limit:
                try:
                    lc = (
                        (cand_s11.feature_snapshot_json or {}).get("football_analytics")
                        if isinstance(cand_s11.feature_snapshot_json, dict)
                        else None
                    )
                    minute0 = lc.get("minute") if isinstance(lc, dict) else None
                    sh0 = lc.get("score_home") if isinstance(lc, dict) else None
                    sa0 = lc.get("score_away") if isinstance(lc, dict) else None
                    ctx0 = fmt.describe_total_context(
                        market_type=cand_s11.market.market_type,
                        market_label=cand_s11.market.market_label,
                        selection=cand_s11.market.selection,
                        home_team=cand_s11.match.home_team,
                        away_team=cand_s11.match.away_team,
                        section_name=cand_s11.market.section_name,
                        subsection_name=cand_s11.market.subsection_name,
                    )
                    line0 = None
                    if ctx0 and ctx0.total_line:
                        line0 = float(str(ctx0.total_line).replace(",", "."))
                    goals_needed0 = None
                    if line0 is not None and sh0 is not None and sa0 is not None:
                        goals_needed0 = int(math.floor(float(line0))) + 1 - (int(sh0) + int(sa0))
                    s11_pass_samples.append(
                        {
                            "event_id": str(eid or ""),
                            "match": str(cand_s11.match.match_name or ""),
                            "minute": minute0,
                            "score": f"{sh0}:{sa0}" if sh0 is not None and sa0 is not None else None,
                            "line": line0,
                            "odds": str(cand_s11.market.odds_value) if cand_s11.market.odds_value is not None else None,
                            "goals_needed_to_win": goals_needed0,
                        }
                    )
                except Exception:
                    pass

        strategy_passed = [*s8_passed, *s9_passed, *s10_passed, *s11_passed]
        candidates_to_ingest = strategy_passed
        side_after_s8 = _side_breakdown(list(s8_passed))
        strategy_gate_debug = {
            "strategy_stats": dict(strategy_stats),
            "strategy_matches": int(len(strategy_by_eid)),
            "post_integrity_result_like": int(post_integrity_result_like),
            "post_integrity_result_like_1x2": int(post_integrity_result_like_1x2),
            "s8_evaluated_candidates": int(s8_evaluated_count),
            "s8_clean_ft_1x2_candidates": int(s8_result_1x2_candidate_count),
            "s8_clean_ft_1x2_passed": int(s8_result_1x2_pass_count),
            "post_integrity_flow": {
                "market_family": dict(sorted(flow_family.items(), key=lambda kv: kv[1], reverse=True)),
                "market_type": dict(sorted(flow_market_type.items(), key=lambda kv: kv[1], reverse=True)[:80]),
                "normalized_bet_type": dict(sorted(flow_norm_bet.items(), key=lambda kv: kv[1], reverse=True)[:120]),
                "result_subtype": dict(sorted(flow_result_subtype.items(), key=lambda kv: kv[1], reverse=True)),
                "totals_over_under": dict(sorted(flow_ou.items(), key=lambda kv: kv[1], reverse=True)),
                "totals_scope": dict(sorted(flow_total_scope.items(), key=lambda kv: kv[1], reverse=True)[:40]),
                "totals_line": dict(sorted(flow_line.items(), key=lambda kv: kv[1], reverse=True)[:40]),
                "minute_buckets": dict(sorted(flow_minute_bucket.items(), key=lambda kv: kv[1], reverse=True)),
                "score_buckets": dict(sorted(flow_score_bucket.items(), key=lambda kv: kv[1], reverse=True)),
                "odds_buckets": dict(sorted(flow_odds_bucket.items(), key=lambda kv: kv[1], reverse=True)),
            },
            "strategy_breakdown_s1": dict(sorted(s1_fail.items(), key=lambda kv: kv[1], reverse=True)[:50]),
            "strategy_breakdown_s2": dict(sorted(s2_fail.items(), key=lambda kv: kv[1], reverse=True)[:50]),
            "strategy_breakdown_s8": dict(sorted(s8_fail.items(), key=lambda kv: kv[1], reverse=True)[:60]),
            "strategy_breakdown_s8_ft_1x2_only": dict(sorted(s8_ft_fail.items(), key=lambda kv: kv[1], reverse=True)[:60]),
            "strategy_breakdown_s9": dict(sorted(s9_fail.items(), key=lambda kv: kv[1], reverse=True)[:60]),
            "s10_team_total_candidates": int(s10_team_total_count),
            "s10_pass": int(len(s10_passed)),
            "s10_reject_breakdown": dict(sorted(s10_fail.items(), key=lambda kv: kv[1], reverse=True)[:60]),
            "s11_pass": int(len(s11_passed)),
            "s11_reject_breakdown": dict(sorted(s11_fail.items(), key=lambda kv: kv[1], reverse=True)[:60]),
            "s11_pass_samples": s11_pass_samples,
            "after_s8": int(len(s8_passed)),
            "after_s9": int(len(s8_passed) + len(s9_passed)),
            "after_s10": int(len(s8_passed) + len(s9_passed) + len(s10_passed)),
            "after_s11": int(len(strategy_passed)),
            "strategy_rejected_samples": strategy_rejected_samples,
            "ft_1x2_rejected_samples": ft_1x2_rejected_samples,
        }
        diagnostics.update(
            football_live_cycle_after_s8=int(len(s8_passed)),
            football_live_cycle_after_s9=int(len(s8_passed) + len(s9_passed)),
            football_live_cycle_after_s10=int(len(s8_passed) + len(s9_passed) + len(s10_passed)),
            football_live_cycle_after_s11=int(len(strategy_passed)),
            football_live_cycle_after_strategy=int(len(strategy_passed)),
            football_live_cycle_after_context_filter=0,
            football_live_rejected_no_pressure=0,
            football_live_rejected_s8_home_00_without_api_context=0,
            football_live_passed_pressure=0,
            football_live_rejected_s8_1x2_00_without_api_context=0,
            football_live_rejected_s8_1x2_00_no_pressure=0,
            football_live_passed_s8_1x2_00_with_api_pressure=0,
            football_live_context_filter_last_cycle_json=None,
            football_live_cycle_after_value_filter=0,
            football_live_rejected_value_low_favorite=0,
            football_live_rejected_value_no_edge=0,
            football_live_passed_value_filter=0,
        )

        if not candidates_to_ingest:
            # Persist side funnel even when nothing passes strategy (critical to locate where away/draw disappear).
            _persist_side_funnel_once(
                stages={
                    "after_integrity": side_after_integrity,
                    "after_s8": side_after_s8,
                    "after_scoring_all": {"home": 0, "away": 0, "draw": 0, "unknown": 0, "total": 0},
                    "after_scoring": {"home": 0, "away": 0, "draw": 0, "unknown": 0, "total": 0},
                    "final_selected": {"home": 0, "away": 0, "draw": 0, "unknown": 0, "total": 0},
                },
                samples=away_draw_samples,
            )
            diagnostics.update(
                final_signals_count=0,
                messages_sent_count=0,
                football_sent_count=0,
                last_delivery_reason="blocked_no_strategy_match",
                note="no candidate matched explicit football live strategies",
            )
            _dbg_strat = compile_football_cycle_debug(
                fb_preview=fb_preview,
                fb_cvf=fb_cvf,
                fb_post_send=fb_post_send_saved,
                fb_post_integrity=fb_post_integrity_saved,
                enriched_scored=enriched,
                finalists=[],
                finalists_pre_session=[],
                min_score=float(settings.football_min_signal_score or 60.0),
                family_svc=FootballSignalSendFilterService(),
                send_filter_stats=send_filter_result.stats if send_filter_result else None,
                integrity_dropped_checks=list(integrity_result.dropped_checks),
                dry_run=dry_run,
                global_block="blocked_no_strategy_match",
                min_score_base=float(settings.football_min_signal_score or 60.0),
                score_relief_note="explicit_strategies",
                live_send_stats=strategy_gate_debug,
                finalist_send_meta={},
                single_relief_max_gap=float(single_gap_max),
            )
            return AutoSignalCycleResult(
                endpoint=fetch_res.endpoint,
                fetch_ok=True,
                preview_candidates=preview_candidates,
                preview_skipped_items=preview_skipped_items,
                created_signal_ids=[],
                created_signals_count=0,
                skipped_candidates_count=int(omitted_by_limit),
                notifications_sent_count=0,
                preview_only=False,
                message="no_strategy_match",
                raw_events_count=raw_events_count,
                normalized_markets_count=normalized_markets_count,
                candidates_before_filter_count=len(candidates_before_filter),
                candidates_after_filter_count=len(filtered_candidates),
                runtime_paused=False,
                runtime_active_sports=active_sports,
                source_name=source_name,
                live_auth_status=live_auth_status,
                last_live_http_status=fetch_res.status_code,
                fallback_used=fallback_used,
                fallback_source_name=fallback_source_name,
                rejection_reason="blocked_no_strategy_match",
                dry_run=dry_run,
                report_matches_found=report_matches_found,
                report_candidates=report_candidates,
                report_after_filter=report_after_filter,
                report_after_integrity=report_after_integrity,
                report_after_scoring=0,
                report_final_signal="НЕТ",
                report_rejection_code="blocked_no_strategy_match",
                football_cycle_debug=_dbg_strat,
            )

        def _live_ctx_from_candidate(cand: ProviderSignalCandidate) -> tuple[int | None, int | None, int | None]:
            fs = getattr(cand, "feature_snapshot_json", None) or {}
            if not isinstance(fs, dict):
                fs = {}
            fa = fs.get("football_analytics") if isinstance(fs.get("football_analytics"), dict) else {}
            minute = fa.get("minute")
            sh = fa.get("score_home")
            sa = fa.get("score_away")
            try:
                minute_i = int(minute) if minute is not None else None
            except Exception:
                minute_i = None
            try:
                sh_i = int(sh) if sh is not None else None
            except Exception:
                sh_i = None
            try:
                sa_i = int(sa) if sa is not None else None
            except Exception:
                sa_i = None
            return minute_i, sh_i, sa_i

        # --- CONTEXT FILTER (post S8, pre value filter) ---
        # API-Football is a soft, fail-open context layer for classic 1X2 picks.
        context_rejected = 0
        context_passed = 0
        context_rejected_samples: list[dict[str, object]] = []
        context_passed_samples: list[dict[str, object]] = []
        context_sample_limit = 6
        after_context: list[ProviderSignalCandidate] = []
        from app.services.api_football_service import ApiFootballService

        api_football_runtime_ok = bool(getattr(settings, "api_football_enabled", False))
        api_ctx_svc: ApiFootballService | None = None
        live_fixtures_af: list | None = None
        stats_by_fixture: dict[int, object] = {}

        def _winner_side_from_score(sh: int | None, sa: int | None) -> str | None:
            if sh is None or sa is None:
                return None
            if sh > sa:
                return "home"
            if sa > sh:
                return "away"
            return "draw"

        def _pressure_score_details(
            *,
            shots_total_for: int | None,
            shots_total_against: int | None,
            shots_on_for: int | None,
            shots_on_against: int | None,
            possession_for: int | None,
        ) -> tuple[int, str]:
            score = 0
            hits: list[str] = []
            miss: list[str] = []
            shots_total_diff = (
                (shots_total_for - shots_total_against)
                if shots_total_for is not None and shots_total_against is not None
                else None
            )
            shots_on_diff = (
                (shots_on_for - shots_on_against)
                if shots_on_for is not None and shots_on_against is not None
                else None
            )
            if shots_total_diff is not None and shots_total_diff >= 4:
                score += 1
                hits.append(f"shots_total_diff={shots_total_diff}>=4")
            else:
                miss.append(f"shots_total_diff={shots_total_diff}")
            if shots_on_diff is not None and shots_on_diff >= 2:
                score += 1
                hits.append(f"shots_on_target_diff={shots_on_diff}>=2")
            else:
                miss.append(f"shots_on_target_diff={shots_on_diff}")
            if possession_for is not None and possession_for >= 58:
                score += 1
                hits.append(f"possession={possession_for}>=58")
            else:
                miss.append(f"possession={possession_for}")
            parts: list[str] = []
            if hits:
                parts.append("met: " + ", ".join(hits))
            if miss:
                parts.append("missed: " + ", ".join(miss))
            return score, "; ".join(parts) if parts else "no_api_football_pressure_stats"

        def _context_payload_from_stats(stats: object) -> dict[str, object]:
            if stats is None:
                return {}
            return {
                "home_shots_total": getattr(stats, "home_shots_total", None),
                "away_shots_total": getattr(stats, "away_shots_total", None),
                "home_shots_on_target": getattr(stats, "home_shots_on_target", None),
                "away_shots_on_target": getattr(stats, "away_shots_on_target", None),
                "home_possession": getattr(stats, "home_possession", None),
                "away_possession": getattr(stats, "away_possession", None),
                "home_attacks": getattr(stats, "home_attacks", None),
                "away_attacks": getattr(stats, "away_attacks", None),
                "home_dangerous_attacks": getattr(stats, "home_dangerous_attacks", None),
                "away_dangerous_attacks": getattr(stats, "away_dangerous_attacks", None),
            }

        if api_football_runtime_ok:
            try:
                api_ctx_svc = ApiFootballService()
                live_fixtures_af = api_ctx_svc.get_live_fixtures()
            except Exception:
                live_fixtures_af = None

        for cand in candidates_to_ingest:
            fam_ctx = family_svc.get_market_family(cand)
            mt_ctx = _norm_side(str(cand.market.market_type or ""))
            sel_side = _side_from_selection_1x2(cand)
            minute_i, sh_i, sa_i = _live_ctx_from_candidate(cand)

            # Only classic result 1X2 picks are context-enriched. All other markets pass through unchanged.
            if not (fam_ctx == "result" and mt_ctx in {"1x2", "match_winner"} and sel_side in {"home", "away"}):
                after_context.append(cand)
                continue

            if not api_ctx_svc or not live_fixtures_af:
                cand_ctx = _attach_api_football_context(
                    cand,
                    {
                    "source": "api_football",
                    "enabled": False,
                    "decision": "skip",
                    "skip_reason": "api_football_unavailable",
                    },
                )
                logger.info(
                    "[CONTEXT] match=%s selection=%s side=%s -> skip(api_football_unavailable)",
                    str(cand.match.match_name or "")[:140],
                    str(cand.market.selection or ""),
                    sel_side,
                )
                after_context.append(cand_ctx)
                continue

            fx = api_ctx_svc.map_winline_match_to_fixture(
                winline_home=str(cand.match.home_team or ""),
                winline_away=str(cand.match.away_team or ""),
                fixtures=live_fixtures_af,
            )
            if fx is None:
                cand_ctx = _attach_api_football_context(
                    cand,
                    {
                    "source": "api_football",
                    "enabled": True,
                    "decision": "skip",
                    "skip_reason": "api_football_fixture_not_mapped",
                    },
                )
                logger.info(
                    "[CONTEXT] match=%s selection=%s side=%s -> skip(api_football_fixture_not_mapped)",
                    str(cand.match.match_name or "")[:140],
                    str(cand.market.selection or ""),
                    sel_side,
                )
                after_context.append(cand_ctx)
                continue

            stats = stats_by_fixture.get(int(fx.fixture_id))
            if stats is None:
                try:
                    stats = api_ctx_svc.get_fixture_statistics(int(fx.fixture_id))
                except Exception:
                    stats = None
                stats_by_fixture[int(fx.fixture_id)] = stats
            if stats is None:
                cand_ctx = _attach_api_football_context(
                    cand,
                    {
                    "source": "api_football",
                    "enabled": True,
                    "fixture_id": int(fx.fixture_id),
                    "decision": "skip",
                    "skip_reason": "api_football_stats_missing",
                    },
                )
                logger.info(
                    "[CONTEXT] match=%s selection=%s side=%s fixture=%s -> skip(api_football_stats_missing)",
                    str(cand.match.match_name or "")[:140],
                    str(cand.market.selection or ""),
                    sel_side,
                    int(fx.fixture_id),
                )
                after_context.append(cand_ctx)
                continue

            home_pressure, home_pressure_reason = _pressure_score_details(
                shots_total_for=getattr(stats, "home_shots_total", None),
                shots_total_against=getattr(stats, "away_shots_total", None),
                shots_on_for=getattr(stats, "home_shots_on_target", None),
                shots_on_against=getattr(stats, "away_shots_on_target", None),
                possession_for=getattr(stats, "home_possession", None),
            )
            away_pressure, away_pressure_reason = _pressure_score_details(
                shots_total_for=getattr(stats, "away_shots_total", None),
                shots_total_against=getattr(stats, "home_shots_total", None),
                shots_on_for=getattr(stats, "away_shots_on_target", None),
                shots_on_against=getattr(stats, "home_shots_on_target", None),
                possession_for=getattr(stats, "away_possession", None),
            )
            selected_pressure = home_pressure if sel_side == "home" else away_pressure
            selected_pressure_reason = home_pressure_reason if sel_side == "home" else away_pressure_reason
            winner_side = _winner_side_from_score(sh_i, sa_i)
            trailing = bool(winner_side in {"home", "away"} and winner_side != sel_side)
            pressure_supportive = bool(selected_pressure >= 2)
            decision = "pass_soft_positive" if pressure_supportive else "pass_soft_neutral"
            reject_reason: str | None = None
            if not pressure_supportive:
                reject_reason = "soft_context_trailing_no_pressure" if trailing else "soft_context_no_pressure"

            ctx_payload = {
                "source": "api_football",
                "enabled": True,
                "fixture_id": int(fx.fixture_id),
                "selected_side": sel_side,
                "selected_pressure_score": int(selected_pressure),
                "selected_pressure_reason": selected_pressure_reason,
                "home_pressure_score": int(home_pressure),
                "home_pressure_reason": home_pressure_reason,
                "away_pressure_score": int(away_pressure),
                "away_pressure_reason": away_pressure_reason,
                "pressure_supportive": pressure_supportive,
                "decision": decision,
                "reject_reason": reject_reason,
                "minute": minute_i,
                "score_home": sh_i,
                "score_away": sa_i,
                "stats": _context_payload_from_stats(stats),
            }
            logger.info(
                "[CONTEXT] match=%s fixture=%s side=%s pressure home=%s away=%s selected=%s minute=%s score=%s:%s -> %s%s",
                str(cand.match.match_name or "")[:140],
                int(fx.fixture_id),
                sel_side,
                int(home_pressure),
                int(away_pressure),
                int(selected_pressure),
                minute_i,
                sh_i,
                sa_i,
                decision,
                f" ({reject_reason})" if reject_reason else "",
            )

            cand_ctx = _attach_api_football_context(cand, ctx_payload)
            sample = {
                "event_id": str(_football_event_id(cand_ctx) or ""),
                "match": str(cand_ctx.match.match_name or ""),
                "fixture_id": int(fx.fixture_id),
                "minute": minute_i,
                "score": f"{sh_i}:{sa_i}" if sh_i is not None and sa_i is not None else None,
                "selection": str(cand_ctx.market.selection or ""),
                "selected_side": sel_side,
                "home_pressure": int(home_pressure),
                "away_pressure": int(away_pressure),
                "selected_pressure": int(selected_pressure),
                "selected_pressure_reason": selected_pressure_reason,
                "decision": decision,
                "reject_reason": reject_reason,
                "stats": _context_payload_from_stats(stats),
            }
            if pressure_supportive:
                context_passed += 1
                if len(context_passed_samples) < context_sample_limit:
                    context_passed_samples.append(sample)
            else:
                context_rejected += 1
                if len(context_rejected_samples) < context_sample_limit:
                    context_rejected_samples.append(sample)
            after_context.append(cand_ctx)

        side_after_context_filter = _side_breakdown(list(after_context))
        candidates_to_ingest = after_context
        diagnostics.update(
            football_live_rejected_no_pressure=0,
            football_live_passed_pressure=int(context_passed),
            football_live_cycle_after_context_filter=int(len(after_context)),
            football_live_context_filter_last_cycle_json=json.dumps(
                {
                    "api_football_runtime_ok": bool(api_football_runtime_ok),
                    "mapped_live_fixtures": len(live_fixtures_af or []),
                    "soft_positive_count": int(context_passed),
                    "soft_neutral_count": int(context_rejected),
                    "soft_neutral_samples": context_rejected_samples,
                    "soft_positive_samples": context_passed_samples,
                },
                ensure_ascii=False,
            )[:12000],
        )
        try:
            strategy_gate_debug["context_filter_rejected_samples"] = context_rejected_samples
            strategy_gate_debug["context_filter_passed_samples"] = context_passed_samples
        except Exception:
            pass

        if not candidates_to_ingest:
            _persist_side_funnel_once(
                stages={
                    "after_integrity": side_after_integrity,
                    "after_s8": side_after_s8,
                    "after_context_filter": side_after_context_filter,
                    "after_value_filter": {"home": 0, "away": 0, "draw": 0, "unknown": 0, "total": 0},
                    "after_scoring_all": {"home": 0, "away": 0, "draw": 0, "unknown": 0, "total": 0},
                    "after_scoring": {"home": 0, "away": 0, "draw": 0, "unknown": 0, "total": 0},
                    "final_selected": {"home": 0, "away": 0, "draw": 0, "unknown": 0, "total": 0},
                },
                samples=away_draw_samples,
            )
            try:
                strategy_gate_debug["context_filter_rejected_samples"] = context_rejected_samples
                strategy_gate_debug["context_filter_passed_samples"] = context_passed_samples
            except Exception:
                pass
            diagnostics.update(
                final_signals_count=0,
                messages_sent_count=0,
                football_sent_count=0,
                last_delivery_reason="blocked_context_filter",
                note="all strategy-approved candidates rejected by context filter",
            )
            _dbg_c = compile_football_cycle_debug(
                fb_preview=fb_preview,
                fb_cvf=fb_cvf,
                fb_post_send=fb_post_send_saved,
                fb_post_integrity=fb_post_integrity_saved,
                enriched_scored=enriched,
                finalists=[],
                finalists_pre_session=[],
                min_score=float(settings.football_min_signal_score or 60.0),
                family_svc=FootballSignalSendFilterService(),
                send_filter_stats=send_filter_result.stats if send_filter_result else None,
                integrity_dropped_checks=list(integrity_result.dropped_checks),
                dry_run=dry_run,
                global_block="blocked_context_filter",
                min_score_base=float(settings.football_min_signal_score or 60.0),
                score_relief_note="context_filter",
                live_send_stats=strategy_gate_debug,
                finalist_send_meta={},
                single_relief_max_gap=float(single_gap_max),
            )
            return AutoSignalCycleResult(
                endpoint=fetch_res.endpoint,
                fetch_ok=True,
                preview_candidates=preview_candidates,
                preview_skipped_items=preview_skipped_items,
                created_signal_ids=[],
                created_signals_count=0,
                skipped_candidates_count=int(omitted_by_limit),
                notifications_sent_count=0,
                preview_only=False,
                message="blocked_context_filter",
                raw_events_count=raw_events_count,
                normalized_markets_count=normalized_markets_count,
                candidates_before_filter_count=len(candidates_before_filter),
                candidates_after_filter_count=len(filtered_candidates),
                runtime_paused=False,
                runtime_active_sports=active_sports,
                source_name=source_name,
                live_auth_status=live_auth_status,
                last_live_http_status=fetch_res.status_code,
                fallback_used=fallback_used,
                fallback_source_name=fallback_source_name,
                rejection_reason="blocked_context_filter",
                dry_run=dry_run,
                report_matches_found=report_matches_found,
                report_candidates=report_candidates,
                report_after_filter=report_after_filter,
                report_after_integrity=report_after_integrity,
                report_after_scoring=0,
                report_final_signal="НЕТ",
                report_rejection_code="blocked_context_filter",
                football_cycle_debug=_dbg_c,
            )

        # --- TEMP SAFETY GATE (post context, pre value) ---
        # Only for the dangerous pattern: S8 + classic 1X2 + 0:0. All other markets
        # remain fail-open and continue unchanged.
        s8_1x2_00_without_api_context_rejected = 0
        s8_1x2_00_no_pressure_rejected = 0
        s8_1x2_00_with_api_pressure_passed = 0
        s8_1x2_00_without_api_context_samples: list[dict[str, object]] = []
        s8_1x2_00_no_pressure_samples: list[dict[str, object]] = []
        s8_1x2_00_with_api_pressure_samples: list[dict[str, object]] = []
        s8_1x2_00_sample_limit = 6
        s8_1x2_00_drop_by_eid: dict[str, str] = {}
        s8_1x2_00_drop_reasons: dict[str, str] = {}
        after_s8_safety: list[ProviderSignalCandidate] = []

        def _build_s8_1x2_00_safety_sample(
            cand: ProviderSignalCandidate,
            participation: dict[str, object],
            *,
            minute_i: int | None,
            sh_i: int | None,
            sa_i: int | None,
            safety_decision: str,
        ) -> dict[str, object]:
            bet = fmt.format_bet(
                market_type=cand.market.market_type,
                market_label=cand.market.market_label,
                selection=cand.market.selection,
                home_team=cand.match.home_team,
                away_team=cand.match.away_team,
                section_name=cand.market.section_name,
                subsection_name=cand.market.subsection_name,
            )
            ctx_payload = (
                (cand.feature_snapshot_json or {}).get("football_live_context_filter")
                if isinstance((cand.feature_snapshot_json or {}).get("football_live_context_filter"), dict)
                else {}
            )
            return {
                "event_id": str(_football_event_id(cand) or ""),
                "match": str(cand.match.match_name or ""),
                "minute": minute_i,
                "score": f"{sh_i}:{sa_i}" if sh_i is not None and sa_i is not None else None,
                "odds": str(cand.market.odds_value) if cand.market.odds_value is not None else None,
                "bet_text": bet.main_label + (f" ({bet.detail_label})" if bet.detail_label else ""),
                "selected_side": _side_from_selection_1x2(cand),
                "context_label": participation.get("context_label"),
                "api_football_mapping_ok": bool(participation.get("api_football_mapping_ok")),
                "api_football_stats_ok": bool(participation.get("api_football_stats_ok")),
                "api_football_pressure_used": bool(participation.get("api_football_pressure_used")),
                "api_football_pressure_score": participation.get("api_football_pressure_score"),
                "api_football_pressure_reason": participation.get("api_football_pressure_reason"),
                "context_decision": ctx_payload.get("decision") if isinstance(ctx_payload, dict) else None,
                "safety_decision": safety_decision,
            }

        for cand in candidates_to_ingest:
            ex0 = dict(cand.explanation_json or {})
            sid0 = str(ex0.get("football_live_strategy_id") or "").strip()
            fam0 = family_svc.get_market_family(cand)
            mt0 = _norm_side(str(cand.market.market_type or ""))
            st0 = _result_subtype_local(cand)
            side0 = _side_from_selection_1x2(cand)
            minute_i, sh_i, sa_i = _live_ctx_from_candidate(cand)
            is_s8_1x2_00 = (
                sid0 == "S8_LIVE_1X2_WINLINE_STRICT"
                and fam0 == "result"
                and mt0 in {"1x2", "match_winner"}
                and st0 == "ft_1x2_candidate"
                and side0 in {"home", "away"}
                and (sh_i, sa_i) == (0, 0)
            )
            if not is_s8_1x2_00:
                after_s8_safety.append(cand)
                continue

            participation = _build_live_context_participation(cand)
            api_mapping_ok = bool(participation.get("api_football_mapping_ok"))
            api_stats_ok = bool(participation.get("api_football_stats_ok"))
            api_pressure_used = bool(participation.get("api_football_pressure_used"))
            pressure_score_raw = participation.get("api_football_pressure_score")
            try:
                pressure_score = int(pressure_score_raw) if pressure_score_raw is not None else None
            except Exception:
                pressure_score = None

            if not (api_mapping_ok and api_stats_ok and api_pressure_used):
                s8_1x2_00_without_api_context_rejected += 1
                sample = _build_s8_1x2_00_safety_sample(
                    cand,
                    participation,
                    minute_i=minute_i,
                    sh_i=sh_i,
                    sa_i=sa_i,
                    safety_decision="blocked_s8_1x2_00_without_api_context",
                )
                eid0 = str(sample.get("event_id") or "")
                if eid0:
                    s8_1x2_00_drop_by_eid[eid0] = "blocked_s8_1x2_00_without_api_context"
                    s8_1x2_00_drop_reasons[eid0] = (
                        "temporary safety rule: S8 1X2 at 0:0 requires mapped API-Football stats + pressure"
                    )
                    try:
                        strategy_by_eid.pop(eid0, None)
                    except Exception:
                        pass
                if len(s8_1x2_00_without_api_context_samples) < s8_1x2_00_sample_limit:
                    s8_1x2_00_without_api_context_samples.append(sample)
                continue

            if pressure_score is None or pressure_score < 2:
                s8_1x2_00_no_pressure_rejected += 1
                sample = _build_s8_1x2_00_safety_sample(
                    cand,
                    participation,
                    minute_i=minute_i,
                    sh_i=sh_i,
                    sa_i=sa_i,
                    safety_decision="blocked_s8_1x2_00_no_pressure",
                )
                eid0 = str(sample.get("event_id") or "")
                if eid0:
                    s8_1x2_00_drop_by_eid[eid0] = "blocked_s8_1x2_00_no_pressure"
                    s8_1x2_00_drop_reasons[eid0] = str(
                        participation.get("api_football_pressure_reason")
                        or "temporary safety rule: pressure_score < 2 for S8 1X2 at 0:0"
                    )
                    try:
                        strategy_by_eid.pop(eid0, None)
                    except Exception:
                        pass
                if len(s8_1x2_00_no_pressure_samples) < s8_1x2_00_sample_limit:
                    s8_1x2_00_no_pressure_samples.append(sample)
                continue

            s8_1x2_00_with_api_pressure_passed += 1
            if len(s8_1x2_00_with_api_pressure_samples) < s8_1x2_00_sample_limit:
                s8_1x2_00_with_api_pressure_samples.append(
                    _build_s8_1x2_00_safety_sample(
                        cand,
                        participation,
                        minute_i=minute_i,
                        sh_i=sh_i,
                        sa_i=sa_i,
                        safety_decision="passed_s8_1x2_00_with_api_pressure",
                    )
                )
            after_s8_safety.append(cand)

        candidates_to_ingest = after_s8_safety
        diagnostics.update(
            football_live_rejected_s8_home_00_without_api_context=int(s8_1x2_00_without_api_context_rejected),
            football_live_rejected_s8_1x2_00_without_api_context=int(s8_1x2_00_without_api_context_rejected),
            football_live_rejected_s8_1x2_00_no_pressure=int(s8_1x2_00_no_pressure_rejected),
            football_live_passed_s8_1x2_00_with_api_pressure=int(s8_1x2_00_with_api_pressure_passed),
        )
        try:
            strategy_gate_debug["rejected_s8_1x2_00_without_api_context"] = int(
                s8_1x2_00_without_api_context_rejected
            )
            strategy_gate_debug["rejected_s8_1x2_00_no_pressure"] = int(s8_1x2_00_no_pressure_rejected)
            strategy_gate_debug["passed_s8_1x2_00_with_api_pressure"] = int(
                s8_1x2_00_with_api_pressure_passed
            )
            strategy_gate_debug["s8_1x2_00_without_api_context_samples"] = s8_1x2_00_without_api_context_samples
            strategy_gate_debug["s8_1x2_00_no_pressure_samples"] = s8_1x2_00_no_pressure_samples
            strategy_gate_debug["s8_1x2_00_with_api_pressure_samples"] = s8_1x2_00_with_api_pressure_samples
        except Exception:
            pass
        if not candidates_to_ingest:
            _persist_side_funnel_once(
                stages={
                    "after_integrity": side_after_integrity,
                    "after_s8": side_after_s8,
                    "after_context_filter": side_after_context_filter,
                    "after_value_filter": {"home": 0, "away": 0, "draw": 0, "unknown": 0, "total": 0},
                    "after_scoring_all": {"home": 0, "away": 0, "draw": 0, "unknown": 0, "total": 0},
                    "after_scoring": {"home": 0, "away": 0, "draw": 0, "unknown": 0, "total": 0},
                    "final_selected": {"home": 0, "away": 0, "draw": 0, "unknown": 0, "total": 0},
                },
                samples=away_draw_samples,
            )
            diagnostics.update(
                final_signals_count=0,
                messages_sent_count=0,
                football_sent_count=0,
                last_delivery_reason=(
                    "blocked_s8_1x2_00_without_api_context"
                    if s8_1x2_00_without_api_context_rejected > 0
                    else "blocked_s8_1x2_00_no_pressure"
                ),
                note=(
                    "temporary safety rule blocked S8 1X2 at 0:0 without mapped API-Football context"
                    if s8_1x2_00_without_api_context_rejected > 0
                    else "temporary safety rule blocked S8 1X2 at 0:0 because pressure_score < 2"
                ),
            )
            _dbg_s8safe = compile_football_cycle_debug(
                fb_preview=fb_preview,
                fb_cvf=fb_cvf,
                fb_post_send=fb_post_send_saved,
                fb_post_integrity=fb_post_integrity_saved,
                enriched_scored=enriched,
                finalists=[],
                finalists_pre_session=[],
                min_score=float(settings.football_min_signal_score or 60.0),
                family_svc=FootballSignalSendFilterService(),
                send_filter_stats=send_filter_result.stats if send_filter_result else None,
                integrity_dropped_checks=list(integrity_result.dropped_checks),
                dry_run=dry_run,
                global_block=(
                    "blocked_s8_1x2_00_without_api_context"
                    if s8_1x2_00_without_api_context_rejected > 0
                    else "blocked_s8_1x2_00_no_pressure"
                ),
                min_score_base=float(settings.football_min_signal_score or 60.0),
                score_relief_note="s8_safety_gate",
                live_send_stats=strategy_gate_debug,
                finalist_send_meta={},
                single_relief_max_gap=float(single_gap_max),
                live_sanity_drop_by_eid=s8_1x2_00_drop_by_eid,
                live_sanity_drop_reasons=s8_1x2_00_drop_reasons,
            )
            return AutoSignalCycleResult(
                endpoint=fetch_res.endpoint,
                fetch_ok=True,
                preview_candidates=preview_candidates,
                preview_skipped_items=preview_skipped_items,
                created_signal_ids=[],
                created_signals_count=0,
                skipped_candidates_count=int(omitted_by_limit),
                notifications_sent_count=0,
                preview_only=False,
                message=(
                    "blocked_s8_1x2_00_without_api_context"
                    if s8_1x2_00_without_api_context_rejected > 0
                    else "blocked_s8_1x2_00_no_pressure"
                ),
                raw_events_count=raw_events_count,
                normalized_markets_count=normalized_markets_count,
                candidates_before_filter_count=len(candidates_before_filter),
                candidates_after_filter_count=len(filtered_candidates),
                runtime_paused=False,
                runtime_active_sports=active_sports,
                source_name=source_name,
                live_auth_status=live_auth_status,
                last_live_http_status=fetch_res.status_code,
                fallback_used=fallback_used,
                fallback_source_name=fallback_source_name,
                rejection_reason=(
                    "blocked_s8_1x2_00_without_api_context"
                    if s8_1x2_00_without_api_context_rejected > 0
                    else "blocked_s8_1x2_00_no_pressure"
                ),
                dry_run=dry_run,
                report_matches_found=report_matches_found,
                report_candidates=report_candidates,
                report_after_filter=report_after_filter,
                report_after_integrity=report_after_integrity,
                report_after_scoring=0,
                report_final_signal="НЕТ",
                report_rejection_code=(
                    "blocked_s8_1x2_00_without_api_context"
                    if s8_1x2_00_without_api_context_rejected > 0
                    else "blocked_s8_1x2_00_no_pressure"
                ),
                football_cycle_debug=_dbg_s8safe,
            )

        # --- VALUE FILTER (post context filter, pre scoring) ---
        # Minimal "value-only" constraints on S8 picks to avoid auto-favorites / no-edge 0:0.
        # Do NOT change scoring or architecture; this is an explicit gate.
        def _safe_float(v: object) -> float | None:
            if v is None or isinstance(v, bool):
                return None
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        rej_low_fav = 0
        rej_no_edge = 0
        passed_value = 0
        after_value: list[ProviderSignalCandidate] = []
        value_rejected_samples: list[dict[str, object]] = []
        value_rejected_limit = 6
        fmt_v = FootballBetFormatterService()

        for cand in candidates_to_ingest:
            odds_f = _safe_float(getattr(cand.market, "odds_value", None))
            minute_i, sh_i, sa_i = _live_ctx_from_candidate(cand)
            # If context missing, treat as no-edge (we don't want blind favorites)
            if odds_f is None or minute_i is None or sh_i is None or sa_i is None:
                rej_no_edge += 1
                if len(value_rejected_samples) < value_rejected_limit:
                    bet = fmt_v.format_bet(
                        market_type=cand.market.market_type,
                        market_label=cand.market.market_label,
                        selection=cand.market.selection,
                        home_team=cand.match.home_team,
                        away_team=cand.match.away_team,
                        section_name=cand.market.section_name,
                        subsection_name=cand.market.subsection_name,
                    )
                    value_rejected_samples.append(
                        {
                            "event_id": str(_football_event_id(cand) or ""),
                            "match": str(cand.match.match_name or ""),
                            "minute": minute_i,
                            "score": f"{sh_i}:{sa_i}" if sh_i is not None and sa_i is not None else None,
                            "odds": str(getattr(cand.market, "odds_value", None)),
                            "bet_text": bet.main_label + (f" ({bet.detail_label})" if bet.detail_label else ""),
                            "value_reject": "missing_live_context_or_odds",
                        }
                    )
                continue

            # B) Obvious favorites ban (any time)
            if odds_f < 1.45:
                rej_no_edge += 1
                if len(value_rejected_samples) < value_rejected_limit:
                    bet = fmt_v.format_bet(
                        market_type=cand.market.market_type,
                        market_label=cand.market.market_label,
                        selection=cand.market.selection,
                        home_team=cand.match.home_team,
                        away_team=cand.match.away_team,
                        section_name=cand.market.section_name,
                        subsection_name=cand.market.subsection_name,
                    )
                    value_rejected_samples.append(
                        {
                            "event_id": str(_football_event_id(cand) or ""),
                            "match": str(cand.match.match_name or ""),
                            "minute": minute_i,
                            "score": f"{sh_i}:{sa_i}",
                            "odds": odds_f,
                            "bet_text": bet.main_label + (f" ({bet.detail_label})" if bet.detail_label else ""),
                            "value_reject": "blocked_obvious_favorite_odds_lt_1_45",
                        }
                    )
                continue

            # A) Auto-favorites ban (0:0 early-mid)
            if (sh_i, sa_i) == (0, 0) and minute_i < 40 and odds_f < 1.55:
                rej_low_fav += 1
                if len(value_rejected_samples) < value_rejected_limit:
                    bet = fmt_v.format_bet(
                        market_type=cand.market.market_type,
                        market_label=cand.market.market_label,
                        selection=cand.market.selection,
                        home_team=cand.match.home_team,
                        away_team=cand.match.away_team,
                        section_name=cand.market.section_name,
                        subsection_name=cand.market.subsection_name,
                    )
                    value_rejected_samples.append(
                        {
                            "event_id": str(_football_event_id(cand) or ""),
                            "match": str(cand.match.match_name or ""),
                            "minute": minute_i,
                            "score": f"{sh_i}:{sa_i}",
                            "odds": odds_f,
                            "bet_text": bet.main_label + (f" ({bet.detail_label})" if bet.detail_label else ""),
                            "value_reject": "blocked_low_value_favorite",
                        }
                    )
                continue

            # C) Zero-information ban (0:0 very early with short odds)
            if (sh_i, sa_i) == (0, 0) and minute_i < 25 and odds_f < 2.00:
                rej_no_edge += 1
                if len(value_rejected_samples) < value_rejected_limit:
                    bet = fmt_v.format_bet(
                        market_type=cand.market.market_type,
                        market_label=cand.market.market_label,
                        selection=cand.market.selection,
                        home_team=cand.match.home_team,
                        away_team=cand.match.away_team,
                        section_name=cand.market.section_name,
                        subsection_name=cand.market.subsection_name,
                    )
                    value_rejected_samples.append(
                        {
                            "event_id": str(_football_event_id(cand) or ""),
                            "match": str(cand.match.match_name or ""),
                            "minute": minute_i,
                            "score": f"{sh_i}:{sa_i}",
                            "odds": odds_f,
                            "bet_text": bet.main_label + (f" ({bet.detail_label})" if bet.detail_label else ""),
                            "value_reject": "blocked_zero_info_00_odds_lt_2_00",
                        }
                    )
                continue

            # D) Keep only potential value
            allow = (1.70 <= odds_f <= 3.80) or (((sh_i, sa_i) == (0, 0)) and minute_i >= 25 and odds_f > 2.20)
            if not allow:
                rej_no_edge += 1
                if len(value_rejected_samples) < value_rejected_limit:
                    bet = fmt_v.format_bet(
                        market_type=cand.market.market_type,
                        market_label=cand.market.market_label,
                        selection=cand.market.selection,
                        home_team=cand.match.home_team,
                        away_team=cand.match.away_team,
                        section_name=cand.market.section_name,
                        subsection_name=cand.market.subsection_name,
                    )
                    value_rejected_samples.append(
                        {
                            "event_id": str(_football_event_id(cand) or ""),
                            "match": str(cand.match.match_name or ""),
                            "minute": minute_i,
                            "score": f"{sh_i}:{sa_i}",
                            "odds": odds_f,
                            "bet_text": bet.main_label + (f" ({bet.detail_label})" if bet.detail_label else ""),
                            "value_reject": "blocked_no_edge_outside_value_window",
                        }
                    )
                continue

            passed_value += 1
            after_value.append(cand)

        candidates_to_ingest = after_value
        diagnostics.update(
            football_live_rejected_value_low_favorite=int(rej_low_fav),
            football_live_rejected_value_no_edge=int(rej_no_edge),
            football_live_passed_value_filter=int(passed_value),
            football_live_cycle_after_value_filter=int(len(after_value)),
        )

        # If value filter wipes pool, return with proper diagnostics (keep pipeline intact).
        if not candidates_to_ingest:
            _persist_side_funnel_once(
                stages={
                    "after_integrity": side_after_integrity,
                    "after_s8": side_after_s8,
                    "after_context_filter": side_after_context_filter,
                    "after_value_filter": {"home": 0, "away": 0, "draw": 0, "unknown": 0, "total": 0},
                    "after_scoring_all": {"home": 0, "away": 0, "draw": 0, "unknown": 0, "total": 0},
                    "after_scoring": {"home": 0, "away": 0, "draw": 0, "unknown": 0, "total": 0},
                    "final_selected": {"home": 0, "away": 0, "draw": 0, "unknown": 0, "total": 0},
                },
                samples=away_draw_samples,
            )
            try:
                # Attach value filter samples into existing strategy_gate_debug for visibility in debug dumps
                strategy_gate_debug["value_filter_rejected_samples"] = value_rejected_samples
            except Exception:
                pass
            diagnostics.update(
                final_signals_count=0,
                messages_sent_count=0,
                football_sent_count=0,
                last_delivery_reason="blocked_value_filter",
                note="all strategy-approved candidates rejected by value filter",
            )
            _dbg_v = compile_football_cycle_debug(
                fb_preview=fb_preview,
                fb_cvf=fb_cvf,
                fb_post_send=fb_post_send_saved,
                fb_post_integrity=fb_post_integrity_saved,
                enriched_scored=enriched,
                finalists=[],
                finalists_pre_session=[],
                min_score=float(settings.football_min_signal_score or 60.0),
                family_svc=FootballSignalSendFilterService(),
                send_filter_stats=send_filter_result.stats if send_filter_result else None,
                integrity_dropped_checks=list(integrity_result.dropped_checks),
                dry_run=dry_run,
                global_block="blocked_value_filter",
                min_score_base=float(settings.football_min_signal_score or 60.0),
                score_relief_note="value_filter",
                live_send_stats=strategy_gate_debug,
                finalist_send_meta={},
                single_relief_max_gap=float(single_gap_max),
                live_sanity_drop_by_eid=s8_1x2_00_drop_by_eid,
                live_sanity_drop_reasons=s8_1x2_00_drop_reasons,
            )
            return AutoSignalCycleResult(
                endpoint=fetch_res.endpoint,
                fetch_ok=True,
                preview_candidates=preview_candidates,
                preview_skipped_items=preview_skipped_items,
                created_signal_ids=[],
                created_signals_count=0,
                skipped_candidates_count=int(omitted_by_limit),
                notifications_sent_count=0,
                preview_only=False,
                message="blocked_value_filter",
                raw_events_count=raw_events_count,
                normalized_markets_count=normalized_markets_count,
                candidates_before_filter_count=len(candidates_before_filter),
                candidates_after_filter_count=len(filtered_candidates),
                runtime_paused=False,
                runtime_active_sports=active_sports,
                source_name=source_name,
                live_auth_status=live_auth_status,
                last_live_http_status=fetch_res.status_code,
                fallback_used=fallback_used,
                fallback_source_name=fallback_source_name,
                rejection_reason="blocked_value_filter",
                dry_run=dry_run,
                report_matches_found=report_matches_found,
                report_candidates=report_candidates,
                report_after_filter=report_after_filter,
                report_after_integrity=report_after_integrity,
                report_after_scoring=0,
                report_final_signal="НЕТ",
                report_rejection_code="blocked_value_filter",
                football_cycle_debug=_dbg_v,
            )

        # --- Scoring (applied only to strategy-approved candidates) ---
        scoring_svc = FootballSignalScoringService()
        learning_helper = FootballLearningService()
        live_fields_seen = False
        enriched: list[ProviderSignalCandidate] = []
        enriched_scored: list[ProviderSignalCandidate] = []
        for idx, cand in enumerate(candidates_to_ingest):
            family = family_svc.get_market_family(cand)
            analytics = analytics_svc.build_snapshot(cand, market_family=family)
            if analytics.get("score_home") is not None or analytics.get("minute") is not None:
                live_fields_seen = True
            lf = learning_helper.multiplier_for_family(learning_multipliers, family) if learning_enabled else 1.0
            breakdown = scoring_svc.score(
                candidate=cand,
                analytics=analytics,
                market_family=family,
                learning_factor=lf,
            )
            base_decimal = scoring_svc.to_signal_score_decimal(breakdown)
            prev_fs = dict(cand.feature_snapshot_json or {})
            prev_expl = dict(cand.explanation_json or {})
            summary = [a.as_dict() for a in learning_aggregates[:20]] if learning_aggregates else []
            learning_payload: dict = {"enabled": learning_enabled, "family_multiplier": lf}
            if idx == 0 and summary:
                learning_payload["aggregates_top"] = summary
            if idx == 0:
                league_top = learning_helper.get_last_league_aggregates()
                if league_top:
                    learning_payload["by_league_top"] = league_top
            fs_out: dict = {
                **prev_fs,
                "football_scoring": breakdown.as_dict(),
                "football_learning": learning_payload,
            }
            if analytics_enabled:
                fs_out["football_analytics"] = analytics

            eff_decimal = base_decimal
            if (
                live_adaptive_enabled
                and live_adaptive_snapshot is not None
                and getattr(cand.match, "is_live", False)
                and cand.match.sport == SportType.FOOTBALL
            ):
                tag_keys, prev_meta = preview_live_adaptive_tag_keys(cand, analytics, family)
                eff_decimal, _adj_f, la_reasons, la_detail = apply_live_adaptive_adjustment(
                    base_signal_score=base_decimal,
                    tag_keys=tag_keys,
                    snapshot=live_adaptive_snapshot,
                )
                fs_out["football_live_adaptive_learning"] = {
                    "enabled": True,
                    "base_signal_score": float(base_decimal),
                    "learning_adjustment_total": la_detail.get("learning_adjustment_total"),
                    "learning_adjustment_reasons": la_reasons,
                    "effective_live_score": float(eff_decimal),
                    "preview_tag_keys": tag_keys,
                    "preview_meta": prev_meta,
                    "detail": la_detail,
                }
            else:
                fs_out["football_live_adaptive_learning"] = {"enabled": False}

            context_bonus = 0.0
            context_bonus_sources: list[dict[str, object]] = []
            ctx_eval = (
                fs_out.get("football_live_context_filter")
                if isinstance(fs_out.get("football_live_context_filter"), dict)
                else {}
            )
            if bool(ctx_eval.get("pressure_supportive")) and ctx_eval.get("source") == "api_football":
                context_bonus = 1.5
                context_bonus_sources.append(
                    {
                        "source": "api_football",
                        "bonus": context_bonus,
                        "reason": "pressure_supportive",
                        "fixture_id": ctx_eval.get("fixture_id"),
                    }
                )
            fs_out["football_external_context_bonus"] = {
                "total_bonus": round(context_bonus, 4),
                "sources": context_bonus_sources,
            }
            if context_bonus > 0:
                eff_decimal = Decimal(str(round(float(eff_decimal) + context_bonus, 4))).quantize(Decimal("0.0001"))

            expl_out = {
                **prev_expl,
                "football_scoring_reason_codes": breakdown.reason_codes,
            }
            new_cand = cand.model_copy(
                update={
                    "signal_score": eff_decimal,
                    "feature_snapshot_json": fs_out,
                    "explanation_json": expl_out,
                }
            )
            new_cand = _attach_live_context_participation(new_cand)
            enriched_scored.append(new_cand)
        candidates_to_ingest = enriched_scored
        enriched = enriched_scored
        side_after_scoring_all = _side_breakdown(list(enriched_scored))

        if live_adaptive_enabled and live_adaptive_snapshot is not None:
            diagnostics.update(
                football_live_adaptive_learning_json=snapshot_json_for_diagnostics(live_adaptive_snapshot)
            )

        scored_sorted = sorted(
            candidates_to_ingest,
            key=lambda c: float(c.signal_score or 0),
            reverse=True,
        )
        score_relief_note = "soft_sendable_live"
        scored_tuples: list[tuple[ProviderSignalCandidate, str, str | None]] = []
        for c in scored_sorted:
            tier, sub = classify_live_sendable_candidate(
                c, min_score_base, family_svc, single_relief_max_gap=single_gap_max
            )
            if tier != "reject":
                scored_tuples.append((c, tier, sub))

        rejected_total = 0
        for c in scored_sorted:
            t, _ = classify_live_sendable_candidate(
                c, min_score_base, family_svc, single_relief_max_gap=single_gap_max
            )
            if t == "reject":
                rejected_total += 1

        live_send_stats = {
            "normal_sendable": sum(1 for _, t, _ in scored_tuples if t == "normal"),
            "soft_sendable_total": sum(1 for _, t, _ in scored_tuples if t == "soft"),
            "soft_sendable_tight": sum(
                1 for _, t, s in scored_tuples if t == "soft" and s == "soft_sendable"
            ),
            "soft_sendable_relief_single": sum(
                1 for _, t, s in scored_tuples if t == "soft" and s == "soft_sendable_relief_single"
            ),
            "soft_sendable_dc": sum(
                1 for _, t, s in scored_tuples if t == "soft" and s == "soft_sendable_dc"
            ),
            "rejected_total": int(rejected_total),
        }
        # Preserve strategy gate telemetry for dry_run/debug.
        if strategy_gate_debug:
            try:
                live_send_stats.update({k: v for k, v in strategy_gate_debug.items() if k not in live_send_stats})
            except Exception:
                pass

        ordered = order_live_finalist_tuples(scored_tuples, min_score_base, family_svc)
        finalists_pre_session = [c for c, _, _ in ordered]
        side_after_scoring = _side_breakdown(list(finalists_pre_session))
        n_after_min_score = len(finalists_pre_session)
        session_dup_blocked = 0
        send_meta_final: dict[int, tuple[str, str | None]] = {}
        logger.info(
            "[FOOTBALL][LIVE_THRESHOLD] base=%s single_relief_max_gap=%s stats=%s",
            min_score_base,
            single_gap_max,
            live_send_stats,
        )
        for rank, c in enumerate(scored_sorted, start=1):
            fam = family_svc.get_market_family(c)
            is_corner_like = bool(family_svc.is_corner_market(c))
            fam_w = family_svc.family_priority_weight(fam)
            logger.info(
                "[FOOTBALL][SCORING] rank=%s match=%s market_type=%s family=%s corners=%s family_w=%.1f score=%s base=%s",
                rank,
                c.match.match_name,
                c.market.market_type,
                fam,
                str(is_corner_like).lower(),
                fam_w,
                float(c.signal_score or 0),
                min_score_base,
            )
        if dry_run:
            kept_fin = [c for c, _, _ in ordered]
            for c, t, s in ordered:
                send_meta_final[id(c)] = (t, s)
            finalists = _sort_finalists_main_market_first(kept_fin, family_svc)
        else:
            ls_fin = FootballLiveSessionService()
            kept_fin: list[ProviderSignalCandidate] = []
            batch_seen: set[str] = set()
            def _safe_int(v: object) -> int | None:
                if v is None:
                    return None
                try:
                    return int(v)
                except (TypeError, ValueError):
                    return None

            def _candidate_live_state_fingerprint(cand: ProviderSignalCandidate) -> str | None:
                fs = getattr(cand, "feature_snapshot_json", None) or {}
                if not isinstance(fs, dict):
                    fs = {}
                minute = _football_match_minute_from_candidate(cand)
                sh = _safe_int(fs.get("score_home"))
                sa = _safe_int(fs.get("score_away"))
                fa = fs.get("football_analytics")
                if isinstance(fa, dict):
                    if sh is None:
                        sh = _safe_int(fa.get("score_home"))
                    if sa is None:
                        sa = _safe_int(fa.get("score_away"))
                    if minute is None:
                        minute = _safe_int(fa.get("minute"))
                # Compact fingerprint: minute + score if present; otherwise minute only.
                if minute is None and sh is None and sa is None:
                    return None
                if sh is None or sa is None:
                    return f"m{minute}" if minute is not None else None
                if minute is None:
                    return f"s{sh}:{sa}"
                return f"m{minute}_s{sh}:{sa}"

            def _norm(s: str) -> str:
                return (s or "").strip().lower().replace("ё", "е")

            def _result_1x2_direction_key(cand: ProviderSignalCandidate) -> str | None:
                """Direction key used to block flip per event: result_1x2:{home|away|draw}."""
                try:
                    mt = _norm(str(cand.market.market_type or ""))
                    if mt not in {"1x2", "match_winner"}:
                        return None
                    sel = _norm(str(cand.market.selection or ""))
                    if not sel:
                        return None
                    home = _norm(str(cand.match.home_team or ""))
                    away = _norm(str(cand.match.away_team or ""))
                    tok = sel.replace("х", "x").replace(" ", "")
                    if tok in {"1", "p1", "п1", "home"}:
                        return "result_1x2:home"
                    if tok in {"2", "p2", "п2", "away"}:
                        return "result_1x2:away"
                    if tok in {"x", "draw", "н", "ничья"}:
                        return "result_1x2:draw"
                    # team-name match (provider often sets selection to team)
                    if home and (sel == home or home in sel or sel in home):
                        return "result_1x2:home"
                    if away and (sel == away or away in sel or sel in away):
                        return "result_1x2:away"
                    return None
                except Exception:
                    return None
            for c, t, s in ordered:
                ik = build_live_idea_key(c)
                fp = _candidate_live_state_fingerprint(c)
                blocked, breason = ls_fin.should_block_duplicate_idea(
                    ik,
                    min_repeat_minutes=10,
                    state_fingerprint=fp,
                )
                # Hard safety: do not allow flip (opposite side) on the same live event within one session.
                # This is applied only to classic 1X2 result markets.
                dkey = _result_1x2_direction_key(c)
                flip_blocked, flip_reason = ls_fin.should_block_event_direction_flip(
                    event_external_id=str(getattr(getattr(c, "match", None), "external_event_id", "") or "") or None,
                    direction_key=(dkey or ""),
                )
                if ik in batch_seen or blocked:
                    ls_fin.record_duplicate_idea_blocked(1)
                    logger.info(
                        "[FOOTBALL][SESSION_IDEA_DEDUP] blocked key=%s reason=%s fp=%s",
                        ik[:200],
                        breason,
                        (fp or "—"),
                    )
                    continue
                if dkey and flip_blocked:
                    ls_fin.record_duplicate_idea_blocked(1)
                    logger.info(
                        "[FOOTBALL][SESSION_IDEA_DEDUP] blocked_flip event_id=%s dkey=%s reason=%s",
                        str(getattr(getattr(c, "match", None), "external_event_id", "") or "")[:32],
                        dkey,
                        flip_reason,
                    )
                    continue
                batch_seen.add(ik)
                kept_fin.append(c)
                send_meta_final[id(c)] = (t, s)
            session_dup_blocked = max(0, n_after_min_score - len(kept_fin))
            finalists = _sort_finalists_main_market_first(kept_fin, family_svc)

        _bfs = len(finalists)
        finalists = [
            c
            for c in finalists
            if _assert_finalist_safe_for_live_send(c, min_score_base, family_svc)
        ]
        if _bfs != len(finalists):
            logger.warning(
                "[FOOTBALL][LIVE_SAFETY] dropped %s candidates failing recheck (floor / codes / not corner-exotic)",
                _bfs - len(finalists),
            )

        finalist_set = set(id(x) for x in finalists)
        for c in finalists:
            logger.info(
                "[FOOTBALL][SCORING] finalist match=%s score=%s selected=yes",
                c.match.match_name,
                float(c.signal_score or 0),
            )
        for c in scored_sorted:
            if id(c) not in finalist_set:
                logger.info(
                    "[FOOTBALL][SCORING] match=%s score=%s selected=no reason=below_min_score",
                    c.match.match_name,
                    float(c.signal_score or 0),
                )

        live_sanity_drop_by_eid: dict[str, str] = {}
        live_sanity_drop_reasons: dict[str, str] = {}
        final_live_gate_debug: dict[str, Any] = {}
        if finalists:
            from app.services.football_final_live_send_gate import apply_final_live_send_gate

            _n_fin_pre = len(finalists)
            finalists, final_live_gate_debug, live_sanity_drop_by_eid, live_sanity_drop_reasons = (
                apply_final_live_send_gate(finalists, family_svc)
            )
            logger.info(
                "[FOOTBALL][FINAL_LIVE_GATE] in=%s out=%s matches_skipped=%s",
                _n_fin_pre,
                len(finalists),
                int((final_live_gate_debug or {}).get("matches_skipped") or 0),
            )
            _br = None
            _b0 = None
            if live_sanity_drop_reasons:
                _e0 = next(iter(live_sanity_drop_reasons.keys()))
                _br = (live_sanity_drop_reasons.get(_e0) or "")[:500]
                _b0 = str(live_sanity_drop_by_eid.get(_e0) or "")
            diagnostics.update(
                football_live_sanity_blocked_last_cycle=int(len(live_sanity_drop_reasons)),
                football_live_sanity_last_blocker=_b0,
                football_live_sanity_last_best_rejected=_br,
            )
        else:
            diagnostics.update(
                football_live_sanity_blocked_last_cycle=0,
                football_live_sanity_last_blocker=None,
                football_live_sanity_last_best_rejected=None,
            )

        report_after_scoring = n_after_min_score
        side_final_selected = _side_breakdown(list(finalists))

        # Attach "passed but lost" away/draw candidates (if any) with score/selected.
        try:
            if len(away_draw_samples) < away_draw_samples_limit:
                finalist_ids = {id(x) for x in finalists}
                for c in finalists_pre_session:
                    if len(away_draw_samples) >= away_draw_samples_limit:
                        break
                    famx = family_svc.get_market_family(c)
                    if famx != "result" or _result_subtype_local(c) != "ft_1x2_candidate":
                        continue
                    sside = _side_from_selection_1x2(c)
                    if sside not in {"away", "draw"}:
                        continue
                    lc = (
                        (c.feature_snapshot_json or {}).get("football_analytics")
                        if isinstance(c.feature_snapshot_json, dict)
                        else None
                    )
                    minute0 = (lc.get("minute") if isinstance(lc, dict) else None)
                    sh0 = (lc.get("score_home") if isinstance(lc, dict) else None)
                    sa0 = (lc.get("score_away") if isinstance(lc, dict) else None)
                    bet = fmt.format_bet(
                        market_type=c.market.market_type,
                        market_label=c.market.market_label,
                        selection=c.market.selection,
                        home_team=c.match.home_team,
                        away_team=c.match.away_team,
                        section_name=c.market.section_name,
                        subsection_name=c.market.subsection_name,
                    )
                    away_draw_samples.append(
                        {
                            "event_id": str(_football_event_id(c) or ""),
                            "match": str(c.match.match_name or ""),
                            "minute": minute0,
                            "score": f"{sh0}:{sa0}" if sh0 is not None and sa0 is not None else None,
                            "market_type": str(c.market.market_type or ""),
                            "bet_text": bet.main_label + (f" ({bet.detail_label})" if bet.detail_label else ""),
                            "odds": (str(c.market.odds_value) if c.market.odds_value is not None else None),
                            "side": sside,
                            "fate_stage": "after_scoring",
                            "score": float(c.signal_score or 0),
                            "selected": ("yes" if id(c) in finalist_ids else "no"),
                        }
                    )
        except Exception:
            pass

        _persist_side_funnel_once(
            stages={
                "after_integrity": side_after_integrity,
                "after_s8": side_after_s8,
                "after_context_filter": side_after_context_filter,
                "after_value_filter": _side_breakdown(list(after_value)),
                "after_scoring_all": side_after_scoring_all,
                "after_scoring": side_after_scoring,
                "final_selected": side_final_selected,
            },
            samples=away_draw_samples,
        )

        combined_live_drop_by_eid = dict(s8_1x2_00_drop_by_eid)
        combined_live_drop_by_eid.update(live_sanity_drop_by_eid)
        combined_live_drop_reasons = dict(s8_1x2_00_drop_reasons)
        combined_live_drop_reasons.update(live_sanity_drop_reasons)
        cycle_dbg = compile_football_cycle_debug(
            fb_preview=fb_preview,
            fb_cvf=fb_cvf,
            fb_post_send=fb_post_send_saved,
            fb_post_integrity=fb_post_integrity_saved,
            enriched_scored=enriched,
            finalists=finalists,
            finalists_pre_session=finalists_pre_session,
            min_score=min_score_base,
            min_score_base=min_score_base,
            score_relief_note=score_relief_note,
            live_send_stats=live_send_stats,
            finalist_send_meta=send_meta_final,
            family_svc=FootballSignalSendFilterService(),
            send_filter_stats=send_filter_result.stats if send_filter_result else None,
            integrity_dropped_checks=list(integrity_result.dropped_checks),
            dry_run=dry_run,
            single_relief_max_gap=float(single_gap_max),
            live_sanity_drop_by_eid=combined_live_drop_by_eid,
            live_sanity_drop_reasons=combined_live_drop_reasons,
        )
        lq_live = cycle_dbg.get("live_quality_summary") or {}
        if isinstance(cycle_dbg, dict):
            cycle_dbg["session_idea_dedup_this_cycle"] = int(session_dup_blocked)
            cycle_dbg["final_live_send_gate"] = final_live_gate_debug or {}

        def _pick_bet_text(cand: ProviderSignalCandidate) -> str:
            from app.services.football_bet_formatter_service import FootballBetFormatterService

            pres = FootballBetFormatterService().format_bet(
                market_type=cand.market.market_type,
                market_label=cand.market.market_label,
                selection=cand.market.selection,
                home_team=cand.match.home_team,
                away_team=cand.match.away_team,
                section_name=cand.market.section_name,
                subsection_name=cand.market.subsection_name,
            )
            if pres.detail_label:
                return f"{pres.main_label} ({pres.detail_label})"
            return pres.main_label

        best = finalists[0] if finalists else None
        codes: list[str] = []
        human_rs: list[str] = []
        if best is not None:
            codes = list((best.explanation_json or {}).get("football_scoring_reason_codes") or [])
            human_rs = FootballSignalScoringService.humanize_reason_codes(codes)

        # Strategy gate stats (unique matches) — derived from cycle debug rows.
        s_total_matches = 0
        s1_matches = 0
        s2_matches = 0
        try:
            _ms = cycle_dbg.get("matches") or []
            if isinstance(_ms, list):
                eids = {}
                for r in _ms:
                    if not isinstance(r, dict):
                        continue
                    eid = str(r.get("event_id") or "").strip()
                    sid = str(r.get("strategy_id") or "").strip()
                    if not eid or not sid:
                        continue
                    if eid in eids:
                        continue
                    eids[eid] = sid
                s_total_matches = len(eids)
                s1_matches = sum(1 for _eid, sid in eids.items() if sid.startswith("S1_"))
                s2_matches = sum(1 for _eid, sid in eids.items() if sid.startswith("S2_"))
        except Exception:
            pass

        diagnostics.update(
            football_analytics_enabled=analytics_enabled,
            football_learning_enabled=learning_enabled,
            football_learning_families_tracked=len(learning_aggregates),
            football_live_fields_in_last_cycle=bool(live_fields_seen),
            football_injuries_data_available=False,
            football_line_movement_available=False,
            football_live_cycle_after_score=n_after_min_score,
            football_live_cycle_new_ideas_sendable=len(finalists),
            football_live_cycle_duplicate_ideas_blocked=session_dup_blocked,
            football_live_strategy_matches_last_cycle=int(s_total_matches),
            football_live_strategy_s1_matches_last_cycle=int(s1_matches),
            football_live_strategy_s2_matches_last_cycle=int(s2_matches),
            football_live_quality_fresh_matches=int(lq_live.get("fresh_live_matches") or 0),
            football_live_quality_strong_idea_matches=int(lq_live.get("matches_with_strong_idea") or 0),
            football_live_quality_no_sendable_matches=int(lq_live.get("matches_without_sendable") or 0),
            football_live_quality_main_blocker=str(lq_live.get("main_blocker_code") or "—"),
            football_live_quality_main_blocker_ru=str(lq_live.get("main_blocker_ru") or "—"),
            football_live_best_scores_distribution_hint=(
                ", ".join(str(x) for x in (lq_live.get("fresh_live_best_scores_distribution") or [])[:12])
                or "—"
            ),
            football_live_min_signal_score_base=float(lq_live.get("min_signal_score_base") or min_score_base),
            football_live_min_signal_score_effective=float(lq_live.get("min_signal_score_effective") or min_score_base),
            football_live_score_relief_note=str(lq_live.get("score_relief_note") or score_relief_note),
            football_live_quality_hint_ru=str(lq_live.get("football_live_quality_hint_ru") or "—"),
            football_live_normal_sendable_count=int((lq_live.get("live_send_stats") or {}).get("normal_sendable") or 0),
            football_live_soft_sendable_count=int((lq_live.get("live_send_stats") or {}).get("soft_sendable_total") or 0),
            football_live_soft_sendable_tight_count=int((lq_live.get("live_send_stats") or {}).get("soft_sendable_tight") or 0),
            football_live_soft_sendable_relief_single_count=int(
                (lq_live.get("live_send_stats") or {}).get("soft_sendable_relief_single") or 0
            ),
            football_live_rejected_at_send_gate=int(
                (lq_live.get("live_send_stats") or {}).get("rejected_total") or 0
            ),
        )

        try:
            logger.info(
                "[FOOTBALL][LIVE_SEND_CONVERSION] %s",
                json.dumps(
                    {
                        "live_send_stats": lq_live.get("live_send_stats") or {},
                        "dry_run": dry_run,
                        "bottleneck_code": lq_live.get("main_blocker_code"),
                    },
                    default=str,
                    ensure_ascii=False,
                )[:12000],
            )
        except Exception:
            pass

        if not finalists:
            diagnostics.update(
                final_signals_count=0,
                messages_sent_count=0,
                football_sent_count=0,
                last_delivery_reason="low_score",
                note="no candidate passed live send gate (normal or soft)",
            )
            return AutoSignalCycleResult(
                endpoint=fetch_res.endpoint,
                fetch_ok=True,
                preview_candidates=preview_candidates,
                preview_skipped_items=preview_skipped_items,
                created_signal_ids=[],
                created_signals_count=0,
                skipped_candidates_count=int(omitted_by_limit),
                notifications_sent_count=0,
                preview_only=False,
                message="dry_run_low_score" if dry_run else "low_score",
                raw_events_count=raw_events_count,
                normalized_markets_count=normalized_markets_count,
                candidates_before_filter_count=len(candidates_before_filter),
                candidates_after_filter_count=len(filtered_candidates),
                runtime_paused=False,
                runtime_active_sports=active_sports,
                source_name=source_name,
                live_auth_status=live_auth_status,
                last_live_http_status=fetch_res.status_code,
                fallback_used=fallback_used,
                fallback_source_name=fallback_source_name,
                rejection_reason="low_score",
                dry_run=dry_run,
                report_matches_found=report_matches_found,
                report_candidates=report_candidates,
                report_after_filter=report_after_filter,
                report_after_integrity=report_after_integrity,
                report_after_scoring=n_after_min_score,
                report_final_signal="НЕТ",
                report_rejection_code="low_score",
                report_selected_reason_codes=codes,
                report_human_reasons=human_rs,
                report_dedup_skipped=0,
                football_cycle_debug=cycle_dbg,
            )

        if dry_run:
            if best is not None:
                _wsm = _resolve_football_live_send_meta(best, ordered, send_meta_final) or ("normal", None)
                _m = "soft" if _wsm[0] == "soft" else "normal"
                _tr0 = _build_live_ingest_traces(
                    [best], min_score_base, FootballSignalSendFilterService(), send_meta_final
                )
                diagnostics.update(
                    football_last_cycle_send_mode=_m,
                    football_last_cycle_ingest_normal=1 if _m == "normal" else 0,
                    football_last_cycle_ingest_soft=1 if _m == "soft" else 0,
                    football_last_cycle_db_dedup_skipped=0,
                    football_last_cycle_sent_traces_json=(
                        json.dumps(_tr0, default=str, ensure_ascii=False)[:20000] if _tr0 else None
                    ),
                    football_live_post_selection_hint_ru=None,
                )
            else:
                diagnostics.update(
                    football_last_cycle_send_mode="none",
                    football_last_cycle_ingest_normal=0,
                    football_last_cycle_ingest_soft=0,
                    football_last_cycle_sent_traces_json=None,
                )
            diagnostics.update(
                final_signals_count=0,
                messages_sent_count=0,
                football_sent_count=0,
                last_delivery_reason="dry_run_no_channel",
                note="dry run: scoring path only, no DB / no channel",
            )
            return AutoSignalCycleResult(
                endpoint=fetch_res.endpoint,
                fetch_ok=True,
                preview_candidates=preview_candidates,
                preview_skipped_items=preview_skipped_items,
                created_signal_ids=[],
                created_signals_count=0,
                skipped_candidates_count=int(omitted_by_limit),
                notifications_sent_count=0,
                preview_only=False,
                message="dry_run_ok",
                raw_events_count=raw_events_count,
                normalized_markets_count=normalized_markets_count,
                candidates_before_filter_count=len(candidates_before_filter),
                candidates_after_filter_count=len(filtered_candidates),
                runtime_paused=False,
                runtime_active_sports=active_sports,
                source_name=source_name,
                live_auth_status=live_auth_status,
                last_live_http_status=fetch_res.status_code,
                fallback_used=fallback_used,
                fallback_source_name=fallback_source_name,
                rejection_reason=None,
                dry_run=True,
                report_matches_found=report_matches_found,
                report_candidates=report_candidates,
                report_after_filter=report_after_filter,
                report_after_integrity=report_after_integrity,
                report_after_scoring=report_after_scoring,
                report_final_signal="ДА",
                report_selected_match=best.match.match_name,
                report_selected_bet=_pick_bet_text(best),
                report_selected_odds=str(best.market.odds_value),
                report_selected_score=str(best.signal_score) if best.signal_score is not None else "",
                report_selected_reason_codes=codes,
                report_human_reasons=human_rs,
                report_dedup_skipped=0,
                football_cycle_debug=cycle_dbg,
            )

        candidates_to_ingest = [
            _attach_football_live_send_meta(
                c,
                _resolve_football_live_send_meta(c, ordered, send_meta_final) or ("normal", None),
            )
            for c in finalists
        ]
        relaxed_dedup = runtime_source_kind == "semi_live_manual"

        async with sessionmaker() as session:
            ingest_res = await IngestionService().ingest_candidates(
                session,
                candidates_to_ingest,
                dedup_exclude_notes=("fallback_json", "manual_json", "demo"),
                dedup_required_notes=(delivery_scope,),
                dedup_relaxed_semi_manual=relaxed_dedup,
                dedup_relaxed_minutes=int(settings.football_dedup_relaxed_interval_minutes or 30),
            )
            await session.commit()

        ls_ing = FootballLiveSessionService()
        for cr in ingest_res.created_from_candidates:
            try:
                fs = getattr(cr, "feature_snapshot_json", None) or {}
                if not isinstance(fs, dict):
                    fs = {}
                minute = _football_match_minute_from_candidate(cr)
                sh = fs.get("score_home")
                sa = fs.get("score_away")
                fa = fs.get("football_analytics")
                if isinstance(fa, dict):
                    if sh is None:
                        sh = fa.get("score_home")
                    if sa is None:
                        sa = fa.get("score_away")
                    if minute is None:
                        minute = fa.get("minute")
                fp = None
                try:
                    mv = int(minute) if minute is not None else None
                except (TypeError, ValueError):
                    mv = None
                try:
                    shv = int(sh) if sh is not None else None
                except (TypeError, ValueError):
                    shv = None
                try:
                    sav = int(sa) if sa is not None else None
                except (TypeError, ValueError):
                    sav = None
                if mv is not None or shv is not None or sav is not None:
                    if shv is not None and sav is not None and mv is not None:
                        fp = f"m{mv}_s{shv}:{sav}"
                    elif mv is not None and shv is not None and sav is not None:
                        fp = f"m{mv}_s{shv}:{sav}"
                    elif mv is not None:
                        fp = f"m{mv}"
                    elif shv is not None and sav is not None:
                        fp = f"s{shv}:{sav}"
                ls_ing.register_idea_sent_with_state(build_live_idea_key(cr), state_fingerprint=fp)
                # Register per-event direction to prevent flip within the same session.
                try:
                    dkey = _result_1x2_direction_key(cr)
                    if dkey:
                        ls_ing.register_event_direction_sent(
                            event_external_id=str(getattr(getattr(cr, "match", None), "external_event_id", "") or "") or None,
                            direction_key=dkey,
                        )
                except Exception:
                    pass
            except Exception:
                ls_ing.register_idea_sent(build_live_idea_key(cr))
        ls_ing.record_signals_created(len(ingest_res.created_signal_ids))

        created_context_flags = [_build_live_context_participation(cr) for cr in ingest_res.created_from_candidates]
        created_with_api_football = sum(1 for row in created_context_flags if bool(row.get("used_api_football")))
        created_without_api_football = max(0, len(created_context_flags) - created_with_api_football)
        created_with_sportmonks = sum(1 for row in created_context_flags if bool(row.get("used_sportmonks")))
        created_with_external_context = sum(
            1
            for row in created_context_flags
            if bool(row.get("used_api_football"))
            or bool(row.get("used_sportmonks"))
            or bool(row.get("used_openai_analysis"))
        )
        diag_prev = diagnostics.get_state()
        diagnostics.update(
            signals_with_api_football=int(diag_prev.get("signals_with_api_football") or 0) + created_with_api_football,
            signals_without_api_football=int(diag_prev.get("signals_without_api_football") or 0)
            + created_without_api_football,
            signals_with_sportmonks=int(diag_prev.get("signals_with_sportmonks") or 0) + created_with_sportmonks,
            signals_with_external_context=int(diag_prev.get("signals_with_external_context") or 0)
            + created_with_external_context,
        )

        notifications_sent_count = 0
        per_signal_notified: dict[int, bool] = {}
        orch = OrchestrationService()
        logger.info("[FOOTBALL] final signals: %s", ingest_res.created_signals)
        for signal_id in ingest_res.created_signal_ids:
            try:
                async with sessionmaker() as session2:
                    sent = await orch.notify_signal_if_configured(session2, bot, signal_id)
                per_signal_notified[int(signal_id)] = bool(sent)
                if sent:
                    notifications_sent_count += 1
                    FootballLiveSessionService().record_telegram_message_sent(1)
            except Exception:
                logger.exception("Auto signal notification failed for signal_id=%s", signal_id)
                per_signal_notified[int(signal_id)] = False
        logger.info("[FOOTBALL] messages sent: %s", notifications_sent_count)
        if notifications_sent_count:
            diagnostics.update(football_live_last_notify_path="NotificationService.send_signal_notification")
        elif ingest_res.created_signal_ids:
            diagnostics.update(football_live_last_notify_path="notify_skipped_see_orchestration_logs")
        else:
            diagnostics.update(football_live_last_notify_path=None)

        _trace_rows = await _combat_e2e_delivery_rows(
            sessionmaker,
            settings,
            delivery_scope=delivery_scope,
            relaxed_dedup=relaxed_dedup,
            dedup_relaxed_minutes=int(settings.football_dedup_relaxed_interval_minutes or 30),
            candidates_to_ingest=candidates_to_ingest,
            ingest_res=ingest_res,
            per_signal_notified=per_signal_notified,
            runtime_paused=runtime.is_paused(),
        )
        _n_sent = sum(1 for r in _trace_rows if r.get("final_outcome") == "sent")
        _n_db = sum(1 for r in _trace_rows if r.get("final_outcome") == "blocked_db_dedup")
        _combat_sum = (
            f"rows={len(_trace_rows)} created_in_db={ingest_res.created_signals} "
            f"telegram_ok={_n_sent} db_dedup_row={_n_db} batch_skipped={ingest_res.skipped_candidates}"
        )
        if isinstance(cycle_dbg, dict):
            cycle_dbg["combat_delivery_trace"] = _trace_rows
            _fg0 = cycle_dbg.get("final_live_send_gate")
            if isinstance(_fg0, dict):
                _enrich_final_live_gate_with_delivery(_fg0, _trace_rows)
        _tj = json.dumps(_trace_rows, default=str, ensure_ascii=False)[:60000]
        SignalRuntimeDiagnosticsService().update(
            football_live_combat_delivery_trace_json=_tj,
            football_live_combat_delivery_last_summary=_combat_sum,
        )
        diagnostics.update(
            football_live_combat_delivery_last_summary=_combat_sum,
        )
        try:
            logger.info("[FOOTBALL][COMBAT_E2E] %s", _tj[:24000])
        except Exception:
            pass

        if ingest_res.created_signals == 0 and finalists and int(ingest_res.skipped_candidates) > 0:
            _db_sk = int(ingest_res.skipped_candidates)
            logger.info(
                "[FOOTBALL][LIVE_SEND_TRACE] all candidates blocked at DB dedup skipped=%s",
                _db_sk,
            )
            diagnostics.update(
                final_signals_count=0,
                messages_sent_count=0,
                football_sent_count=0,
                last_delivery_reason="blocked_by_dedup",
                note="candidates passed scoring but dedup skipped all",
                football_last_cycle_ingest_normal=0,
                football_last_cycle_ingest_soft=0,
                football_last_cycle_send_mode="none",
                football_last_cycle_db_dedup_skipped=_db_sk,
                football_last_cycle_sent_traces_json=None,
                football_live_post_selection_hint_ru=_post_selection_bottleneck_ru(
                    session_dup_blocked=0, db_dedup_skipped=_db_sk, created_n=0
                ),
            )
            return AutoSignalCycleResult(
                endpoint=fetch_res.endpoint,
                fetch_ok=True,
                preview_candidates=preview_candidates,
                preview_skipped_items=preview_skipped_items,
                created_signal_ids=[],
                created_signals_count=0,
                skipped_candidates_count=int(ingest_res.skipped_candidates + omitted_by_limit),
                notifications_sent_count=0,
                preview_only=False,
                message="dedup_blocked",
                raw_events_count=raw_events_count,
                normalized_markets_count=normalized_markets_count,
                candidates_before_filter_count=len(candidates_before_filter),
                candidates_after_filter_count=len(filtered_candidates),
                runtime_paused=False,
                runtime_active_sports=active_sports,
                source_name=source_name,
                live_auth_status=live_auth_status,
                last_live_http_status=fetch_res.status_code,
                fallback_used=fallback_used,
                fallback_source_name=fallback_source_name,
                rejection_reason="blocked_by_dedup",
                dry_run=False,
                report_matches_found=report_matches_found,
                report_candidates=report_candidates,
                report_after_filter=report_after_filter,
                report_after_integrity=report_after_integrity,
                report_after_scoring=report_after_scoring,
                report_final_signal="НЕТ",
                report_rejection_code="blocked_by_dedup",
                report_selected_match=best.match.match_name,
                report_selected_bet=_pick_bet_text(best),
                report_selected_odds=str(best.market.odds_value),
                report_selected_score=str(best.signal_score) if best.signal_score is not None else "",
                report_selected_reason_codes=codes,
                report_human_reasons=human_rs,
                report_dedup_skipped=int(ingest_res.skipped_candidates),
                football_cycle_debug=cycle_dbg,
            )

        diagnostics.update(
            final_signals_count=int(ingest_res.created_signals),
            messages_sent_count=notifications_sent_count,
            football_sent_count=notifications_sent_count,
            last_delivery_reason=(
                None
                if notifications_sent_count
                else ("duplicate_in_db_or_no_new_signals" if post_integrity_count > 0 else "no_created_signals")
            ),
            note=None if ingest_res.created_signals else "no created football signals",
        )

        n_ing = int(ingest_res.created_signals)
        n_norm_ing = sum(
            1
            for c in ingest_res.created_from_candidates
            if send_meta_final.get(id(c), ("normal", None))[0] == "normal"
        )
        n_soft_ing = n_ing - n_norm_ing
        if n_ing and n_norm_ing and n_soft_ing:
            mmode = "mixed"
        elif n_soft_ing and not n_norm_ing and n_ing:
            mmode = "soft"
        elif n_norm_ing and not n_soft_ing and n_ing:
            mmode = "normal"
        else:
            mmode = "none"
        tr_list = _build_live_ingest_traces(
            list(ingest_res.created_from_candidates), min_score_base, family_svc, send_meta_final
        )
        for row in tr_list:
            try:
                logger.info("[FOOTBALL][LIVE_SEND_TRACE] %s", json.dumps(row, default=str, ensure_ascii=False)[:2000])
            except Exception:
                pass
        try:
            logger.info(
                "[FOOTBALL][LIVE_SEND_CONVERSION] ingest_mode=%s normal_ingest=%s soft_ingest=%s db_dedup_skipped=%s",
                mmode,
                n_norm_ing,
                n_soft_ing,
                int(ingest_res.skipped_candidates),
            )
        except Exception:
            pass
        diagnostics.update(
            football_last_cycle_ingest_normal=n_norm_ing,
            football_last_cycle_ingest_soft=n_soft_ing,
            football_last_cycle_send_mode=mmode,
            football_last_cycle_db_dedup_skipped=int(ingest_res.skipped_candidates),
            football_last_cycle_sent_traces_json=(
                json.dumps(tr_list, default=str, ensure_ascii=False)[:20000] if tr_list else None
            ),
            football_live_post_selection_hint_ru=None,
        )

        return AutoSignalCycleResult(
            endpoint=fetch_res.endpoint,
            fetch_ok=True,
            preview_candidates=preview_candidates,
            preview_skipped_items=preview_skipped_items,
            created_signal_ids=list(ingest_res.created_signal_ids),
            created_signals_count=int(ingest_res.created_signals),
            skipped_candidates_count=int(ingest_res.skipped_candidates + omitted_by_limit),
            notifications_sent_count=notifications_sent_count,
            preview_only=False,
            message="ok",
            raw_events_count=raw_events_count,
            normalized_markets_count=normalized_markets_count,
            candidates_before_filter_count=len(candidates_before_filter),
            candidates_after_filter_count=len(filtered_candidates),
            runtime_paused=False,
            runtime_active_sports=active_sports,
            source_name=source_name,
            live_auth_status=live_auth_status,
            last_live_http_status=fetch_res.status_code,
            fallback_used=fallback_used,
            fallback_source_name=fallback_source_name,
            dry_run=False,
            report_matches_found=report_matches_found,
            report_candidates=report_candidates,
            report_after_filter=report_after_filter,
            report_after_integrity=report_after_integrity,
            report_after_scoring=report_after_scoring,
            report_final_signal="ДА" if ingest_res.created_signals else "НЕТ",
            report_selected_match=best.match.match_name if best else None,
            report_selected_bet=_pick_bet_text(best) if best else None,
            report_selected_odds=str(best.market.odds_value) if best else None,
            report_selected_score=str(best.signal_score) if best and best.signal_score is not None else None,
            report_selected_reason_codes=codes,
            report_human_reasons=human_rs,
            report_dedup_skipped=int(ingest_res.skipped_candidates),
            football_cycle_debug=cycle_dbg,
        )

    def log_football_cycle_trace(self, res: AutoSignalCycleResult) -> None:
        diag = SignalRuntimeDiagnosticsService().get_state()
        bn = _infer_football_live_cycle_bottleneck(res, diag)
        SignalRuntimeDiagnosticsService().update(football_live_cycle_bottleneck=bn)
        _apply_last_combat_cycle_diagnostics(res)
        _football_log_live_session_report(res=res, diag=SignalRuntimeDiagnosticsService().get_state())
        try:
            snap = FootballLiveSessionService().snapshot()
            pers = bool(snap.persistent) if snap.active else True
            dbg_txt = format_football_session_start_debug_message(res, persistent=pers)
            if res.dry_run:
                SignalRuntimeDiagnosticsService().update(
                    football_live_last_dry_run_debug_telegram_text=dbg_txt[:12000]
                )
            else:
                # Keep combat/debug stable: never overwrite combat debug with a dry_run snapshot.
                SignalRuntimeDiagnosticsService().update(
                    football_live_last_cycle_debug_telegram_text=dbg_txt[:12000],
                    football_live_last_combat_debug_telegram_text=dbg_txt[:12000],
                )
        except Exception:
            logger.debug("football live debug telegram text cache failed", exc_info=True)

    def update_football_live_session_diagnostics_with_pacing(
        self, cres: AutoSignalCycleResult, *, cycle_wall_seconds: float
    ) -> None:
        """Session snapshot + adaptive pacing fields (same rules as background live loop)."""
        settings = get_settings()
        pacing = get_football_live_runtime_pacing()
        sess = FootballLiveSessionService()
        snap = sess.snapshot()
        rem = sess.remaining_seconds()
        diag_fn = SignalRuntimeDiagnosticsService().update
        diag_fn(
            football_live_last_cycle_wall_seconds=float(cycle_wall_seconds),
            football_live_last_cycle_fetch_ok=bool(cres.fetch_ok),
            football_live_last_cycle_created_signals=int(cres.created_signals_count or 0),
        )
        dcur = SignalRuntimeDiagnosticsService().get_state()
        p_snap = build_football_live_pacing_cycle_snapshot(
            dcur, cycle_wall_seconds=float(cycle_wall_seconds)
        )
        _next_iv, pacing_updates = pacing.compute_sleep_seconds(settings, p_snap)
        diag_fn(
            football_live_session_active=snap.active,
            football_live_session_started_at=(
                snap.started_at.isoformat() if snap.started_at else None
            ),
            football_live_session_expires_at=(
                snap.expires_at.isoformat() if snap.expires_at else None
            ),
            football_live_session_persistent=bool(snap.persistent),
            football_live_session_last_cycle_at=(
                snap.last_cycle_at.isoformat() if snap.last_cycle_at else None
            ),
            football_live_session_remaining_minutes=(
                (rem / 60.0) if rem is not None else None
            ),
            football_live_signals_sent_session=snap.signals_sent_in_session,
            football_live_telegram_sent_session=snap.telegram_messages_sent_in_session,
            football_live_duplicate_ideas_blocked=snap.duplicate_ideas_blocked_session,
            football_live_sent_ideas_count=snap.sent_idea_keys_count,
            **pacing_updates,
        )

    async def run_football_live_forever(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        bot: Bot,
    ) -> None:
        """Фоновые циклы пока активна football live-сессия (после ▶️ Старт); пауза между циклами — adaptive pacing."""
        settings = get_settings()
        idle_sleep = max(45, min(180, int(settings.auto_signal_polling_interval_seconds or 60)))
        logger.info("[FOOTBALL][LIVE_LOOP] started idle_sleep=%ss", idle_sleep)
        while True:
            sleep_time = idle_sleep
            try:
                sess = FootballLiveSessionService()
                sess.expire_if_needed()
                diag_fn = SignalRuntimeDiagnosticsService().update
                snap0 = sess.snapshot()
                active0 = bool(sess.is_active())
                logger.info(
                    "[FOOTBALL][LIVE_LOOP] tick session_active=%s persistent=%s started_at=%s last_cycle_at=%s",
                    str(active0).lower(),
                    str(bool(getattr(snap0, "persistent", False))).lower(),
                    (snap0.started_at.isoformat() if snap0.started_at else None),
                    (snap0.last_cycle_at.isoformat() if snap0.last_cycle_at else None),
                )
                if active0:
                    t_cycle = time.perf_counter()
                    cres = await self.run_single_cycle(sessionmaker, bot, dry_run=False)
                    cycle_wall = float(time.perf_counter() - t_cycle)
                    self.update_football_live_session_diagnostics_with_pacing(
                        cres, cycle_wall_seconds=cycle_wall
                    )
                    snap = sess.snapshot()
                    rem = sess.remaining_seconds()
                    pacing_updates = SignalRuntimeDiagnosticsService().get_state()
                    next_iv = float(
                        pacing_updates.get("football_live_pacing_current_interval_seconds") or idle_sleep
                    )
                    bn = _infer_football_live_cycle_bottleneck(
                        cres, SignalRuntimeDiagnosticsService().get_state()
                    )
                    SignalRuntimeDiagnosticsService().update(
                        football_live_cycle_bottleneck=bn,
                        football_live_cycle_bottleneck_ru=_combat_bottleneck_ru(bn),
                    )
                    _apply_last_combat_cycle_diagnostics(cres)
                    _football_log_live_session_report(
                        res=cres, diag=SignalRuntimeDiagnosticsService().get_state()
                    )
                    logger.info(
                        "[FOOTBALL][LIVE_LOOP] cycle done wall=%.2fs next_sleep=%.1fs fetch_s=%s avg_fetch=%s "
                        "backoff=%s signals_sent_session=%s telegram_sent=%s dup_blocked=%s ideas=%s remaining_min=%s bottleneck=%s reason=%s",
                        cycle_wall,
                        next_iv,
                        str(pacing_updates.get("football_live_pacing_last_fetch_seconds")),
                        str(pacing_updates.get("football_live_pacing_avg_fetch_seconds")),
                        str(pacing_updates.get("football_live_pacing_backoff_level")),
                        snap.signals_sent_in_session,
                        snap.telegram_messages_sent_in_session,
                        snap.duplicate_ideas_blocked_session,
                        snap.sent_idea_keys_count,
                        round((rem or 0) / 60.0, 2) if rem is not None else None,
                        bn,
                        str(pacing_updates.get("football_live_pacing_last_reason_ru") or "")[:240],
                    )
                    sleep_time = float(next_iv)
                else:
                    snap_idle = sess.snapshot()
                    diag_fn(
                        football_live_session_active=False,
                        football_live_session_started_at=(
                            snap_idle.started_at.isoformat() if snap_idle.started_at else None
                        ),
                        football_live_session_expires_at=None,
                        football_live_session_persistent=False,
                        football_live_session_last_cycle_at=(
                            snap_idle.last_cycle_at.isoformat() if snap_idle.last_cycle_at else None
                        ),
                        football_live_session_remaining_minutes=None,
                        football_live_signals_sent_session=snap_idle.signals_sent_in_session,
                        football_live_telegram_sent_session=snap_idle.telegram_messages_sent_in_session,
                        football_live_duplicate_ideas_blocked=snap_idle.duplicate_ideas_blocked_session,
                        football_live_sent_ideas_count=snap_idle.sent_idea_keys_count,
                    )
                    sleep_time = idle_sleep
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[FOOTBALL][LIVE_LOOP][ERROR] football live session loop failed")
                sleep_time = idle_sleep
            await asyncio.sleep(sleep_time)

    async def run_forever(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        bot: Bot,
    ) -> None:
        await self.run_football_live_forever(sessionmaker, bot)

    def _build_provider_client_config(self, settings: Settings) -> ProviderClientConfig | None:
        base_url = self._clean_optional_str(settings.odds_provider_base_url)
        if not base_url:
            return None
        return ProviderClientConfig(
            base_url=base_url,
            api_key=self._clean_optional_str(settings.odds_provider_api_key),
            sport=self._clean_optional_str(settings.odds_provider_sport),
            regions=self._clean_optional_str(settings.odds_provider_regions),
            markets=self._clean_optional_str(settings.odds_provider_markets),
            bookmakers=self._clean_optional_str(settings.odds_provider_bookmakers),
            odds_format=self._clean_optional_str(settings.odds_provider_odds_format),
            date_format=self._clean_optional_str(settings.odds_provider_date_format),
            timeout_seconds=int(settings.odds_provider_timeout_seconds),
        )

    def _filter_football_live_only(self, candidates: list[ProviderSignalCandidate]) -> list[ProviderSignalCandidate]:
        """Оставляет только футбол с is_live=True (без prematch)."""
        out: list[ProviderSignalCandidate] = []
        for candidate in candidates:
            match = getattr(candidate, "match", None)
            if getattr(match, "sport", None) != SportType.FOOTBALL:
                continue
            if not bool(getattr(match, "is_live", False)):
                continue
            out.append(candidate)
        return out

    def _filter_candidates_by_runtime(self, candidates, runtime: SignalRuntimeSettingsService):
        accepted = []
        for candidate in candidates:
            sport = getattr(getattr(candidate, "match", None), "sport", None)
            if sport is None:
                continue
            if runtime.is_sport_enabled(sport):
                accepted.append(candidate)
            else:
                logger.info(
                    "[FOOTBALL] candidate skipped by runtime sport filter: sport=%s event=%s match=%s",
                    getattr(sport, "value", sport),
                    getattr(getattr(candidate, "match", None), "external_event_id", None),
                    getattr(getattr(candidate, "match", None), "match_name", None),
                )
        return accepted

    def _infer_provider_sport(self, config: ProviderClientConfig | None):
        if config is None:
            return None
        joined = " ".join(
            [
                str(config.base_url or ""),
                str(config.sport or ""),
            ]
        ).lower()
        if any(token in joined for token in ("soccer", "football", "epl")):
            from app.core.enums import SportType

            return SportType.FOOTBALL
        if any(token in joined for token in ("counterstrike", "counter_strike", "cs2", "cs_")):
            from app.core.enums import SportType

            return SportType.CS2
        if any(token in joined for token in ("dota2", "dota 2", "dota")):
            from app.core.enums import SportType

            return SportType.DOTA2
        return None

    def _detect_provider_name(self, settings: Settings) -> str:
        base = str(settings.odds_provider_base_url or "").lower()
        if "the-odds-api" in base:
            return "the_odds_api"
        return "odds_http"

    def _build_manual_football_fallback_preview(self):
        svc = WinlineManualCycleService()
        source_truth = WinlineManualPayloadService().get_line_source_truth()
        raw, err = svc._manual.load_line_payload()
        if raw is None or err:
            return None
        normalized, nerr = svc._normalize_line_or_error(raw)
        if normalized is None or nerr:
            return None
        preview = AdapterIngestionService().preview_payload(normalized)
        football_candidates = [
            c
            for c in preview.candidates
            if getattr(getattr(c, "match", None), "sport", None) == SportType.FOOTBALL
        ]
        football_candidates = [
            c.model_copy(
                update={
                    "notes": "fallback_json",
                    "feature_snapshot_json": {
                        **(c.feature_snapshot_json or {}),
                        "runtime_source_kind": "fallback_json",
                        "runtime_primary_source": "the_odds_api",
                    },
                }
            )
            for c in football_candidates
        ]
        preview = preview.model_copy(
            update={
                "total_events": len(normalized.get("events") or []),
                "total_markets": len(normalized.get("markets") or []),
                "created_candidates": len(football_candidates),
                "candidates": football_candidates,
            }
        )
        return {
            "payload": normalized,
            "preview": preview,
            "source_mode": str(source_truth.get("source_mode") or "manual_example"),
            "is_real_source": bool(source_truth.get("is_real_source", False)),
            "source_reason": str(source_truth.get("reason") or "manual payload"),
            "source_origin": str(source_truth.get("source_origin") or "manual payload"),
            "provenance_present": bool(source_truth.get("provenance_present", False)),
            "uploaded_at": source_truth.get("uploaded_at"),
            "file_path": source_truth.get("file_path"),
            "checksum": source_truth.get("checksum"),
        }

    def _resolve_zero_candidate_reason(
        self,
        *,
        preview_candidates: int,
        runtime_candidates_count: int,
        filtered_accepted_count: int,
        deduped_count: int,
        filter_rejections: dict[str, int],
    ) -> str:
        if preview_candidates == 0:
            return "no football events in payload"
        if runtime_candidates_count == 0:
            return "filtered by runtime sport settings"
        if filtered_accepted_count == 0:
            if filter_rejections:
                parts = ", ".join(f"{k}={v}" for k, v in sorted(filter_rejections.items()))
                return f"filtered by candidate rules: {parts}"
            return "filtered by candidate rules"
        if deduped_count == 0:
            return "deduplicated to zero candidates"
        return "unknown_zero_candidate_reason"

    def _log_candidates_per_match(self, candidates) -> None:
        counts: dict[tuple[str, str], int] = {}
        for candidate in candidates:
            match = getattr(candidate, "match", None)
            event_id = str(getattr(match, "external_event_id", "") or "—")
            match_name = str(getattr(match, "match_name", "") or "—")
            key = (event_id, match_name)
            counts[key] = counts.get(key, 0) + 1
        logger.info("[FOOTBALL] candidates per match:")
        for (event_id, match_name), count in sorted(counts.items()):
            logger.info("- event_id=%s, match=%s, count=%s", event_id, match_name, count)

    def _log_final_candidates(self, candidates) -> None:
        for candidate in candidates:
            match = getattr(candidate, "match", None)
            market = getattr(candidate, "market", None)
            logger.info(
                "- match=%s, market=%s, odds=%s, family=%s",
                getattr(match, "match_name", "—"),
                getattr(market, "market_label", "—"),
                getattr(market, "odds_value", "—"),
                FootballSignalSendFilterService().get_market_family(candidate),
            )

