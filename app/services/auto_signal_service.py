from __future__ import annotations

import asyncio
import json
import logging
from collections import Counter
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from urllib.parse import parse_qsl, urlparse

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
from app.services.football_live_session_service import FootballLiveSessionService, build_live_idea_key
from app.services.football_analytics_service import FootballAnalyticsService
from app.services.football_learning_service import FootballLearningService
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


logger = logging.getLogger(__name__)


def _football_event_id(candidate: ProviderSignalCandidate) -> str:
    return str(getattr(getattr(candidate, "match", None), "external_event_id", "") or "")


def _football_only(candidates: list[ProviderSignalCandidate]) -> list[ProviderSignalCandidate]:
    return [c for c in candidates if getattr(getattr(c, "match", None), "sport", None) == SportType.FOOTBALL]


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
    sc = float(c.signal_score or 0)
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
    return c.model_copy(update={"explanation_json": expl})


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
    return None


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
) -> dict:
    """Aggregated per-match football pipeline diagnostics for dry_run Telegram + logs."""
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
            final_status = "blocked_unknown"
        elif n_send == 0:
            if send_fail_by_eid.get(eid) == "too_far_in_time":
                final_status = "blocked_too_far_in_time"
            else:
                final_status = "blocked_send_filter"
        elif n_int == 0:
            final_status = "blocked_integrity"
        elif n_int > 0 and best_c is None:
            final_status = "blocked_unknown"
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
            why_code, why_ru = "no_scoring", "Нет данных scoring по матчу"
        elif n_final > 0:
            why_code, why_ru = "sendable", "Готова к рассмотрению на отправку"
        elif eid in pre_session_eids and eid not in finalists_post_eids:
            why_code, why_ru = "duplicate_idea", "Та же идея по матчу уже отправлялась в этой сессии"
        elif best_sc < base_for_display:
            why_code, why_ru = "low_score", f"Score {best_sc:.1f} ниже порога {base_for_display:.0f}"
        else:
            why_ru = "См. final_status"

        gap_to_sendable = None
        if best_c is not None and final_status == "blocked_low_score":
            gap_to_sendable = round(float(base_for_display) - float(best_sc), 2)

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
                "candidates_after_score_threshold": n_final,
                "best_market_family": best_market_family,
                "best_candidate_market": best_market,
                "best_bet_text": best_bet_text,
                "best_candidate_odds": best_odds,
                "best_candidate_score": round(best_sc, 2) if best_c is not None else None,
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
        forced = "blocked_non_real_source" if ("non_live" in gb or "non_real" in gb) else "blocked_unknown"
        for r in rows:
            r["final_status"] = forced

    status_counts = Counter(str(r["final_status"]) for r in rows)
    fresh_accepted = [
        r
        for r in rows
        if int(r.get("candidates_after_freshness") or 0) > 0 and bool(r.get("is_live"))
    ]
    status_counts_fresh = Counter(str(r["final_status"]) for r in fresh_accepted) if fresh_accepted else Counter()
    fresh_live_send_breakdown = {
        "blocked_send_filter": int(status_counts_fresh.get("blocked_send_filter", 0)),
        "blocked_integrity": int(status_counts_fresh.get("blocked_integrity", 0)),
        "blocked_low_score": int(status_counts_fresh.get("blocked_low_score", 0)),
        "blocked_duplicate_idea": int(status_counts_fresh.get("blocked_duplicate_idea", 0)),
        "blocked_dedup_db": int(db_dedup_blocked_count),
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
        "blocked_duplicate_idea",
        "blocked_low_score",
        "blocked_send_filter",
        "blocked_integrity",
        "blocked_too_far_in_time",
    ]
    main_blocker_status = "unknown"
    if fresh_accepted:
        if not problem_status_rows_fresh:
            main_blocker_status = "none"
        else:
            for token in blocker_priority:
                if any(str(r.get("final_status")) == token for r in problem_status_rows_fresh):
                    main_blocker_status = token
                    break
    elif rows:
        main_blocker_status = bottleneck_hint or "unknown"
        if not problem_status_rows:
            main_blocker_status = "none"
        else:
            for token in blocker_priority:
                if any(str(r.get("final_status")) == token for r in problem_status_rows):
                    main_blocker_status = token
                    break

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
            "blocked_low_score": "порог score + мягкий live send-gate",
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
        "blocked_low_score": ("score", "порог score"),
        "blocked_duplicate_idea": ("duplicate_idea", "повтор идеи в сессии"),
        "blocked_send_filter": ("send_filter", "фильтр отправки"),
        "blocked_integrity": ("integrity", "проверка целостности"),
        "blocked_too_far_in_time": ("time", "слишком далеко по времени"),
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
        "blocked_low_score": "score чуть ниже порога",
        "blocked_duplicate_idea": "идея уже отправлялась в сессии",
        "blocked_send_filter": "рынок отсеян фильтром отправки",
        "blocked_integrity": "рынок не прошёл integrity",
        "blocked_too_far_in_time": "слишком далеко по времени",
        "blocked_unknown": "причина не классифицирована",
    }
    non_sel_fresh = [r for r in fresh_accepted if str(r.get("final_status")) != "selected"]
    _cnt_nf = Counter(str(r.get("final_status")) for r in non_sel_fresh)
    _why_order = (
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
        "score": "Сигналы есть, но лучшим live-идеям не хватает score до порога",
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
    }
    return m.get(str(token), str(token).replace("_", " "))


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
                    note="Нажмите ▶️ Старт для запуска 15-минутной live-сессии",
                    football_live_cycle_bottleneck="blocked_no_live_session",
                    football_live_cycle_candidates_before_filter=0,
                    football_live_cycle_after_send_filter=0,
                    football_live_cycle_after_integrity=0,
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
            wr, winline_werr = await WinlineLiveFeedService().fetch_football_live_raw_payload(settings)
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
            fetch_res = await asyncio.to_thread(OddsHttpClient().fetch, config)
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
        if not candidates_to_ingest:
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

        analytics_enabled = bool(settings.football_analytics_enabled)
        learning_enabled = bool(settings.football_learning_enabled)
        learning_multipliers: dict[str, float] = {}
        learning_aggregates: list = []
        if learning_enabled and candidates_to_ingest:
            async with sessionmaker() as learn_session:
                learning_multipliers, learning_aggregates = await FootballLearningService().compute_family_multipliers(
                    learn_session
                )

        analytics_svc = FootballAnalyticsService()
        scoring_svc = FootballSignalScoringService()
        family_svc = FootballSignalSendFilterService()
        learning_helper = FootballLearningService()
        live_fields_seen = False
        enriched: list[ProviderSignalCandidate] = []
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
            new_cand = cand.model_copy(
                update={
                    "signal_score": scoring_svc.to_signal_score_decimal(breakdown),
                    "feature_snapshot_json": fs_out,
                    "explanation_json": {
                        **prev_expl,
                        "football_scoring_reason_codes": breakdown.reason_codes,
                    },
                }
            )
            enriched.append(new_cand)
        candidates_to_ingest = enriched

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

        ordered = order_live_finalist_tuples(scored_tuples, min_score_base, family_svc)
        finalists_pre_session = [c for c, _, _ in ordered]
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
            for c, t, s in ordered:
                ik = build_live_idea_key(c)
                if ik in batch_seen or ls_fin.has_idea(ik):
                    ls_fin.record_duplicate_idea_blocked(1)
                    logger.info("[FOOTBALL][SESSION_IDEA_DEDUP] blocked key=%s", ik[:200])
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

        report_after_scoring = n_after_min_score

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
        )
        lq_live = cycle_dbg.get("live_quality_summary") or {}
        if isinstance(cycle_dbg, dict):
            cycle_dbg["session_idea_dedup_this_cycle"] = int(session_dup_blocked)

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
                _wsm = send_meta_final.get(id(best), ("normal", None))
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
            _attach_football_live_send_meta(c, send_meta_final.get(id(c))) for c in finalists
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
            ls_ing.register_idea_sent(build_live_idea_key(cr))
        ls_ing.record_signals_created(len(ingest_res.created_signal_ids))

        notifications_sent_count = 0
        orch = OrchestrationService()
        logger.info("[FOOTBALL] final signals: %s", ingest_res.created_signals)
        for signal_id in ingest_res.created_signal_ids:
            try:
                async with sessionmaker() as session2:
                    sent = await orch.notify_signal_if_configured(session2, bot, signal_id)
                if sent:
                    notifications_sent_count += 1
                    FootballLiveSessionService().record_telegram_message_sent(1)
            except Exception:
                logger.exception("Auto signal notification failed for signal_id=%s", signal_id)
        logger.info("[FOOTBALL] messages sent: %s", notifications_sent_count)
        if notifications_sent_count:
            diagnostics.update(football_live_last_notify_path="NotificationService.send_signal_notification")
        elif ingest_res.created_signal_ids:
            diagnostics.update(football_live_last_notify_path="notify_skipped_see_orchestration_logs")
        else:
            diagnostics.update(football_live_last_notify_path=None)

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

    async def run_football_live_forever(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        bot: Bot,
    ) -> None:
        """Фоновые циклы только пока активна 15-минутная football live-сессия (после ▶️ Старт)."""
        settings = get_settings()
        idle_sleep = max(45, min(180, int(settings.auto_signal_polling_interval_seconds or 60)))
        poll_sleep = max(15, int(settings.auto_signal_polling_interval_seconds or 60))
        while True:
            sleep_time = idle_sleep
            try:
                sess = FootballLiveSessionService()
                sess.expire_if_needed()
                diag_fn = SignalRuntimeDiagnosticsService().update
                if sess.is_active():
                    cres = await self.run_single_cycle(sessionmaker, bot, dry_run=False)
                    snap = sess.snapshot()
                    rem = sess.remaining_seconds()
                    diag_fn(
                        football_live_session_active=snap.active,
                        football_live_session_started_at=(
                            snap.started_at.isoformat() if snap.started_at else None
                        ),
                        football_live_session_expires_at=(
                            snap.expires_at.isoformat() if snap.expires_at else None
                        ),
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
                    )
                    dcur = SignalRuntimeDiagnosticsService().get_state()
                    bn = _infer_football_live_cycle_bottleneck(cres, dcur)
                    diag_fn(football_live_cycle_bottleneck=bn)
                    _apply_last_combat_cycle_diagnostics(cres)
                    _football_log_live_session_report(
                        res=cres, diag=SignalRuntimeDiagnosticsService().get_state()
                    )
                    logger.info(
                        "[FOOTBALL][LIVE_LOOP] cycle done signals_sent_session=%s telegram_sent=%s dup_blocked=%s ideas=%s remaining_min=%s bottleneck=%s",
                        snap.signals_sent_in_session,
                        snap.telegram_messages_sent_in_session,
                        snap.duplicate_ideas_blocked_session,
                        snap.sent_idea_keys_count,
                        round((rem or 0) / 60.0, 2) if rem is not None else None,
                        bn,
                    )
                    sleep_time = poll_sleep
                else:
                    snap_idle = sess.snapshot()
                    diag_fn(
                        football_live_session_active=False,
                        football_live_session_started_at=(
                            snap_idle.started_at.isoformat() if snap_idle.started_at else None
                        ),
                        football_live_session_expires_at=None,
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
                logger.exception("Football live session loop failed")
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

