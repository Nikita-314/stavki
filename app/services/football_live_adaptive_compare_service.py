"""Fair OFF vs ON comparison for football live adaptive learning (same candidates, same pipeline)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.core.enums import SportType
from app.schemas.provider_models import ProviderSignalCandidate
from app.services.football_analytics_service import FootballAnalyticsService
from app.services.football_final_live_send_gate import apply_final_live_send_gate
from app.services.football_learning_service import FootballLearningService
from app.services.football_live_adaptive_learning_service import (
    apply_live_adaptive_adjustment,
    build_live_adaptive_snapshot,
    preview_live_adaptive_tag_keys,
)
from app.services.football_signal_scoring_service import FootballSignalScoringService
from app.services.football_signal_send_filter_service import FootballSignalSendFilterService
from app.services.auto_signal_service import (
    _assert_finalist_safe_for_live_send,
    _sort_finalists_main_market_first,
    classify_live_sendable_candidate,
    order_live_finalist_tuples,
)


def _eid(c: ProviderSignalCandidate) -> str:
    return str(getattr(getattr(c, "match", None), "external_event_id", "") or "").strip() or "—"


def _score_triplet(c: ProviderSignalCandidate) -> tuple[float, float, float]:
    fs = c.feature_snapshot_json or {}
    la = fs.get("football_live_adaptive_learning")
    if isinstance(la, dict) and la.get("enabled"):
        b = float(la.get("base_signal_score") or 0.0)
        adj = float(la.get("learning_adjustment_total") or 0.0)
        eff = float(la.get("effective_live_score") or c.signal_score or 0.0)
        return b, adj, eff
    sc = float(c.signal_score or 0.0)
    return sc, 0.0, sc


@dataclass
class _VariantOut:
    scored_sorted: list[ProviderSignalCandidate]
    finalists: list[ProviderSignalCandidate]
    final_live_gate_debug: dict[str, Any]
    live_sanity_drop_by_eid: dict[str, str]
    live_sanity_drop_reasons: dict[str, str]
    live_send_stats: dict[str, Any]
    n_after_min_score: int
    adaptive_snapshot_meta: dict[str, Any]


async def _run_pipeline_variant(
    sessionmaker: async_sessionmaker[AsyncSession],
    candidates_in: list[ProviderSignalCandidate],
    settings: Settings,
    *,
    live_adaptive_enabled: bool,
    dry_run: bool,
) -> _VariantOut:
    """Same scoring → classify → order → dry finalists → safety → final gate as production dry_run."""
    analytics_enabled = bool(settings.football_analytics_enabled)
    learning_enabled = bool(settings.football_learning_enabled)
    candidates_to_ingest = [c.model_copy() for c in candidates_in]

    learning_multipliers: dict[str, float] = {}
    learning_aggregates: list = []
    live_adaptive_snapshot = None
    snap_meta: dict[str, Any] = {}
    if candidates_to_ingest and (learning_enabled or live_adaptive_enabled):
        async with sessionmaker() as learn_session:
            if learning_enabled:
                learning_multipliers, learning_aggregates = await FootballLearningService().compute_family_multipliers(
                    learn_session
                )
            if live_adaptive_enabled:
                live_adaptive_snapshot = await build_live_adaptive_snapshot(learn_session)
                if live_adaptive_snapshot:
                    snap_meta = {
                        **(live_adaptive_snapshot.meta or {}),
                        "delta_keys_loaded": len(live_adaptive_snapshot.deltas),
                    }

    analytics_svc = FootballAnalyticsService()
    scoring_svc = FootballSignalScoringService()
    family_svc = FootballSignalSendFilterService()
    learning_helper = FootballLearningService()
    enriched: list[ProviderSignalCandidate] = []

    for idx, cand in enumerate(candidates_to_ingest):
        family = family_svc.get_market_family(cand)
        analytics = analytics_svc.build_snapshot(cand, market_family=family)
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

        enriched.append(
            cand.model_copy(
                update={
                    "signal_score": eff_decimal,
                    "feature_snapshot_json": fs_out,
                    "explanation_json": {
                        **prev_expl,
                        "football_scoring_reason_codes": breakdown.reason_codes,
                    },
                }
            )
        )

    min_score_base = float(settings.football_min_signal_score or 60.0)
    single_gap_max = float(getattr(settings, "football_live_single_relief_max_gap", 2.0) or 2.0)
    scored_sorted = sorted(enriched, key=lambda c: float(c.signal_score or 0), reverse=True)

    scored_tuples: list[tuple[ProviderSignalCandidate, str, str | None]] = []
    for c in scored_sorted:
        tier, sub = classify_live_sendable_candidate(
            c, min_score_base, family_svc, single_relief_max_gap=single_gap_max
        )
        if tier != "reject":
            scored_tuples.append((c, tier, sub))

    ordered = order_live_finalist_tuples(scored_tuples, min_score_base, family_svc)
    finalists_pre_session = [c for c, _, _ in ordered]
    n_after_min_score = len(finalists_pre_session)
    send_meta_final: dict[int, tuple[str, str | None]] = {}
    if dry_run:
        kept_fin = [c for c, _, _ in ordered]
        for c, t, s in ordered:
            send_meta_final[id(c)] = (t, s)
        finalists = _sort_finalists_main_market_first(kept_fin, family_svc)
    else:
        raise RuntimeError("compare service expects dry_run=True for deterministic finalist pool")

    _bfs = len(finalists)
    finalists = [
        c
        for c in finalists
        if _assert_finalist_safe_for_live_send(c, min_score_base, family_svc)
    ]
    if _bfs != len(finalists):
        pass

    live_sanity_drop_by_eid: dict[str, str] = {}
    live_sanity_drop_reasons: dict[str, str] = {}
    final_live_gate_debug: dict[str, Any] = {}
    if finalists:
        finalists, final_live_gate_debug, live_sanity_drop_by_eid, live_sanity_drop_reasons = (
            apply_final_live_send_gate(finalists, family_svc)
        )
    else:
        finalists = []

    live_send_stats = {
        "normal_sendable": sum(1 for _, t, _ in scored_tuples if t == "normal"),
        "soft_sendable_total": sum(1 for _, t, _ in scored_tuples if t == "soft"),
        "rejected_total": sum(
            1
            for c in scored_sorted
            if classify_live_sendable_candidate(c, min_score_base, family_svc, single_relief_max_gap=single_gap_max)[0]
            == "reject"
        ),
    }

    return _VariantOut(
        scored_sorted=scored_sorted,
        finalists=list(finalists),
        final_live_gate_debug=final_live_gate_debug or {},
        live_sanity_drop_by_eid=live_sanity_drop_by_eid,
        live_sanity_drop_reasons=live_sanity_drop_reasons,
        live_send_stats=live_send_stats,
        n_after_min_score=n_after_min_score,
        adaptive_snapshot_meta=snap_meta,
    )


def _global_ranks(scored_sorted: list[ProviderSignalCandidate]) -> dict[int, int]:
    out: dict[int, int] = {}
    for i, c in enumerate(scored_sorted, start=1):
        out[id(c)] = i
    return out


def _gate_by_eid(debug: dict[str, Any]) -> dict[str, str | None]:
    m: dict[str, str | None] = {}
    for row in debug.get("per_match") or []:
        if not isinstance(row, dict):
            continue
        e = str(row.get("event_id") or "").strip() or "—"
        m[e] = str(row.get("final_gate_decision") or "") or None
    return m


def _sent_by_eid(debug: dict[str, Any]) -> dict[str, bool]:
    out: dict[str, bool] = {}
    for row in debug.get("per_match") or []:
        if not isinstance(row, dict):
            continue
        e = str(row.get("event_id") or "").strip() or "—"
        out[e] = str(row.get("final_gate_decision") or "") == "sent"
    return out


def _best_per_eid(scored_sorted: list[ProviderSignalCandidate]) -> dict[str, ProviderSignalCandidate]:
    """Highest effective score per event."""
    best: dict[str, ProviderSignalCandidate] = {}
    for c in sorted(scored_sorted, key=lambda x: float(x.signal_score or 0.0), reverse=True):
        e = _eid(c)
        if e not in best:
            best[e] = c
    return best


async def run_adaptive_compare_report(
    sessionmaker: async_sessionmaker[AsyncSession],
    post_integrity_candidates: list[ProviderSignalCandidate],
    settings: Settings,
    *,
    dry_run: bool = True,
    pipeline_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run OFF then ON on copies of the same candidate list; return aggregates + per-match + diffs."""
    pm = dict(pipeline_meta or {})
    off = await _run_pipeline_variant(
        sessionmaker, post_integrity_candidates, settings, live_adaptive_enabled=False, dry_run=dry_run
    )
    on = await _run_pipeline_variant(
        sessionmaker, post_integrity_candidates, settings, live_adaptive_enabled=True, dry_run=dry_run
    )

    ro = _global_ranks(off.scored_sorted)
    rn = _global_ranks(on.scored_sorted)
    gate_off = _gate_by_eid(off.final_live_gate_debug)
    gate_on = _gate_by_eid(on.final_live_gate_debug)
    sent_off = _sent_by_eid(off.final_live_gate_debug)
    sent_on = _sent_by_eid(on.final_live_gate_debug)

    best_off = _best_per_eid(off.scored_sorted)
    best_on = _best_per_eid(on.scored_sorted)
    all_eids = set(best_off) | set(best_on)

    per_match: list[dict[str, Any]] = []
    changed: list[dict[str, Any]] = []
    max_adj = 0.0

    for eid in sorted(all_eids, key=lambda x: (x == "—", x)):
        co = best_off.get(eid)
        cn = best_on.get(eid)
        if co is None and cn is None:
            continue
        bo, ado, eo = _score_triplet(co) if co else (0.0, 0.0, 0.0)
        bn, adn, en = _score_triplet(cn) if cn else (0.0, 0.0, 0.0)
        max_adj = max(max_adj, abs(ado), abs(adn))

        rok = ro.get(id(co)) if co else None
        rnk = rn.get(id(cn)) if cn else None
        go = gate_off.get(eid)
        gn = gate_on.get(eid)
        so = bool(sent_off.get(eid))
        sn = bool(sent_on.get(eid))
        row = {
            "event_id": eid,
            "match": str((co or cn).match.match_name or "—") if (co or cn) else "—",
            "base_signal_score_off": round(bo, 4) if co else None,
            "learning_adjustment_total_off": round(ado, 4) if co else None,
            "effective_live_score_off": round(eo, 4) if co else None,
            "global_rank_off": rok,
            "base_signal_score_on": round(bn, 4) if cn else None,
            "learning_adjustment_total_on": round(adn, 4) if cn else None,
            "effective_live_score_on": round(en, 4) if cn else None,
            "global_rank_on": rnk,
            "final_gate_decision_off": go,
            "final_gate_decision_on": gn,
            "sent_off": so,
            "sent_on": sn,
        }
        per_match.append(row)

        if (
            rok != rnk
            or go != gn
            or so != sn
            or abs((eo if co else 0) - (en if cn else 0)) > 1e-6
        ):
            changed.append(
                {
                    **row,
                    "why_changed": [
                        x
                        for x in [
                            "rank" if rok != rnk else None,
                            "gate_decision" if go != gn else None,
                            "sent" if so != sn else None,
                            "effective_score" if co and cn and abs(eo - en) > 1e-6 else None,
                        ]
                        if x
                    ],
                }
            )

    agg_off = {
        "live_matches_total": pm.get("live_matches_total"),
        "matches_after_freshness": pm.get("matches_after_freshness"),
        "matches_with_scored_candidates": len({_eid(c) for c in off.scored_sorted if _eid(c) != "—"}),
        "candidates_after_integrity": len(post_integrity_candidates),
        "matches_reaching_final_gate": off.final_live_gate_debug.get("matches_reaching_final_gate"),
        "matches_blocked_by_final_gate": off.final_live_gate_debug.get("matches_blocked_by_final_gate"),
        "matches_sent_after_final_gate": off.final_live_gate_debug.get("matches_sent_after_final_gate"),
        "finalists_after_safety": len(off.finalists),
        "live_send_stats": off.live_send_stats,
        "n_after_min_score": off.n_after_min_score,
        "created_signals_count": 0,
        "notifications_sent_count": 0,
        "note": "compare_only dry_run — no DB / Telegram",
    }
    agg_on = {
        **agg_off,
        "matches_with_scored_candidates": len({_eid(c) for c in on.scored_sorted if _eid(c) != "—"}),
        "matches_reaching_final_gate": on.final_live_gate_debug.get("matches_reaching_final_gate"),
        "matches_blocked_by_final_gate": on.final_live_gate_debug.get("matches_blocked_by_final_gate"),
        "matches_sent_after_final_gate": on.final_live_gate_debug.get("matches_sent_after_final_gate"),
        "finalists_after_safety": len(on.finalists),
        "live_send_stats": on.live_send_stats,
        "n_after_min_score": on.n_after_min_score,
    }

    assessment = _assess_impact(
        changed=changed,
        max_abs_adj_seen=max_adj,
        n_candidates=len(post_integrity_candidates),
        snapshot_meta_on=on.adaptive_snapshot_meta,
    )

    return {
        "pipeline_meta": pm,
        "aggregate_off": agg_off,
        "aggregate_on": agg_on,
        "per_match": per_match,
        "changed_cases": changed,
        "max_abs_adjustment_observed": round(max_adj, 4),
        "adaptive_snapshot_meta_on": on.adaptive_snapshot_meta,
        "assessment": assessment,
    }


def _assess_impact(
    *,
    changed: list[dict],
    max_abs_adj_seen: float,
    n_candidates: int,
    snapshot_meta_on: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Heuristic verdict for operators (not ML)."""
    n_changed = len(changed)
    sm = dict(snapshot_meta_on or {})
    n_keys = int(sm.get("delta_keys_loaded") or 0)
    rationale_rows = sm.get("rows_with_rationale")
    if n_candidates <= 0:
        return {"verdict": "no_data", "summary_ru": "Нет кандидатов после integrity — сравнение пустое."}
    if max_abs_adj_seen < 0.05 and n_changed == 0 and n_keys == 0:
        return {
            "verdict": "no_active_deltas",
            "summary_ru": "В снимке adaptive нет ни одной калиброванной дельты по истории "
            f"(rationale_rows≈{rationale_rows}) — OFF и ON совпадают, ждём больше settled live с rationale.",
        }
    if max_abs_adj_seen < 0.05 and n_changed == 0:
        return {
            "verdict": "negligible",
            "summary_ru": "Дельты из БД есть ("
            f"{n_keys} ключей), но к текущим тегам не применились или слишком малы — отбор не сдвинулся.",
        }
    if n_changed == 0:
        return {
            "verdict": "low_signal_noise_ok",
            "summary_ru": "Дельты score есть, но ранги и final gate совпали — влияние на отбор минимальное.",
        }
    gates = [c for c in changed if "gate_decision" in c.get("why_changed") or "sent" in c.get("why_changed")]
    if gates:
        return {
            "verdict": "material",
            "summary_ru": f"Есть {len(gates)} матч(ей), где изменился gate/sent — слой реально влияет на отбор.",
        }
    return {
        "verdict": "rank_only",
        "summary_ru": "Меняется только относительный ранг кандидатов (важно при конкуренции матчей), gate по матчам тот же.",
    }


def compare_report_to_json(report: dict[str, Any]) -> str:
    def _default(o: object) -> Any:
        if isinstance(o, ProviderSignalCandidate):
            return {
                "event_id": _eid(o),
                "match": str(o.match.match_name or ""),
                "score": float(o.signal_score or 0),
            }
        raise TypeError

    return json.dumps(report, ensure_ascii=False, indent=2, default=_default)
