"""Final football live send gate: one candidate per match, whitelist, mandatory sanity, no fallback garbage."""

from __future__ import annotations

import logging
from typing import Any

from app.schemas.provider_models import ProviderSignalCandidate
from app.services.football_live_market_sanity_service import (
    FootballLiveMarketSanityService,
    _format_bet_line,
    _next_goal_broken_bet,
)
from app.services.football_signal_send_filter_service import FootballSignalSendFilterService

logger = logging.getLogger(__name__)

# Minimal allowed families for combat live send (main football).
ALLOWED_LIVE_FAMILIES = frozenset({"result", "double_chance", "totals", "btts", "handicap"})


def _bet_line(c: ProviderSignalCandidate) -> str:
    return _format_bet_line(c)


def _forbidden_cards_or_special_outcome(c: ProviderSignalCandidate) -> bool:
    parts = [
        str(c.market.market_type or ""),
        str(c.market.market_label or ""),
        str(c.market.selection or ""),
        str(c.market.section_name or ""),
        str(c.market.subsection_name or ""),
    ]
    blob = " ".join(parts).lower()
    if "карточ" in blob:
        return True
    if "card" in blob and ("bet" in blob or "карт" in blob or "желт" in blob or "red " in blob):
        return True
    if "исход" in blob and "1x2" in blob and "карточ" in blob:
        return True
    if "bookings" in blob or "booking" in blob:
        return True
    return False


def _is_next_goal_market(c: ProviderSignalCandidate, bet: str) -> bool:
    ml = (c.market.market_label or "").lower()
    mt = (c.market.market_type or "").lower()
    if "след" in ml and "гол" in ml:
        return True
    if "next goal" in ml or "next goal" in mt:
        return True
    if "след" in bet.lower() and "гол" in bet.lower():
        return True
    return False


def final_live_whitelist_ok(
    c: ProviderSignalCandidate,
    family_svc: FootballSignalSendFilterService,
) -> tuple[bool, str]:
    """Returns (ok, reason_token)."""
    if family_svc.is_corner_market(c):
        return False, "blocked_corners"
    if _forbidden_cards_or_special_outcome(c):
        return False, "blocked_cards_or_special"
    fam = family_svc.get_market_family(c)
    if fam in ALLOWED_LIVE_FAMILIES:
        return True, "ok"
    bet = _bet_line(c)
    if fam == "exotic" and _is_next_goal_market(c, bet):
        if _next_goal_broken_bet(bet):
            return False, "blocked_broken_next_goal_text"
        return True, "ok_next_goal_strict"
    return False, f"blocked_family_{fam}"


def apply_final_live_send_gate(
    finalists: list[ProviderSignalCandidate],
    family_svc: FootballSignalSendFilterService,
) -> tuple[
    list[ProviderSignalCandidate],
    dict[str, Any],
    dict[str, str],
    dict[str, str],
]:
    """
    One signal per match per cycle:
    - among finalists, per event take highest score that passes whitelist;
    - run live sanity on that candidate only;
    - if sanity fails, send nothing for that match (no fallback to weaker ideas).

    Returns: (new_finalists, debug_blob, live_sanity_drop_by_eid, live_sanity_drop_reasons)
    """
    sanity_svc = FootballLiveMarketSanityService()
    by_eid: dict[str, list[ProviderSignalCandidate]] = {}
    for c in finalists:
        eid = str(getattr(getattr(c, "match", None), "external_event_id", "") or "").strip() or "—"
        by_eid.setdefault(eid, []).append(c)

    out: list[ProviderSignalCandidate] = []
    per_match: list[dict[str, Any]] = []
    drop_by_eid: dict[str, str] = {}
    drop_reasons: dict[str, str] = {}

    for eid, group in sorted(by_eid.items(), key=lambda kv: (kv[0] == "—", kv[0])):
        mname = str(group[0].match.match_name or "—") if group else "—"
        # Highest model score first
        sorted_g = sorted(group, key=lambda x: float(x.signal_score or 0.0), reverse=True)
        wl_pass: list[tuple[ProviderSignalCandidate, str]] = []
        wl_fail_reasons: list[str] = []
        for c in sorted_g:
            ok, tok = final_live_whitelist_ok(c, family_svc)
            if ok:
                wl_pass.append((c, tok))
            else:
                wl_fail_reasons.append(f"{tok}:{_bet_line(c)[:80]}")

        row: dict[str, Any] = {
            "event_id": eid,
            "match_name": mname,
            "finalists_found": len(group),
            "candidates_after_whitelist": len(wl_pass),
            "whitelist_rejected": wl_fail_reasons[:12],
            "chosen_final_candidate": None,
            "chosen_reason": None,
            "dropped_other_finalists_count": max(0, len(wl_pass) - 1) if wl_pass else 0,
            "dropped_other_finalists_reasons": [],
            "match_send_skipped": False,
            "skip_reason": None,
        }

        if not wl_pass:
            row["match_send_skipped"] = True
            row["skip_reason"] = "all_finalists_failed_final_whitelist"
            per_match.append(row)
            continue

        top_c, top_why = wl_pass[0]
        others = wl_pass[1:]
        row["dropped_other_finalists_reasons"] = [
            f"not_selected_one_per_match:{_bet_line(x)[:120]}" for x, _ in others
        ]
        fam = family_svc.get_market_family(top_c)
        res = sanity_svc.validate(top_c, fam, family_svc)
        ex = {
            **(top_c.explanation_json or {}),
            "live_sanity": {
                "passed": res.passed,
                "plausibility": res.plausibility,
                "plausibility_score": res.plausibility_score,
                "block_token": res.block_token,
                "reason_ru": res.reason_ru,
                "bet_text": res.bet_text,
                "final_live_gate": True,
            },
        }
        top2 = top_c.model_copy(update={"explanation_json": ex})

        if not res.passed:
            row["match_send_skipped"] = True
            row["skip_reason"] = "top_whitelist_candidate_failed_live_sanity_no_fallback"
            row["chosen_final_candidate"] = _bet_line(top_c)
            row["chosen_reason"] = f"blocked:{res.block_token}"
            if eid and eid != "—":
                drop_by_eid[eid] = res.block_token
                drop_reasons[eid] = res.reason_ru
            per_match.append(row)
            logger.info(
                "[FOOTBALL][FINAL_LIVE_GATE] skip_match event_id=%s match=%s reason=top_failed_sanity token=%s",
                eid,
                mname[:120],
                res.block_token,
            )
            continue

        row["chosen_final_candidate"] = _bet_line(top2)
        row["chosen_reason"] = f"selected:{top_why}+sanity_ok"
        out.append(top2)
        per_match.append(row)
        logger.info(
            "[FOOTBALL][FINAL_LIVE_GATE] keep_one event_id=%s match=%s bet=%s",
            eid,
            mname[:120],
            row["chosen_final_candidate"][:160],
        )

    debug_blob = {
        "per_match": per_match,
        "matches_with_send": len(out),
        "matches_skipped": sum(1 for r in per_match if r.get("match_send_skipped")),
        "allowed_families": sorted(ALLOWED_LIVE_FAMILIES),
    }
    return out, debug_blob, drop_by_eid, drop_reasons
