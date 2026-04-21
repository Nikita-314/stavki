"""Final football live send gate: core main markets only, one candidate per match, mandatory sanity."""

from __future__ import annotations

import logging
from typing import Any

from app.schemas.provider_models import ProviderSignalCandidate
from app.services.football_live_market_sanity_service import (
    FootballLiveMarketSanityService,
    LiveSanityResult,
    _format_bet_line,
)
from app.services.football_signal_send_filter_service import FootballSignalSendFilterService

logger = logging.getLogger(__name__)

# Allowed high-level families (classifier). Sub-types under `result` are filtered separately.
ALLOWED_LIVE_FAMILIES = frozenset({"result", "double_chance", "totals", "btts", "handicap"})

# Result-like / special markets that must never go to combat Telegram (label/type/bet heuristics).
_EXOTIC_RESULT_LIKE_SUBSTRINGS = (
    "азиатск",
    "asian handicap",
    "asian hcap",
    "следующий гол",
    "next goal",
    "nextgoal",
    "матч с минут",
    "from minute",
    "первый гол",
    "first goal",
    "последний гол",
    "last goal",
    "кто забьет",
    "кто забьёт",
    "goalscorer",
    "anytime scorer",
    "точный счёт",
    "точный счет",
    "correct score",
    "тайм/матч",
    "half time/full time",
    "ht/ft",
    "ht-ft",
    "метод победы",
    "method of victory",
    "winning margin",
    "разниц",
    "race to",
    "гол в обе",
    "both halves",
    # Interval / minute-range / partial-match result (not full-time plain 1X2).
    "в интервале",
    "интервал с",
    "интервале с",
    "интервальн",
    " interval",
    "interval ",
    "interval:",
    "time range",
    "time-range",
    "minute range",
    "до конца тайма",
    "до конца матча",
    "остаток тайма",
    "остаток периода",
    "остаток 1",
    "остаток 2",
    "1-й тайм",
    "2-й тайм",
    "1й тайм",
    "2й тайм",
    "@1ht@",
    "@2ht@",
    "first half",
    "second half",
    # Cup / progression / qualification (not match 1X2).
    "следующий раунд",
    "next round",
    "выйдет в ",
    "to advance",
    "кто пройдет",
    "кто пройдёт",
)

_REMAINDER_RESULT_SUBSTRINGS = (
    "остаток матча",
    "rest of the match",
    "rest of match",
    "выиграет остаток",
    "win the rest",
)


def _bet_line(c: ProviderSignalCandidate) -> str:
    return _format_bet_line(c)


def _candidate_text_blob(c: ProviderSignalCandidate) -> str:
    parts = [
        str(c.market.market_type or ""),
        str(c.market.market_label or ""),
        str(c.market.selection or ""),
        str(c.market.section_name or ""),
        str(c.market.subsection_name or ""),
        _bet_line(c),
    ]
    return " ".join(parts).lower()


def _forbidden_cards_or_special_outcome(c: ProviderSignalCandidate) -> bool:
    blob = _candidate_text_blob(c)
    if "карточ" in blob:
        return True
    if "card" in blob and ("bet" in blob or "карт" in blob or "желт" in blob or "red " in blob):
        return True
    if "исход" in blob and "1x2" in blob and "карточ" in blob:
        return True
    if "bookings" in blob or "booking" in blob:
        return True
    return False


def _is_exotic_result_like(c: ProviderSignalCandidate) -> bool:
    b = _candidate_text_blob(c)
    return any(s in b for s in _EXOTIC_RESULT_LIKE_SUBSTRINGS)


def _is_plain_main_result_1x2(c: ProviderSignalCandidate, family_svc: FootballSignalSendFilterService) -> bool:
    """True only for classic match-winner / 1x2 outcome, not European HC, next goal, rest-of-match, etc."""
    if family_svc.get_market_family(c) != "result":
        return False
    mt = str(c.market.market_type or "").strip().lower()
    if mt not in {"1x2", "match_winner"}:
        return False
    if _is_exotic_result_like(c):
        return False
    return True


def _is_remainder_result_1x2(c: ProviderSignalCandidate, family_svc: FootballSignalSendFilterService) -> bool:
    """Allow 'rest of the match' / remainder result if it's still a plain 1x2-like bet."""
    if family_svc.get_market_family(c) != "result":
        return False
    mt = str(c.market.market_type or "").strip().lower()
    if mt not in {"1x2", "match_winner"}:
        return False
    blob = _candidate_text_blob(c)
    if not any(s in blob for s in _REMAINDER_RESULT_SUBSTRINGS):
        return False
    # Still forbid other exotic result-like patterns (next goal, correct score, etc.)
    if any(s in blob for s in _EXOTIC_RESULT_LIKE_SUBSTRINGS):
        return False
    return True


def main_combat_live_send_ok(
    c: ProviderSignalCandidate,
    family_svc: FootballSignalSendFilterService,
) -> tuple[bool, str]:
    """
    Strict combat-live whitelist: only core main markets for Telegram.
    Returns (ok, reason_token).
    """
    if family_svc.is_corner_market(c):
        return False, "blocked_corners"
    if _forbidden_cards_or_special_outcome(c):
        return False, "blocked_cards_or_special"
    fam = family_svc.get_market_family(c)
    if fam not in ALLOWED_LIVE_FAMILIES:
        return False, f"blocked_family_{fam}"
    if fam == "result":
        if _is_exotic_result_like(c):
            return False, "blocked_exotic_result_market"
        if not (_is_plain_main_result_1x2(c, family_svc) or _is_remainder_result_1x2(c, family_svc)):
            return False, "blocked_non_main_live_market"
        return True, "ok_main_result"
    if fam == "double_chance":
        if _is_exotic_result_like(c):
            return False, "blocked_exotic_result_market"
        return True, "ok_double_chance"
    if fam == "totals":
        return True, "ok_totals"
    if fam == "btts":
        return True, "ok_btts"
    if fam == "handicap":
        if _is_exotic_result_like(c):
            return False, "blocked_exotic_result_market"
        return True, "ok_handicap"
    return False, "blocked_non_main_live_market"


def _decision_when_no_main_allowed(reject_lines: list[str]) -> str:
    if not reject_lines:
        return "blocked_no_core_main_candidate"
    toks = []
    for fl in reject_lines:
        t = fl.split(":", 1)[0].strip() if ":" in fl else fl
        toks.append(t)
    if all(t == "blocked_exotic_result_market" for t in toks):
        return "blocked_exotic_result_market"
    if all(t in {"blocked_non_main_live_market", "blocked_exotic_result_market"} for t in toks):
        if any(t == "blocked_exotic_result_market" for t in toks):
            return "blocked_exotic_result_market"
        return "blocked_non_main_live_market"
    if all(t == "blocked_cards_or_special" for t in toks):
        return "blocked_cards_or_special"
    if all(t.startswith("blocked_family") or t == "blocked_corners" for t in toks):
        return "blocked_family_or_corners"
    return "blocked_no_core_main_candidate"


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
    - sort finalists by model score;
    - keep only core main-market candidates (no European HC, next goal, etc.);
    - walk allowed candidates in score order; first that passes live sanity is sent;
    - no fallback to disallowed market types (forbidden top → try next allowed only).
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
    token_hits: dict[str, int] = {}
    suspicious_core_signals_blocked = 0
    core_live_extra_sanity_blocked = 0
    late_game_live_sanity_blocked = 0

    for eid, group in sorted(by_eid.items(), key=lambda kv: (kv[0] == "—", kv[0])):
        mname = str(group[0].match.match_name or "—") if group else "—"
        sorted_g = sorted(group, key=lambda x: float(x.signal_score or 0.0), reverse=True)
        best0 = sorted_g[0]
        best_bet = _bet_line(best0)
        best_fam = family_svc.get_market_family(best0)
        best_sc = round(float(best0.signal_score or 0.0), 2)
        tn = str(getattr(best0.match, "tournament_name", "") or "").strip() or "—"
        best_ok, best_tok = main_combat_live_send_ok(best0, family_svc)

        main_allowed: list[tuple[ProviderSignalCandidate, str]] = []
        main_rejects: list[str] = []
        forbidden_finalists_count = 0
        for c in sorted_g:
            ok, tok = main_combat_live_send_ok(c, family_svc)
            token_hits[tok] = int(token_hits.get(tok, 0) or 0) + 1
            if ok:
                main_allowed.append((c, tok))
            else:
                forbidden_finalists_count += 1
                main_rejects.append(f"{tok}:{_bet_line(c)[:80]}")

        row: dict[str, Any] = {
            "event_id": eid,
            "match_name": mname,
            "tournament": tn,
            "best_scored_candidate_bet": best_bet,
            "best_scored_candidate_family": best_fam,
            "best_scored_candidate_score": best_sc,
            "best_scored_main_market_ok": bool(best_ok),
            "best_scored_main_market_token": best_tok,
            "finalists_found_before_gate": len(group),
            "forbidden_finalists_count": forbidden_finalists_count,
            "main_allowed_finalists_count": len(main_allowed),
            "candidates_after_whitelist": len(main_allowed),
            "whitelist_rejected": main_rejects[:16],
            "chosen_allowed_candidate": None,
            "chosen_final_candidate": None,
            "chosen_reason": None,
            "final_sent_bet": None,
            "final_gate_whitelist_passed": bool(main_allowed),
            "final_gate_sanity_passed": None,
            "final_gate_decision": None,
            "blocked_reason": None,
            "sanity_attempts": [],
            "dropped_other_finalists_count": 0,
            "dropped_other_finalists_reasons": [],
            "match_send_skipped": False,
            "skip_reason": None,
        }

        if not main_allowed:
            row["match_send_skipped"] = True
            row["skip_reason"] = "no_core_main_market_finalist"
            row["blocked_reason"] = "no_core_main_market_in_finalists"
            row["final_gate_sanity_passed"] = False
            row["final_gate_decision"] = _decision_when_no_main_allowed(main_rejects)
            per_match.append(row)
            continue

        chosen: ProviderSignalCandidate | None = None
        chosen_why: str | None = None
        ok_sanity_res: LiveSanityResult | None = None
        sanity_attempts: list[dict[str, Any]] = []
        for cand, tok_ok in main_allowed:
            fam = family_svc.get_market_family(cand)
            res = sanity_svc.validate(cand, fam, family_svc)
            sanity_attempts.append(
                {
                    "bet": _bet_line(cand)[:120],
                    "passed": res.passed,
                    "block_token": res.block_token,
                    "reason_ru": (res.reason_ru or "")[:200],
                }
            )
            if res.passed:
                chosen = cand
                chosen_why = tok_ok
                ok_sanity_res = res
                break
            tk = str(res.block_token or "")
            if tk in (
                "blocked_suspicious_core_live_signal",
                "blocked_missing_live_context_from_source",
                "blocked_live_quality_gate",
            ):
                suspicious_core_signals_blocked += 1
            elif tk == "blocked_core_late_high_gap_total":
                core_live_extra_sanity_blocked += 1
            elif tk == "blocked_late_live_market":
                late_game_live_sanity_blocked += 1

        row["sanity_attempts"] = sanity_attempts

        if chosen is None:
            row["match_send_skipped"] = True
            row["skip_reason"] = "all_core_main_candidates_failed_live_sanity"
            row["blocked_reason"] = "all_allowed_main_failed_sanity"
            row["chosen_allowed_candidate"] = None
            row["chosen_final_candidate"] = _bet_line(main_allowed[0][0])
            row["chosen_reason"] = "sanity_failed_all_tried"
            row["final_gate_sanity_passed"] = False
            row["final_gate_decision"] = "blocked_live_sanity"
            if eid and eid != "—":
                last_tok = sanity_attempts[-1].get("block_token") if sanity_attempts else ""
                drop_by_eid[eid] = str(last_tok or "blocked_live_sanity")
                drop_reasons[eid] = str(sanity_attempts[-1].get("reason_ru") or "") if sanity_attempts else ""
            row["dropped_other_finalists_count"] = max(0, len(main_allowed) - 1)
            row["dropped_other_finalists_reasons"] = [
                f"not_selected_sanity_fail:{_bet_line(x)[:100]}" for x, _ in main_allowed
            ]
            per_match.append(row)
            logger.info(
                "[FOOTBALL][FINAL_LIVE_GATE] skip_match event_id=%s match=%s reason=all_main_failed_sanity",
                eid,
                mname[:120],
            )
            continue

        others = [(x, w) for x, w in main_allowed if id(x) != id(chosen)]
        row["chosen_allowed_candidate"] = _bet_line(chosen)
        row["dropped_other_finalists_count"] = len(others)
        row["dropped_other_finalists_reasons"] = [
            f"not_selected_one_per_match:{_bet_line(x)[:120]}" for x, _ in others
        ]

        _sr = ok_sanity_res
        ex = {
            **(chosen.explanation_json or {}),
            "live_sanity": {
                "passed": bool(_sr and _sr.passed),
                "plausibility": getattr(_sr, "plausibility", None),
                "plausibility_score": getattr(_sr, "plausibility_score", None),
                "block_token": getattr(_sr, "block_token", None),
                "reason_ru": getattr(_sr, "reason_ru", None),
                "bet_text": getattr(_sr, "bet_text", None) or _bet_line(chosen),
                "final_live_gate": True,
            },
        }
        _h2, _a2, _mn2 = sanity_svc._live_snapshot(chosen)
        if _mn2 is not None:
            _mv = int(_mn2)
            if _mv >= 88:
                ex["football_live_late_stage_warning_ru"] = (
                    "⚠️ Очень поздняя стадия матча: сигнал допущен только как core live после sanity."
                )
            elif _mv >= 82:
                ex["football_live_late_stage_warning_ru"] = (
                    "🕒 Концовка матча — оцените оставшееся время и ликвидность линии."
                )
        top2 = chosen.model_copy(update={"explanation_json": ex})

        row["chosen_final_candidate"] = _bet_line(top2)
        row["chosen_reason"] = f"selected:{chosen_why}+sanity_ok"
        row["final_sent_bet"] = row["chosen_final_candidate"]
        row["final_gate_sanity_passed"] = True
        row["final_gate_decision"] = "sent"
        row["blocked_reason"] = None
        out.append(top2)
        per_match.append(row)
        logger.info(
            "[FOOTBALL][FINAL_LIVE_GATE] keep_one event_id=%s match=%s bet=%s",
            eid,
            mname[:120],
            row["chosen_final_candidate"][:160],
        )

    n_blocked_gate = sum(1 for r in per_match if r.get("match_send_skipped"))
    n_sent_decision = sum(1 for r in per_match if r.get("final_gate_decision") == "sent")
    matches_blocked_sanity = sum(
        1 for r in per_match if r.get("final_gate_decision") == "blocked_live_sanity"
    )
    matches_blocked_non_main = sum(
        1
        for r in per_match
        if str(r.get("final_gate_decision") or "")
        in {"blocked_non_main_live_market", "blocked_exotic_result_market", "blocked_no_core_main_candidate"}
    )
    debug_blob = {
        "per_match": per_match,
        "matches_with_send": len(out),
        "matches_skipped": n_blocked_gate,
        "allowed_families": sorted(ALLOWED_LIVE_FAMILIES),
        "live_matches_total": len(by_eid),
        "matches_reaching_final_gate": len(by_eid),
        "matches_blocked_by_final_gate": n_blocked_gate,
        "matches_sent_after_final_gate": n_sent_decision,
        "sent_main_markets_count": n_sent_decision,
        "matches_blocked_live_sanity": matches_blocked_sanity,
        "matches_blocked_non_main_aggregate": matches_blocked_non_main,
        "main_market_token_hits": dict(sorted(token_hits.items(), key=lambda x: -x[1])),
        "blocked_cards_or_special_hits": int(token_hits.get("blocked_cards_or_special", 0)),
        "blocked_corners_hits": int(token_hits.get("blocked_corners", 0)),
        "blocked_exotic_result_market_hits": int(token_hits.get("blocked_exotic_result_market", 0)),
        "blocked_non_main_live_market_hits": int(token_hits.get("blocked_non_main_live_market", 0)),
        "blocked_family_hits": sum(v for k, v in token_hits.items() if k.startswith("blocked_family_")),
        "core_main_only": True,
        "suspicious_core_signals_blocked": suspicious_core_signals_blocked,
        "core_live_extra_sanity_blocked": core_live_extra_sanity_blocked,
        "late_game_live_sanity_blocked": late_game_live_sanity_blocked,
        "matches_sent_after_timing_sanity": n_sent_decision,
    }
    return out, debug_blob, drop_by_eid, drop_reasons
