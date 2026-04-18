from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Literal

from app.schemas.provider_models import ProviderSignalCandidate
from app.services.football_analytics_service import FootballAnalyticsService
from app.services.football_bet_formatter_service import FootballBetFormatterService, FootballTotalContext
from app.services.football_signal_send_filter_service import FootballSignalSendFilterService

logger = logging.getLogger(__name__)

PlLevel = Literal["ok", "weak", "impossible"]


@dataclass
class LiveSanityResult:
    passed: bool
    plausibility: PlLevel
    plausibility_score: int  # 0..100
    block_token: str  # blocked_… when not passed; still set when passed for UI
    reason_ru: str
    bet_text: str


def _format_bet_line(c: ProviderSignalCandidate) -> str:
    f = FootballBetFormatterService()
    p = f.format_bet(
        market_type=c.market.market_type,
        market_label=c.market.market_label,
        selection=c.market.selection,
        home_team=c.match.home_team,
        away_team=c.match.away_team,
        section_name=c.market.section_name,
        subsection_name=c.market.subsection_name,
    )
    if p.detail_label:
        return f"{p.main_label} ({p.detail_label})"
    return p.main_label


def _european_hcap_broken(
    mlabel: str, selection: str, bet: str, fmt: FootballBetFormatterService
) -> bool:
    mll = (mlabel or "").lower()
    if "европ" not in mll and "european" not in mll:
        return False
    if "гандик" not in mll and "handicap" not in mll:
        return False
    if fmt._normalize_yes_no((selection or "").strip()):
        return True
    b = (bet or "").lower()
    if re.search(r"гандик[а-яa-z]*\s*:\s*да(\s|$|\.|,)", b, re.I):
        return True
    if re.search(r"european[^:]*:\s*yes", b, re.I):
        return True
    return False


def _next_goal_broken_bet(bet: str) -> bool:
    if "след" in (bet or "").lower() and "гол" in (bet or "").lower():
        if re.search(r"гол\s*[-–—]?\s*й", bet or "", re.I) and "?" not in bet and not re.search(r"(\d+)[-–—]?\s*й", bet or ""):
            if re.search(r"гол\s*[-–—]\s*й", bet or ""):
                return True
    return False


def _result_pick_side(
    c: ProviderSignalCandidate, mtype: str, fmt: FootballBetFormatterService
) -> str | None:
    s = (c.market.selection or "").strip()
    sl = s.lower()
    t = mtype.lower()
    if t in ("double_chance",) or "double" in t:
        return None
    o = fmt._normalize_outcome_token(s)
    if o == "П1":
        return "home"
    if o == "П2":
        return "away"
    if o == "Х" or s.upper() in fmt._DRAW_TOKENS or sl in ("ничья", "draw"):
        return "draw"
    home = fmt._humanize_team(c.match.home_team)
    away = fmt._humanize_team(c.match.away_team)
    if home and (home.lower() in sl or sl in home.lower()):
        return "home"
    if away and (away.lower() in sl or sl in away.lower()):
        return "away"
    return None


def _min_goals_strict_over(line: float) -> int:
    """Smallest integer total goals G with G > line (standard .5 football lines)."""
    return int(line + 0.5)


def _is_total_over_side(ctx: FootballTotalContext) -> bool:
    return (ctx.total_side or "").upper() == "ТБ"


def _core_live_totals_quality(
    c: ProviderSignalCandidate,
    family: str,
    h: int | None,
    a: int | None,
    minute: int | None,
    fmt: FootballBetFormatterService,
    bet: str,
) -> LiveSanityResult | None:
    """Extra plausibility for core totals (combat live): weak late gaps, missing snapshot on aggressive lines."""
    if family != "totals":
        return None
    ctx: FootballTotalContext | None = fmt.describe_total_context(
        market_type=c.market.market_type,
        market_label=c.market.market_label,
        selection=c.market.selection,
        home_team=c.match.home_team,
        away_team=c.match.away_team,
        section_name=c.market.section_name,
        subsection_name=c.market.subsection_name,
    )
    if not ctx or not ctx.total_line:
        return None
    try:
        line = float((ctx.total_line or "0").replace(",", "."))
    except ValueError:
        return None
    if not _is_total_over_side(ctx):
        return None

    scope = (ctx.target_scope or "").lower()
    if ("home" in scope or "it1" in scope) and "away" not in scope:
        goals = h
    elif ("away" in scope or "it2" in scope) and "home" not in scope:
        goals = a
    else:
        goals = (h + a) if h is not None and a is not None else None

    missing_any = h is None or a is None or minute is None
    combined = scope == "match"

    # Aggressive match total OVER without reliable live snapshot — low channel value.
    if combined and line >= 4.5 and missing_any:
        return LiveSanityResult(
            passed=False,
            plausibility="weak",
            plausibility_score=28,
            block_token="blocked_suspicious_core_live_signal",
            reason_ru="Крупный тотал матча без полного live-снимка (счёт/минута) — не публикуем",
            bet_text=bet,
        )

    if goals is None or minute is None:
        return None

    min_goals = _min_goals_strict_over(line)
    need_more = max(0, min_goals - int(goals))
    m = int(minute)

    if need_more >= 3 and m >= 70:
        return LiveSanityResult(
            passed=False,
            plausibility="weak",
            plausibility_score=22,
            block_token="blocked_core_late_high_gap_total",
            reason_ru=f"Тотал больше {line}: нужно ещё ≥{need_more} гол(а) при {m}' — слишком жёсткий хвост матча",
            bet_text=bet,
        )
    if need_more >= 2 and m >= 78:
        return LiveSanityResult(
            passed=False,
            plausibility="weak",
            plausibility_score=35,
            block_token="blocked_core_late_high_gap_total",
            reason_ru=f"Тотал больше {line}: к {m}' не хватает {need_more} гол(ов) — малореалистично для публикации",
            bet_text=bet,
        )
    if need_more >= 2 and m >= 74 and line >= 3.5:
        return LiveSanityResult(
            passed=False,
            plausibility="weak",
            plausibility_score=40,
            block_token="blocked_core_late_high_gap_total",
            reason_ru=f"Высокая линия ({line}) и {m}': нужно ещё {need_more} гол(а) — режем как слабый live-сигнал",
            bet_text=bet,
        )
    # Team / IT1/IT2: late need 2+ goals on short clock
    if scope != "match" and need_more >= 2 and m >= 78:
        return LiveSanityResult(
            passed=False,
            plausibility="weak",
            plausibility_score=38,
            block_token="blocked_core_late_high_gap_total",
            reason_ru=f"Командный тотал: к {m}' нужно ещё {need_more} гол(а) команды — слишком поздно",
            bet_text=bet,
        )
    return None


def _totals_sanity(
    c: ProviderSignalCandidate,
    family: str,
    h: int,
    a: int,
    fmt: FootballBetFormatterService,
) -> LiveSanityResult | None:
    if family != "totals":
        return None
    ctx: FootballTotalContext | None = fmt.describe_total_context(
        market_type=c.market.market_type,
        market_label=c.market.market_label,
        selection=c.market.selection,
        home_team=c.match.home_team,
        away_team=c.match.away_team,
        section_name=c.market.section_name,
        subsection_name=c.market.subsection_name,
    )
    if not ctx or not ctx.total_line:
        return None
    try:
        line = float((ctx.total_line or "0").replace(",", "."))
    except ValueError:
        return None
    side = (ctx.total_side or "").upper()
    h_goals, a_goals = h, a
    scope = (ctx.target_scope or "").lower()
    if ("home" in scope or "it1" in scope) and "away" not in scope:
        goals = h_goals
    elif ("away" in scope or "it2" in scope) and "home" not in scope:
        goals = a_goals
    else:
        goals = h_goals + a_goals

    bet = _format_bet_line(c)
    if side in ("ТБ", "O", "TB") or (ctx.total_side and ctx.total_side.upper() in ("ТБ",)):
        if goals > line + 1e-6:
            return LiveSanityResult(
                passed=False,
                plausibility="impossible",
                plausibility_score=5,
                block_token="blocked_impossible_live_outcome",
                reason_ru=f"Тотал: уже {goals} мяч(а), больше {line} сработал — сигнал бессмысленен",
                bet_text=bet,
            )
    if side in ("ТМ", "U", "TM") or (ctx.total_side and ctx.total_side.upper() in ("ТМ",)):
        if goals > line + 1e-6:
            return LiveSanityResult(
                passed=False,
                plausibility="impossible",
                plausibility_score=3,
                block_token="blocked_impossible_live_outcome",
                reason_ru=f"Тотал «меньше {line}»: в матче уже {goals} гол(а) > линии",
                bet_text=bet,
            )
    return None


def _result_late_comeback(
    c: ProviderSignalCandidate,
    pick: str,
    h: int,
    a: int,
    minute: int,
    mtype: str,
) -> LiveSanityResult | None:
    if mtype in ("double_chance",) or "double" in mtype:
        return None
    if pick is None or pick == "draw":
        return None
    diff = h - a if pick == "home" else (a - h if pick == "away" else 0)
    if diff >= 0:
        return None
    m = int(minute)
    bet = _format_bet_line(c)
    # Home «победа», счёт 2-3: diff = -1 if pick home, etc.
    if diff <= -2 and m >= 70:
        return LiveSanityResult(
            passed=False,
            plausibility="impossible",
            plausibility_score=8,
            block_token="blocked_impossible_live_outcome",
            reason_ru=f"Поздняя стадия ({m}'), отставание {abs(diff)} — победа в основное время с «пласта» 2+ маловероятна",
            bet_text=bet,
        )
    if diff <= -1 and m >= 82:
        return LiveSanityResult(
            passed=False,
            plausibility="impossible",
            plausibility_score=12,
            block_token="blocked_impossible_live_outcome",
            reason_ru=f"Очень конец ({m}'), идёте в победу при -1: как правило, не публикуем",
            bet_text=bet,
        )
    if diff <= -1 and m >= 78:
        return LiveSanityResult(
            passed=False,
            plausibility="weak",
            plausibility_score=45,
            block_token="blocked_low_live_plausibility",
            reason_ru=f"Поздно ({m}') при отставании 1+ на исход «победа» — plausibility низкий",
            bet_text=bet,
        )
    return None


def _coerce_int0(v: Any) -> int | None:
    if v is None:
        return None
    try:
        i = int(v)
    except (TypeError, ValueError):
        return None
    if i < 0:
        return None
    return i


def _as_tuple_from_dict(fa: dict) -> tuple[int | None, int | None, int | None]:
    h = _coerce_int0(fa.get("score_home"))
    a = _coerce_int0(fa.get("score_away"))
    mn = _coerce_int0(fa.get("minute"))
    return h, a, mn


class FootballLiveMarketSanityService:
    """Pre-send live-only checks: bet text, European handicap, totals vs score, late W outcome."""

    def _live_snapshot(
        self, c: ProviderSignalCandidate
    ) -> tuple[int | None, int | None, int | None]:
        raw: dict[str, Any] = dict(c.feature_snapshot_json or {})
        fa: dict | None = raw.get("football_analytics")
        if isinstance(fa, dict) and (fa.get("score_home") is not None or fa.get("score_away") is not None):
            return _as_tuple_from_dict(fa)
        sh, sa, mn, _, _ = FootballAnalyticsService()._extract_live_fields(raw)
        return sh, sa, mn

    def validate(
        self,
        c: ProviderSignalCandidate,
        family: str,
        family_svc: FootballSignalSendFilterService,
    ) -> LiveSanityResult:
        fmt = FootballBetFormatterService()
        bet = _format_bet_line(c)
        mtype = (c.market.market_type or "").strip()
        mlabel = (c.market.market_label or "")

        if _european_hcap_broken(mlabel, c.market.selection or "", bet, fmt):
            r = LiveSanityResult(
                passed=False,
                plausibility="impossible",
                plausibility_score=0,
                block_token="blocked_invalid_live_market_text",
                reason_ru="Европейский гандикап: нельзя да/нет — только 1/Х/2 или сторона",
                bet_text=bet,
            )
            return r
        if _next_goal_broken_bet(bet) and (
            "след" in mlabel.lower() or "след" in bet.lower() or "next goal" in mlabel.lower()
        ):
            return LiveSanityResult(
                passed=False,
                plausibility="impossible",
                plausibility_score=0,
                block_token="blocked_invalid_live_market_text",
                reason_ru="Следующий гол: кривой/обрезанный текст (минут/номер не распознаны)",
                bet_text=bet,
            )

        h, a, minute = self._live_snapshot(c)
        ok_default = LiveSanityResult(
            passed=True,
            plausibility="ok",
            plausibility_score=100,
            block_token="ok_live_sanity",
            reason_ru="live sanity: ok",
            bet_text=bet,
        )

        if h is not None and a is not None:
            tot = _totals_sanity(c, family, h, a, fmt)
            if tot and not tot.passed:
                return tot

        if family == "totals":
            qtot = _core_live_totals_quality(c, family, h, a, minute, fmt, bet)
            if qtot and not qtot.passed:
                return qtot

        mtl = mtype.lower()
        if (
            h is not None
            and a is not None
            and minute is not None
            and family == "result"
            and mtl in ("1x2", "match_winner")
            and not family_svc.is_corner_market(c)
        ):
            pick = _result_pick_side(c, mtype, fmt)
            if pick in ("home", "away"):
                late = _result_late_comeback(c, pick, h, a, int(minute), mtype)
                if late is not None and not late.passed:
                    return late
        if "служебно" in (bet or "").lower() and "гандик" in (bet or "").lower():
            return LiveSanityResult(
                passed=False,
                plausibility="impossible",
                plausibility_score=0,
                block_token="blocked_invalid_live_market_text",
                reason_ru="Европейский гандикап: сырой исход (да/нет) не допускается",
                bet_text=bet,
            )
        return ok_default

    def filter_finalists(
        self,
        finalists: list[ProviderSignalCandidate],
        family_svc: FootballSignalSendFilterService,
    ) -> tuple[list[ProviderSignalCandidate], list[tuple[ProviderSignalCandidate, LiveSanityResult]]]:
        kept: list[ProviderSignalCandidate] = []
        dropped: list[tuple[ProviderSignalCandidate, LiveSanityResult]] = []
        for c in finalists:
            if not getattr(c.match, "is_live", False):
                e = c.model_copy(
                    update={
                        "explanation_json": {
                            **(c.explanation_json or {}),
                            "live_sanity": {
                                "passed": True,
                                "skipped": "not_is_live",
                                "plausibility": "ok",
                            },
                        }
                    }
                )
                kept.append(e)
                continue
            fam = family_svc.get_market_family(c)
            r = self.validate(c, fam, family_svc)
            ex = {
                **(c.explanation_json or {}),
                "live_sanity": {
                    "passed": r.passed,
                    "plausibility": r.plausibility,
                    "plausibility_score": r.plausibility_score,
                    "block_token": r.block_token,
                    "reason_ru": r.reason_ru,
                    "bet_text": r.bet_text,
                },
            }
            c2 = c.model_copy(update={"explanation_json": ex})
            if r.passed:
                kept.append(c2)
            else:
                snap = self._live_snapshot(c)
                _log_sanity_reject(c, r, snap)
                dropped.append((c2, r))
        return kept, dropped


def _log_sanity_reject(
    c: ProviderSignalCandidate,
    r: LiveSanityResult,
    snap: tuple[int | None, int | None, int | None],
) -> None:
    h, a, mn = snap
    event_id = str(c.match.external_event_id or "—")
    mname = str(c.match.match_name or "—")
    mkt = str(c.market.market_type or "—")[:64]
    try:
        odds = str(c.market.odds_value)
    except Exception:
        odds = "—"
    logger.info(
        "[FOOTBALL][LIVE_SANITY] result=blocked event_id=%s match=%s score=%s minute=%s mkt_type=%s bet=%s odds=%s "
        "pl=%s pscore=%s code=%s reason=%s",
        event_id,
        mname[:200],
        f"{h}:{a}" if h is not None and a is not None else "—",
        str(mn) if mn is not None else "—",
        mkt,
        (r.bet_text or "—")[:200],
        odds,
        r.plausibility,
        r.plausibility_score,
        r.block_token,
        (r.reason_ru or "")[:500],
    )
