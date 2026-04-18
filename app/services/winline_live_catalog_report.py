"""Winline live football: debug catalog (raw step-4 → bridge event row → freshness).

Not part of the signal or scoring path; scan-only reporting.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterable, Sequence

from app.core.config import Settings, get_settings
from app.core.enums import BookmakerType, SportType
from app.schemas.provider_models import (
    ProviderMatch,
    ProviderOddsMarket,
    ProviderSignalCandidate,
)
from app.services.football_live_freshness_service import evaluate_live_event_staleness
from app.services.winline_live_feed_service import (
    _is_football_event,
    enrich_winline_event_for_ingest,
    winline_websocket_scan_accumulator,
)
from app.services.winline_raw_line_bridge_service import WinlineRawLineBridgeService
from app.services.winline_ws_live_binary_codec import attach_tip_templates

_FOLD_RE = re.compile(r"[-–—\s'\"`]+", re.UNICODE)


@dataclass(frozen=True)
class ExpectedMatchHint:
    home_substr: str
    away_substr: str
    label: str


# Substrings: both must appear somewhere in the two team names (RU/EN mix OK).
EXPECTED_FOOTBALL_LIVE_BY_SCREENSHOTS: tuple[ExpectedMatchHint, ...] = (
    ExpectedMatchHint("напол", "лаци", "Наполи — Лацио"),
    ExpectedMatchHint("мадей", "алвер", "Насьонал — Алверка"),
    ExpectedMatchHint("нефтех", "костр", "Нефтехимик — Кострома"),
    ExpectedMatchHint("вольфс", "линц", "Wolfsberg — BW Linz"),
    ExpectedMatchHint("тирол", "альтах", "Wacker/Тироль — Айтх/Альтах"),
    ExpectedMatchHint("вестер", "генк", "Westerlo — Genk"),
    ExpectedMatchHint("шарл", "станд", "Шарлеруа — Стандарт"),
    ExpectedMatchHint("ofi", "левад", "OFI — Левадиа"),
    ExpectedMatchHint("арис", "волос", "Арис — Волос"),
    ExpectedMatchHint("палер", "чезен", "Палермо — Чезена"),
    ExpectedMatchHint("омони", "поел", "Омония — АПОЕЛ"),
    ExpectedMatchHint("пафос", "ларна", "Пафос — АЕК/Ларнака"),
    # Limassol derby: two teams with Limassol + different club names
    ExpectedMatchHint("apollon", "лимас", "Apollon — (Лимассол)"),  # club name
    ExpectedMatchHint("arис", "лимас", "Aris L. — (Лимассол)"),
    ExpectedMatchHint("viking", "krc", "Викинг — KRC/Кра"),  # fuzzy
    ExpectedMatchHint("забж", "корон", "Гурник — Корона"),
    ExpectedMatchHint("спарта", "ялон", "Спарта — Яблонец"),
    ExpectedMatchHint("жилин", "слова", "Жилина — Слован"),
    ExpectedMatchHint("белин", "ньон", "Bellinzona — Nyon"),
    ExpectedMatchHint("ланд", "юнгс", "Landskrona — …"),
    ExpectedMatchHint("марие", "экен", "IFK M — Ekenäs"),
    ExpectedMatchHint("уль", "havel", "Ulm — H."),
    ExpectedMatchHint("zimbr", "milsa", "Зимбру — Milsami"),
    ExpectedMatchHint("радом", "koper", "Радомле — Копер"),
    ExpectedMatchHint("пахтак", "самарк", "Пахтакор — Самарканд"),
    ExpectedMatchHint("haverf", "llan", "Wales pair"),
    ExpectedMatchHint("barri", "saint", "Barry — New S."),
    ExpectedMatchHint("карди", "flint", "Cardiff M — Flint"),
    ExpectedMatchHint("томислав", "томис", "Bosnia pair Sloboda"),
    ExpectedMatchHint("будуч", "трав", "Buduć. — T."),
    ExpectedMatchHint("faso", "vitesse", "Real du F. — Vitesse"),
    ExpectedMatchHint("maj", "cfff", "Maj — FEB"),
    ExpectedMatchHint("бико", "gambi", "Biko — G. Lions"),
    ExpectedMatchHint("эспа", "саба", "U19 Espanol — Sabadell"),
    ExpectedMatchHint("адар", "бург", "U19 Adarve — Burgos"),
    ExpectedMatchHint("kanaa", "mosq", "U19 LA — Mosqu"),
)


@dataclass
class WinlineMatchCatalogRow:
    event_id: int
    stage_a_league: str
    home_team: str
    away_team: str
    raw_is_live: int | str | bool | None
    raw_date: str | None
    raw_time: str | None
    raw_source_time: str | None
    raw_numer: int | None
    line_count: int
    mappable_line_count: int
    in_feed_tips: bool
    b_ok: bool
    b_reason: str
    parsed_kickoff: str | None
    c_stale: bool
    c_reason: str
    one_line: str


@dataclass
class WinlineLiveCatalogReport:
    error: str | None
    prescan: int
    tipline_ok: bool
    scan_meta: str | None
    football_raw_count: int
    after_norm_count: int
    after_freshness_ok_count: int
    group_a: list[WinlineMatchCatalogRow]
    group_b: list[WinlineMatchCatalogRow]
    group_c: list[WinlineMatchCatalogRow]
    expected_matched: list[str] = field(default_factory=list)
    expected_unmatched: list[str] = field(default_factory=list)


def _norm(s: str) -> str:
    t = unicodedata.normalize("NFKC", s).casefold()
    t = t.replace("ё", "е")
    return _FOLD_RE.sub(" ", t).strip()


def _hint_in_names(home: str, away: str, ha: str, hb: str) -> bool:
    a, b_ = _norm(ha), _norm(hb)
    bag = f" {_norm(home)} {_norm(away)} "
    return a in bag and b_ in bag


def _parse_kickoff(val: object) -> datetime | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        d = val
    else:
        s = str(val).strip()
        if not s:
            return None
        try:
            s2 = s.replace("Z", "+00:00")
            d = datetime.fromisoformat(s2)
        except (ValueError, OSError, TypeError):
            return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d


def _mappable_lines_for_event(
    eid: int, lines: list[dict[str, Any]], tips: dict[int, dict[str, Any]]
) -> int:
    n = 0
    for ln in lines:
        if int(ln.get("idEvent") or 0) != int(eid):
            continue
        mid = ln.get("idTipMarket")
        try:
            mid_i = int(mid) if mid is not None else -1
        except (TypeError, ValueError):
            continue
        if mid_i in tips:
            n += 1
    return n


def _feed_like_attach_ok(eid: int, acc_lines: list[dict[str, Any]], tips: dict[int, dict[str, Any]]) -> bool:
    sub = [x for x in acc_lines if int(x.get("idEvent") or 0) == int(eid)]
    merged, _miss = attach_tip_templates(sub, tips)
    return bool(merged)


def _stale_for_row(e_row: dict[str, Any], eid: int, settings: Settings) -> tuple[bool, str]:
    es = e_row.get("event_start_at")
    m = ProviderMatch(
        external_event_id=str(eid),
        sport=SportType.FOOTBALL,
        tournament_name=str(e_row.get("tournament_name", "")),
        match_name=str(e_row.get("match_name", "")),
        home_team=str(e_row.get("home_team", "")),
        away_team=str(e_row.get("away_team", "")),
        event_start_at=_parse_kickoff(es),
        is_live=True,
        source_name="winline_live",
    )
    c = ProviderSignalCandidate(
        match=m,
        market=ProviderOddsMarket(
            bookmaker=BookmakerType.WINLINE,
            market_type="1x2",
            market_label="c",
            selection="1",
            odds_value=Decimal("1.5"),
        ),
        min_entry_odds=Decimal("1.5"),
        feature_snapshot_json={
            "source_name": "winline_live",
            "raw_event_id": str(eid),
            "winline_time": e_row.get("winline_time"),
            "winline_source_time": e_row.get("winline_source_time"),
            "winline_numer": e_row.get("winline_numer"),
        },
    )
    st, reas = evaluate_live_event_staleness(
        candidate=c,
        source_mode="winline_catalog",
        source_age_seconds=None,
        settings=settings,
    )
    return st, reas


def _one_line(r: WinlineMatchCatalogRow) -> str:
    sc = "—"  # score not in LIVE_EVENT_GET binary block here
    mn = r.raw_time or r.raw_source_time or (str(r.raw_numer) if r.raw_numer is not None else "—")
    stg = "C" if (r.b_ok and not r.c_stale) else "B" if r.b_ok else "A"
    if r.c_stale or not r.b_ok:
        res = f"REJECT|{r.c_reason if r.b_ok else r.b_reason}"
    else:
        res = f"ACCEPT|{r.c_reason}"
    lg = (r.stage_a_league or "—").replace("|", "/")
    return f"[{r.event_id}] {lg} | {r.home_team} — {r.away_team} | {sc} | {mn} | {stg} | {res}"


async def build_winline_football_live_catalog_report(
    settings: Settings | None = None,
) -> WinlineLiveCatalogReport:
    s = settings or get_settings()
    pres = int(s.winline_live_catalog_max_prescan or 450)
    acc, err = await winline_websocket_scan_accumulator(s, prescan=pres)
    if acc is None:
        return WinlineLiveCatalogReport(
            error=err,
            prescan=pres,
            tipline_ok=False,
            scan_meta=err,
            football_raw_count=0,
            after_norm_count=0,
            after_freshness_ok_count=0,
            group_a=[],
            group_b=[],
            group_c=[],
        )

    tipline_ok = bool(acc.tips)
    fball: list[int] = sorted(
        eid for eid, ev in acc.events.items() if _is_football_event(ev, acc.champs)
    )
    if not tipline_ok or not fball:
        return WinlineLiveCatalogReport(
            error="no_football_or_tips" if tipline_ok else "no_tipline",
            prescan=pres,
            tipline_ok=tipline_ok,
            scan_meta=None,
            football_raw_count=len(fball),
            after_norm_count=0,
            after_freshness_ok_count=0,
            group_a=[],
            group_b=[],
            group_c=[],
        )

    ch_map: dict[str, str] = {}
    for c in acc.champs.values():
        if not isinstance(c, dict) or c.get("id") is None:
            continue
        ch_map[str(c["id"])] = str(c.get("name", ""))
    br = WinlineRawLineBridgeService()
    a_rows: list[WinlineMatchCatalogRow] = []
    b_rows: list[WinlineMatchCatalogRow] = []
    c_rows: list[WinlineMatchCatalogRow] = []
    n_norm = 0
    n_fresh = 0
    for eid in fball:
        ev = acc.events.get(eid) or {}
        cobj = acc.champs.get(int(ev.get("idChampionship") or 0)) or {}
        league = str(cobj.get("name") or "—")
        mem = ev.get("members") or []
        home = str((mem[0] if len(mem) > 0 else "") or "—")
        away = str((mem[1] if len(mem) > 1 else "") or "—")
        n_lines = sum(1 for x in acc.lines if int(x.get("idEvent") or 0) == int(eid))
        mappable = _mappable_lines_for_event(eid, acc.lines, acc.tips)
        in_tips = _feed_like_attach_ok(eid, acc.lines, acc.tips)
        enriched = enrich_winline_event_for_ingest(ev, acc.champs)
        b_ok = False
        b_reas = "—"
        e_row: dict[str, Any] | None = None
        pko: str | None = None
        if not enriched:
            b_reas = "enrich_failed_members_champ"
        else:
            try:
                evs = br._build_events_from_raw([enriched], ch_map)  # noqa: SLF001
            except (ValueError, TypeError) as ex:
                b_reas = f"bridge_events:{ex!s}"
            else:
                e_row = next((r for r in evs if str(r.get("external_event_id")) == str(eid)), None)
                if e_row is None:
                    b_reas = "dropped_by_bridge_rules"
                else:
                    b_ok = True
                    b_reas = "ok"
                    pko = str(e_row.get("event_start_at") or "—")
        c_st, c_reas = (True, b_reas) if not b_ok or not e_row else _stale_for_row(e_row, eid, s)
        if b_ok and not c_st:
            n_fresh += 1
        if b_ok:
            n_norm += 1
        n_raw: int | None
        if ev.get("numer") is None:
            n_raw = None
        else:
            try:
                n_raw = int(ev["numer"])
            except (TypeError, ValueError):
                n_raw = None
        row_wo = WinlineMatchCatalogRow(
            event_id=eid,
            stage_a_league=league,
            home_team=home,
            away_team=away,
            raw_is_live=ev.get("isLive"),
            raw_date=ev.get("date") and str(ev.get("date")) or None,
            raw_time=ev.get("time") and str(ev.get("time")) or None,
            raw_source_time=ev.get("sourceTime") and str(ev.get("sourceTime")) or None,
            raw_numer=n_raw,
            line_count=n_lines,
            mappable_line_count=mappable,
            in_feed_tips=in_tips,
            b_ok=b_ok,
            b_reason=b_reas,
            parsed_kickoff=pko,
            c_stale=c_st,
            c_reason=c_reas,
            one_line="",
        )
        row = replace(row_wo, one_line=_one_line(row_wo))
        a_rows.append(row)
        if b_ok:
            b_rows.append(row)
        if b_ok and not c_st:
            c_rows.append(row)
    em, uem = _match_expected_hints(EXPECTED_FOOTBALL_LIVE_BY_SCREENSHOTS, a_rows)
    eids_in_lines: set[int] = set()
    for ln in acc.lines:
        try:
            eids_in_lines.add(int(ln.get("idEvent") or 0))
        except (TypeError, ValueError):
            continue
    eids_in_lines.discard(0)
    n_ev_acc = len(acc.events)
    return WinlineLiveCatalogReport(
        error=None,
        prescan=pres,
        tipline_ok=True,
        scan_meta=(
            f"football_in_snapshot={len(fball)} events_in_acc={n_ev_acc} "
            f"distinct_idEvent_in_lines={len(eids_in_lines)} acc_lines={len(acc.lines)} champs={len(acc.champs)}"
        ),
        football_raw_count=len(fball),
        after_norm_count=n_norm,
        after_freshness_ok_count=n_fresh,
        group_a=a_rows,
        group_b=b_rows,
        group_c=c_rows,
        expected_matched=em,
        expected_unmatched=uem,
    )


def _match_expected_hints(
    hints: Sequence[ExpectedMatchHint], rows: Iterable[WinlineMatchCatalogRow]
) -> tuple[list[str], list[str]]:
    rlist = list(rows)
    matched: list[str] = []
    unmatched: list[str] = []
    for h in hints:
        hit = next(
            (r for r in rlist if _hint_in_names(r.home_team, r.away_team, h.home_substr, h.away_substr)),
            None,
        )
        if hit:
            matched.append(f"{h.label} → eid={hit.event_id} {hit.home_team} / {hit.away_team}")
        else:
            unmatched.append(f"{h.label}")
    return matched, unmatched
