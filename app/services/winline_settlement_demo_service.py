"""Manual Winline line + result demo: ingest sample JSON → settle via `ResultIngestionService`.

No background loop; call only from debug/handlers or CLI. DB writes + commit inside sessions.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.enums import SportType
from app.schemas.event_result import EventResultInput
from app.services.adapter_ingestion_service import AdapterIngestionService
from app.services.balance_service import BalanceService
from app.services.result_ingestion_service import ResultIngestionService
from app.services.sanity_check_service import SanityCheckService


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    s = str(value).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


def _map_sport(raw: str | None) -> SportType:
    key = (raw or "").strip().lower()
    if key in {"football", "soccer"}:
        return SportType.FOOTBALL
    if key == "cs2":
        return SportType.CS2
    if key in {"dota2", "dota 2"}:
        return SportType.DOTA2
    raise ValueError(f"Unsupported sport for demo result: {raw!r}")


def normalize_winline_line_payload(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Map repo `examples/winline_line_sample.json` into `GenericOddsAdapter` shape."""
    events_out: list[dict[str, Any]] = []
    for e in raw.get("events") or []:
        if not isinstance(e, dict):
            continue
        eid = e.get("external_event_id") or e.get("event_external_id")
        if not eid:
            continue
        events_out.append(
            {
                "external_event_id": str(eid),
                "sport": e.get("sport", ""),
                "tournament_name": e.get("tournament_name", ""),
                "match_name": e.get("match_name", ""),
                "home_team": e.get("home_team", ""),
                "away_team": e.get("away_team", ""),
                "event_start_at": e.get("event_start_at"),
                "is_live": bool(e.get("is_live", False)),
            }
        )

    markets_out: list[dict[str, Any]] = []
    for m in raw.get("markets") or []:
        if not isinstance(m, dict):
            continue
        eid = m.get("external_event_id") or m.get("event_external_id")
        if not eid:
            continue
        ov = m.get("odds_value")
        markets_out.append(
            {
                "external_event_id": str(eid),
                "bookmaker": "winline",
                "market_type": str(m.get("market_type", "")),
                "market_label": str(m.get("market_label", "")),
                "selection": str(m.get("selection", "")),
                "odds_value": ov if isinstance(ov, (int, float, Decimal, str)) else str(ov),
                "section_name": m.get("section_name"),
                "subsection_name": m.get("subsection_name"),
                "search_hint": m.get("search_hint"),
            }
        )

    return {
        "source_name": str(raw.get("source_name") or "winline"),
        "events": events_out,
        "markets": markets_out,
    }


class WinlineSettlementDemoService:
    """Load packaged Winline JSON, ingest line, process results, return structured report."""

    def __init__(self, *, examples_dir: Path | None = None) -> None:
        self._examples_dir = examples_dir or (_repo_root() / "examples")

    def load_line_payload(self) -> dict[str, Any]:
        path = self._examples_dir / "winline_line_sample.json"
        raw = json.loads(path.read_text(encoding="utf-8"))
        return normalize_winline_line_payload(raw)

    def load_result_payload(self) -> dict[str, Any]:
        path = self._examples_dir / "winline_result_sample.json"
        return json.loads(path.read_text(encoding="utf-8"))

    async def run_demo_and_collect(
        self, sessionmaker: async_sessionmaker[AsyncSession]
    ) -> dict[str, Any]:
        out: dict[str, Any] = {
            "ok": True,
            "error": None,
            "preview_candidates": 0,
            "adapter_candidates": 0,
            "created_signals": 0,
            "created_signal_ids": [],
            "skipped_candidates": 0,
            "raw_results": 0,
            "processed_event_results": [],
            "settled_signal_ids": [],
            "wins": 0,
            "losses": 0,
            "voids": 0,
            "current_balance_rub": None,
            "sanity_issues_count": 0,
            "intersection_event_ids": [],
        }

        try:
            payload = self.load_line_payload()
            result_payload = self.load_result_payload()
        except Exception as exc:  # noqa: BLE001 — demo surface: surface IO errors to Telegram
            out["ok"] = False
            out["error"] = f"load: {exc!s}"
            return out

        ad = AdapterIngestionService()
        try:
            preview = ad.preview_payload(payload)
        except Exception as exc:
            out["ok"] = False
            out["error"] = f"preview: {exc!s}"
            return out

        out["preview_candidates"] = len(preview.candidates)
        out["adapter_candidates"] = len(preview.candidates)

        line_event_ids = {
            str(e.get("external_event_id") or e.get("event_external_id"))
            for e in (payload.get("events") or [])
            if isinstance(e, dict) and (e.get("external_event_id") or e.get("event_external_id"))
        }
        raw_results = list(result_payload.get("results") or [])
        out["raw_results"] = len(raw_results)
        result_event_ids = {
            str(r.get("event_external_id"))
            for r in raw_results
            if isinstance(r, dict) and r.get("event_external_id") is not None
        }
        out["intersection_event_ids"] = sorted(line_event_ids & result_event_ids)

        try:
            async with sessionmaker() as session:
                _ar, ingest = await ad.ingest_payload(session, payload)
                await session.commit()
                out["created_signals"] = ingest.created_signals
                out["created_signal_ids"] = list(ingest.created_signal_ids)
                out["skipped_candidates"] = ingest.skipped_candidates
        except Exception as exc:
            out["ok"] = False
            out["error"] = f"ingest: {exc!s}"
            return out

        settled_all: list[int] = []
        wins = losses = voids = 0

        for row in raw_results:
            if not isinstance(row, dict):
                continue
            eid = row.get("event_external_id")
            if not eid:
                continue
            try:
                sport = _map_sport(row.get("sport"))
                inp = EventResultInput(
                    event_external_id=str(eid),
                    sport=sport,
                    winner_selection=row.get("winner_selection"),
                    is_void=bool(row.get("is_void", False)),
                    settled_at=_parse_dt(row.get("settled_at")),
                    result_payload_json=row if isinstance(row, dict) else None,
                )
                async with sessionmaker() as session:
                    pr = await ResultIngestionService().process_event_result(session, inp)
                    await session.commit()
                    settled_all.extend(pr.processed_signal_ids)
                    out["processed_event_results"].append(
                        {
                            "event_external_id": str(eid),
                            "total_signals_found": pr.total_signals_found,
                            "settled_signals": pr.settled_signals,
                            "skipped_signals": pr.skipped_signals,
                            "processed_signal_ids": list(pr.processed_signal_ids),
                        }
                    )
            except Exception as exc:
                out["ok"] = False
                err = out.get("error")
                extra = f"result[{eid}]: {exc!s}"
                out["error"] = f"{err}; {extra}" if err else extra

        out["settled_signal_ids"] = sorted(set(settled_all))

        try:
            async with sessionmaker() as session:
                realistic = await BalanceService().get_realistic_balance_overview(session)
                sanity = await SanityCheckService().run_sanity_check(session)
                out["current_balance_rub"] = realistic.current_balance_rub
                out["wins"] = int(realistic.wins)
                out["losses"] = int(realistic.losses)
                out["voids"] = int(realistic.voids)
                out["sanity_issues_count"] = len(sanity.issues)
        except Exception as exc:
            out["ok"] = False
            err = out.get("error")
            extra = f"summary: {exc!s}"
            out["error"] = f"{err}; {extra}" if err else extra

        return out

    async def run_demo(self, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
        """CLI-friendly: structured run + print."""
        data = await self.run_demo_and_collect(sessionmaker)
        self.print_demo_report(data)

    def print_demo_report(self, data: Mapping[str, Any]) -> None:
        """Human-readable dump for local runs."""
        print("=== Winline settlement demo ===")
        for key in (
            "ok",
            "error",
            "preview_candidates",
            "created_signals",
            "raw_results",
            "wins",
            "losses",
            "voids",
            "current_balance_rub",
            "sanity_issues_count",
            "intersection_event_ids",
        ):
            print(f"{key}: {data.get(key)}")
        print(f"created_signal_ids: {data.get('created_signal_ids')}")
        print(f"settled_signal_ids: {data.get('settled_signal_ids')}")


async def _cli() -> None:
    from app.core.config import get_settings
    from app.db.session import create_engine, create_sessionmaker

    settings = get_settings()
    engine = create_engine(settings.database_url, echo=False)
    sm = create_sessionmaker(engine)
    try:
        await WinlineSettlementDemoService().run_demo(sm)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    import asyncio

    asyncio.run(_cli())
