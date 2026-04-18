from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel


class RawEventItem(BaseModel):
    external_event_id: str
    sport: str
    tournament_name: str
    match_name: str
    home_team: str
    away_team: str
    event_start_at: datetime | None = None
    is_live: bool = False
    # Winline LIVE binary: match clock and scheduled kickoff are separate; used for fresh/live checks
    winline_time: str | None = None
    winline_source_time: str | None = None
    winline_numer: int | None = None
    # Optional live context (if provider supplies it)
    score_home: int | None = None
    score_away: int | None = None
    minute: int | None = None
    period: str | None = None
    live_state: str | None = None
    raw_json: dict[str, Any] | None = None


class RawMarketItem(BaseModel):
    external_event_id: str
    bookmaker: str
    market_type: str
    market_label: str
    selection: str
    odds_value: Decimal
    section_name: str | None = None
    subsection_name: str | None = None
    search_hint: str | None = None
    raw_json: dict[str, Any] | None = None


class RawProviderPayload(BaseModel):
    source_name: str
    events: list[RawEventItem]
    markets: list[RawMarketItem]

