from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel


class WinlineRawEventItem(BaseModel):
    event_external_id: str
    sport: str
    tournament_name: str
    match_name: str
    home_team: str
    away_team: str
    event_start_at: datetime | None = None
    is_live: bool = False
    raw_json: dict[str, Any] | None = None


class WinlineRawMarketItem(BaseModel):
    event_external_id: str
    market_type: str
    market_label: str
    selection: str
    odds_value: Decimal
    section_name: str | None = None
    subsection_name: str | None = None
    search_hint: str | None = None
    raw_json: dict[str, Any] | None = None


class WinlineRawPayload(BaseModel):
    source_name: str
    events: list[WinlineRawEventItem]
    markets: list[WinlineRawMarketItem]


class WinlineRawResultItem(BaseModel):
    event_external_id: str
    winner_selection: str | None = None
    is_void: bool = False
    settled_at: datetime | None = None
    raw_json: dict[str, Any] | None = None


class WinlineRawResultPayload(BaseModel):
    source_name: str
    results: list[WinlineRawResultItem]
