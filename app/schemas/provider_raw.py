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

