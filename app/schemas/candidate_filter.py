from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field

from app.core.enums import BookmakerType, SportType
from app.schemas.provider_models import ProviderSignalCandidate


class CandidateFilterConfig(BaseModel):
    allowed_bookmakers: list[BookmakerType] = Field(default_factory=list)
    allowed_sports: list[SportType] = Field(default_factory=list)
    allowed_market_types: list[str] = Field(default_factory=list)

    allow_live: bool = True
    allow_prematch: bool = True

    require_search_hint: bool = False
    require_section_name: bool = False
    require_subsection_name: bool = False

    min_odds: Decimal | None = None
    max_odds: Decimal | None = None

    @classmethod
    def default_for_russian_manual_betting(cls) -> "CandidateFilterConfig":
        return cls(
            allowed_bookmakers=[BookmakerType.FONBET, BookmakerType.WINLINE, BookmakerType.BETBOOM],
            allowed_sports=[SportType.CS2, SportType.DOTA2, SportType.FOOTBALL],
            allowed_market_types=[
                "match_winner",
                "map_winner",
                "maps_total",
                "handicap_maps",
                "1x2",
                "total_goals",
                "handicap",
            ],
            allow_live=True,
            allow_prematch=True,
            require_search_hint=False,
            require_section_name=False,
            require_subsection_name=False,
            min_odds=Decimal("1.20"),
            max_odds=Decimal("10.00"),
        )


class CandidateFilterDecision(BaseModel):
    accepted: bool
    reason: str
    normalized_market_type: str | None = None


class CandidateFilterBatchResult(BaseModel):
    accepted_candidates: list[ProviderSignalCandidate]
    rejected_count: int
    accepted_count: int
    rejection_reasons: dict[str, int]

