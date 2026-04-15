from __future__ import annotations

from app.schemas.candidate_filter import (
    CandidateFilterBatchResult,
    CandidateFilterConfig,
    CandidateFilterDecision,
)
from app.schemas.provider_models import ProviderSignalCandidate


class CandidateFilterService:
    def filter_candidate(self, candidate: ProviderSignalCandidate, config: CandidateFilterConfig) -> CandidateFilterDecision:
        """Apply sequential rule-based filtering to a single provider candidate."""
        market = candidate.market
        match = candidate.match

        if market.bookmaker not in config.allowed_bookmakers:
            return CandidateFilterDecision(accepted=False, reason="bookmaker_not_allowed")

        if match.sport not in config.allowed_sports:
            return CandidateFilterDecision(accepted=False, reason="sport_not_allowed")

        if match.is_live is True and config.allow_live is False:
            return CandidateFilterDecision(accepted=False, reason="live_not_allowed")

        if match.is_live is False and config.allow_prematch is False:
            return CandidateFilterDecision(accepted=False, reason="prematch_not_allowed")

        normalized_market_type = self._normalize_market_type(market.market_type)
        if normalized_market_type not in config.allowed_market_types:
            return CandidateFilterDecision(
                accepted=False,
                reason="market_type_not_allowed",
                normalized_market_type=normalized_market_type,
            )

        if config.min_odds is not None and market.odds_value < config.min_odds:
            return CandidateFilterDecision(
                accepted=False,
                reason="odds_below_min",
                normalized_market_type=normalized_market_type,
            )
        if config.max_odds is not None and market.odds_value > config.max_odds:
            return CandidateFilterDecision(
                accepted=False,
                reason="odds_above_max",
                normalized_market_type=normalized_market_type,
            )

        if config.require_search_hint and not (market.search_hint or "").strip():
            return CandidateFilterDecision(
                accepted=False,
                reason="missing_search_hint",
                normalized_market_type=normalized_market_type,
            )
        if config.require_section_name and not (market.section_name or "").strip():
            return CandidateFilterDecision(
                accepted=False,
                reason="missing_section_name",
                normalized_market_type=normalized_market_type,
            )
        if config.require_subsection_name and not (market.subsection_name or "").strip():
            return CandidateFilterDecision(
                accepted=False,
                reason="missing_subsection_name",
                normalized_market_type=normalized_market_type,
            )

        return CandidateFilterDecision(
            accepted=True,
            reason="accepted",
            normalized_market_type=normalized_market_type,
        )

    def filter_candidates(
        self, candidates: list[ProviderSignalCandidate], config: CandidateFilterConfig
    ) -> CandidateFilterBatchResult:
        """Filter a batch of candidates and return accepted list plus rejection statistics."""
        accepted: list[ProviderSignalCandidate] = []
        rejection_reasons: dict[str, int] = {}

        for c in candidates:
            decision = self.filter_candidate(c, config)
            if decision.accepted:
                # Return accepted candidates with normalized market_type applied
                updated_market = c.market.model_copy(update={"market_type": decision.normalized_market_type})
                accepted.append(c.model_copy(update={"market": updated_market}))
            else:
                rejection_reasons[decision.reason] = rejection_reasons.get(decision.reason, 0) + 1

        return CandidateFilterBatchResult(
            accepted_candidates=accepted,
            rejected_count=sum(rejection_reasons.values()),
            accepted_count=len(accepted),
            rejection_reasons=rejection_reasons,
        )

    def _normalize_market_type(self, value: str) -> str:
        v = (value or "").strip().lower().replace(" ", "_")

        aliases = {
            "winner": "match_winner",
            "match_winner": "match_winner",
            "moneyline": "match_winner",
            "map_winner": "map_winner",
            "map_wins": "map_winner",
            "winner_map": "map_winner",
            "maps_total": "maps_total",
            "total_maps": "maps_total",
            "map_total": "maps_total",
            "handicap_maps": "handicap_maps",
            "map_handicap": "handicap_maps",
            "1x2": "1x2",
            "match_result": "1x2",
            "full_time_result": "1x2",
            "total_goals": "total_goals",
            "goals_total": "total_goals",
            "over_under_goals": "total_goals",
            "handicap": "handicap",
            "asian_handicap": "handicap",
        }
        return aliases.get(v, v)

