from __future__ import annotations

from app.schemas.deduplication import DeduplicationBatchResult, SignalDeduplicationKey
from app.schemas.provider_models import ProviderSignalCandidate


class DeduplicationService:
    def build_key(self, candidate: ProviderSignalCandidate) -> SignalDeduplicationKey:
        """Build deterministic deduplication key for a provider candidate."""
        match = candidate.match
        market = candidate.market

        event_external_id = (match.external_event_id or "").strip() or None

        return SignalDeduplicationKey(
            sport=match.sport,
            bookmaker=market.bookmaker,
            event_external_id=event_external_id,
            home_team=(match.home_team or "").strip(),
            away_team=(match.away_team or "").strip(),
            market_type=market.market_type,  # already normalized upstream
            selection=(market.selection or "").strip(),
            is_live=bool(match.is_live),
            event_start_at=match.event_start_at,
        )

    def deduplicate_candidates(self, candidates: list[ProviderSignalCandidate]) -> DeduplicationBatchResult:
        """Deduplicate candidates within a batch (preserves order, no mutation)."""
        seen: set[str] = set()
        unique: list[ProviderSignalCandidate] = []
        duplicate_reasons: dict[str, int] = {}

        for c in candidates:
            key = self.build_key(c)
            key_str = key.model_dump_json()
            if key_str in seen:
                duplicate_reasons["duplicate_in_batch"] = duplicate_reasons.get("duplicate_in_batch", 0) + 1
                continue
            seen.add(key_str)
            unique.append(c)

        return DeduplicationBatchResult(
            unique_candidates=unique,
            duplicate_count=sum(duplicate_reasons.values()),
            unique_count=len(unique),
            duplicate_reasons=duplicate_reasons,
        )

