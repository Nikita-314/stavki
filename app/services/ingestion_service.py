from __future__ import annotations

from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.provider_models import ProviderBatchIngestResult, ProviderSignalCandidate
from app.schemas.signal import PredictionLogCreate, SignalCreate, SignalCreateBundle
from app.services.signal_service import SignalService


class IngestionService:
    def __init__(self, signal_service: SignalService | None = None) -> None:
        self._signal_service = signal_service or SignalService()

    async def ingest_candidates(
        self, session: AsyncSession, candidates: list[ProviderSignalCandidate]
    ) -> ProviderBatchIngestResult:
        """Ingest a batch of provider candidates into Signal + PredictionLog (no commit).

        - maps each candidate into SignalCreateBundle
        - creates Signal + PredictionLog via SignalService
        - skips invalid candidates without stopping the batch
        """
        created_ids: list[int] = []
        skipped = 0

        for candidate in candidates:
            try:
                bundle = self._candidate_to_bundle(candidate)
                signal = await self._signal_service.create_signal_with_prediction_log(session, bundle)
                created_ids.append(int(signal.id))
            except (ValidationError, ValueError, TypeError):
                skipped += 1
                continue

        return ProviderBatchIngestResult(
            total_candidates=len(candidates),
            created_signals=len(created_ids),
            skipped_candidates=skipped,
            created_signal_ids=created_ids,
        )

    def _candidate_to_bundle(self, candidate: ProviderSignalCandidate) -> SignalCreateBundle:
        match = candidate.match
        market = candidate.market

        signal = SignalCreate(
            sport=match.sport,
            bookmaker=market.bookmaker,
            event_external_id=match.external_event_id,
            tournament_name=match.tournament_name,
            match_name=match.match_name,
            home_team=match.home_team,
            away_team=match.away_team,
            market_type=market.market_type,
            market_label=market.market_label,
            selection=market.selection,
            odds_at_signal=market.odds_value,
            min_entry_odds=candidate.min_entry_odds,
            predicted_prob=candidate.predicted_prob,
            implied_prob=candidate.implied_prob,
            edge=candidate.edge,
            model_name=candidate.model_name,
            model_version_name=candidate.model_version_name,
            signal_score=candidate.signal_score,
            section_name=market.section_name,
            subsection_name=market.subsection_name,
            search_hint=market.search_hint,
            is_live=match.is_live,
            event_start_at=match.event_start_at,
            notes=candidate.notes,
        )

        prediction_log = PredictionLogCreate(
            feature_snapshot_json=candidate.feature_snapshot_json,
            raw_model_output_json=candidate.raw_model_output_json,
            explanation_json=candidate.explanation_json,
        )

        return SignalCreateBundle(signal=signal, prediction_log=prediction_log)

