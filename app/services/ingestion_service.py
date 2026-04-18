from __future__ import annotations

import logging
from typing import Any

from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import SportType
from app.schemas.candidate_filter import CandidateFilterConfig
from app.schemas.provider_models import ProviderBatchIngestResult, ProviderSignalCandidate
from app.schemas.signal import PredictionLogCreate, SignalCreate, SignalCreateBundle
from app.services.candidate_filter_service import CandidateFilterService
from app.services.deduplication_service import DeduplicationService
from app.services.signal_service import SignalService
from app.db.repositories.signal_repository import SignalRepository


logger = logging.getLogger(__name__)


class IngestionService:
    def __init__(self, signal_service: SignalService | None = None) -> None:
        self._signal_service = signal_service or SignalService()

    def candidate_to_bundle(self, candidate: ProviderSignalCandidate) -> SignalCreateBundle:
        """Public helper: map provider candidate to SignalCreateBundle (no side effects)."""
        return self._candidate_to_bundle(candidate)

    async def ingest_candidates(
        self,
        session: AsyncSession,
        candidates: list[ProviderSignalCandidate],
        *,
        dedup_exclude_notes: tuple[str, ...] = (),
        dedup_required_notes: tuple[str, ...] = (),
        dedup_relaxed_semi_manual: bool = False,
        dedup_relaxed_minutes: int = 30,
    ) -> ProviderBatchIngestResult:
        """Ingest a batch of provider candidates into Signal + PredictionLog (no commit).

        - maps each candidate into SignalCreateBundle
        - creates Signal + PredictionLog via SignalService
        - skips invalid candidates without stopping the batch
        """
        created_ids: list[int] = []
        created_pairs: list[ProviderSignalCandidate] = []
        skipped = 0

        for candidate in candidates:
            try:
                bundle = self._candidate_to_bundle(candidate)

                # DB-level dedup check (exact match by key fields).
                relaxed = bool(dedup_relaxed_semi_manual)
                existing = await SignalRepository().find_existing_similar_signal(
                    session,
                    sport=bundle.signal.sport,
                    bookmaker=bundle.signal.bookmaker,
                    event_external_id=bundle.signal.event_external_id,
                    home_team=bundle.signal.home_team,
                    away_team=bundle.signal.away_team,
                    market_type=bundle.signal.market_type,
                    selection=bundle.signal.selection,
                    is_live=bundle.signal.is_live,
                    exclude_notes=dedup_exclude_notes,
                    required_notes=dedup_required_notes,
                    relaxed_semi_manual=relaxed,
                    candidate_odds=bundle.signal.odds_at_signal,
                    candidate_event_start_at=bundle.signal.event_start_at,
                    relaxed_interval_minutes=int(dedup_relaxed_minutes),
                )
                if existing is not None:
                    logger.info(
                        "[FOOTBALL][DEDUP] reason=duplicate_in_db existing_signal_id=%s event_external_id=%s "
                        "market_type=%s selection=%s is_live=%s match_name=%s signaled_at=%s decision=blocked relaxed=%s",
                        existing.id,
                        existing.event_external_id,
                        existing.market_type,
                        existing.selection,
                        str(bool(existing.is_live)).lower(),
                        existing.match_name,
                        existing.signaled_at.isoformat() if existing.signaled_at else None,
                        str(relaxed).lower(),
                    )
                    skipped += 1
                    continue

                signal = await self._signal_service.create_signal_with_prediction_log(session, bundle)
                created_ids.append(int(signal.id))
                created_pairs.append(candidate)
            except (ValidationError, ValueError, TypeError):
                skipped += 1
                continue

        return ProviderBatchIngestResult(
            total_candidates=len(candidates),
            created_signals=len(created_ids),
            skipped_candidates=skipped,
            created_signal_ids=created_ids,
            created_from_candidates=created_pairs,
        )

    async def ingest_candidates_with_filter(
        self,
        session: AsyncSession,
        candidates: list[ProviderSignalCandidate],
        config: CandidateFilterConfig | None = None,
    ) -> ProviderBatchIngestResult:
        """Filter candidates and ingest only accepted ones (no commit).

        If config is not provided, uses defaults for russian manual betting.
        """
        config = config or CandidateFilterConfig.default_for_russian_manual_betting()
        batch = CandidateFilterService().filter_candidates(candidates, config)
        # TODO: later extend ProviderBatchIngestResult with filter rejection stats if needed.
        return await self.ingest_candidates(session, batch.accepted_candidates)

    async def ingest_candidates_with_filter_and_dedup(
        self,
        session: AsyncSession,
        candidates: list[ProviderSignalCandidate],
        config: CandidateFilterConfig | None = None,
    ) -> ProviderBatchIngestResult:
        """Filter, deduplicate in-batch, then ingest (no commit)."""
        config = config or CandidateFilterConfig.default_for_russian_manual_betting()
        filtered = CandidateFilterService().filter_candidates(candidates, config)
        deduped = DeduplicationService().deduplicate_candidates(filtered.accepted_candidates)
        # TODO: later extend ProviderBatchIngestResult with filter/dedup stats if needed.
        return await self.ingest_candidates(session, deduped.unique_candidates)

    def _candidate_to_bundle(self, candidate: ProviderSignalCandidate) -> SignalCreateBundle:
        match = candidate.match
        market = candidate.market

        fs0: dict[str, Any] = dict(candidate.feature_snapshot_json or {})
        ex0: dict[str, Any] = dict(candidate.explanation_json or {})
        if match.sport == SportType.FOOTBALL:
            sp = ex0.get("football_live_send_path")
            audit: dict[str, Any] = {
                "main_market": f"{market.market_type}:{(market.market_label or '')[:120]}",
                "selection": (market.selection or "")[:200],
                "is_live": bool(match.is_live),
            }
            if sp in ("soft", "normal"):
                audit["send_path"] = sp
            elif sp is not None and str(sp).strip():
                audit["send_path"] = str(sp)[:48]
            ls = ex0.get("live_sanity")
            if isinstance(ls, dict) and ls:
                slim = {k: ls[k] for k in ("plausibility", "plausibility_score", "block_token") if k in ls}
                if slim:
                    audit["live_sanity"] = slim
            for key in ("score_home", "score_away", "live_minute", "match_minute"):
                v = fs0.get(key)
                if v is not None:
                    audit[key] = v
            prev = fs0.get("football_send_audit")
            if isinstance(prev, dict):
                merged = {**prev, **audit}
            else:
                merged = audit
            fs0["football_send_audit"] = merged

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
            feature_snapshot_json=fs0,
            raw_model_output_json=candidate.raw_model_output_json,
            explanation_json=candidate.explanation_json,
        )

        return SignalCreateBundle(signal=signal, prediction_log=prediction_log)

