from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from threading import Lock
from typing import Any


@dataclass
class SignalRuntimeDiagnosticsState:
    updated_at: str | None = None
    active_mode: str = "football"
    football_source: str | None = None
    football_fallback_source: str | None = None
    live_provider_name: str | None = None
    live_auth_status: str | None = None
    last_live_http_status: int | None = None
    last_live_endpoint: str | None = None
    last_live_error_body: str | None = None
    fallback_source_available: bool = False
    manual_production_fallback_allowed: bool = False
    source_mode: str | None = None
    is_real_source: bool = False
    source_origin: str | None = None
    upload_provenance_present: bool = False
    uploaded_at: str | None = None
    source_file_path: str | None = None
    source_checksum: str | None = None
    last_fetch_status: str | None = None
    last_error: str | None = None
    last_delivery_reason: str | None = None
    raw_events_count: int = 0
    normalized_markets_count: int = 0
    candidates_before_filter_count: int = 0
    candidates_after_filter_count: int = 0
    football_candidates_count: int = 0
    football_real_candidates_count: int = 0
    football_after_filter_count: int = 0
    football_after_integrity_count: int = 0
    dropped_invalid_market_mapping_count: int = 0
    dropped_invalid_total_scope_count: int = 0
    dropped_too_far_in_time_count: int = 0
    live_matches_count: int = 0
    near_matches_count: int = 0
    too_far_matches_count: int = 0
    selected_match_reason: str | None = None
    football_sent_count: int = 0
    final_signals_count: int = 0
    messages_sent_count: int = 0
    preview_only: bool = False
    fallback_used: bool = False
    note: str | None = None
    football_analytics_enabled: bool = True
    football_learning_enabled: bool = True
    football_learning_families_tracked: int = 0
    football_live_fields_in_last_cycle: bool = False
    football_injuries_data_available: bool = False
    football_line_movement_available: bool = False
    football_live_session_active: bool = False
    football_live_session_started_at: str | None = None
    football_live_session_expires_at: str | None = None
    football_live_session_persistent: bool = False
    """True when session has no wall-clock expiry (runs until ⏸ Стоп)."""
    football_live_session_last_cycle_at: str | None = None
    football_live_session_remaining_minutes: float | None = None
    football_live_signals_sent_session: int = 0
    football_live_telegram_sent_session: int = 0
    football_live_duplicate_ideas_blocked: int = 0
    football_live_sent_ideas_count: int = 0
    football_live_cycle_live_matches_found: int = 0
    football_live_cycle_candidates_before_filter: int = 0
    football_live_cycle_after_send_filter: int = 0
    football_live_cycle_after_integrity: int = 0
    football_live_cycle_after_strategy: int = 0
    football_live_cycle_after_s8: int = 0
    football_live_cycle_after_s9: int = 0
    football_live_cycle_after_s10: int = 0
    football_live_cycle_after_s11: int = 0
    football_live_cycle_after_s12_controlled: int = 0
    football_live_cycle_after_context_filter: int = 0
    football_live_cycle_after_value_filter: int = 0
    football_live_cycle_after_score: int = 0
    football_live_cycle_new_ideas_sendable: int = 0
    football_live_cycle_duplicate_ideas_blocked: int = 0
    football_live_cycle_bottleneck: str | None = None
    football_live_cycle_bottleneck_ru: str | None = None
    """Human RU label for football_live_cycle_bottleneck (same cycle)."""
    football_live_effective_source: str | None = None
    football_live_last_notify_path: str | None = None
    football_live_source_timestamp: str | None = None
    football_live_source_age_seconds: float | None = None
    football_live_stale_source: bool = False
    football_live_source_freshness: str | None = None
    football_live_freshness_candidates_before: int = 0
    football_live_freshness_live_events_accepted: int = 0
    football_live_freshness_stale_events_dropped: int = 0
    football_live_freshness_stale_markets_dropped: int = 0
    football_live_quality_fresh_matches: int = 0
    football_live_quality_strong_idea_matches: int = 0
    football_live_quality_no_sendable_matches: int = 0
    football_live_quality_main_blocker: str | None = None
    football_live_quality_main_blocker_ru: str | None = None
    football_live_best_scores_distribution_hint: str | None = None
    football_live_min_signal_score_base: float | None = None
    football_live_min_signal_score_effective: float | None = None
    football_live_score_relief_note: str | None = None
    football_live_quality_hint_ru: str | None = None
    football_live_normal_sendable_count: int = 0
    football_live_soft_sendable_count: int = 0
    football_live_soft_sendable_tight_count: int = 0
    football_live_soft_sendable_relief_single_count: int = 0
    football_live_rejected_at_send_gate: int = 0
    """Candidates scored but rejected by live send gate (classify: reject)."""

    api_football_intelligence_attempted: int = 0
    api_football_intelligence_mapped: int = 0
    api_football_intelligence_loaded: int = 0
    api_football_intelligence_missing: int = 0
    api_football_intelligence_cache_hits: int = 0
    api_football_intelligence_requests_used: int = 0
    api_football_intelligence_examples_json: str | None = None
    football_live_ranker_candidates: int = 0
    football_live_ranker_top_count: int = 0
    football_live_ranker_api_count: int = 0
    football_live_ranker_blocked_count: int = 0
    football_live_ranker_eligible_count: int = 0
    football_live_ranker_watchlist_count: int = 0
    football_live_ranker_blocked_breakdown_json: str | None = None
    football_live_ranker_top_json: str | None = None
    football_live_ranker_eligible_top_json: str | None = None
    football_live_ranker_watchlist_top_json: str | None = None
    football_live_ranker_preview_snapshot_json: str | None = None
    football_live_probability_matches: int = 0
    football_live_probability_with_api: int = 0
    football_live_probability_without_api: int = 0
    football_live_probability_top_count: int = 0
    football_live_probability_value_edge_7_count: int = 0
    football_live_probability_confidence_60_count: int = 0
    football_live_probability_top_json: str | None = None
    football_live_s12_controlled_candidates: int = 0
    football_live_s12_controlled_sent: int = 0
    football_live_s12_controlled_blocked: int = 0

    football_live_rejected_invalid_selection: int = 0
    """Candidates dropped because 1X2 selection could not be strictly parsed (1/2/X only)."""
    football_live_rejected_no_pressure: int = 0
    football_live_rejected_s8_home_00_without_api_context: int = 0
    football_live_passed_pressure: int = 0
    football_live_rejected_s8_1x2_00_without_api_context: int = 0
    football_live_rejected_s8_1x2_00_no_pressure: int = 0
    football_live_passed_s8_1x2_00_with_api_pressure: int = 0
    football_live_context_filter_last_cycle_json: str | None = None

    # --- S8 value filter diagnostics (per-cycle) ---
    football_live_rejected_value_low_favorite: int = 0
    football_live_rejected_value_no_edge: int = 0
    football_live_passed_value_filter: int = 0
    football_live_strategy_matches_last_cycle: int = 0
    """Unique matches that passed explicit strategy gate (last cycle)."""
    football_live_strategy_s1_matches_last_cycle: int = 0
    football_live_strategy_s2_matches_last_cycle: int = 0
    football_live_sportmonks_baseline_enriched_last_cycle: int = 0
    football_live_sportmonks_baseline_missing_last_cycle: int = 0
    football_live_sportmonks_fixture_mapped_last_cycle: int = 0
    football_live_sportmonks_fixture_not_mapped_last_cycle: int = 0
    football_live_post_selection_hint_ru: str | None = None
    """If bottleneck is after scoring (dedup), short RU line for status."""
    football_last_cycle_ingest_normal: int = 0
    football_last_cycle_ingest_soft: int = 0
    """Counts among signals actually created in DB last non-dry cycle."""
    football_last_cycle_send_mode: str = "none"
    """last batch: normal | soft | mixed | none — by send_path of created signals."""
    football_last_cycle_db_dedup_skipped: int = 0
    """DB dedup skips in the last ingest batch (non-dry)."""
    football_last_cycle_sent_traces_json: str | None = None
    """JSON array: per created signal, match, bet, score, path, gap, family, was_main, codes."""
    football_last_combat_cycle_at: str | None = None
    """ISO time of last non-dry football live cycle (combat or script)."""
    football_last_combat_messages_sent: int = 0
    football_last_combat_created_signals: int = 0
    football_last_combat_bottleneck: str | None = None
    football_last_combat_bottleneck_ru: str | None = None
    football_last_combat_send_mode: str = "none"
    football_last_combat_fresh_live_matches: int = 0
    football_last_combat_normal_sendable: int = 0
    football_last_combat_soft_sendable_total: int = 0
    football_last_combat_rejected_total: int = 0
    football_last_combat_session_idea_dedup: int = 0
    football_last_combat_db_dedup_skipped: int = 0
    football_primary_live_source: str | None = None
    """winline_live | the_odds_api | manual_winline_json | —"""
    football_winline_ws_active_last_cycle: bool = False
    """True if the last cycle actually used a successful Winline live fetch (not only attempted)."""
    football_winline_football_event_count: int = 0
    football_winline_line_count_raw: int = 0
    football_winline_error_last: str | None = None
    """Last Winline error token if primary fetch failed."""
    football_winline_football_candidate_count: int = 0
    """ProviderSignalCandidate count (football) in preview after Winline+bridge in last cycle."""
    football_live_combat_delivery_trace_json: str | None = None
    """Per-finalist E2E rows: ingest, db dedup, notify (last non-dry live cycle)."""
    football_live_combat_delivery_last_summary: str | None = None
    """One line: created / Telegram / db_dedup_from_last_combat."""
    football_live_sanity_blocked_last_cycle: int = 0
    """Finalists removed by pre-send live market sanity in the last football live cycle."""
    football_live_sanity_last_blocker: str | None = None
    """e.g. blocked_invalid_live_market_text | blocked_impossible_live_outcome — first drop if any."""
    football_live_sanity_last_best_rejected: str | None = None
    """Human line: best-score rejected candidate and reason."""
    football_postmatch_settled_count: int = 0
    """How many latest settled football rows were scanned for post-match summary."""
    football_postmatch_wins_last: int = 0
    football_postmatch_losses_last: int = 0
    football_postmatch_voids_last: int = 0
    football_postmatch_top_loss_reasons: str | None = None
    """Short joined list of top loss reason codes in the last sample."""
    football_postmatch_status_lines_json: str | None = None
    """JSON blob: sample wins/losses/voids and loss_by_reason."""
    football_postmatch_rationale_aggregate_json: str | None = None
    """WIN/LOSE aggregates for football_live_signal_rationale codes (last refresh)."""
    football_live_adaptive_learning_json: str | None = None
    """Active LIVE adaptive penalties/boosts and per-key deltas (last cycle or postmatch refresh)."""
    # --- OpenAI post-settlement analysis diagnostics ---
    openai_analysis_total: int = 0
    openai_analysis_success: int = 0
    openai_analysis_failed: int = 0
    openai_penalty_patterns_count: int = 0
    openai_boost_patterns_count: int = 0
    openai_learning_active_patterns: int = 0
    external_api_openai_status: str | None = None
    external_api_openai_last_error: str | None = None
    external_api_openai_last_success: str | None = None
    external_api_api_football_status: str | None = None
    external_api_api_football_last_error: str | None = None
    external_api_api_football_last_success: str | None = None
    external_api_sportmonks_status: str | None = None
    external_api_sportmonks_last_error: str | None = None
    external_api_sportmonks_last_success: str | None = None
    signals_with_api_football: int = 0
    signals_without_api_football: int = 0
    signals_with_sportmonks: int = 0
    signals_with_external_context: int = 0
    football_live_combat_signals_total: int = 0
    """Exact COUNT: football + is_live + notes=live_auto."""
    football_live_with_any_rationale_count: int = 0
    """Within last scan: explanation_json has football_live_signal_rationale dict."""
    football_live_with_training_ready_rationale_count: int = 0
    """Within last scan: rationale passes _rationale_training_ready (full codes/path/context)."""
    football_live_with_settlement_winlose_count: int = 0
    """Within last scan: settlement WIN or LOSE."""
    football_live_with_outcome_reason_code_count: int = 0
    """Within last scan: football_outcome_audit.outcome_reason_code present."""
    adaptive_training_ready_signals_count: int = 0
    """Within last scan: ready rationale + settled WIN/LOSE + outcome_reason_code."""
    football_live_adaptive_training_warning_ru: str | None = None
    """If adaptive_training_ready is low vs threshold."""
    football_live_winline_fetch_seconds_last: float | None = None
    """Wall time of last Winline WS fetch attempt in a football live cycle."""
    football_live_http_fetch_seconds_last: float | None = None
    """Wall time of last HTTP odds fetch in a football live cycle (if ran)."""
    football_live_winline_attempted_last_cycle: bool = False
    """True if Winline WS path was entered for this cycle."""
    football_live_last_cycle_wall_seconds: float | None = None
    """Wall time of last run_single_cycle (football live loop)."""
    football_live_last_cycle_fetch_ok: bool = False
    football_live_last_cycle_created_signals: int = 0
    football_live_pacing_current_interval_seconds: float | None = None
    football_live_pacing_last_fetch_seconds: float | None = None
    football_live_pacing_avg_fetch_seconds: float | None = None
    football_live_pacing_backoff_level: float = 0.0
    football_live_pacing_last_reason_ru: str | None = None
    football_live_pacing_consecutive_errors: int = 0
    football_live_pacing_consecutive_empty_snapshots: int = 0
    football_live_last_cycle_debug_telegram_text: str | None = None
    """Admin-only: full football live cycle breakdown (same as legacy ▶️ Старт dump)."""
    football_live_last_combat_debug_telegram_text: str | None = None
    """Admin-only: full breakdown for last non-dry (combat) football live cycle."""
    football_live_last_dry_run_debug_telegram_text: str | None = None
    """Admin-only: last dry_run debug breakdown (kept separate from combat)."""

    # --- Side funnel diagnostics (home/away/draw) ---
    football_live_side_funnel_last_cycle_json: str | None = None
    """
    JSON object:
      {
        "cycle_at": "...",
        "stages": {
          "after_integrity": {"home":..,"away":..,"draw":..,"unknown":..,"total":..},
          "after_s8": {...},
          "after_scoring": {...},
          "final_selected": {...}
        }
      }
    """
    football_live_side_funnel_window_agg_json: str | None = None
    """JSON object: aggregated counts across last N cycles for each stage."""
    football_live_side_funnel_window_cycles: int = 0
    """How many cycles contributed to window agg."""
    football_live_side_funnel_window_started_at: str | None = None
    """ISO time when side funnel window started (or last reset)."""
    football_live_side_away_draw_samples_last_cycle_json: str | None = None
    """
    JSON array of up to 10 objects: real after_integrity away/draw candidates with fate:
      {match, event_id, minute, score, market_type, bet_text, odds, side,
       fate_stage, s8_reject_reasons?, score?, selected?}
    """


_STATE = SignalRuntimeDiagnosticsState()
_LOCK = Lock()


class SignalRuntimeDiagnosticsService:
    def _snapshot(self) -> dict[str, Any]:
        return dict(asdict(_STATE))

    def get_state(self) -> dict[str, Any]:
        with _LOCK:
            return self._snapshot()

    def update(self, **values: Any) -> dict[str, Any]:
        with _LOCK:
            for key, value in values.items():
                if hasattr(_STATE, key):
                    setattr(_STATE, key, value)
            _STATE.updated_at = datetime.now(timezone.utc).isoformat()
            return self._snapshot()

    def reset(self) -> dict[str, Any]:
        with _LOCK:
            global _STATE
            _STATE = SignalRuntimeDiagnosticsState()
            _STATE.updated_at = datetime.now(timezone.utc).isoformat()
            return self._snapshot()
