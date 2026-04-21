from __future__ import annotations

from decimal import Decimal
from typing import Annotated

from pydantic import BeforeValidator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _empty_str_to_none(v: object) -> object:
    if isinstance(v, str) and not v.strip():
        return None
    return v


def _parse_admin_user_ids(v: object) -> list[int]:
    if v is None:
        return []
    if isinstance(v, list):
        return [int(x) for x in v]
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return []
        parts = [p.strip() for p in s.split(",")]
        return [int(p) for p in parts if p]
    return [int(v)]


class Settings(BaseSettings):
    app_name: str = "stavki-signals-bot"
    bot_token: str
    database_url: str
    debug: bool = False
    admin_user_ids: Annotated[list[int], BeforeValidator(_parse_admin_user_ids)] = []
    signal_chat_id: Annotated[int | None, BeforeValidator(_empty_str_to_none)] = None
    result_chat_id: Annotated[int | None, BeforeValidator(_empty_str_to_none)] = None
    virtual_flat_stake_rub: Decimal = Decimal("1000.00")
    virtual_start_balance_rub: Decimal = Decimal("50000.00")
    provider_test_url: str | None = None
    provider_test_timeout_seconds: int = 20
    odds_provider_base_url: str | None = None
    odds_provider_api_key: str | None = None
    odds_provider_sport: str | None = None
    odds_provider_regions: str | None = None
    odds_provider_markets: str | None = None
    odds_provider_bookmakers: str | None = None
    odds_provider_odds_format: str | None = "decimal"
    odds_provider_date_format: str | None = "iso"
    odds_provider_timeout_seconds: int = 20
    sportmonks_api_key: str | None = None
    """Sportmonks Football API token (set via SPORTMONKS_API_KEY in .env)."""
    auto_signal_polling_enabled: bool = False
    auto_signal_polling_interval_seconds: int = 60
    auto_signal_preview_only: bool = False
    auto_signal_max_created_per_cycle: Annotated[int | None, BeforeValidator(_empty_str_to_none)] = None
    football_debug_disable_filter: bool = False
    football_allow_manual_production_fallback: bool = False
    football_analytics_enabled: bool = True
    football_learning_enabled: bool = True
    football_live_adaptive_learning_enabled: bool = True
    """Football LIVE only: bounded additive score adjustments from settled rationale/outcome history."""
    football_min_signal_score: float = 55.0
    football_dedup_relaxed_interval_minutes: int = 30
    football_live_session_duration_minutes: int = 15
    """Used only when starting a timed session (scripts); ▶️ Старт uses persistent session (no auto-expiry)."""
    # Football LIVE loop pacing (between cycles, Winline-friendly). Bounds match prior idle clamp 45–180s and base poll 60s.
    football_live_pacing_min_interval_seconds: int = 45
    football_live_pacing_max_interval_seconds: int = 180
    football_live_pacing_base_interval_seconds: int = 60
    football_live_pacing_backoff_step: float = 0.12
    football_live_pacing_max_backoff_level: float = 4.0
    football_live_pacing_fetch_heavy_seconds: int = 45
    football_live_pacing_fetch_heavy_extra_seconds: int = 25
    football_live_pacing_fetch_above_avg_extra_seconds: int = 15
    football_live_pacing_cycle_heavy_seconds: int = 95
    football_live_pacing_cycle_heavy_extra_seconds: int = 20
    football_live_pacing_network_stress_extra_seconds: int = 35
    football_live_pacing_empty_snapshot_extra_seconds: int = 25
    football_live_pacing_unchanged_snapshot_extra_seconds: int = 12
    football_live_pacing_error_extra_seconds: int = 30
    football_live_pacing_cycle_light_seconds: int = 55
    football_live_pacing_fetch_light_seconds: int = 28
    football_live_pacing_light_cycle_relief_seconds: int = 10
    football_live_max_signals_per_match: int = 12
    # Live freshness / staleness (football live-session contour only)
    football_live_manual_max_age_minutes: int = 45
    football_live_event_max_kickoff_age_hours: float = 5.0
    football_live_max_declared_live_minute: int = 130
    # If the live HTTP payload was fetched this many minutes ago and only then processed, block (abnormal delay / stuck worker)
    football_live_runtime_snapshot_max_age_minutes: int = 30
    # Legacy multi-match pool relief (optional logging only; send gate uses per-candidate soft path)
    football_live_score_relief_max_points: float = 1.0
    # Single-candidate main-market soft send: max gap below base (points), absolute floor still applies
    football_live_single_relief_max_gap: float = 2.0
    # Football live data: Winline WebSocket (primary) vs The Odds API
    football_live_winline_primary: bool = True
    """When True, football `run_single_cycle` loads live from Winline WS first; The Odds API is optional."""
    winline_live_ws_url: str = "wss://wss.winline.ru/data_ng?client=newsite&nb=true"
    winline_live_max_prescan: int = 100
    winline_live_catalog_max_prescan: int = 450
    """Extra prescan for debug catalog script (more WS step-4 messages)."""
    # 0 = do not cap football event count in one fetch (all detected football live ids in snapshot)
    winline_live_max_football_events: int = 0
    winline_live_event_plus_min_lines: int = 1
    winline_live_event_plus_rounds: int = 2
    winline_live_event_plus_postscan: int = 100
    winline_live_connect_timeout_seconds: int = 25
    winline_live_recv_timeout_seconds: int = 20
    winline_live_total_timeout_seconds: int = 180
    """Max wall time for the complete Winline WS prescan+event.plus (per cycle). Longer when all football live events are included."""
    football_live_odds_api_fallback: bool = False
    """If True, try The Odds API when Winline live feed fails. Default False: honest `blocked_winline` instead."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


def get_settings() -> Settings:
    return Settings()

