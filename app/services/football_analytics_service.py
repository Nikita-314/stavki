from __future__ import annotations

import math
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from app.schemas.provider_models import ProviderSignalCandidate


@dataclass
class FootballAnalyticsSnapshot:
    """Rule-based analytics layer on top of provider line data. No invented facts."""

    is_live: bool
    hours_to_start: float | None
    league_or_tournament: str | None
    market_family: str | None
    odds_value: float | None
    implied_prob: float | None
    line_movement_available: bool
    red_cards_home: int | None
    red_cards_away: int | None
    score_home: int | None
    score_away: int | None
    minute: int | None
    favorite_side: str | None
    underdog_side: str | None
    current_side_strength_signal: str | None
    historical_matchup_available: bool
    team_form_available: bool
    injury_news_available: bool
    lineup_available: bool
    transfers_context_available: bool
    cards_live_available: bool
    statistics_available: bool
    corner_or_cards_like_market: bool

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class FootballAnalyticsService:
    """Collects honest feature flags + simple time/implied-prob signals for football."""

    def build_snapshot(self, candidate: ProviderSignalCandidate, *, market_family: str | None) -> dict[str, Any]:
        snap = self._build(candidate, market_family=market_family)
        return snap.as_dict()

    def _build(self, candidate: ProviderSignalCandidate, *, market_family: str | None) -> FootballAnalyticsSnapshot:
        match = candidate.match
        market = candidate.market
        raw = candidate.feature_snapshot_json or {}

        hours = self._hours_to_start(match.event_start_at, match.is_live)
        implied = self._implied_prob(candidate.implied_prob, market.odds_value)

        score_h, score_a, minute, red_h, red_a = self._extract_live_fields(raw)

        fav, dog = self._favorite_underdog_from_line(
            selection=market.selection,
            home_team=match.home_team,
            away_team=match.away_team,
            odds_value=float(market.odds_value),
        )

        strength = self._side_strength_signal(
            is_live=match.is_live,
            selection=market.selection,
            home_team=match.home_team,
            away_team=match.away_team,
            score_home=score_h,
            score_away=score_a,
        )

        corner_like = self._is_corner_or_cards_like(market.market_label, market.selection, market_family)

        return FootballAnalyticsSnapshot(
            is_live=bool(match.is_live),
            hours_to_start=hours,
            league_or_tournament=(match.tournament_name or "").strip() or None,
            market_family=market_family,
            odds_value=float(market.odds_value) if market.odds_value is not None else None,
            implied_prob=implied,
            line_movement_available=False,
            red_cards_home=red_h,
            red_cards_away=red_a,
            score_home=score_h,
            score_away=score_a,
            minute=minute,
            favorite_side=fav,
            underdog_side=dog,
            current_side_strength_signal=strength,
            historical_matchup_available=False,
            team_form_available=False,
            injury_news_available=False,
            lineup_available=False,
            transfers_context_available=False,
            cards_live_available=red_h is not None or red_a is not None,
            statistics_available=score_h is not None and score_a is not None,
            corner_or_cards_like_market=corner_like,
        )

    def _hours_to_start(self, event_start_at: datetime | None, is_live: bool) -> float | None:
        if is_live or event_start_at is None:
            return None
        now = datetime.now(timezone.utc)
        start = event_start_at
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        delta_hours = (start - now).total_seconds() / 3600.0
        return round(delta_hours, 3)

    def _implied_prob(self, explicit: Decimal | None, odds_value: Decimal) -> float | None:
        if explicit is not None:
            return float(explicit)
        try:
            o = float(odds_value)
            if o > 1:
                return round(1.0 / o, 6)
        except Exception:
            return None
        return None

    def _extract_live_fields(self, raw: dict[str, Any]) -> tuple[int | None, int | None, int | None, int | None, int | None]:
        """Best-effort scan of known snapshot shapes; returns Nones if nothing reliable."""
        blobs: list[Any] = [raw]
        for key in ("source_raw_market_json", "source_raw_event_json", "raw_event_json"):
            v = raw.get(key)
            if isinstance(v, dict):
                blobs.append(v)
        score_h = score_a = minute = red_h = red_a = None
        for blob in blobs:
            sh, sa, mn, rh, ra = self._scan_dict_for_live(blob)
            score_h = score_h if score_h is not None else sh
            score_a = score_a if score_a is not None else sa
            minute = minute if minute is not None else mn
            red_h = red_h if red_h is not None else rh
            red_a = red_a if red_a is not None else ra
        return score_h, score_a, minute, red_h, red_a

    def _scan_dict_for_live(self, d: dict[str, Any]) -> tuple[int | None, int | None, int | None, int | None, int | None]:
        if not d:
            return None, None, None, None, None
        lowered_keys = {str(k).lower(): v for k, v in d.items()}
        score_h = self._coerce_int(
            lowered_keys.get("home_score")
            or lowered_keys.get("score_home")
            or lowered_keys.get("homescore")
            or lowered_keys.get("goals_home")
        )
        score_a = self._coerce_int(
            lowered_keys.get("away_score")
            or lowered_keys.get("score_away")
            or lowered_keys.get("awayscore")
            or lowered_keys.get("goals_away")
        )
        minute = self._coerce_int(lowered_keys.get("minute") or lowered_keys.get("match_minute") or lowered_keys.get("time"))
        red_h = self._coerce_int(lowered_keys.get("red_cards_home") or lowered_keys.get("redcardshome"))
        red_a = self._coerce_int(lowered_keys.get("red_cards_away") or lowered_keys.get("redcardsaway"))
        return score_h, score_a, minute, red_h, red_a

    def _coerce_int(self, v: Any) -> int | None:
        if v is None:
            return None
        if isinstance(v, bool):
            return None
        if isinstance(v, int):
            return v if v >= 0 else None
        if isinstance(v, float) and not math.isnan(v):
            return int(v) if v >= 0 else None
        if isinstance(v, str) and re.fullmatch(r"\d+", v.strip()):
            return int(v.strip())
        return None

    def _favorite_underdog_from_line(
        self,
        *,
        selection: str,
        home_team: str,
        away_team: str,
        odds_value: float,
    ) -> tuple[str | None, str | None]:
        """Very shallow heuristic: lower odds usually mean market leans that side as likely — still optional."""
        if odds_value <= 1.0:
            return None, None
        sel = (selection or "").strip().lower()
        home = (home_team or "").strip().lower()
        away = (away_team or "").strip().lower()
        if not home or not away:
            return None, None
        if sel == home:
            return "home", "away"
        if sel == away:
            return "away", "home"
        if home in sel and away not in sel:
            return "home", "away"
        if away in sel and home not in sel:
            return "away", "home"
        return None, None

    def _side_strength_signal(
        self,
        *,
        is_live: bool,
        selection: str,
        home_team: str,
        away_team: str,
        score_home: int | None,
        score_away: int | None,
    ) -> str | None:
        if not is_live or score_home is None or score_away is None:
            return None
        diff = score_home - score_away
        sel = (selection or "").strip().lower()
        home = (home_team or "").strip().lower()
        away = (away_team or "").strip().lower()
        if home in sel and away not in sel:
            if diff >= 2:
                return "backed_side_ahead_by_2_or_more"
            if diff <= -2:
                return "backed_side_behind_by_2_or_more"
        elif away in sel and home not in sel:
            if diff <= -2:
                return "backed_side_ahead_by_2_or_more"
            if diff >= 2:
                return "backed_side_behind_by_2_or_more"
        return None

    def _is_corner_or_cards_like(self, market_label: str, selection: str, family: str | None) -> bool:
        blob = f"{market_label} {selection}".lower()
        if family in {"special", "exotic"}:
            return True
        return any(k in blob for k in ("углов", "corner", "карточ", "card", "booking"))
