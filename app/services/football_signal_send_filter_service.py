from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from app.core.enums import SportType
from app.schemas.provider_models import ProviderSignalCandidate
import logging


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FootballSendFilterStats:
    before: int
    after_whitelist: int
    after_ranking: int
    after_family_dedup: int
    after_per_match_cap: int
    drop_reasons: dict[str, int]
    families_left: dict[str, int]
    selected_per_match: list[str]
    live_matches: int
    near_matches: int
    too_far_matches_dropped: int


@dataclass(frozen=True)
class FootballSendFilterResult:
    candidates: list[ProviderSignalCandidate]
    stats: FootballSendFilterStats


class FootballSignalSendFilterService:
    MAX_SIGNALS_PER_MATCH = 1
    NEAR_MATCH_HOURS = 6.0
    MAX_PREMATCH_HOURS = 24.0

    _ALLOWED_AUTO_FAMILIES = {"result", "double_chance", "totals", "btts", "handicap"}
    _SOFT_ALLOWED_AUTO_FAMILIES = {"combo"}
    _BLOCKED_AUTO_FAMILIES = {
        "correct_score",
        "winning_margin",
        "odd_even",
        "special",
        "exotic",
    }

    _FAMILY_PRIORITY = {
        "result": 140.0,
        "double_chance": 130.0,
        "totals": 120.0,
        "btts": 110.0,
        "handicap": 100.0,
        "combo": 80.0,
        "correct_score": 20.0,
        "winning_margin": 15.0,
        "odd_even": 10.0,
        "special": 5.0,
        "exotic": 0.0,
    }

    _IDEA_FAMILY = {
        "result": "result_family",
        "double_chance": "result_family",
        "totals": "goals_family",
        "btts": "goals_family",
        "handicap": "handicap_family",
        "combo": "combo_family",
        "correct_score": "exotic_family",
        "winning_margin": "exotic_family",
        "odd_even": "exotic_family",
        "special": "special_family",
        "exotic": "exotic_family",
    }

    def filter_auto_send_candidates(
        self,
        candidates: list[ProviderSignalCandidate],
    ) -> FootballSendFilterResult:
        drop_reasons = {
            "blocked_family": 0,
            "low_score": 0,
            "dedup_family": 0,
            "cap_per_match": 0,
            "too_far_in_time": 0,
        }
        football_candidates = [
            candidate
            for candidate in candidates
            if getattr(getattr(candidate, "match", None), "sport", None) == SportType.FOOTBALL
        ]
        before = len(football_candidates)
        logger.info("[FOOTBALL][FILTER] incoming candidates: %s", before)
        live_matches, near_matches, too_far_matches = self._summarize_match_timing(football_candidates)
        logger.info(
            "[FOOTBALL][TIME] match buckets: live_matches=%s near_matches=%s too_far_matches=%s",
            live_matches,
            near_matches,
            too_far_matches,
        )

        ranked = sorted(football_candidates, key=self._candidate_rank_key, reverse=True)
        whitelisted: list[ProviderSignalCandidate] = []
        for candidate in ranked:
            allowed, reason = self._is_allowed_for_auto_send(candidate)
            if allowed:
                whitelisted.append(candidate)
            elif reason in drop_reasons:
                drop_reasons[reason] += 1
        after_whitelist = len(whitelisted)
        logger.info("[FOOTBALL][FILTER] after whitelist: %s", after_whitelist)
        after_ranking = len(whitelisted)
        logger.info("[FOOTBALL][FILTER] after ranking: %s", after_ranking)

        family_deduped: list[ProviderSignalCandidate] = []
        seen_families: set[tuple[str, str]] = set()
        for candidate in whitelisted:
            event_id = str(getattr(getattr(candidate, "match", None), "external_event_id", "") or "")
            idea_family = self.get_signal_idea_family(candidate)
            dedup_key = (event_id, idea_family)
            if dedup_key in seen_families:
                drop_reasons["dedup_family"] += 1
                continue
            seen_families.add(dedup_key)
            family_deduped.append(candidate)
        after_family_dedup = len(family_deduped)
        logger.info("[FOOTBALL][FILTER] after family dedup: %s", after_family_dedup)

        capped: list[ProviderSignalCandidate] = []
        per_match_count: dict[str, int] = {}
        for candidate in family_deduped:
            event_id = str(getattr(getattr(candidate, "match", None), "external_event_id", "") or "")
            used = per_match_count.get(event_id, 0)
            if used >= self.MAX_SIGNALS_PER_MATCH:
                drop_reasons["cap_per_match"] += 1
                continue
            per_match_count[event_id] = used + 1
            capped.append(candidate)
        logger.info("[FOOTBALL][FILTER] after per-match cap: %s", len(capped))

        families_left: dict[str, int] = {}
        for candidate in capped:
            family = self.get_market_family(candidate)
            families_left[family] = families_left.get(family, 0) + 1
        logger.info(
            "[FOOTBALL][FILTER] families left: %s",
            ", ".join(f"{family}={count}" for family, count in sorted(families_left.items())) or "—",
        )

        selected_per_match = [self._describe_candidate(candidate) for candidate in capped]
        if selected_per_match:
            logger.info("[FOOTBALL][FILTER] selected per match:")
            for row in selected_per_match:
                logger.info("- %s", row)
        if not capped:
            logger.warning("[FOOTBALL][FILTER][WARNING] no signals after filtering")
        logger.info(
            "[FOOTBALL][FILTER][DROP REASONS]: blocked_family=%s, low_score=%s, dedup_family=%s, cap_per_match=%s, too_far_in_time=%s",
            drop_reasons["blocked_family"],
            drop_reasons["low_score"],
            drop_reasons["dedup_family"],
            drop_reasons["cap_per_match"],
            drop_reasons["too_far_in_time"],
        )

        return FootballSendFilterResult(
            candidates=capped,
            stats=FootballSendFilterStats(
                before=before,
                after_whitelist=after_whitelist,
                after_ranking=after_ranking,
                after_family_dedup=after_family_dedup,
                after_per_match_cap=len(capped),
                drop_reasons=dict(drop_reasons),
                families_left=families_left,
                selected_per_match=selected_per_match,
                live_matches=live_matches,
                near_matches=near_matches,
                too_far_matches_dropped=too_far_matches,
            ),
        )

    def get_market_family(self, candidate: ProviderSignalCandidate) -> str:
        market_type = str(getattr(getattr(candidate, "market", None), "market_type", "") or "").strip().lower()
        market_label = str(getattr(getattr(candidate, "market", None), "market_label", "") or "").strip().lower()
        selection = str(getattr(getattr(candidate, "market", None), "selection", "") or "").strip().lower()
        text = " | ".join(x for x in [market_type, market_label, selection] if x)
        selection_token = selection.replace(" ", "").upper().replace("Х", "X")

        if market_type in {"1x2", "match_winner"}:
            if selection_token in {"1X", "12", "X2"} or any(
                token in text for token in ("double chance", "двойной шанс")
            ):
                return "double_chance"
            return "result"
        if market_type == "total_goals":
            return "totals"
        if market_type == "both_teams_to_score":
            return "btts"
        if market_type == "handicap":
            return "handicap"
        if any(token in text for token in ("half time/full time", "half/full time", "тайм/матч", "п1/п1", "н/п1", "п2/п2")):
            return "combo"
        if any(token in text for token in ("correct score", "точный счет", "точный счёт", "2:0", "1:0", "1:1")):
            return "correct_score"
        if any(token in text for token in ("margin", "разниц")):
            return "winning_margin"
        if any(token in text for token in ("odd", "even", "чет", "чёт", "нечет", "нечёт")):
            return "odd_even"
        if any(token in text for token in ("clean sheet", "не пропуст", "любая команда победит", "both halves")):
            return "special"
        return "exotic"

    def get_signal_idea_family(self, candidate: ProviderSignalCandidate) -> str:
        family = self.get_market_family(candidate)
        return self._IDEA_FAMILY.get(family, "exotic_family")

    def get_market_tier(self, candidate: ProviderSignalCandidate) -> int:
        family = self.get_market_family(candidate)
        if family in {"result", "double_chance", "totals", "btts", "handicap"}:
            return 1
        if family == "combo":
            return 2
        return 3

    def build_football_signal_score(self, candidate: ProviderSignalCandidate) -> float:
        family = self.get_market_family(candidate)
        tier = self.get_market_tier(candidate)
        score = self._FAMILY_PRIORITY.get(family, 0.0) - (tier - 1) * 25.0
        score += self._build_time_priority_score(candidate)

        score += self._numeric_value(getattr(candidate, "signal_score", None)) * 100.0
        score += self._numeric_value(getattr(candidate, "predicted_prob", None)) * 80.0
        score += self._numeric_value(getattr(candidate, "edge", None)) * 60.0
        score += self._numeric_value(getattr(candidate, "implied_prob", None)) * 10.0
        score += self._feature_value(candidate, "confidence_score") * 70.0
        score += self._feature_value(candidate, "expected_value") * 50.0
        score += self._feature_value(candidate, "recommended_stake_units") * 15.0

        live_codes = self._feature_list(candidate, "live_reason_codes")
        value_codes = self._feature_list(candidate, "value_reason_codes")
        score += min(len(live_codes), 3) * 5.0
        score += min(len(value_codes), 2) * 4.0
        return score

    def _candidate_rank_key(self, candidate: ProviderSignalCandidate) -> tuple[float, float]:
        score = self.build_football_signal_score(candidate)
        family_priority = self._FAMILY_PRIORITY.get(self.get_market_family(candidate), 0.0)
        return (score, family_priority)

    def _is_allowed_for_auto_send(self, candidate: ProviderSignalCandidate) -> tuple[bool, str | None]:
        is_live, hours_to_start = self._time_window_info(candidate)
        if not is_live and hours_to_start is not None and hours_to_start > self.MAX_PREMATCH_HOURS:
            logger.info(
                "[FOOTBALL][TIME] drop too_far_in_time: match=%s event_start_at=%s is_live=%s hours_to_start=%.2f priority_score=%.2f",
                getattr(getattr(candidate, "match", None), "match_name", "—"),
                getattr(getattr(candidate, "match", None), "event_start_at", None),
                str(is_live).lower(),
                hours_to_start,
                self.build_football_signal_score(candidate),
            )
            return False, "too_far_in_time"
        family = self.get_market_family(candidate)
        if family in self._BLOCKED_AUTO_FAMILIES:
            return False, "blocked_family"
        if family in self._ALLOWED_AUTO_FAMILIES:
            return True, None
        if family in self._SOFT_ALLOWED_AUTO_FAMILIES:
            if self._is_strong_combo(candidate):
                return True, None
            return False, "low_score"
        return False, "blocked_family"

    def _is_strong_combo(self, candidate: ProviderSignalCandidate) -> bool:
        return any(
            (
                self._numeric_value(getattr(candidate, "signal_score", None)) >= 0.70,
                self._numeric_value(getattr(candidate, "predicted_prob", None)) >= 0.60,
                self._numeric_value(getattr(candidate, "edge", None)) >= 0.05,
                self._feature_value(candidate, "confidence_score") >= 0.75,
                self._feature_value(candidate, "recommended_stake_units") >= 1.50,
            )
        )

    def _feature_value(self, candidate: ProviderSignalCandidate, key: str) -> float:
        snapshot = getattr(candidate, "feature_snapshot_json", None) or {}
        return self._numeric_value(snapshot.get(key))

    def _feature_list(self, candidate: ProviderSignalCandidate, key: str) -> list[Any]:
        snapshot = getattr(candidate, "feature_snapshot_json", None) or {}
        value = snapshot.get(key)
        return value if isinstance(value, list) else []

    def _numeric_value(self, value: Any) -> float:
        if value is None:
            return 0.0
        if isinstance(value, Decimal):
            return float(value)
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def _describe_candidate(self, candidate: ProviderSignalCandidate) -> str:
        match_name = str(getattr(getattr(candidate, "match", None), "match_name", "") or "unknown_match")
        market_label = str(getattr(getattr(candidate, "market", None), "market_label", "") or "unknown_market")
        selection = str(getattr(getattr(candidate, "market", None), "selection", "") or "")
        odds = getattr(getattr(candidate, "market", None), "odds_value", None)
        family = self.get_market_family(candidate)
        is_live, hours_to_start = self._time_window_info(candidate)
        priority_score = self.build_football_signal_score(candidate)
        suffix = f" ({selection})" if selection else ""
        odds_text = f"@{odds}" if odds is not None else "@?"
        if is_live:
            time_part = "live"
        elif hours_to_start is None:
            time_part = "start=unknown"
        else:
            time_part = f"hours_to_start={hours_to_start:.2f}"
        logger.info(
            "[FOOTBALL][TIME] candidate: match=%s event_start_at=%s is_live=%s hours_to_start=%s priority_score=%.2f",
            match_name,
            getattr(getattr(candidate, "match", None), "event_start_at", None),
            str(is_live).lower(),
            "—" if hours_to_start is None else f"{hours_to_start:.2f}",
            priority_score,
        )
        return f"{match_name} -> {family} [{market_label}{suffix}] {odds_text} [{time_part}; score={priority_score:.2f}]"

    def _build_time_priority_score(self, candidate: ProviderSignalCandidate) -> float:
        is_live, hours_to_start = self._time_window_info(candidate)
        if is_live:
            return 10000.0
        if hours_to_start is None:
            return -250.0
        if hours_to_start <= 0:
            return 6000.0
        if hours_to_start <= 1:
            return 4000.0 - hours_to_start * 50.0
        if hours_to_start <= self.NEAR_MATCH_HOURS:
            return 3000.0 - hours_to_start * 40.0
        if hours_to_start <= self.MAX_PREMATCH_HOURS:
            return 1500.0 - hours_to_start * 20.0
        return -5000.0 - hours_to_start * 10.0

    def _time_window_info(self, candidate: ProviderSignalCandidate) -> tuple[bool, float | None]:
        match = getattr(candidate, "match", None)
        is_live = bool(getattr(match, "is_live", False))
        event_start_at = getattr(match, "event_start_at", None)
        if is_live:
            return True, 0.0
        if event_start_at is None:
            return False, None
        dt = event_start_at
        if getattr(dt, "tzinfo", None) is None:
            dt = dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return False, (dt - now).total_seconds() / 3600.0

    def _summarize_match_timing(self, candidates: list[ProviderSignalCandidate]) -> tuple[int, int, int]:
        buckets: dict[str, str] = {}
        for candidate in candidates:
            match = getattr(candidate, "match", None)
            event_id = str(getattr(match, "external_event_id", "") or "")
            if not event_id or event_id in buckets:
                continue
            is_live, hours_to_start = self._time_window_info(candidate)
            if is_live:
                buckets[event_id] = "live"
            elif hours_to_start is not None and 0 <= hours_to_start <= self.NEAR_MATCH_HOURS:
                buckets[event_id] = "near"
            elif hours_to_start is not None and hours_to_start > self.MAX_PREMATCH_HOURS:
                buckets[event_id] = "too_far"
            else:
                buckets[event_id] = "prematch"
        return (
            sum(1 for bucket in buckets.values() if bucket == "live"),
            sum(1 for bucket in buckets.values() if bucket == "near"),
            sum(1 for bucket in buckets.values() if bucket == "too_far"),
        )
