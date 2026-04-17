from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from decimal import Decimal

from app.schemas.provider_models import ProviderSignalCandidate
from app.services.football_bet_formatter_service import FootballBetFormatterService
from app.services.football_signal_send_filter_service import FootballSignalSendFilterService


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FootballSignalIntegrityCheck:
    candidate: ProviderSignalCandidate
    source_market_type: str
    source_market_label: str
    source_selection: str
    source_family: str
    source_odds_value: str
    final_bet_text: str
    source_total_scope: str
    final_total_scope: str
    source_total_line: str
    final_total_line: str
    allowed_selection: bool
    exact_market_match: bool
    exact_selection_match: bool
    exact_odds_match: bool
    exact_total_scope_match: bool
    exact_total_line_match: bool
    family_match: bool
    integrity_check_passed: bool
    integrity_check_reason: str
    detected_special_scope: str | None = None
    detected_period_scope: str | None = None


@dataclass(frozen=True)
class FootballSignalIntegrityBatchResult:
    valid_candidates: list[ProviderSignalCandidate]
    dropped_checks: list[FootballSignalIntegrityCheck]
    passed_checks: list[FootballSignalIntegrityCheck]


class FootballSignalIntegrityService:
    _RESULT_TOKENS = {"1", "HOME", "П1", "X", "DRAW", "Х", "2", "AWAY", "П2"}
    _DOUBLE_CHANCE_TOKENS = {"1X", "12", "X2", "1Х", "Х2"}
    _YES_NO_TOKENS = {"YES", "NO", "ДА", "НЕТ", "Y", "N"}
    _HTFT_PATTERN = re.compile(r"^(HOME|AWAY|DRAW|1|2|X|П1|П2|Х)[/](HOME|AWAY|DRAW|1|2|X|П1|П2|Х)$", re.I)
    _EXACT_SCORE_PATTERN = re.compile(r"^\d+:\d+$")
    _TOTAL_PATTERN = re.compile(r"^(OVER|UNDER|O|U|ТБ|ТМ|Б|М|БОЛЬШЕ|МЕНЬШЕ)[ ]*[0-9]+(?:[.,][0-9]+)?$", re.I)
    _HANDICAP_PATTERN = re.compile(
        r"^((HOME|AWAY|П1|П2|Ф1|Ф2|[A-Za-zА-Яа-я0-9 ._-]+)[ ]*)?([+-]\d+(?:[.,]\d+)?)$",
        re.I,
    )

    def __init__(self) -> None:
        self._formatter = FootballBetFormatterService()
        self._family_service = FootballSignalSendFilterService()

    def validate_candidates(
        self,
        candidates: list[ProviderSignalCandidate],
    ) -> FootballSignalIntegrityBatchResult:
        valid_candidates: list[ProviderSignalCandidate] = []
        dropped: list[FootballSignalIntegrityCheck] = []
        passed: list[FootballSignalIntegrityCheck] = []

        for candidate in candidates:
            check = self.validate_candidate(candidate)
            self._log_check(check, candidate)
            updated_candidate = candidate.model_copy(
                update={
                    "feature_snapshot_json": {
                        **(candidate.feature_snapshot_json or {}),
                        "source_market_label": check.source_market_label,
                        "source_selection": check.source_selection,
                        "source_market_type": check.source_market_type,
                        "source_family": check.source_family,
                        "source_odds_value": check.source_odds_value,
                        "source_total_scope": check.source_total_scope,
                        "source_total_line": check.source_total_line,
                        "final_total_scope": check.final_total_scope,
                        "final_total_line": check.final_total_line,
                        "integrity_check_passed": check.integrity_check_passed,
                        "integrity_check_reason": check.integrity_check_reason,
                        "detected_special_scope": check.detected_special_scope,
                        "detected_period_scope": check.detected_period_scope,
                    }
                }
            )
            if check.integrity_check_passed:
                valid_candidates.append(updated_candidate)
                passed.append(check)
            else:
                dropped.append(check)

        return FootballSignalIntegrityBatchResult(
            valid_candidates=valid_candidates,
            dropped_checks=dropped,
            passed_checks=passed,
        )

    def validate_candidate(self, candidate: ProviderSignalCandidate) -> FootballSignalIntegrityCheck:
        snapshot = candidate.feature_snapshot_json or {}
        source_market_type = str(snapshot.get("source_market_type") or candidate.market.market_type or "").strip()
        source_market_label = str(snapshot.get("source_market_label") or candidate.market.market_label or "").strip()
        source_selection = str(snapshot.get("source_selection") or candidate.market.selection or "").strip()
        source_odds_value = str(snapshot.get("source_odds_value") or candidate.market.odds_value or "").strip()

        source_family = self._infer_family(
            market_type=source_market_type,
            market_label=source_market_label,
            selection=source_selection,
        )
        final_family = self._family_service.get_market_family(candidate)

        exact_market_match = self._norm(source_market_type) == self._norm(candidate.market.market_type) and self._norm(
            source_market_label
        ) == self._norm(candidate.market.market_label)
        exact_selection_match = self._norm(source_selection) == self._norm(candidate.market.selection)
        exact_odds_match = self._decimal_equal(source_odds_value, candidate.market.odds_value)
        family_match = source_family == final_family
        allowed_selection = self._is_allowed_selection(
            family=final_family,
            selection=candidate.market.selection,
            home_team=candidate.match.home_team,
            away_team=candidate.match.away_team,
        )

        final_bet = self._formatter.format_bet(
            market_type=candidate.market.market_type,
            market_label=candidate.market.market_label,
            selection=candidate.market.selection,
            home_team=candidate.match.home_team,
            away_team=candidate.match.away_team,
            section_name=candidate.market.section_name,
            subsection_name=candidate.market.subsection_name,
        )
        final_bet_text = final_bet.detail_label or final_bet.main_label
        source_total = self._formatter.describe_total_context(
            market_type=source_market_type,
            market_label=source_market_label,
            selection=source_selection,
            home_team=candidate.match.home_team,
            away_team=candidate.match.away_team,
            section_name=str(snapshot.get("source_section_name") or ""),
            subsection_name=str(snapshot.get("source_subsection_name") or ""),
        )
        final_total = self._formatter.describe_total_context(
            market_type=candidate.market.market_type,
            market_label=final_bet_text,
            selection=final_bet_text,
            home_team=candidate.match.home_team,
            away_team=candidate.match.away_team,
            section_name=candidate.market.section_name,
            subsection_name=candidate.market.subsection_name,
        )
        source_total_scope = source_total.total_scope if source_total else ""
        final_total_scope = final_total.total_scope if final_total else ""
        source_total_line = source_total.total_line if source_total and source_total.total_line else ""
        final_total_line = final_total.total_line if final_total and final_total.total_line else ""
        exact_total_scope_match = True
        exact_total_line_match = True
        if final_family == "totals":
            exact_total_scope_match = source_total_scope == final_total_scope
            exact_total_line_match = source_total_line == final_total_line

        reason = "ok"
        if not allowed_selection:
            reason = f"invalid_selection_for_family:{final_family}"
        elif final_family == "totals" and not exact_total_scope_match:
            reason = "invalid_total_scope"
        elif final_family == "totals" and not exact_total_line_match:
            reason = "invalid_total_line"
        elif not exact_market_match:
            reason = "market_label_or_type_mismatch"
        elif not exact_selection_match:
            reason = "selection_mismatch"
        elif not exact_odds_match:
            reason = "odds_mismatch"
        elif not family_match:
            reason = f"family_mismatch:{source_family}->{final_family}"

        return FootballSignalIntegrityCheck(
            candidate=candidate,
            source_market_type=source_market_type,
            source_market_label=source_market_label,
            source_selection=source_selection,
            source_family=source_family,
            source_odds_value=source_odds_value,
            final_bet_text=final_bet_text,
            source_total_scope=source_total_scope,
            final_total_scope=final_total_scope,
            source_total_line=source_total_line,
            final_total_line=final_total_line,
            allowed_selection=allowed_selection,
            exact_market_match=exact_market_match,
            exact_selection_match=exact_selection_match,
            exact_odds_match=exact_odds_match,
            exact_total_scope_match=exact_total_scope_match,
            exact_total_line_match=exact_total_line_match,
            family_match=family_match,
            integrity_check_passed=reason == "ok",
            integrity_check_reason=reason,
            detected_special_scope=final_bet.detected_special_scope,
            detected_period_scope=final_bet.detected_period_scope,
        )

    def _log_check(self, check: FootballSignalIntegrityCheck, candidate: ProviderSignalCandidate) -> None:
        logger.info(
            "[FOOTBALL][INTEGRITY] selected signal candidate: event_id=%s match=%s source_market_type=%s source_market_label=%s source_selection=%s source_odds=%s detected_special_scope=%s detected_period_scope=%s final_bet_text=%s",
            candidate.match.external_event_id,
            candidate.match.match_name,
            check.source_market_type,
            check.source_market_label,
            check.source_selection,
            check.source_odds_value,
            check.detected_special_scope,
            check.detected_period_scope,
            check.final_bet_text,
        )
        logger.info(
            "[FOOTBALL][INTEGRITY] validation: family=%s allowed_selection=%s exact_market_match=%s exact_selection_match=%s exact_odds_match=%s result=%s reason=%s",
            check.source_family,
            str(check.allowed_selection).lower(),
            str(check.exact_market_match).lower(),
            str(check.exact_selection_match).lower(),
            str(check.exact_odds_match).lower(),
            "pass" if check.integrity_check_passed else "fail",
            check.integrity_check_reason,
        )
        if check.source_family == "totals":
            logger.info(
                "[FOOTBALL][TOTALS] source_market_label=%s source_selection=%s source_odds=%s normalized_market_text=%s source_total_scope=%s source_total_line=%s final_total_scope=%s final_total_line=%s exact_total_scope_match=%s exact_total_line_match=%s",
                check.source_market_label,
                check.source_selection,
                check.source_odds_value,
                check.final_bet_text,
                check.source_total_scope or "—",
                check.source_total_line or "—",
                check.final_total_scope or "—",
                check.final_total_line or "—",
                str(check.exact_total_scope_match).lower(),
                str(check.exact_total_line_match).lower(),
            )
        if not check.integrity_check_passed:
            logger.info(
                "[FOOTBALL][INTEGRITY][DROP] event_id=%s match=%s reason=%s",
                candidate.match.external_event_id,
                candidate.match.match_name,
                check.integrity_check_reason,
            )

    def _infer_family(self, *, market_type: str, market_label: str, selection: str) -> str:
        market = type(
            "MarketStub",
            (),
            {
                "market_type": market_type,
                "market_label": market_label,
                "selection": selection,
                "odds_value": Decimal("2.00"),
            },
        )()
        candidate = type("CandidateStub", (), {"market": market})()
        return self._family_service.get_market_family(candidate)  # type: ignore[arg-type]

    def _is_allowed_selection(self, *, family: str, selection: str, home_team: str, away_team: str) -> bool:
        token = self._selection_token(selection)
        if family == "result":
            home = self._norm(home_team)
            away = self._norm(away_team)
            return token in self._RESULT_TOKENS or self._norm(selection) in {home, away}
        if family == "double_chance":
            return token in self._DOUBLE_CHANCE_TOKENS
        if family == "btts":
            return token in self._YES_NO_TOKENS
        if family == "totals":
            return bool(self._TOTAL_PATTERN.match(token))
        if family == "handicap":
            return bool(self._HANDICAP_PATTERN.match(selection.strip()))
        if family == "combo":
            return bool(self._HTFT_PATTERN.match(token))
        if family == "correct_score":
            return bool(self._EXACT_SCORE_PATTERN.match(selection.strip()))
        if family == "odd_even":
            return token in {"ODD", "EVEN", "НЕЧЕТ", "НЕЧЁТ", "ЧЕТ", "ЧЁТ"}
        return True

    def _selection_token(self, value: str) -> str:
        return self._norm(value).upper().replace("DRAW", "X")

    def _norm(self, value: str | None) -> str:
        return re.sub(r"\s+", " ", str(value or "").strip()).replace(",", ".")

    def _decimal_equal(self, left: str, right: Decimal) -> bool:
        try:
            return Decimal(str(left)) == Decimal(str(right))
        except Exception:
            return False
