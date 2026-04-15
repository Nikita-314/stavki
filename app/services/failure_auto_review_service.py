from __future__ import annotations

from decimal import Decimal
from typing import Any

from app.core.enums import BetResult, EntryStatus, FailureCategory
from app.schemas.failure_auto_review import FailureAutoReviewInput, FailureAutoReviewResult


class FailureAutoReviewService:
    def build_auto_review(self, report: FailureAutoReviewInput) -> FailureAutoReviewResult:
        """Build a rule-based automatic failure classification for a single signal report."""
        entries = report.entries
        settlement = report.settlement

        has_entries = len(entries) > 0
        entered_entry = next((e for e in entries if e.status == EntryStatus.ENTERED), None)
        has_entered_entry = entered_entry is not None
        has_settlement = settlement is not None

        has_found_market: bool | None
        if not has_entries:
            has_found_market = None
        else:
            # True if at least one entry explicitly says it was found
            any_found_true = any(e.was_found_in_bookmaker is True for e in entries)
            # False if all provided values are explicitly False
            all_false = all(e.was_found_in_bookmaker is False for e in entries)
            has_found_market = True if any_found_true else (False if all_false else None)

        base_tags: dict[str, Any] = {
            "has_entries": has_entries,
            "has_entered_entry": has_entered_entry,
            "has_found_market": has_found_market,
            "signal_status": str(report.signal.status),
        }

        # Rule 1: not entered
        if (not has_entries) or (not has_entered_entry):
            if has_entries and all(e.was_found_in_bookmaker is False for e in entries):
                return FailureAutoReviewResult(
                    category=FailureCategory.MARKET_UNAVAILABLE,
                    auto_reason="market was not found in bookmaker",
                    failure_tags_json=base_tags,
                    confidence_score=Decimal("0.95"),
                )
            return FailureAutoReviewResult(
                category=FailureCategory.EXECUTION_ERROR,
                auto_reason="signal was not entered manually",
                failure_tags_json=base_tags,
                confidence_score=Decimal("0.95"),
            )

        # Rule 2: entered but no settlement yet
        if has_entered_entry and not has_settlement:
            tags = {
                **base_tags,
                "has_entered_entry": True,
                "has_settlement": False,
            }
            return FailureAutoReviewResult(
                category=FailureCategory.DATA_ISSUE,
                auto_reason="entered signal has no settlement yet",
                failure_tags_json=tags,
                confidence_score=Decimal("0.95"),
            )

        # From here settlement exists
        assert settlement is not None

        # Rule 4/5/6/3 based on settlement.result
        if settlement.result == BetResult.WIN:
            return FailureAutoReviewResult(
                category=FailureCategory.UNKNOWN,
                auto_reason="winning signal does not require failure classification",
                failure_tags_json={**base_tags, "settled_result": "WIN"},
                confidence_score=Decimal("0.50"),
            )

        if settlement.result == BetResult.VOID:
            return FailureAutoReviewResult(
                category=FailureCategory.UNKNOWN,
                auto_reason="void signal does not require failure classification",
                failure_tags_json={**base_tags, "settled_result": "VOID"},
                confidence_score=Decimal("0.50"),
            )

        if settlement.result == BetResult.UNKNOWN:
            return FailureAutoReviewResult(
                category=FailureCategory.DATA_ISSUE,
                auto_reason="settlement result is unknown",
                failure_tags_json={**base_tags, "settled_result": "UNKNOWN"},
                confidence_score=Decimal("0.95"),
            )

        # Rule 3: LOSE
        if settlement.result == BetResult.LOSE:
            entered_odds = entered_entry.entered_odds
            min_entry_odds = report.signal.min_entry_odds

            if entered_odds is not None and entered_odds < min_entry_odds:
                return FailureAutoReviewResult(
                    category=FailureCategory.LINE_MOVEMENT,
                    auto_reason="entry odds were below minimum acceptable odds",
                    failure_tags_json={
                        **base_tags,
                        "signal_odds": report.signal.odds_at_signal,
                        "min_entry_odds": min_entry_odds,
                        "entered_odds": entered_odds,
                        "settled_result": "LOSE",
                    },
                    confidence_score=Decimal("0.85"),
                )

            return FailureAutoReviewResult(
                category=FailureCategory.VARIANCE,
                auto_reason="signal was entered and settled as lose",
                failure_tags_json={
                    **base_tags,
                    "settled_result": "LOSE",
                    "entered_odds": entered_odds,
                    "predicted_prob": report.signal.predicted_prob,
                    "implied_prob": report.signal.implied_prob,
                    "edge": report.signal.edge,
                },
                confidence_score=Decimal("0.70"),
            )

        # Fallback (should not happen)
        return FailureAutoReviewResult(
            category=FailureCategory.UNKNOWN,
            auto_reason="unable to classify failure with current rules",
            failure_tags_json={**base_tags, "settled_result": str(settlement.result)},
            confidence_score=Decimal("0.50"),
        )

