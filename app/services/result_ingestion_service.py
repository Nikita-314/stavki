from __future__ import annotations

import logging
import math
from decimal import Decimal, InvalidOperation

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import BetResult, SportType
from app.db.repositories.signal_repository import SignalRepository
from app.schemas.event_result import EventResultInput, EventResultProcessingResult
from app.services.football_bet_formatter_service import FootballBetFormatterService
from app.schemas.settlement import SettlementCreate
from app.services.failure_review_service import FailureReviewService
from app.services.football_signal_outcome_reason_service import FootballSignalOutcomeReasonService
from app.services.openai_signal_analysis_service import OpenAISignalAnalysisService
from app.services.settlement_service import SettlementService

logger = logging.getLogger(__name__)


class ResultIngestionService:
    async def process_event_result(self, session: AsyncSession, data: EventResultInput) -> EventResultProcessingResult:
        """Process an external event result and auto-settle matching signals (no commit).

        This is an analytical auto-settlement (unit-based P/L), not an execution ledger.
        """
        signals = await SignalRepository().list_unsettled_by_event_external_id(session, data.event_external_id, data.sport)

        processed_ids: list[int] = []
        settled_count = 0
        skipped = 0
        created_reviews = 0

        for s in signals:
            if not self._supports_market_type(signal=s):
                skipped += 1
                continue

            result = self._determine_result(
                signal=s,
                is_void=data.is_void,
                winner_selection=data.winner_selection,
                result_payload_json=data.result_payload_json,
            )
            if result is None:
                skipped += 1
                continue

            profit_loss = self._unit_profit_loss(result=result, odds_at_signal=s.odds_at_signal)

            await SettlementService().register_settlement(
                session,
                SettlementCreate(
                    signal_id=s.id,
                    result=result,
                    profit_loss=profit_loss,
                    settled_at=data.settled_at,
                    result_details=None,
                    bankroll_before=None,
                    bankroll_after=None,
                ),
            )
            if s.sport == SportType.FOOTBALL:
                try:
                    await FootballSignalOutcomeReasonService().apply_to_signal(session, s, result, data)
                except Exception:
                    logger.exception("football outcome reason apply failed (signal_id=%s)", s.id)
                # OpenAI analysis is strictly post-settlement and best-effort (must never break settlement pipeline).
                if bool(s.is_live):
                    try:
                        await OpenAISignalAnalysisService().analyze_settled_live_football_signal(
                            session, signal_id=int(s.id)
                        )
                    except Exception:
                        logger.exception("openai post-settlement analysis failed (signal_id=%s)", s.id)
            settled_count += 1
            processed_ids.append(int(s.id))

            if result in {BetResult.LOSE, BetResult.UNKNOWN, BetResult.VOID}:
                await FailureReviewService().register_auto_failure_review(session, s.id)
                created_reviews += 1

        return EventResultProcessingResult(
            total_signals_found=len(signals),
            settled_signals=settled_count,
            skipped_signals=skipped,
            created_failure_reviews=created_reviews,
            processed_signal_ids=processed_ids,
        )

    def _determine_result(
        self,
        *,
        signal,
        is_void: bool,
        winner_selection: str | None,
        result_payload_json: dict | None,
    ) -> BetResult | None:
        if is_void:
            return BetResult.VOID
        if signal.sport == SportType.FOOTBALL:
            by_score = self._determine_football_result_from_score(signal=signal, payload=result_payload_json)
            if by_score is not None:
                return by_score
        if winner_selection is None:
            return None

        return (
            BetResult.WIN
            if self._selection_matches_outcome(
                signal_selection=signal.selection,
                winner_selection=winner_selection,
                home_team=signal.home_team,
                away_team=signal.away_team,
            )
            else BetResult.LOSE
        )

    def _determine_football_result_from_score(self, *, signal, payload: dict | None) -> BetResult | None:
        if not isinstance(payload, dict):
            return None
        sh = self._safe_int(self._first_present(payload, "score_home", "home_score", "goals_home"))
        sa = self._safe_int(self._first_present(payload, "score_away", "away_score", "goals_away"))
        if sh is None or sa is None:
            return None
        market_type = str(signal.market_type or "").strip().lower()
        if market_type in {"match_winner", "1x2"}:
            return self._determine_football_1x2_result_from_score(signal=signal, score_home=sh, score_away=sa)
        return self._determine_football_total_result_from_score(signal=signal, score_home=sh, score_away=sa)

    def _determine_football_1x2_result_from_score(
        self,
        *,
        signal,
        score_home: int,
        score_away: int,
    ) -> BetResult | None:
        if score_home > score_away:
            winner = "home"
        elif score_away > score_home:
            winner = "away"
        else:
            winner = "draw"
        sel_side = self._selection_side(
            selection=signal.selection,
            home_team=signal.home_team,
            away_team=signal.away_team,
        )
        if sel_side is None:
            return None
        return BetResult.WIN if sel_side == winner else BetResult.LOSE

    def _determine_football_total_result_from_score(
        self,
        *,
        signal,
        score_home: int,
        score_away: int,
    ) -> BetResult | None:
        ctx = FootballBetFormatterService().describe_total_context(
            market_type=getattr(signal, "market_type", None),
            market_label=getattr(signal, "market_label", None),
            selection=getattr(signal, "selection", None),
            home_team=getattr(signal, "home_team", None),
            away_team=getattr(signal, "away_team", None),
            section_name=getattr(signal, "section_name", None),
            subsection_name=getattr(signal, "subsection_name", None),
        )
        if ctx is None or not ctx.total_side or not ctx.total_line:
            return None
        line = self._safe_decimal(ctx.total_line)
        if line is None:
            return None
        observed: Decimal | None
        if ctx.target_scope == "match":
            observed = Decimal(score_home + score_away)
        elif ctx.target_scope == "home_team":
            observed = Decimal(score_home)
        elif ctx.target_scope == "away_team":
            observed = Decimal(score_away)
        elif ctx.target_scope == "team_total":
            side = self._team_total_side_from_context(
                team_name=ctx.team_name,
                home_team=getattr(signal, "home_team", None),
                away_team=getattr(signal, "away_team", None),
            )
            if side == "home":
                observed = Decimal(score_home)
            elif side == "away":
                observed = Decimal(score_away)
            else:
                return None
        else:
            return None
        if ctx.total_side == "ТБ":
            if observed > line:
                return BetResult.WIN
            if observed == line:
                return BetResult.VOID
            return BetResult.LOSE
        if ctx.total_side == "ТМ":
            if observed < line:
                return BetResult.WIN
            if observed == line:
                return BetResult.VOID
            return BetResult.LOSE
        return None

    def _selection_matches_outcome(
        self,
        *,
        signal_selection: str,
        winner_selection: str,
        home_team: str,
        away_team: str,
    ) -> bool:
        sig_side = self._selection_side(selection=signal_selection, home_team=home_team, away_team=away_team)
        win_side = self._selection_side(selection=winner_selection, home_team=home_team, away_team=away_team)
        if sig_side and win_side:
            return sig_side == win_side
        return (signal_selection or "").strip().lower() == (winner_selection or "").strip().lower()

    def _selection_side(self, *, selection: str | None, home_team: str | None, away_team: str | None) -> str | None:
        sel = (selection or "").strip().lower().replace("ё", "е")
        tok = sel.replace("х", "x").replace(" ", "").strip(".")
        if tok in {"1", "p1", "п1", "home"}:
            return "home"
        if tok in {"2", "p2", "п2", "away"}:
            return "away"
        if tok in {"x", "draw", "ничья", "нич", "н"}:
            return "draw"
        home = (home_team or "").strip().lower().replace("ё", "е")
        away = (away_team or "").strip().lower().replace("ё", "е")
        if home and (sel == home or home in sel or sel in home):
            return "home"
        if away and (sel == away or away in sel or sel in away):
            return "away"
        return None

    def _safe_int(self, value: object) -> int | None:
        if value is None or isinstance(value, bool):
            return None
        try:
            f = float(value)
            if math.isnan(f):
                return None
            return int(f)
        except (TypeError, ValueError):
            return None

    def _safe_decimal(self, value: object) -> Decimal | None:
        if value is None or isinstance(value, bool):
            return None
        try:
            return Decimal(str(value).strip().replace(",", "."))
        except (InvalidOperation, ValueError):
            return None

    def _first_present(self, payload: dict, *keys: str) -> object | None:
        for key in keys:
            if key in payload:
                return payload.get(key)
        return None

    def _team_total_side_from_context(
        self,
        *,
        team_name: str | None,
        home_team: str | None,
        away_team: str | None,
    ) -> str | None:
        if not team_name:
            return None
        return self._selection_side(selection=team_name, home_team=home_team, away_team=away_team)

    def _supports_market_type(self, *, signal) -> bool:
        market_type = str(getattr(signal, "market_type", "") or "").strip().lower()
        if market_type in {"match_winner", "map_winner", "1x2"}:
            return True
        if getattr(signal, "sport", None) != SportType.FOOTBALL:
            return False
        ctx = FootballBetFormatterService().describe_total_context(
            market_type=getattr(signal, "market_type", None),
            market_label=getattr(signal, "market_label", None),
            selection=getattr(signal, "selection", None),
            home_team=getattr(signal, "home_team", None),
            away_team=getattr(signal, "away_team", None),
            section_name=getattr(signal, "section_name", None),
            subsection_name=getattr(signal, "subsection_name", None),
        )
        return bool(ctx and ctx.total_side and ctx.total_line)

    def _unit_profit_loss(self, *, result: BetResult, odds_at_signal: Decimal) -> Decimal:
        if result == BetResult.WIN:
            return odds_at_signal - Decimal("1")
        if result == BetResult.LOSE:
            return Decimal("-1")
        if result == BetResult.VOID:
            return Decimal("0")
        return Decimal("0")

