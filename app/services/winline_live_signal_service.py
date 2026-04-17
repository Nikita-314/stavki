"""Demo live inputs for the Winline stack: synthetic states, features, line candidates.

No HTTP/websocket — bundled scenarios only. Names and shapes must stay aligned with
`WinlineValueService` / `WinlineFinalSignalService` consumption.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field

from app.core.enums import SportType
from app.schemas.provider_models import ProviderSignalCandidate


class WinlineDemoCandidate(BaseModel):
    """Synthetic line candidate matching expected consumer attributes."""

    event_external_id: str
    market_kind: str = "MATCH_RESULT"
    market_label: str
    selection: str
    odds_value: Decimal
    implied_prob: Decimal | None = None
    tournament_name: str | None = None
    home_team: str | None = None
    away_team: str | None = None


class WinlineDemoLiveState(BaseModel):
    event_external_id: str
    sport: str
    match_name: str
    tournament_name: str | None = None
    home_team: str | None = None
    away_team: str | None = None
    source_kind: str = "demo"


class WinlineDemoLiveFeatures(BaseModel):
    is_home_leading: bool | None = None
    is_away_leading: bool | None = None


class WinlineDemoLiveEvaluation(BaseModel):
    reason_codes: list[str] = Field(default_factory=list)
    confidence_score: Decimal = Decimal("0.80")


class WinlineLineAdapterResult(BaseModel):
    candidates: list[WinlineDemoCandidate]


def _implied(odds: Decimal) -> Decimal:
    return (Decimal("1") / odds).quantize(Decimal("0.0001"))


class WinlineLiveSignalService:
    """Build deterministic demo bundles per case (live → line adapter)."""

    def build_live_demo_inputs(self) -> dict[str, dict[str, Any]]:
        # 1) Football: late lead + opponent red → HOME when home leads (value path).
        ftb_late = Decimal("1.85")
        c1 = WinlineDemoCandidate(
            event_external_id="winline_demo_ftb_late_001",
            market_kind="MATCH_RESULT",
            market_label="Match Result",
            selection="HOME",
            odds_value=ftb_late,
            implied_prob=_implied(ftb_late),
        )
        bundle_late = {
            "live_state": WinlineDemoLiveState(
                event_external_id=c1.event_external_id,
                sport="football",
                match_name="Zenit vs Spartak",
                tournament_name="РПЛ",
                home_team="Zenit",
                away_team="Spartak",
                source_kind="demo",
            ),
            "live_features": WinlineDemoLiveFeatures(is_home_leading=True, is_away_leading=False),
            "live_evaluation": WinlineDemoLiveEvaluation(
                reason_codes=["late_lead", "opponent_red_card"],
                confidence_score=Decimal("0.82"),
            ),
            "line_adapter_result": WinlineLineAdapterResult(candidates=[c1]),
        }

        # 2) Football: favorite collapsed → AWAY.
        ftb_fav = Decimal("2.40")
        c2 = WinlineDemoCandidate(
            event_external_id="winline_demo_ftb_fav_002",
            market_kind="MATCH_RESULT",
            market_label="Match Result",
            selection="AWAY",
            odds_value=ftb_fav,
            implied_prob=_implied(ftb_fav),
        )
        bundle_fav = {
            "live_state": WinlineDemoLiveState(
                event_external_id=c2.event_external_id,
                sport="football",
                match_name="Liverpool vs Everton",
                tournament_name="АПЛ",
                home_team="Liverpool",
                away_team="Everton",
                source_kind="demo",
            ),
            "live_features": WinlineDemoLiveFeatures(is_home_leading=False, is_away_leading=True),
            "live_evaluation": WinlineDemoLiveEvaluation(
                reason_codes=["favorite_collapsed"],
                confidence_score=Decimal("0.81"),
            ),
            "line_adapter_result": WinlineLineAdapterResult(candidates=[c2]),
        }

        # 3) CS2: momentum + win streak.
        cs_odds = Decimal("1.83")
        c3 = WinlineDemoCandidate(
            event_external_id="winline_demo_cs2_001",
            market_kind="MATCH_RESULT",
            market_label="Match Winner",
            selection="HOME",
            odds_value=cs_odds,
            implied_prob=_implied(cs_odds),
        )
        bundle_cs2 = {
            "live_state": WinlineDemoLiveState(
                event_external_id=c3.event_external_id,
                sport="cs2",
                match_name="Team Spirit vs NAVI",
                tournament_name="PGL CS2",
                home_team="Team Spirit",
                away_team="NAVI",
                source_kind="demo",
            ),
            "live_features": WinlineDemoLiveFeatures(),
            "live_evaluation": WinlineDemoLiveEvaluation(
                reason_codes=["momentum_rounds", "win_streak"],
                confidence_score=Decimal("0.88"),
            ),
            "line_adapter_result": WinlineLineAdapterResult(candidates=[c3]),
        }

        # 4) Dota 2: economy / map control heuristics.
        dota_odds = Decimal("1.90")
        c4 = WinlineDemoCandidate(
            event_external_id="winline_demo_dota2_001",
            market_kind="MATCH_RESULT",
            market_label="Match Winner",
            selection="AWAY",
            odds_value=dota_odds,
            implied_prob=_implied(dota_odds),
        )
        bundle_dota = {
            "live_state": WinlineDemoLiveState(
                event_external_id=c4.event_external_id,
                sport="dota2",
                match_name="Team A vs Team B",
                tournament_name="DreamLeague",
                home_team="Team A",
                away_team="Team B",
                source_kind="demo",
            ),
            "live_features": WinlineDemoLiveFeatures(),
            "live_evaluation": WinlineDemoLiveEvaluation(
                reason_codes=["economy_advantage", "objective_control"],
                confidence_score=Decimal("0.84"),
            ),
            "line_adapter_result": WinlineLineAdapterResult(candidates=[c4]),
        }

        # 5) Filtered: same structure as (1) but low confidence → not a value bet.
        ftb_noise = Decimal("1.85")
        c5 = WinlineDemoCandidate(
            event_external_id="winline_demo_ftb_noise_001",
            market_kind="MATCH_RESULT",
            market_label="Match Result",
            selection="HOME",
            odds_value=ftb_noise,
            implied_prob=_implied(ftb_noise),
        )
        bundle_noise = {
            "live_state": WinlineDemoLiveState(
                event_external_id=c5.event_external_id,
                sport="football",
                match_name="Demo vs Demo B",
                tournament_name="Тестовый матч",
                home_team="Demo",
                away_team="Demo B",
                source_kind="demo",
            ),
            "live_features": WinlineDemoLiveFeatures(is_home_leading=True, is_away_leading=False),
            "live_evaluation": WinlineDemoLiveEvaluation(
                reason_codes=["late_lead", "opponent_red_card"],
                confidence_score=Decimal("0.55"),
            ),
            "line_adapter_result": WinlineLineAdapterResult(candidates=[c5]),
        }

        return {
            "football_late_lead": bundle_late,
            "football_favorite_collapsed": bundle_fav,
            "cs2_momentum": bundle_cs2,
            "dota2_macro": bundle_dota,
            "football_low_conf_filtered": bundle_noise,
        }

    def collect_live_candidates(
        self,
        candidates: list[WinlineDemoCandidate] | list[Any],
        event_external_id: str,
    ) -> list[WinlineDemoCandidate]:
        eid = (event_external_id or "").strip()
        out: list[WinlineDemoCandidate] = []
        for c in candidates:
            ce = getattr(c, "event_external_id", None)
            if ce is not None and str(ce).strip() == eid:
                if isinstance(c, WinlineDemoCandidate):
                    out.append(c)
                else:
                    out.append(WinlineDemoCandidate.model_validate(c))
        return out

    def choose_best_candidate(
        self,
        candidates: list[WinlineDemoCandidate],
        live_evaluation: Any,
        features: Any,
        state: Any,
    ) -> WinlineDemoCandidate | None:
        """Demo: single candidate per case; return first when event matches."""
        if not candidates:
            return None
        return candidates[0]

    def _resolve_match_side(self, cand: ProviderSignalCandidate) -> str | None:
        """Map provider selection / team names to HOME | AWAY | DRAW for demo heuristics."""
        sel = (cand.market.selection or "").strip()
        sl = sel.lower()
        h = (cand.match.home_team or "").strip().lower()
        a = (cand.match.away_team or "").strip().lower()
        if sl in {"home", "1", "п1", "h"}:
            return "HOME"
        if sl in {"away", "2", "п2", "a"}:
            return "AWAY"
        if sl in {"draw", "x", "ничья"}:
            return "DRAW"
        if h and sl == h:
            return "HOME"
        if a and sl == a:
            return "AWAY"
        return None

    def build_synthetic_bundle_from_provider_candidate(
        self, cand: ProviderSignalCandidate
    ) -> dict[str, Any] | None:
        """Synthetic live bundle so manual line rows can reuse value/stake/final (semi-real)."""
        side = self._resolve_match_side(cand)
        if side is None:
            return None
        if side == "DRAW":
            return None

        sport = cand.match.sport
        if sport not in (SportType.FOOTBALL, SportType.CS2, SportType.DOTA2):
            return None

        sport_str = {SportType.FOOTBALL: "football", SportType.CS2: "cs2", SportType.DOTA2: "dota2"}[sport]
        odds = cand.market.odds_value
        wc = WinlineDemoCandidate(
            event_external_id=str(cand.match.external_event_id),
            market_kind="MATCH_RESULT",
            market_label=str(cand.market.market_label or "Match"),
            selection=side,
            odds_value=odds,
            implied_prob=_implied(odds),
            tournament_name=str(cand.match.tournament_name or ""),
            home_team=str(cand.match.home_team or ""),
            away_team=str(cand.match.away_team or ""),
        )
        state = WinlineDemoLiveState(
            event_external_id=wc.event_external_id,
            sport=sport_str,
            match_name=str(cand.match.match_name or "?"),
            tournament_name=str(cand.match.tournament_name or ""),
            home_team=str(cand.match.home_team or ""),
            away_team=str(cand.match.away_team or ""),
            source_kind=str(
                cand.feature_snapshot_json.get("runtime_source_kind")
                or cand.notes
                or "live"
            ),
        )

        if sport == SportType.FOOTBALL:
            if side == "HOME":
                feats = WinlineDemoLiveFeatures(is_home_leading=True, is_away_leading=False)
                ev = WinlineDemoLiveEvaluation(
                    reason_codes=["late_lead", "opponent_red_card"],
                    confidence_score=Decimal("0.82"),
                )
            else:
                feats = WinlineDemoLiveFeatures(is_home_leading=False, is_away_leading=True)
                ev = WinlineDemoLiveEvaluation(
                    reason_codes=["favorite_collapsed"],
                    confidence_score=Decimal("0.81"),
                )
        elif sport == SportType.CS2:
            feats = WinlineDemoLiveFeatures()
            ev = WinlineDemoLiveEvaluation(
                reason_codes=["momentum_rounds", "win_streak"],
                confidence_score=Decimal("0.88"),
            )
        else:
            feats = WinlineDemoLiveFeatures()
            ev = WinlineDemoLiveEvaluation(
                reason_codes=["economy_advantage", "objective_control"],
                confidence_score=Decimal("0.84"),
            )

        return {
            "live_state": state,
            "live_features": feats,
            "live_evaluation": ev,
            "line_adapter_result": WinlineLineAdapterResult(candidates=[wc]),
        }
