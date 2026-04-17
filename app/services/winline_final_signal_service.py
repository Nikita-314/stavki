"""Unified final demo signal object for the Winline stack (candidate → value → stake).

Orchestration only: no DB, no Telegram, no changes to lower layers.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field

from app.services.winline_stake_service import WinlineStakeRecommendation, WinlineStakeService
from app.services.winline_value_service import WinlineValueAssessment, WinlineValueService


class WinlineFinalSignal(BaseModel):
    event_external_id: str
    sport: str
    match_name: str
    tournament_name: str | None = None
    home_team: str | None = None
    away_team: str | None = None
    source_kind: str | None = None
    market_kind: str | None = None
    market_label: str
    selection: str
    odds_value: Decimal
    implied_prob: Decimal | None = None
    estimated_prob: Decimal | None = None
    edge: Decimal | None = None
    expected_value: Decimal | None = None
    confidence_score: Decimal | None = None
    is_value_bet: bool = False
    recommended_stake_units: Decimal | None = None
    recommended_stake_fraction: Decimal | None = None
    sizing_method: str | None = None
    live_reason_codes: list[str] = Field(default_factory=list)
    value_reason_codes: list[str] = Field(default_factory=list)
    stake_reason_codes: list[str] = Field(default_factory=list)
    summary_label: str | None = None
    short_explanation: str | None = None
    raw_json: dict[str, Any] | None = None


class WinlineFinalSignalPreview(BaseModel):
    case_name: str
    has_signal: bool
    signal: WinlineFinalSignal | None = None
    skip_reason: str | None = None


class WinlineFinalSignalService:
    """Build `WinlineFinalSignal` / previews from live + value + stake services."""

    def make_summary_label(
        self,
        *,
        sport: str,
        match_name: str,
        market_label: str,
        selection: str,
        odds_value: Decimal,
    ) -> str:
        sport_u = (sport or "unknown").strip().upper()
        mn = (match_name or "").strip() or "?"
        ml = (market_label or "").strip() or "?"
        sel = (selection or "").strip().upper() or "?"
        return f"{sport_u} | {mn} | {ml} {sel} @ {odds_value}"

    def make_short_explanation(self, live_reason_codes: list[str]) -> str:
        codes = {c.strip().lower() for c in (live_reason_codes or [])}

        if "late_lead" in codes and "opponent_red_card" in codes:
            return "Команда ведёт в счёте, а соперник остался в меньшинстве."
        if "favorite_collapsed" in codes:
            return "По live-картине фаворит просел, текущий исход выглядит недооценённым."
        if "momentum_rounds" in codes and "win_streak" in codes:
            return "Текущая динамика матча поддерживает выбранный исход."
        if "momentum_rounds" in codes or "win_streak" in codes:
            return "Динамика матча сейчас на стороне выбранного исхода."
        if codes & {"economy_advantage", "strong_advantage", "objective_control", "late_game_lock"}:
            return "Игровое преимущество подтверждает выбранный исход."
        if codes:
            return "Live-оценка поддерживает выбранную ставку."
        return "Сигнал собран по правилам live-оценки."

    def build_final_signal(
        self,
        *,
        case_name: str,
        state: Any,
        candidate: Any,
        live_evaluation: Any,
        value_assessment: WinlineValueAssessment | None,
        stake_recommendation: WinlineStakeRecommendation | None,
    ) -> WinlineFinalSignal | None:
        if candidate is None or value_assessment is None or stake_recommendation is None:
            return None

        live_codes = list(getattr(live_evaluation, "reason_codes", None) or [])
        conf = getattr(live_evaluation, "confidence_score", None)
        if conf is not None and not isinstance(conf, Decimal):
            try:
                conf = Decimal(str(conf))
            except Exception:
                conf = None

        raw_json: dict[str, Any] = {
            "event_external_id": str(getattr(candidate, "event_external_id", "")),
            "case_name": case_name,
            "live_reason_codes": live_codes,
            "confidence_score": str(conf) if conf is not None else None,
            "value_is_positive": bool(value_assessment.is_value_bet),
            "sizing_method": stake_recommendation.sizing_method,
            "source_kind": str(getattr(state, "source_kind", "") or "demo"),
        }

        match_name = str(getattr(state, "match_name", "") or getattr(candidate, "match_name", ""))
        sport = str(getattr(state, "sport", "") or getattr(candidate, "sport", ""))

        summary = self.make_summary_label(
            sport=sport,
            match_name=match_name,
            market_label=str(getattr(candidate, "market_label", "")),
            selection=str(getattr(candidate, "selection", "")),
            odds_value=getattr(candidate, "odds_value", Decimal("1")),
        )
        explain = self.make_short_explanation(live_codes)

        return WinlineFinalSignal(
            event_external_id=str(getattr(candidate, "event_external_id", "")),
            sport=sport,
            match_name=match_name,
            tournament_name=str(getattr(state, "tournament_name", "") or getattr(candidate, "tournament_name", "") or ""),
            home_team=str(getattr(state, "home_team", "") or getattr(candidate, "home_team", "") or ""),
            away_team=str(getattr(state, "away_team", "") or getattr(candidate, "away_team", "") or ""),
            source_kind=str(getattr(state, "source_kind", "") or raw_json.get("source_kind") or "demo"),
            market_kind=getattr(candidate, "market_kind", None),
            market_label=str(getattr(candidate, "market_label", "")),
            selection=str(getattr(candidate, "selection", "")),
            odds_value=getattr(candidate, "odds_value", Decimal("1")),
            implied_prob=value_assessment.implied_prob,
            estimated_prob=value_assessment.estimated_prob,
            edge=value_assessment.edge,
            expected_value=value_assessment.expected_value,
            confidence_score=value_assessment.confidence_score,
            is_value_bet=value_assessment.is_value_bet,
            recommended_stake_units=stake_recommendation.recommended_stake_units,
            recommended_stake_fraction=stake_recommendation.recommended_stake_fraction,
            sizing_method=stake_recommendation.sizing_method,
            live_reason_codes=live_codes,
            value_reason_codes=list(value_assessment.reason_codes or []),
            stake_reason_codes=list(stake_recommendation.reason_codes or []),
            summary_label=summary,
            short_explanation=explain,
            raw_json=raw_json,
        )

    def _preview_from_bundle(self, case_name: str, bundle: dict[str, Any]) -> WinlineFinalSignalPreview:
        from app.services.winline_live_signal_service import WinlineLiveSignalService

        sig = WinlineLiveSignalService()
        state = bundle["live_state"]
        feats = bundle["live_features"]
        ev = bundle["live_evaluation"]
        ar = bundle["line_adapter_result"]

        cands = sig.collect_live_candidates(ar.candidates, state.event_external_id)
        chosen = sig.choose_best_candidate(cands, ev, feats, state)

        if chosen is None:
            return WinlineFinalSignalPreview(
                case_name=case_name,
                has_signal=False,
                signal=None,
                skip_reason="no_candidate",
            )

        val = WinlineValueService()
        assessment = val.assess_candidate(evaluation=ev, features=feats, candidate=chosen)

        if not assessment.is_value_bet:
            return WinlineFinalSignalPreview(
                case_name=case_name,
                has_signal=False,
                signal=None,
                skip_reason="not_value_bet",
            )

        stake_svc = WinlineStakeService()
        stake = stake_svc.build_stake_recommendation(assessment)

        units = stake.recommended_stake_units
        if units is None or units <= 0:
            return WinlineFinalSignalPreview(
                case_name=case_name,
                has_signal=False,
                signal=None,
                skip_reason="no_stake",
            )

        final = self.build_final_signal(
            case_name=case_name,
            state=state,
            candidate=chosen,
            live_evaluation=ev,
            value_assessment=assessment,
            stake_recommendation=stake,
        )
        if final is None:
            return WinlineFinalSignalPreview(
                case_name=case_name,
                has_signal=False,
                signal=None,
                skip_reason="build_failed",
            )

        return WinlineFinalSignalPreview(
            case_name=case_name,
            has_signal=True,
            signal=final,
            skip_reason=None,
        )

    def build_preview_for_case(self, case_name: str) -> WinlineFinalSignalPreview:
        from app.services.winline_live_signal_service import WinlineLiveSignalService

        sig = WinlineLiveSignalService()
        bundles = sig.build_live_demo_inputs()
        if case_name not in bundles:
            return WinlineFinalSignalPreview(
                case_name=case_name,
                has_signal=False,
                signal=None,
                skip_reason="unknown_case",
            )

        return self._preview_from_bundle(case_name, bundles[case_name])

    def build_all_previews(self) -> list[WinlineFinalSignalPreview]:
        """All demo cases through `build_preview_for_case` (handlers avoid duplicating loop logic)."""
        from app.services.winline_live_signal_service import WinlineLiveSignalService

        sig = WinlineLiveSignalService()
        return [self.build_preview_for_case(name) for name in sig.build_live_demo_inputs().keys()]

    def build_previews_from_normalized_line_payload(
        self, payload: dict[str, Any], *, max_candidates: int = 20
    ) -> list[WinlineFinalSignalPreview]:
        """Final-signal previews from an adapter-normalized line payload (manual JSON → synthetic live bundle)."""
        from app.services.adapter_ingestion_service import AdapterIngestionService
        from app.services.winline_live_signal_service import WinlineLiveSignalService

        ad = AdapterIngestionService()
        try:
            ar = ad.preview_payload(payload)
        except Exception:
            return []

        live = WinlineLiveSignalService()
        out: list[WinlineFinalSignalPreview] = []
        for i, cand in enumerate(ar.candidates[:max_candidates]):
            eid = str(getattr(getattr(cand, "match", None), "external_event_id", "") or i)
            case_name = f"manual_{eid}_{i}"
            bundle = live.build_synthetic_bundle_from_provider_candidate(cand)
            if bundle is None:
                out.append(
                    WinlineFinalSignalPreview(
                        case_name=case_name,
                        has_signal=False,
                        signal=None,
                        skip_reason="manual_bundle_unresolvable_selection",
                    )
                )
                continue
            out.append(self._preview_from_bundle(case_name, bundle))
        return out

    def preview_final_signal_demo(self) -> None:
        from app.services.winline_live_signal_service import WinlineLiveSignalService

        sig = WinlineLiveSignalService()
        for case_name in sig.build_live_demo_inputs().keys():
            prev = self.build_preview_for_case(case_name)
            print(f"=== FINAL SIGNAL CASE: {case_name} ===")
            if not prev.has_signal or prev.signal is None:
                print("has_signal: no")
                print(f"skip_reason: {prev.skip_reason}")
                print()
                continue

            s = prev.signal
            print("has_signal: yes")
            print("summary:")
            print(f"- {s.summary_label}")
            print("explanation:")
            print(f"- {s.short_explanation}")
            print("metrics:")
            ip = s.implied_prob
            print(f"- implied: {float(ip):.4f}" if ip is not None else "- implied: n/a")
            ep = s.estimated_prob
            print(f"- estimated: {float(ep):.4f}" if ep is not None else "- estimated: n/a")
            print(f"- edge: {float(s.edge):.4f}" if s.edge is not None else "- edge: n/a")
            print(f"- ev: {float(s.expected_value):.4f}" if s.expected_value is not None else "- ev: n/a")
            cs = s.confidence_score
            print(f"- confidence: {float(cs):.4f}" if cs is not None else "- confidence: n/a")
            su = s.recommended_stake_units
            print(f"- stake_units: {float(su):.2f}" if su is not None else "- stake_units: n/a")
            kf = s.recommended_stake_fraction
            print(f"- kelly_fraction: {float(kf):.4f}" if kf is not None else "- kelly_fraction: n/a")
            print()


if __name__ == "__main__":
    WinlineFinalSignalService().preview_final_signal_demo()
