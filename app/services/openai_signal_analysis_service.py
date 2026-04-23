from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.config import get_settings
from app.core.enums import BetResult, SportType
from app.db.models.settlement import Settlement
from app.db.models.signal import Signal
from app.services.signal_runtime_diagnostics_service import SignalRuntimeDiagnosticsService

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OpenAISignalAnalysisRunResult:
    ok: bool
    error_text: str | None = None


def _safe_int(v: Any) -> int | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _selection_side(home: str | None, away: str | None, sel: str | None) -> str:
    s = (sel or "").strip().lower().replace("ё", "е")
    tok = s.replace("х", "x").replace(" ", "").strip(".")
    if tok in {"x", "draw", "н", "ничья"}:
        return "draw"
    if tok in {"1", "p1", "п1", "home"}:
        return "home"
    if tok in {"2", "p2", "п2", "away"}:
        return "away"
    h = (home or "").strip().lower().replace("ё", "е")
    a = (away or "").strip().lower().replace("ё", "е")
    if h and (s == h or h in s or s in h):
        return "home"
    if a and (s == a or a in s or s in a):
        return "away"
    return "unknown"


class OpenAISignalAnalysisService:
    """
    Post-settlement ONLY:
    - called after settlement is registered
    - stores machine-readable JSON into PredictionLog.feature_snapshot_json['openai_analysis']
    - never used in live decisions/scoring
    """

    _BASE_URL = "https://api.openai.com"
    _MODEL = "gpt-4o-mini"
    _ALLOWED_VERDICTS = {"good_signal", "bad_signal", "neutral_signal"}
    _ALLOWED_ERROR_TYPES = {
        "no_value_favorite",
        "late_random_pick",
        "wrong_side_bias",
        "market_misread",
        "noise_signal",
        "correct_value_bet",
        "insufficient_context",
        "other",
    }

    def _normalize_and_validate(self, analysis: dict[str, Any]) -> dict[str, Any]:
        """Hard-normalize OpenAI JSON to a safe schema; never raise."""
        out: dict[str, Any] = {}
        warn: list[str] = []

        verdict = str(analysis.get("verdict") or "").strip()
        if verdict not in self._ALLOWED_VERDICTS:
            verdict = "neutral_signal"
            warn.append("verdict_invalid_defaulted")
        out["verdict"] = verdict

        et = str(analysis.get("error_type") or "").strip()
        if et not in self._ALLOWED_ERROR_TYPES:
            et = "other"
            warn.append("error_type_invalid_defaulted")
        out["error_type"] = et

        conf = 0.0
        try:
            conf = float(analysis.get("confidence"))
            if conf > 1.0 and conf <= 100.0:
                conf = conf / 100.0
                warn.append("confidence_scaled_from_percent")
            conf = max(0.0, min(1.0, conf))
        except Exception:
            conf = 0.0
            warn.append("confidence_parse_failed_defaulted")
        out["confidence"] = round(conf, 4)

        mr = str(analysis.get("mistake_reason") or "").strip()
        out["mistake_reason"] = mr[:220] if mr else ""

        tags_in = analysis.get("pattern_tags")
        tags: list[str] = []
        if isinstance(tags_in, list):
            for t in tags_in[:30]:
                if not t:
                    continue
                ts = str(t).strip()
                if ts:
                    tags.append(ts[:60])
        out["pattern_tags"] = tags

        out["should_penalize"] = bool(analysis.get("should_penalize"))
        out["should_boost"] = bool(analysis.get("should_boost"))

        # Basic consistency: don't allow confident contradictory actions
        if out["verdict"] == "good_signal" and out["should_penalize"] and not out["should_boost"]:
            warn.append("inconsistent_good_but_penalize")
            if out["confidence"] >= 0.75:
                out["should_penalize"] = False
        if out["verdict"] == "bad_signal" and out["should_boost"] and not out["should_penalize"]:
            warn.append("inconsistent_bad_but_boost")
            if out["confidence"] >= 0.75:
                out["should_boost"] = False

        sr = str(analysis.get("summary_ru") or "").strip()
        out["summary_ru"] = sr[:260] if sr else ""

        out["analysis_version"] = 1
        if warn:
            out["_validation_warnings"] = warn
        return out

    async def analyze_settled_live_football_signal(
        self,
        session: AsyncSession,
        *,
        signal_id: int,
    ) -> OpenAISignalAnalysisRunResult:
        settings = get_settings()
        if not getattr(settings, "openai_enabled", False):
            return OpenAISignalAnalysisRunResult(ok=False, error_text="openai_disabled")

        stmt = (
            select(Signal)
            .where(Signal.id == int(signal_id))
            .options(selectinload(Signal.prediction_logs), selectinload(Signal.settlement))
        )
        sig = (await session.execute(stmt)).scalar_one_or_none()
        if not sig:
            return OpenAISignalAnalysisRunResult(ok=False, error_text="signal_not_found")
        if sig.sport != SportType.FOOTBALL or not bool(sig.is_live):
            return OpenAISignalAnalysisRunResult(ok=False, error_text="not_live_football")
        st: Settlement | None = sig.settlement
        if not st or st.result is None:
            return OpenAISignalAnalysisRunResult(ok=False, error_text="not_settled")

        if not sig.prediction_logs:
            return OpenAISignalAnalysisRunResult(ok=False, error_text="no_prediction_log")
        pl0 = min(sig.prediction_logs, key=lambda p: p.id)
        fs0 = dict(pl0.feature_snapshot_json or {})
        ex0 = dict(pl0.explanation_json or {})

        # Idempotency: skip if already analyzed
        existing = fs0.get("openai_analysis")
        if (
            isinstance(existing, dict)
            and existing.get("analysis_version") == 1
            and isinstance(existing.get("verdict"), str)
            and isinstance(existing.get("pattern_tags"), list)
        ):
            return OpenAISignalAnalysisRunResult(ok=True, error_text=None)

        fa = fs0.get("football_analytics") if isinstance(fs0.get("football_analytics"), dict) else {}
        minute = _safe_int(fa.get("minute"))
        sh = _safe_int(fa.get("score_home"))
        sa = _safe_int(fa.get("score_away"))
        side = _selection_side(sig.home_team, sig.away_team, sig.selection)

        strategy_id = str(ex0.get("football_live_strategy_id") or "").strip() or None
        base_score = None
        eff_score = None
        try:
            la = fs0.get("football_live_adaptive_learning") if isinstance(fs0.get("football_live_adaptive_learning"), dict) else {}
            if la and la.get("base_signal_score") is not None:
                base_score = float(la.get("base_signal_score"))
            if la and la.get("effective_live_score") is not None:
                eff_score = float(la.get("effective_live_score"))
        except Exception:
            base_score = None
            eff_score = None

        outcome_audit = fs0.get("football_outcome_audit") if isinstance(fs0.get("football_outcome_audit"), dict) else {}
        outcome_reason_code = (outcome_audit.get("outcome_reason_code") or "").strip() or None
        pattern_keys = outcome_audit.get("signal_pattern_keys") if isinstance(outcome_audit.get("signal_pattern_keys"), list) else []

        payload = {
            "match": sig.match_name,
            "tournament": sig.tournament_name,
            "minute": minute,
            "score_home": sh,
            "score_away": sa,
            "bet_text": f"{sig.market_label}: {sig.selection}",
            "market_family": fa.get("market_family"),
            "market_type": sig.market_type,
            "side": side,
            "odds": float(sig.odds_at_signal) if sig.odds_at_signal is not None else None,
            "strategy_id": strategy_id,
            "base_signal_score": base_score,
            "effective_live_score": eff_score,
            "signal_pattern_keys": pattern_keys,
            "settlement_result": (st.result.value if isinstance(st.result, BetResult) else str(st.result)),
            "profit_loss": float(st.profit_loss) if st.profit_loss is not None else None,
            "outcome_reason_code": outcome_reason_code,
            "football_outcome_audit": outcome_audit,
        }

        sys = (
            "You are a post-match football betting signal auditor. "
            "Return STRICT JSON only, no prose. "
            "Follow the schema exactly. Confidence must be a float in [0.0, 1.0]. Keep strings short."
        )
        schema = {
            "analysis_version": 1,
            "verdict": "good_signal | bad_signal | neutral_signal",
            "confidence": 0.0,
            "error_type": "no_value_favorite | late_random_pick | wrong_side_bias | market_misread | noise_signal | correct_value_bet | insufficient_context | other",
            "mistake_reason": "short text",
            "pattern_tags": ["..."],
            "should_penalize": True,
            "should_boost": False,
            "summary_ru": "short russian summary",
        }
        user = (
            "Input signal JSON:\n"
            + json.dumps(payload, ensure_ascii=False)
            + "\n\nReturn JSON with keys:\n"
            + json.dumps(schema, ensure_ascii=False)
        )

        url = f"{self._BASE_URL}/v1/responses"
        headers = {"Authorization": f"Bearer {settings.openai_api_key}", "Content-Type": "application/json"}
        body = {
            "model": self._MODEL,
            "input": [
                {"role": "system", "content": sys},
                {"role": "user", "content": user},
            ],
            "text": {"format": {"type": "json_object"}},
        }
        tmo = httpx.Timeout(connect=8.0, read=30.0, write=8.0, pool=8.0)

        diag = SignalRuntimeDiagnosticsService()
        diag.update(openai_analysis_total=int((diag.get_state().get("openai_analysis_total") or 0)) + 1)

        try:
            async with httpx.AsyncClient(timeout=tmo) as client:
                r = await client.post(url, headers=headers, json=body)
            if r.status_code != 200:
                txt = (r.text or "").strip()
                if len(txt) > 900:
                    txt = txt[:900] + "..."
                diag.update(openai_analysis_failed=int((diag.get_state().get("openai_analysis_failed") or 0)) + 1)
                return OpenAISignalAnalysisRunResult(ok=False, error_text=f"http_{r.status_code}: {txt or 'empty_body'}")
            data = r.json()
            raw_text: str | None = None
            # Responses API usually returns: output[...].content[...].text
            if isinstance(data, dict) and isinstance(data.get("output"), list):
                for item in data.get("output") or []:
                    if not isinstance(item, dict):
                        continue
                    content = item.get("content")
                    if not isinstance(content, list):
                        continue
                    for c in content:
                        if not isinstance(c, dict):
                            continue
                        if c.get("type") in {"output_text", "text"} and isinstance(c.get("text"), str) and c.get("text"):
                            raw_text = c.get("text")
                            break
                    if raw_text:
                        break
            if raw_text is None and isinstance(data, dict) and isinstance(data.get("output_text"), str):
                raw_text = data.get("output_text")
            raw_text = (raw_text or "").strip()
            analysis0 = json.loads(raw_text) if raw_text else {}
            if not isinstance(analysis0, dict):
                analysis0 = {}
            analysis = self._normalize_and_validate(analysis0)
            analysis["_meta"] = {
                "model": self._MODEL,
                "analyzed_at_utc": datetime.now(timezone.utc).isoformat(),
                "response_id": (data.get("id") if isinstance(data, dict) else None),
            }

            fs0["openai_analysis"] = analysis
            pl0.feature_snapshot_json = fs0
            session.add(pl0)

            # diagnostics derived counts
            try:
                conf = float(analysis.get("confidence") or 0.0)
            except Exception:
                conf = 0.0
            tags = analysis.get("pattern_tags") if isinstance(analysis.get("pattern_tags"), list) else []
            pen = bool(analysis.get("should_penalize"))
            boo = bool(analysis.get("should_boost"))
            succ = int((diag.get_state().get("openai_analysis_success") or 0)) + 1
            diag.update(openai_analysis_success=succ)
            if conf >= 0.75 and tags:
                if pen:
                    diag.update(
                        openai_penalty_patterns_count=int((diag.get_state().get("openai_penalty_patterns_count") or 0))
                        + len(tags)
                    )
                if boo:
                    diag.update(
                        openai_boost_patterns_count=int((diag.get_state().get("openai_boost_patterns_count") or 0))
                        + len(tags)
                    )
            return OpenAISignalAnalysisRunResult(ok=True, error_text=None)
        except Exception as exc:  # noqa: BLE001
            logger.exception("[OPENAI_ANALYSIS] failed signal_id=%s", signal_id)
            diag.update(openai_analysis_failed=int((diag.get_state().get("openai_analysis_failed") or 0)) + 1)
            return OpenAISignalAnalysisRunResult(ok=False, error_text=f"request_error: {exc!s}")

