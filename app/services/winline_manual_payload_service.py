"""Read and summarize manual Winline JSON files under `examples/manual_winline/`."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.services.adapter_ingestion_service import AdapterIngestionService
from app.services.winline_raw_line_bridge_service import WinlineRawLineBridgeService


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _root_type_name(value: Any) -> str:
    if isinstance(value, dict):
        return "dict"
    if isinstance(value, list):
        return "list"
    return type(value).__name__


def _len_if_list(d: dict[str, Any], key: str) -> int | None:
    v = d.get(key)
    if isinstance(v, list):
        return len(v)
    return None


class WinlineManualPayloadService:
    """Paths + safe load + structured previews for manual line/result JSON."""

    def __init__(self, *, manual_dir: Path | None = None) -> None:
        self._manual_dir = manual_dir or (_repo_root() / "examples" / "manual_winline")

    def get_line_payload_path(self) -> Path:
        return self._manual_dir / "line_payload.json"

    def get_result_payload_path(self) -> Path:
        return self._manual_dir / "result_payload.json"

    def line_payload_exists(self) -> bool:
        return self.get_line_payload_path().is_file()

    def result_payload_exists(self) -> bool:
        return self.get_result_payload_path().is_file()

    def load_line_payload(self) -> tuple[dict[str, Any] | None, str | None]:
        path = self.get_line_payload_path()
        if not path.is_file():
            return None, "file_not_found"
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            return None, f"read_error: {exc!s}"
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            return None, f"json_invalid: {exc!s}"
        if not isinstance(data, dict):
            return None, "invalid_root_type"
        return data, None

    def load_result_payload(self) -> tuple[dict[str, Any] | None, str | None]:
        path = self.get_result_payload_path()
        if not path.is_file():
            return None, "file_not_found"
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            return None, f"read_error: {exc!s}"
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            return None, f"json_invalid: {exc!s}"
        if not isinstance(data, dict):
            return None, "invalid_root_type"
        return data, None

    def preview_line_payload(self) -> dict[str, Any]:
        """Summary for Telegram / diagnostics; supports normalized and raw Winline-ish shapes."""
        raw, err = self.load_line_payload()
        exists = self.line_payload_exists()
        bridge = WinlineRawLineBridgeService()
        base: dict[str, Any] = {
            "ok": False,
            "payload_exists": exists,
            "root_type": None,
            "top_level_keys": [],
            "detected_shape": "unsupported",
            "raw_events_count": None,
            "events_count": None,
            "lines_count": None,
            "championships_count": None,
            "normalized_events_count": None,
            "normalized_markets_count": None,
            "preview_candidates": None,
            "ingestible_shape": False,
            "error": err,
        }
        if raw is None:
            if not exists:
                base["error"] = err or "manual_payload_not_loaded"
            return base

        base["root_type"] = _root_type_name(raw)
        base["top_level_keys"] = sorted(raw.keys())
        base["detected_shape"] = bridge.detect_payload_shape(raw)
        base["raw_events_count"] = _len_if_list(raw, "events")
        base["events_count"] = _len_if_list(raw, "events")
        base["lines_count"] = _len_if_list(raw, "lines")
        base["championships_count"] = _len_if_list(raw, "championships")

        try:
            normalized = bridge.normalize_raw_winline_line_payload(raw)
            base["normalized_events_count"] = len(normalized.get("events") or [])
            base["normalized_markets_count"] = len(normalized.get("markets") or [])
            ar = AdapterIngestionService().preview_payload(normalized)
            base["preview_candidates"] = len(ar.candidates)
            base["ingestible_shape"] = (
                base["normalized_events_count"] > 0
                and base["normalized_markets_count"] > 0
                and base["preview_candidates"] is not None
            )
            base["ok"] = bool(base["ingestible_shape"])
            base["error"] = None if base["ok"] else "manual_line_payload_incomplete"
        except Exception as exc:  # noqa: BLE001
            base["ingestible_shape"] = False
            base["preview_candidates"] = None
            if base["error"] is None:
                base["error"] = str(exc)

        return base

    def preview_result_payload(self) -> dict[str, Any]:
        raw, err = self.load_result_payload()
        exists = self.result_payload_exists()
        base: dict[str, Any] = {
            "ok": False,
            "payload_exists": exists,
            "root_type": None,
            "top_level_keys": [],
            "raw_results_count": None,
            "event_results_count": None,
            "error": err,
        }
        if raw is None:
            if not exists:
                base["error"] = err or "manual_payload_not_loaded"
            return base

        base["root_type"] = _root_type_name(raw)
        base["top_level_keys"] = sorted(raw.keys())
        results = raw.get("results")
        if isinstance(results, list):
            base["raw_results_count"] = len(results)
            base["event_results_count"] = len(results)
            base["ok"] = True
            base["error"] = None
        else:
            base["raw_results_count"] = None
            base["event_results_count"] = None
            base["ok"] = False
            base["error"] = "result_payload_missing_results_array"

        return base
