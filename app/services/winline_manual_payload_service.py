"""Read and summarize manual Winline JSON files under `examples/manual_winline/`."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from app.services.adapter_ingestion_service import AdapterIngestionService
from app.services.winline_raw_line_bridge_service import WinlineRawLineBridgeService
from app.services.winline_raw_result_bridge_service import WinlineRawResultBridgeService


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

    def __init__(self, *, manual_dir: Path | None = None, uploaded_dir: Path | None = None) -> None:
        self._manual_dir = manual_dir or (_repo_root() / "examples" / "manual_winline")
        self._uploaded_dir = uploaded_dir or (_repo_root() / "runtime" / "manual_winline")

    def get_example_line_payload_path(self) -> Path:
        return self._manual_dir / "line_payload.json"

    def get_example_result_payload_path(self) -> Path:
        return self._manual_dir / "result_payload.json"

    def get_uploaded_line_payload_path(self) -> Path:
        return self._uploaded_dir / "line_payload.json"

    def get_uploaded_result_payload_path(self) -> Path:
        return self._uploaded_dir / "result_payload.json"

    def get_line_payload_path(self) -> Path:
        uploaded = self.get_uploaded_line_payload_path()
        if uploaded.is_file():
            return uploaded
        return self.get_example_line_payload_path()

    def get_result_payload_path(self) -> Path:
        uploaded = self.get_uploaded_result_payload_path()
        if uploaded.is_file():
            return uploaded
        return self.get_example_result_payload_path()

    def get_line_metadata_path(self) -> Path:
        return self.get_uploaded_line_payload_path().with_suffix(".meta.json")

    def load_line_metadata(self) -> tuple[dict[str, Any] | None, str | None]:
        path = self.get_line_metadata_path()
        if not path.is_file():
            return None, "metadata_not_found"
        try:
            text = path.read_text(encoding="utf-8")
            data = json.loads(text)
        except OSError as exc:
            return None, f"metadata_read_error: {exc!s}"
        except json.JSONDecodeError as exc:
            return None, f"metadata_json_invalid: {exc!s}"
        if not isinstance(data, dict):
            return None, "metadata_invalid_root_type"
        return data, None

    def _sha256_file(self, path: Path) -> str | None:
        if not path.is_file():
            return None
        try:
            return hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError:
            return None

    def _example_line_checksums(self) -> set[str]:
        checksums: set[str] = set()
        for path in self._manual_dir.glob("*line*.json"):
            digest = self._sha256_file(path)
            if digest:
                checksums.add(digest)
        return checksums

    def get_line_source_truth(self) -> dict[str, Any]:
        uploaded_path = self.get_uploaded_line_payload_path()
        uploaded_checksum = self._sha256_file(uploaded_path)
        metadata, meta_err = self.load_line_metadata()
        example_checksum_match = uploaded_checksum in self._example_line_checksums() if uploaded_checksum else False
        if uploaded_path.is_file():
            origin = str((metadata or {}).get("source_origin") or (metadata or {}).get("origin") or "unknown_upload")
            uploaded_at = (metadata or {}).get("uploaded_at")
            provenance_present = metadata is not None
            if provenance_present and bool((metadata or {}).get("is_real_source", False)) and not example_checksum_match:
                return {
                    "source_mode": str((metadata or {}).get("source_mode") or "semi_live_manual"),
                    "is_real_source": True,
                    "reason": origin,
                    "source_origin": origin,
                    "uploaded_at": uploaded_at,
                    "metadata_error": None,
                    "provenance_present": True,
                    "file_path": str(uploaded_path),
                    "checksum": uploaded_checksum,
                }
            return {
                "source_mode": "manual_example" if example_checksum_match else "manual_uploaded_untrusted",
                "is_real_source": False,
                "reason": "uploaded_matches_bundled_example" if example_checksum_match else "uploaded_without_real_provenance",
                "source_origin": origin,
                "uploaded_at": uploaded_at,
                "metadata_error": meta_err,
                "provenance_present": provenance_present,
                "file_path": str(uploaded_path),
                "checksum": uploaded_checksum,
            }
        return {
            "source_mode": "manual_example",
            "is_real_source": False,
            "reason": "bundled_example_fixture",
            "source_origin": "bundled_example_fixture",
            "uploaded_at": None,
            "metadata_error": meta_err,
            "provenance_present": False,
            "file_path": str(self.get_example_line_payload_path()),
            "checksum": self._sha256_file(self.get_example_line_payload_path()),
        }

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
        bridge = WinlineRawResultBridgeService()
        base: dict[str, Any] = {
            "ok": False,
            "payload_exists": exists,
            "root_type": None,
            "top_level_keys": [],
            "detected_shape": "unsupported",
            "raw_results_count": None,
            "normalized_results_count": None,
            "event_results_count": None,
            "processible": False,
            "error": err,
        }
        if raw is None:
            if not exists:
                base["error"] = err or "manual_payload_not_loaded"
            return base

        base["root_type"] = _root_type_name(raw)
        base["top_level_keys"] = sorted(raw.keys())
        base["detected_shape"] = bridge.detect_payload_shape(raw)

        raw_rows = bridge._extract_raw_result_rows(raw)
        base["raw_results_count"] = len(raw_rows) if raw_rows else (_len_if_list(raw, "results") or 0)

        try:
            normalized = bridge.normalize_raw_winline_result_payload(raw)
            normalized_rows = list(normalized.get("results") or [])
            base["normalized_results_count"] = len(normalized_rows)
            base["event_results_count"] = len(normalized_rows)
            base["processible"] = len(normalized_rows) > 0
            base["ok"] = bool(base["processible"])
            base["error"] = None if base["ok"] else "no_normalized_results"
        except Exception as exc:  # noqa: BLE001
            base["normalized_results_count"] = None
            base["event_results_count"] = None
            base["processible"] = False
            base["ok"] = False
            base["error"] = str(exc)

        return base
