"""Persist manual Winline JSON files under `examples/manual_winline/` (UTF-8, validated writes)."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

from app.services.winline_manual_payload_service import WinlineManualPayloadService

_MAX_BYTES = 5 * 1024 * 1024  # 5 MB — aligned with Telegram upload guard in handlers


def _root_type_name(value: Any) -> str:
    if isinstance(value, dict):
        return "dict"
    if isinstance(value, list):
        return "list"
    return type(value).__name__


class WinlineManualFileStorageService:
    """Atomic-ish writes: validate JSON before replacing existing file."""

    def __init__(self, *, manual_dir: Path | None = None) -> None:
        self._paths = WinlineManualPayloadService(manual_dir=manual_dir)

    def get_line_payload_path(self) -> Path:
        return self._paths.get_line_payload_path()

    def get_result_payload_path(self) -> Path:
        return self._paths.get_result_payload_path()

    def validate_json_bytes(self, data: bytes) -> dict[str, Any]:
        out: dict[str, Any] = {
            "ok": False,
            "path": None,
            "bytes": len(data),
            "top_level_type": None,
            "top_level_keys": None,
            "error": None,
        }
        if len(data) > _MAX_BYTES:
            out["error"] = f"file_too_large: max {_MAX_BYTES} bytes"
            return out
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError as exc:
            out["error"] = f"utf8_decode: {exc!s}"
            return out
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            out["error"] = f"json_parse: {exc!s}"
            return out
        out["ok"] = True
        out["top_level_type"] = _root_type_name(parsed)
        if isinstance(parsed, dict):
            out["top_level_keys"] = sorted(parsed.keys())
        else:
            out["top_level_keys"] = None
        return out

    def _atomic_write(self, path: Path, text: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix="winline_manual_", suffix=".json", dir=path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(text)
            os.replace(tmp, path)
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def save_line_payload_bytes(self, data: bytes) -> dict[str, Any]:
        path = self.get_line_payload_path()
        v = self.validate_json_bytes(data)
        v["path"] = str(path)
        if not v["ok"]:
            return v
        self._atomic_write(path, data.decode("utf-8"))
        return v

    def save_result_payload_bytes(self, data: bytes) -> dict[str, Any]:
        path = self.get_result_payload_path()
        v = self.validate_json_bytes(data)
        v["path"] = str(path)
        if not v["ok"]:
            return v
        self._atomic_write(path, data.decode("utf-8"))
        return v

    def read_line_payload_text(self) -> str | None:
        path = self.get_line_payload_path()
        if not path.is_file():
            return None
        try:
            return path.read_text(encoding="utf-8")
        except OSError:
            return None

    def read_result_payload_text(self) -> str | None:
        path = self.get_result_payload_path()
        if not path.is_file():
            return None
        try:
            return path.read_text(encoding="utf-8")
        except OSError:
            return None

    def clear_line_payload(self) -> dict[str, Any]:
        path = self.get_line_payload_path()
        try:
            self._atomic_write(path, "{}\n")
            return {"ok": True, "path": str(path), "error": None}
        except OSError as exc:
            return {"ok": False, "path": str(path), "error": str(exc)}

    def clear_result_payload(self) -> dict[str, Any]:
        path = self.get_result_payload_path()
        try:
            self._atomic_write(path, '{"source_name":"winline","results":[]}\n')
            return {"ok": True, "path": str(path), "error": None}
        except OSError as exc:
            return {"ok": False, "path": str(path), "error": str(exc)}

    def get_file_status(self) -> dict[str, Any]:
        lp = self.get_line_payload_path()
        rp = self.get_result_payload_path()

        def one(path: Path, label: str) -> dict[str, Any]:
            ex = path.is_file()
            size = path.stat().st_size if ex else 0
            readable = False
            keys: list[str] | None = None
            err: str | None = None
            if ex:
                try:
                    text = path.read_text(encoding="utf-8")
                    obj = json.loads(text)
                    readable = True
                    if isinstance(obj, dict):
                        keys = sorted(obj.keys())
                except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                    err = str(exc)
            return {
                f"{label}_exists": ex,
                f"{label}_size_bytes": size,
                f"{label}_readable": readable,
                f"{label}_keys": keys,
                f"{label}_error": err,
            }

        out: dict[str, Any] = {"ok": True}
        out.update(one(lp, "line"))
        out.update(one(rp, "result"))
        return out
