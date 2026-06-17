from __future__ import annotations

from pathlib import Path
from typing import Any


SENSITIVE_KEY_PARTS = (
    "access_token",
    "client_secret",
    "email",
    "form",
    "link",
    "notion_token",
    "onedrive_path",
    "refresh_token",
    "secret",
    "source",
    "sources",
    "token",
    "upload_url",
    "url",
)


def safe_path_segment(value: Any, fallback: str = "entry", max_length: int = 80) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in str(value or "").strip())
    cleaned = cleaned.strip("._")
    if not cleaned:
        cleaned = fallback
    return cleaned[:max_length]


def safe_upload_filename(filename: str, fallback: str = "upload", max_length: int = 120) -> str:
    name = Path(filename or "").name.strip()
    if not name:
        return fallback
    stem = Path(name).stem
    suffix = Path(name).suffix.lower()
    safe_stem = safe_path_segment(stem, fallback=fallback, max_length=max_length - len(suffix))
    return f"{safe_stem}{suffix}"[:max_length]


def file_extension(filename: str) -> str:
    return Path(filename or "").suffix.lower()


def is_allowed_extension(filename: str, allowed_extensions: tuple[str, ...]) -> bool:
    return file_extension(filename) in allowed_extensions


def redact_for_log(value: Any) -> Any:
    if isinstance(value, dict):
        redacted = {}
        for key, item in value.items():
            key_text = str(key).lower()
            if any(part in key_text for part in SENSITIVE_KEY_PARTS):
                redacted[key] = "<redacted>"
            else:
                redacted[key] = redact_for_log(item)
        return redacted
    if isinstance(value, list):
        return [redact_for_log(item) for item in value]
    return value
