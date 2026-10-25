from __future__ import annotations

from dataclasses import dataclass
from typing import BinaryIO, Protocol


@dataclass(frozen=True)
class UploadResult:
    path: str
    web_url: str = ""


class Storage(Protocol):
    """Where archived files go. Implemented by OneDriveClient and LocalStorage."""

    def upload_bytes(self, relative_path: str, content: bytes, content_type: str = "application/octet-stream") -> UploadResult: ...

    def upload_json(self, relative_path: str, payload: dict) -> UploadResult: ...

    def upload_file(self, relative_path: str, file_obj: BinaryIO, content_type: str = "application/octet-stream") -> UploadResult: ...
