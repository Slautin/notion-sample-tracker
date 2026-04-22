from __future__ import annotations

import json
from dataclasses import dataclass
from typing import BinaryIO
from urllib.parse import quote

import requests


GRAPH_ROOT = "https://graph.microsoft.com/v1.0"


@dataclass(frozen=True)
class UploadResult:
    path: str
    web_url: str = ""


class OneDriveClient:
    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        root_folder: str,
        drive_id: str = "",
        timeout: int = 60,
    ):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.root_folder = root_folder.strip("/")
        self.drive_id = drive_id
        self.timeout = timeout

    def upload_json(self, relative_path: str, payload: dict) -> UploadResult:
        content = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        return self.upload_bytes(relative_path, content, "application/json")

    def upload_file(self, relative_path: str, file_obj: BinaryIO, content_type: str = "application/octet-stream") -> UploadResult:
        return self.upload_bytes(relative_path, file_obj.read(), content_type)

    def upload_bytes(self, relative_path: str, content: bytes, content_type: str = "application/octet-stream") -> UploadResult:
        token = self._access_token()
        path = self._remote_path(relative_path)
        url = f"{self._drive_base()}/root:/{quote(path)}:/content"
        response = requests.put(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": content_type,
            },
            data=content,
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        return UploadResult(path=path, web_url=data.get("webUrl", ""))

    def _remote_path(self, relative_path: str) -> str:
        return f"{self.root_folder}/{relative_path.strip('/')}"

    def _drive_base(self) -> str:
        if self.drive_id:
            return f"{GRAPH_ROOT}/drives/{self.drive_id}"
        return f"{GRAPH_ROOT}/me/drive"

    def _access_token(self) -> str:
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        response = requests.post(
            url,
            data={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": "https://graph.microsoft.com/.default",
                "grant_type": "client_credentials",
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()["access_token"]
