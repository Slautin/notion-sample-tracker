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
        if not self.drive_id:
            raise RuntimeError(
                "ONEDRIVE_DRIVE_ID is required for server-side Microsoft Graph uploads with client-credentials auth."
            )

    def upload_json(self, relative_path: str, payload: dict) -> UploadResult:
        content = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        return self.upload_bytes(relative_path, content, "application/json")

    def upload_file(self, relative_path: str, file_obj: BinaryIO, content_type: str = "application/octet-stream") -> UploadResult:
        return self.upload_bytes(relative_path, file_obj.read(), content_type)

    def upload_bytes(self, relative_path: str, content: bytes, content_type: str = "application/octet-stream") -> UploadResult:
        token = self._access_token()
        path = self._remote_path(relative_path)
        self._ensure_parent_folders(token, path)
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

    def create_upload_session(self, relative_path: str) -> dict:
        token = self._access_token()
        path = self._remote_path(relative_path)
        self._ensure_parent_folders(token, path)
        url = f"{self._drive_base()}/root:/{quote(path)}:/createUploadSession"
        response = requests.post(
            url,
            headers={"Authorization": f"Bearer {token}"},
            json={"item": {"@microsoft.graph.conflictBehavior": "replace"}},
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        return {
            "upload_url": data["uploadUrl"],
            "onedrive_path": path,
        }

    def _remote_path(self, relative_path: str) -> str:
        return f"{self.root_folder}/{relative_path.strip('/')}"

    def _drive_base(self) -> str:
        return f"{GRAPH_ROOT}/drives/{self.drive_id}"

    def _ensure_parent_folders(self, token: str, file_path: str) -> None:
        parts = [part for part in file_path.strip("/").split("/")[:-1] if part]
        current = ""
        for part in parts:
            parent_path = current
            current = f"{current}/{part}" if current else part
            if self._path_exists(token, current):
                continue
            self._create_folder(token, parent_path, part)

    def _path_exists(self, token: str, path: str) -> bool:
        url = f"{self._drive_base()}/root:/{quote(path)}"
        response = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=self.timeout)
        if response.status_code == 404:
            return False
        response.raise_for_status()
        return True

    def _create_folder(self, token: str, parent_path: str, name: str) -> None:
        if parent_path:
            url = f"{self._drive_base()}/root:/{quote(parent_path)}:/children"
        else:
            url = f"{self._drive_base()}/root/children"
        response = requests.post(
            url,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={
                "name": name,
                "folder": {},
                "@microsoft.graph.conflictBehavior": "replace",
            },
            timeout=self.timeout,
        )
        if response.status_code == 409:
            return
        response.raise_for_status()

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
