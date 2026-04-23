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
        auth_mode: str = "client_credentials",
        drive_id: str = "",
        refresh_token: str = "",
        timeout: int = 60,
    ):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.auth_mode = auth_mode.strip().lower()
        self.root_folder = root_folder.strip("/")
        self.drive_id = drive_id
        self.refresh_token = refresh_token
        self.timeout = timeout
        self._delegated_access_token = ""

    def upload_json(self, relative_path: str, payload: dict) -> UploadResult:
        content = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        return self.upload_bytes(relative_path, content, "application/json")

    def upload_file(self, relative_path: str, file_obj: BinaryIO, content_type: str = "application/octet-stream") -> UploadResult:
        return self.upload_bytes(relative_path, file_obj.read(), content_type)

    def upload_bytes(self, relative_path: str, content: bytes, content_type: str = "application/octet-stream") -> UploadResult:
        self._validate_config()
        token = self._access_token()
        path = self._remote_path(relative_path)
        self._ensure_parent_folders(token, path)
        url = f"{self._drive_base()}/root:/{self._quote_path(path)}:/content"
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
        self._validate_config()
        token = self._access_token()
        path = self._remote_path(relative_path)
        self._ensure_parent_folders(token, path)
        url = f"{self._drive_base()}/root:/{self._quote_path(path)}:/createUploadSession"
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
        if self.auth_mode == "delegated_refresh" and not self.drive_id:
            return f"{GRAPH_ROOT}/me/drive"
        return f"{GRAPH_ROOT}/drives/{self.drive_id}"

    def _validate_config(self) -> None:
        if self.auth_mode == "client_credentials" and not self.drive_id:
            raise RuntimeError(
                "OneDrive is configured for client_credentials but ONEDRIVE_DRIVE_ID is missing. "
                "Set ONEDRIVE_DRIVE_ID, or set ONEDRIVE_AUTH_MODE=delegated_refresh with ONEDRIVE_REFRESH_TOKEN."
            )
        if self.auth_mode == "delegated_refresh" and not self.refresh_token:
            raise RuntimeError("ONEDRIVE_REFRESH_TOKEN is required when ONEDRIVE_AUTH_MODE=delegated_refresh.")

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
        url = f"{self._drive_base()}/root:/{self._quote_path(path)}:"
        response = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=self.timeout)
        if response.status_code == 404:
            return False
        response.raise_for_status()
        return True

    def _create_folder(self, token: str, parent_path: str, name: str) -> None:
        if parent_path:
            url = f"{self._drive_base()}/root:/{self._quote_path(parent_path)}:/children"
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

    @staticmethod
    def _quote_path(path: str) -> str:
        return "/".join(quote(part, safe="") for part in path.strip("/").split("/") if part)

    def _access_token(self) -> str:
        if self.auth_mode == "delegated_refresh":
            return self._delegated_token()
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

    def _delegated_token(self) -> str:
        token_tenant = self.tenant_id or "consumers"
        url = f"https://login.microsoftonline.com/{token_tenant}/oauth2/v2.0/token"
        response = requests.post(
            url,
            data={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
                "scope": "Files.ReadWrite offline_access",
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        token = response.json()
        self.refresh_token = token.get("refresh_token", self.refresh_token)
        self._delegated_access_token = token["access_token"]
        return self._delegated_access_token
