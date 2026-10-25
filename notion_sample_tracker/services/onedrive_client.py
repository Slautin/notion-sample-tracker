from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from typing import BinaryIO
from urllib.parse import quote, urlparse

import requests

from notion_sample_tracker.services.storage import UploadResult


GRAPH_ROOT = "https://graph.microsoft.com/v1.0"

# Authority aliases that do not name a single organization. "consumers" is the personal Microsoft account endpoint.
BLOCKED_TENANT_ALIASES = frozenset({"common", "consumers", "organizations"})
# Graph driveType values we accept. A "personal" drive (personal OneDrive) is never allowed.
ALLOWED_DRIVE_TYPES = frozenset({"business", "documentlibrary"})
PERSONAL_HOST_SUFFIXES = ("live.com", "1drv.ms")
TOKEN_REFRESH_MARGIN_SECONDS = 300


class OneDriveConfigError(RuntimeError):
    """The OneDrive configuration or target drive is not an allowed organizational drive."""


@dataclass(frozen=True)
class DriveInfo:
    drive_id: str
    drive_type: str
    web_url: str
    host: str


class OneDriveClient:
    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        root_folder: str,
        auth_mode: str = "client_credentials",
        public_client: bool = False,
        drive_id: str = "",
        refresh_token: str = "",
        timeout: int = 60,
        allowed_host_suffixes: tuple[str, ...] = (),
    ):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.auth_mode = auth_mode.strip().lower()
        self.public_client = public_client
        self.root_folder = root_folder.strip("/")
        self.drive_id = drive_id
        self.refresh_token = refresh_token
        self.timeout = timeout
        self.allowed_host_suffixes = tuple(allowed_host_suffixes)
        self._token = ""
        self._token_expires_at = 0.0
        self._drive_info: DriveInfo | None = None
        self._lock = threading.RLock()
        self._known_folders: set[str] = set()

    def upload_json(self, relative_path: str, payload: dict) -> UploadResult:
        content = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        return self.upload_bytes(relative_path, content, "application/json")

    def upload_file(self, relative_path: str, file_obj: BinaryIO, content_type: str = "application/octet-stream") -> UploadResult:
        return self.upload_bytes(relative_path, file_obj.read(), content_type)

    def upload_bytes(self, relative_path: str, content: bytes, content_type: str = "application/octet-stream") -> UploadResult:
        token = self._begin_request()
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
        token = self._begin_request()
        path = self._remote_path(relative_path)
        self._ensure_parent_folders(token, path)
        url = f"{self._drive_base()}/root:/{self._quote_path(path)}:/createUploadSession"
        response = requests.post(
            url,
            headers={"Authorization": f"Bearer {token}"},
            json={"item": {"@microsoft.graph.conflictBehavior": "rename"}},
            timeout=self.timeout,
        )
        self._raise_for_graph_error(response, "create upload session")
        data = response.json()
        return {
            "upload_url": data["uploadUrl"],
            "onedrive_path": path,
        }

    def verify_drive(self) -> DriveInfo:
        """Confirm the configured drive is an allowed organizational drive. Raises OneDriveConfigError if not."""
        self._validate_config()
        return self._ensure_drive_verified(self._access_token())

    def _begin_request(self) -> str:
        """Validate config, get a token, and make sure the target drive is allowed before any write."""
        self._validate_config()
        token = self._access_token()
        self._ensure_drive_verified(token)
        return token

    def _remote_path(self, relative_path: str) -> str:
        return f"{self.root_folder}/{relative_path.strip('/')}"

    def _drive_base(self) -> str:
        return f"{GRAPH_ROOT}/drives/{self.drive_id}"

    def _validate_config(self) -> None:
        tenant = self.tenant_id.strip().lower()
        if not tenant:
            raise OneDriveConfigError("ONEDRIVE_TENANT_ID is required.")
        if tenant in BLOCKED_TENANT_ALIASES:
            raise OneDriveConfigError(
                f"ONEDRIVE_TENANT_ID must be your organization's tenant ID, not '{tenant}'. "
                "The common, consumers and organizations endpoints can reach personal accounts."
            )
        if self.auth_mode not in {"client_credentials", "delegated_refresh"}:
            raise OneDriveConfigError("ONEDRIVE_AUTH_MODE must be client_credentials or delegated_refresh.")
        if not self.drive_id:
            raise OneDriveConfigError(
                "ONEDRIVE_DRIVE_ID is required. The app never falls back to /me/drive because that is a personal drive."
            )
        if self.auth_mode == "delegated_refresh" and not self.refresh_token:
            raise OneDriveConfigError("ONEDRIVE_REFRESH_TOKEN is required when ONEDRIVE_AUTH_MODE=delegated_refresh.")

    def _ensure_drive_verified(self, token: str) -> DriveInfo:
        with self._lock:
            if self._drive_info is None:
                self._drive_info = self._fetch_and_check_drive(token)
            return self._drive_info

    def _fetch_and_check_drive(self, token: str) -> DriveInfo:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(f"{self._drive_base()}?$select=id,driveType,webUrl", headers=headers, timeout=self.timeout)
        if response.status_code == 403:
            # Some app-only grants (e.g. Sites.Selected) let the app use a drive's content but not read the drive
            # object itself. Fall back to the root folder, which every upload needs anyway. It has no driveType, so
            # only the host can be checked; set ONEDRIVE_ALLOWED_HOST_SUFFIXES to make that strict.
            root = requests.get(f"{self._drive_base()}/root?$select=webUrl", headers=headers, timeout=self.timeout)
            self._raise_for_graph_error(root, "verify drive")
            return self._check_drive({"webUrl": root.json().get("webUrl")}, type_known=False)
        self._raise_for_graph_error(response, "verify drive")
        return self._check_drive(response.json())

    def _check_drive(self, data: dict, type_known: bool = True) -> DriveInfo:
        drive_type = str(data.get("driveType") or "") if type_known else "unknown"
        web_url = str(data.get("webUrl") or "")
        host = (urlparse(web_url).hostname or "").lower()
        if drive_type.lower() == "personal":
            raise OneDriveConfigError(
                "ONEDRIVE_DRIVE_ID points at a personal OneDrive. Refusing to use it; "
                "configure the organization's SharePoint document library or OneDrive for Business drive."
            )
        if type_known and drive_type.lower() not in ALLOWED_DRIVE_TYPES:
            raise OneDriveConfigError(
                f"ONEDRIVE_DRIVE_ID resolved to a '{drive_type or 'unknown'}' drive; "
                "only business or documentLibrary drives are allowed."
            )
        if not type_known and not host:
            raise OneDriveConfigError(
                "Could not read the drive's type or address, so it cannot be verified as an organizational drive."
            )
        if _host_matches(host, PERSONAL_HOST_SUFFIXES):
            raise OneDriveConfigError(f"Drive host '{host}' is a personal OneDrive host. Refusing to use it.")
        if self.allowed_host_suffixes and not _host_matches(host, self.allowed_host_suffixes):
            raise OneDriveConfigError(
                f"Drive host '{host or 'unknown'}' is not allowed by ONEDRIVE_ALLOWED_HOST_SUFFIXES "
                f"({', '.join(self.allowed_host_suffixes)})."
            )
        return DriveInfo(drive_id=self.drive_id, drive_type=drive_type, web_url=web_url, host=host)

    def _ensure_parent_folders(self, token: str, file_path: str) -> None:
        parts = [part for part in file_path.strip("/").split("/")[:-1] if part]
        current = ""
        for part in parts:
            parent_path = current
            current = f"{current}/{part}" if current else part
            if current in self._known_folders:
                continue
            if self._path_exists(token, current):
                self._known_folders.add(current)
                continue
            self._create_folder(token, parent_path, part)
            self._known_folders.add(current)

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
                "@microsoft.graph.conflictBehavior": "fail",
            },
            timeout=self.timeout,
        )
        if response.status_code == 409:
            if self._path_exists(token, f"{parent_path}/{name}" if parent_path else name):
                self._known_folders.add(f"{parent_path}/{name}" if parent_path else name)
                return
            self._raise_for_graph_error(response, "create folder")
            return
        self._raise_for_graph_error(response, "create folder")

    @staticmethod
    def _quote_path(path: str) -> str:
        return "/".join(quote(part, safe="") for part in path.strip("/").split("/") if part)

    def _now(self) -> float:
        return time.time()

    def _access_token(self) -> str:
        with self._lock:
            if self._token and self._now() < self._token_expires_at:
                return self._token
            payload = self._request_delegated_token() if self.auth_mode == "delegated_refresh" else self._request_app_token()
            expires_in = int(payload.get("expires_in") or 3600)
            self._token = payload["access_token"]
            self._token_expires_at = self._now() + max(expires_in - TOKEN_REFRESH_MARGIN_SECONDS, 60)
            return self._token

    def _request_app_token(self) -> dict:
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
        self._raise_for_graph_token_error(response)
        return response.json()

    def _request_delegated_token(self) -> dict:
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        data = {
            "client_id": self.client_id,
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "scope": "Files.ReadWrite offline_access",
        }
        if not self.public_client:
            data["client_secret"] = self.client_secret
        response = requests.post(
            url,
            data=data,
            timeout=self.timeout,
        )
        self._raise_for_graph_token_error(response)
        token = response.json()
        self.refresh_token = token.get("refresh_token", self.refresh_token)
        return token

    @staticmethod
    def _raise_for_graph_token_error(response: requests.Response) -> None:
        if response.ok:
            return
        try:
            payload = response.json()
        except ValueError:
            response.raise_for_status()
        error = payload.get("error", "token_error")
        description = payload.get("error_description", response.text)
        raise RuntimeError(f"Microsoft token request failed: {error}: {description}")

    @staticmethod
    def _raise_for_graph_error(response: requests.Response, action: str) -> None:
        if response.ok:
            return
        try:
            payload = response.json()
        except ValueError:
            response.raise_for_status()
        error = payload.get("error", {})
        if isinstance(error, dict):
            code = error.get("code", "graph_error")
            message = error.get("message", response.text)
        else:
            code = "graph_error"
            message = str(error) or response.text
        raise RuntimeError(f"Microsoft Graph {action} failed: {code}: {message}")


def _host_matches(host: str, suffixes: tuple[str, ...]) -> bool:
    """True if host equals a suffix or is a subdomain of it (so 'utk.example.com' does not match 'tk.example.com')."""
    host = host.lower()
    for suffix in suffixes:
        suffix = suffix.strip().lower().lstrip(".")
        if suffix and (host == suffix or host.endswith("." + suffix)):
            return True
    return False
