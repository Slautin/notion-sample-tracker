from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv

from notion_sample_tracker.services.onedrive_client import BLOCKED_TENANT_ALIASES


load_dotenv()


DEFAULT_DEV_SECRET = "dev-only-change-me"
LOCAL_ENVS = {"development", "dev", "local", "test", "testing"}
PRODUCTION_ENVS = {"production", "prod"}
STORAGE_BACKENDS = {"onedrive", "local"}
TENANT_GUID = re.compile(r"^[0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$")
DEFAULT_UPLOAD_EXTENSIONS = (
    ".csv",
    ".doc",
    ".docx",
    ".gif",
    ".jpeg",
    ".jpg",
    ".json",
    ".pdf",
    ".png",
    ".ppt",
    ".pptx",
    ".tif",
    ".tiff",
    ".txt",
    ".webp",
    ".xls",
    ".xlsx",
    ".zip",
)


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _optional(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _bool(name: str, default: bool = False) -> bool:
    raw = _optional(name, "true" if default else "false").lower()
    return raw in {"1", "true", "yes", "on"}


def _int(name: str, default: int) -> int:
    raw = _optional(name, str(default))
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer.") from exc


def _extensions(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    raw = _optional(name, ",".join(default))
    values = []
    for item in raw.split(","):
        ext = item.strip().lower()
        if not ext:
            continue
        values.append(ext if ext.startswith(".") else f".{ext}")
    return tuple(dict.fromkeys(values))


def _csv(name: str) -> tuple[str, ...]:
    values = [item.strip().lower() for item in _optional(name).split(",") if item.strip()]
    return tuple(dict.fromkeys(values))


def _backlog_dir(app_env: str) -> Path:
    raw = _optional("BACKLOG_DIR", "/data/backlog" if app_env in PRODUCTION_ENVS else "./backlog")
    path = Path(raw)
    if not path.is_absolute() and Path("/data").exists():
        return Path("/data") / path
    if not path.is_absolute() and app_env in PRODUCTION_ENVS:
        return Path("/data") / path
    return path


NOTION_HOST_SUFFIXES = ("notion.so", "notion.site", "notion.com")


def _public_address_problem(url: str) -> str:
    """Why `url` cannot be the address people use to reach this app ("" if it can)."""
    parts = urlparse(url)
    host = (parts.hostname or "").lower()
    if not host:
        return "it has no host name"
    if any(host == suffix or host.endswith("." + suffix) for suffix in NOTION_HOST_SUFFIXES):
        return f"it is a Notion address ({host}), so file links saved in Notion would open Notion pages"
    if parts.username or parts.password:
        return "it must not contain a user name or password"
    if parts.path not in ("", "/") or parts.query or parts.fragment:
        return "it must be only the address of the app, with no path, ?query or #fragment"
    return ""


def _storage_root(app_env: str) -> Path:
    return Path(_optional("STORAGE_ROOT", "/data/files" if app_env in PRODUCTION_ENVS else "./files"))


@dataclass(frozen=True)
class Settings:
    app_secret_key: str
    public_base_url: str
    notion_home_url: str
    notion_token: str
    notion_samples_database_id: str
    notion_results_database_id: str
    notion_people_database_id: str
    onedrive_tenant_id: str
    onedrive_client_id: str
    onedrive_client_secret: str
    onedrive_auth_mode: str
    onedrive_public_client: bool
    onedrive_drive_id: str
    onedrive_refresh_token: str
    onedrive_root_folder: str
    backlog_dir: Path
    max_upload_mb: int
    app_env: str = "development"
    onedrive_timeout_seconds: int = 20
    enable_backlog_view: bool = False
    backlog_read_limit: int = 50
    max_upload_files: int = 12
    max_upload_file_mb: int = 25
    allowed_upload_extensions: tuple[str, ...] = DEFAULT_UPLOAD_EXTENSIONS
    onedrive_allowed_host_suffixes: tuple[str, ...] = ()
    storage_backend: str = "onedrive"
    storage_root: Path = Path("./files")
    storage_min_free_mb: int = 1024
    max_result_file_mb: int = 2048
    upload_chunk_mb: int = 8
    upload_ttl_hours: int = 24
    access_password: str = ""
    access_username: str = "lab"
    access_behind_auth_proxy: bool = False

    @classmethod
    def from_env(cls) -> "Settings":
        app_env = _optional("APP_ENV", _optional("FLASK_ENV", "development")).lower()
        app_secret_key = _optional("APP_SECRET_KEY")
        if not app_secret_key and app_env in LOCAL_ENVS:
            app_secret_key = DEFAULT_DEV_SECRET
        storage_backend = _optional("STORAGE_BACKEND", "onedrive").lower()
        if storage_backend not in STORAGE_BACKENDS:
            # Checked before anything else so a typo is reported as a typo, not as missing OneDrive settings.
            raise RuntimeError("STORAGE_BACKEND must be onedrive or local.")

        def onedrive_setting(name: str) -> str:
            # The OneDrive variables are only mandatory when OneDrive is the storage backend.
            return _optional(name) if storage_backend == "local" else _required(name)

        settings = cls(
            app_secret_key=app_secret_key,
            public_base_url=_optional("PUBLIC_BASE_URL", "http://localhost:8000").rstrip("/"),
            notion_home_url=_optional("NOTION_HOME_URL", "") or _optional("NOTION_PAGE", ""),
            notion_token=_required("NOTION_TOKEN"),
            notion_samples_database_id=_required("NOTION_SAMPLES_DATABASE_ID"),
            notion_results_database_id=_required("NOTION_RESULTS_DATABASE_ID"),
            notion_people_database_id=_required("NOTION_PEOPLE_DATABASE_ID"),
            onedrive_tenant_id=onedrive_setting("ONEDRIVE_TENANT_ID"),
            onedrive_client_id=onedrive_setting("ONEDRIVE_CLIENT_ID"),
            onedrive_client_secret=onedrive_setting("ONEDRIVE_CLIENT_SECRET"),
            onedrive_auth_mode=_optional("ONEDRIVE_AUTH_MODE", "client_credentials"),
            onedrive_public_client=_optional("ONEDRIVE_PUBLIC_CLIENT", "false").lower() in {"1", "true", "yes"},
            onedrive_drive_id=_optional("ONEDRIVE_DRIVE_ID"),
            onedrive_refresh_token=_optional("ONEDRIVE_REFRESH_TOKEN"),
            onedrive_root_folder=_optional("ONEDRIVE_ROOT_FOLDER", "SampleTracker"),
            onedrive_timeout_seconds=_int("ONEDRIVE_TIMEOUT_SECONDS", 20),
            onedrive_allowed_host_suffixes=_csv("ONEDRIVE_ALLOWED_HOST_SUFFIXES"),
            backlog_dir=_backlog_dir(app_env),
            max_upload_mb=_int("MAX_UPLOAD_MB", 200),
            app_env=app_env,
            enable_backlog_view=_bool("ENABLE_BACKLOG_VIEW", False),
            backlog_read_limit=_int("BACKLOG_READ_LIMIT", 50),
            max_upload_files=_int("MAX_UPLOAD_FILES", 12),
            max_upload_file_mb=_int("MAX_UPLOAD_FILE_MB", 25),
            allowed_upload_extensions=_extensions("ALLOWED_UPLOAD_EXTENSIONS", DEFAULT_UPLOAD_EXTENSIONS),
            storage_backend=storage_backend,
            storage_root=_storage_root(app_env),
            storage_min_free_mb=_int("STORAGE_MIN_FREE_MB", 1024),
            max_result_file_mb=_int("MAX_RESULT_FILE_MB", 2048),
            upload_chunk_mb=_int("UPLOAD_CHUNK_MB", 8),
            upload_ttl_hours=_int("UPLOAD_TTL_HOURS", 24),
            access_password=_optional("ACCESS_PASSWORD"),
            access_username=_optional("ACCESS_USERNAME", "lab"),
            access_behind_auth_proxy=_bool("ACCESS_BEHIND_AUTH_PROXY", False),
        )
        settings.validate()
        return settings

    @property
    def is_production(self) -> bool:
        return self.app_env in PRODUCTION_ENVS

    @property
    def storage_label(self) -> str:
        return "server storage" if self.storage_backend == "local" else "OneDrive"

    def validate(self) -> None:
        if not self.app_secret_key:
            raise RuntimeError("APP_SECRET_KEY is required outside local development.")
        if self.is_production and self.app_secret_key == DEFAULT_DEV_SECRET:
            raise RuntimeError("APP_SECRET_KEY must be set to a non-default value in production.")
        if self.is_production and len(self.app_secret_key) < 32:
            raise RuntimeError("APP_SECRET_KEY must be at least 32 characters in production.")
        if not self.public_base_url.startswith(("http://", "https://")):
            raise RuntimeError("PUBLIC_BASE_URL must start with http:// or https://.")
        if self.is_production and "localhost" in self.public_base_url:
            raise RuntimeError("PUBLIC_BASE_URL must not point to localhost in production.")
        if self.max_upload_mb <= 0:
            raise RuntimeError("MAX_UPLOAD_MB must be greater than zero.")
        if self.max_upload_file_mb <= 0:
            raise RuntimeError("MAX_UPLOAD_FILE_MB must be greater than zero.")
        if self.max_upload_file_mb > self.max_upload_mb:
            raise RuntimeError("MAX_UPLOAD_FILE_MB must be less than or equal to MAX_UPLOAD_MB.")
        if self.max_upload_files <= 0:
            raise RuntimeError("MAX_UPLOAD_FILES must be greater than zero.")
        if self.backlog_read_limit <= 0:
            raise RuntimeError("BACKLOG_READ_LIMIT must be greater than zero.")
        if not self.allowed_upload_extensions:
            raise RuntimeError("ALLOWED_UPLOAD_EXTENSIONS must contain at least one extension.")
        if self.storage_backend not in STORAGE_BACKENDS:
            raise RuntimeError("STORAGE_BACKEND must be onedrive or local.")
        self._validate_access()
        if self.storage_backend == "local":
            self._validate_local_storage()
        else:
            self._validate_onedrive()

    def _validate_access(self) -> None:
        if not self.access_password:
            return
        if not self.access_username or ":" in self.access_username:
            raise RuntimeError("ACCESS_USERNAME must not be empty or contain a colon.")
        if self.is_production and len(self.access_password) < 12:
            raise RuntimeError("ACCESS_PASSWORD must be at least 12 characters in production.")
        if self.is_production and not self.public_base_url.startswith("https://"):
            raise RuntimeError(
                "PUBLIC_BASE_URL must use https when ACCESS_PASSWORD is set in production "
                "(HTTP Basic sends the password with every request)."
            )

    def _validate_local_storage(self) -> None:
        # Links to stored files are PUBLIC_BASE_URL + /files/..., so it must be the app's own address.
        problem = _public_address_problem(self.public_base_url)
        if problem:
            raise RuntimeError(
                f"PUBLIC_BASE_URL must be the address of this app, for example https://your-app.up.railway.app: {problem}."
            )
        if self.max_result_file_mb <= 0:
            raise RuntimeError("MAX_RESULT_FILE_MB must be greater than zero.")
        if self.upload_chunk_mb <= 0:
            raise RuntimeError("UPLOAD_CHUNK_MB must be greater than zero.")
        if self.upload_chunk_mb > self.max_upload_mb:
            raise RuntimeError("UPLOAD_CHUNK_MB must be less than or equal to MAX_UPLOAD_MB.")
        if self.storage_min_free_mb < 0:
            raise RuntimeError("STORAGE_MIN_FREE_MB must not be negative.")
        if self.upload_ttl_hours <= 0:
            raise RuntimeError("UPLOAD_TTL_HOURS must be greater than zero.")
        if self.is_production and not self.storage_root.is_absolute():
            raise RuntimeError("STORAGE_ROOT must be an absolute path in production, outside the application directory.")
        if self.is_production and not (self.access_password or self.access_behind_auth_proxy):
            raise RuntimeError(
                "This app serves the stored files itself, so production needs access control: set ACCESS_PASSWORD, "
                "or ACCESS_BEHIND_AUTH_PROXY=true if a login in front of the app protects every path including /files/."
            )

    def _validate_onedrive(self) -> None:
        if self.onedrive_auth_mode not in {"client_credentials", "delegated_refresh"}:
            raise RuntimeError("ONEDRIVE_AUTH_MODE must be client_credentials or delegated_refresh.")
        if self.onedrive_tenant_id.strip().lower() in BLOCKED_TENANT_ALIASES:
            raise RuntimeError(
                "ONEDRIVE_TENANT_ID must be your organization's tenant ID; common, consumers and organizations are not allowed."
            )
        if self.is_production and not TENANT_GUID.match(self.onedrive_tenant_id.strip()):
            raise RuntimeError("ONEDRIVE_TENANT_ID must be the organization's tenant GUID in production.")
        if self.is_production and self.onedrive_auth_mode == "delegated_refresh":
            raise RuntimeError(
                "ONEDRIVE_AUTH_MODE=delegated_refresh is not allowed in production; "
                "use client_credentials with the organization's SharePoint drive."
            )
        if not self.onedrive_drive_id:
            raise RuntimeError("ONEDRIVE_DRIVE_ID is required. The app never falls back to /me/drive (a personal drive).")
        if self.onedrive_auth_mode == "delegated_refresh" and not self.onedrive_refresh_token:
            raise RuntimeError("ONEDRIVE_REFRESH_TOKEN is required when ONEDRIVE_AUTH_MODE=delegated_refresh.")
        if self.onedrive_timeout_seconds <= 0:
            raise RuntimeError("ONEDRIVE_TIMEOUT_SECONDS must be greater than zero.")
