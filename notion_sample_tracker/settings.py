from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


load_dotenv()


DEFAULT_DEV_SECRET = "dev-only-change-me"
LOCAL_ENVS = {"development", "dev", "local", "test", "testing"}
PRODUCTION_ENVS = {"production", "prod"}
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


def _backlog_dir(app_env: str) -> Path:
    raw = _optional("BACKLOG_DIR", "/data/backlog" if app_env in PRODUCTION_ENVS else "./backlog")
    path = Path(raw)
    if app_env in PRODUCTION_ENVS and not path.is_absolute():
        return Path("/data") / path
    return path


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
    enable_backlog_view: bool = False
    backlog_read_limit: int = 50
    max_upload_files: int = 12
    max_upload_file_mb: int = 25
    allowed_upload_extensions: tuple[str, ...] = DEFAULT_UPLOAD_EXTENSIONS

    @classmethod
    def from_env(cls) -> "Settings":
        app_env = _optional("APP_ENV", _optional("FLASK_ENV", "development")).lower()
        app_secret_key = _optional("APP_SECRET_KEY")
        if not app_secret_key and app_env in LOCAL_ENVS:
            app_secret_key = DEFAULT_DEV_SECRET
        settings = cls(
            app_secret_key=app_secret_key,
            public_base_url=_optional("PUBLIC_BASE_URL", "http://localhost:8000").rstrip("/"),
            notion_home_url=_optional("NOTION_HOME_URL", "") or _optional("NOTION_PAGE", ""),
            notion_token=_required("NOTION_TOKEN"),
            notion_samples_database_id=_required("NOTION_SAMPLES_DATABASE_ID"),
            notion_results_database_id=_required("NOTION_RESULTS_DATABASE_ID"),
            notion_people_database_id=_required("NOTION_PEOPLE_DATABASE_ID"),
            onedrive_tenant_id=_required("ONEDRIVE_TENANT_ID"),
            onedrive_client_id=_required("ONEDRIVE_CLIENT_ID"),
            onedrive_client_secret=_required("ONEDRIVE_CLIENT_SECRET"),
            onedrive_auth_mode=_optional("ONEDRIVE_AUTH_MODE", "client_credentials"),
            onedrive_public_client=_optional("ONEDRIVE_PUBLIC_CLIENT", "false").lower() in {"1", "true", "yes"},
            onedrive_drive_id=_optional("ONEDRIVE_DRIVE_ID"),
            onedrive_refresh_token=_optional("ONEDRIVE_REFRESH_TOKEN"),
            onedrive_root_folder=_optional("ONEDRIVE_ROOT_FOLDER", "SampleTracker"),
            backlog_dir=_backlog_dir(app_env),
            max_upload_mb=_int("MAX_UPLOAD_MB", 200),
            app_env=app_env,
            enable_backlog_view=_bool("ENABLE_BACKLOG_VIEW", False),
            backlog_read_limit=_int("BACKLOG_READ_LIMIT", 50),
            max_upload_files=_int("MAX_UPLOAD_FILES", 12),
            max_upload_file_mb=_int("MAX_UPLOAD_FILE_MB", 25),
            allowed_upload_extensions=_extensions("ALLOWED_UPLOAD_EXTENSIONS", DEFAULT_UPLOAD_EXTENSIONS),
        )
        settings.validate()
        return settings

    @property
    def is_production(self) -> bool:
        return self.app_env in PRODUCTION_ENVS

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
        if self.onedrive_auth_mode not in {"client_credentials", "delegated_refresh"}:
            raise RuntimeError("ONEDRIVE_AUTH_MODE must be client_credentials or delegated_refresh.")
        if self.onedrive_auth_mode == "client_credentials" and not self.onedrive_drive_id:
            raise RuntimeError("ONEDRIVE_DRIVE_ID is required when ONEDRIVE_AUTH_MODE=client_credentials.")
        if self.onedrive_auth_mode == "delegated_refresh" and not self.onedrive_refresh_token:
            raise RuntimeError("ONEDRIVE_REFRESH_TOKEN is required when ONEDRIVE_AUTH_MODE=delegated_refresh.")
