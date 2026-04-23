from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


load_dotenv()


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _optional(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


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
    onedrive_drive_id: str
    onedrive_refresh_token: str
    onedrive_root_folder: str
    backlog_dir: Path
    max_upload_mb: int

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            app_secret_key=_optional("APP_SECRET_KEY", "dev-only-change-me"),
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
            onedrive_drive_id=_optional("ONEDRIVE_DRIVE_ID"),
            onedrive_refresh_token=_optional("ONEDRIVE_REFRESH_TOKEN"),
            onedrive_root_folder=_optional("ONEDRIVE_ROOT_FOLDER", "SampleTracker"),
            backlog_dir=Path(_optional("BACKLOG_DIR", "./backlog")),
            max_upload_mb=int(_optional("MAX_UPLOAD_MB", "200")),
        )
