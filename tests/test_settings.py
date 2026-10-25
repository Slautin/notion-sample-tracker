import pytest

from notion_sample_tracker.settings import DEFAULT_DEV_SECRET, Settings


REQUIRED_ENV = {
    "NOTION_TOKEN": "secret_test",
    "NOTION_SAMPLES_DATABASE_ID": "samples",
    "NOTION_RESULTS_DATABASE_ID": "results",
    "NOTION_PEOPLE_DATABASE_ID": "people",
    "ONEDRIVE_TENANT_ID": "11111111-2222-3333-4444-555555555555",
    "ONEDRIVE_CLIENT_ID": "client",
    "ONEDRIVE_CLIENT_SECRET": "secret",
    "ONEDRIVE_DRIVE_ID": "drive",
}


def _set_required(monkeypatch):
    for key, value in REQUIRED_ENV.items():
        monkeypatch.setenv(key, value)
    for key in ("ONEDRIVE_AUTH_MODE", "ONEDRIVE_REFRESH_TOKEN", "ONEDRIVE_ALLOWED_HOST_SUFFIXES"):
        monkeypatch.delenv(key, raising=False)


def _set_production(monkeypatch):
    _set_required(monkeypatch)
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("APP_SECRET_KEY", "x" * 32)
    monkeypatch.setenv("PUBLIC_BASE_URL", "https://sample-tracker.example")


def test_development_allows_default_secret(monkeypatch):
    _set_required(monkeypatch)
    monkeypatch.delenv("APP_SECRET_KEY", raising=False)
    monkeypatch.setenv("APP_ENV", "development")

    settings = Settings.from_env()

    assert settings.app_secret_key == DEFAULT_DEV_SECRET
    assert not settings.is_production


def test_production_requires_non_default_secret(monkeypatch):
    _set_required(monkeypatch)
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("APP_SECRET_KEY", DEFAULT_DEV_SECRET)
    monkeypatch.setenv("PUBLIC_BASE_URL", "https://sample-tracker.example")

    with pytest.raises(RuntimeError, match="non-default"):
        Settings.from_env()


def test_production_rejects_localhost_public_url(monkeypatch):
    _set_required(monkeypatch)
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("APP_SECRET_KEY", "x" * 32)
    monkeypatch.setenv("PUBLIC_BASE_URL", "http://localhost:8000")

    with pytest.raises(RuntimeError, match="localhost"):
        Settings.from_env()


def test_production_relative_backlog_dir_uses_data_directory(monkeypatch):
    _set_required(monkeypatch)
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("APP_SECRET_KEY", "x" * 32)
    monkeypatch.setenv("PUBLIC_BASE_URL", "https://sample-tracker.example")
    monkeypatch.setenv("BACKLOG_DIR", "backlog")

    settings = Settings.from_env()

    assert str(settings.backlog_dir) == "/data/backlog"


def test_relative_backlog_dir_uses_existing_data_directory(monkeypatch):
    _set_required(monkeypatch)
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.setenv("BACKLOG_DIR", "backlog")
    monkeypatch.setattr("notion_sample_tracker.settings.Path.exists", lambda self: str(self) == "/data")

    settings = Settings.from_env()

    assert str(settings.backlog_dir) == "/data/backlog"


def test_upload_file_limit_must_fit_total_limit(monkeypatch):
    _set_required(monkeypatch)
    monkeypatch.setenv("MAX_UPLOAD_MB", "10")
    monkeypatch.setenv("MAX_UPLOAD_FILE_MB", "25")

    with pytest.raises(RuntimeError, match="MAX_UPLOAD_FILE_MB"):
        Settings.from_env()


def test_onedrive_timeout_is_configurable(monkeypatch):
    _set_required(monkeypatch)
    monkeypatch.setenv("ONEDRIVE_TIMEOUT_SECONDS", "7")

    settings = Settings.from_env()

    assert settings.onedrive_timeout_seconds == 7


@pytest.mark.parametrize("alias", ["common", "consumers", "organizations", " Consumers "])
def test_tenant_aliases_that_can_reach_personal_accounts_are_rejected(monkeypatch, alias):
    _set_required(monkeypatch)
    monkeypatch.setenv("ONEDRIVE_TENANT_ID", alias)

    with pytest.raises(RuntimeError, match="tenant ID"):
        Settings.from_env()


def test_production_requires_tenant_guid(monkeypatch):
    _set_production(monkeypatch)
    monkeypatch.setenv("ONEDRIVE_TENANT_ID", "utk-tenant")

    with pytest.raises(RuntimeError, match="tenant GUID"):
        Settings.from_env()


def test_production_accepts_tenant_guid_with_client_credentials(monkeypatch):
    _set_production(monkeypatch)

    settings = Settings.from_env()

    assert settings.onedrive_auth_mode == "client_credentials"
    assert settings.onedrive_tenant_id == REQUIRED_ENV["ONEDRIVE_TENANT_ID"]


def test_production_rejects_delegated_refresh(monkeypatch):
    _set_production(monkeypatch)
    monkeypatch.setenv("ONEDRIVE_AUTH_MODE", "delegated_refresh")
    monkeypatch.setenv("ONEDRIVE_REFRESH_TOKEN", "refresh")

    with pytest.raises(RuntimeError, match="not allowed in production"):
        Settings.from_env()


def test_development_still_allows_delegated_refresh_with_drive_id(monkeypatch):
    _set_required(monkeypatch)
    monkeypatch.setenv("ONEDRIVE_AUTH_MODE", "delegated_refresh")
    monkeypatch.setenv("ONEDRIVE_REFRESH_TOKEN", "refresh")

    settings = Settings.from_env()

    assert settings.onedrive_auth_mode == "delegated_refresh"
    assert settings.onedrive_drive_id == "drive"


@pytest.mark.parametrize("auth_mode", ["client_credentials", "delegated_refresh"])
def test_drive_id_is_required_in_every_auth_mode(monkeypatch, auth_mode):
    _set_required(monkeypatch)
    monkeypatch.delenv("ONEDRIVE_DRIVE_ID")
    monkeypatch.setenv("ONEDRIVE_AUTH_MODE", auth_mode)
    monkeypatch.setenv("ONEDRIVE_REFRESH_TOKEN", "refresh")

    with pytest.raises(RuntimeError, match="ONEDRIVE_DRIVE_ID"):
        Settings.from_env()


def test_allowed_host_suffixes_are_parsed(monkeypatch):
    _set_required(monkeypatch)
    monkeypatch.setenv("ONEDRIVE_ALLOWED_HOST_SUFFIXES", " Utk.sharepoint.com, utk-my.sharepoint.com ,,")

    settings = Settings.from_env()

    assert settings.onedrive_allowed_host_suffixes == ("utk.sharepoint.com", "utk-my.sharepoint.com")


def test_allowed_host_suffixes_default_to_unset(monkeypatch):
    _set_required(monkeypatch)

    assert Settings.from_env().onedrive_allowed_host_suffixes == ()
