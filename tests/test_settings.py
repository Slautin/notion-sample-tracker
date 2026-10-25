from pathlib import Path

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
    for key in (
        "ONEDRIVE_AUTH_MODE",
        "ONEDRIVE_REFRESH_TOKEN",
        "ONEDRIVE_ALLOWED_HOST_SUFFIXES",
        "STORAGE_BACKEND",
        "STORAGE_ROOT",
        "STORAGE_MIN_FREE_MB",
        "MAX_RESULT_FILE_MB",
        "UPLOAD_CHUNK_MB",
        "UPLOAD_TTL_HOURS",
        "ACCESS_PASSWORD",
        "ACCESS_USERNAME",
        "ACCESS_BEHIND_AUTH_PROXY",
    ):
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


# --- storage backend and access control ---


def _set_local(monkeypatch, production=False):
    (_set_production if production else _set_required)(monkeypatch)
    for key in ("ONEDRIVE_TENANT_ID", "ONEDRIVE_CLIENT_ID", "ONEDRIVE_CLIENT_SECRET", "ONEDRIVE_DRIVE_ID"):
        monkeypatch.delenv(key)
    monkeypatch.setenv("STORAGE_BACKEND", "local")


def test_onedrive_stays_the_default_backend_and_still_needs_its_settings(monkeypatch):
    _set_required(monkeypatch)

    assert Settings.from_env().storage_backend == "onedrive"
    monkeypatch.delenv("ONEDRIVE_TENANT_ID")
    with pytest.raises(RuntimeError, match="ONEDRIVE_TENANT_ID"):
        Settings.from_env()


def test_local_storage_needs_no_onedrive_settings(monkeypatch):
    _set_local(monkeypatch)

    settings = Settings.from_env()

    assert settings.storage_backend == "local"
    assert settings.storage_label == "server storage"
    assert settings.storage_root == Path("./files")
    assert (settings.upload_chunk_mb, settings.max_result_file_mb, settings.upload_ttl_hours) == (8, 2048, 24)
    assert settings.access_password == "" and settings.access_behind_auth_proxy is False


def test_the_backend_name_is_case_insensitive_and_unknown_names_are_rejected(monkeypatch):
    _set_local(monkeypatch)
    monkeypatch.setenv("STORAGE_BACKEND", "LOCAL")
    assert Settings.from_env().storage_backend == "local"

    monkeypatch.setenv("STORAGE_BACKEND", "s3")
    with pytest.raises(RuntimeError, match="onedrive or local"):
        Settings.from_env()


def test_production_local_storage_defaults_to_the_data_volume(monkeypatch):
    _set_local(monkeypatch, production=True)
    monkeypatch.setenv("ACCESS_BEHIND_AUTH_PROXY", "true")

    assert Settings.from_env().storage_root == Path("/data/files")


def test_production_local_storage_refuses_to_start_without_access_control(monkeypatch):
    _set_local(monkeypatch, production=True)

    with pytest.raises(RuntimeError, match="access control"):
        Settings.from_env()


def test_production_local_storage_accepts_a_password_or_a_declared_proxy(monkeypatch):
    _set_local(monkeypatch, production=True)
    monkeypatch.setenv("ACCESS_PASSWORD", "a-long-shared-password")
    assert Settings.from_env().access_password == "a-long-shared-password"

    monkeypatch.delenv("ACCESS_PASSWORD")
    monkeypatch.setenv("ACCESS_BEHIND_AUTH_PROXY", "true")
    assert Settings.from_env().access_behind_auth_proxy is True


def test_local_storage_in_development_needs_no_access_control(monkeypatch):
    _set_local(monkeypatch)

    Settings.from_env()


def test_production_passwords_must_be_long_and_sent_over_https(monkeypatch):
    _set_local(monkeypatch, production=True)
    monkeypatch.setenv("ACCESS_PASSWORD", "too-short")
    with pytest.raises(RuntimeError, match="12 characters"):
        Settings.from_env()

    monkeypatch.setenv("ACCESS_PASSWORD", "a-long-shared-password")
    monkeypatch.setenv("PUBLIC_BASE_URL", "http://sample-tracker.example")
    with pytest.raises(RuntimeError, match="https"):
        Settings.from_env()


def test_the_password_rules_apply_to_the_onedrive_backend_too(monkeypatch):
    _set_production(monkeypatch)
    monkeypatch.setenv("ACCESS_PASSWORD", "too-short")

    with pytest.raises(RuntimeError, match="12 characters"):
        Settings.from_env()


def test_the_access_username_cannot_contain_a_colon(monkeypatch):
    _set_local(monkeypatch)
    monkeypatch.setenv("ACCESS_PASSWORD", "a-long-shared-password")
    monkeypatch.setenv("ACCESS_USERNAME", "a:b")

    with pytest.raises(RuntimeError, match="ACCESS_USERNAME"):
        Settings.from_env()


def test_production_storage_root_must_be_absolute(monkeypatch):
    _set_local(monkeypatch, production=True)
    monkeypatch.setenv("ACCESS_BEHIND_AUTH_PROXY", "true")
    monkeypatch.setenv("STORAGE_ROOT", "files")

    with pytest.raises(RuntimeError, match="absolute"):
        Settings.from_env()


@pytest.mark.parametrize(
    "name, value, message",
    [
        ("MAX_RESULT_FILE_MB", "0", "MAX_RESULT_FILE_MB"),
        ("UPLOAD_CHUNK_MB", "0", "UPLOAD_CHUNK_MB"),
        ("UPLOAD_CHUNK_MB", "300", "UPLOAD_CHUNK_MB must be less than or equal to MAX_UPLOAD_MB"),
        ("UPLOAD_TTL_HOURS", "0", "UPLOAD_TTL_HOURS"),
        ("STORAGE_MIN_FREE_MB", "-1", "STORAGE_MIN_FREE_MB"),
        ("UPLOAD_CHUNK_MB", "eight", "UPLOAD_CHUNK_MB must be an integer"),
    ],
)
def test_local_storage_limits_are_validated(monkeypatch, name, value, message):
    _set_local(monkeypatch)
    monkeypatch.setenv(name, value)

    with pytest.raises(RuntimeError, match=message):
        Settings.from_env()
