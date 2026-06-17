import pytest

from notion_sample_tracker.settings import DEFAULT_DEV_SECRET, Settings


REQUIRED_ENV = {
    "NOTION_TOKEN": "secret_test",
    "NOTION_SAMPLES_DATABASE_ID": "samples",
    "NOTION_RESULTS_DATABASE_ID": "results",
    "NOTION_PEOPLE_DATABASE_ID": "people",
    "ONEDRIVE_TENANT_ID": "tenant",
    "ONEDRIVE_CLIENT_ID": "client",
    "ONEDRIVE_CLIENT_SECRET": "secret",
    "ONEDRIVE_DRIVE_ID": "drive",
}


def _set_required(monkeypatch):
    for key, value in REQUIRED_ENV.items():
        monkeypatch.setenv(key, value)


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


def test_upload_file_limit_must_fit_total_limit(monkeypatch):
    _set_required(monkeypatch)
    monkeypatch.setenv("MAX_UPLOAD_MB", "10")
    monkeypatch.setenv("MAX_UPLOAD_FILE_MB", "25")

    with pytest.raises(RuntimeError, match="MAX_UPLOAD_FILE_MB"):
        Settings.from_env()
