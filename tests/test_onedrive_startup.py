from pathlib import Path

import pytest

import notion_sample_tracker.app as app_module
from notion_sample_tracker.services.onedrive_client import DriveInfo, OneDriveConfigError
from notion_sample_tracker.settings import Settings


TENANT = "11111111-2222-3333-4444-555555555555"
VERIFIED = DriveInfo(drive_id="drive", drive_type="documentLibrary", web_url="https://utk.sharepoint.com/sites/lab", host="utk.sharepoint.com")


class StubClient:
    def __init__(self, outcome):
        self.outcome = outcome

    def verify_drive(self):
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


def test_startup_check_stops_the_app_on_a_disallowed_drive():
    with pytest.raises(OneDriveConfigError, match="personal"):
        app_module._verify_onedrive_at_startup(StubClient(OneDriveConfigError("personal OneDrive")))


def test_startup_check_tolerates_a_temporary_graph_outage(capsys):
    app_module._verify_onedrive_at_startup(StubClient(RuntimeError("timed out")))

    assert "onedrive_startup_check_deferred" in capsys.readouterr().out


def test_startup_check_logs_the_verified_drive(capsys):
    app_module._verify_onedrive_at_startup(StubClient(VERIFIED))

    output = capsys.readouterr().out
    assert "onedrive_drive_verified" in output
    assert "utk.sharepoint.com" in output


def _settings(tmp_path, **overrides) -> Settings:
    values = dict(
        app_secret_key="x" * 32,
        public_base_url="https://sample-tracker.example",
        notion_home_url="",
        notion_token="secret_test",
        notion_samples_database_id="samples-db",
        notion_results_database_id="results-db",
        notion_people_database_id="people-db",
        onedrive_tenant_id=TENANT,
        onedrive_client_id="client-id",
        onedrive_client_secret="client-secret",
        onedrive_auth_mode="client_credentials",
        onedrive_public_client=False,
        onedrive_drive_id="drive-id",
        onedrive_refresh_token="",
        onedrive_root_folder="SampleTracker",
        backlog_dir=Path(tmp_path),
        max_upload_mb=25,
        app_env="production",
    )
    values.update(overrides)
    return Settings(**values)


@pytest.fixture
def onedrive_stub(monkeypatch):
    created = []

    class Stub:
        outcome = VERIFIED

        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.verified = False
            created.append(self)

        def verify_drive(self):
            self.verified = True
            if isinstance(self.outcome, Exception):
                raise self.outcome
            return self.outcome

    monkeypatch.setattr(app_module, "OneDriveClient", Stub)
    return Stub, created


def test_create_app_verifies_the_drive_in_production_and_passes_host_pinning(tmp_path, onedrive_stub):
    _, created = onedrive_stub
    settings = _settings(tmp_path, onedrive_allowed_host_suffixes=("utk.sharepoint.com",))

    app_module.create_app(settings_factory=lambda: settings)

    assert created[0].verified
    assert created[0].kwargs["allowed_host_suffixes"] == ("utk.sharepoint.com",)
    assert created[0].kwargs["tenant_id"] == TENANT


def test_create_app_does_not_call_graph_at_startup_in_development(tmp_path, onedrive_stub):
    _, created = onedrive_stub
    settings = _settings(tmp_path, app_env="development", app_secret_key="dev-secret")

    app_module.create_app(settings_factory=lambda: settings)

    assert not created[0].verified


def test_create_app_refuses_to_start_when_production_drive_is_personal(tmp_path, onedrive_stub):
    stub, _ = onedrive_stub
    stub.outcome = OneDriveConfigError("ONEDRIVE_DRIVE_ID points at a personal OneDrive.")

    with pytest.raises(OneDriveConfigError, match="personal OneDrive"):
        app_module.create_app(settings_factory=lambda: _settings(tmp_path))


def test_create_app_rejects_delegated_refresh_in_production_even_without_from_env(tmp_path, onedrive_stub):
    _, created = onedrive_stub
    settings = _settings(tmp_path, onedrive_auth_mode="delegated_refresh", onedrive_refresh_token="refresh")

    with pytest.raises(RuntimeError, match="not allowed in production"):
        app_module.create_app(settings_factory=lambda: settings)

    assert created == []
