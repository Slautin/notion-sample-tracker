import pytest
from graph_fakes import FakeRequests

import notion_sample_tracker.onedrive_check as check
from notion_sample_tracker.services import onedrive_client
from notion_sample_tracker.services.onedrive_client import OneDriveClient


TENANT = "11111111-2222-3333-4444-555555555555"
PERSONAL = {"driveType": "personal", "webUrl": "https://onedrive.live.com/?cid=abc"}


def _client(monkeypatch, fake):
    monkeypatch.setattr(onedrive_client, "requests", fake)
    monkeypatch.setattr(check, "requests", fake)
    return OneDriveClient(
        tenant_id=TENANT,
        client_id="client",
        client_secret="secret",
        root_folder="SampleTracker",
        drive_id="drive",
    )


def _run(monkeypatch, fake, **kwargs):
    lines = []
    ok = check.run_checks(_client(monkeypatch, fake), emit=lines.append, **kwargs)
    return ok, lines


def test_all_checks_pass_and_exercise_both_upload_paths(monkeypatch):
    fake = FakeRequests()

    ok, lines = _run(monkeypatch, fake)

    assert ok is True
    assert all(not line.startswith("FAIL") for line in lines)
    assert any(line.startswith("ok    Sign in and verify the drive: documentLibrary drive on utk.sharepoint.com") for line in lines)
    assert any(line.startswith("ok    Write a small test file: SampleTracker/_connection_check/check-") for line in lines)
    assert "All checks passed." in lines
    assert fake.count("PUT", "/_connection_check/check-") == 1
    assert fake.count("POST", "createUploadSession") == 1
    assert fake.count("DELETE", "https://upload.example/session") == 1  # the test session is cancelled, not left open


def test_read_only_writes_nothing(monkeypatch):
    fake = FakeRequests()

    ok, lines = _run(monkeypatch, fake, read_only=True)

    assert ok is True
    assert fake.count("PUT") == 0
    assert fake.count("POST", "createUploadSession") == 0
    assert fake.count("DELETE") == 0
    assert "Skipped the write checks (--read-only)." in lines


def test_personal_drive_fails_the_check_and_nothing_is_written(monkeypatch):
    fake = FakeRequests(drive=PERSONAL)

    ok, lines = _run(monkeypatch, fake)

    assert ok is False
    assert lines[0].startswith("FAIL  Sign in and verify the drive") and "personal OneDrive" in lines[0]
    assert "Nothing was written" in lines[-1]
    assert fake.count("PUT") == 0


def test_wrong_client_secret_gets_a_plain_hint(monkeypatch):
    fake = FakeRequests(token_error=(401, {"error": "invalid_client", "error_description": "AADSTS7000215: Invalid client secret provided."}))

    ok, lines = _run(monkeypatch, fake)

    assert ok is False
    assert any("Secret ID" in line for line in lines)


@pytest.mark.parametrize(
    "description",
    [
        "AADSTS700016: Application with identifier 'x' was not found in the directory 'Some University'.",
        "AADSTS700038: x is not a valid application identifier.",
    ],
)
def test_wrong_client_id_points_at_the_client_id_setting(monkeypatch, description):
    fake = FakeRequests(token_error=(400, {"error": "unauthorized_client", "error_description": description}))

    ok, lines = _run(monkeypatch, fake)

    assert ok is False
    assert any("ONEDRIVE_CLIENT_ID" in line and line.lstrip().startswith("->") for line in lines)


def test_missing_site_grant_gets_a_sites_selected_hint(monkeypatch):
    fake = FakeRequests(drive_status=403, root_status=403)

    ok, lines = _run(monkeypatch, fake)

    assert ok is False
    assert any("Sites.Selected" in line and line.lstrip().startswith("->") for line in lines)
    assert fake.count("PUT") == 0


def test_drive_object_forbidden_but_root_readable_still_passes_with_unknown_type(monkeypatch):
    fake = FakeRequests(drive_status=403)

    ok, lines = _run(monkeypatch, fake)

    assert ok is True
    assert any("unknown drive on utk.sharepoint.com" in line for line in lines)


def test_a_failed_write_is_reported_but_the_upload_session_is_still_tried(monkeypatch):
    fake = FakeRequests(put_status=403)

    ok, lines = _run(monkeypatch, fake)

    assert ok is False
    assert any(line.startswith("FAIL  Write a small test file") for line in lines)
    assert any(line.startswith("ok    Create and cancel an upload session") for line in lines)
    assert "Some checks failed." in lines


def test_a_session_that_cannot_be_cancelled_is_noted_but_not_a_failure(monkeypatch):
    fake = FakeRequests(cancel_status=500)

    ok, lines = _run(monkeypatch, fake)

    assert ok is True
    assert any("cancelling returned HTTP 500" in line for line in lines)


# --- main(): reads the environment ---

ENV = {
    "ONEDRIVE_TENANT_ID": TENANT,
    "ONEDRIVE_CLIENT_ID": "client-id",
    "ONEDRIVE_CLIENT_SECRET": "super-secret-value",
    "ONEDRIVE_DRIVE_ID": "drive",
}


def _env(monkeypatch, **overrides):
    for key in (
        "ONEDRIVE_AUTH_MODE",
        "ONEDRIVE_REFRESH_TOKEN",
        "ONEDRIVE_ALLOWED_HOST_SUFFIXES",
        "ONEDRIVE_ROOT_FOLDER",
        "APP_ENV",
        "FLASK_ENV",
    ):
        monkeypatch.delenv(key, raising=False)
    for key, value in {**ENV, **overrides}.items():
        monkeypatch.setenv(key, value)


def test_main_runs_the_checks_from_the_environment_and_never_prints_the_secret(monkeypatch):
    _env(monkeypatch, ONEDRIVE_ALLOWED_HOST_SUFFIXES="utk.sharepoint.com")
    fake = FakeRequests()
    monkeypatch.setattr(onedrive_client, "requests", fake)
    monkeypatch.setattr(check, "requests", fake)
    lines = []

    code = check.main([], emit=lines.append)

    assert code == 0
    assert lines[0] == f"Target: tenant {TENANT}, drive drive, folder 'SampleTracker', auth client_credentials"
    assert all("super-secret-value" not in line for line in lines)


def test_main_reports_a_missing_setting_instead_of_crashing(monkeypatch):
    _env(monkeypatch)
    monkeypatch.delenv("ONEDRIVE_CLIENT_SECRET")
    lines = []

    code = check.main([], emit=lines.append)

    assert code == 1
    assert lines == ["FAIL  Configuration: Missing required environment variable: ONEDRIVE_CLIENT_SECRET"]


def test_main_refuses_delegated_auth_in_production(monkeypatch):
    _env(monkeypatch, APP_ENV="production", ONEDRIVE_AUTH_MODE="delegated_refresh", ONEDRIVE_REFRESH_TOKEN="refresh")
    lines = []

    code = check.main([], emit=lines.append)

    assert code == 1
    assert "not allowed in production" in lines[0]


def test_main_read_only_flag_is_passed_through(monkeypatch):
    _env(monkeypatch)
    fake = FakeRequests()
    monkeypatch.setattr(onedrive_client, "requests", fake)
    monkeypatch.setattr(check, "requests", fake)

    code = check.main(["--read-only"], emit=lambda line: None)

    assert code == 0
    assert fake.count("PUT") == 0
