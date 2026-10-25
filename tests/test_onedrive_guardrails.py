import pytest
from graph_fakes import UTK_LIBRARY, FakeRequests

from notion_sample_tracker.services import onedrive_client
from notion_sample_tracker.services.onedrive_client import DriveInfo, OneDriveClient, OneDriveConfigError


TENANT = "11111111-2222-3333-4444-555555555555"


def _client(monkeypatch, fake, **overrides):
    monkeypatch.setattr(onedrive_client, "requests", fake)
    options = dict(
        tenant_id=TENANT,
        client_id="client",
        client_secret="secret",
        root_folder="SampleTracker",
        drive_id="drive",
    )
    options.update(overrides)
    return OneDriveClient(**options)


def test_personal_drive_is_refused_before_any_write(monkeypatch):
    fake = FakeRequests(drive={"driveType": "personal", "webUrl": "https://onedrive.live.com/?cid=abc"})
    client = _client(monkeypatch, fake)

    with pytest.raises(OneDriveConfigError, match="personal OneDrive"):
        client.upload_bytes("samples/a.txt", b"data")

    assert fake.count("PUT") == 0
    assert fake.count("POST", "/children") == 0


def test_personal_onedrive_host_is_refused_even_if_drive_type_looks_organizational(monkeypatch):
    fake = FakeRequests(drive={"driveType": "business", "webUrl": "https://1drv.ms/f/abc"})
    client = _client(monkeypatch, fake)

    with pytest.raises(OneDriveConfigError, match="personal OneDrive host"):
        client.upload_bytes("samples/a.txt", b"data")

    assert fake.count("PUT") == 0


@pytest.mark.parametrize("drive_type", ["", "unexpected"])
def test_unknown_drive_type_is_refused(monkeypatch, drive_type):
    fake = FakeRequests(drive={"driveType": drive_type, "webUrl": "https://utk.sharepoint.com/sites/lab"})
    client = _client(monkeypatch, fake)

    with pytest.raises(OneDriveConfigError, match="only business or documentLibrary"):
        client.upload_bytes("samples/a.txt", b"data")

    assert fake.count("PUT") == 0


@pytest.mark.parametrize(
    "drive",
    [
        {"driveType": "documentLibrary", "webUrl": "https://utk.sharepoint.com/sites/lab/Shared%20Documents"},
        {"driveType": "business", "webUrl": "https://utk-my.sharepoint.com/personal/someone_utk_edu/Documents"},
    ],
)
def test_organizational_drives_are_accepted(monkeypatch, drive):
    fake = FakeRequests(drive=drive)
    client = _client(monkeypatch, fake)

    result = client.upload_bytes("samples/a.txt", b"data")

    assert result.path == "SampleTracker/samples/a.txt"
    assert fake.count("PUT") == 1


@pytest.mark.parametrize(
    "suffixes, host, allowed",
    [
        (("utk.sharepoint.com",), "utk.sharepoint.com", True),
        (("utk.sharepoint.com",), "sub.utk.sharepoint.com", True),
        (("utk.sharepoint.com",), "notutk.sharepoint.com", False),
        ((".sharepoint.com",), "utk.sharepoint.com", True),
        (("utk.sharepoint.com", "utk-my.sharepoint.com"), "utk-my.sharepoint.com", True),
        (("utk.sharepoint.com",), "other.example.com", False),
    ],
)
def test_allowed_host_suffixes_are_enforced(monkeypatch, suffixes, host, allowed):
    fake = FakeRequests(drive={"driveType": "documentLibrary", "webUrl": f"https://{host}/sites/lab"})
    client = _client(monkeypatch, fake, allowed_host_suffixes=suffixes)

    if allowed:
        client.upload_bytes("a.txt", b"data")
        assert fake.count("PUT") == 1
    else:
        with pytest.raises(OneDriveConfigError, match="ONEDRIVE_ALLOWED_HOST_SUFFIXES"):
            client.upload_bytes("a.txt", b"data")
        assert fake.count("PUT") == 0


def test_the_real_utk_hosts_are_not_mistaken_for_personal_hosts(monkeypatch):
    for host in ("liveutk.sharepoint.com", "liveutk-my.sharepoint.com"):
        fake = FakeRequests(drive={"driveType": "business", "webUrl": f"https://{host}/personal/someone/Documents"})
        client = _client(monkeypatch, fake, allowed_host_suffixes=("liveutk.sharepoint.com", "liveutk-my.sharepoint.com"))

        assert client.upload_bytes("a.txt", b"data").path == "SampleTracker/a.txt"


def test_drive_is_verified_once_across_uploads(monkeypatch):
    fake = FakeRequests()
    client = _client(monkeypatch, fake)

    client.upload_bytes("a.txt", b"1")
    client.upload_json("b.json", {"k": "v"})
    client.create_upload_session("c.bin")

    assert fake.drive_checks() == 1


def test_failed_drive_check_is_never_cached_as_success(monkeypatch):
    fake = FakeRequests(drive={"driveType": "personal", "webUrl": "https://onedrive.live.com/"})
    client = _client(monkeypatch, fake)

    for _ in range(2):
        with pytest.raises(OneDriveConfigError):
            client.upload_bytes("a.txt", b"data")

    assert fake.drive_checks() == 2
    assert fake.count("PUT") == 0


def test_upload_session_also_verifies_the_drive(monkeypatch):
    fake = FakeRequests(drive={"driveType": "personal", "webUrl": "https://onedrive.live.com/"})
    client = _client(monkeypatch, fake)

    with pytest.raises(OneDriveConfigError):
        client.create_upload_session("results/x/file.csv")

    assert fake.count("POST", "createUploadSession") == 0


def test_verify_drive_reports_what_it_found(monkeypatch):
    client = _client(monkeypatch, FakeRequests())

    info = client.verify_drive()

    assert info == DriveInfo(
        drive_id="drive",
        drive_type="documentLibrary",
        web_url=UTK_LIBRARY["webUrl"],
        host="utk.sharepoint.com",
    )


# --- when the app may use a drive's content but not read the drive object (403), fall back to the root folder ---


def test_forbidden_drive_object_falls_back_to_the_root_folder_host(monkeypatch):
    fake = FakeRequests(drive_status=403)
    client = _client(monkeypatch, fake)

    info = client.verify_drive()

    assert info.drive_type == "unknown"
    assert info.host == "utk.sharepoint.com"
    assert fake.root_checks() == 1


def test_fallback_still_refuses_a_personal_host(monkeypatch):
    fake = FakeRequests(drive_status=403, root={"webUrl": "https://onedrive.live.com/?cid=abc"})
    client = _client(monkeypatch, fake)

    with pytest.raises(OneDriveConfigError, match="personal OneDrive host"):
        client.upload_bytes("a.txt", b"data")

    assert fake.count("PUT") == 0


def test_fallback_still_enforces_host_pinning(monkeypatch):
    fake = FakeRequests(drive_status=403)
    client = _client(monkeypatch, fake, allowed_host_suffixes=("other.sharepoint.com",))

    with pytest.raises(OneDriveConfigError, match="ONEDRIVE_ALLOWED_HOST_SUFFIXES"):
        client.upload_bytes("a.txt", b"data")


def test_fallback_without_any_host_cannot_be_verified(monkeypatch):
    fake = FakeRequests(drive_status=403, root={"webUrl": ""})
    client = _client(monkeypatch, fake)

    with pytest.raises(OneDriveConfigError, match="cannot be verified"):
        client.upload_bytes("a.txt", b"data")

    assert fake.count("PUT") == 0


def test_no_access_to_the_drive_or_its_root_is_an_error_not_a_pass(monkeypatch):
    fake = FakeRequests(drive_status=403, root_status=403)
    client = _client(monkeypatch, fake)

    with pytest.raises(RuntimeError, match="accessDenied"):
        client.upload_bytes("a.txt", b"data")

    assert fake.count("PUT") == 0


def test_a_missing_drive_does_not_trigger_the_fallback(monkeypatch):
    fake = FakeRequests(drive_status=404)
    client = _client(monkeypatch, fake)

    with pytest.raises(RuntimeError, match="verify drive failed"):
        client.upload_bytes("a.txt", b"data")

    assert fake.root_checks() == 0


def test_fallback_result_is_cached(monkeypatch):
    fake = FakeRequests(drive_status=403)
    client = _client(monkeypatch, fake)

    client.upload_bytes("a.txt", b"1")
    client.upload_bytes("b.txt", b"2")

    assert fake.drive_checks() == 1
    assert fake.root_checks() == 1


# --- tenants and drive IDs ---


@pytest.mark.parametrize("tenant", ["common", "consumers", "organizations", " Consumers ", ""])
def test_blocked_tenants_fail_before_any_network_call(monkeypatch, tenant):
    fake = FakeRequests()
    client = _client(monkeypatch, fake, tenant_id=tenant)

    with pytest.raises(OneDriveConfigError):
        client.upload_bytes("a.txt", b"data")

    assert fake.calls == []


@pytest.mark.parametrize("auth_mode", ["client_credentials", "delegated_refresh"])
def test_missing_drive_id_never_falls_back_to_me_drive(monkeypatch, auth_mode):
    fake = FakeRequests()
    client = _client(monkeypatch, fake, auth_mode=auth_mode, drive_id="", refresh_token="refresh")

    with pytest.raises(OneDriveConfigError, match="ONEDRIVE_DRIVE_ID"):
        client.upload_bytes("a.txt", b"data")

    assert fake.calls == []
    assert "/me/" not in client._drive_base()


def test_delegated_token_request_uses_the_configured_tenant_not_consumers(monkeypatch):
    fake = FakeRequests()
    client = _client(monkeypatch, fake, auth_mode="delegated_refresh", refresh_token="refresh")

    client.upload_bytes("a.txt", b"data")

    assert fake.token_urls == [f"https://login.microsoftonline.com/{TENANT}/oauth2/v2.0/token"]
    assert all("/me/" not in url and "consumers" not in url for _, url in fake.calls)


# --- token caching ---


def test_access_token_is_cached_until_shortly_before_expiry(monkeypatch):
    fake = FakeRequests(expires_in=3600)
    client = _client(monkeypatch, fake)
    clock = {"now": 1_000.0}
    client._now = lambda: clock["now"]

    client.upload_bytes("a.txt", b"1")
    client.upload_bytes("b.txt", b"2")
    assert len(fake.token_urls) == 1

    clock["now"] += 3_299  # still inside the 3600s lifetime minus the 300s margin
    client.upload_bytes("c.txt", b"3")
    assert len(fake.token_urls) == 1

    clock["now"] += 2  # now past the margin
    client.upload_bytes("d.txt", b"4")
    assert len(fake.token_urls) == 2


def test_short_lived_tokens_are_still_reused_briefly(monkeypatch):
    fake = FakeRequests(expires_in=120)
    client = _client(monkeypatch, fake)
    clock = {"now": 0.0}
    client._now = lambda: clock["now"]

    client.upload_bytes("a.txt", b"1")
    clock["now"] = 59
    client.upload_bytes("b.txt", b"2")
    assert len(fake.token_urls) == 1

    clock["now"] = 61
    client.upload_bytes("c.txt", b"3")
    assert len(fake.token_urls) == 2
