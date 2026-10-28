import base64
import io
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

import notion_sample_tracker.app as app_module
from notion_sample_tracker.settings import Settings


PASSWORD = "correct horse battery"
PUBLIC = "http://localhost:8000"
CHUNK = 1024 * 1024  # UPLOAD_CHUNK_MB=1 in these tests


def _settings(tmp_path, **overrides) -> Settings:
    values = dict(
        app_secret_key="test-secret",
        public_base_url=PUBLIC,
        notion_home_url="",
        notion_token="secret_test",
        notion_samples_database_id="samples-db",
        notion_results_database_id="results-db",
        notion_people_database_id="people-db",
        onedrive_tenant_id="",
        onedrive_client_id="",
        onedrive_client_secret="",
        onedrive_auth_mode="client_credentials",
        onedrive_public_client=False,
        onedrive_drive_id="",
        onedrive_refresh_token="",
        onedrive_root_folder="SampleTracker",
        backlog_dir=tmp_path / "backlog",
        max_upload_mb=25,
        storage_backend="local",
        storage_root=tmp_path / "files",
        storage_min_free_mb=0,
        upload_chunk_mb=1,
        max_result_file_mb=50,
    )
    values.update(overrides)
    return Settings(**values)


@pytest.fixture
def notion_state(monkeypatch):
    state = SimpleNamespace(results=[], samples=[], attached=[], qr=[], statuses=[])

    class StubNotion:
        def __init__(self, **kwargs):
            pass

        def sample_storage_info(self, name):
            if name == "PTO54":
                return {"folder": "samples/PbTiO3/PTO54"}
            raise ValueError(f"Could not find Notion page named '{name}'")

        def sample_storage_info_from_page(self, page):
            return {"folder": "samples/PbTiO3/PTO54"}

        def result_page_by_submission(self, submission_id):
            return None

        def result_exists(self, name):
            return False

        def create_result(self, form):
            state.results.append(form)
            return {"id": "result-1", "url": "https://notion.example/result-1", "properties": {}}

        def sample_page_by_submission(self, submission_id):
            return None

        def sample_page_by_name(self, name):
            return None

        def create_sample(self, form):
            state.samples.append(form)
            return {"id": "sample-1", "url": "https://notion.example/sample-1", "properties": {}}

        def attach_uploaded_files(self, page_id, property_name, files):
            state.attached.append((page_id, property_name, [item["name"] for item in files]))

        def set_uploaded_file(self, page_id, property_name, name, content, content_type=None):
            state.qr.append((page_id, property_name, name))

        def update_archive_status(self, page_id, status, error=""):
            state.statuses.append((status, error))

        def describe_relations(self, page):
            return {}, [], []

    monkeypatch.setattr(app_module, "NotionRepository", StubNotion)
    return state


def _client(tmp_path, **overrides):
    app = app_module.create_app(settings_factory=lambda: _settings(tmp_path, **overrides))
    return app.test_client()


def _basic(password=PASSWORD, username="lab"):
    return {"Authorization": "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode()}


def _start(client, filename="scan.csv", size=10, **extra):
    payload = {"filename": filename, "size": size, "entry_name": "run 1", "parent_sample": "PTO54", **extra}
    return client.post("/api/uploads", json=payload)


def _put(client, upload_id, offset, data):
    return client.put(
        f"/api/uploads/{upload_id}",
        data=data,
        headers={"Upload-Offset": str(offset), "Content-Type": "application/octet-stream"},
    )


def _upload(client, data, filename="scan.csv"):
    started = _start(client, filename, len(data)).get_json()
    offset = 0
    while offset < len(data):
        offset = _put(client, started["upload_id"], offset, data[offset : offset + started["chunk_size"]]).get_json()["received"]
    return client.post(f"/api/uploads/{started['upload_id']}/complete").get_json()


# --- the chunked upload API ---


def test_a_file_is_uploaded_in_chunks_and_stored_in_the_sample_results_folder(tmp_path, notion_state):
    client = _client(tmp_path)
    data = bytes(range(256)) * 6144  # 1.5 MB: two chunks

    started = _start(client, size=len(data)).get_json()
    assert started["success"] is True and started["chunk_size"] == CHUNK and started["received"] == 0
    first = _put(client, started["upload_id"], 0, data[:CHUNK])
    assert first.status_code == 200 and first.get_json()["received"] == CHUNK
    second = _put(client, started["upload_id"], CHUNK, data[CHUNK:])
    assert second.get_json()["received"] == len(data)
    done = client.post(f"/api/uploads/{started['upload_id']}/complete")

    body = done.get_json()
    assert done.status_code == 200 and body["complete"] is True
    assert body["path"] == "samples/PbTiO3/PTO54/results/run_1/scan.csv"
    assert body["url"] == f"{PUBLIC}/files/samples/PbTiO3/PTO54/results/run_1/scan.csv"
    assert (tmp_path / "files" / body["path"]).read_bytes() == data


def test_upload_status_lets_a_client_resume(tmp_path, notion_state):
    client = _client(tmp_path)
    started = _start(client, size=8).get_json()
    _put(client, started["upload_id"], 0, b"abcd")

    status = client.get(f"/api/uploads/{started['upload_id']}").get_json()

    assert status["received"] == 4 and status["size"] == 8 and status["complete"] is False
    assert _put(client, started["upload_id"], 4, b"efgh").get_json()["received"] == 8


def test_a_resent_chunk_gets_409_with_the_servers_offset(tmp_path, notion_state):
    client = _client(tmp_path)
    started = _start(client, size=8).get_json()
    _put(client, started["upload_id"], 0, b"abcd")

    response = _put(client, started["upload_id"], 0, b"abcd")

    assert response.status_code == 409
    assert response.get_json()["received"] == 4


def test_completing_too_early_is_409(tmp_path, notion_state):
    client = _client(tmp_path)
    started = _start(client, size=8).get_json()
    _put(client, started["upload_id"], 0, b"abcd")

    response = client.post(f"/api/uploads/{started['upload_id']}/complete")

    assert response.status_code == 409
    assert response.get_json()["received"] == 4


def test_completing_twice_gives_the_same_link(tmp_path, notion_state):
    client = _client(tmp_path)
    started = _start(client, size=4).get_json()
    _put(client, started["upload_id"], 0, b"abcd")

    first = client.post(f"/api/uploads/{started['upload_id']}/complete").get_json()
    second = client.post(f"/api/uploads/{started['upload_id']}/complete").get_json()

    assert first["url"] == second["url"]


@pytest.mark.parametrize(
    "payload, expected",
    [
        ({"filename": "tool.exe", "size": 4, "parent_sample": "PTO54"}, "not allowed"),
        ({"filename": "", "size": 4, "parent_sample": "PTO54"}, "filename is required"),
        ({"size": 4, "parent_sample": "PTO54"}, "filename is required"),
        ({"filename": "a.csv", "size": 0, "parent_sample": "PTO54"}, "empty"),
        ({"filename": "a.csv", "size": 51 * 1024 * 1024, "parent_sample": "PTO54"}, "limit"),
        ({"filename": "a.csv", "size": "4", "parent_sample": "PTO54"}, "whole number"),
        ({"filename": "a.csv", "size": 4, "parent_sample": "NoSuchSample"}, "Could not find"),
    ],
)
def test_starting_an_upload_validates_the_request(tmp_path, notion_state, payload, expected):
    client = _client(tmp_path)

    response = client.post("/api/uploads", json=payload)

    assert response.status_code == 400
    assert expected in response.get_json()["error"]
    assert list((tmp_path / "files" / ".uploads").iterdir()) == []


def test_starting_an_upload_needs_a_json_object(tmp_path, notion_state):
    client = _client(tmp_path)

    assert client.post("/api/uploads", data="not json", content_type="text/plain").status_code == 400
    assert client.post("/api/uploads", json=["a"]).status_code == 400


def test_filenames_are_sanitised_before_they_reach_the_disk(tmp_path, notion_state):
    client = _client(tmp_path)

    done = _upload(client, b"abcd", filename="../../etc/pass wd.csv")

    assert done["path"] == "samples/PbTiO3/PTO54/results/run_1/pass_wd.csv"
    assert (tmp_path / "files" / done["path"]).exists()
    assert not (tmp_path / "etc").exists()


def test_chunk_requests_are_checked(tmp_path, notion_state):
    client = _client(tmp_path)
    started = _start(client, size=8).get_json()
    upload_id = started["upload_id"]

    assert client.put(f"/api/uploads/{upload_id}", data=b"abcd").status_code == 400  # no Upload-Offset
    assert _put(client, upload_id, "not-a-number", b"abcd").status_code == 400
    assert _put(client, "0" * 32, 0, b"abcd").status_code == 404
    assert _put(client, "../../etc", 0, b"abcd").status_code == 404
    assert client.get("/api/uploads/not-an-id").status_code == 404
    assert client.post("/api/uploads/not-an-id/complete").status_code == 404
    over = _put(client, upload_id, 0, b"x" * (CHUNK + 1))
    assert over.status_code == 400 and "larger than" in over.get_json()["error"]
    assert client.get(f"/api/uploads/{upload_id}").get_json()["received"] == 0


def test_the_onedrive_only_upload_session_route_is_unavailable_with_local_storage(tmp_path, notion_state):
    client = _client(tmp_path)

    response = client.post("/api/create-upload-session", json={"filename": "a.csv", "size": 4})

    assert response.status_code == 404
    assert "/api/uploads" in response.get_json()["error"]


# --- downloading ---


def test_a_stored_file_downloads_as_an_attachment_with_safe_headers(tmp_path, notion_state):
    client = _client(tmp_path)
    data = b"x,y\n1,2\n"
    done = _upload(client, data)

    response = client.get(done["url"].replace(PUBLIC, ""))

    assert response.status_code == 200
    assert response.data == data
    assert response.headers["Content-Disposition"].startswith("attachment;")
    assert "scan.csv" in response.headers["Content-Disposition"]
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert "sandbox" in response.headers["Content-Security-Policy"]
    assert response.headers["Cache-Control"] == "private, no-cache"


def test_a_file_with_a_non_ascii_name_can_be_linked_and_downloaded(tmp_path, notion_state):
    client = _client(tmp_path)
    done = _upload(client, b"data", filename="пленка 1.csv")

    assert done["url"].endswith("/results/run_1/%D0%BF%D0%BB%D0%B5%D0%BD%D0%BA%D0%B0_1.csv")
    response = client.get(done["url"].replace(PUBLIC, ""))

    assert response.status_code == 200 and response.data == b"data"
    assert "filename*=UTF-8''" in response.headers["Content-Disposition"]
    assert client.post("/api/submit-data", data=_result_form(done["url"])).status_code == 200


def test_html_is_never_rendered_inline_from_the_apps_own_origin(tmp_path, notion_state):
    client = _client(tmp_path, allowed_upload_extensions=(".html",))
    done = _upload(client, b"<script>alert(1)</script>", filename="page.html")

    response = client.get(done["url"].replace(PUBLIC, ""))

    assert response.headers["Content-Disposition"].startswith("attachment;")
    assert response.headers["X-Content-Type-Options"] == "nosniff"


def test_range_requests_work_for_resuming_big_downloads(tmp_path, notion_state):
    client = _client(tmp_path)
    done = _upload(client, b"0123456789")

    response = client.get(done["url"].replace(PUBLIC, ""), headers={"Range": "bytes=2-5"})

    assert response.status_code == 206
    assert response.data == b"2345"
    assert response.headers["Content-Range"] == "bytes 2-5/10"


def test_download_cannot_reach_anything_outside_the_stored_files(tmp_path, notion_state):
    client = _client(tmp_path)
    done = _upload(client, b"abcd")
    upload_id = next((tmp_path / "files" / ".uploads").iterdir()).name
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.txt").write_text("secret")
    os.symlink(outside, tmp_path / "files" / "link")
    (tmp_path / "secret-in-parent.txt").write_text("secret")

    for path in (
        "/files/../secret-in-parent.txt",
        "/files/%2e%2e/secret-in-parent.txt",
        "/files/samples/%2e%2e/%2e%2e/secret-in-parent.txt",
        "/files/..%2fsecret-in-parent.txt",
        "/files/samples/PbTiO3/PTO54/results/run_1/..%2f..%2f..%2f..%2f..%2fsecret-in-parent.txt",
        f"/files/.uploads/{upload_id}/meta.json",
        "/files/.uploads",
        "/files/link/secret.txt",
        "/files/samples",  # a directory
        "/files/",
        "/files/nope.txt",
        "/files/a%00b",
        "/files/a%5Cb",
    ):
        assert client.get(path).status_code == 404, path


def test_the_staging_area_is_never_served_even_with_its_real_path(tmp_path, notion_state):
    client = _client(tmp_path)
    started = _start(client, size=8).get_json()
    _put(client, started["upload_id"], 0, b"abcd")

    assert client.get(f"/files/.uploads/{started['upload_id']}/data.part").status_code == 404


# --- sample and result submissions end to end ---


def test_a_sample_submission_writes_its_record_qr_code_and_photos_to_disk(tmp_path, notion_state):
    client = _client(tmp_path)
    photo = io.BytesIO(b"\x89PNG fake image bytes")

    response = client.post(
        "/api/submit",
        data={"sample_name": "PTO54", "sample_type": "Root Sample", "composition": "PbTiO3", "photos": (photo, "front view.png")},
        content_type="multipart/form-data",
    )

    assert response.status_code == 200, response.get_json()
    assert response.get_json()["success"] is True
    folder = tmp_path / "files" / "samples" / "PbTiO3" / "PTO54"
    assert (folder / "photos" / "front_view.png").read_bytes() == b"\x89PNG fake image bytes"
    assert (folder / "PTO54_qr.png").read_bytes().startswith(b"\x89PNG")
    assert '"form"' in (folder / "record.json").read_text()
    assert notion_state.attached == [("sample-1", "Photos", ["front_view.png"])]
    assert notion_state.statuses[-1] == ("Archive complete", "")


def _result_form(url, **extra):
    return {
        "name": "run 1",
        "entry_type": "XRD",
        "parent_entry": "sample",
        "parent_sample": "PTO54",
        "data_type": "file",
        "onedrive_path": url,
        "submission_id": "result-submission-1",
        **extra,
    }


def test_a_data_entry_with_an_uploaded_file_links_to_it_and_archives_its_record(tmp_path, notion_state):
    client = _client(tmp_path)
    done = _upload(client, b"abcdefgh")

    response = client.post("/api/submit-data", data=_result_form(done["url"]))

    assert response.status_code == 200, response.get_json()
    assert notion_state.results[0].link == done["url"]
    folder = tmp_path / "files" / "samples" / "PbTiO3" / "PTO54" / "results" / "run_1"
    assert sorted(path.name for path in folder.iterdir()) == ["record.json", "run_1_qr.png", "scan.csv"]
    assert notion_state.statuses[-1] == ("Archive complete", "")


def test_a_link_typed_before_switching_to_file_does_not_replace_the_uploaded_files_link(tmp_path, notion_state):
    client = _client(tmp_path)
    done = _upload(client, b"abcdefgh")

    client.post("/api/submit-data", data=_result_form(done["url"], data_link="https://stale.example/typed-earlier"))

    assert notion_state.results[0].link == done["url"]


@pytest.mark.parametrize(
    "link",
    [
        "https://evil.example/files/samples/PbTiO3/PTO54/results/run_1/scan.csv",
        f"{PUBLIC}/files/samples/PbTiO3/PTO54/results/run_1/never-uploaded.csv",
        f"{PUBLIC}/files/../secret.txt",
        f"{PUBLIC}/files/.uploads/anything",
        "not a url",
    ],
)
def test_a_data_entry_cannot_point_at_a_file_this_server_does_not_have(tmp_path, notion_state, link):
    client = _client(tmp_path)

    response = client.post("/api/submit-data", data=_result_form(link))

    assert response.status_code == 400
    assert "not found on the server" in response.get_json()["error"]
    assert notion_state.results == []


def test_a_data_entry_without_the_uploaded_files_link_is_refused(tmp_path, notion_state):
    client = _client(tmp_path)

    response = client.post("/api/submit-data", data=_result_form(""))

    assert response.status_code == 400
    assert "link is missing" in response.get_json()["error"]


def test_the_preflight_validation_does_not_require_the_file_yet(tmp_path, notion_state):
    client = _client(tmp_path)

    response = client.post("/api/validate-result", data=_result_form(""))

    assert response.status_code == 200


# --- the pages ---


def test_the_results_page_tells_the_browser_which_backend_to_use(tmp_path, notion_state):
    local = _client(tmp_path).get("/results/new").get_data(as_text=True)

    assert 'const STORAGE_BACKEND = "local";' in local
    assert "uploaded to this server in small chunks" in local
    assert "browser uploads directly to OneDrive" not in local
    assert "uploadViaServer" in local and "uploadViaOneDrive" in local


def test_health_reports_the_backend_and_stays_open_to_health_checks(tmp_path, notion_state):
    client = _client(tmp_path, access_password=PASSWORD)

    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.get_json()["storage_backend"] == "local"


# --- the access gate ---


def test_with_a_password_every_page_and_file_needs_it(tmp_path, notion_state):
    client = _client(tmp_path, access_password=PASSWORD)
    done = _upload_with_auth(client)
    file_path = done["url"].replace(PUBLIC, "")

    for path in ("/", "/results/new", "/samples/new", "/api/options", file_path, "/static/app.css"):
        response = client.get(path)
        assert response.status_code == 401, path
        assert response.headers["WWW-Authenticate"].startswith("Basic ")
    assert client.post("/api/uploads", json={}).status_code == 401
    assert client.put("/api/uploads/" + "0" * 32, data=b"x").status_code == 401

    assert client.get(file_path, headers=_basic()).data == b"abcd"
    assert client.get("/results/new", headers=_basic()).status_code == 200


def _upload_with_auth(client, data=b"abcd"):
    auth = _basic()
    started = client.post("/api/uploads", json={"filename": "scan.csv", "size": len(data), "parent_sample": "PTO54"}, headers=auth).get_json()
    client.put(
        f"/api/uploads/{started['upload_id']}",
        data=data,
        headers={**auth, "Upload-Offset": "0", "Content-Type": "application/octet-stream"},
    )
    return client.post(f"/api/uploads/{started['upload_id']}/complete", headers=auth).get_json()


@pytest.mark.parametrize("headers", [_basic(password="wrong"), _basic(username="intruder"), _basic(password=""), {"Authorization": "Bearer abc"}])
def test_wrong_credentials_are_refused(tmp_path, notion_state, headers):
    client = _client(tmp_path, access_password=PASSWORD)

    assert client.get("/results/new", headers=headers).status_code == 401


def test_without_a_password_the_site_is_open_as_before(tmp_path, notion_state):
    client = _client(tmp_path)

    assert client.get("/results/new").status_code == 200


def test_posts_from_other_sites_are_refused_but_the_apps_own_pages_still_work(tmp_path, notion_state):
    client = _client(tmp_path)

    assert client.post("/api/validate-result", data={}, headers={"Origin": "https://evil.example"}).status_code == 403
    assert client.post("/api/validate-result", data={}, headers={"Origin": "null"}).status_code == 403
    assert client.post("/api/uploads", json={}, headers={"Origin": "https://evil.example"}).status_code == 403
    assert client.put("/api/uploads/" + "0" * 32, data=b"x", headers={"Origin": "https://evil.example"}).status_code == 403
    # the app's own origin, and requests without an Origin header, get through to normal validation (400 here)
    assert client.post("/api/validate-result", data={}, headers={"Origin": PUBLIC}).status_code == 400
    assert client.post("/api/validate-result", data={}, headers={"Origin": "http://localhost"}).status_code == 400
    assert client.post("/api/validate-result", data={}).status_code == 400
    assert client.get("/results/new", headers={"Origin": "https://evil.example"}).status_code == 200


# --- the OneDrive backend is unchanged ---


def test_with_onedrive_storage_the_local_routes_do_not_exist_and_the_upload_session_route_works(tmp_path, notion_state, monkeypatch):
    class StubOneDrive:
        def __init__(self, **kwargs):
            pass

        def create_upload_session(self, relative_path):
            return {"upload_url": "https://upload.example/session", "onedrive_path": relative_path}

    monkeypatch.setattr(app_module, "OneDriveClient", StubOneDrive)
    client = _client(
        tmp_path,
        storage_backend="onedrive",
        onedrive_tenant_id="tenant-id",
        onedrive_client_id="client-id",
        onedrive_client_secret="client-secret",
        onedrive_drive_id="drive-id",
    )

    assert client.post("/api/uploads", json={"filename": "a.csv", "size": 4}).status_code == 404
    assert client.get("/files/samples/a.txt").status_code == 404
    response = client.post("/api/create-upload-session", json={"filename": "a.csv", "size": 4, "parent_sample": "PTO54", "entry_name": "run 1"})
    assert response.status_code == 200
    assert response.get_json()["onedrive_path"] == "samples/PbTiO3/PTO54/results/run_1/a.csv"
    # The app writes record.json and <entry>_qr.png into this folder after the upload; OneDrive ignores case.
    for filename, stored in [("record.json", "record-1.json"), ("RUN_1_QR.png", "RUN_1_QR-1.png"), ("run_2_qr.png", "run_2_qr.png")]:
        response = client.post("/api/create-upload-session", json={"filename": filename, "size": 4, "parent_sample": "PTO54", "entry_name": "run 1"})
        assert response.get_json()["onedrive_path"] == f"samples/PbTiO3/PTO54/results/run_1/{stored}"
    assert 'const STORAGE_BACKEND = "onedrive";' in client.get("/results/new").get_data(as_text=True)
    assert not (tmp_path / "files").exists()  # nothing is created on disk for the OneDrive backend


def test_startup_logs_where_file_links_will_point(tmp_path, notion_state, capsys):
    _client(tmp_path)

    output = capsys.readouterr().out

    assert '"event": "local_storage_ready"' in output
    assert f'"public_address": "{PUBLIC}"' in output
    assert str(tmp_path / "files") in output
