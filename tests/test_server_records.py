"""The record.json files saved on the server match Notion after a submission, and the CSV export command reads them.

These run the real routes and the real NotionRepository against a small in-memory Notion, so the records are
compared with what Notion itself would hold, not with what a stub returned.
"""
import copy
import csv
import io
import json
from pathlib import Path

import pytest

import notion_sample_tracker.app as app_module
from notion_sample_tracker import records_export
from notion_sample_tracker.services.local_storage import LocalStorage
from notion_sample_tracker.services.notion_client import NotionRepository
from notion_sample_tracker.settings import Settings


TITLES = {"samples-db": "Name", "results-db": "Name", "people-db": "Person"}
SOURCES = json.dumps([{"name": "Ada Lovelace", "email": "ada@utk.edu", "affiliation": "UTK"}])


def _text(items):
    return [{"plain_text": item.get("plain_text", item.get("text", {}).get("content", ""))} for item in items]


def _as_stored(value):
    """The property value Notion returns for one the app sent."""
    for kind in ("select", "multi_select", "relation", "url", "email", "number", "date", "checkbox"):
        if kind in value:
            return {"type": kind, kind: value[kind]}
    if "title" in value:
        return {"type": "title", "title": _text(value["title"])}
    if "rich_text" in value:
        return {"type": "rich_text", "rich_text": _text(value["rich_text"])}
    if "files" in value:
        files = [
            item if item.get("type") == "external" else {"name": item["name"], "type": "file", "file": {"url": "https://notion-files/" + item["name"]}}
            for item in value["files"]
        ]
        return {"type": "files", "files": files}
    return value


class FakeNotionApi:
    """Pages and data sources kept in memory, answering the calls NotionRepository makes."""

    def __init__(self):
        self.store: dict[str, dict] = {}
        self.on_update = None  # called with (page_id, properties) before an update is applied
        self.unreadable: set[str] = set()
        self.pages = self
        self.data_sources = self

    # pages
    def create(self, parent, properties):
        page_id = f"{len(self.store) + 1:08d}-aaaa-bbbb-cccc-dddddddddddd"
        self.store[page_id] = {
            "object": "page",
            "id": page_id,
            "url": f"https://www.notion.so/{page_id.replace('-', '')}",
            "parent": parent,
            "created_time": f"2026-10-28T10:{len(self.store):02d}:00.000Z",
            "last_edited_time": f"2026-10-28T10:{len(self.store):02d}:00.000Z",
            "properties": {name: _as_stored(value) for name, value in properties.items()},
        }
        return copy.deepcopy(self.store[page_id])

    def update(self, page_id, properties):
        if self.on_update:
            self.on_update(page_id, properties)
        self.store[page_id]["properties"].update({name: _as_stored(value) for name, value in properties.items()})
        return copy.deepcopy(self.store[page_id])

    def retrieve(self, page_id=None, data_source_id=None):
        if data_source_id is not None:
            return {"properties": {TITLES[data_source_id]: {"type": "title"}}}
        if page_id in self.unreadable:
            raise RuntimeError("Could not find page")
        return copy.deepcopy(self.store[page_id])

    # data sources
    def query(self, data_source_id, filter=None, **kwargs):
        rows = [page for page in self.store.values() if page["parent"].get("data_source_id") == data_source_id]
        if filter:
            kind = "title" if "title" in filter else "rich_text"
            wanted = filter[kind]["equals"]
            rows = [
                page
                for page in rows
                if "".join(item["plain_text"] for item in page["properties"].get(filter["property"], {}).get(kind, [])) == wanted
            ]
        return {"results": copy.deepcopy(rows), "has_more": False}

    def page_named(self, title):
        return next(page for page in self.store.values() if _title(page) == title)

    def status_of(self, title):
        return self.page_named(title)["properties"]["Archive Status"]["select"]["name"]


def _title(page):
    for prop in page["properties"].values():
        if prop.get("type") == "title":
            return "".join(item["plain_text"] for item in prop["title"])
    return ""


@pytest.fixture
def api(monkeypatch):
    fake = FakeNotionApi()

    class InMemoryNotion(NotionRepository):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.client = fake

        def _upload_file_to_notion(self, name, content, content_type):
            return "upload-" + name

    monkeypatch.setattr(app_module, "NotionRepository", InMemoryNotion)
    return fake


def _settings(tmp_path, **overrides):
    values = dict(
        app_secret_key="test-secret",
        public_base_url="http://localhost:8000",
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
    )
    values.update(overrides)
    return Settings(**values)


@pytest.fixture
def client(tmp_path, api):
    return app_module.create_app(settings_factory=lambda: _settings(tmp_path)).test_client()


def _submit_sample(client, **fields):
    data = {
        "sample_name": "PTO54",
        "sample_type": "Root Sample",
        "composition": "PbTiO3",
        "synthesis": "PLD",
        "sources_data": SOURCES,
        "photos": (io.BytesIO(b"\x89PNG photo"), "front.png"),
        **fields,
    }
    response = client.post("/api/submit", data=data, content_type="multipart/form-data")
    return response


def _chunked_upload(client, filename, content, entry_name="XRD run 1"):
    started = client.post("/api/uploads", json={"filename": filename, "size": len(content), "entry_name": entry_name, "parent_sample": "PTO54"}).get_json()
    client.put(f"/api/uploads/{started['upload_id']}", data=content, headers={"Upload-Offset": "0"})
    return client.post(f"/api/uploads/{started['upload_id']}/complete").get_json()


def _submit_result(client, name="XRD run 1", content=b"a,b\n1,2\n"):
    done = _chunked_upload(client, "scan.csv", content, entry_name=name)
    return client.post(
        "/api/submit-data",
        data={
            "name": name,
            "entry_type": "XRD",
            "parent_entry": "sample",
            "parent_sample": "PTO54",
            "data_type": "file",
            "onedrive_path": done["url"],
            "brief_description": "first scan",
            "submission_id": f"submission-{name}",
        },
    )


def _read(tmp_path, relative):
    return json.loads((tmp_path / "files" / relative).read_text())


# --- records match Notion ---


def test_a_sample_record_is_the_page_as_notion_holds_it_after_the_last_write(tmp_path, api, client):
    response = _submit_sample(client)
    assert response.status_code == 200, response.get_json()

    record = _read(tmp_path, "samples/TiPbO3/PTO54/record.json")
    notion_now = api.page_named("PTO54")
    assert record["notion"] == notion_now
    assert record["kind"] == "sample"
    assert record["captured_at"]
    # The writes that used to come after the snapshot are in it now.
    assert record["notion"]["properties"]["Archive Status"]["select"]["name"] == "Archive complete"
    assert [item["name"] for item in record["notion"]["properties"]["QRCode"]["files"]] == ["PTO54_qr.png"]
    assert [item["name"] for item in record["notion"]["properties"]["Photos"]["files"]] == ["front.png"]


def test_a_sample_record_names_its_sources_and_each_source_is_saved(tmp_path, api, client):
    _submit_sample(client)

    record = _read(tmp_path, "samples/TiPbO3/PTO54/record.json")
    person = api.page_named("Ada Lovelace")
    assert record["relations"]["Source"] == [{"id": person["id"], "name": "Ada Lovelace"}]
    assert "relations_error" not in record

    source = _read(tmp_path, "sources/Ada_Lovelace/record.json")
    assert source["kind"] == "source"
    assert source["notion"] == person


def test_a_result_record_is_the_page_as_notion_holds_it_and_names_its_sample(tmp_path, api, client):
    _submit_sample(client)
    response = _submit_result(client)
    assert response.status_code == 200, response.get_json()
    assert "warning" not in response.get_json()

    record = _read(tmp_path, "samples/TiPbO3/PTO54/results/XRD_run_1/record.json")
    notion_now = api.page_named("XRD run 1")
    assert record["notion"] == notion_now
    assert record["kind"] == "result"
    assert api.status_of("XRD run 1") == "Archive complete"
    sample = api.page_named("PTO54")
    assert {"id": sample["id"], "name": "PTO54"} in [item for items in record["relations"].values() for item in items]


def test_an_amendment_leaves_current_and_revision_records_matching_notion(tmp_path, api, client):
    _submit_sample(client)
    sample = api.page_named("PTO54")

    response = _submit_sample(client, synthesis="Sol-gel", amend_existing="true", existing_sample_id=sample["id"], photos=(io.BytesIO(b"\x89PNG side"), "side.png"))
    body = response.get_json()
    assert response.status_code == 200, body
    assert body["amended"] is True

    notion_now = api.page_named("PTO54")
    current = _read(tmp_path, "samples/TiPbO3/PTO54/current/record.json")
    revision = _read(tmp_path, f"samples/TiPbO3/PTO54/revisions/{body['revision_id']}/record.json")
    assert current["notion"] == notion_now
    assert revision == current
    assert sorted(item["name"] for item in current["notion"]["properties"]["Photos"]["files"]) == ["front.png", "side.png"]
    assert current["changed_fields"]


def test_a_record_is_on_disk_before_notion_is_told_the_archive_is_complete(tmp_path, api, client):
    seen = []

    def check(page_id, properties):
        status = properties.get("Archive Status", {}).get("select", {}).get("name")
        if status == "Archive complete":
            seen.append((tmp_path / "files/samples/TiPbO3/PTO54/record.json").exists())

    api.on_update = check
    _submit_sample(client)

    assert seen == [True]


def test_a_failed_final_record_write_marks_the_notion_page_failed(tmp_path, api, client, monkeypatch):
    _failing_json_writes(monkeypatch, "PTO54/record.json", from_call=2)

    response = _submit_sample(client)

    assert response.status_code == 400
    assert api.status_of("PTO54") == "Failed archive"
    # The first record is still there, so the files are not orphaned.
    assert _read(tmp_path, "samples/TiPbO3/PTO54/record.json")["notion"]["id"] == api.page_named("PTO54")["id"]


def _failing_json_writes(monkeypatch, path_suffix, from_call=1):
    """Make LocalStorage.upload_json fail for paths ending in `path_suffix`, from the `from_call`-th such write on."""
    real_upload_json = LocalStorage.upload_json
    calls = []

    def upload_json(self, relative_path, payload):
        if relative_path.endswith(path_suffix):
            calls.append(relative_path)
            if len(calls) >= from_call:
                raise OSError("disk went away")
        return real_upload_json(self, relative_path, payload)

    monkeypatch.setattr(LocalStorage, "upload_json", upload_json)


def test_a_sample_with_a_failed_photo_is_saved_as_failed_with_the_reason(tmp_path, api, client, monkeypatch):
    real_upload_bytes = LocalStorage.upload_bytes

    def upload_bytes(self, relative_path, content, content_type="application/octet-stream"):
        if "/photos/" in relative_path:
            raise OSError("photo disk full")
        return real_upload_bytes(self, relative_path, content, content_type)

    monkeypatch.setattr(LocalStorage, "upload_bytes", upload_bytes)
    response = _submit_sample(client)

    assert response.status_code == 200
    assert "photo disk full" in response.get_json()["warning"]
    record = _read(tmp_path, "samples/TiPbO3/PTO54/record.json")
    assert record["notion"] == api.page_named("PTO54")
    assert record["notion"]["properties"]["Archive Status"]["select"]["name"] == "Failed archive"
    assert "photo disk full" in record["notion"]["properties"]["Archive Error"]["rich_text"][0]["plain_text"]


def test_a_failed_result_record_write_marks_the_notion_page_failed(tmp_path, api, client, monkeypatch):
    _submit_sample(client)
    _failing_json_writes(monkeypatch, "XRD_run_1/record.json")

    response = _submit_result(client)

    assert response.status_code == 200  # the entry is in Notion; the archive part is reported
    assert "snapshot_upload: disk went away" in response.get_json()["warning"]
    assert api.status_of("XRD run 1") == "Failed archive"


def test_a_failed_amendment_record_write_marks_the_notion_page_failed(tmp_path, api, client, monkeypatch):
    _submit_sample(client)
    sample = api.page_named("PTO54")
    # current/record.json is written last, after Notion was told the archive is complete.
    _failing_json_writes(monkeypatch, "current/record.json")

    response = _submit_sample(client, synthesis="Sol-gel", amend_existing="true", existing_sample_id=sample["id"])

    assert response.status_code == 400
    assert api.status_of("PTO54") == "Failed archive"
    assert "archive: disk went away" in "".join(item["plain_text"] for item in api.page_named("PTO54")["properties"]["Archive Error"]["rich_text"])


def test_linked_records_are_looked_up_once_and_a_cut_off_list_is_reported(api):
    notion = app_module.NotionRepository(token="t", samples_db="samples-db", results_db="results-db", people_db="people-db", formula_parser=None)
    person = api.create({"data_source_id": "people-db"}, {"Person": {"title": [{"text": {"content": "Ada"}}]}})
    looked_up = []
    real_retrieve = api.retrieve
    api.retrieve = lambda page_id=None, **kwargs: looked_up.append(page_id) or real_retrieve(page_id=page_id, **kwargs)
    page = {
        "id": "sample",
        "properties": {
            "Source": {"type": "relation", "relation": [{"id": person["id"]}, {"id": person["id"]}], "has_more": True},
            "Parent Sample": {"type": "relation", "relation": []},
            "Name": {"type": "title", "title": []},
        },
    }

    relations, people, problems = notion.describe_relations(page)

    assert looked_up == [person["id"]]
    assert relations == {"Source": [{"id": person["id"], "name": "Ada"}] * 2, "Parent Sample": []}
    assert [item["id"] for item in people] == [person["id"]]
    assert problems == ["Source: Notion listed only the first linked records"]


def test_a_linked_record_notion_cannot_read_keeps_its_id_and_is_reported(tmp_path, api, client):
    original_create = api.create

    def create(parent, properties):
        page = original_create(parent, properties)
        if parent.get("data_source_id") == "people-db":
            api.unreadable.add(page["id"])
        return page

    api.create = create
    response = _submit_sample(client)
    assert response.status_code == 200, response.get_json()

    record = _read(tmp_path, "samples/TiPbO3/PTO54/record.json")
    person_id = api.page_named("Ada Lovelace")["id"]
    assert record["relations"]["Source"] == [{"id": person_id, "name": ""}]
    assert "Source" in record["relations_error"]
    assert not (tmp_path / "files/sources").exists()


# --- uploaded files are never overwritten ---


def test_uploaded_files_named_like_the_app_record_and_qr_code_are_kept(tmp_path, api, client):
    _submit_sample(client)
    mine = {
        "record.json": b'{"instrument": "my own record"}',
        "Record.JSON": b'{"instrument": "upper case"}',
        "XRD_run_1_qr.png": b"my own QR picture",
    }
    names = {filename: _chunked_upload(client, filename, content)["name"] for filename, content in mine.items()}

    response = _submit_result(client)
    assert response.status_code == 200, response.get_json()

    folder = tmp_path / "files/samples/TiPbO3/PTO54/results/XRD_run_1"
    assert names["record.json"] == "record-1.json"
    assert names["XRD_run_1_qr.png"] == "XRD_run_1_qr-1.png"
    # Record-1.json on a case-sensitive disk (Linux); Record-2.json where record-1.json already counts (macOS).
    assert names["Record.JSON"] in {"Record-1.json", "Record-2.json"}
    for filename, content in mine.items():
        assert (folder / names[filename]).read_bytes() == content
    assert json.loads((folder / "record.json").read_text())["kind"] == "result"
    assert (folder / "XRD_run_1_qr.png").read_bytes().startswith(b"\x89PNG")


def test_photos_with_the_same_name_are_all_kept(tmp_path, api, client):
    response = client.post(
        "/api/submit",
        data={
            "sample_name": "PTO54",
            "sample_type": "Root Sample",
            "composition": "PbTiO3",
            "sources_data": SOURCES,
            "photos": [(io.BytesIO(b"\x89PNG first"), "image.jpg"), (io.BytesIO(b"\x89PNG second"), "image.jpg")],
        },
        content_type="multipart/form-data",
    )
    assert response.status_code == 200, response.get_json()

    photos = tmp_path / "files/samples/TiPbO3/PTO54/photos"
    assert (photos / "image.jpg").read_bytes() == b"\x89PNG first"
    assert (photos / "image-1.jpg").read_bytes() == b"\x89PNG second"
    record = _read(tmp_path, "samples/TiPbO3/PTO54/record.json")
    assert [item["name"] for item in record["notion"]["properties"]["Photos"]["files"]] == ["image.jpg", "image-1.jpg"]
    assert [item["path"] for item in record["files"]] == ["samples/TiPbO3/PTO54/photos/image.jpg", "samples/TiPbO3/PTO54/photos/image-1.jpg"]


# --- CSV tables from the saved records ---


def _table(tmp_path, table):
    """The CSV the export command writes for `table`, read back as rows."""
    out = tmp_path / "exports"
    assert records_export.main(["--root", str(tmp_path / "files"), "--out", str(out)]) == 0
    data = (out / f"{table}.csv").read_bytes()
    assert data.startswith(b"\xef\xbb\xbf")
    return list(csv.DictReader(io.StringIO(data.decode("utf-8-sig"))))


def test_the_samples_table_has_one_row_per_sample_with_notion_values(tmp_path, api, client):
    _submit_sample(client)
    sample = api.page_named("PTO54")
    amended = _submit_sample(client, synthesis="Sol-gel", amend_existing="true", existing_sample_id=sample["id"], photos=(io.BytesIO(b"\x89PNG side"), "side.png"))
    assert amended.status_code == 200, amended.get_json()

    rows = _table(tmp_path, "samples")

    assert len(rows) == 1
    row = rows[0]
    assert list(row)[0] == "Name"
    assert row["Name"] == "PTO54"
    assert row["Archive Status"] == "Archive complete"
    assert row["Source"] == "Ada Lovelace"
    assert row["Synthesis"] == "Sol-gel"
    assert row["Photos"] == "front.png; side.png"
    assert row["notion_page_id"] == sample["id"]
    # The amendment's records (current/ and its revision hold the same content) win over the first record.
    assert row["record_path"].startswith(("samples/TiPbO3/PTO54/current/", "samples/TiPbO3/PTO54/revisions/"))


def test_the_results_and_sources_tables(tmp_path, api, client):
    _submit_sample(client)
    _submit_result(client)

    results = _table(tmp_path, "results")
    sources = _table(tmp_path, "sources")

    assert [row["Name"] for row in results] == ["XRD run 1"]
    assert "PTO54" in results[0].values()
    assert [row["Person"] for row in sources] == ["Ada Lovelace"]
    assert sources[0]["Email"] == "ada@utk.edu"


def test_the_website_offers_no_csv_downloads(client):
    # The tables are for whoever runs the server (python -m notion_sample_tracker.records_export), not for every user.
    assert "/exports/" not in client.get("/").get_data(as_text=True)
    assert client.get("/exports/samples.csv").status_code == 404
