"""An in-memory Notion for tests that run the real routes and the real NotionRepository.

Records and amendments are then compared with what Notion itself would hold, not with what a stub returned.
"""
import copy
import io
import json

import pytest

import notion_sample_tracker.app as app_module
from notion_sample_tracker.schema_contract import TRACKER_SCHEMA
from notion_sample_tracker.services.local_storage import LocalStorage
from notion_sample_tracker.services.notion_client import NotionRepository
from notion_sample_tracker.settings import Settings


TITLES = {"samples-db": "Name", "results-db": "Name", "people-db": "Person"}
DATABASES = {"samples-db": "Samples", "results-db": "Results", "people-db": "People"}
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
        # Fields a test takes out of a database, as if nobody had added them in Notion yet.
        self.missing_properties: dict[str, set[str]] = {database_id: set() for database_id in DATABASES}
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
            # The database as the app's schema contract describes it.
            properties = {
                requirement.name: {"type": requirement.notion_type, requirement.notion_type: {"options": [{"name": option} for option in requirement.options]}}
                for requirement in TRACKER_SCHEMA[DATABASES[data_source_id]]
                if requirement.name not in self.missing_properties[data_source_id]
            }
            return {"properties": properties}
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

    def titles(self, database_id):
        return sorted(_title(page) for page in self.store.values() if page["parent"].get("data_source_id") == database_id)

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


def settings_for(tmp_path, **overrides):
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
    return app_module.create_app(settings_factory=lambda: settings_for(tmp_path)).test_client()


def submit_sample(client, **fields):
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


def chunked_upload(client, filename, content, entry_name="XRD run 1"):
    started = client.post("/api/uploads", json={"filename": filename, "size": len(content), "entry_name": entry_name, "parent_sample": "PTO54"}).get_json()
    client.put(f"/api/uploads/{started['upload_id']}", data=content, headers={"Upload-Offset": "0"})
    return client.post(f"/api/uploads/{started['upload_id']}/complete").get_json()


def submit_result(client, name="XRD run 1", content=b"a,b\n1,2\n"):
    done = chunked_upload(client, "scan.csv", content, entry_name=name)
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


def read_record(tmp_path, relative):
    return json.loads((tmp_path / "files" / relative).read_text())


def failing_json_writes(monkeypatch, path_suffix, from_call=1):
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
