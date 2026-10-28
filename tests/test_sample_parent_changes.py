from pathlib import Path
from types import SimpleNamespace

import pytest

import notion_sample_tracker.app as app_module
from notion_sample_tracker.models import SampleForm
from notion_sample_tracker.services.formula import FormulaParser
from notion_sample_tracker.services.notion_client import NotionRepository
from notion_sample_tracker.services.onedrive_client import UploadResult
from notion_sample_tracker.settings import Settings


PAGE_ID = "aaaaaaaa-1111-2222-3333-444444444444"
PARENT_ID = "bbbbbbbb-1111-2222-3333-444444444444"
OTHER_ID = "cccccccc-1111-2222-3333-444444444444"


def _sub_sample_page(parent_id=PARENT_ID):
    return {
        "id": PAGE_ID,
        "url": "https://notion.example/PTO54-a",
        "properties": {
            "Name": {"type": "title", "title": [{"plain_text": "PTO54-a"}]},
            "Sample Type": {"type": "select", "select": {"name": "Sub-Sample"}},
            "Composition": {"type": "rich_text", "rich_text": []},
            "Parent Sample": {"type": "relation", "relation": [{"id": parent_id}] if parent_id else []},
            "Synthesis": {"type": "multi_select", "multi_select": []},
            "Processing": {"type": "multi_select", "multi_select": []},
        },
    }


def _form(parent="PTO54", sample_type="Sub-Sample"):
    return SampleForm(name="PTO54-a", sample_type=sample_type, parent_sample_id=parent, submission_id="sub-1")


def _resolver(mapping, calls=None):
    def resolve(value):
        if calls is not None:
            calls.append(value)
        if value in mapping:
            return mapping[value]
        raise ValueError(f"Could not find Notion page named '{value}'")

    return resolve


# --- change detection compares IDs, not a name against an ID ---


def test_parent_given_by_name_is_not_a_change_when_it_is_the_stored_parent():
    changes = app_module._sample_field_changes(_form("PTO54"), _sub_sample_page(), _resolver({"PTO54": PARENT_ID}))

    assert changes == []


def test_parent_given_as_an_id_is_not_a_change_even_without_a_resolver():
    assert app_module._sample_field_changes(_form(PARENT_ID), _sub_sample_page()) == []


@pytest.mark.parametrize("variant", [PARENT_ID.upper(), PARENT_ID.replace("-", ""), f" {PARENT_ID} "])
def test_id_comparison_ignores_dashes_case_and_spaces(variant):
    changes = app_module._sample_field_changes(_form("PTO54"), _sub_sample_page(), _resolver({"PTO54": variant}))

    assert changes == []


def test_a_different_parent_is_reported_with_both_ids():
    changes = app_module._sample_field_changes(_form("PTO99"), _sub_sample_page(), _resolver({"PTO99": OTHER_ID}))

    assert changes == [{"field": "Parent Sample", "old": PARENT_ID, "new": OTHER_ID}]


def test_a_parent_is_reported_when_the_sample_had_none():
    changes = app_module._sample_field_changes(_form("PTO54"), _sub_sample_page(parent_id=""), _resolver({"PTO54": PARENT_ID}))

    assert changes == [{"field": "Parent Sample", "old": "", "new": PARENT_ID}]


def test_an_unknown_parent_name_counts_as_a_change_instead_of_raising():
    changes = app_module._sample_field_changes(_form("Nope"), _sub_sample_page(), _resolver({}))

    assert changes == [{"field": "Parent Sample", "old": PARENT_ID, "new": "Nope"}]


def test_a_blank_parent_is_not_a_change_and_is_not_looked_up():
    calls = []

    changes = app_module._sample_field_changes(_form(""), _sub_sample_page(), _resolver({}, calls))

    assert changes == []
    assert calls == []


def test_root_samples_never_look_up_a_parent():
    calls = []
    page = _sub_sample_page()
    page["properties"]["Sample Type"] = {"type": "select", "select": {"name": "Root Sample"}}

    changes = app_module._sample_field_changes(_form("PTO54", sample_type="Root Sample"), page, _resolver({}, calls))

    assert changes == []
    assert calls == []


def test_the_parent_is_looked_up_once_per_check():
    calls = []

    app_module._sample_field_changes(_form("PTO54"), _sub_sample_page(), _resolver({"PTO54": PARENT_ID}, calls))

    assert calls == ["PTO54"]


class SubSampleNotion:
    def __init__(self, page, parents):
        self.page = page
        self.parents = parents

    def sample_page_by_name(self, name):
        return self.page

    def sample_page_by_submission(self, submission_id):
        return None

    def resolve_sample_page_id(self, value):
        return _resolver(self.parents)(value)


def test_duplicate_info_for_an_unchanged_sub_sample_has_no_changes():
    info = app_module._sample_duplicate_info(_form("PTO54"), SubSampleNotion(_sub_sample_page(), {"PTO54": PARENT_ID}))

    assert info is not None
    assert info["changes"] == []


def test_duplicate_info_reports_only_the_parent_when_it_changed():
    info = app_module._sample_duplicate_info(_form("PTO99"), SubSampleNotion(_sub_sample_page(), {"PTO99": OTHER_ID}))

    assert [item["field"] for item in info["changes"]] == ["Parent Sample"]


# --- NotionRepository.resolve_sample_page_id ---


def test_resolve_sample_page_id_passes_ids_through_and_looks_names_up():
    repo = NotionRepository(
        token="secret_test",
        samples_db="samples-db",
        results_db="results-db",
        people_db="people-db",
        formula_parser=FormulaParser(),
    )
    repo._find_by_any_title = lambda database_id, title: {"id": PARENT_ID} if (database_id, title) == ("samples-db", "PTO54") else None

    assert repo.resolve_sample_page_id(OTHER_ID) == OTHER_ID
    assert repo.resolve_sample_page_id(" PTO54 ") == PARENT_ID
    with pytest.raises(ValueError, match="Could not find"):
        repo.resolve_sample_page_id("Nope")


# --- the route: amending an unchanged sub-sample really is a no-op ---


def _settings(tmp_path) -> Settings:
    return Settings(
        app_secret_key="test-secret",
        public_base_url="http://localhost:8000",
        notion_home_url="",
        notion_token="secret_test",
        notion_samples_database_id="samples-db",
        notion_results_database_id="results-db",
        notion_people_database_id="people-db",
        onedrive_tenant_id="tenant-id",
        onedrive_client_id="client-id",
        onedrive_client_secret="client-secret",
        onedrive_auth_mode="client_credentials",
        onedrive_public_client=False,
        onedrive_drive_id="drive-id",
        onedrive_refresh_token="",
        onedrive_root_folder="SampleTracker",
        backlog_dir=Path(tmp_path),
        max_upload_mb=25,
    )


@pytest.fixture
def sample_client(tmp_path, monkeypatch):
    state = SimpleNamespace(
        page=_sub_sample_page(),
        parents={"PTO54": PARENT_ID, "PTO99": OTHER_ID},
        field_updates=[],
        uploads=[],
    )

    class StubNotion:
        def __init__(self, **kwargs):
            pass

        def sample_page_by_submission(self, submission_id):
            return None

        def sample_page_by_name(self, name):
            return state.page if name == "PTO54-a" else None

        def resolve_sample_page_id(self, value):
            return _resolver(state.parents)(value)

        def sample_storage_info_from_page(self, page):
            return {"folder": "samples/PTO54/PTO54-a"}

        def update_sample_fields(self, page_id, form, field_names):
            state.field_updates.append(list(field_names))
            return state.page

        def update_archive_status(self, page_id, status, error=""):
            return state.page

        def describe_relations(self, page):
            return {}, [], []

    class StubOneDrive:
        def __init__(self, **kwargs):
            pass

        def upload_json(self, relative_path, payload):
            state.uploads.append(relative_path)
            return UploadResult(path=f"SampleTracker/{relative_path}")

    monkeypatch.setattr(app_module, "NotionRepository", StubNotion)
    monkeypatch.setattr(app_module, "OneDriveClient", StubOneDrive)
    app = app_module.create_app(settings_factory=lambda: _settings(tmp_path))
    return app.test_client(), state


def _amend(client, parent):
    return client.post(
        "/api/submit",
        data={
            "sample_name": "PTO54-a",
            "sample_type": "Sub-Sample",
            "parent_sample": parent,
            "amend_existing": "true",
            "existing_sample_id": PAGE_ID,
        },
    )


@pytest.mark.parametrize("parent", ["PTO54", PARENT_ID])
def test_amending_an_unchanged_sub_sample_reports_no_changes(sample_client, parent):
    client, state = sample_client
    state.parents[PARENT_ID] = PARENT_ID

    response = _amend(client, parent)

    body = response.get_json()
    assert response.status_code == 200
    assert body["success"] is True
    assert body["no_changes"] is True
    assert state.field_updates == []
    assert state.uploads == []


def test_amending_to_a_different_parent_changes_only_the_parent(sample_client):
    client, state = sample_client

    response = _amend(client, "PTO99")

    body = response.get_json()
    assert response.status_code == 200
    assert body["amended"] is True
    assert body["changed_fields"] == [{"field": "Parent Sample", "old": PARENT_ID, "new": OTHER_ID}]
    assert state.field_updates == [["Parent Sample"]]
    assert state.uploads  # the revision records were archived to OneDrive
