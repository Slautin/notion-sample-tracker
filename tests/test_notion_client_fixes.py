import pytest

from notion_sample_tracker.models import SampleForm
from notion_sample_tracker.services.formula import FormulaParser
from notion_sample_tracker.services.notion_client import NotionRepository


PAGE_ID = "aaaaaaaa-1111-2222-3333-444444444444"
OTHER_ID = "bbbbbbbb-1111-2222-3333-444444444444"


class FakePages:
    def __init__(self, page, events):
        self.page = page
        self.events = events
        self.updates = []

    def retrieve(self, page_id):
        self.events.append("retrieve")
        return self.page

    def update(self, page_id, properties):
        self.events.append("update")
        self.updates.append((page_id, properties))
        return {"id": page_id, "properties": properties}


class FakeClient:
    def __init__(self, page, events):
        self.pages = FakePages(page, events)


def _repo(page=None):
    events = []
    repo = NotionRepository(
        token="secret_test",
        samples_db="samples-db",
        results_db="results-db",
        people_db="people-db",
        formula_parser=FormulaParser(),
    )
    repo.client = FakeClient(page if page is not None else {"id": PAGE_ID, "properties": {}}, events)
    repo.events = events

    def fake_upload(name, content, content_type):
        events.append("upload")
        return f"upload-{name}"

    repo._upload_file_to_notion = fake_upload
    return repo


def _page_with_photos(*items):
    return {"id": PAGE_ID, "properties": {"Photos": {"type": "files", "files": list(items)}}}


def _hosted(name, url="https://s3.example/photo.png?sig=1", expiry="2026-10-25T23:00:00.000Z"):
    file = {"url": url}
    if expiry:
        file["expiry_time"] = expiry
    return {"name": name, "type": "file", "file": file}


def _external(name, url="https://example.com/photo.png"):
    return {"name": name, "type": "external", "external": {"url": url}}


def _new(name, content=b"bytes"):
    return {"name": name, "content": content, "content_type": "image/png"}


def _sent_files(repo):
    page_id, properties = repo.client.pages.updates[-1]
    assert page_id == PAGE_ID
    return properties["Photos"]["files"]


# --- attach_uploaded_files must not wipe existing Photos ---


def test_new_photos_are_added_after_existing_hosted_photos():
    repo = _repo(_page_with_photos(_hosted("old.png")))

    repo.attach_uploaded_files(PAGE_ID, "Photos", [_new("new.png")])

    assert _sent_files(repo) == [
        _hosted("old.png"),
        {"name": "new.png", "type": "file_upload", "file_upload": {"id": "upload-new.png"}},
    ]


def test_existing_external_photos_are_kept_too():
    repo = _repo(_page_with_photos(_external("linked.png"), _hosted("old.png")))

    repo.attach_uploaded_files(PAGE_ID, "Photos", [_new("new.png")])

    assert [item["name"] for item in _sent_files(repo)] == ["linked.png", "old.png", "new.png"]


def test_reuploading_a_photo_with_the_same_name_replaces_it_instead_of_duplicating():
    repo = _repo(_page_with_photos(_hosted("a.png"), _hosted("b.png")))

    repo.attach_uploaded_files(PAGE_ID, "Photos", [_new("a.png")])

    files = _sent_files(repo)
    assert [item["name"] for item in files] == ["b.png", "a.png"]
    assert files[-1]["type"] == "file_upload"


def test_first_photos_on_an_empty_property():
    repo = _repo(_page_with_photos())

    repo.attach_uploaded_files(PAGE_ID, "Photos", [_new("one.png"), _new("two.png")])

    assert [item["name"] for item in _sent_files(repo)] == ["one.png", "two.png"]


def test_existing_files_are_read_after_uploading_so_hosted_urls_are_fresh():
    repo = _repo(_page_with_photos(_hosted("old.png")))

    repo.attach_uploaded_files(PAGE_ID, "Photos", [_new("new.png")])

    assert repo.events == ["upload", "retrieve", "update"]


def test_nothing_to_attach_makes_no_notion_calls():
    repo = _repo(_page_with_photos(_hosted("old.png")))

    assert repo.attach_uploaded_files(PAGE_ID, "Photos", []) is None
    assert repo.events == []


def test_hosted_file_without_expiry_time_is_sent_back_without_one():
    repo = _repo(_page_with_photos(_hosted("old.png", expiry=None)))

    repo.attach_uploaded_files(PAGE_ID, "Photos", [_new("new.png")])

    assert _sent_files(repo)[0] == {"name": "old.png", "type": "file", "file": {"url": "https://s3.example/photo.png?sig=1"}}


def test_the_other_attach_helpers_no_longer_drop_hosted_files():
    repo = _repo(_page_with_photos(_hosted("old.png")))

    repo.attach_external_file(PAGE_ID, "Photos", "extra.png", "https://example.com/extra.png")

    assert [item["name"] for item in _sent_files(repo)] == ["old.png", "extra.png"]


# --- amending a sample must not crash on Parent Sample ---


def _sub_sample_form(parent):
    return SampleForm(name="PTO54-a", sample_type="Sub-Sample", parent_sample_id=parent, submission_id="sub-1")


def test_amending_parent_sample_by_id_sets_the_relation():
    repo = _repo()

    repo.update_sample_fields(PAGE_ID, _sub_sample_form(OTHER_ID), ["Parent Sample"])

    page_id, properties = repo.client.pages.updates[-1]
    assert page_id == PAGE_ID
    assert properties["Parent Sample"] == {"relation": [{"id": OTHER_ID}]}
    assert properties["Submission ID"]["rich_text"][0]["text"]["content"] == "sub-1"


def test_amending_parent_sample_by_name_resolves_it():
    repo = _repo()
    repo._find_by_any_title = lambda database_id, title: {"id": OTHER_ID} if title == "PTO54" else None

    repo.update_sample_fields(PAGE_ID, _sub_sample_form("PTO54"), ["Parent Sample", "Status"])

    _, properties = repo.client.pages.updates[-1]
    assert properties["Parent Sample"] == {"relation": [{"id": OTHER_ID}]}


def test_clearing_the_parent_sample_empties_the_relation():
    repo = _repo()

    repo.update_sample_fields(PAGE_ID, _sub_sample_form(""), ["Parent Sample"])

    _, properties = repo.client.pages.updates[-1]
    assert properties["Parent Sample"] == {"relation": []}


@pytest.mark.parametrize("parent", [PAGE_ID, PAGE_ID.upper(), PAGE_ID.replace("-", "")])
def test_a_sample_cannot_become_its_own_parent(parent):
    repo = _repo()
    repo._resolve_page_id = lambda database_id, value: value

    with pytest.raises(ValueError, match="itself as Parent Sample"):
        repo.update_sample_fields(PAGE_ID, _sub_sample_form(parent), ["Parent Sample"])

    assert repo.client.pages.updates == []


def test_a_parent_name_that_resolves_to_the_sample_itself_is_rejected():
    repo = _repo()
    repo._find_by_any_title = lambda database_id, title: {"id": PAGE_ID}

    with pytest.raises(ValueError, match="itself as Parent Sample"):
        repo.update_sample_fields(PAGE_ID, _sub_sample_form("PTO54-a"), ["Parent Sample"])

    assert repo.client.pages.updates == []
