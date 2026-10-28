from io import BytesIO
from pathlib import Path

from werkzeug.datastructures import FileStorage

from notion_sample_tracker.app import _archive_sample_photos
from notion_sample_tracker.services.onedrive_client import UploadResult
from notion_sample_tracker.settings import Settings


class FakeNotion:
    def __init__(self):
        self.attached = []

    def attach_uploaded_files(self, page_id, property_name, files):
        self.attached.append((page_id, property_name, files))


class FailingOneDrive:
    def upload_bytes(self, relative_path, content, content_type):
        raise TimeoutError("graph upload timed out")


class WorkingOneDrive:
    def upload_bytes(self, relative_path, content, content_type):
        return UploadResult(path=f"SampleTracker/{relative_path}", web_url="https://example.com/photo")


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


def _photo(name="sample.png", content=b"image bytes") -> FileStorage:
    return FileStorage(stream=BytesIO(content), filename=name, content_type="image/png")


def test_sample_photo_upload_failure_returns_warning(tmp_path):
    notion = FakeNotion()

    uploaded, errors = _archive_sample_photos(notion, FailingOneDrive(), "page-id", "samples/S1/photos", [_photo()], _settings(tmp_path))

    assert uploaded == []
    assert errors
    assert "photo_upload:sample.png" in errors[0]
    assert notion.attached == []


def test_sample_photo_upload_success_attaches_to_notion(tmp_path):
    notion = FakeNotion()

    uploaded, errors = _archive_sample_photos(notion, WorkingOneDrive(), "page-id", "samples/S1/photos", [_photo()], _settings(tmp_path))

    assert errors == []
    assert uploaded[0]["path"] == "SampleTracker/samples/S1/photos/sample.png"
    assert notion.attached[0][0] == "page-id"
    assert notion.attached[0][1] == "Photos"
    assert notion.attached[0][2][0]["content"] == b"image bytes"
    assert notion.attached[0][2][0]["content_type"] == "image/png"


def test_photos_with_the_same_name_get_their_own_names_on_the_server_and_in_notion(tmp_path):
    notion = FakeNotion()
    photos = [_photo("image.jpg", b"one"), _photo("image.jpg", b"two"), _photo("IMAGE.JPG", b"three"), _photo("other.jpg", b"four")]

    uploaded, errors = _archive_sample_photos(notion, WorkingOneDrive(), "page-id", "samples/S1/photos", photos, _settings(tmp_path))

    assert errors == []
    assert [item["name"] for item in uploaded] == ["image.jpg", "image-1.jpg", "IMAGE-2.jpg", "other.jpg"]
    assert [item["name"] for item in notion.attached[0][2]] == ["image.jpg", "image-1.jpg", "IMAGE-2.jpg", "other.jpg"]
    assert [item["content"] for item in notion.attached[0][2]] == [b"one", b"two", b"three", b"four"]
