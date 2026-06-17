from pathlib import Path

from notion_sample_tracker import create_app
from notion_sample_tracker.settings import Settings


def test_create_app_registers_core_routes(tmp_path):
    def fake_settings() -> Settings:
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

    app = create_app(settings_factory=fake_settings)

    assert app.config["SECRET_KEY"] == "test-secret"
    assert app.config["MAX_CONTENT_LENGTH"] == 25 * 1024 * 1024

    client = app.test_client()
    assert client.get("/samples/new").status_code == 200
    assert client.get("/results/new").status_code == 200
    assert client.get("/healthz").status_code == 200
    assert client.get("/backlog").status_code == 404
    assert client.post("/api/create-upload-session", json={"filename": "bad.exe"}).status_code == 400
