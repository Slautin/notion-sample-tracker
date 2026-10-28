import base64

import pytest

import notion_sample_tracker.app as app_module
from notion_sample_tracker.settings import Settings


SAMPLES = "https://www.notion.so/camm/Samples-1111?v=aaaa"
RESULTS = "https://www.notion.so/camm/Results-2222?v=bbbb"
SOURCES = "https://www.notion.so/camm/People-3333"
HOME = "https://www.notion.so/camm-sample-tracking/Sample-Tracking-2866e75c45cb80c099f1e040193b9ed1"


def _settings(tmp_path, **overrides) -> Settings:
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
    )
    values.update(overrides)
    return Settings(**values)


@pytest.fixture(autouse=True)
def no_home_url_in_the_environment(monkeypatch):
    # The workspace link is also read from these variables at request time.
    monkeypatch.delenv("NOTION_HOME_URL", raising=False)
    monkeypatch.delenv("NOTION_PAGE", raising=False)


def _page(tmp_path, path="/", **overrides):
    client = app_module.create_app(settings_factory=lambda: _settings(tmp_path, **overrides)).test_client()
    return client.get(path)


def test_the_start_page_is_shown_instead_of_redirecting_to_notion(tmp_path):
    response = _page(tmp_path, notion_home_url=HOME)

    assert response.status_code == 200
    assert "Location" not in response.headers
    page = response.get_data(as_text=True)
    assert "<h1>Center for Advanced Materials & Manufacturing</h1>" in page
    assert '<h1 class="page-title">Sample Tracking</h1>' in page


def test_the_add_buttons_open_the_apps_own_forms(tmp_path):
    page = _page(tmp_path).get_data(as_text=True)

    assert '<a class="btn btn-primary" href="/samples/new">+ Add sample</a>' in page
    assert '<a class="btn btn-primary" href="/results/new">+ Add results</a>' in page


def test_the_database_buttons_open_notion_in_a_new_tab_in_order(tmp_path):
    page = _page(tmp_path, notion_samples_url=SAMPLES, notion_results_url=RESULTS, notion_sources_url=SOURCES).get_data(as_text=True)

    for label, url in (("Samples", SAMPLES), ("Results", RESULTS), ("Sources", SOURCES)):
        assert f'<a class="btn btn-secondary" href="{url}" target="_blank" rel="noopener noreferrer">{label} ' in page
    assert page.index(SAMPLES) < page.index(RESULTS) < page.index(SOURCES)


def test_a_database_button_without_a_link_is_left_out(tmp_path):
    page = _page(tmp_path, notion_results_url=RESULTS).get_data(as_text=True)

    assert f'href="{RESULTS}"' in page
    assert ">Samples <span" not in page
    assert ">Sources <span" not in page


def test_without_any_database_link_the_notion_section_is_hidden(tmp_path):
    page = _page(tmp_path).get_data(as_text=True)

    assert "Browse in Notion" not in page
    assert 'class="btn btn-secondary"' not in page
    assert "+ Add sample" in page


def test_the_notion_workspace_link_comes_from_notion_home_url(tmp_path, monkeypatch):
    assert "Open the Notion workspace" not in _page(tmp_path).get_data(as_text=True)

    page = _page(tmp_path, notion_home_url=HOME).get_data(as_text=True)
    assert f'<a href="{HOME}" target="_blank" rel="noopener noreferrer">Open the Notion workspace ↗</a>' in page

    monkeypatch.setenv("NOTION_HOME_URL", HOME)  # the environment variable still works, as before
    assert f'href="{HOME}"' in _page(tmp_path).get_data(as_text=True)


def test_links_are_escaped_so_a_bad_value_cannot_inject_markup(tmp_path):
    nasty = 'https://www.notion.so/x" onmouseover="alert(1)'

    page = _page(tmp_path, notion_samples_url=nasty).get_data(as_text=True)

    assert 'onmouseover="alert(1)"' not in page
    assert "&#34; onmouseover=&#34;alert(1)" in page


def test_the_start_page_is_behind_the_site_password_like_everything_else(tmp_path):
    client = app_module.create_app(
        settings_factory=lambda: _settings(tmp_path, access_password="correct horse battery")
    ).test_client()
    login = {"Authorization": "Basic " + base64.b64encode(b"lab:correct horse battery").decode()}

    assert client.get("/").status_code == 401
    assert client.get("/", headers=login).status_code == 200


@pytest.mark.parametrize("path", ["/samples/new", "/results/new", "/add_sample", "/add_results"])
def test_both_forms_link_back_to_the_start_page(tmp_path, path):
    page = _page(tmp_path, path).get_data(as_text=True)

    assert '<a class="home-link" href="/">← Home</a>' in page
