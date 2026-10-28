"""The record.json files saved on the server match Notion after a submission, and the CSV export command reads them.

They run the real routes against the in-memory Notion in notion_fakes.py.
"""
import csv
import io
import json

import notion_sample_tracker.app as app_module
from notion_sample_tracker import records_export
from notion_sample_tracker.services.local_storage import LocalStorage

from notion_fakes import SOURCES, api, chunked_upload, client, read_record, submit_result, submit_sample  # noqa: F401 (fixtures)





# --- records match Notion ---


def test_a_sample_record_is_the_page_as_notion_holds_it_after_the_last_write(tmp_path, api, client):
    response = submit_sample(client)
    assert response.status_code == 200, response.get_json()

    record = read_record(tmp_path, "samples/TiPbO3/PTO54/record.json")
    notion_now = api.page_named("PTO54")
    assert record["notion"] == notion_now
    assert record["kind"] == "sample"
    assert record["captured_at"]
    # The writes that used to come after the snapshot are in it now.
    assert record["notion"]["properties"]["Archive Status"]["select"]["name"] == "Archive complete"
    assert [item["name"] for item in record["notion"]["properties"]["QRCode"]["files"]] == ["PTO54_qr.png"]
    assert [item["name"] for item in record["notion"]["properties"]["Photos"]["files"]] == ["front.png"]


def test_a_sample_record_names_its_sources_and_each_source_is_saved(tmp_path, api, client):
    submit_sample(client)

    record = read_record(tmp_path, "samples/TiPbO3/PTO54/record.json")
    person = api.page_named("Ada Lovelace")
    assert record["relations"]["Source"] == [{"id": person["id"], "name": "Ada Lovelace"}]
    assert "relations_error" not in record

    source = read_record(tmp_path, "sources/Ada_Lovelace/record.json")
    assert source["kind"] == "source"
    assert source["notion"] == person


def test_a_result_record_is_the_page_as_notion_holds_it_and_names_its_sample(tmp_path, api, client):
    submit_sample(client)
    response = submit_result(client)
    assert response.status_code == 200, response.get_json()
    assert "warning" not in response.get_json()

    record = read_record(tmp_path, "samples/TiPbO3/PTO54/results/XRD_run_1/record.json")
    notion_now = api.page_named("XRD run 1")
    assert record["notion"] == notion_now
    assert record["kind"] == "result"
    assert api.status_of("XRD run 1") == "Archive complete"
    sample = api.page_named("PTO54")
    assert {"id": sample["id"], "name": "PTO54"} in [item for items in record["relations"].values() for item in items]


def test_an_amendment_leaves_current_and_revision_records_matching_notion(tmp_path, api, client):
    submit_sample(client)
    sample = api.page_named("PTO54")

    response = submit_sample(client, synthesis="Sol-gel", amend_existing="true", existing_sample_id=sample["id"], photos=(io.BytesIO(b"\x89PNG side"), "side.png"))
    body = response.get_json()
    assert response.status_code == 200, body
    assert body["amended"] is True

    notion_now = api.page_named("PTO54")
    current = read_record(tmp_path, "samples/TiPbO3/PTO54/current/record.json")
    revision = read_record(tmp_path, f"samples/TiPbO3/PTO54/revisions/{body['revision_id']}/record.json")
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
    submit_sample(client)

    assert seen == [True]


def test_a_failed_final_record_write_marks_the_notion_page_failed(tmp_path, api, client, monkeypatch):
    _failing_json_writes(monkeypatch, "PTO54/record.json", from_call=2)

    response = submit_sample(client)

    assert response.status_code == 400
    assert api.status_of("PTO54") == "Failed archive"
    # The first record is still there, so the files are not orphaned.
    assert read_record(tmp_path, "samples/TiPbO3/PTO54/record.json")["notion"]["id"] == api.page_named("PTO54")["id"]


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
    response = submit_sample(client)

    assert response.status_code == 200
    assert "photo disk full" in response.get_json()["warning"]
    record = read_record(tmp_path, "samples/TiPbO3/PTO54/record.json")
    assert record["notion"] == api.page_named("PTO54")
    assert record["notion"]["properties"]["Archive Status"]["select"]["name"] == "Failed archive"
    assert "photo disk full" in record["notion"]["properties"]["Archive Error"]["rich_text"][0]["plain_text"]


def test_a_failed_result_record_write_marks_the_notion_page_failed(tmp_path, api, client, monkeypatch):
    submit_sample(client)
    _failing_json_writes(monkeypatch, "XRD_run_1/record.json")

    response = submit_result(client)

    assert response.status_code == 200  # the entry is in Notion; the archive part is reported
    assert "snapshot_upload: disk went away" in response.get_json()["warning"]
    assert api.status_of("XRD run 1") == "Failed archive"


def test_a_failed_amendment_record_write_marks_the_notion_page_failed(tmp_path, api, client, monkeypatch):
    submit_sample(client)
    sample = api.page_named("PTO54")
    # current/record.json is written last, after Notion was told the archive is complete.
    _failing_json_writes(monkeypatch, "current/record.json")

    response = submit_sample(client, synthesis="Sol-gel", amend_existing="true", existing_sample_id=sample["id"])

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
    response = submit_sample(client)
    assert response.status_code == 200, response.get_json()

    record = read_record(tmp_path, "samples/TiPbO3/PTO54/record.json")
    person_id = api.page_named("Ada Lovelace")["id"]
    assert record["relations"]["Source"] == [{"id": person_id, "name": ""}]
    assert "Source" in record["relations_error"]
    assert not (tmp_path / "files/sources").exists()


# --- uploaded files are never overwritten ---


def test_uploaded_files_named_like_the_app_record_and_qr_code_are_kept(tmp_path, api, client):
    submit_sample(client)
    mine = {
        "record.json": b'{"instrument": "my own record"}',
        "Record.JSON": b'{"instrument": "upper case"}',
        "XRD_run_1_qr.png": b"my own QR picture",
    }
    names = {filename: chunked_upload(client, filename, content)["name"] for filename, content in mine.items()}

    response = submit_result(client)
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
    record = read_record(tmp_path, "samples/TiPbO3/PTO54/record.json")
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
    submit_sample(client)
    sample = api.page_named("PTO54")
    amended = submit_sample(client, synthesis="Sol-gel", amend_existing="true", existing_sample_id=sample["id"], photos=(io.BytesIO(b"\x89PNG side"), "side.png"))
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
    submit_sample(client)
    submit_result(client)

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
