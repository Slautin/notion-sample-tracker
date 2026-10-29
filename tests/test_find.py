"""The Find page: look up a sample or data entry as Notion has it now, and download it as PDF or JSON."""
import io
import json
from base64 import b64encode

from PIL import Image
from pypdf import PdfReader

import notion_sample_tracker.app as app_module

from notion_fakes import api, chunked_upload, client, submit_sample  # noqa: F401 (fixtures)


ENTRY = {"name": "XRD run 1", "entry_type": "XRD", "parent_entry": "sample", "parent_sample": "PTO54"}
NOTION_FILE_URL = "https://prod-files-secure.s3.us-west-2.amazonaws.com/space/front.png?X-Amz-Signature=abc"


def _create_entry(client, name="XRD run 1"):
    done = chunked_upload(client, "scan.csv", b"a,b\n", entry_name=name)
    fields = {**ENTRY, "name": name, "data_type": "file", "onedrive_path": done["url"], "char_data": '["XRD", "Raman"]', "submission_id": f"create-{name}"}
    response = client.post("/api/submit-data", data=fields)
    assert response.status_code == 200, response.get_json()
    return done


def _withdraw(client, name, kind):
    checked = client.post("/api/withdraw/check", json={"kind": kind, "name": name}).get_json()
    return client.post("/api/withdraw", json={"kind": kind, "name": name, "page_id": checked["page_id"], "reason": "wrong file", "submission_id": f"w-{name}"})


def _record(client, name, kind="sample"):
    response = client.get("/api/find/record", query_string={"kind": kind, "name": name})
    return response, response.get_json()


def _pdf_text(data):
    return "\n".join(page.extract_text() for page in PdfReader(io.BytesIO(data)).pages)


def _png():
    buffer = io.BytesIO()
    Image.new("RGB", (60, 40), (255, 130, 0)).save(buffer, format="PNG")
    return buffer.getvalue()


def test_the_start_page_links_the_find_page(client):
    assert 'href="/find"' in client.get("/").get_data(as_text=True)
    page = client.get("/find").get_data(as_text=True)
    assert "Find a record" in page
    assert "/api/find/list" in page


def test_the_list_has_every_record_and_marks_withdrawn_ones(api, client):
    submit_sample(client)
    _create_entry(client)
    _create_entry(client, "XRD run 2")
    _withdraw(client, "XRD run 2", "result")

    samples = client.get("/api/find/list", query_string={"kind": "sample"}).get_json()["records"]
    entries = client.get("/api/find/list", query_string={"kind": "result"}).get_json()["records"]

    assert samples == [{"name": "PTO54", "withdrawn": False}]
    assert entries == [{"name": "XRD run 1", "withdrawn": False}, {"name": "XRD run 2", "withdrawn": True}]
    assert client.get("/api/find/list", query_string={"kind": "people"}).status_code == 400


def test_a_sample_is_shown_with_its_fields_in_order_and_what_links_to_it(api, client):
    submit_sample(client, status="in work", processing="Anneal")
    _create_entry(client)
    _create_entry(client, "XRD run 2")
    _withdraw(client, "XRD run 2", "result")

    response, body = _record(client, "PTO54")

    assert response.status_code == 200, body
    record = body["record"]
    sample = api.page_named("PTO54")
    assert record["name"] == "PTO54" and record["kind"] == "sample" and record["status"] == "in work"
    assert record["notion_page_id"] == sample["id"] and record["notion_url"] == sample["url"]
    fields = record["fields"]
    assert list(fields)[:5] == ["Sample Type", "Status", "Composition", "Elements", "Synthesis"]
    assert fields["Composition"] == "TiPbO3"
    assert fields["Elements"] == ["Ti", "Pb", "O"]
    assert fields["Source"] == ["Ada Lovelace"]
    assert "Photos" not in fields and "QRCode" not in fields  # file names say nothing; photos appear in the PDF
    assert "Archive Error" in api.page_named("PTO54")["properties"]  # present in Notion, but empty
    assert "Submission ID" not in fields and "Archive Error" not in fields  # bookkeeping and empty fields are left out
    assert record["linked"]["Data entries"] == [
        {"id": api.page_named("XRD run 1")["id"], "name": "XRD run 1", "status": ""},
        {"id": api.page_named("XRD run 2")["id"], "name": "XRD run 2", "status": "withdrawn"},
    ]


def test_notions_system_fields_are_not_shown(api, client):
    submit_sample(client)
    bot = {"object": "user", "id": "bot-49018d41"}
    api.page_named("PTO54")["properties"].update(
        {
            "Created by": {"type": "created_by", "created_by": bot},
            "Last edited by": {"type": "last_edited_by", "last_edited_by": bot},
            "Last edited time": {"type": "last_edited_time", "last_edited_time": "2026-10-28T20:43:00.000Z"},
            "Files": {"type": "files", "files": [{"name": "notes.pdf", "type": "file", "file": {"url": NOTION_FILE_URL}}]},
        }
    )

    record = _record(client, "PTO54")[1]["record"]

    assert not {"Created by", "Last edited by", "Last edited time", "Files"} & set(record["fields"])
    assert record["created_time"] and record["last_edited_time"]  # the dates are shown once, as Created and Last edited


def test_the_page_lists_records_like_the_withdraw_page(client):
    page = client.get("/find").get_data(as_text=True)

    assert '<select id="record-name">' in page and "2. Which one?" in page
    assert 'type="search"' not in page
    # Links inside the record table only: a rule for every link in the box gave the dark button dark text.
    assert ".record td a { color: var(--slate); }" in page and ".record a {" not in page


def test_a_data_entry_is_shown_with_its_parent_and_link(api, client):
    submit_sample(client)
    done = _create_entry(client)

    _, body = _record(client, "XRD run 1", kind="result")

    fields = body["record"]["fields"]
    assert fields["Sample"] == ["PTO54"]
    assert fields["Characterisation"] == ["XRD", "Raman"]
    assert fields["Link"] == done["url"]
    assert list(fields)[0] == "Data Type"


def test_a_withdrawn_record_can_be_found(api, client):
    submit_sample(client)
    _withdraw(client, "PTO54", "sample")

    _, body = _record(client, "PTO54")

    assert body["record"]["withdrawn"] is True
    assert body["record"]["fields"]["Withdrawn Reason"] == "wrong file"


def test_names_are_looked_up_in_the_right_database_only(api, client):
    submit_sample(client)
    _create_entry(client)

    response, body = _record(client, "XRD run 1", kind="sample")

    assert response.status_code == 400
    assert "No sample named 'XRD run 1'" in body["error"]
    assert client.get("/api/find/pdf", query_string={"kind": "sample", "name": "XRD run 1"}).status_code == 404
    assert client.get("/api/find/json", query_string={"kind": "sample", "name": "nope"}).status_code == 404


def test_the_json_download_is_the_record(api, client):
    submit_sample(client, synthesis_details="grown at 600 °C, 5 µm film")

    response = client.get("/api/find/json", query_string={"kind": "sample", "name": "PTO54"})

    assert response.status_code == 200
    assert response.mimetype == "application/json"
    assert response.headers["Content-Disposition"] == 'attachment; filename="PTO54.json"'
    assert response.headers["Cache-Control"] == "private, no-store"
    assert "600 °C, 5 µm" in response.get_data(as_text=True)  # readable, not \\u escapes
    downloaded = json.loads(response.data)
    shown = _record(client, "PTO54")[1]["record"]
    assert {key: value for key, value in downloaded.items() if key != "exported_at"} == {key: value for key, value in shown.items() if key != "exported_at"}


def test_the_pdf_download_is_a_record_sheet(api, client):
    submit_sample(client, status="in work")
    _create_entry(client)

    response = client.get("/api/find/pdf", query_string={"kind": "sample", "name": "PTO54"})

    assert response.status_code == 200
    assert response.mimetype == "application/pdf"
    assert 'filename=PTO54.pdf' in response.headers["Content-Disposition"]
    text = _pdf_text(response.data)
    for expected in ("Sample - PTO54", "Sample record", "Record Details", "in work", "TiPbO3", "Ada Lovelace", "XRD run 1"):
        assert expected in text, expected
    assert "receipt" not in text.lower()


def test_the_pdf_shows_photos_stored_by_notion(api, client, monkeypatch):
    submit_sample(client)
    api.page_named("PTO54")["properties"]["Photos"]["files"][0]["file"]["url"] = NOTION_FILE_URL
    fetched = []
    monkeypatch.setattr(app_module, "_download_image", lambda url: fetched.append(url) or (_png(), "image/png"))

    response = client.get("/api/find/pdf", query_string={"kind": "sample", "name": "PTO54"})

    assert fetched == [NOTION_FILE_URL]
    assert "Photos" in _pdf_text(response.data)
    assert sum(len(page.images) for page in PdfReader(io.BytesIO(response.data)).pages) >= 1


def test_only_files_stored_by_notion_are_fetched_for_the_pdf(api, client, monkeypatch):
    submit_sample(client)
    photos = api.page_named("PTO54")["properties"]["Photos"]
    photos["files"] = [
        {"name": "typed link", "type": "external", "external": {"url": "https://intranet.example/secret.png"}},
        {"name": "plain http", "type": "file", "file": {"url": "http://prod-files-secure.s3.amazonaws.com/a.png"}},
        {"name": "other host", "type": "file", "file": {"url": "https://evil.example/a.png"}},
        {"name": "lookalike", "type": "file", "file": {"url": "https://amazonaws.com.evil.example/a.png"}},
    ]
    fetched = []
    monkeypatch.setattr(app_module, "_download_image", lambda url: fetched.append(url) or (_png(), "image/png"))

    response = client.get("/api/find/pdf", query_string={"kind": "sample", "name": "PTO54"})

    assert response.status_code == 200
    assert fetched == []


def test_a_photo_that_is_not_an_image_or_too_large_is_left_out(monkeypatch):
    class Response:
        def __init__(self, content_type, content, status=200):
            self.headers = {"Content-Type": content_type}
            self.status_code = status
            self.raw = io.BytesIO(content)
            self.raw.read = lambda size, decode_content=True, data=content: data[:size]

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    responses = iter([
        Response("text/html", b"<html>"),
        Response("image/png", b"x" * (app_module.MAX_PHOTO_BYTES + 1)),
        Response("image/png", b"png", status=403),
        Response("image/png; charset=binary", b"png"),
    ])
    calls = []
    monkeypatch.setattr(app_module.requests, "get", lambda url, **kwargs: calls.append(kwargs) or next(responses))

    results = [app_module._download_image("https://x.amazonaws.com/a.png") for _ in range(4)]

    assert results == [(b"", ""), (b"", ""), (b"", ""), (b"png", "image/png")]
    assert all(call["allow_redirects"] is False and call["timeout"] for call in calls)


def test_the_pdf_receipt_is_unchanged(api, client):
    from notion_sample_tracker.services.pdf_receipt import make_receipt_pdf

    text = _pdf_text(make_receipt_pdf("Sample Submission - PTO54", [("Record Type", "Sample"), ("Name", "PTO54"), ("Notion URL", "https://n.example/p")]))

    assert "Sample receipt" in text and "Submission Details" in text


def test_the_record_sheet_status_card_shows_the_records_status():
    from notion_sample_tracker.services.pdf_receipt import make_record_pdf

    text = _pdf_text(make_record_pdf("Sample - X", [("Record Type", "Sample record"), ("Name", "X"), ("Archive Status", "Archive complete")], status="withdrawn"))

    assert "withdrawn" in text and "Record Details" in text
