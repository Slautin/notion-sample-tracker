"""Withdrawing a sample or data entry: an amendment with a reason. Nothing is deleted."""
import io

import notion_sample_tracker.app as app_module
from notion_sample_tracker.schema_contract import validate_tracker_schema

from notion_fakes import api, chunked_upload, client, read_record, settings_for, submit_sample  # noqa: F401 (fixtures)


ENTRY = {"name": "XRD run 1", "entry_type": "XRD", "parent_entry": "sample", "parent_sample": "PTO54"}


def _text(page, name):
    return "".join(item["plain_text"] for item in page["properties"].get(name, {}).get("rich_text", []))


def _status(page):
    return (page["properties"].get("Status", {}).get("select") or {}).get("name")


def _withdraw_sample(client, api, **fields):
    sample = api.page_named("PTO54")
    return submit_sample(client, amend_existing="true", existing_sample_id=sample["id"], photos=(io.BytesIO(b""), ""), **fields)


def _create_entry(client, **fields):
    done = chunked_upload(client, "scan.csv", b"a,b\n")
    response = client.post("/api/submit-data", data={**ENTRY, "data_type": "file", "onedrive_path": done["url"], "submission_id": "create", **fields})
    assert response.status_code == 200, response.get_json()
    return done


def _amend_entry(client, api, **fields):
    entry = api.page_named("XRD run 1")
    return client.post("/api/submit-data", data={**ENTRY, "amend_existing": "true", "existing_result_id": entry["id"], "submission_id": "amend", **fields})


def test_a_sample_is_withdrawn_with_its_reason_and_keeps_everything_else(tmp_path, api, client):
    submit_sample(client, status="in work")

    response = _withdraw_sample(client, api, status="withdrawn", withdrawn_reason="used up in the anneal series")

    body = response.get_json()
    assert response.status_code == 200, body
    assert [item["field"] for item in body["changed_fields"]] == ["Status", "Withdrawn Reason"]
    sample = api.page_named("PTO54")
    assert _status(sample) == "withdrawn"
    assert _text(sample, "Withdrawn Reason") == "used up in the anneal series"
    assert [item["name"] for item in sample["properties"]["Photos"]["files"]] == ["front.png"]
    assert read_record(tmp_path, "samples/TiPbO3/PTO54/current/record.json")["notion"] == sample
    assert ["Withdrawn Reason", "used up in the anneal series"] in body["receipt"]["rows"]


def test_withdrawing_needs_a_reason(api, client):
    submit_sample(client, status="in work")

    checked = client.post("/api/validate-sample", data={"sample_name": "PTO54", "sample_type": "Root Sample", "composition": "PbTiO3", "status": "withdrawn"})
    submitted = _withdraw_sample(client, api, status="withdrawn", withdrawn_reason="  ")

    for response in (checked, submitted):
        assert response.status_code == 400
        assert "reason" in response.get_json()["error"]
    assert _status(api.page_named("PTO54")) == "in work"


def test_a_new_sample_cannot_start_withdrawn(api, client):
    response = submit_sample(client, status="withdrawn", withdrawn_reason="mistake")

    assert response.status_code == 400
    assert "Only an existing sample can be withdrawn" in response.get_json()["error"]
    assert api.titles("samples-db") == []


def test_a_reason_is_ignored_unless_the_status_is_withdrawn(api, client):
    submit_sample(client, status="in work")

    response = _withdraw_sample(client, api, status="waiting", withdrawn_reason="typed by accident")

    assert [item["field"] for item in response.get_json()["changed_fields"]] == ["Status"]
    assert _text(api.page_named("PTO54"), "Withdrawn Reason") == ""


def test_a_withdrawn_sample_can_be_reinstated_and_keeps_the_reason_on_record(api, client):
    submit_sample(client, status="in work")
    _withdraw_sample(client, api, status="withdrawn", withdrawn_reason="thought lost")

    response = _withdraw_sample(client, api, status="in work")

    assert [item["field"] for item in response.get_json()["changed_fields"]] == ["Status"]
    sample = api.page_named("PTO54")
    assert _status(sample) == "in work"
    assert _text(sample, "Withdrawn Reason") == "thought lost"


def test_a_data_entry_is_withdrawn_and_keeps_its_data(api, client):
    submit_sample(client)
    done = _create_entry(client, status="active")
    assert _status(api.page_named("XRD run 1")) == "active"

    response = _amend_entry(client, api, status="withdrawn", withdrawn_reason="wrong scan file")

    body = response.get_json()
    assert response.status_code == 200, body
    assert [item["field"] for item in body["changed_fields"]] == ["Status", "Withdrawn Reason"]
    entry = api.page_named("XRD run 1")
    assert _status(entry) == "withdrawn"
    assert _text(entry, "Withdrawn Reason") == "wrong scan file"
    assert entry["properties"]["Link"]["url"] == done["url"]


def test_a_new_data_entry_cannot_start_withdrawn(api, client):
    submit_sample(client)

    response = client.post("/api/validate-result", data={**ENTRY, "data_type": "link", "data_link": "https://example.org/a", "status": "withdrawn", "withdrawn_reason": "x"})

    assert response.status_code == 400
    assert "Only an existing data entry can be withdrawn" in response.get_json()["error"]


def test_fields_not_yet_added_in_notion_are_named_before_anything_is_uploaded(api, client, tmp_path):
    submit_sample(client, status="in work")
    api.missing_properties["results-db"] |= {"Status", "Withdrawn Reason"}
    api.missing_properties["samples-db"] |= {"Withdrawn Reason"}
    client = app_module.create_app(settings_factory=lambda: settings_for(tmp_path)).test_client()  # fresh schema cache

    entry = client.post("/api/validate-result", data={**ENTRY, "data_type": "link", "data_link": "https://example.org/a", "status": "active"})
    sample = client.post(
        "/api/validate-sample",
        data={"sample_name": "PTO54", "sample_type": "Root Sample", "composition": "PbTiO3", "status": "withdrawn", "withdrawn_reason": "used up"},
    )

    assert entry.status_code == 400
    assert "The Results database in Notion has no 'Status' field yet" in entry.get_json()["error"]
    assert sample.status_code == 400
    assert "The Samples database in Notion has no 'Withdrawn Reason' field yet" in sample.get_json()["error"]


def test_entries_without_a_status_still_work_before_the_field_exists(api, client, tmp_path):
    submit_sample(client)
    api.missing_properties["results-db"] |= {"Status", "Withdrawn Reason"}
    client = app_module.create_app(settings_factory=lambda: settings_for(tmp_path)).test_client()

    _create_entry(client)

    assert "Status" not in api.page_named("XRD run 1")["properties"]


def test_the_schema_check_asks_for_the_withdrawn_fields():
    issues = validate_tracker_schema({"Samples": {"properties": {"Status": {"type": "select", "select": {"options": [{"name": "in work"}]}}}}, "Results": {}, "People": {}})

    found = {(issue.database, issue.property_name, issue.message) for issue in issues}
    assert ("Samples", "Status", "missing options: withdrawn") in found
    assert ("Samples", "Withdrawn Reason", "missing property") in found
    assert ("Results", "Status", "missing property") in found
    assert ("Results", "Withdrawn Reason", "missing property") in found
