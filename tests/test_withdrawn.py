"""Withdrawing on the Withdraw page (a name and a reason), and amending to make a record active again."""
import io
from pathlib import Path

import notion_sample_tracker.app as app_module
from notion_sample_tracker.schema_contract import validate_tracker_schema

from notion_fakes import api, chunked_upload, client, read_record, settings_for, submit_sample  # noqa: F401 (fixtures)


ENTRY = {"name": "XRD run 1", "entry_type": "XRD", "parent_entry": "sample", "parent_sample": "PTO54"}
SAMPLE_FOLDER = "samples/TiPbO3/PTO54"
ENTRY_FOLDER = f"{SAMPLE_FOLDER}/results/XRD_run_1"


def _text(page, name):
    return "".join(item["plain_text"] for item in page["properties"].get(name, {}).get("rich_text", []))


def _status(page):
    return (page["properties"].get("Status", {}).get("select") or {}).get("name")


def _create_entry(client, **fields):
    done = chunked_upload(client, "scan.csv", b"a,b\n", entry_name=fields.get("name", "XRD run 1"))
    data = {**ENTRY, "data_type": "file", "onedrive_path": done["url"], "submission_id": f"create-{fields.get('name', 'XRD run 1')}", **fields}
    response = client.post("/api/submit-data", data=data)
    assert response.status_code == 200, response.get_json()
    return done


def _check(client, name, kind="sample"):
    return client.post("/api/withdraw/check", json={"kind": kind, "name": name})


def _withdraw(client, name, kind="sample", reason="used up", submission_id="withdraw-1", **overrides):
    checked = _check(client, name, kind).get_json()
    payload = {"kind": kind, "name": name, "page_id": checked.get("page_id"), "reason": reason, "submission_id": submission_id, **overrides}
    return client.post("/api/withdraw", json=payload)


# --- the Withdraw page ---


def test_the_start_page_links_the_withdraw_page(client):
    assert 'href="/withdraw"' in client.get("/").get_data(as_text=True)
    page = client.get("/withdraw").get_data(as_text=True)
    assert "Withdraw a record" in page
    assert "/api/withdraw/check" in page


def test_the_check_shows_the_record_and_what_stays_linked_to_it(api, client):
    submit_sample(client, status="in work")
    _create_entry(client)
    _create_entry(client, name="XRD run 2")
    _withdraw(client, "XRD run 2", kind="result", submission_id="withdraw-run-2")  # withdrawn: not listed
    submit_sample(client, sample_name="PTO54-a", sample_type="Sub-Sample", composition="", parent_sample="PTO54", photos=(io.BytesIO(b""), ""))

    body = _check(client, "PTO54").get_json()

    assert body["page_id"] == api.page_named("PTO54")["id"]
    assert body["withdrawn"] is False
    assert ["Composition", "TiPbO3"] in body["details"]
    assert ["Status", "in work"] in body["details"]
    assert body["linked"] == {
        "Data entries": [{"id": api.page_named("XRD run 1")["id"], "name": "XRD run 1", "status": ""}],
        "Sub-samples": [{"id": api.page_named("PTO54-a")["id"], "name": "PTO54-a", "status": ""}],
    }


def test_the_check_refuses_unknown_records_and_kinds(api, client):
    assert "No sample named 'nope'" in _check(client, "nope").get_json()["error"]
    assert _check(client, "PTO54", kind="person").status_code == 400


# --- withdrawing ---


def test_a_sample_is_withdrawn_with_only_its_name_and_a_reason(tmp_path, api, client):
    submit_sample(client, status="in work")
    before = api.page_named("PTO54")

    response = _withdraw(client, "PTO54", reason="used up in the anneal series")

    body = response.get_json()
    assert response.status_code == 200, body
    sample = api.page_named("PTO54")
    assert _status(sample) == "withdrawn"
    assert _text(sample, "Withdrawn Reason") == "used up in the anneal series"
    for name in ("Composition", "Sample Type", "Synthesis", "Photos", "Source"):
        assert sample["properties"][name] == before["properties"][name]
    current = read_record(tmp_path, f"{SAMPLE_FOLDER}/current/record.json")
    assert current["notion"] == sample
    assert current["reason"] == "used up in the anneal series"
    assert read_record(tmp_path, f"{SAMPLE_FOLDER}/revisions/{body['revision_id']}/record.json") == current
    assert ["Reason", "used up in the anneal series"] in body["receipt"]["rows"]
    assert ["Previous Status", "in work"] in body["receipt"]["rows"]


def test_a_data_entry_is_withdrawn_in_its_own_folder(tmp_path, api, client):
    submit_sample(client)
    done = _create_entry(client)

    response = _withdraw(client, "XRD run 1", kind="result", reason="wrong scan file")

    assert response.status_code == 200, response.get_json()
    entry = api.page_named("XRD run 1")
    assert _status(entry) == "withdrawn"
    assert entry["properties"]["Link"]["url"] == done["url"]
    assert read_record(tmp_path, f"{ENTRY_FOLDER}/current/record.json")["notion"] == entry


def test_a_data_entry_under_a_dataset_is_withdrawn_in_the_datasets_folder(tmp_path, api, client):
    submit_sample(client)
    _create_entry(client)
    client.post("/api/submit-data", data={"name": "fit 1", "entry_type": "XRD", "parent_entry": "dataset", "parent_dataset": "XRD run 1", "data_type": "link", "data_link": "https://example.org/fit", "submission_id": "fit"})

    response = _withdraw(client, "fit 1", kind="result", reason="superseded")

    assert response.status_code == 200, response.get_json()
    assert read_record(tmp_path, "results/XRD_run_1/results/fit_1/current/record.json")["notion"] == api.page_named("fit 1")


def test_withdrawing_needs_a_reason_and_the_checked_record(api, client):
    submit_sample(client, status="in work")

    no_reason = _withdraw(client, "PTO54", reason="  ")
    wrong_record = _withdraw(client, "PTO54", page_id="someone-else")

    assert "reason" in no_reason.get_json()["error"]
    assert "Choose it again" in wrong_record.get_json()["error"]
    assert _status(api.page_named("PTO54")) == "in work"


def test_a_withdrawn_record_is_not_withdrawn_twice_but_a_retry_is_recognised(api, client):
    submit_sample(client)
    _withdraw(client, "PTO54", submission_id="first")

    retry = _withdraw(client, "PTO54", submission_id="first")
    again = _withdraw(client, "PTO54", submission_id="second")

    assert retry.status_code == 200 and retry.get_json()["already_processed"] is True
    assert again.status_code == 400 and "already withdrawn" in again.get_json()["error"]


def test_fields_not_yet_added_in_notion_are_named(api, tmp_path):
    api.missing_properties["results-db"] |= {"Status", "Withdrawn Reason"}
    client = app_module.create_app(settings_factory=lambda: settings_for(tmp_path)).test_client()
    submit_sample(client)
    _create_entry(client)

    response = _withdraw(client, "XRD run 1", kind="result")

    assert response.status_code == 400
    assert "The Results database in Notion has no 'Status' and 'Withdrawn Reason' field yet" in response.get_json()["error"]
    assert _status(api.page_named("XRD run 1")) is None


def test_the_forms_send_people_to_the_withdraw_page(api, client):
    submit_sample(client)
    _create_entry(client)

    sample = submit_sample(client, status="withdrawn", amend_existing="true", existing_sample_id=api.page_named("PTO54")["id"])
    entry = client.post("/api/validate-result", data={**ENTRY, "status": "withdrawn"})

    for response in (sample, entry):
        assert response.status_code == 400
        assert "use Withdraw a record on the start page" in response.get_json()["error"]


# --- amending a withdrawn record makes it active again ---


def test_amending_a_withdrawn_sample_makes_it_active_again(tmp_path, api, client):
    submit_sample(client, status="archived")
    _withdraw(client, "PTO54", reason="thought lost")
    sample_id = api.page_named("PTO54")["id"]

    checked = client.post("/api/validate-sample", data={"sample_name": "PTO54", "sample_type": "Root Sample", "composition": "PbTiO3", "synthesis": "PLD"})
    amended = submit_sample(client, amend_existing="true", existing_sample_id=sample_id, photos=(io.BytesIO(b""), ""))

    assert checked.status_code == 409
    assert checked.get_json()["reinstates"] is True
    assert [(item["field"], item["old"], item["new"]) for item in checked.get_json()["changes"]] == [
        ("Status", "withdrawn", "in work"),
        ("Withdrawn Reason", "thought lost", ""),
    ]
    assert amended.status_code == 200, amended.get_json()
    sample = api.page_named("PTO54")
    assert _status(sample) == "in work"
    assert _text(sample, "Withdrawn Reason") == ""
    # The reason stays in the server's history.
    revisions = sorted((tmp_path / "files" / SAMPLE_FOLDER / "revisions").iterdir())
    assert read_record(tmp_path, f"{SAMPLE_FOLDER}/revisions/{revisions[0].name}/record.json")["reason"] == "thought lost"


def test_a_status_chosen_in_the_form_is_used_when_a_sample_comes_back(api, client):
    submit_sample(client)
    _withdraw(client, "PTO54")

    submit_sample(client, status="waiting", amend_existing="true", existing_sample_id=api.page_named("PTO54")["id"], photos=(io.BytesIO(b""), ""))

    assert _status(api.page_named("PTO54")) == "waiting"


def test_amending_a_withdrawn_data_entry_makes_it_active_again(api, client):
    submit_sample(client)
    _create_entry(client)
    _withdraw(client, "XRD run 1", kind="result", reason="wrong file")
    entry_id = api.page_named("XRD run 1")["id"]

    response = client.post("/api/submit-data", data={**ENTRY, "brief_description": "right file after all", "amend_existing": "true", "existing_result_id": entry_id, "submission_id": "amend"})

    body = response.get_json()
    assert response.status_code == 200, body
    assert [item["field"] for item in body["changed_fields"]] == ["Brief Description", "Status", "Withdrawn Reason"]
    entry = api.page_named("XRD run 1")
    assert _status(entry) == "active"
    assert _text(entry, "Withdrawn Reason") == ""


def test_an_active_record_is_amended_without_touching_its_status(api, client):
    submit_sample(client, status="archived")

    checked = client.post("/api/validate-sample", data={"sample_name": "PTO54", "sample_type": "Root Sample", "composition": "PbTiO3", "synthesis": "Sol-gel"})

    assert checked.get_json()["reinstates"] is False
    assert [item["field"] for item in checked.get_json()["changes"]] == ["Synthesis"]


# --- withdrawn parents ---


def test_withdrawn_records_are_left_out_of_the_parent_lists(api, client):
    submit_sample(client)
    submit_sample(client, sample_name="PTO99", photos=(io.BytesIO(b""), ""))
    _create_entry(client)
    _withdraw(client, "PTO99")
    _withdraw(client, "XRD run 1", kind="result", submission_id="withdraw-2")

    samples = [item["name"] for item in client.get("/api/parent-samples").get_json()["samples"]]
    datasets = [item["name"] for item in client.get("/api/parent-datasets").get_json()["datasets"]]

    assert samples == ["PTO54"]
    assert datasets == []


def test_nothing_new_goes_under_a_withdrawn_parent(api, client):
    submit_sample(client)
    _create_entry(client)
    _withdraw(client, "PTO54")
    _withdraw(client, "XRD run 1", kind="result", submission_id="withdraw-2")

    entry = client.post("/api/validate-result", data={**ENTRY, "name": "XRD run 2", "data_type": "link", "data_link": "https://example.org/b"})
    derived = client.post("/api/validate-result", data={"name": "fit", "entry_type": "XRD", "parent_entry": "dataset", "parent_dataset": "XRD run 1", "data_type": "link", "data_link": "https://example.org/c"})
    sub_sample = submit_sample(client, sample_name="PTO54-b", sample_type="Sub-Sample", composition="", parent_sample="PTO54", photos=(io.BytesIO(b""), ""))

    for response, label in ((entry, "sample 'PTO54'"), (derived, "data entry 'XRD run 1'"), (sub_sample, "sample 'PTO54'")):
        assert response.status_code == 400
        assert f"The {label} is withdrawn" in response.get_json()["error"]
    assert api.titles("samples-db") == ["PTO54"]


def test_the_schema_check_asks_for_the_withdrawn_fields():
    issues = validate_tracker_schema({"Samples": {"properties": {"Status": {"type": "select", "select": {"options": [{"name": "in work"}]}}}}, "Results": {}, "People": {}})

    found = {(issue.database, issue.property_name, issue.message) for issue in issues}
    assert ("Samples", "Status", "missing options: withdrawn") in found
    assert ("Samples", "Withdrawn Reason", "missing property") in found
    assert ("Results", "Status", "missing property") in found
    assert ("Results", "Withdrawn Reason", "missing property") in found


def test_both_forms_say_when_an_amendment_makes_a_record_active_again():
    templates = Path(__file__).resolve().parents[1] / "notion_sample_tracker" / "templates"
    for template, function in (("add_sample.html", "confirmDuplicateAmendment"), ("add_results.html", "confirmEntryAmendment")):
        source = (templates / template).read_text(encoding="utf-8")
        start = source.index(f"function {function}(")
        body = source[start : source.index("function ", start + 1)]
        assert "duplicate.reinstates ?" in body and "makes it active again" in body, template
