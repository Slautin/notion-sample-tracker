"""Amending an existing data entry (and sample) through the forms: only what differs changes, nothing is lost."""
import io

from notion_fakes import api, chunked_upload, client, failing_json_writes, read_record, submit_sample  # noqa: F401 (fixtures)


FOLDER = "samples/TiPbO3/PTO54/results/XRD_run_1"


def _fields(**overrides):
    fields = {
        "name": "XRD run 1",
        "entry_type": "XRD",
        "parent_entry": "sample",
        "parent_sample": "PTO54",
        "brief_description": "first scan",
        "char_data": '["XRD"]',
        "sources_data": '[{"name": "Ada Lovelace"}]',
    }
    fields.update(overrides)
    return {name: value for name, value in fields.items() if value is not None}


def _create(client, **overrides):
    done = chunked_upload(client, "scan.csv", b"a,b\n1,2\n")
    response = client.post("/api/submit-data", data=_fields(data_type="file", onedrive_path=done["url"], submission_id="create", **overrides))
    assert response.status_code == 200, response.get_json()
    return done


def _amend(client, api, **overrides):
    entry = api.page_named(overrides.get("name", "XRD run 1"))
    data = _fields(amend_existing="true", existing_result_id=entry["id"], submission_id="amend", **overrides)
    return client.post("/api/submit-data", data=data)


def _text(page, name):
    return "".join(item["plain_text"] for item in page["properties"][name]["rich_text"])


def _names(page, name):
    return [item["name"] for item in page["properties"][name]["multi_select"]]


def test_the_check_before_upload_offers_to_amend_an_existing_entry(api, client):
    submit_sample(client)
    _create(client)

    response = client.post("/api/validate-result", data=_fields(brief_description="second look"))

    body = response.get_json()
    assert response.status_code == 409
    assert body["duplicate"] is True
    assert body["existing_result"]["page_id"] == api.page_named("XRD run 1")["id"]
    # No data chosen: an amendment keeps the entry's file, so that is not an error.
    assert body["changes"] == [{"field": "Brief Description", "old": "first scan", "new": "second look"}]


def test_an_existing_name_without_confirmation_is_not_amended(api, client):
    submit_sample(client)
    _create(client)

    response = client.post("/api/submit-data", data=_fields(brief_description="second look", submission_id="amend"))

    assert response.status_code == 409
    assert _text(api.page_named("XRD run 1"), "Brief Description") == "first scan"


def test_an_amendment_changes_only_what_differs_and_keeps_the_data(tmp_path, api, client):
    submit_sample(client)
    done = _create(client)

    response = _amend(client, api, brief_description="second look", char_data=None, sources_data=None)

    body = response.get_json()
    assert response.status_code == 200, body
    assert body["amended"] is True
    assert [item["field"] for item in body["changed_fields"]] == ["Brief Description"]
    entry = api.page_named("XRD run 1")
    assert _text(entry, "Brief Description") == "second look"
    assert _names(entry, "Characterisation") == ["XRD"]  # left empty, kept
    assert entry["properties"]["Link"]["url"] == done["url"]  # no new data, kept
    assert entry["properties"]["Source"]["relation"] == [{"id": api.page_named("Ada Lovelace")["id"]}]
    assert entry["properties"]["Archive Status"]["select"]["name"] == "Archive complete"
    assert body["receipt"]["title"] == "Data Entry Amendment - XRD run 1"


def test_an_amendment_saves_a_revision_and_the_current_record_and_keeps_the_first(tmp_path, api, client):
    submit_sample(client)
    _create(client)
    first = read_record(tmp_path, f"{FOLDER}/record.json")

    body = _amend(client, api, brief_description="second look").get_json()

    entry = api.page_named("XRD run 1")
    current = read_record(tmp_path, f"{FOLDER}/current/record.json")
    revision = read_record(tmp_path, f"{FOLDER}/revisions/{body['revision_id']}/record.json")
    assert current["notion"] == entry
    assert revision == current
    assert current["kind"] == "result"
    assert current["changed_fields"] == body["changed_fields"]
    assert read_record(tmp_path, f"{FOLDER}/revisions/{body['revision_id']}/changed_fields.json")["changed_fields"] == body["changed_fields"]
    assert read_record(tmp_path, f"{FOLDER}/record.json") == first


def test_an_amendment_whose_record_cannot_be_saved_is_marked_failed(tmp_path, api, client, monkeypatch):
    submit_sample(client)
    _create(client)
    failing_json_writes(monkeypatch, "XRD_run_1/current/record.json")

    response = _amend(client, api, brief_description="second look")

    assert response.status_code == 400
    assert response.get_json()["stage"] == "amendment"
    entry = api.page_named("XRD run 1")
    assert entry["properties"]["Archive Status"]["select"]["name"] == "Failed archive"
    assert "disk went away" in _text(entry, "Archive Error")


def test_an_amendment_with_a_new_file_keeps_the_old_file(tmp_path, api, client):
    submit_sample(client)
    old = _create(client)
    new = chunked_upload(client, "scan.csv", b"a,b\n1,3\n")

    response = _amend(client, api, data_type="file", onedrive_path=new["url"])

    assert response.status_code == 200, response.get_json()
    assert [item["field"] for item in response.get_json()["changed_fields"]] == ["Link"]
    assert api.page_named("XRD run 1")["properties"]["Link"]["url"] == new["url"]
    assert new["name"] == "scan-1.csv"
    assert (tmp_path / "files" / FOLDER / "scan.csv").read_bytes() == b"a,b\n1,2\n"
    assert (tmp_path / "files" / FOLDER / "scan-1.csv").read_bytes() == b"a,b\n1,3\n"
    assert old["url"] != new["url"]


def test_a_link_differing_only_in_case_is_a_change(api, client):
    submit_sample(client)
    client.post("/api/submit-data", data=_fields(data_type="link", data_link="https://example.org/Data/run1", submission_id="create"))

    response = _amend(client, api, data_type="link", data_link="https://example.org/data/run1")

    assert [item["field"] for item in response.get_json()["changed_fields"]] == ["Link"]


def test_the_parent_of_an_existing_entry_cannot_change(api, client):
    submit_sample(client)
    submit_sample(client, sample_name="PTO99", photos=(io.BytesIO(b"\x89PNG"), "p.png"))
    _create(client)

    checked = client.post("/api/validate-result", data=_fields(parent_sample="PTO99"))
    submitted = _amend(client, api, parent_sample="PTO99", brief_description="moved")

    for response in (checked, submitted):
        assert response.status_code == 400
        assert "already exists under sample 'PTO54'" in response.get_json()["error"]
        assert "can't change" in response.get_json()["error"]
    entry = api.page_named("XRD run 1")
    assert entry["properties"]["Sample"]["relation"] == [{"id": api.page_named("PTO54")["id"]}]
    assert _text(entry, "Brief Description") == "first scan"


def test_an_amendment_with_nothing_new_changes_nothing(tmp_path, api, client):
    submit_sample(client)
    _create(client)
    before = api.page_named("XRD run 1")

    response = _amend(client, api)

    assert response.status_code == 200
    assert response.get_json()["no_changes"] is True
    assert api.page_named("XRD run 1") == before
    assert not (tmp_path / "files" / FOLDER / "revisions").exists()


def test_a_confirmation_for_another_entry_is_refused(api, client):
    submit_sample(client)
    _create(client)

    response = client.post("/api/submit-data", data=_fields(brief_description="x", amend_existing="true", existing_result_id="someone-else", submission_id="amend"))

    assert response.status_code == 400
    assert "did not match" in response.get_json()["error"]
    assert _text(api.page_named("XRD run 1"), "Brief Description") == "first scan"


def test_an_amendment_can_replace_the_sources(api, client):
    submit_sample(client)
    _create(client)

    response = _amend(client, api, sources_data='[{"name": "Grace Hopper", "email": "grace@utk.edu"}]')

    assert [item["field"] for item in response.get_json()["changed_fields"]] == ["Source"]
    assert api.page_named("XRD run 1")["properties"]["Source"]["relation"] == [{"id": api.page_named("Grace Hopper")["id"]}]


def test_a_retried_amendment_is_not_applied_twice(tmp_path, api, client):
    submit_sample(client)
    _create(client)
    first = _amend(client, api, brief_description="second look").get_json()

    again = _amend(client, api, brief_description="second look")

    assert again.status_code == 200
    assert again.get_json()["already_processed"] is True
    assert [path.name for path in (tmp_path / "files" / FOLDER / "revisions").iterdir()] == [first["revision_id"]]


def test_a_sample_amendment_with_empty_choices_keeps_them(api, client):
    submit_sample(client, processing="Anneal")
    sample = api.page_named("PTO54")

    response = submit_sample(client, synthesis="", processing="", status="waiting", amend_existing="true", existing_sample_id=sample["id"])

    assert [item["field"] for item in response.get_json()["changed_fields"]] == ["Status"]
    amended = api.page_named("PTO54")
    assert _names(amended, "Synthesis") == ["PLD"]
    assert _names(amended, "Processing") == ["Anneal"]


# --- compositions ---


def _text_of(page, name):
    return "".join(item["plain_text"] for item in page["properties"].get(name, {}).get("rich_text", []))


def test_a_sample_keeps_the_composition_as_typed_next_to_the_normalised_one(api, tmp_path):
    import notion_sample_tracker.app as app_module
    from notion_fakes import settings_for

    api.extra_properties["samples-db"]["User composition names"] = "rich_text"
    client = app_module.create_app(settings_factory=lambda: settings_for(tmp_path)).test_client()

    submit_sample(client, sample_name="BN", composition="Ba(NO3)2")
    submit_sample(client, sample_name="LN", composition="LiNbO3", photos=(io.BytesIO(b""), ""))

    nitrate = api.page_named("BN")
    assert _text_of(nitrate, "Composition") == "BaN2O6"
    assert _names(nitrate, "Elements") == ["Ba", "N", "O"]
    assert _text_of(nitrate, "User composition names") == "Ba(NO3)2"
    assert _text_of(api.page_named("LN"), "User composition names") == ""  # typed as stored: nothing extra
    assert (tmp_path / "files/samples/BaN2O6/BN/record.json").exists()


def test_re_entering_a_composition_as_typed_is_no_change_and_a_new_one_updates_both_fields(api, tmp_path):
    import notion_sample_tracker.app as app_module
    from notion_fakes import settings_for

    api.extra_properties["samples-db"]["User composition names"] = "rich_text"
    client = app_module.create_app(settings_factory=lambda: settings_for(tmp_path)).test_client()
    submit_sample(client, sample_name="BN", composition="Ba(NO3)2")
    sample_id = api.page_named("BN")["id"]

    same = client.post("/api/validate-sample", data={"sample_name": "BN", "sample_type": "Root Sample", "composition": "Ba(NO3)2", "synthesis": "PLD"})
    changed = submit_sample(client, sample_name="BN", composition="Sr(NO3)2", amend_existing="true", existing_sample_id=sample_id, photos=(io.BytesIO(b""), ""))

    assert [item["field"] for item in same.get_json()["changes"]] == []
    assert [item["field"] for item in changed.get_json()["changed_fields"]] == ["Composition"]
    sample = api.page_named("BN")
    assert _text_of(sample, "Composition") == "SrN2O6"
    assert _text_of(sample, "User composition names") == "Sr(NO3)2"


def test_a_database_without_the_typed_composition_field_gets_nothing_extra(api, client):
    submit_sample(client, sample_name="BN", composition="Ba(NO3)2")

    assert "User composition names" not in api.page_named("BN")["properties"]
