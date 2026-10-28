"""A form can correct an existing Source's email or affiliation, but only after the person submitting confirms it."""
import io
import json
import re
from pathlib import Path

from notion_fakes import api, chunked_upload, client, read_record, submit_sample  # noqa: F401 (fixtures)


def _sources(**person):
    return json.dumps([{"type": "new", "name": "Ada Lovelace", **person}])


def _person(api):
    page = api.page_named("Ada Lovelace")
    return page["properties"].get("Email", {}).get("email"), "".join(item["plain_text"] for item in page["properties"]["Affiliation"]["rich_text"])


def _check_sample(client, **fields):
    data = {"sample_name": "PTO55", "sample_type": "Root Sample", "composition": "PbTiO3", **fields}
    return client.post("/api/validate-sample", data=data)


def test_the_check_before_upload_lists_changed_details_of_an_existing_source(api, client):
    submit_sample(client)  # creates Ada Lovelace, ada@utk.edu, UTK

    response = _check_sample(client, sources_data=_sources(email="ADA@utk.edu", affiliation="ORNL"))

    assert response.status_code == 200
    assert response.get_json()["source_updates"] == [
        {"page_id": api.page_named("Ada Lovelace")["id"], "name": "Ada Lovelace", "field": "Affiliation", "old": "UTK", "new": "ORNL"}
    ]


def test_details_left_empty_or_unchanged_and_new_people_are_not_changes(api, client):
    submit_sample(client)

    for sources in (
        _sources(),
        _sources(email="ada@utk.edu"),  # affiliation left empty: kept
        _sources(email="ada@utk.edu", affiliation="UTK"),
        json.dumps([{"name": "Grace Hopper", "affiliation": "Navy"}]),
    ):
        assert _check_sample(client, sources_data=sources).get_json()["source_updates"] == []


def test_without_confirmation_an_existing_source_is_left_as_it_is(api, client):
    submit_sample(client)

    response = submit_sample(client, sample_name="PTO55", sources_data=_sources(affiliation="ORNL"))

    assert response.status_code == 200, response.get_json()
    assert _person(api) == ("ada@utk.edu", "UTK")
    assert api.page_named("PTO55")["properties"]["Source"]["relation"] == [{"id": api.page_named("Ada Lovelace")["id"]}]


def test_a_confirmed_change_updates_the_source_and_keeps_a_revision(tmp_path, api, client):
    submit_sample(client)

    response = submit_sample(client, sample_name="PTO55", sources_data=_sources(email="ada@ornl.gov", affiliation="ORNL"), update_sources="true")

    assert response.status_code == 200, response.get_json()
    assert _person(api) == ("ada@ornl.gov", "ORNL")
    revisions = list((tmp_path / "files/sources/Ada_Lovelace/revisions").iterdir())
    assert len(revisions) == 1
    revision = read_record(tmp_path, f"sources/Ada_Lovelace/revisions/{revisions[0].name}/record.json")
    assert revision["kind"] == "source"
    assert [(change["field"], change["old"], change["new"]) for change in revision["changed_fields"]] == [("Email", "ada@utk.edu", "ada@ornl.gov"), ("Affiliation", "UTK", "ORNL")]
    assert revision["notion"] == api.page_named("Ada Lovelace")
    # The current Source record, saved with the sample, shows the new details too.
    assert read_record(tmp_path, "sources/Ada_Lovelace/record.json")["notion"] == api.page_named("Ada Lovelace")


def test_the_data_entry_form_can_update_a_source_too(api, client):
    submit_sample(client)
    done = chunked_upload(client, "scan.csv", b"a,b\n")
    fields = {"name": "XRD run 1", "entry_type": "XRD", "parent_entry": "sample", "parent_sample": "PTO54", "data_type": "file", "onedrive_path": done["url"], "sources_data": _sources(affiliation="ORNL")}

    checked = client.post("/api/validate-result", data=fields)
    submitted = client.post("/api/submit-data", data={**fields, "update_sources": "true"})

    assert [change["new"] for change in checked.get_json()["source_updates"]] == ["ORNL"]
    assert submitted.status_code == 200, submitted.get_json()
    assert _person(api) == ("ada@utk.edu", "ORNL")


def test_a_sample_amendment_can_replace_its_sources_and_keeps_them_when_left_empty(api, client):
    submit_sample(client)
    sample = api.page_named("PTO54")

    kept = submit_sample(client, sources_data="[]", status="waiting", amend_existing="true", existing_sample_id=sample["id"])
    replaced = submit_sample(client, sources_data=json.dumps([{"name": "Grace Hopper"}]), amend_existing="true", existing_sample_id=sample["id"], photos=(io.BytesIO(b"\x89PNG"), "b.png"))

    assert [item["field"] for item in kept.get_json()["changed_fields"]] == ["Status"]
    assert [item["field"] for item in replaced.get_json()["changed_fields"]] == ["Source"]
    assert api.page_named("PTO54")["properties"]["Source"]["relation"] == [{"id": api.page_named("Grace Hopper")["id"]}]


def test_both_forms_send_the_update_only_after_asking():
    templates = Path(__file__).resolve().parents[1] / "notion_sample_tracker" / "templates"
    for template in ("add_sample.html", "add_results.html"):
        source = (templates / template).read_text(encoding="utf-8")
        asked = re.search(r"if \(!confirmSourceUpdates\(validation\.source_updates\)\) \{.*?return;\s*\}\s*(.*?);", source, re.S)
        assert asked, template
        assert asked.group(1).strip() in {"formData.append('update_sources', 'true')", "updateSources = true"}, template
        if "updateSources = true" in asked.group(1):
            assert "if (updateSources) {\n      formData.append('update_sources', 'true');" in source
