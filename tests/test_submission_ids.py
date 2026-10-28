"""A submission ID identifies one submission; a repeated ID is a retry of it, never a new record."""
import re
from pathlib import Path

from notion_fakes import api, chunked_upload, client, submit_sample  # noqa: F401 (fixtures)


TEMPLATES = Path(__file__).resolve().parents[1] / "notion_sample_tracker" / "templates"


def _entry(client, name, submission_id):
    done = chunked_upload(client, "scan.csv", b"a,b\n", entry_name=name)
    return client.post(
        "/api/submit-data",
        data={
            "name": name,
            "entry_type": "XRD",
            "parent_entry": "sample",
            "parent_sample": "PTO54",
            "data_type": "file",
            "onedrive_path": done["url"],
            "submission_id": submission_id,
        },
    )


def test_a_new_entry_sent_with_an_earlier_entrys_id_is_refused_not_reported_as_saved(api, client):
    submit_sample(client)
    assert _entry(client, "XRD run 1", "open-form").status_code == 200

    response = _entry(client, "XRD run 2", "open-form")

    body = response.get_json()
    assert response.status_code == 400
    assert body["success"] is False
    assert "already used to submit 'XRD run 1'" in body["error"]
    assert "Reload the page" in body["error"]
    assert api.titles("results-db") == ["XRD run 1"]


def test_the_check_before_upload_refuses_it_too(api, client):
    submit_sample(client)
    _entry(client, "XRD run 1", "open-form")

    response = client.post("/api/validate-result", data={"name": "XRD run 2", "entry_type": "XRD", "parent_entry": "sample", "parent_sample": "PTO54", "data_type": "file", "submission_id": "open-form"})

    assert response.status_code == 400
    assert "already used to submit 'XRD run 1'" in response.get_json()["error"]


def test_a_real_retry_is_still_recognised(api, client):
    submit_sample(client)
    _entry(client, "XRD run 1", "same-submission")

    response = _entry(client, "XRD run 1", "same-submission")

    assert response.status_code == 200
    assert response.get_json()["already_processed"] is True


def _after_each_reset(template: str) -> list[str]:
    """The first statement after each place a form clears itself following a saved submission."""
    source = (TEMPLATES / template).read_text(encoding="utf-8")
    firsts = []
    for match in re.finditer(r"\.reset\(\);\n", source):
        lines = (line.strip() for line in source[match.end() :].splitlines())
        firsts.append(next(line for line in lines if line and not line.startswith("//")))
    return firsts


def test_both_forms_take_a_new_submission_id_after_each_saved_submission():
    # The results form clears itself once after a save; the sample form after a save and after a confirmed amendment.
    assert _after_each_reset("add_results.html") == ["resetSubmissionId();"]
    assert _after_each_reset("add_sample.html") == ["resetSubmissionId();", "resetSubmissionId();"]
