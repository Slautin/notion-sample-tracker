import pytest

from notion_sample_tracker.app import _sample_duplicate_info, _sample_field_changes, _validate_sample_form
from notion_sample_tracker.models import SampleForm


class DuplicateSampleNotion:
    def __init__(self, existing_submission=None, existing_name=None):
        self.existing_submission = existing_submission
        self.existing_name = existing_name

    def sample_page_by_submission(self, submission_id):
        return self.existing_submission

    def sample_page_by_name(self, name):
        return self.existing_name if name == "PTO54" else None

    def sample_exists(self, name):
        return name == "PTO54"


def test_duplicate_sample_name_requires_change():
    form = SampleForm(name="PTO54", sample_type="Root Sample", composition="PbTiO3", submission_id="new-submission")

    with pytest.raises(ValueError, match="already exists"):
        _validate_sample_form(form, DuplicateSampleNotion())


def test_matching_submission_id_is_allowed_as_retry():
    page = _sample_page("PTO54", "Root Sample", "PbTiO3")
    form = SampleForm(name="PTO54", sample_type="Root Sample", composition="PbTiO3", submission_id="old-submission")

    _validate_sample_form(form, DuplicateSampleNotion(existing_submission=page))


def test_changed_loaded_json_reusing_submission_id_is_rejected():
    page = _sample_page("PTO54", "Root Sample", "PbTiO3")
    form = SampleForm(name="PTO501", sample_type="Root Sample", composition="PbTiO3", submission_id="old-submission")

    with pytest.raises(ValueError, match="already submitted"):
        _validate_sample_form(form, DuplicateSampleNotion(existing_submission=page))


def test_duplicate_info_lists_changed_fields():
    page = _sample_page("PTO54", "Root Sample", "PbTiO3")
    form = SampleForm(name="PTO54", sample_type="Root Sample", composition="PbTiO3", processing=["Anneal"], status="in work")

    duplicate = _sample_duplicate_info(form, DuplicateSampleNotion(existing_name=page))

    assert duplicate is not None
    assert duplicate["page"] == page
    assert [item["field"] for item in duplicate["changes"]] == ["Processing", "Status"]


def test_sample_field_changes_ignores_blank_optional_values():
    page = _sample_page("PTO54", "Sub-Sample", "PbTiO3")
    form = SampleForm(name="PTO54", sample_type="Sub-Sample", composition="", submission_id="new-submission")

    assert _sample_field_changes(form, page) == []


def test_root_sample_changes_ignore_parent_sample_value():
    page = _sample_page("PTO54", "Root Sample", "PbTiO3")
    form = SampleForm(name="PTO54", sample_type="Root Sample", composition="PbTiO3", parent_sample_id="PTO54")

    assert _sample_field_changes(form, page) == []


def _sample_page(name: str, sample_type: str, composition: str) -> dict:
    return {
        "id": "page-id",
        "properties": {
            "Name": {"type": "title", "title": [{"plain_text": name}]},
            "Sample Type": {"type": "select", "select": {"name": sample_type}},
            "Composition": {"type": "rich_text", "rich_text": [{"plain_text": composition}]},
            "Synthesis": {"type": "multi_select", "multi_select": []},
            "Processing": {"type": "multi_select", "multi_select": []},
        },
    }
