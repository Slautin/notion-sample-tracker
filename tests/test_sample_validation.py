import pytest

from notion_sample_tracker.app import _validate_sample_form
from notion_sample_tracker.models import SampleForm


class DuplicateSampleNotion:
    def sample_page_by_submission(self, submission_id):
        return None

    def sample_exists(self, name):
        return name == "PTO54"


def test_duplicate_sample_name_requires_change():
    form = SampleForm(name="PTO54", sample_type="Root Sample", composition="PbTiO3", submission_id="new-submission")

    with pytest.raises(ValueError, match="already exists"):
        _validate_sample_form(form, DuplicateSampleNotion())
