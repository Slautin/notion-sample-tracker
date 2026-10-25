from notion_sample_tracker.models import ResultForm


def _form(**fields):
    return ResultForm.from_form({"name": "run 1", "entry_type": "XRD", **fields})


def test_for_a_file_the_stored_files_address_wins_over_a_link_typed_earlier():
    form = _form(data_type="file", onedrive_path="https://tracker.example/files/a.csv", data_link="https://stale.example/typed")

    assert form.link == "https://tracker.example/files/a.csv"
    assert form.upload_method == "file"


def test_for_a_link_the_typed_link_is_used_even_if_a_file_address_is_left_over():
    form = _form(data_type="link", data_link="https://doi.org/10.1/abc", onedrive_path="https://tracker.example/files/a.csv")

    assert form.link == "https://doi.org/10.1/abc"


def test_the_older_link_field_still_works_for_files_without_a_stored_address():
    form = _form(upload_method="file", link="https://example.com/data")

    assert form.link == "https://example.com/data"


def test_an_explicit_link_field_is_still_preferred_for_links():
    form = _form(upload_method="link", link="https://a.example", data_link="https://b.example")

    assert form.link == "https://a.example"


def test_nothing_given_means_no_link():
    assert _form(data_type="file").link == ""
