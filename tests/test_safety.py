from notion_sample_tracker.safety import redact_for_log, safe_path_segment, safe_upload_filename


def test_safe_path_segment_removes_path_separators():
    assert safe_path_segment("Fe/O sample 1") == "Fe_O_sample_1"


def test_safe_upload_filename_keeps_extension_and_drops_paths():
    assert safe_upload_filename("../raw data.csv") == "raw_data.csv"


def test_redact_for_log_hides_sensitive_nested_fields():
    redacted = redact_for_log({"form": {"source_email": "a@example.com"}, "count": 1})

    assert redacted["form"] == "<redacted>"
    assert redacted["count"] == 1
