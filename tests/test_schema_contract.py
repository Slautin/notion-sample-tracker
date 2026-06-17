from notion_sample_tracker.schema_contract import validate_tracker_schema


def _property(notion_type, options=()):
    payload = {"type": notion_type}
    if options:
        payload[notion_type] = {"options": [{"name": item} for item in options]}
    return payload


def test_schema_contract_accepts_required_operational_fields():
    collections = {
        "Samples": {
            "properties": {
                "Name": _property("title"),
                "Sample Type": _property("select"),
                "Composition": _property("rich_text"),
                "Elements": _property("multi_select"),
                "Synthesis": _property("multi_select"),
                "Synthesis Details": _property("rich_text"),
                "Processing": _property("multi_select"),
                "Processing Details": _property("rich_text"),
                "Status": _property("select"),
                "Parent Sample": _property("relation"),
                "Source": _property("relation"),
                "QRCode": _property("files"),
                "Photos": _property("files"),
                "Submission ID": _property("rich_text"),
                "Archive Status": _property("select", ("Pending archive", "Archive complete", "Failed archive")),
                "Archive Error": _property("rich_text"),
            }
        },
        "Results": {
            "properties": {
                "Name": _property("title"),
                "Data Type": _property("select"),
                "Brief Description": _property("rich_text"),
                "Characterisation": _property("multi_select"),
                "Upload Method": _property("select"),
                "Parent Entry": _property("select"),
                "Link": _property("url"),
                "Sample": _property("relation"),
                "Related Results": _property("relation"),
                "Source": _property("relation"),
                "QRCode": _property("files"),
                "Submission ID": _property("rich_text"),
                "Archive Status": _property("select", ("Pending archive", "Archive complete", "Failed archive")),
                "Archive Error": _property("rich_text"),
            }
        },
        "People": {
            "properties": {
                "Person": _property("title"),
                "Email": _property("email"),
                "Affiliation": _property("rich_text"),
            }
        },
    }

    assert validate_tracker_schema(collections) == []


def test_schema_contract_reports_missing_archive_status_option():
    collections = {
        "Samples": {"properties": {"Archive Status": _property("select", ("Pending archive",))}},
        "Results": {"properties": {}},
        "People": {"properties": {}},
    }

    issues = validate_tracker_schema(collections)

    assert any(issue.property_name == "Archive Status" and "Archive complete" in issue.message for issue in issues)
