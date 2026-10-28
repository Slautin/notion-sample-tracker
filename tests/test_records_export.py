import csv
import io
import json
import os
import stat

import pytest

from notion_sample_tracker import records_export
from notion_sample_tracker.records_export import build_table, format_property, latest_per_page, load_records


def _page(page_id, title, **properties):
    return {
        "id": page_id,
        "url": f"https://www.notion.so/{page_id.replace('-', '')}",
        "created_time": "2026-10-01T09:00:00.000Z",
        "last_edited_time": "2026-10-02T09:00:00.000Z",
        "properties": {"Name": {"type": "title", "title": [{"plain_text": title}]}, **properties},
    }


def _write(root, relative, data, mtime=None):
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(data if isinstance(data, str) else json.dumps(data), encoding="utf-8")
    if mtime is not None:
        os.utime(path, (mtime, mtime))
    return path


def _record(page, kind="sample", captured_at="2026-10-28T10:00:00+00:00", **extra):
    return {"kind": kind, "captured_at": captured_at, "notion": page, "relations": {}, **extra}


# --- property values ---


@pytest.mark.parametrize(
    "prop, expected",
    [
        ({"type": "title", "title": [{"plain_text": "PTO"}, {"plain_text": "54"}]}, "PTO54"),
        ({"type": "rich_text", "rich_text": []}, ""),
        ({"type": "select", "select": None}, ""),
        ({"type": "select", "select": {"name": "Active"}}, "Active"),
        ({"type": "status", "status": {"name": "Done"}}, "Done"),
        ({"type": "multi_select", "multi_select": [{"name": "PLD"}, {"name": "Anneal"}]}, "PLD; Anneal"),
        ({"type": "number", "number": 0}, "0"),
        ({"type": "number", "number": 2.5}, "2.5"),
        ({"type": "number", "number": None}, ""),
        ({"type": "checkbox", "checkbox": False}, "FALSE"),
        ({"type": "checkbox", "checkbox": True}, "TRUE"),
        ({"type": "date", "date": {"start": "2026-10-01"}}, "2026-10-01"),
        ({"type": "date", "date": {"start": "2026-10-01", "end": "2026-10-05"}}, "2026-10-01 → 2026-10-05"),
        ({"type": "url", "url": "https://example.org/a"}, "https://example.org/a"),
        ({"type": "email", "email": "ada@utk.edu"}, "ada@utk.edu"),
        ({"type": "phone_number", "phone_number": "865 555 0100"}, "865 555 0100"),
        ({"type": "created_time", "created_time": "2026-10-01T09:00:00.000Z"}, "2026-10-01T09:00:00.000Z"),
        (
            {
                "type": "files",
                "files": [
                    {"name": "scan.csv", "type": "external", "external": {"url": "https://app/files/scan.csv"}},
                    {"name": "PTO54_qr.png", "type": "file", "file": {"url": "https://s3.example/signed?expires=1h"}},
                ],
            },
            "scan.csv (https://app/files/scan.csv); PTO54_qr.png",
        ),
        ({"type": "people", "people": [{"id": "u1", "name": "Ada"}, {"id": "u2"}]}, "Ada; u2"),
        ({"type": "created_by", "created_by": {"id": "u1", "name": "Ada"}}, "Ada"),
        ({"type": "unique_id", "unique_id": {"prefix": "S", "number": 12}}, "S-12"),
        ({"type": "unique_id", "unique_id": {"prefix": None, "number": 12}}, "12"),
        ({"type": "formula", "formula": {"type": "string", "string": "Ti0.5"}}, "Ti0.5"),
        ({"type": "formula", "formula": {"type": "number", "number": 3}}, "3"),
        ({"type": "formula", "formula": {"type": "boolean", "boolean": True}}, "TRUE"),
        ({"type": "formula", "formula": {"type": "date", "date": {"start": "2026-10-01"}}}, "2026-10-01"),
        ({"type": "rollup", "rollup": {"type": "number", "number": 7, "function": "count"}}, "7"),
        (
            {"type": "rollup", "rollup": {"type": "array", "array": [{"type": "title", "title": [{"plain_text": "A"}]}, {"type": "number", "number": 2}]}},
            "A; 2",
        ),
        ({"type": "verification", "verification": {"state": "verified"}}, '{"state": "verified"}'),
        ({}, ""),
    ],
)
def test_property_values_are_written_as_plain_text(prop, expected):
    assert format_property(prop) == expected


def test_linked_records_are_written_by_name_and_fall_back_to_the_id():
    prop = {"type": "relation", "relation": [{"id": "a"}, {"id": "b"}]}

    assert format_property(prop, [{"id": "a", "name": "PTO54"}, {"id": "b", "name": ""}]) == "PTO54; b"
    assert format_property(prop) == "a; b"


# --- reading the records ---


def test_records_are_read_and_unusable_files_are_reported(tmp_path):
    _write(tmp_path, "samples/X/PTO54/record.json", _record(_page("p1", "PTO54")))
    _write(tmp_path, "samples/X/PTO54/results/run/record.json", "{not json")
    _write(tmp_path, "samples/X/PTO54/revisions/r1/record.json", {"revision_id": "r1", "notion_page_id": "p1"})
    _write(tmp_path, "samples/X/PTO54/results/run2/record.json", "[1, 2]")
    _write(tmp_path, ".uploads/abc/record.json", _record(_page("p9", "staged")))
    _write(tmp_path, "samples/X/PTO54/changed_fields.json", {"changed_fields": []})

    records, skipped = load_records(tmp_path)

    assert [record.path for record in records] == ["samples/X/PTO54/record.json"]
    assert sorted(note.split(":")[0] for note in skipped) == [
        "samples/X/PTO54/results/run/record.json",
        "samples/X/PTO54/results/run2/record.json",
        "samples/X/PTO54/revisions/r1/record.json",
    ]


def test_a_missing_root_has_no_records(tmp_path):
    assert load_records(tmp_path / "nothing-here") == ([], [])


def test_the_kind_of_an_older_record_comes_from_its_folder(tmp_path):
    _write(tmp_path, "samples/X/PTO54/record.json", {"form": {}, "notion": _page("p1", "PTO54")})
    _write(tmp_path, "samples/X/PTO54/results/run/record.json", {"form": {}, "notion": _page("p2", "run")})
    _write(tmp_path, "results/p3/record.json", {"form": {}, "notion": _page("p3", "legacy run")})
    _write(tmp_path, "sources/Ada/record.json", {"notion": _page("p4", "Ada")})

    kinds = {record.page["id"]: record.kind for record in load_records(tmp_path)[0]}

    assert kinds == {"p1": "sample", "p2": "result", "p3": "result", "p4": "source"}


def test_the_newest_record_of_a_page_wins_by_capture_time_then_file_time(tmp_path):
    # The amended record was captured later, though its file is older on disk (copied, restored from backup).
    _write(tmp_path, "s/PTO54/record.json", _record(_page("aaaa-1", "first"), captured_at="2026-10-28T10:00:00+00:00"), mtime=2_000_000_000)
    _write(tmp_path, "s/PTO54/current/record.json", _record(_page("aaaa1", "amended"), captured_at="2026-10-28T11:00:00+00:00"), mtime=1_000_000_000)
    # Records from before captured_at was saved lose to any newer record and are ordered by file time.
    _write(tmp_path, "s/OLD/record.json", {"notion": _page("b1", "old, older file")}, mtime=1_000_000_000)
    _write(tmp_path, "s/OLD/current/record.json", {"notion": _page("b1", "old, newer file")}, mtime=1_500_000_000)

    titles = sorted(record.page["properties"]["Name"]["title"][0]["plain_text"] for record in latest_per_page(load_records(tmp_path)[0]))

    assert titles == ["amended", "old, newer file"]


# --- the tables ---


def test_columns_are_the_title_then_properties_a_to_z_then_where_the_row_came_from(tmp_path):
    first = _page("p1", "b-sample", zeta={"type": "rich_text", "rich_text": [{"plain_text": "z"}]}, Alpha={"type": "number", "number": 1})
    second = _page("p2", "A-sample", beta={"type": "select", "select": {"name": "x"}})
    _write(tmp_path, "s/b/record.json", _record(first))
    _write(tmp_path, "s/a/record.json", _record(second))

    columns, rows = build_table(latest_per_page(load_records(tmp_path)[0]))

    assert columns == ["Name", "Alpha", "beta", "zeta", *records_export.META_COLUMNS]
    assert [row[:4] for row in rows] == [["A-sample", "", "x", ""], ["b-sample", "1", "", "z"]]
    assert rows[0][4:] == [
        "p2",
        "https://www.notion.so/p2",
        "2026-10-01T09:00:00.000Z",
        "2026-10-02T09:00:00.000Z",
        "2026-10-28T10:00:00+00:00",
        "s/a/record.json",
    ]


def test_cells_that_a_spreadsheet_would_run_as_formulas_are_neutralised(tmp_path):
    page = _page(
        "p1",
        "=HYPERLINK(\"http://evil\")",
        Note={"type": "rich_text", "rich_text": [{"plain_text": "@SUM(A1)"}]},
        Phone={"type": "phone_number", "phone_number": "+1 865 555 0100"},
        Delta={"type": "number", "number": -5},
        Offset={"type": "rich_text", "rich_text": [{"plain_text": "-0.25"}]},
        Plain={"type": "rich_text", "rich_text": [{"plain_text": "a = b"}]},
    )
    page["properties"]["=cmd"] = {"type": "rich_text", "rich_text": []}
    _write(tmp_path, "s/p1/record.json", _record(page))

    columns, rows = build_table(load_records(tmp_path)[0])
    row = dict(zip(columns, rows[0]))

    assert row["Name"] == "'=HYPERLINK(\"http://evil\")"
    assert row["Note"] == "'@SUM(A1)"
    assert row["Phone"] == "'+1 865 555 0100"
    assert row["Delta"] == "-5"
    assert row["Offset"] == "-0.25"
    assert row["Plain"] == "a = b"
    assert "'=cmd" in columns


def test_the_csv_round_trips_commas_quotes_newlines_and_symbols():
    columns, rows = ["Name", "Notes"], [["PTO54", 'grown at 600 °C, "fast"\nsecond line; µm Å']]

    data = records_export.csv_bytes(columns, rows)

    assert data.startswith(b"\xef\xbb\xbf")
    assert list(csv.reader(io.StringIO(data.decode("utf-8-sig")))) == [columns, rows[0]]


# --- the command line ---


def test_the_command_writes_three_csv_files(tmp_path, capsys):
    root, out = tmp_path / "files", tmp_path / "exports"
    _write(root, "samples/X/PTO54/record.json", _record(_page("p1", "PTO54")))
    _write(root, "samples/X/PTO54/results/run/record.json", _record(_page("p2", "run"), kind="result"))
    _write(root, "samples/X/PTO54/revisions/r1/record.json", {"notion_page_id": "p1"})

    assert records_export.main(["--root", str(root), "--out", str(out)]) == 0

    printed = capsys.readouterr().out
    assert "samples.csv: 1 rows" in printed and "results.csv: 1 rows" in printed and "sources.csv: 0 rows" in printed
    assert "Skipped 1 file(s)" in printed
    assert sorted(path.name for path in out.iterdir()) == ["results.csv", "samples.csv", "sources.csv"]
    samples = list(csv.DictReader(io.StringIO((out / "samples.csv").read_text(encoding="utf-8-sig"))))
    assert [row["Name"] for row in samples] == ["PTO54"]
    assert stat.S_IMODE((out / "samples.csv").stat().st_mode) == 0o644


def test_the_command_refuses_a_root_that_is_not_a_folder(tmp_path, capsys):
    assert records_export.main(["--root", str(tmp_path / "missing"), "--out", str(tmp_path)]) == 1
    assert "is not a folder" in capsys.readouterr().out
