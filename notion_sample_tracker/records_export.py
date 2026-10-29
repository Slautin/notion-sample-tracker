"""CSV tables built from the records this app saved, one row per Notion page.

Every submission saves a record.json holding the Notion page as Notion returned it after the submission's last
write, plus the names of its linked records. This module reads those files, keeps the newest record of each
page, and lays them out like the Notion databases: one table each for samples, results and sources.

    python -m notion_sample_tracker.records_export --root /var/lib/sampletracker/files --out ./exports

--root can also be a synced copy of the OneDrive/SharePoint folder (the folder named by ONEDRIVE_ROOT_FOLDER).
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


TABLES = {"samples": "sample", "results": "result", "sources": "source"}
RECORD_NAME = "record.json"
SKIP_DIRS = {".uploads"}  # unfinished chunked uploads
META_COLUMNS = ["notion_page_id", "notion_url", "created_time", "last_edited_time", "captured_at", "record_path"]
JOIN = "; "
# A cell starting with one of these is a formula to Excel and LibreOffice.
FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r")


@dataclass
class Record:
    kind: str  # "sample", "result" or "source"
    path: str  # relative to the root, with forward slashes
    data: dict[str, Any]
    mtime: float

    @property
    def page(self) -> dict[str, Any]:
        return self.data["notion"]


def load_records(root: Path | str) -> tuple[list[Record], list[str]]:
    """Every record.json under `root` that holds a Notion page, and a note for each one that could not be used."""
    root = Path(root)
    records: list[Record] = []
    skipped: list[str] = []
    for directory, subdirs, files in os.walk(root):
        subdirs[:] = sorted(name for name in subdirs if name not in SKIP_DIRS)
        if RECORD_NAME not in files:
            continue
        path = Path(directory) / RECORD_NAME
        relative = path.relative_to(root).as_posix()
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            mtime = path.stat().st_mtime
        except (OSError, ValueError) as exc:
            skipped.append(f"{relative}: {exc}")
            continue
        page = data.get("notion") if isinstance(data, dict) else None
        if not (isinstance(page, dict) and page.get("id") and isinstance(page.get("properties"), dict)):
            # Amendment records written before records held the whole page, or a user file that happens to be
            # named record.json.
            skipped.append(f"{relative}: no Notion page in this file")
            continue
        records.append(Record(_kind(data, relative), relative, data, mtime))
    return records, skipped


def latest_per_page(records: list[Record]) -> list[Record]:
    """The newest record of each Notion page: by captured_at, then file time (older records have no captured_at)."""
    latest: dict[str, Record] = {}
    for record in records:
        key = str(record.page["id"]).replace("-", "").lower()
        if key not in latest or _newness(record) > _newness(latest[key]):
            latest[key] = record
    return list(latest.values())


def build_table(records: list[Record]) -> tuple[list[str], list[list[str]]]:
    """Columns and rows: the title property first, the other properties A to Z, then where each row came from."""
    title = ""
    names: set[str] = set()
    for record in records:
        for name, prop in record.page["properties"].items():
            if prop.get("type") == "title":
                title = title or name
            else:
                names.add(name)
    names.discard(title)
    properties = ([title] if title else []) + sorted(names, key=lambda name: (name.casefold(), name))

    rows = []
    for record in sorted(records, key=lambda item: (_title(item.page).casefold(), item.page.get("created_time", ""), item.path)):
        page = record.page
        linked = record.data.get("relations") if isinstance(record.data.get("relations"), dict) else {}
        row = [format_property(page["properties"].get(name, {}), linked.get(name)) for name in properties]
        row += [
            page.get("id", ""),
            page.get("url", ""),
            page.get("created_time", ""),
            page.get("last_edited_time", ""),
            str(record.data.get("captured_at") or ""),
            record.path,
        ]
        rows.append([_safe_cell(cell) for cell in row])
    return [_safe_cell(name) for name in properties + META_COLUMNS], rows


def export_tables(root: Path | str) -> tuple[dict[str, tuple[list[str], list[list[str]]]], list[str]]:
    """All three tables, keyed by "samples", "results" and "sources", and the notes on skipped files."""
    records, skipped = load_records(root)
    latest = latest_per_page(records)
    return {table: build_table([record for record in latest if record.kind == kind]) for table, kind in TABLES.items()}, skipped


def to_csv(columns: list[str], rows: list[list[str]]) -> str:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(columns)
    writer.writerows(rows)
    return buffer.getvalue()


def csv_bytes(columns: list[str], rows: list[list[str]]) -> bytes:
    """UTF-8 with a byte order mark, so Excel shows accents and symbols (µ, Å, °) correctly."""
    return to_csv(columns, rows).encode("utf-8-sig")


def property_items(prop: dict[str, Any], linked: list[dict[str, str]] | None = None) -> list[str] | None:
    """The values of a many-valued property (choices, linked records, people, files) as a list; None for others."""
    kind = prop.get("type")
    value = prop.get(kind) if kind else None
    if kind == "multi_select":
        return [item.get("name", "") for item in value or []]
    if kind == "relation":
        names = {item.get("id"): item.get("name") for item in linked or []}
        return [names.get(ref.get("id")) or ref.get("id", "") for ref in value or []]
    if kind == "people":
        return [person.get("name") or person.get("id", "") for person in value or []]
    if kind == "files":
        return [_file(item) for item in value or []]
    return None


def format_property(prop: dict[str, Any], linked: list[dict[str, str]] | None = None) -> str:
    """One Notion property value as text. `linked` holds the names of related pages, saved with the record."""
    items = property_items(prop, linked)
    if items is not None:
        return JOIN.join(items)
    kind = prop.get("type")
    value = prop.get(kind) if kind else None
    if value is None:
        return ""
    if kind in {"title", "rich_text"}:
        return "".join(item.get("plain_text", "") for item in value)
    if kind in {"select", "status"}:
        return value.get("name", "")
    if kind == "number":
        return str(value)
    if kind in {"checkbox", "boolean"}:
        return "TRUE" if value else "FALSE"
    if kind == "date":
        start, end = value.get("start") or "", value.get("end") or ""
        return f"{start} → {end}" if end else start
    if kind in {"string", "url", "email", "phone_number", "created_time", "last_edited_time"}:
        return str(value)
    if kind in {"created_by", "last_edited_by"}:
        return value.get("name") or value.get("id", "")
    if kind == "unique_id":
        number = "" if value.get("number") is None else str(value["number"])
        return f"{value['prefix']}-{number}" if value.get("prefix") else number
    if kind == "formula":
        return format_property(value)
    if kind == "rollup":
        if value.get("type") == "array":
            return JOIN.join(format_property(item) for item in value.get("array", []))
        return format_property(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _file(item: dict[str, Any]) -> str:
    name = item.get("name", "")
    if item.get("type") == "external":
        url = item.get("external", {}).get("url", "")
        return f"{name} ({url})" if url and url != name else name
    # A file stored by Notion: its link expires within the hour, so only the name is useful.
    return name


def _kind(data: dict[str, Any], relative: str) -> str:
    if data.get("kind") in TABLES.values():
        return data["kind"]
    # Records written before "kind" was saved: sources/<name>/, .../results/<entry>/ or a sample folder.
    folders = relative.split("/")[:-1]
    if folders[:1] == ["sources"]:
        return "source"
    return "result" if "results" in folders else "sample"


def _newness(record: Record) -> tuple[float, float, str]:
    try:
        captured = datetime.fromisoformat(str(record.data.get("captured_at") or "")).timestamp()
    except ValueError:
        captured = 0.0
    return captured, record.mtime, record.path


def _title(page: dict[str, Any]) -> str:
    for prop in page["properties"].values():
        if prop.get("type") == "title":
            return format_property(prop)
    return ""


def _safe_cell(text: str) -> str:
    """Stop spreadsheet programs from running a cell as a formula; numbers such as -5 stay numbers."""
    if text.startswith(FORMULA_PREFIXES) and not _is_number(text):
        return "'" + text
    return text


def _is_number(text: str) -> bool:
    try:
        float(text)
    except ValueError:
        return False
    return True


def _write_atomically(path: Path, content: bytes) -> None:
    handle, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(content)
        os.chmod(temporary, 0o644)  # mkstemp creates the file readable by its owner only
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write samples.csv, results.csv and sources.csv from the saved records.")
    parser.add_argument("--root", default=os.getenv("STORAGE_ROOT", "./files"), help="folder holding the records (default: STORAGE_ROOT)")
    parser.add_argument("--out", default=".", help="folder to write the CSV files to (default: here)")
    args = parser.parse_args(argv)

    root = Path(args.root).expanduser()
    if not root.is_dir():
        print(f"{root} is not a folder.")
        return 1
    out = Path(args.out).expanduser()
    out.mkdir(parents=True, exist_ok=True)
    tables, skipped = export_tables(root)
    for table, (columns, rows) in tables.items():
        target = out / f"{table}.csv"
        _write_atomically(target, csv_bytes(columns, rows))
        print(f"{target}: {len(rows)} rows")
    if skipped:
        print(f"Skipped {len(skipped)} file(s):")
        for note in skipped[:20]:
            print(f"  {note}")
        if len(skipped) > 20:
            print(f"  ... and {len(skipped) - 20} more")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
