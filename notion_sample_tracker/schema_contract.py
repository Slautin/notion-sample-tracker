from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from notion_sample_tracker.models import ARCHIVE_COMPLETE, ARCHIVE_FAILED, ARCHIVE_PENDING


@dataclass(frozen=True)
class PropertyRequirement:
    name: str
    notion_type: str
    options: tuple[str, ...] = ()


@dataclass(frozen=True)
class SchemaIssue:
    database: str
    property_name: str
    message: str


ARCHIVE_STATUS_OPTIONS = (ARCHIVE_PENDING, ARCHIVE_COMPLETE, ARCHIVE_FAILED)

TRACKER_SCHEMA: dict[str, tuple[PropertyRequirement, ...]] = {
    "Samples": (
        PropertyRequirement("Name", "title"),
        PropertyRequirement("Sample Type", "select"),
        PropertyRequirement("Composition", "rich_text"),
        PropertyRequirement("Elements", "multi_select"),
        PropertyRequirement("Synthesis", "multi_select"),
        PropertyRequirement("Synthesis Details", "rich_text"),
        PropertyRequirement("Processing", "multi_select"),
        PropertyRequirement("Processing Details", "rich_text"),
        PropertyRequirement("Status", "select"),
        PropertyRequirement("Parent Sample", "relation"),
        PropertyRequirement("Source", "relation"),
        PropertyRequirement("QRCode", "files"),
        PropertyRequirement("Photos", "files"),
        PropertyRequirement("Submission ID", "rich_text"),
        PropertyRequirement("Archive Status", "select", ARCHIVE_STATUS_OPTIONS),
        PropertyRequirement("Archive Error", "rich_text"),
    ),
    "Results": (
        PropertyRequirement("Name", "title"),
        PropertyRequirement("Data Type", "select"),
        PropertyRequirement("Brief Description", "rich_text"),
        PropertyRequirement("Characterisation", "multi_select"),
        PropertyRequirement("Upload Method", "select"),
        PropertyRequirement("Parent Entry", "select"),
        PropertyRequirement("Link", "url"),
        PropertyRequirement("Sample", "relation"),
        PropertyRequirement("Related Results", "relation"),
        PropertyRequirement("Source", "relation"),
        PropertyRequirement("QRCode", "files"),
        PropertyRequirement("Submission ID", "rich_text"),
        PropertyRequirement("Archive Status", "select", ARCHIVE_STATUS_OPTIONS),
        PropertyRequirement("Archive Error", "rich_text"),
    ),
    "People": (
        PropertyRequirement("Person", "title"),
        PropertyRequirement("Email", "email"),
        PropertyRequirement("Affiliation", "rich_text"),
    ),
}


def validate_tracker_schema(collections: dict[str, dict[str, Any]]) -> list[SchemaIssue]:
    issues: list[SchemaIssue] = []
    for database_name, requirements in TRACKER_SCHEMA.items():
        collection = collections.get(database_name) or {}
        issues.extend(validate_collection(database_name, collection, requirements))
    return issues


def validate_collection(
    database_name: str,
    collection: dict[str, Any],
    requirements: tuple[PropertyRequirement, ...],
) -> list[SchemaIssue]:
    properties = collection.get("properties", {})
    issues: list[SchemaIssue] = []
    for requirement in requirements:
        prop = properties.get(requirement.name)
        if not prop:
            issues.append(SchemaIssue(database_name, requirement.name, "missing property"))
            continue
        actual_type = prop.get("type")
        if actual_type != requirement.notion_type:
            issues.append(
                SchemaIssue(
                    database_name,
                    requirement.name,
                    f"expected {requirement.notion_type}, found {actual_type or 'unknown'}",
                )
            )
            continue
        missing_options = _missing_options(prop, requirement)
        if missing_options:
            issues.append(
                SchemaIssue(
                    database_name,
                    requirement.name,
                    "missing options: " + ", ".join(missing_options),
                )
            )
    return issues


def _missing_options(prop: dict[str, Any], requirement: PropertyRequirement) -> list[str]:
    if not requirement.options:
        return []
    options = prop.get(requirement.notion_type, {}).get("options", [])
    names = {item.get("name") for item in options}
    return [option for option in requirement.options if option not in names]
