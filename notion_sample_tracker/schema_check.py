from __future__ import annotations

from notion_sample_tracker.schema_contract import validate_tracker_schema
from notion_sample_tracker.services.formula import FormulaParser
from notion_sample_tracker.services.notion_client import NotionRepository
from notion_sample_tracker.settings import Settings


def main() -> int:
    settings = Settings.from_env()
    notion = NotionRepository(
        token=settings.notion_token,
        samples_db=settings.notion_samples_database_id,
        results_db=settings.notion_results_database_id,
        people_db=settings.notion_people_database_id,
        formula_parser=FormulaParser(),
    )
    collections = {
        "Samples": notion.retrieve_schema(settings.notion_samples_database_id),
        "Results": notion.retrieve_schema(settings.notion_results_database_id),
        "People": notion.retrieve_schema(settings.notion_people_database_id),
    }
    issues = validate_tracker_schema(collections)
    if issues:
        for issue in issues:
            print(f"{issue.database}.{issue.property_name}: {issue.message}")
        return 1
    print("Notion schema contract OK.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
