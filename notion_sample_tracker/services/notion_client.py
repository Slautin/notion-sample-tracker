from __future__ import annotations

from dataclasses import asdict
from typing import Any

from notion_client import Client

from notion_sample_tracker.models import PersonRef, ResultForm, SampleForm
from notion_sample_tracker.services.formula import FormulaParser, ParsedFormula


class NotionRepository:
    def __init__(self, token: str, samples_db: str, results_db: str, people_db: str, formula_parser: FormulaParser):
        self.client = Client(auth=token)
        self.samples_db = samples_db
        self.results_db = results_db
        self.people_db = people_db
        self.formula_parser = formula_parser

    def list_samples(self) -> list[dict[str, str]]:
        return self._list_titles(self.samples_db)

    def list_results(self) -> list[dict[str, str]]:
        return self._list_titles(self.results_db)

    def create_sample(self, form: SampleForm) -> dict[str, Any]:
        parsed = self._parse_sample_formula(form)
        source_relations = self._source_relations(form.sources)
        properties = self._sample_properties(form, parsed, source_relations)
        return self.client.pages.create(parent={"database_id": self.samples_db}, properties=properties)

    def update_sample(self, page_id: str, form: SampleForm) -> dict[str, Any]:
        parsed = self._parse_sample_formula(form)
        source_relations = self._source_relations(form.sources)
        properties = self._sample_properties(form, parsed, source_relations)
        return self.client.pages.update(page_id=page_id, properties=properties)

    def create_result(self, form: ResultForm) -> dict[str, Any]:
        source_relations = self._source_relations(form.sources)
        properties = self._result_properties(form, source_relations)
        return self.client.pages.create(parent={"database_id": self.results_db}, properties=properties)

    def update_result(self, page_id: str, form: ResultForm) -> dict[str, Any]:
        source_relations = self._source_relations(form.sources)
        properties = self._result_properties(form, source_relations)
        return self.client.pages.update(page_id=page_id, properties=properties)

    def attach_external_file(self, page_id: str, property_name: str, name: str, url: str) -> dict[str, Any]:
        return self.client.pages.update(
            page_id=page_id,
            properties={
                property_name: {
                    "files": [
                        {
                            "name": name,
                            "type": "external",
                            "external": {"url": url},
                        }
                    ]
                }
            },
        )

    def _sample_properties(self, form: SampleForm, parsed: ParsedFormula | None, source_relations: list[dict]) -> dict[str, Any]:
        properties: dict[str, Any] = {
            "Name": {"title": [{"text": {"content": form.name}}]},
            "Sample Type": {"select": {"name": form.sample_type}},
        }
        if parsed:
            properties["Composition"] = {"rich_text": [{"text": {"content": parsed.normalized_formula}}]}
            properties["Elements"] = {"multi_select": [{"name": element} for element in parsed.elements]}
        if form.parent_sample_id:
            properties["Parent Sample"] = {"relation": [{"id": form.parent_sample_id}]}
        if form.synthesis:
            properties["Synthesis"] = self._multi_select(form.synthesis)
        if form.synthesis_details:
            properties["Synthesis Details"] = self._rich_text(form.synthesis_details)
        if form.processing:
            properties["Processing"] = self._multi_select(form.processing)
        if form.processing_details:
            properties["Processing Details"] = self._rich_text(form.processing_details)
        if form.status:
            properties["Status"] = {"select": {"name": form.status}}
        if form.location:
            properties["Location"] = self._rich_text(form.location)
        if source_relations:
            properties["Source"] = {"relation": source_relations}
        return properties

    def _result_properties(self, form: ResultForm, source_relations: list[dict]) -> dict[str, Any]:
        properties: dict[str, Any] = {
            "Name": {"title": [{"text": {"content": form.name}}]},
            "Sample": {"relation": [{"id": form.sample_id}]},
        }
        if form.data_type:
            properties["Data Type"] = {"select": {"name": form.data_type}}
        if form.upload_method:
            properties["Upload Method"] = {"select": {"name": form.upload_method}}
        if form.description:
            properties["Brief Description"] = self._rich_text(form.description)
        if form.characterization:
            properties["Characterization"] = self._multi_select(form.characterization)
        if form.link:
            properties["Link"] = {"url": form.link}
        if form.related_result_id:
            properties["Related Results"] = {"relation": [{"id": form.related_result_id}]}
        if source_relations:
            properties["Source"] = {"relation": source_relations}
        return properties

    def _parse_sample_formula(self, form: SampleForm) -> ParsedFormula | None:
        if form.sample_type.lower().startswith("sub") and not form.composition:
            return None
        return self.formula_parser.parse(form.composition)

    def _source_relations(self, sources: list[PersonRef]) -> list[dict[str, str]]:
        relations = []
        for source in sources:
            if source.notion_id:
                relations.append({"id": source.notion_id})
            elif source.name:
                page = self._get_or_create_person(source)
                relations.append({"id": page["id"]})
        return relations

    def _get_or_create_person(self, person: PersonRef) -> dict[str, Any]:
        existing = self._find_by_title(self.people_db, "Person", person.name)
        if existing:
            return existing
        properties = {"Person": {"title": [{"text": {"content": person.name}}]}}
        if person.email:
            properties["Email"] = {"email": person.email}
        if person.affiliation:
            properties["Affiliation"] = self._rich_text(person.affiliation)
        return self.client.pages.create(parent={"database_id": self.people_db}, properties=properties)

    def _find_by_title(self, database_id: str, property_name: str, title: str) -> dict[str, Any] | None:
        response = self.client.databases.query(
            database_id=database_id,
            filter={"property": property_name, "title": {"equals": title}},
            page_size=1,
        )
        results = response.get("results", [])
        return results[0] if results else None

    def _list_titles(self, database_id: str) -> list[dict[str, str]]:
        pages: list[dict[str, Any]] = []
        cursor = None
        while True:
            kwargs: dict[str, Any] = {"database_id": database_id, "page_size": 100}
            if cursor:
                kwargs["start_cursor"] = cursor
            response = self.client.databases.query(**kwargs)
            pages.extend(response.get("results", []))
            if not response.get("has_more"):
                break
            cursor = response.get("next_cursor")

        return [{"id": page["id"], "name": self._title_from_page(page)} for page in pages]

    @staticmethod
    def _title_from_page(page: dict[str, Any]) -> str:
        for prop in page.get("properties", {}).values():
            if prop.get("type") == "title":
                return "".join(item.get("plain_text", "") for item in prop.get("title", []))
        return page["id"]

    @staticmethod
    def _rich_text(value: str) -> dict[str, Any]:
        return {"rich_text": [{"text": {"content": value}}]}

    @staticmethod
    def _multi_select(values: list[str]) -> dict[str, Any]:
        return {"multi_select": [{"name": value} for value in values]}
