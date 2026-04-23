from __future__ import annotations

import mimetypes
from typing import Any

import requests
from notion_client import Client

from notion_sample_tracker.models import PersonRef, ResultForm, SampleForm
from notion_sample_tracker.services.formula import FormulaParser, ParsedFormula


class NotionRepository:
    def __init__(self, token: str, samples_db: str, results_db: str, people_db: str, formula_parser: FormulaParser):
        self.token = token
        self.client = Client(auth=token)
        self.samples_db = samples_db
        self.results_db = results_db
        self.people_db = people_db
        self.formula_parser = formula_parser

    def list_samples(self) -> list[dict[str, str]]:
        return self._list_titles(self.samples_db)

    def list_results(self) -> list[dict[str, str]]:
        return self._list_titles(self.results_db)

    def get_options(self) -> dict[str, list[str]]:
        char_property = self._first_existing_property(self.results_db, ["Characterisation", "Characterization"])
        return {
            "synthesis": self._database_property_options(self.samples_db, "Synthesis", "multi_select"),
            "processing": self._database_property_options(self.samples_db, "Processing", "multi_select"),
            "source": [item["name"] for item in self._list_titles(self.people_db)],
            "entry_type": self._database_property_options(self.results_db, "Data Type", "select"),
            "char_data": self._database_property_options(self.results_db, char_property, "multi_select") if char_property else [],
        }

    def create_sample(self, form: SampleForm) -> dict[str, Any]:
        parsed = self._parse_sample_formula(form)
        source_relations = self._source_relations(form.sources)
        properties = self._sample_properties(form, parsed, source_relations)
        return self._create_page(self.samples_db, properties)

    def update_sample(self, page_id: str, form: SampleForm) -> dict[str, Any]:
        parsed = self._parse_sample_formula(form)
        source_relations = self._source_relations(form.sources)
        properties = self._sample_properties(form, parsed, source_relations)
        return self.client.pages.update(page_id=page_id, properties=properties)

    def create_result(self, form: ResultForm) -> dict[str, Any]:
        source_relations = self._source_relations(form.sources)
        properties = self._result_properties(form, source_relations)
        return self._create_page(self.results_db, properties)

    def sample_exists(self, name: str) -> bool:
        return self._find_by_any_title(self.samples_db, name) is not None

    def update_result(self, page_id: str, form: ResultForm) -> dict[str, Any]:
        source_relations = self._source_relations(form.sources)
        properties = self._result_properties(form, source_relations)
        return self.client.pages.update(page_id=page_id, properties=properties)

    def attach_external_file(self, page_id: str, property_name: str, name: str, url: str) -> dict[str, Any]:
        existing_files = self._existing_files(page_id, property_name)
        return self.client.pages.update(
            page_id=page_id,
            properties={
                property_name: {
                    "files": existing_files
                    + [
                        {
                            "name": name,
                            "type": "external",
                            "external": {"url": url},
                        }
                    ]
                }
            },
        )

    def attach_external_files(self, page_id: str, property_name: str, files: list[dict[str, str]]) -> dict[str, Any] | None:
        if not files:
            return None
        existing_files = self._existing_files(page_id, property_name)
        external_files = [
            {"name": item["name"], "type": "external", "external": {"url": item["url"]}}
            for item in files
            if item.get("name") and item.get("url")
        ]
        if not external_files:
            return None
        return self.client.pages.update(
            page_id=page_id,
            properties={property_name: {"files": existing_files + external_files}},
        )

    def attach_uploaded_file(self, page_id: str, property_name: str, name: str, content: bytes, content_type: str | None = None) -> dict[str, Any]:
        file_id = self._upload_file_to_notion(name, content, content_type or mimetypes.guess_type(name)[0] or "application/octet-stream")
        existing_files = self._existing_files(page_id, property_name)
        return self.client.pages.update(
            page_id=page_id,
            properties={
                property_name: {
                    "files": existing_files
                    + [
                        {
                            "name": name,
                            "type": "file_upload",
                            "file_upload": {"id": file_id},
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
            parent_id = self._resolve_page_id(self.samples_db, form.parent_sample_id)
            properties["Parent Sample"] = {"relation": [{"id": parent_id}]}
            if not parsed:
                parent_page = self.client.pages.retrieve(page_id=parent_id)
                for property_name in ("Composition", "Elements"):
                    parent_property = parent_page.get("properties", {}).get(property_name)
                    if parent_property:
                        properties[property_name] = self._page_property_value(parent_property)
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
        sample_id = self._resolve_page_id(self.samples_db, form.sample_id) if form.sample_id else ""
        related_result_id = self._resolve_page_id(self.results_db, form.related_result_id) if form.related_result_id else ""
        sample_relation = [{"id": sample_id}] if sample_id else []
        if not sample_relation and related_result_id:
            sample_relation = self._relation_from_page(related_result_id, "Sample")

        properties: dict[str, Any] = {
            "Name": {"title": [{"text": {"content": form.name}}]},
        }
        if sample_relation:
            properties["Sample"] = {"relation": sample_relation}
        if form.data_type:
            properties["Data Type"] = {"select": {"name": form.data_type}}
        if form.upload_method:
            properties["Upload Method"] = {"select": {"name": form.upload_method}}
        if form.description:
            properties["Brief Description"] = self._rich_text(form.description)
        if form.characterization:
            char_property = self._first_existing_property(self.results_db, ["Characterisation", "Characterization"]) or "Characterization"
            properties[char_property] = self._multi_select(form.characterization)
        if form.link:
            properties["Link"] = {"url": form.link}
        if related_result_id:
            properties["Related Results"] = {"relation": [{"id": related_result_id}]}
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
        return self._create_page(self.people_db, properties)

    def _resolve_page_id(self, database_id: str, value: str) -> str:
        value = value.strip()
        if not value:
            return ""
        if "-" in value and len(value) >= 32:
            return value
        existing = self._find_by_any_title(database_id, value)
        if not existing:
            raise ValueError(f"Could not find Notion page named '{value}'")
        return existing["id"]

    def _find_by_title(self, database_id: str, property_name: str, title: str) -> dict[str, Any] | None:
        response = self._query_collection(database_id, filter={"property": property_name, "title": {"equals": title}}, page_size=1)
        results = response.get("results", [])
        return results[0] if results else None

    def _find_by_any_title(self, database_id: str, title: str) -> dict[str, Any] | None:
        title_property = self._title_property_name(database_id)
        return self._find_by_title(database_id, title_property, title)

    def _list_titles(self, database_id: str) -> list[dict[str, str]]:
        pages: list[dict[str, Any]] = []
        cursor = None
        while True:
            kwargs: dict[str, Any] = {"page_size": 100}
            if cursor:
                kwargs["start_cursor"] = cursor
            response = self._query_collection(database_id, **kwargs)
            pages.extend(response.get("results", []))
            if not response.get("has_more"):
                break
            cursor = response.get("next_cursor")

        return [{"id": page["id"], "name": self._title_from_page(page)} for page in pages]

    def _database_property_options(self, database_id: str, property_name: str, property_type: str) -> list[str]:
        database = self._retrieve_collection(database_id)
        prop = database.get("properties", {}).get(property_name)
        if not prop or prop.get("type") != property_type:
            return []
        return [item["name"] for item in prop[property_type].get("options", [])]

    def _first_existing_property(self, database_id: str, names: list[str]) -> str:
        database = self._retrieve_collection(database_id)
        properties = database.get("properties", {})
        for name in names:
            if name in properties:
                return name
        return ""

    def _title_property_name(self, database_id: str) -> str:
        database = self._retrieve_collection(database_id)
        for name, prop in database.get("properties", {}).items():
            if prop.get("type") == "title":
                return name
        return "Name"

    def _relation_from_page(self, page_id: str, property_name: str) -> list[dict[str, str]]:
        page = self.client.pages.retrieve(page_id=page_id)
        relation = page.get("properties", {}).get(property_name, {}).get("relation", [])
        return [{"id": item["id"]} for item in relation if item.get("id")]

    def _existing_files(self, page_id: str, property_name: str) -> list[dict[str, Any]]:
        page = self.client.pages.retrieve(page_id=page_id)
        prop = page.get("properties", {}).get(property_name, {})
        files = prop.get("files", []) if prop.get("type") == "files" else []
        kept = []
        for item in files:
            file_type = item.get("type")
            if file_type == "external":
                kept.append({"name": item.get("name", "file"), "type": "external", "external": item["external"]})
            elif file_type == "file_upload":
                kept.append({"name": item.get("name", "file"), "type": "file_upload", "file_upload": item["file_upload"]})
        return kept

    def _upload_file_to_notion(self, name: str, content: bytes, content_type: str) -> str:
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Notion-Version": "2025-09-03",
        }
        response = requests.post("https://api.notion.com/v1/file_uploads", headers=headers, timeout=60)
        response.raise_for_status()
        upload_url = response.json()["upload_url"]
        upload_response = requests.post(
            upload_url,
            headers=headers,
            files={"file": (name, content, content_type)},
            data={"part_number": "1"},
            timeout=120,
        )
        upload_response.raise_for_status()
        return upload_response.json()["id"]

    def _query_collection(self, collection_id: str, **kwargs) -> dict[str, Any]:
        if hasattr(self.client, "data_sources"):
            try:
                return self.client.data_sources.query(data_source_id=collection_id, **kwargs)
            except Exception:
                pass
        return self.client.databases.query(database_id=collection_id, **kwargs)

    def _retrieve_collection(self, collection_id: str) -> dict[str, Any]:
        if hasattr(self.client, "data_sources"):
            try:
                return self.client.data_sources.retrieve(data_source_id=collection_id)
            except Exception:
                pass
        return self.client.databases.retrieve(database_id=collection_id)

    def _create_page(self, collection_id: str, properties: dict[str, Any]) -> dict[str, Any]:
        if hasattr(self.client, "data_sources"):
            try:
                return self.client.pages.create(parent={"data_source_id": collection_id}, properties=properties)
            except Exception:
                pass
        return self.client.pages.create(parent={"database_id": collection_id}, properties=properties)

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

    @staticmethod
    def _page_property_value(property_value: dict[str, Any]) -> dict[str, Any]:
        property_type = property_value.get("type")
        if property_type and property_type in property_value:
            return {property_type: property_value[property_type]}
        return property_value
