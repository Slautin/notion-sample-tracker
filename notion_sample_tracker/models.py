from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
from typing import Any


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _json_items(value: str | None) -> list[dict[str, Any]]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def names_from_json(value: str | None) -> list[str]:
    names: list[str] = []
    for item in _json_items(value):
        name = str(item.get("name", "")).strip()
        if name:
            names.append(name)
    return names


def people_from_json(value: str | None) -> list["PersonRef"]:
    people: list[PersonRef] = []
    for item in _json_items(value):
        name = str(item.get("name", "")).strip()
        if not name:
            continue
        people.append(
            PersonRef(
                name=name,
                email=str(item.get("email") or "").strip(),
                affiliation=str(item.get("affiliation") or "").strip(),
            )
        )
    return people


@dataclass
class PersonRef:
    name: str
    email: str = ""
    affiliation: str = ""
    notion_id: str = ""


@dataclass
class SampleForm:
    name: str
    sample_type: str
    composition: str = ""
    parent_sample_id: str = ""
    synthesis: list[str] = field(default_factory=list)
    synthesis_details: str = ""
    processing: list[str] = field(default_factory=list)
    processing_details: str = ""
    status: str = ""
    location: str = ""
    sources: list[PersonRef] = field(default_factory=list)

    @classmethod
    def from_form(cls, form: Any) -> "SampleForm":
        sources = people_from_json(form.get("sources_data"))
        if not sources and form.get("source_name", "").strip():
            sources = [
                PersonRef(
                    name=form.get("source_name", "").strip(),
                    email=form.get("source_email", "").strip(),
                    affiliation=form.get("source_affiliation", "").strip(),
                )
            ]
        return cls(
            name=(form.get("sample_name") or form.get("name", "")).strip(),
            sample_type=form.get("sample_type", "").strip(),
            composition=form.get("composition", "").strip(),
            parent_sample_id=(form.get("parent_sample") or form.get("parent_sample_id", "")).strip(),
            synthesis=split_csv(form.get("synthesis")),
            synthesis_details=form.get("synthesis_details", "").strip(),
            processing=names_from_json(form.get("processing_data")) or split_csv(form.get("processing")),
            processing_details=form.get("processing_details", "").strip(),
            status=form.get("status", "").strip(),
            location=form.get("location", "").strip(),
            sources=sources,
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ResultForm:
    name: str
    sample_id: str
    data_type: str
    upload_method: str
    description: str = ""
    characterization: list[str] = field(default_factory=list)
    link: str = ""
    related_result_id: str = ""
    sources: list[PersonRef] = field(default_factory=list)

    @classmethod
    def from_form(cls, form: Any) -> "ResultForm":
        sources = people_from_json(form.get("sources_data"))
        if not sources and form.get("source_name", "").strip():
            sources = [
                PersonRef(
                    name=form.get("source_name", "").strip(),
                    email=form.get("source_email", "").strip(),
                    affiliation=form.get("source_affiliation", "").strip(),
                )
            ]
        upload_method = (form.get("upload_method") or form.get("data_type", "")).strip()
        link = (form.get("link") or form.get("data_link") or form.get("onedrive_path") or "").strip()
        related_result_id = (form.get("related_result_id") or form.get("parent_dataset") or "").strip()
        sample_id = (form.get("sample_id") or form.get("parent_sample") or "").strip()
        return cls(
            name=form.get("name", "").strip(),
            sample_id=sample_id,
            data_type=(form.get("entry_type") or form.get("data_type", "")).strip(),
            upload_method=upload_method,
            description=(form.get("brief_description") or form.get("description", "")).strip(),
            characterization=names_from_json(form.get("char_data")) or split_csv(form.get("characterization")),
            link=link,
            related_result_id=related_result_id,
            sources=sources,
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class BacklogEvent:
    action: str
    entity: str
    payload: dict[str, Any]
    notion_page_id: str = ""
    onedrive_paths: list[str] = field(default_factory=list)
    status: str = "pending"
    error: str = ""
    created_at: str = field(default_factory=now_iso)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
