from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


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
        return cls(
            name=form.get("name", "").strip(),
            sample_type=form.get("sample_type", "").strip(),
            composition=form.get("composition", "").strip(),
            parent_sample_id=form.get("parent_sample_id", "").strip(),
            synthesis=split_csv(form.get("synthesis")),
            synthesis_details=form.get("synthesis_details", "").strip(),
            processing=split_csv(form.get("processing")),
            processing_details=form.get("processing_details", "").strip(),
            status=form.get("status", "").strip(),
            location=form.get("location", "").strip(),
            sources=[
                PersonRef(
                    name=form.get("source_name", "").strip(),
                    email=form.get("source_email", "").strip(),
                    affiliation=form.get("source_affiliation", "").strip(),
                )
            ]
            if form.get("source_name", "").strip()
            else [],
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
        return cls(
            name=form.get("name", "").strip(),
            sample_id=form.get("sample_id", "").strip(),
            data_type=form.get("data_type", "").strip(),
            upload_method=form.get("upload_method", "").strip(),
            description=form.get("description", "").strip(),
            characterization=split_csv(form.get("characterization")),
            link=form.get("link", "").strip(),
            related_result_id=form.get("related_result_id", "").strip(),
            sources=[
                PersonRef(
                    name=form.get("source_name", "").strip(),
                    email=form.get("source_email", "").strip(),
                    affiliation=form.get("source_affiliation", "").strip(),
                )
            ]
            if form.get("source_name", "").strip()
            else [],
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
