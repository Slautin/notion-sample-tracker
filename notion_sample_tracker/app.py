from __future__ import annotations

from pathlib import Path
from typing import Callable

from flask import Flask, flash, redirect, render_template, request, url_for

from notion_sample_tracker.models import BacklogEvent, ResultForm, SampleForm
from notion_sample_tracker.services.backlog import JsonlBacklog
from notion_sample_tracker.services.formula import FormulaParser
from notion_sample_tracker.services.notion_client import NotionRepository
from notion_sample_tracker.services.onedrive_client import OneDriveClient
from notion_sample_tracker.settings import Settings


def create_app(settings_factory: Callable[[], Settings] = Settings.from_env) -> Flask:
    settings = settings_factory()
    app = Flask(__name__)
    app.config["SECRET_KEY"] = settings.app_secret_key
    app.config["MAX_CONTENT_LENGTH"] = settings.max_upload_mb * 1024 * 1024

    formula_parser = FormulaParser()
    backlog = JsonlBacklog(settings.backlog_dir)
    notion = NotionRepository(
        token=settings.notion_token,
        samples_db=settings.notion_samples_database_id,
        results_db=settings.notion_results_database_id,
        people_db=settings.notion_people_database_id,
        formula_parser=formula_parser,
    )
    onedrive = OneDriveClient(
        tenant_id=settings.onedrive_tenant_id,
        client_id=settings.onedrive_client_id,
        client_secret=settings.onedrive_client_secret,
        drive_id=settings.onedrive_drive_id,
        root_folder=settings.onedrive_root_folder,
    )

    @app.get("/")
    def index():
        return render_template("index.html")

    @app.get("/samples/new")
    def new_sample():
        return render_template("sample_form.html", mode="create", sample=None, samples=_safe_list(notion.list_samples))

    @app.post("/samples")
    def create_sample():
        form = SampleForm.from_form(request.form)
        event = BacklogEvent(action="create", entity="sample", payload=form.to_dict())
        backlog.append(event)
        try:
            page = notion.create_sample(form)
            snapshot_path = f"samples/{page['id']}/record.json"
            onedrive_result = onedrive.upload_json(snapshot_path, {"form": form.to_dict(), "notion": page})
            backlog.append(
                BacklogEvent(
                    action="create",
                    entity="sample",
                    payload=form.to_dict(),
                    notion_page_id=page["id"],
                    onedrive_paths=[onedrive_result.path],
                    status="complete",
                )
            )
            flash("Sample created in Notion and archived to OneDrive.", "success")
            return redirect(url_for("index"))
        except Exception as exc:
            backlog.append(BacklogEvent(action="create", entity="sample", payload=form.to_dict(), status="failed", error=str(exc)))
            flash(str(exc), "error")
            return render_template("sample_form.html", mode="create", sample=form, samples=_safe_list(notion.list_samples)), 400

    @app.get("/samples/<page_id>/edit")
    def edit_sample(page_id: str):
        return render_template("sample_form.html", mode="edit", page_id=page_id, sample=None, samples=_safe_list(notion.list_samples))

    @app.post("/samples/<page_id>")
    def update_sample(page_id: str):
        form = SampleForm.from_form(request.form)
        backlog.append(BacklogEvent(action="update", entity="sample", payload=form.to_dict(), notion_page_id=page_id))
        try:
            page = notion.update_sample(page_id, form)
            onedrive_result = onedrive.upload_json(f"samples/{page_id}/record.json", {"form": form.to_dict(), "notion": page})
            backlog.append(
                BacklogEvent(
                    action="update",
                    entity="sample",
                    payload=form.to_dict(),
                    notion_page_id=page_id,
                    onedrive_paths=[onedrive_result.path],
                    status="complete",
                )
            )
            flash("Sample revised in Notion and archived to OneDrive.", "success")
            return redirect(url_for("index"))
        except Exception as exc:
            backlog.append(BacklogEvent(action="update", entity="sample", payload=form.to_dict(), notion_page_id=page_id, status="failed", error=str(exc)))
            flash(str(exc), "error")
            return render_template("sample_form.html", mode="edit", page_id=page_id, sample=form, samples=_safe_list(notion.list_samples)), 400

    @app.get("/results/new")
    def new_result():
        return render_template(
            "result_form.html",
            mode="create",
            result=None,
            samples=_safe_list(notion.list_samples),
            results=_safe_list(notion.list_results),
        )

    @app.post("/results")
    def create_result():
        form = ResultForm.from_form(request.form)
        event = BacklogEvent(action="create", entity="result", payload=form.to_dict())
        backlog.append(event)
        try:
            uploaded_paths = _archive_uploads(onedrive, "results/pending", request.files.getlist("files"))
            page = notion.create_result(form)
            snapshot = onedrive.upload_json(f"results/{page['id']}/record.json", {"form": form.to_dict(), "notion": page, "files": uploaded_paths})
            backlog.append(
                BacklogEvent(
                    action="create",
                    entity="result",
                    payload=form.to_dict(),
                    notion_page_id=page["id"],
                    onedrive_paths=[snapshot.path, *uploaded_paths],
                    status="complete",
                )
            )
            flash("Data entry created in Notion and archived to OneDrive.", "success")
            return redirect(url_for("index"))
        except Exception as exc:
            backlog.append(BacklogEvent(action="create", entity="result", payload=form.to_dict(), status="failed", error=str(exc)))
            flash(str(exc), "error")
            return render_template("result_form.html", mode="create", result=form, samples=_safe_list(notion.list_samples), results=_safe_list(notion.list_results)), 400

    @app.get("/results/<page_id>/edit")
    def edit_result(page_id: str):
        return render_template(
            "result_form.html",
            mode="edit",
            page_id=page_id,
            result=None,
            samples=_safe_list(notion.list_samples),
            results=_safe_list(notion.list_results),
        )

    @app.post("/results/<page_id>")
    def update_result(page_id: str):
        form = ResultForm.from_form(request.form)
        backlog.append(BacklogEvent(action="update", entity="result", payload=form.to_dict(), notion_page_id=page_id))
        try:
            uploaded_paths = _archive_uploads(onedrive, f"results/{page_id}/files", request.files.getlist("files"))
            page = notion.update_result(page_id, form)
            snapshot = onedrive.upload_json(f"results/{page_id}/record.json", {"form": form.to_dict(), "notion": page, "files": uploaded_paths})
            backlog.append(
                BacklogEvent(
                    action="update",
                    entity="result",
                    payload=form.to_dict(),
                    notion_page_id=page_id,
                    onedrive_paths=[snapshot.path, *uploaded_paths],
                    status="complete",
                )
            )
            flash("Data entry revised in Notion and archived to OneDrive.", "success")
            return redirect(url_for("index"))
        except Exception as exc:
            backlog.append(BacklogEvent(action="update", entity="result", payload=form.to_dict(), notion_page_id=page_id, status="failed", error=str(exc)))
            flash(str(exc), "error")
            return render_template("result_form.html", mode="edit", page_id=page_id, result=form, samples=_safe_list(notion.list_samples), results=_safe_list(notion.list_results)), 400

    @app.get("/backlog")
    def backlog_view():
        return render_template("backlog.html", samples=backlog.recent("sample"), results=backlog.recent("result"))

    @app.get("/health")
    def health():
        return {"status": "ok"}

    return app


def _safe_list(loader: Callable[[], list[dict[str, str]]]) -> list[dict[str, str]]:
    try:
        return loader()
    except Exception:
        return []


def _archive_uploads(onedrive: OneDriveClient, prefix: str, files) -> list[str]:
    paths: list[str] = []
    for file_storage in files:
        if not file_storage or not file_storage.filename:
            continue
        safe_name = Path(file_storage.filename).name.replace("/", "_")
        result = onedrive.upload_file(f"{prefix}/{safe_name}", file_storage.stream, file_storage.mimetype)
        paths.append(result.path)
    return paths
