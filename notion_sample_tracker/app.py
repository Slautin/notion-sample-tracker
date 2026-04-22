from __future__ import annotations

import json
import io
import os
from pathlib import Path
from typing import Callable

from flask import Flask, flash, jsonify, redirect, render_template, request, send_file, url_for

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
        notion_home_url = _notion_home_url(settings)
        if notion_home_url:
            return redirect(notion_home_url)
        return render_template("index.html")

    @app.get("/add_sample")
    def add_sample():
        return render_template("add_sample.html")

    @app.get("/add_results")
    def add_results():
        return render_template("add_results.html")

    @app.get("/samples/new")
    def new_sample():
        return render_template("add_sample.html")

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
        return render_template("add_results.html")

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

    @app.get("/api/options")
    def api_options():
        try:
            return jsonify({"success": True, "options": notion.get_options()})
        except Exception as exc:
            return jsonify({"success": False, "error": str(exc)}), 500

    @app.get("/api/parent-samples")
    def api_parent_samples():
        try:
            return jsonify({"success": True, "samples": [{"name": item["name"]} for item in notion.list_samples()]})
        except Exception as exc:
            return jsonify({"success": False, "error": str(exc)}), 500

    @app.get("/api/parent-datasets")
    def api_parent_datasets():
        try:
            return jsonify({"success": True, "datasets": [{"id": item["name"], "name": item["name"]} for item in notion.list_results()]})
        except Exception as exc:
            return jsonify({"success": False, "error": str(exc)}), 500

    @app.post("/api/submit")
    def api_submit_sample():
        form = SampleForm.from_form(request.form)
        backlog.append(BacklogEvent(action="create", entity="sample", payload=_form_payload(request.form)))
        try:
            page = notion.create_sample(form)
            uploaded_paths = _archive_uploads(onedrive, f"samples/{page['id']}/files", request.files.getlist("photos"))
            snapshot = onedrive.upload_json(
                f"samples/{page['id']}/record.json",
                {"form": form.to_dict(), "raw_form": _form_payload(request.form), "notion": page, "files": uploaded_paths},
            )
            backlog.append(
                BacklogEvent(
                    action="create",
                    entity="sample",
                    payload=form.to_dict(),
                    notion_page_id=page["id"],
                    onedrive_paths=[snapshot.path, *uploaded_paths],
                    status="complete",
                )
            )
            return jsonify({"success": True, "message": "Sample submitted successfully"})
        except Exception as exc:
            backlog.append(BacklogEvent(action="create", entity="sample", payload=form.to_dict(), status="failed", error=str(exc)))
            return jsonify({"success": False, "error": str(exc)}), 400

    @app.post("/api/submit-data")
    def api_submit_result():
        form = ResultForm.from_form(request.form)
        backlog.append(BacklogEvent(action="create", entity="result", payload=_form_payload(request.form)))
        try:
            page = notion.create_result(form)
            snapshot = onedrive.upload_json(
                f"results/{page['id']}/record.json",
                {"form": form.to_dict(), "raw_form": _form_payload(request.form), "notion": page},
            )
            backlog.append(
                BacklogEvent(
                    action="create",
                    entity="result",
                    payload=form.to_dict(),
                    notion_page_id=page["id"],
                    onedrive_paths=[snapshot.path],
                    status="complete",
                )
            )
            return jsonify({"success": True, "message": "Data submitted successfully"})
        except Exception as exc:
            backlog.append(BacklogEvent(action="create", entity="result", payload=form.to_dict(), status="failed", error=str(exc)))
            return jsonify({"success": False, "error": str(exc)}), 400

    @app.post("/api/create-upload-session")
    def api_create_upload_session():
        try:
            data = request.get_json(force=True)
            filename = Path(data.get("filename", "")).name
            entry_name = _safe_segment(data.get("entry_name", "data_entry"))
            if not filename:
                return jsonify({"success": False, "error": "filename is required"}), 400
            session = onedrive.create_upload_session(f"results/{entry_name}/{filename}")
            return jsonify({"success": True, **session})
        except Exception as exc:
            return jsonify({"success": False, "error": str(exc)}), 500

    @app.post("/api/save-json")
    def api_save_json():
        data = _form_payload(request.form)
        data["_meta"] = {"exported_from": "sample_submission_form"}
        filename_base = _safe_segment(data.get("sample_name") or data.get("name") or "sample")
        buffer = io.BytesIO(json.dumps(data, indent=2).encode("utf-8"))
        buffer.seek(0)
        return send_file(buffer, mimetype="application/json", as_attachment=True, download_name=f"{filename_base}.json")

    @app.post("/api/load-json")
    def api_load_json():
        uploaded = request.files.get("json_file")
        if not uploaded:
            return jsonify({"success": False, "error": "No JSON file provided"}), 400
        try:
            data = json.load(uploaded)
        except json.JSONDecodeError:
            return jsonify({"success": False, "error": "Invalid JSON format"}), 400
        data.pop("_meta", None)
        data.pop("photos", None)
        return jsonify({"success": True, "data": data})

    @app.get("/health")
    def health():
        notion_home_url = _notion_home_url(settings)
        return {
            "status": "ok",
            "notion_home_configured": bool(notion_home_url),
            "notion_home_url_host": _url_host(notion_home_url),
            "public_base_url": settings.public_base_url,
        }

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


def _form_payload(form) -> dict:
    return {key: form.get(key) for key in form.keys()}


def _safe_segment(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in str(value or "").strip())
    return cleaned or "entry"


def _notion_home_url(settings: Settings) -> str:
    return (
        os.getenv("NOTION_HOME_URL", "").strip()
        or os.getenv("NOTION_PAGE", "").strip()
        or settings.notion_home_url.strip()
    )


def _url_host(url: str) -> str:
    if "://" not in url:
        return ""
    return url.split("://", 1)[1].split("/", 1)[0]
