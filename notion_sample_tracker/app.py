from __future__ import annotations

import json
import io
import os
import time
from pathlib import Path
from typing import Any, Callable

from flask import Flask, abort, flash, jsonify, redirect, render_template, request, send_file, url_for

from notion_sample_tracker.models import ARCHIVE_COMPLETE, ARCHIVE_FAILED, ARCHIVE_PENDING, BacklogEvent, ResultForm, SampleForm
from notion_sample_tracker.safety import is_allowed_extension, redact_for_log, safe_path_segment, safe_upload_filename
from notion_sample_tracker.services.backlog import JsonlBacklog
from notion_sample_tracker.services.formula import FormulaParser
from notion_sample_tracker.services.notion_client import NotionRepository
from notion_sample_tracker.services.onedrive_client import OneDriveClient
from notion_sample_tracker.services.pdf_receipt import make_receipt_pdf
from notion_sample_tracker.services.qrcode_service import make_qr_png_bytes
from notion_sample_tracker.settings import Settings


def create_app(settings_factory: Callable[[], Settings] = Settings.from_env) -> Flask:
    settings = settings_factory()
    app = Flask(__name__)
    app.config["SECRET_KEY"] = settings.app_secret_key
    app.config["MAX_CONTENT_LENGTH"] = settings.max_upload_mb * 1024 * 1024
    app.config["SAMPLE_TRACKER_SETTINGS"] = settings

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
        auth_mode=settings.onedrive_auth_mode,
        public_client=settings.onedrive_public_client,
        drive_id=settings.onedrive_drive_id,
        refresh_token=settings.onedrive_refresh_token,
        root_folder=settings.onedrive_root_folder,
        timeout=settings.onedrive_timeout_seconds,
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
        page: dict[str, Any] | None = None
        try:
            _validate_sample_form(form, notion)
            page = notion.create_sample(form)
            snapshot_path = f"samples/{page['id']}/record.json"
            onedrive_result = onedrive.upload_json(snapshot_path, {"form": form.to_dict(), "notion": page})
            notion.update_archive_status(page["id"], ARCHIVE_COMPLETE)
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
            if page:
                _safe_archive_status(notion, page["id"], ARCHIVE_FAILED, str(exc))
            backlog.append(BacklogEvent(action="create", entity="sample", payload=form.to_dict(), notion_page_id=page["id"] if page else "", status="failed", error=str(exc)))
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
            notion.update_archive_status(page_id, ARCHIVE_COMPLETE)
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
            _safe_archive_status(notion, page_id, ARCHIVE_FAILED, str(exc))
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
        page: dict[str, Any] | None = None
        try:
            _validate_result_raw(_form_payload(request.form), notion, preflight=True)
            uploaded_paths = _archive_uploads(onedrive, "results/pending", request.files.getlist("files"), settings)
            page = notion.create_result(form)
            snapshot = onedrive.upload_json(f"results/{page['id']}/record.json", {"form": form.to_dict(), "notion": page, "files": uploaded_paths})
            notion.update_archive_status(page["id"], ARCHIVE_COMPLETE)
            backlog.append(
                BacklogEvent(
                    action="create",
                    entity="result",
                    payload=form.to_dict(),
                    notion_page_id=page["id"],
                    onedrive_paths=[snapshot.path, *[item["path"] for item in uploaded_paths]],
                    status="complete",
                )
            )
            flash("Data entry created in Notion and archived to OneDrive.", "success")
            return redirect(url_for("index"))
        except Exception as exc:
            if page:
                _safe_archive_status(notion, page["id"], ARCHIVE_FAILED, str(exc))
            backlog.append(BacklogEvent(action="create", entity="result", payload=form.to_dict(), notion_page_id=page["id"] if page else "", status="failed", error=str(exc)))
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
            uploaded_paths = _archive_uploads(onedrive, f"results/{page_id}/files", request.files.getlist("files"), settings)
            page = notion.update_result(page_id, form)
            snapshot = onedrive.upload_json(f"results/{page_id}/record.json", {"form": form.to_dict(), "notion": page, "files": uploaded_paths})
            notion.update_archive_status(page_id, ARCHIVE_COMPLETE)
            backlog.append(
                BacklogEvent(
                    action="update",
                    entity="result",
                    payload=form.to_dict(),
                    notion_page_id=page_id,
                    onedrive_paths=[snapshot.path, *[item["path"] for item in uploaded_paths]],
                    status="complete",
                )
            )
            flash("Data entry revised in Notion and archived to OneDrive.", "success")
            return redirect(url_for("index"))
        except Exception as exc:
            _safe_archive_status(notion, page_id, ARCHIVE_FAILED, str(exc))
            backlog.append(BacklogEvent(action="update", entity="result", payload=form.to_dict(), notion_page_id=page_id, status="failed", error=str(exc)))
            flash(str(exc), "error")
            return render_template("result_form.html", mode="edit", page_id=page_id, result=form, samples=_safe_list(notion.list_samples), results=_safe_list(notion.list_results)), 400

    @app.get("/backlog")
    def backlog_view():
        if not settings.enable_backlog_view:
            abort(404)
        limit = _bounded_int(request.args.get("limit"), default=settings.backlog_read_limit, minimum=1, maximum=250)
        return render_template("backlog.html", samples=backlog.recent("sample", limit=limit), results=backlog.recent("result", limit=limit))

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

    @app.post("/api/validate-sample")
    def api_validate_sample():
        try:
            form = SampleForm.from_form(request.form)
            _validate_sample_form(form, notion)
            return jsonify({"success": True})
        except Exception as exc:
            return jsonify({"success": False, "error": str(exc)}), 400

    @app.post("/api/validate-result")
    def api_validate_result():
        try:
            _validate_result_raw(_form_payload(request.form), notion, preflight=True)
            return jsonify({"success": True})
        except Exception as exc:
            return jsonify({"success": False, "error": str(exc)}), 400

    @app.post("/api/submit")
    def api_submit_sample():
        form = SampleForm.from_form(request.form)
        backlog.append(BacklogEvent(action="create", entity="sample", payload=_form_payload(request.form)))
        page: dict[str, Any] | None = None
        try:
            _validate_sample_form(form, notion)
            page = notion.sample_page_by_submission(form.submission_id)
            if page and notion.archive_status_from_page(page) == ARCHIVE_COMPLETE:
                return jsonify(
                    {
                        "success": True,
                        "already_processed": True,
                        "message": "Sample submission was already processed",
                        "receipt": _sample_receipt(form, page),
                    }
                )
            if page:
                app_log("sample_submission_retry", page_id=page["id"], submission_id=form.submission_id)
                notion.update_archive_status(page["id"], ARCHIVE_PENDING)
            else:
                started = time.perf_counter()
                page = notion.create_sample(form)
                app_log("notion_sample_created", page_id=page["id"], seconds=round(time.perf_counter() - started, 3))
            sample_folder = notion.sample_storage_info_from_page(page)["folder"]
            uploaded_files, photo_errors = _archive_sample_photos(notion, onedrive, page["id"], f"{sample_folder}/photos", request.files.getlist("photos"), settings)
            qr_name = f"{_safe_segment(form.name)}_qr.png"
            qr_bytes = make_qr_png_bytes(page["url"])
            started = time.perf_counter()
            qr_upload = onedrive.upload_bytes(f"{sample_folder}/{qr_name}", qr_bytes, "image/png")
            notion.set_uploaded_file(page["id"], "QRCode", qr_name, qr_bytes, "image/png")
            app_log("sample_qr_uploaded", seconds=round(time.perf_counter() - started, 3))
            started = time.perf_counter()
            snapshot = onedrive.upload_json(
                f"{sample_folder}/record.json",
                {
                    "form": form.to_dict(),
                    "raw_form": _form_payload(request.form),
                    "notion": page,
                    "files": uploaded_files,
                    "photo_errors": photo_errors,
                    "qr": qr_upload.path,
                },
            )
            if photo_errors:
                _safe_archive_status(notion, page["id"], ARCHIVE_FAILED, "; ".join(photo_errors))
            else:
                notion.update_archive_status(page["id"], ARCHIVE_COMPLETE)
            app_log("sample_snapshot_uploaded", seconds=round(time.perf_counter() - started, 3))
            backlog.append(
                BacklogEvent(
                    action="create",
                    entity="sample",
                    payload=form.to_dict(),
                    notion_page_id=page["id"],
                    onedrive_paths=[snapshot.path, qr_upload.path, *[item["path"] for item in uploaded_files]],
                    status="partial" if photo_errors else "complete",
                    error="; ".join(photo_errors),
                )
            )
            response = {
                "success": True,
                "message": "Sample submitted successfully" if not photo_errors else "Sample saved in Notion; some photo archive uploads failed.",
                "receipt": _sample_receipt(form, page),
            }
            if photo_errors:
                response["warning"] = "; ".join(photo_errors)
            return jsonify(response)
        except Exception as exc:
            if page:
                _safe_archive_status(notion, page["id"], ARCHIVE_FAILED, str(exc))
            backlog.append(BacklogEvent(action="create", entity="sample", payload=form.to_dict(), notion_page_id=page["id"] if page else "", status="failed", error=str(exc)))
            return jsonify({"success": False, "error": str(exc)}), 400

    @app.post("/api/submit-data")
    def api_submit_result():
        raw_form = _form_payload(request.form)
        backlog.append(BacklogEvent(action="create", entity="result", payload=raw_form))
        page: dict | None = None
        artifact_errors: list[str] = []
        onedrive_paths: list[str] = []
        try:
            _validate_result_raw(raw_form, notion, preflight=False)
            form = ResultForm.from_form(request.form)
        except Exception as exc:
            app_log("result_submission_failed", stage="form_parse", error=str(exc), form=raw_form)
            backlog.append(BacklogEvent(action="create", entity="result", payload=raw_form, status="failed", error=f"form_parse: {exc}"))
            return jsonify({"success": False, "stage": "form_parse", "error": str(exc)}), 400

        try:
            page = notion.result_page_by_submission(form.submission_id)
            if page and notion.archive_status_from_page(page) == ARCHIVE_COMPLETE:
                return jsonify(
                    {
                        "success": True,
                        "already_processed": True,
                        "message": "Data submission was already processed",
                        "notion_page_id": page["id"],
                        "notion_page_url": page.get("url", ""),
                        "receipt": _result_receipt(form, page),
                    }
                )
            if page:
                app_log("result_submission_retry", page_id=page["id"], submission_id=form.submission_id)
                notion.update_archive_status(page["id"], ARCHIVE_PENDING)
            else:
                started = time.perf_counter()
                page = notion.create_result(form)
                app_log("notion_result_created", page_id=page["id"], seconds=round(time.perf_counter() - started, 3))
        except Exception as exc:
            app_log("result_submission_failed", stage="notion_create", error=str(exc), form=raw_form)
            backlog.append(BacklogEvent(action="create", entity="result", payload=form.to_dict(), status="failed", error=f"notion_create: {exc}"))
            return jsonify({"success": False, "stage": "notion_create", "error": str(exc)}), 400

        result_folder = f"{_result_parent_folder(notion, form)}/results/{_safe_segment(form.name)}"
        qr_name = f"{_safe_segment(form.name)}_qr.png"
        qr_path = ""
        try:
            qr_bytes = make_qr_png_bytes(page["url"])
            started = time.perf_counter()
            qr_upload = onedrive.upload_bytes(f"{result_folder}/{qr_name}", qr_bytes, "image/png")
            qr_path = qr_upload.path
            onedrive_paths.append(qr_upload.path)
            notion.set_uploaded_file(page["id"], "QRCode", qr_name, qr_bytes, "image/png")
            app_log("result_qr_uploaded", page_id=page["id"], seconds=round(time.perf_counter() - started, 3))
        except Exception as exc:
            error = f"qr_upload: {exc}"
            artifact_errors.append(error)
            app_log("result_artifact_failed", page_id=page["id"], stage="qr_upload", error=str(exc))

        try:
            started = time.perf_counter()
            snapshot = onedrive.upload_json(
                f"{result_folder}/record.json",
                {"form": form.to_dict(), "raw_form": raw_form, "notion": page, "qr": qr_path},
            )
            onedrive_paths.append(snapshot.path)
            app_log("result_snapshot_uploaded", page_id=page["id"], seconds=round(time.perf_counter() - started, 3))
        except Exception as exc:
            error = f"snapshot_upload: {exc}"
            artifact_errors.append(error)
            app_log("result_artifact_failed", page_id=page["id"], stage="snapshot_upload", error=str(exc))

        try:
            backlog.append(
                BacklogEvent(
                    action="create",
                    entity="result",
                    payload=form.to_dict(),
                    notion_page_id=page["id"],
                    onedrive_paths=onedrive_paths,
                    status="partial" if artifact_errors else "complete",
                    error="; ".join(artifact_errors),
                )
            )
        except Exception as exc:
            app_log("result_backlog_failed", page_id=page["id"], error=str(exc))

        response = {
            "success": True,
            "message": "Data submitted successfully" if not artifact_errors else "Data entry saved in Notion; some archive artifacts failed.",
            "notion_page_id": page["id"],
            "notion_page_url": page.get("url", ""),
            "receipt": _result_receipt(form, page),
        }
        if artifact_errors:
            response["warning"] = "; ".join(artifact_errors)
            _safe_archive_status(notion, page["id"], ARCHIVE_FAILED, response["warning"])
        else:
            notion.update_archive_status(page["id"], ARCHIVE_COMPLETE)
        return jsonify(response)

    @app.post("/api/receipt-pdf")
    def api_receipt_pdf():
        data = request.get_json(force=True, silent=True) or {}
        title = str(data.get("title") or "Submission Receipt").strip()
        rows = data.get("rows") if isinstance(data.get("rows"), list) else []
        images = data.get("images") if isinstance(data.get("images"), list) else []
        clean_rows = []
        for item in rows:
            if isinstance(item, list) and len(item) >= 2:
                clean_rows.append((str(item[0]), item[1]))
            elif isinstance(item, dict):
                clean_rows.append((str(item.get("label", "")), item.get("value", "")))
        pdf = make_receipt_pdf(title, clean_rows, images=images)
        filename = f"{_safe_segment(title.lower())}.pdf"
        return send_file(io.BytesIO(pdf), mimetype="application/pdf", as_attachment=True, download_name=filename)

    @app.post("/api/create-upload-session")
    def api_create_upload_session():
        try:
            data = request.get_json(force=True)
            filename = safe_upload_filename(data.get("filename", ""))
            entry_name = _safe_segment(data.get("entry_name", "data_entry"))
            parent_sample = data.get("parent_sample", "")
            parent_dataset = data.get("parent_dataset", "")
            if not filename:
                return jsonify({"success": False, "error": "filename is required"}), 400
            if not is_allowed_extension(filename, settings.allowed_upload_extensions):
                return jsonify({"success": False, "error": f"File type is not allowed: {Path(filename).suffix.lower()}"}), 400
            size = int(data.get("size") or data.get("file_size") or 0)
            if size and size > settings.max_upload_file_mb * 1024 * 1024:
                return jsonify({"success": False, "error": f"File exceeds {settings.max_upload_file_mb} MB limit."}), 400
            parent_folder = _result_upload_parent_folder(notion, parent_sample, parent_dataset)
            session = onedrive.create_upload_session(f"{parent_folder}/results/{entry_name}/{filename}")
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

    @app.get("/healthz")
    def healthz():
        return health()

    return app


def _safe_list(loader: Callable[[], list[dict[str, str]]]) -> list[dict[str, str]]:
    try:
        return loader()
    except Exception:
        return []


def _archive_uploads(onedrive: OneDriveClient, prefix: str, files, settings: Settings) -> list[dict[str, str]]:
    files = _validate_uploads(files, settings)
    uploaded: list[dict[str, str]] = []
    for file_storage in files:
        safe_name = safe_upload_filename(file_storage.filename)
        result = onedrive.upload_file(f"{prefix}/{safe_name}", file_storage.stream, file_storage.mimetype)
        uploaded.append({"name": safe_name, "path": result.path, "url": result.web_url})
    return uploaded


def _archive_sample_photos(
    notion: NotionRepository,
    onedrive: OneDriveClient,
    page_id: str,
    prefix: str,
    files,
    settings: Settings,
) -> tuple[list[dict[str, str]], list[str]]:
    started = time.perf_counter()
    uploaded: list[dict[str, str]] = []
    errors: list[str] = []
    files = _validate_uploads(files, settings)
    for file_storage in files:
        safe_name = safe_upload_filename(file_storage.filename)
        content = file_storage.read()
        content_type = file_storage.mimetype or "application/octet-stream"
        try:
            result = onedrive.upload_bytes(f"{prefix}/{safe_name}", content, content_type)
        except Exception as exc:
            message = f"photo_upload:{safe_name}: {exc}"
            errors.append(message)
            app_log("sample_photo_upload_failed", page_id=page_id, filename=safe_name, error=str(exc))
            continue
        uploaded.append({"name": safe_name, "path": result.path, "url": result.web_url})
    if uploaded:
        try:
            notion.attach_external_files(page_id, "Photos", uploaded)
        except Exception as exc:
            message = f"photo_attach: {exc}"
            errors.append(message)
            app_log("sample_photo_attach_failed", page_id=page_id, error=str(exc))
    app_log("sample_photos_uploaded", seconds=round(time.perf_counter() - started, 3), count=len(uploaded), errors=len(errors))
    return uploaded, errors


def _validate_sample_form(form: SampleForm, notion: NotionRepository) -> None:
    errors = []
    if not form.name:
        errors.append("Sample Name is required.")
    if not form.sample_type:
        errors.append("Sample Type is required.")
    is_subsample = form.sample_type.lower().replace("_", "-") in {"sub-sample", "sub sample", "subsample"}
    if is_subsample:
        if not form.parent_sample_id:
            errors.append("Parent Sample is required for a sub-sample.")
    elif form.sample_type and not form.composition:
        errors.append("Composition is required for a root sample.")
    if errors:
        raise ValueError("Please fill required fields: " + " ".join(errors))
    if form.submission_id:
        existing_submission = notion.sample_page_by_submission(form.submission_id)
        if existing_submission:
            if _sample_submission_matches_page(form, existing_submission):
                return
            raise ValueError(
                "This loaded JSON was already submitted and no longer matches the existing Notion record. "
                "Load it as a new submission or change the sample name."
            )
    if notion.sample_exists(form.name):
        raise ValueError(f"A sample with name '{form.name}' already exists. Please choose another name.")


def _sample_submission_matches_page(form: SampleForm, page: dict[str, Any]) -> bool:
    checks = [
        _same_text(form.name, _page_title(page)),
        _same_text(form.sample_type, _page_select(page, "Sample Type")),
        _same_optional_text(form.composition, _page_text(page, "Composition")),
        _same_optional_text(form.status, _page_select(page, "Status")),
        _same_set(form.synthesis, _page_multi_select(page, "Synthesis")),
        _same_set(form.processing, _page_multi_select(page, "Processing")),
    ]
    return all(checks)


def _same_text(left: str, right: str) -> bool:
    return str(left or "").strip().casefold() == str(right or "").strip().casefold()


def _same_optional_text(left: str, right: str) -> bool:
    if not str(left or "").strip():
        return True
    return _same_text(left, right)


def _same_set(left: list[str], right: list[str]) -> bool:
    return {item.strip().casefold() for item in left if item.strip()} == {item.strip().casefold() for item in right if item.strip()}


def _page_title(page: dict[str, Any]) -> str:
    for prop in page.get("properties", {}).values():
        if prop.get("type") == "title":
            return "".join(item.get("plain_text", "") for item in prop.get("title", []))
    return ""


def _page_text(page: dict[str, Any], property_name: str) -> str:
    prop = page.get("properties", {}).get(property_name, {})
    prop_type = prop.get("type")
    if prop_type in {"rich_text", "title"}:
        return "".join(item.get("plain_text", "") for item in prop.get(prop_type, []))
    return ""


def _page_select(page: dict[str, Any], property_name: str) -> str:
    prop = page.get("properties", {}).get(property_name, {})
    if prop.get("type") == "select" and prop.get("select"):
        return prop["select"].get("name", "")
    return ""


def _page_multi_select(page: dict[str, Any], property_name: str) -> list[str]:
    prop = page.get("properties", {}).get(property_name, {})
    if prop.get("type") == "multi_select":
        return [item.get("name", "") for item in prop.get("multi_select", [])]
    return []


def _validate_result_raw(raw_form: dict[str, Any], notion: NotionRepository, preflight: bool) -> None:
    name = str(raw_form.get("name") or "").strip()
    entry_type = str(raw_form.get("entry_type") or "").strip()
    parent_entry = str(raw_form.get("parent_entry") or "").strip()
    parent_sample = str(raw_form.get("parent_sample") or "").strip()
    parent_dataset = str(raw_form.get("parent_dataset") or "").strip()
    upload_method = str(raw_form.get("data_type") or raw_form.get("upload_method") or "").strip()
    data_link = str(raw_form.get("data_link") or raw_form.get("link") or "").strip()
    onedrive_path = str(raw_form.get("onedrive_path") or "").strip()

    errors = []
    if not name:
        errors.append("Name is required.")
    if not entry_type:
        errors.append("Entry type is required.")
    if not parent_entry:
        errors.append("Parent Entry is required.")
    elif parent_entry == "sample" and not parent_sample:
        errors.append("Parent Sample is required when Parent Entry is sample.")
    elif parent_entry == "dataset" and not parent_dataset:
        errors.append("Parent Dataset is required when Parent Entry is dataset.")
    if not upload_method:
        errors.append("Data upload method is required.")
    elif upload_method == "link" and not data_link:
        errors.append("Link is required when Data is Link.")
    elif upload_method == "file" and not preflight and not onedrive_path:
        errors.append("OneDrive file link is missing. Please upload the file before submitting.")
    if errors:
        raise ValueError("Please fill required fields: " + " ".join(errors))
    submission_id = str(raw_form.get("submission_id") or raw_form.get("submissionId") or "").strip()
    if submission_id and notion.result_page_by_submission(submission_id):
        return
    if notion.result_exists(name):
        raise ValueError(f"A result with name '{name}' already exists. Please choose another name.")


def _sample_receipt(form: SampleForm, page: dict[str, Any]) -> dict[str, Any]:
    return {
        "title": f"Sample Submission - {form.name}",
        "rows": [
            ["Record Type", "Sample"],
            ["Name", form.name],
            ["Sample Type", form.sample_type],
            ["Composition", form.composition],
            ["Parent Sample", form.parent_sample_id],
            ["Synthesis", form.synthesis],
            ["Processing", form.processing],
            ["Status", form.status],
            ["Submission ID", form.submission_id],
            ["Notion URL", page.get("url", "")],
        ],
    }


def _result_receipt(form: ResultForm, page: dict[str, Any]) -> dict[str, Any]:
    return {
        "title": f"Result Submission - {form.name}",
        "rows": [
            ["Record Type", "Result"],
            ["Name", form.name],
            ["Data Type", form.data_type],
            ["Upload Method", form.upload_method],
            ["Parent Entry", form.parent_entry],
            ["Sample", form.sample_id],
            ["Related Result", form.related_result_id],
            ["Characterisation", form.characterization],
            ["Link", form.link],
            ["Submission ID", form.submission_id],
            ["Notion URL", page.get("url", "")],
        ],
    }


def _form_payload(form) -> dict:
    return {key: form.get(key) for key in form.keys()}


def _safe_segment(value: str) -> str:
    return safe_path_segment(value, fallback="entry")


def _validate_uploads(files, settings: Settings) -> list[Any]:
    uploads = [item for item in files if item and item.filename]
    if len(uploads) > settings.max_upload_files:
        raise ValueError(f"Upload limit is {settings.max_upload_files} files.")
    max_bytes = settings.max_upload_file_mb * 1024 * 1024
    for item in uploads:
        filename = safe_upload_filename(item.filename)
        if not is_allowed_extension(filename, settings.allowed_upload_extensions):
            raise ValueError(f"File type is not allowed: {Path(filename).suffix.lower()}")
        size = _stream_size(item)
        if size and size > max_bytes:
            raise ValueError(f"{filename} exceeds {settings.max_upload_file_mb} MB limit.")
    return uploads


def _stream_size(file_storage) -> int:
    stream = getattr(file_storage, "stream", None)
    try:
        position = stream.tell()
        stream.seek(0, os.SEEK_END)
        size = stream.tell()
        stream.seek(position)
        return size
    except Exception:
        return int(getattr(file_storage, "content_length", 0) or 0)


def _safe_archive_status(notion: NotionRepository, page_id: str, status: str, error: str = "") -> None:
    try:
        notion.update_archive_status(page_id, status, error)
    except Exception as exc:
        app_log("archive_status_update_failed", page_id=page_id, status=status, error=str(exc))


def _bounded_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(maximum, parsed))


def _result_parent_folder(notion: NotionRepository, form: ResultForm) -> str:
    if form.sample_id:
        return notion.sample_storage_info(form.sample_id)["folder"]
    if form.related_result_id:
        return f"results/{_safe_segment(form.related_result_id)}"
    return "results"


def _result_upload_parent_folder(notion: NotionRepository, parent_sample: str, parent_dataset: str) -> str:
    if parent_sample:
        return notion.sample_storage_info(parent_sample)["folder"]
    if parent_dataset:
        return f"results/{_safe_segment(parent_dataset)}"
    return "results"


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


def app_log(event: str, **fields) -> None:
    print(json.dumps(redact_for_log({"event": event, **fields}), sort_keys=True), flush=True)
