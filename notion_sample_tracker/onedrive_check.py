"""Check the OneDrive/SharePoint connection on its own, without Notion or the web app.

    python -m notion_sample_tracker.onedrive_check               # sign in, verify the drive, write a test file, try an upload session
    python -m notion_sample_tracker.onedrive_check --read-only   # sign in and verify the drive only; nothing is written
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Callable

import requests

from notion_sample_tracker.services.onedrive_client import OneDriveClient
from notion_sample_tracker.settings import PRODUCTION_ENVS, _csv, _int, _optional, _required


CHECK_FOLDER = "_connection_check"

HINTS = (
    ("AADSTS7000215", "The client secret is wrong. Use the secret's Value (shown once when it was created), not its Secret ID."),
    ("AADSTS7000222", "The client secret has expired. Create a new one and update ONEDRIVE_CLIENT_SECRET."),
    ("AADSTS700016", "The client ID was not found in this tenant. Check ONEDRIVE_CLIENT_ID and ONEDRIVE_TENANT_ID."),
    ("AADSTS700038", "ONEDRIVE_CLIENT_ID is not a valid application ID. Copy the Application (client) ID from the app's Overview page."),
    ("AADSTS90002", "The tenant was not found. Check ONEDRIVE_TENANT_ID."),
    ("AADSTS65001", "Consent is missing. A UTK administrator must grant consent for this app."),
    (
        "accessDenied",
        "Microsoft refused the request. For Sites.Selected this usually means admin consent is missing, or the app "
        "has not been granted the 'write' role on this site.",
    ),
    ("itemNotFound", "The drive was not found or the app cannot see it. Check ONEDRIVE_DRIVE_ID and the app's site grant."),
)


def client_from_env() -> OneDriveClient:
    return OneDriveClient(
        tenant_id=_required("ONEDRIVE_TENANT_ID"),
        client_id=_required("ONEDRIVE_CLIENT_ID"),
        client_secret=_required("ONEDRIVE_CLIENT_SECRET"),
        root_folder=_optional("ONEDRIVE_ROOT_FOLDER", "SampleTracker"),
        auth_mode=_optional("ONEDRIVE_AUTH_MODE", "client_credentials"),
        public_client=_optional("ONEDRIVE_PUBLIC_CLIENT", "false").lower() in {"1", "true", "yes"},
        drive_id=_optional("ONEDRIVE_DRIVE_ID"),
        refresh_token=_optional("ONEDRIVE_REFRESH_TOKEN"),
        timeout=_int("ONEDRIVE_TIMEOUT_SECONDS", 20),
        allowed_host_suffixes=_csv("ONEDRIVE_ALLOWED_HOST_SUFFIXES"),
    )


def _hint_for(message: str) -> str:
    for needle, hint in HINTS:
        if needle in message:
            return hint
    return ""


def run_checks(client: OneDriveClient, read_only: bool = False, emit: Callable[[str], None] = print) -> bool:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    results: list[bool] = []

    def step(label: str, action: Callable[[], str]) -> bool:
        try:
            detail = action()
        except Exception as exc:
            message = str(exc)
            emit(f"FAIL  {label}: {message}")
            hint = _hint_for(message)
            if hint:
                emit(f"      -> {hint}")
            results.append(False)
            return False
        emit(f"ok    {label}" + (f": {detail}" if detail else ""))
        results.append(True)
        return True

    def verify() -> str:
        info = client.verify_drive()
        return f"{info.drive_type} drive on {info.host or 'unknown host'} ({info.web_url})"

    def write_file() -> str:
        content = f"Notion Sample Tracker connection check {stamp}\n".encode("utf-8")
        result = client.upload_bytes(f"{CHECK_FOLDER}/check-{stamp}.txt", content, "text/plain")
        return f"{result.path} {result.web_url}".strip()

    def upload_session() -> str:
        session = client.create_upload_session(f"{CHECK_FOLDER}/session-{stamp}.bin")
        response = requests.delete(session["upload_url"], timeout=client.timeout)
        if response.ok:
            return "created and cancelled"
        return f"created; cancelling returned HTTP {response.status_code}, it will expire on its own"

    if not step("Sign in and verify the drive", verify):
        emit("Stopped: fix the above first. Nothing was written.")
        return False
    if read_only:
        emit("Skipped the write checks (--read-only).")
        return True
    step("Write a small test file", write_file)
    step("Create and cancel an upload session (the Results page uses this)", upload_session)

    passed = all(results)
    emit("All checks passed." if passed else "Some checks failed.")
    if passed:
        emit(f"Delete the '{client.root_folder}/{CHECK_FOLDER}' folder in OneDrive when you are done.")
    return passed


def main(argv: list[str] | None = None, emit: Callable[[str], None] = print) -> int:
    parser = argparse.ArgumentParser(description="Check the OneDrive/SharePoint connection.")
    parser.add_argument("--read-only", action="store_true", help="Sign in and verify the drive only; write nothing.")
    args = parser.parse_args(argv)

    try:
        client = client_from_env()
        app_env = _optional("APP_ENV", _optional("FLASK_ENV", "development")).lower()
        if app_env in PRODUCTION_ENVS and client.auth_mode == "delegated_refresh":
            raise RuntimeError("ONEDRIVE_AUTH_MODE=delegated_refresh is not allowed in production.")
    except RuntimeError as exc:
        emit(f"FAIL  Configuration: {exc}")
        return 1

    emit(
        f"Target: tenant {client.tenant_id}, drive {client.drive_id or '(missing)'}, "
        f"folder '{client.root_folder}', auth {client.auth_mode}"
    )
    return 0 if run_checks(client, args.read_only, emit) else 1


if __name__ == "__main__":
    raise SystemExit(main())
