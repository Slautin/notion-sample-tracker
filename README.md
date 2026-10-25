# Notion Sample Tracker

A deployable Notion-centered sample/data tracker built from the ideas in `samplenaming`, but with cleaner boundaries:

- Notion is the source of truth for samples and result/data entries.
- Web forms create and revise Notion pages.
- Every create/update is written to a JSONL backlog for audit and recovery.
- Attached files and record snapshots can be archived to OneDrive.
- Formula parsing reuses the existing `samplenaming.periodictable` logic through a small stable wrapper.

## Deployment Target

This directory is the active deployment target for the Notion Sample Tracking project.
Setup, testing, Docker builds, and production hardening should be done from this
directory unless the project intentionally changes ownership.

The older Flask/Notion code under `../samplenaming/` is legacy reference code. Do not
mix its server routes, Microsoft Graph helpers, or Notion loaders into the deployment
path without a separate review.

## Quick Start

```bash
cd notion_sample_tracker
python -m venv .venv
source .venv/bin/activate
pip install -e ".[test]"
cp .env.example .env
flask --app notion_sample_tracker run --debug --port 8000
```

Use the virtual environment for local work. Installing into a shared Conda or system
environment can upgrade common packages such as `requests` or `pytest` and conflict
with unrelated projects.

Fill `.env` with local secrets and database IDs. Commit changes to `.env.example`
only when adding or documenting configuration keys; never commit real tokens,
client secrets, upload-session JSON, or backlog files.

Verify the local setup with:

```bash
pytest
```

Verify the live Notion database schema with:

```bash
python -m notion_sample_tracker.schema_check
```

For production:

```bash
gunicorn "notion_sample_tracker:create_app()" --bind 0.0.0.0:8000 --workers 2
```

## Required Notion Schema

Samples database:

- `Name` title
- `Sample Type` select
- `Composition` rich text
- `Elements` multi-select
- `Synthesis` multi-select
- `Synthesis Details` rich text
- `Processing` multi-select
- `Processing Details` rich text
- `Status` select
- `Parent Sample` relation to Samples
- `Source` relation to People
- `QRCode` files
- `Photos` files
- `Submission ID` rich text
- `Archive Status` select with `Pending archive`, `Archive complete`, `Failed archive`
- `Archive Error` rich text

Results database:

- `Name` title
- `Data Type` select
- `Brief Description` rich text
- `Characterisation` multi-select
- `Upload Method` select
- `Parent Entry` select
- `Link` url
- `Sample` relation to Samples
- `Related Results` relation to Results
- `Source` relation to People
- `QRCode` files
- `Submission ID` rich text
- `Archive Status` select with `Pending archive`, `Archive complete`, `Failed archive`
- `Archive Error` rich text

People database:

- `Person` title
- `Email` email
- `Affiliation` rich text

The services intentionally keep property names centralized in `settings.py`, so adapting to another Notion workspace is a small config/code change instead of a scattered rewrite.

## Project Layout

- `notion_sample_tracker/app.py` - Flask app factory and routes
- `notion_sample_tracker/settings.py` - environment-driven configuration
- `notion_sample_tracker/models.py` - typed request/record models
- `notion_sample_tracker/services/formula.py` - formula normalization and element parsing
- `notion_sample_tracker/services/notion_client.py` - Notion CRUD and schema helpers
- `notion_sample_tracker/services/onedrive_client.py` - Microsoft Graph upload/archive client
- `notion_sample_tracker/services/backlog.py` - local JSONL audit log
- `notion_sample_tracker/templates/` - deployable web forms

Set `NOTION_HOME_URL` in production if `/` should redirect to your main Notion page. The older `NOTION_PAGE` variable name is also supported. Put links on that Notion page to `/samples/new` and `/results/new` on the deployed app domain.

## OneDrive (UTK only)

Files are archived to the organization's (UTK) SharePoint or OneDrive for Business drive, never a personal OneDrive:

- `ONEDRIVE_TENANT_ID` must be the UTK tenant ID. `common`, `consumers` and `organizations` are rejected, and production requires the GUID.
- `ONEDRIVE_DRIVE_ID` is always required. Uploads go to `/drives/{ONEDRIVE_DRIVE_ID}`; there is no `/me/drive` fallback.
- `ONEDRIVE_AUTH_MODE=client_credentials` (app-only) is the only mode allowed in production. `delegated_refresh` is for local development and is rejected when `APP_ENV=production`.
- Before the first upload the app asks Graph about the drive and refuses to write if it is a `personal` drive or on a personal OneDrive host. Optionally set `ONEDRIVE_ALLOWED_HOST_SUFFIXES` (comma-separated, e.g. `yourtenant.sharepoint.com,yourtenant-my.sharepoint.com`) to pin the drive to your tenant's SharePoint hosts.
- In production the same check also runs at startup. A definite violation stops the app; a temporary Graph outage is logged and the check repeats before the first upload.
- If Graph will not let the app read the drive object itself (HTTP 403, which can happen with `Sites.Selected`), the check falls back to the drive's root folder and judges by host only. Set `ONEDRIVE_ALLOWED_HOST_SUFFIXES` to make that strict.
- Access tokens are cached until shortly before they expire.

Recommended setup: register a single-tenant app in UTK's Entra ID, grant Microsoft Graph application permission `Sites.Selected` with admin consent, have an admin grant the app `write` on the lab's SharePoint site, then set `ONEDRIVE_DRIVE_ID` to that site's document library (`GET /sites/{host}:/sites/{name}:/drives`).

Test the connection on its own, without Notion, before using the app. It signs in, verifies the drive, writes a small file under `<ROOT>/_connection_check/` and opens and cancels a test upload session. Add `--read-only` to write nothing:

```bash
python -m notion_sample_tracker.onedrive_check
```

Set `APP_ENV=production` in production. Production mode requires a non-default
`APP_SECRET_KEY`, a non-local `PUBLIC_BASE_URL`, a UTK tenant GUID, `client_credentials`
auth, and an `ONEDRIVE_DRIVE_ID`. Keep
`ENABLE_BACKLOG_VIEW=false` unless the app is behind a separate trusted access control.

## Stability Notes

The app is designed so a Notion or OneDrive outage does not silently lose submissions. Route handlers write backlog events for create/update attempts and failures. For large deployments, replace `JsonlBacklog` with Postgres or object storage while keeping the same service boundary.
