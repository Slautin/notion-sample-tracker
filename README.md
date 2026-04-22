# Notion Sample Tracker

A deployable Notion-centered sample/data tracker built from the ideas in `samplenaming`, but with cleaner boundaries:

- Notion is the source of truth for samples and result/data entries.
- Web forms create and revise Notion pages.
- Every create/update is written to a JSONL backlog for audit and recovery.
- Attached files and record snapshots can be archived to OneDrive.
- Formula parsing reuses the existing `samplenaming.periodictable` logic through a small stable wrapper.

## Quick Start

```bash
cd notion_sample_tracker
python -m venv .venv
source .venv/bin/activate
pip install -e ".[test]"
cp .env.example .env
flask --app notion_sample_tracker run --debug --port 8000
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
- `Location` rich text
- `Parent Sample` relation to Samples
- `Source` relation to People
- `QRCode` files

Results database:

- `Name` title
- `Data Type` select
- `Brief Description` rich text
- `Characterization` multi-select
- `Upload Method` select
- `Link` url
- `Sample` relation to Samples
- `Related Results` relation to Results
- `Source` relation to People
- `QRCode` files

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

Set `ONEDRIVE_DRIVE_ID` in production. The app uses Microsoft Graph client-credentials auth, so uploads target `/drives/{ONEDRIVE_DRIVE_ID}` rather than `/me/drive`.

## Stability Notes

The app is designed so a Notion or OneDrive outage does not silently lose submissions. Route handlers write backlog events for create/update attempts and failures. For large deployments, replace `JsonlBacklog` with Postgres or object storage while keeping the same service boundary.
