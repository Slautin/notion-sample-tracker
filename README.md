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

## Start page

Opening the app's address shows a start page in the same style as the forms. **Add sample** and **Add results** open the app's forms (`/samples/new`, `/results/new`); **Samples**, **Results** and **Sources** open the Notion databases in a new tab. Both forms have a "← Home" link back to it.

- Set the database links with `NOTION_SAMPLES_URL`, `NOTION_RESULTS_URL` and `NOTION_SOURCES_URL`. In Notion, open the database view you want people to see and use Copy link. A button without a link is hidden; a link without `https://` is refused at startup.
- `NOTION_HOME_URL` (or the older `NOTION_PAGE`) adds an "Open the Notion workspace" link. The start page no longer redirects to Notion.

## Storage backends

`STORAGE_BACKEND` chooses where archived files go. Notion behaves the same with either.

| | `onedrive` (default) | `local` |
|---|---|---|
| Files live in | UTK SharePoint / OneDrive for Business | a directory on the machine running the app (`STORAGE_ROOT`) |
| Result files | the browser uploads straight to OneDrive | the browser uploads to this app in small resumable chunks |
| Who can open a file link | people the drive is shared with | whoever can log in to this app (`/files/...`) |
| Backups | Microsoft | you |
| Needs | an Entra app registration and admin consent | disk space, backups and access control |

Both use the same folder layout (`samples/<composition>/<name>/...`, `.../results/<entry>/...`) and write the same `record.json` snapshots.

## OneDrive (UTK only)

With `STORAGE_BACKEND=onedrive` (the default), files are archived to the organization's (UTK) SharePoint or OneDrive for Business drive, never a personal OneDrive:

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

## Local server storage

Set `STORAGE_BACKEND=local` to keep files on the machine that runs the app. The `ONEDRIVE_*` variables are then not needed.

| Variable | Default | Meaning |
|---|---|---|
| `STORAGE_ROOT` | `./files` (development), `/data/files` (production) | Where files are stored. Must be an absolute path in production, outside the application directory. |
| `STORAGE_MIN_FREE_MB` | `1024` | Uploads that would leave less free disk than this are refused. |
| `MAX_RESULT_FILE_MB` | `2048` | Largest result file. Raise it for bigger datasets. |
| `UPLOAD_CHUNK_MB` | `8` | Size of each upload request. Must not exceed `MAX_UPLOAD_MB` or the proxy's body limit. |
| `UPLOAD_TTL_HOURS` | `24` | An unfinished upload is discarded after this long without activity. |
| `ALLOWED_UPLOAD_EXTENSIONS` | office, image and text types | Lab formats such as `.ibw`, `.h5` or `.mat` are **not** in the default list. Add the ones you need. |

How it works:

- Samples: `record.json`, the QR code and the photos are written under `STORAGE_ROOT/samples/...`.
- Result files: the browser sends the file in `UPLOAD_CHUNK_MB` pieces. The app appends them to a staging file in `STORAGE_ROOT/.uploads/` and moves it into `.../results/<entry>/` when the last piece has arrived. An interrupted upload resumes where it stopped. A name that already exists gets `-1`, `-2`, ... appended instead of being overwritten.
- The link saved in the Notion `Link` field is `PUBLIC_BASE_URL/files/<path>`, served as a download. The app checks that the link points to a stored file before saving it, so a typed-in address is rejected. `PUBLIC_BASE_URL` must be just this app's own address, for example `https://your-app.up.railway.app`, never a Notion page (those belong in `NOTION_HOME_URL` and the `NOTION_*_URL` start-page links). The app refuses to start if it is a Notion address or has a path, `?query` or `#fragment`. Keep it stable: changing it breaks links already stored in Notion.
- Names that start with a dot (the staging area, temporary files) are never served, and symbolic links inside the storage root are refused.
- Put `STORAGE_ROOT` on a local disk. Upload locking uses `flock`, which is not reliable on some network file systems (NFS, SMB).

### Access control

The app serves the files itself, so production refuses to start with local storage unless you choose one of:

- `ACCESS_PASSWORD`: HTTP Basic for the whole site (username `ACCESS_USERNAME`, default `lab`). It needs 12+ characters and an `https://` `PUBLIC_BASE_URL`. There is no brute-force protection, so use a long password. Good for a prototype or a small lab.
- `ACCESS_BEHIND_AUTH_PROXY=true`: your statement that a login in front of the app (nginx SSO etc.) protects every path, including `/files/`. The app cannot check this, so bind gunicorn to `127.0.0.1` (`GUNICORN_BIND`) so the proxy is the only way in.

`/health` and `/healthz` stay open for health checks. POST and PUT requests that a browser marks as coming from another site are refused.

### Deploying on Railway

1. Set the variables: `APP_ENV=production`, `APP_SECRET_KEY`, `PUBLIC_BASE_URL` (your https Railway domain), the Notion variables, `STORAGE_BACKEND=local`, and `ACCESS_PASSWORD`.
2. Add a volume mounted at `/data` and set `RAILWAY_RUN_UID=0`. The image runs as a non-root user and Railway mounts volumes as root; the app checks at startup that it can write and says so if it cannot.
3. Know the limits: one volume per service, no replicas with a volume, a redeploy interrupts uploads in progress, and volume size depends on the plan (5 GB Hobby, 50 GB Pro at the time of writing). Railway is a third-party cloud, so keep sensitive or restricted data off it.

### Deploying on a UTK Linux server

Templates for the steps below are in `deploy/utk-server/`: a systemd unit, an nginx site and an environment file. They have not been run against a real server, so read them before use. Python 3.10 or newer is required.

```bash
sudo useradd --system --home-dir /var/lib/sampletracker --shell /usr/sbin/nologin sampletracker
sudo mkdir -p /opt/sampletracker /etc/sampletracker /var/lib/sampletracker/files /var/lib/sampletracker/backlog
sudo chown -R sampletracker:sampletracker /var/lib/sampletracker
sudo git clone <this repository> /opt/sampletracker/app
sudo python3 -m venv /opt/sampletracker/venv
sudo /opt/sampletracker/venv/bin/pip install /opt/sampletracker/app

sudo cp /opt/sampletracker/app/deploy/utk-server/sampletracker.env.example /etc/sampletracker/sampletracker.env
sudo chmod 600 /etc/sampletracker/sampletracker.env      # then edit it
sudo cp /opt/sampletracker/app/deploy/utk-server/sampletracker.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now sampletracker
```

Then install `nginx-sampletracker.conf` (set the host name and certificate paths, and add your login), reload nginx and check:

```bash
curl -s http://127.0.0.1:8000/healthz        # {"status":"ok","storage_backend":"local",...}
sudo journalctl -u sampletracker -n 50       # startup errors, e.g. an unwritable STORAGE_ROOT
```

Upgrade with `git pull`, `pip install /opt/sampletracker/app`, `systemctl restart sampletracker`.

Gunicorn settings come from `gunicorn.conf.py` and can be overridden with `GUNICORN_BIND`, `WEB_CONCURRENCY`, `GUNICORN_THREADS` and `GUNICORN_TIMEOUT`.

### Backups

With local storage the files exist nowhere else, and Notion only holds links. Back up `STORAGE_ROOT` (not the `.uploads/` staging folder) and `BACKLOG_DIR`, for example nightly to another host:

```bash
rsync -a --exclude '.uploads/' /var/lib/sampletracker/ backup-host:/backups/sampletracker/
```

Test a restore before you rely on it.

## Stability Notes

The app is designed so a Notion or OneDrive outage does not silently lose submissions. Route handlers write backlog events for create/update attempts and failures. For large deployments, replace `JsonlBacklog` with Postgres or object storage while keeping the same service boundary.
