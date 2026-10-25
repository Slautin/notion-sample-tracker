"""Files on the server's own disk: the storage backend for deployments that do not use OneDrive.

The layout matches the OneDrive archive (samples/<composition>/<name>/..., results/<entry>/...), so the files
can also be browsed and backed up directly on the server. Everything lives under one root directory. Names
that start with a dot are never served; they hold upload staging and temporary files.

Large result files arrive through ResumableUploads: many small requests that append to a staging file, which
is moved into place when complete. That avoids per-request time and size limits in gunicorn, nginx and Railway.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO, Callable, Iterator
from urllib.parse import quote, unquote

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows has no flock; the fallback only serializes threads
    fcntl = None

from notion_sample_tracker.services.storage import UploadResult


STAGING_DIR = ".uploads"
FILE_MODE = 0o640
COPY_BLOCK = 1024 * 1024
MAX_NAME_BYTES = 255
UPLOAD_ID = re.compile(r"^[0-9a-f]{32}$")
_FALLBACK_LOCK = threading.RLock()


class InvalidStoragePath(ValueError):
    """The path would leave the storage root, is hidden, or is not a plain file name."""


class StorageFull(RuntimeError):
    """Not enough free disk space to accept the data."""


class UploadNotFound(LookupError):
    """No such upload (never existed, or expired and was cleaned up)."""


class OffsetMismatch(Exception):
    def __init__(self, received: int):
        super().__init__(f"The server has {received} bytes of this upload.")
        self.received = received


class UploadIncomplete(Exception):
    def __init__(self, received: int, size: int):
        super().__init__(f"Only {received} of {size} bytes have arrived.")
        self.received = received
        self.size = size


class LocalStorage:
    def __init__(self, root: Path | str, public_base_url: str, min_free_bytes: int = 0):
        self.root = Path(root).expanduser()
        self.public_base_url = public_base_url.rstrip("/")
        self.min_free_bytes = max(0, int(min_free_bytes))
        self._real_root: Path | None = None

    @property
    def real_root(self) -> Path:
        if self._real_root is None:
            self._real_root = Path(os.path.realpath(self.root))
        return self._real_root

    def check_writable(self) -> None:
        """Create the root and staging directories and prove we can write; call once at startup."""
        try:
            self.root.mkdir(parents=True, exist_ok=True)
            (self.root / STAGING_DIR).mkdir(exist_ok=True)
            handle, name = tempfile.mkstemp(prefix=".write-test-", dir=self.root)
            os.close(handle)
            os.unlink(name)
        except OSError as exc:
            raise RuntimeError(
                f"Storage root {self.root} is not writable ({exc}). Check STORAGE_ROOT and its permissions; "
                "on Railway, mount the volume there and set RAILWAY_RUN_UID=0."
            ) from exc
        self._real_root = None

    # --- writing (same interface as OneDriveClient) ---

    def upload_bytes(self, relative_path: str, content: bytes, content_type: str = "application/octet-stream") -> UploadResult:
        return self._write(relative_path, lambda handle: handle.write(content), expected_bytes=len(content))

    def upload_json(self, relative_path: str, payload: dict) -> UploadResult:
        content = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        return self.upload_bytes(relative_path, content, "application/json")

    def upload_file(self, relative_path: str, file_obj: BinaryIO, content_type: str = "application/octet-stream") -> UploadResult:
        return self._write(relative_path, lambda handle: shutil.copyfileobj(file_obj, handle, COPY_BLOCK))

    def _write(self, relative_path: str, fill: Callable[[BinaryIO], object], expected_bytes: int = 0) -> UploadResult:
        target, relative = self._locate(relative_path)
        self.ensure_free(expected_bytes)
        target.parent.mkdir(parents=True, exist_ok=True)
        handle, temp_name = tempfile.mkstemp(prefix=".tmp-", dir=target.parent)
        try:
            with os.fdopen(handle, "wb") as out:
                fill(out)
                out.flush()
                os.fsync(out.fileno())
            os.chmod(temp_name, FILE_MODE)
            os.replace(temp_name, target)
        except BaseException:
            try:
                os.unlink(temp_name)
            except FileNotFoundError:
                pass
            raise
        return UploadResult(path=relative, web_url=self.url_for(relative))

    def ensure_free(self, extra_bytes: int = 0) -> None:
        free = shutil.disk_usage(self.real_root).free
        if free - max(0, extra_bytes) < self.min_free_bytes:
            raise StorageFull(f"Not enough free disk space on the server ({free // (1024 * 1024)} MB free).")

    # --- addressing and reading ---

    def url_for(self, relative_path: str) -> str:
        return f"{self.public_base_url}/files/{quote(relative_path, safe='/')}"

    def path_for_url(self, url: str) -> str | None:
        """The stored relative path if `url` is a link to an existing stored file of this server, else None."""
        prefix = f"{self.public_base_url}/files/"
        text = str(url or "").strip()
        if not text.startswith(prefix):
            return None
        tail = text[len(prefix):].split("?", 1)[0].split("#", 1)[0]
        try:
            path, relative = self._locate(unquote(tail))
        except InvalidStoragePath:
            return None
        return relative if path.is_file() else None

    def open_for_download(self, relative_path: str) -> Path:
        try:
            path, _ = self._locate(relative_path)
        except InvalidStoragePath as exc:
            raise FileNotFoundError(str(exc)) from exc
        if not path.is_file():
            raise FileNotFoundError(relative_path)
        return path

    def _locate(self, relative_path: str) -> tuple[Path, str]:
        """Resolve a relative path under the root, refusing anything that could escape or is hidden."""
        text = str(relative_path or "")
        if "\x00" in text or "\\" in text:
            raise InvalidStoragePath("Invalid characters in path.")
        parts = [part for part in text.split("/") if part]
        if not parts:
            raise InvalidStoragePath("Empty path.")
        for part in parts:
            if part.startswith("."):
                raise InvalidStoragePath(f"Hidden or relative path segment: {part!r}.")
            try:
                too_long = len(part.encode("utf-8")) > MAX_NAME_BYTES
            except UnicodeEncodeError as exc:
                raise InvalidStoragePath("Path is not valid text.") from exc
            if too_long:
                raise InvalidStoragePath("A path segment is too long for the file system.")
        current = self.real_root
        for part in parts:
            current = current / part
            if current.is_symlink():
                raise InvalidStoragePath("Symbolic links are not allowed.")
        return current, "/".join(parts)


class ResumableUploads:
    """Chunked, resumable uploads staged inside the storage root and moved into place when complete."""

    def __init__(self, storage: LocalStorage, max_file_bytes: int, chunk_bytes: int, ttl_seconds: int = 24 * 3600):
        self.storage = storage
        self.max_file_bytes = max_file_bytes
        self.chunk_bytes = chunk_bytes
        self.ttl_seconds = ttl_seconds

    @property
    def base(self) -> Path:
        return self.storage.real_root / STAGING_DIR

    def create(self, filename: str, size: object, target_dir: str) -> dict:
        if isinstance(size, bool) or not isinstance(size, int):
            raise ValueError("size must be a whole number of bytes.")
        if size <= 0:
            raise ValueError("The file is empty.")
        if size > self.max_file_bytes:
            raise ValueError(f"File exceeds the {self.max_file_bytes // (1024 * 1024)} MB limit.")
        _, name = self.storage._locate(filename)
        if "/" in name:
            raise InvalidStoragePath("The file name must not contain a path.")
        _, folder = self.storage._locate(target_dir)
        self.storage.ensure_free(size)
        self.base.mkdir(parents=True, exist_ok=True)
        self.cleanup_stale()
        upload_id = uuid.uuid4().hex
        directory = self.base / upload_id
        directory.mkdir()
        meta = {
            "upload_id": upload_id,
            "filename": name,
            "size": size,
            "target_dir": folder,
            "created_at": time.time(),
            "completed": None,
        }
        self._write_meta(directory, meta)
        (directory / "data.part").touch()
        return self._describe(directory, meta)

    def status(self, upload_id: str) -> dict:
        directory = self._directory(upload_id)
        with self._locked(directory):
            return self._describe(directory, self._read_meta(directory))

    def append(self, upload_id: str, offset: int, stream: BinaryIO, length: int) -> dict:
        directory = self._directory(upload_id)
        with self._locked(directory):
            meta = self._read_meta(directory)
            if meta["completed"]:
                raise ValueError("This upload is already complete.")
            part = directory / "data.part"
            received = part.stat().st_size
            if offset != received:
                raise OffsetMismatch(received)
            if length <= 0:
                raise ValueError("The chunk is empty.")
            if length > self.chunk_bytes:
                raise ValueError(f"Chunk is larger than {self.chunk_bytes} bytes.")
            if received + length > meta["size"]:
                raise ValueError("The chunk goes past the size announced for this upload.")
            self.storage.ensure_free(length)
            written = 0
            with open(part, "ab") as handle:
                while written < length:
                    block = stream.read(min(COPY_BLOCK, length - written))
                    if not block:
                        break
                    handle.write(block)
                    written += len(block)
            if written != length:
                # Keep what arrived; the client asks where the upload stands and carries on from there.
                raise ValueError(f"The request body ended after {written} of {length} bytes.")
            return self._describe(directory, meta)

    def complete(self, upload_id: str) -> dict:
        directory = self._directory(upload_id)
        with self._locked(directory):
            meta = self._read_meta(directory)
            if meta["completed"]:
                return self._describe(directory, meta)
            part = directory / "data.part"
            received = part.stat().st_size
            if received != meta["size"]:
                raise UploadIncomplete(received, meta["size"])
            target_dir, folder = self.storage._locate(meta["target_dir"])
            target_dir.mkdir(parents=True, exist_ok=True)
            final = self._reserve_unique(target_dir, meta["filename"])
            try:
                with open(part, "rb+") as handle:
                    os.fsync(handle.fileno())
                os.chmod(part, FILE_MODE)
                os.replace(part, final)
            except BaseException:
                try:
                    os.unlink(final)
                except FileNotFoundError:
                    pass
                raise
            relative = f"{folder}/{final.name}"
            meta["completed"] = {"path": relative, "name": final.name, "url": self.storage.url_for(relative)}
            self._write_meta(directory, meta)
            return self._describe(directory, meta)

    def cleanup_stale(self) -> int:
        """Remove staging directories that have seen no activity for the time-to-live. Returns how many."""
        if not self.base.is_dir():
            return 0
        cutoff = time.time() - self.ttl_seconds
        removed = 0
        for directory in self.base.iterdir():
            try:
                if not directory.is_dir():
                    continue
                newest = max(item.stat().st_mtime for item in [directory, *directory.iterdir()])
                if newest < cutoff:
                    shutil.rmtree(directory)
                    removed += 1
            except OSError:
                continue
        return removed

    # --- internals ---

    def _directory(self, upload_id: str) -> Path:
        if not UPLOAD_ID.match(str(upload_id)):
            raise UploadNotFound(upload_id)
        directory = self.base / upload_id
        if not directory.is_dir():
            raise UploadNotFound(upload_id)
        return directory

    def _describe(self, directory: Path, meta: dict) -> dict:
        completed = meta["completed"]
        received = meta["size"] if completed else (directory / "data.part").stat().st_size
        description = {
            "upload_id": meta["upload_id"],
            "filename": meta["filename"],
            "size": meta["size"],
            "received": received,
            "complete": bool(completed),
        }
        if completed:
            description.update({"url": completed["url"], "path": completed["path"], "name": completed["name"]})
        return description

    @contextmanager
    def _locked(self, directory: Path) -> Iterator[None]:
        with open(directory / "lock", "a+b") as handle:
            if fcntl:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            else:  # pragma: no cover
                _FALLBACK_LOCK.acquire()
            try:
                yield
            finally:
                if fcntl:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                else:  # pragma: no cover
                    _FALLBACK_LOCK.release()

    @staticmethod
    def _read_meta(directory: Path) -> dict:
        return json.loads((directory / "meta.json").read_text(encoding="utf-8"))

    @staticmethod
    def _write_meta(directory: Path, meta: dict) -> None:
        temporary = directory / "meta.json.tmp"
        temporary.write_text(json.dumps(meta), encoding="utf-8")
        os.replace(temporary, directory / "meta.json")

    @staticmethod
    def _reserve_unique(directory: Path, filename: str) -> Path:
        """Create an empty file with a name nobody else has taken (name.ext, name-1.ext, ...) and return its path."""
        stem, suffix = os.path.splitext(filename)
        for counter in range(10000):
            candidate = filename if counter == 0 else f"{stem}-{counter}{suffix}"
            if len(candidate.encode("utf-8")) > MAX_NAME_BYTES:
                raise InvalidStoragePath("The file name is too long to make unique.")
            path = directory / candidate
            try:
                handle = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, FILE_MODE)
            except FileExistsError:
                continue
            os.close(handle)
            return path
        raise RuntimeError("Could not find a free file name.")
