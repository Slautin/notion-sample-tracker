from __future__ import annotations

from collections import deque
import json
import os
from pathlib import Path
from threading import Lock

from notion_sample_tracker.models import BacklogEvent

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows fallback
    fcntl = None


class JsonlBacklog:
    def __init__(self, directory: Path):
        self.directory = directory
        self.directory.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()

    def append(self, event: BacklogEvent) -> Path:
        path = self.directory / f"{event.entity}.jsonl"
        with self._lock:
            with path.open("a", encoding="utf-8") as handle:
                _lock_file(handle)
                try:
                    handle.write(json.dumps(event.to_dict(), sort_keys=True) + "\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                finally:
                    _unlock_file(handle)
        return path

    def recent(self, entity: str, limit: int = 50) -> list[dict]:
        path = self.directory / f"{entity}.jsonl"
        if not path.exists():
            return []
        with path.open("r", encoding="utf-8") as handle:
            lines = deque(handle, maxlen=limit)
        events = []
        for line in lines:
            if not line.strip():
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return events


def _lock_file(handle) -> None:
    if fcntl:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)


def _unlock_file(handle) -> None:
    if fcntl:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
