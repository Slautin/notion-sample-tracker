from __future__ import annotations

import json
from pathlib import Path
from threading import Lock

from notion_sample_tracker.models import BacklogEvent


class JsonlBacklog:
    def __init__(self, directory: Path):
        self.directory = directory
        self.directory.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()

    def append(self, event: BacklogEvent) -> Path:
        path = self.directory / f"{event.entity}.jsonl"
        with self._lock:
            with path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(event.to_dict(), sort_keys=True) + "\n")
        return path

    def recent(self, entity: str, limit: int = 50) -> list[dict]:
        path = self.directory / f"{entity}.jsonl"
        if not path.exists():
            return []
        lines = path.read_text(encoding="utf-8").splitlines()[-limit:]
        return [json.loads(line) for line in lines if line.strip()]
