import os

import pytest

from notion_sample_tracker.models import BacklogEvent
from notion_sample_tracker.services.backlog import JsonlBacklog


def test_backlog_appends_and_reads_recent_events(tmp_path):
    backlog = JsonlBacklog(tmp_path)
    backlog.append(BacklogEvent(action="create", entity="sample", payload={"name": "S1"}))

    events = backlog.recent("sample")

    assert len(events) == 1
    assert events[0]["payload"]["name"] == "S1"


def test_a_directory_that_cannot_be_created_is_explained(tmp_path):
    blocker = tmp_path / "afile"
    blocker.write_text("x")

    with pytest.raises(RuntimeError, match="RAILWAY_RUN_UID=0"):
        JsonlBacklog(blocker / "backlog")


@pytest.mark.skipif(os.geteuid() == 0, reason="root can write anywhere, so a read-only directory proves nothing")
def test_an_existing_directory_that_cannot_be_written_is_explained(tmp_path):
    locked = tmp_path / "locked"
    locked.mkdir()
    locked.chmod(0o500)
    try:
        with pytest.raises(RuntimeError, match="not writable"):
            JsonlBacklog(locked)
    finally:
        locked.chmod(0o700)
