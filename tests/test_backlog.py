from notion_sample_tracker.models import BacklogEvent
from notion_sample_tracker.services.backlog import JsonlBacklog


def test_backlog_appends_and_reads_recent_events(tmp_path):
    backlog = JsonlBacklog(tmp_path)
    backlog.append(BacklogEvent(action="create", entity="sample", payload={"name": "S1"}))

    events = backlog.recent("sample")

    assert len(events) == 1
    assert events[0]["payload"]["name"] == "S1"
