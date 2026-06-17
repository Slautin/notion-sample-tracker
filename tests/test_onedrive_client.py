from notion_sample_tracker.services.onedrive_client import OneDriveClient


class CountingOneDriveClient(OneDriveClient):
    def __init__(self):
        super().__init__(
            tenant_id="tenant",
            client_id="client",
            client_secret="secret",
            root_folder="SampleTracker",
            drive_id="drive",
        )
        self.checked = []
        self.created = []

    def _path_exists(self, token, path):
        self.checked.append(path)
        return True

    def _create_folder(self, token, parent_path, name):
        self.created.append((parent_path, name))


def test_parent_folder_checks_are_cached_for_repeated_uploads():
    client = CountingOneDriveClient()

    client._ensure_parent_folders("token", "SampleTracker/samples/PTO/revisions/rev1/record.json")
    client._ensure_parent_folders("token", "SampleTracker/samples/PTO/revisions/rev1/changed_fields.json")

    assert client.created == []
    assert client.checked == [
        "SampleTracker",
        "SampleTracker/samples",
        "SampleTracker/samples/PTO",
        "SampleTracker/samples/PTO/revisions",
        "SampleTracker/samples/PTO/revisions/rev1",
    ]
