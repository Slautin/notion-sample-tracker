import os
import stat
from io import BytesIO

import pytest

from notion_sample_tracker.services.local_storage import InvalidStoragePath, LocalStorage, StorageFull


BASE = "https://tracker.example.utk.edu"


def _storage(tmp_path, **kwargs):
    storage = LocalStorage(tmp_path / "files", BASE, **kwargs)
    storage.check_writable()
    return storage


def _leftover_temp_files(storage):
    return [path for path in storage.root.rglob(".tmp-*")]


# --- writing ---


def test_upload_bytes_writes_the_file_and_returns_path_and_url(tmp_path):
    storage = _storage(tmp_path)

    result = storage.upload_bytes("samples/PTO/PTO54/photos/a b.png", b"data", "image/png")

    assert result.path == "samples/PTO/PTO54/photos/a b.png"
    assert result.web_url == f"{BASE}/files/samples/PTO/PTO54/photos/a%20b.png"
    assert (storage.root / "samples/PTO/PTO54/photos/a b.png").read_bytes() == b"data"
    assert _leftover_temp_files(storage) == []


def test_files_are_not_world_readable(tmp_path):
    storage = _storage(tmp_path)

    storage.upload_bytes("a/b.txt", b"x")

    assert stat.S_IMODE(os.stat(storage.root / "a/b.txt").st_mode) == 0o640


def test_writing_again_replaces_the_file_like_onedrive_does(tmp_path):
    storage = _storage(tmp_path)

    storage.upload_bytes("a/record.json", b"old")
    storage.upload_bytes("a/record.json", b"new")

    assert (storage.root / "a/record.json").read_bytes() == b"new"
    assert _leftover_temp_files(storage) == []


def test_upload_json_is_sorted_readable_json(tmp_path):
    storage = _storage(tmp_path)

    storage.upload_json("a/record.json", {"b": 1, "a": {"é": "ü"}})

    assert (storage.root / "a/record.json").read_text(encoding="utf-8") == '{\n  "a": {\n    "\\u00e9": "\\u00fc"\n  },\n  "b": 1\n}'


def test_upload_file_streams_the_content(tmp_path):
    storage = _storage(tmp_path)
    payload = os.urandom(3 * 1024 * 1024 + 17)

    result = storage.upload_file("results/x/data.bin", BytesIO(payload), "application/octet-stream")

    assert (storage.root / result.path).read_bytes() == payload


def test_a_failed_write_leaves_neither_a_partial_file_nor_a_temp_file(tmp_path):
    storage = _storage(tmp_path)

    class Exploding:
        def read(self, size=-1):
            raise OSError("disk went away")

    with pytest.raises(OSError):
        storage.upload_file("results/x/data.bin", Exploding())

    assert not (storage.root / "results/x/data.bin").exists()
    assert _leftover_temp_files(storage) == []


def test_unicode_names_work(tmp_path):
    storage = _storage(tmp_path)

    result = storage.upload_bytes("samples/образец/пленка.png", b"x")

    assert (storage.root / result.path).read_bytes() == b"x"
    assert storage.path_for_url(result.web_url) == "samples/образец/пленка.png"


def test_a_full_disk_is_refused_before_anything_is_written(tmp_path):
    storage = _storage(tmp_path, min_free_bytes=10**18)

    with pytest.raises(StorageFull):
        storage.upload_bytes("a/b.txt", b"x")

    assert not (storage.root / "a").exists()


# --- path safety ---


@pytest.mark.parametrize(
    "bad",
    ["../escape.txt", "a/../../escape.txt", "a/./b", ".hidden/x", "a/.hidden", ".uploads/x/data.part", "..", ".", "", "/", "a\\b", "a\x00b"],
)
def test_unsafe_paths_are_refused_for_writing_and_reading(tmp_path, bad):
    storage = _storage(tmp_path)

    with pytest.raises(InvalidStoragePath):
        storage.upload_bytes(bad, b"x")
    with pytest.raises(FileNotFoundError):
        storage.open_for_download(bad)

    assert not (tmp_path / "escape.txt").exists()


def test_a_leading_slash_is_ignored_like_onedrive_paths(tmp_path):
    storage = _storage(tmp_path)

    result = storage.upload_bytes("/samples/a.txt", b"x")

    assert result.path == "samples/a.txt"
    assert (storage.root / "samples/a.txt").exists()


def test_a_path_segment_longer_than_the_file_system_allows_is_refused(tmp_path):
    storage = _storage(tmp_path)

    with pytest.raises(InvalidStoragePath, match="too long"):
        storage.upload_bytes("samples/" + "я" * 200 + "/a.txt", b"x")  # 400 bytes in UTF-8


def test_symlinks_cannot_be_used_to_leave_the_storage_root(tmp_path):
    storage = _storage(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.txt").write_text("secret")
    os.symlink(outside, storage.root / "link")
    (storage.root / "samples").mkdir()
    os.symlink(outside / "secret.txt", storage.root / "samples" / "linked.txt")

    with pytest.raises(InvalidStoragePath):
        storage.upload_bytes("link/new.txt", b"x")
    with pytest.raises(FileNotFoundError):
        storage.open_for_download("link/secret.txt")
    with pytest.raises(FileNotFoundError):
        storage.open_for_download("samples/linked.txt")

    assert not (outside / "new.txt").exists()


def test_the_root_itself_may_be_a_symlink(tmp_path):
    real = tmp_path / "mnt" / "data"
    real.mkdir(parents=True)
    os.symlink(real, tmp_path / "files-link")
    storage = LocalStorage(tmp_path / "files-link", BASE)
    storage.check_writable()

    storage.upload_bytes("a/b.txt", b"x")

    assert (real / "a/b.txt").read_bytes() == b"x"


# --- reading and links ---


def test_open_for_download_only_returns_plain_files(tmp_path):
    storage = _storage(tmp_path)
    storage.upload_bytes("a/b.txt", b"x")

    assert storage.open_for_download("a/b.txt").read_bytes() == b"x"
    with pytest.raises(FileNotFoundError):
        storage.open_for_download("a")  # a directory
    with pytest.raises(FileNotFoundError):
        storage.open_for_download("a/missing.txt")


def test_path_for_url_accepts_only_links_to_stored_files_of_this_server(tmp_path):
    storage = _storage(tmp_path)
    stored = storage.upload_bytes("samples/a b/c.txt", b"x")
    (storage.root / "samples" / "folder").mkdir()

    assert storage.path_for_url(stored.web_url) == "samples/a b/c.txt"
    assert storage.path_for_url(stored.web_url + "?download=1#frag") == "samples/a b/c.txt"
    assert storage.path_for_url(f"{BASE}/files/samples/missing.txt") is None
    assert storage.path_for_url(f"{BASE}/files/samples/folder") is None  # a directory
    assert storage.path_for_url(f"{BASE}/files/samples/%2e%2e/%2e%2e/etc/passwd") is None
    assert storage.path_for_url(f"{BASE}/files/.uploads/anything") is None
    assert storage.path_for_url("https://evil.example/files/samples/a b/c.txt") is None
    assert storage.path_for_url("https://evil.example/anything") is None
    # same length as our prefix but a different start: only the prefix check can tell it is not our link
    assert storage.path_for_url("x" * len(f"{BASE}/files/") + "samples/a b/c.txt") is None
    assert storage.path_for_url("") is None
    assert storage.path_for_url(None) is None


def test_the_public_url_may_have_a_trailing_slash_and_a_path_prefix(tmp_path):
    storage = LocalStorage(tmp_path / "files", "https://example.utk.edu/tracker/")
    storage.check_writable()

    result = storage.upload_bytes("a/b.txt", b"x")

    assert result.web_url == "https://example.utk.edu/tracker/files/a/b.txt"
    assert storage.path_for_url(result.web_url) == "a/b.txt"


# --- startup check ---


def test_check_writable_creates_the_root_and_the_staging_area(tmp_path):
    storage = LocalStorage(tmp_path / "deep" / "files", BASE)

    storage.check_writable()

    assert (tmp_path / "deep" / "files" / ".uploads").is_dir()
    assert not list((tmp_path / "deep" / "files").glob(".write-test-*"))


def test_check_writable_explains_how_to_fix_an_unwritable_root(tmp_path):
    blocker = tmp_path / "afile"
    blocker.write_text("x")
    storage = LocalStorage(blocker / "files", BASE)

    with pytest.raises(RuntimeError, match="RAILWAY_RUN_UID=0"):
        storage.check_writable()
