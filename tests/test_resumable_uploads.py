import hashlib
import os
import threading
import time
from io import BytesIO

import pytest

from notion_sample_tracker.services.local_storage import (
    InvalidStoragePath,
    LocalStorage,
    OffsetMismatch,
    ResumableUploads,
    StorageFull,
    UploadIncomplete,
    UploadNotFound,
)


BASE = "https://tracker.example.utk.edu"
TARGET = "samples/PbTiO3/PTO54/results/run_1"


def _uploads(tmp_path, chunk=4, max_file=10_000, ttl=3600, **storage_kwargs):
    storage = LocalStorage(tmp_path / "files", BASE, **storage_kwargs)
    storage.check_writable()
    return ResumableUploads(storage, max_file_bytes=max_file, chunk_bytes=chunk, ttl_seconds=ttl), storage


def _send(uploads, upload_id, offset, data):
    return uploads.append(upload_id, offset, BytesIO(data), len(data))


def _upload_whole(uploads, name, data, target=TARGET, chunk=4):
    info = uploads.create(name, len(data), target)
    offset = 0
    while offset < len(data):
        piece = data[offset : offset + chunk]
        offset = _send(uploads, info["upload_id"], offset, piece)["received"]
    return uploads.complete(info["upload_id"])


# --- the normal path ---


def test_a_file_arrives_in_chunks_and_lands_in_the_target_folder(tmp_path):
    uploads, storage = _uploads(tmp_path)
    data = b"0123456789"

    info = uploads.create("scan.csv", len(data), TARGET)
    assert info["received"] == 0 and info["complete"] is False
    assert _send(uploads, info["upload_id"], 0, data[0:4])["received"] == 4
    assert _send(uploads, info["upload_id"], 4, data[4:8])["received"] == 8
    assert _send(uploads, info["upload_id"], 8, data[8:])["received"] == 10
    done = uploads.complete(info["upload_id"])

    assert done["complete"] is True
    assert done["path"] == f"{TARGET}/scan.csv"
    assert done["url"] == f"{BASE}/files/{TARGET}/scan.csv"
    assert (storage.root / TARGET / "scan.csv").read_bytes() == data


def test_the_staging_file_is_gone_once_the_upload_is_complete(tmp_path):
    uploads, _ = _uploads(tmp_path)
    info = uploads.create("a.bin", 4, TARGET)
    _send(uploads, info["upload_id"], 0, b"abcd")

    uploads.complete(info["upload_id"])

    assert not (uploads.base / info["upload_id"] / "data.part").exists()


def test_a_multi_megabyte_file_survives_intact(tmp_path):
    uploads, storage = _uploads(tmp_path, chunk=1024 * 1024, max_file=50 * 1024 * 1024)
    data = os.urandom(5 * 1024 * 1024 + 123)

    done = _upload_whole(uploads, "big.bin", data, chunk=1024 * 1024)

    stored = (storage.root / done["path"]).read_bytes()
    assert hashlib.sha256(stored).digest() == hashlib.sha256(data).digest()


def test_a_finished_file_has_restricted_permissions(tmp_path):
    uploads, storage = _uploads(tmp_path)

    done = _upload_whole(uploads, "a.bin", b"abcdefgh")

    assert oct(os.stat(storage.root / done["path"]).st_mode & 0o777) == "0o640"


# --- resuming and mismatches ---


def test_a_chunk_sent_at_the_wrong_offset_reports_where_the_server_is(tmp_path):
    uploads, _ = _uploads(tmp_path)
    info = uploads.create("a.bin", 12, TARGET)
    _send(uploads, info["upload_id"], 0, b"abcd")

    with pytest.raises(OffsetMismatch) as caught:
        _send(uploads, info["upload_id"], 0, b"abcd")  # a retry of a chunk that already arrived
    assert caught.value.received == 4

    with pytest.raises(OffsetMismatch) as caught:
        _send(uploads, info["upload_id"], 8, b"ijkl")  # skipping ahead
    assert caught.value.received == 4

    assert uploads.status(info["upload_id"])["received"] == 4


def test_a_dropped_connection_keeps_what_arrived_and_the_upload_continues_from_there(tmp_path):
    uploads, storage = _uploads(tmp_path)
    info = uploads.create("a.bin", 8, TARGET)

    with pytest.raises(ValueError, match="ended after 2 of 4"):
        uploads.append(info["upload_id"], 0, BytesIO(b"ab"), 4)  # the client said 4 bytes but only 2 came

    assert uploads.status(info["upload_id"])["received"] == 2
    _send(uploads, info["upload_id"], 2, b"cdef")
    _send(uploads, info["upload_id"], 6, b"gh")
    done = uploads.complete(info["upload_id"])
    assert (storage.root / done["path"]).read_bytes() == b"abcdefgh"


def test_two_writers_at_the_same_offset_cannot_both_win(tmp_path):
    uploads, _ = _uploads(tmp_path, chunk=1024)
    info = uploads.create("a.bin", 2048, TARGET)
    barrier = threading.Barrier(2)
    outcomes = []

    def writer(payload):
        barrier.wait()
        try:
            _send(uploads, info["upload_id"], 0, payload)
            outcomes.append("ok")
        except OffsetMismatch:
            outcomes.append("mismatch")

    threads = [threading.Thread(target=writer, args=(byte * 1024,)) for byte in (b"a", b"b")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sorted(outcomes) == ["mismatch", "ok"]
    assert uploads.status(info["upload_id"])["received"] == 1024


# --- limits and validation ---


def test_chunk_size_and_declared_size_are_enforced(tmp_path):
    uploads, _ = _uploads(tmp_path, chunk=4)
    info = uploads.create("a.bin", 6, TARGET)

    with pytest.raises(ValueError, match="larger than 4"):
        _send(uploads, info["upload_id"], 0, b"abcde")
    with pytest.raises(ValueError, match="empty"):
        _send(uploads, info["upload_id"], 0, b"")
    _send(uploads, info["upload_id"], 0, b"abcd")
    with pytest.raises(ValueError, match="past the size"):
        _send(uploads, info["upload_id"], 4, b"efg")

    assert uploads.status(info["upload_id"])["received"] == 4


@pytest.mark.parametrize("size", [0, -1, 10_001, True, "10", 1.5, None])
def test_the_announced_size_must_be_a_positive_whole_number_within_the_limit(tmp_path, size):
    uploads, _ = _uploads(tmp_path, max_file=10_000)

    with pytest.raises(ValueError):
        uploads.create("a.bin", size, TARGET)


@pytest.mark.parametrize("name", ["a/b.csv", ".hidden", "..", "", "a\\b.csv"])
def test_the_file_name_must_be_a_single_plain_name(tmp_path, name):
    uploads, _ = _uploads(tmp_path)

    with pytest.raises(InvalidStoragePath):
        uploads.create(name, 4, TARGET)


@pytest.mark.parametrize("target", ["../escape", "a/../../b", ".uploads", "", "a/.hidden/b"])
def test_the_target_folder_cannot_escape_or_hit_the_staging_area(tmp_path, target):
    uploads, _ = _uploads(tmp_path)

    with pytest.raises(InvalidStoragePath):
        uploads.create("a.bin", 4, target)


def test_a_nearly_full_disk_refuses_new_uploads_and_further_chunks(tmp_path):
    uploads, storage = _uploads(tmp_path)
    started = uploads.create("a.bin", 8, TARGET)
    storage.min_free_bytes = 10**18  # the disk fills up while the upload is under way

    with pytest.raises(StorageFull):
        _send(uploads, started["upload_id"], 0, b"abcd")
    with pytest.raises(StorageFull):
        uploads.create("b.bin", 4, TARGET)

    assert uploads.status(started["upload_id"])["received"] == 0


def test_unknown_or_malformed_upload_ids_are_not_found(tmp_path):
    uploads, _ = _uploads(tmp_path)

    for bad in ("../files", "short", "A" * 32, "g" * 32, "0" * 32, "", None):
        with pytest.raises(UploadNotFound):
            uploads.status(bad)
        with pytest.raises(UploadNotFound):
            uploads.complete(bad)


# --- completing ---


def test_completing_early_is_refused_and_the_upload_stays_usable(tmp_path):
    uploads, _ = _uploads(tmp_path)
    info = uploads.create("a.bin", 8, TARGET)
    _send(uploads, info["upload_id"], 0, b"abcd")

    with pytest.raises(UploadIncomplete) as caught:
        uploads.complete(info["upload_id"])
    assert (caught.value.received, caught.value.size) == (4, 8)

    _send(uploads, info["upload_id"], 4, b"efgh")
    assert uploads.complete(info["upload_id"])["complete"] is True


def test_completing_twice_returns_the_same_file(tmp_path):
    uploads, storage = _uploads(tmp_path)
    info = uploads.create("a.bin", 4, TARGET)
    _send(uploads, info["upload_id"], 0, b"abcd")

    first = uploads.complete(info["upload_id"])
    second = uploads.complete(info["upload_id"])

    assert first["path"] == second["path"] and first["url"] == second["url"]
    assert sorted(path.name for path in (storage.root / TARGET).iterdir()) == ["a.bin"]


def test_nothing_more_can_be_added_after_completion(tmp_path):
    uploads, _ = _uploads(tmp_path)
    info = uploads.create("a.bin", 4, TARGET)
    _send(uploads, info["upload_id"], 0, b"abcd")
    uploads.complete(info["upload_id"])

    with pytest.raises(ValueError, match="already complete"):
        _send(uploads, info["upload_id"], 4, b"x")


def test_a_name_that_is_taken_gets_a_number_instead_of_overwriting(tmp_path):
    uploads, storage = _uploads(tmp_path)

    first = _upload_whole(uploads, "scan.tar.gz", b"one!")
    second = _upload_whole(uploads, "scan.tar.gz", b"two!")
    third = _upload_whole(uploads, "scan.tar.gz", b"six!")

    assert [first["name"], second["name"], third["name"]] == ["scan.tar.gz", "scan.tar-1.gz", "scan.tar-2.gz"]
    assert (storage.root / first["path"]).read_bytes() == b"one!"
    assert (storage.root / second["path"]).read_bytes() == b"two!"


def test_an_existing_file_is_never_replaced_even_by_a_concurrent_finish(tmp_path):
    uploads, storage = _uploads(tmp_path)
    infos = [uploads.create("same.bin", 4, TARGET) for _ in range(6)]
    for index, info in enumerate(infos):
        _send(uploads, info["upload_id"], 0, bytes([65 + index]) * 4)
    barrier = threading.Barrier(len(infos))
    results = []

    def finish(info):
        barrier.wait()
        results.append(uploads.complete(info["upload_id"])["path"])

    threads = [threading.Thread(target=finish, args=(info,)) for info in infos]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(set(results)) == 6
    contents = sorted((storage.root / path).read_bytes() for path in results)
    assert contents == sorted(bytes([65 + index]) * 4 for index in range(6))


# --- clean-up ---


def _age(path, seconds):
    old = time.time() - seconds
    for item in [path, *path.iterdir()]:
        os.utime(item, (old, old))


def test_stale_uploads_are_removed_and_fresh_ones_kept(tmp_path):
    uploads, _ = _uploads(tmp_path, ttl=3600)
    stale = uploads.create("old.bin", 4, TARGET)
    fresh = uploads.create("new.bin", 4, TARGET)
    _age(uploads.base / stale["upload_id"], 7200)

    removed = uploads.cleanup_stale()

    assert removed == 1
    with pytest.raises(UploadNotFound):
        uploads.status(stale["upload_id"])
    assert uploads.status(fresh["upload_id"])["filename"] == "new.bin"


def test_starting_an_upload_also_clears_old_ones(tmp_path):
    uploads, _ = _uploads(tmp_path, ttl=3600)
    stale = uploads.create("old.bin", 4, TARGET)
    _age(uploads.base / stale["upload_id"], 7200)

    uploads.create("new.bin", 4, TARGET)

    with pytest.raises(UploadNotFound):
        uploads.status(stale["upload_id"])


def test_an_upload_that_is_still_receiving_data_is_not_considered_stale(tmp_path):
    uploads, _ = _uploads(tmp_path, ttl=3600)
    info = uploads.create("a.bin", 8, TARGET)
    _age(uploads.base / info["upload_id"], 7200)
    _send(uploads, info["upload_id"], 0, b"abcd")  # new activity refreshes the file times

    assert uploads.cleanup_stale() == 0
