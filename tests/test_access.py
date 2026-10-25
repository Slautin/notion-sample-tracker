from types import SimpleNamespace

import pytest

from notion_sample_tracker.access import credentials_ok, origin_allowed


def _basic(username="lab", password="correct horse battery"):
    return SimpleNamespace(type="basic", username=username, password=password)


def test_matching_basic_credentials_are_accepted():
    assert credentials_ok(_basic(), "lab", "correct horse battery") is True


@pytest.mark.parametrize(
    "authorization",
    [
        None,
        SimpleNamespace(type="bearer", username=None, password=None),
        _basic(password="wrong"),
        _basic(username="someone-else"),
        _basic(username="", password=""),
        _basic(username=None, password=None),
        _basic(password="correct horse battery "),
        _basic(password="CORRECT HORSE BATTERY"),
    ],
)
def test_anything_else_is_refused(authorization):
    assert credentials_ok(authorization, "lab", "correct horse battery") is False


def test_non_ascii_passwords_work():
    assert credentials_ok(_basic(password="pässwörd-é-世界"), "lab", "pässwörd-é-世界") is True
    assert credentials_ok(_basic(password="passw0rd"), "lab", "pässwörd-é-世界") is False


@pytest.mark.parametrize(
    "origin, request_host, allowed",
    [
        (None, "tracker.utk.edu", True),  # not a browser, or not cross-site
        ("", "tracker.utk.edu", True),
        ("https://tracker.utk.edu", "tracker.utk.edu", True),
        ("https://TRACKER.utk.edu", "tracker.utk.edu", True),
        ("https://tracker.utk.edu", "127.0.0.1:8000", True),  # matches PUBLIC_BASE_URL even if the proxy rewrites Host
        ("http://127.0.0.1:8000", "127.0.0.1:8000", True),
        ("https://evil.example", "tracker.utk.edu", False),
        ("https://tracker.utk.edu.evil.example", "tracker.utk.edu", False),
        ("null", "tracker.utk.edu", False),
        ("https://tracker.utk.edu:8443", "tracker.utk.edu", False),
    ],
)
def test_cross_site_origins_are_refused(origin, request_host, allowed):
    assert origin_allowed(origin, request_host, "https://tracker.utk.edu") is allowed
