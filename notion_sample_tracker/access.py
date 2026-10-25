"""Optional password gate and a same-origin check for state-changing requests.

When the app serves files itself (STORAGE_BACKEND=local) it must not be open to the world. Either set
ACCESS_PASSWORD (HTTP Basic, meant for prototypes and small labs) or put a login in front of the app and set
ACCESS_BEHIND_AUTH_PROXY=true. The health endpoints stay open so platform health checks keep working.
"""
from __future__ import annotations

import hmac
from urllib.parse import urlparse

from flask import Flask, Response, request

from notion_sample_tracker.settings import Settings


OPEN_PATHS = frozenset({"/health", "/healthz"})
UNSAFE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})


def _same(left: str, right: str) -> bool:
    return hmac.compare_digest(left.encode("utf-8"), right.encode("utf-8"))


def credentials_ok(authorization, username: str, password: str) -> bool:
    if authorization is None or getattr(authorization, "type", "") != "basic":
        return False
    # Both comparisons always run, so timing does not reveal which one failed.
    user_ok = _same(authorization.username or "", username)
    password_ok = _same(authorization.password or "", password)
    return user_ok and password_ok


def origin_allowed(origin: str | None, request_host: str, public_base_url: str) -> bool:
    """A browser sends Origin on cross-site form posts; refuse the ones that do not come from this site."""
    if not origin:
        return True  # not a browser request, or not a cross-site one
    host = urlparse(origin).netloc.lower()
    return bool(host) and host in {request_host.lower(), urlparse(public_base_url).netloc.lower()}


def install_access_control(app: Flask, settings: Settings) -> None:
    @app.before_request
    def enforce_access():
        if request.path in OPEN_PATHS:
            return None
        if settings.access_password and not credentials_ok(request.authorization, settings.access_username, settings.access_password):
            return Response(
                "Authentication required.",
                401,
                {"WWW-Authenticate": 'Basic realm="Sample Tracker", charset="UTF-8"'},
            )
        if request.method in UNSAFE_METHODS and not origin_allowed(
            request.headers.get("Origin"), request.host, settings.public_base_url
        ):
            return Response("Cross-site request refused.", 403)
        return None
