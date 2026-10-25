"""Shared stand-in for the requests module as used by onedrive_client and onedrive_check."""
import json


UTK_LIBRARY = {
    "id": "drive",
    "driveType": "documentLibrary",
    "webUrl": "https://utk.sharepoint.com/sites/lab/Shared%20Documents",
}


class FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = json.dumps(self._payload)

    @property
    def ok(self):
        return self.status_code < 400

    def json(self):
        return self._payload

    def raise_for_status(self):
        if not self.ok:
            raise RuntimeError(f"HTTP {self.status_code}")


def _error(status):
    return {"error": {"code": "accessDenied" if status == 403 else "graph_error", "message": f"HTTP {status}"}}


class FakeRequests:
    """Every folder already exists. Statuses and the token error can be changed per test."""

    def __init__(
        self,
        drive=None,
        expires_in=3600,
        drive_status=200,
        root_status=200,
        root=None,
        token_error=None,
        put_status=200,
        cancel_status=204,
    ):
        self.drive = dict(UTK_LIBRARY if drive is None else drive)
        self.root = dict({"webUrl": self.drive.get("webUrl", "")} if root is None else root)
        self.expires_in = expires_in
        self.drive_status = drive_status
        self.root_status = root_status
        self.token_error = token_error
        self.put_status = put_status
        self.cancel_status = cancel_status
        self.calls = []
        self.token_urls = []

    def count(self, method, contains=""):
        return sum(1 for call_method, url in self.calls if call_method == method and contains in url)

    def drive_checks(self):
        return self.count("GET", "$select=id,driveType")

    def root_checks(self):
        return self.count("GET", "/root?")

    def post(self, url, **kwargs):
        self.calls.append(("POST", url))
        if "login.microsoftonline.com" in url:
            self.token_urls.append(url)
            if self.token_error:
                return FakeResponse(*self.token_error)
            return FakeResponse(200, {"access_token": f"token-{len(self.token_urls)}", "expires_in": self.expires_in})
        if url.endswith("/createUploadSession"):
            return FakeResponse(200, {"uploadUrl": "https://upload.example/session"})
        return FakeResponse(201, {})

    def get(self, url, **kwargs):
        self.calls.append(("GET", url))
        if "/root:" in url:
            return FakeResponse(200, {})
        if "/root?" in url:
            return FakeResponse(self.root_status, self.root if self.root_status < 400 else _error(self.root_status))
        return FakeResponse(self.drive_status, self.drive if self.drive_status < 400 else _error(self.drive_status))

    def put(self, url, **kwargs):
        self.calls.append(("PUT", url))
        if self.put_status >= 400:
            return FakeResponse(self.put_status, _error(self.put_status))
        return FakeResponse(200, {"webUrl": "https://utk.sharepoint.com/sites/lab/file"})

    def delete(self, url, **kwargs):
        self.calls.append(("DELETE", url))
        return FakeResponse(self.cancel_status, {})
