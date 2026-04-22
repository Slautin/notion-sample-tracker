from __future__ import annotations

from io import BytesIO

import qrcode


def make_qr_png_bytes(value: str) -> bytes:
    image = qrcode.make(value)
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()
