from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Any


def make_receipt_pdf(title: str, rows: list[tuple[str, Any]]) -> bytes:
    lines = [title, f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}", ""]
    for key, value in rows:
        text = ", ".join(str(item) for item in value) if isinstance(value, list) else str(value or "")
        lines.extend(_wrap(f"{key}: {text}", 92))
    return _build_pdf(lines)


def _wrap(text: str, width: int) -> list[str]:
    words = text.split()
    if not words:
        return [""]
    lines: list[str] = []
    current = words[0]
    for word in words[1:]:
        if len(current) + len(word) + 1 > width:
            lines.append(current)
            current = word
        else:
            current += " " + word
    lines.append(current)
    return lines


def _build_pdf(lines: list[str]) -> bytes:
    y = 760
    commands = ["BT", "/F1 11 Tf", "72 760 Td"]
    first = True
    for line in lines:
        if not first:
            commands.append("0 -16 Td")
        first = False
        commands.append(f"({_escape_pdf(line)}) Tj")
        y -= 16
        if y < 60:
            break
    commands.append("ET")
    stream = "\n".join(commands).encode("latin-1", "replace")

    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        b"<< /Length " + str(len(stream)).encode("ascii") + b" >>\nstream\n" + stream + b"\nendstream",
    ]

    pdf = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for index, obj in enumerate(objects, start=1):
        offsets.append(len(pdf))
        pdf.extend(f"{index} 0 obj\n".encode("ascii"))
        pdf.extend(obj)
        pdf.extend(b"\nendobj\n")
    xref = len(pdf)
    pdf.extend(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
    pdf.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        pdf.extend(f"{offset:010d} 00000 n \n".encode("ascii"))
    pdf.extend(f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode("ascii"))
    return bytes(pdf)


def _escape_pdf(value: str) -> str:
    value = re.sub(r"[\r\n\t]+", " ", value)
    return value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
