from __future__ import annotations

import base64
from datetime import datetime, timezone
from io import BytesIO
import re
from typing import Any

from PIL import Image as PILImage
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_RIGHT
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import Image, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

try:
    import qrcode
except ImportError:  # pragma: no cover - deployment installs qrcode[pil].
    qrcode = None


PAGE_W, PAGE_H = letter
MAX_PREVIEW_IMAGES = 4
MAX_IMAGE_BYTES = 3 * 1024 * 1024

INK = colors.HexColor("#252a31")
MUTED = colors.HexColor("#667085")
LINE = colors.HexColor("#d8dee8")
SOFT = colors.HexColor("#f5f7fa")
PANEL = colors.HexColor("#ffffff")
ACCENT = colors.HexColor("#f47c20")
DARK = colors.HexColor("#30343a")


def make_receipt_pdf(title: str, rows: list[tuple[str, Any]], images: list[dict[str, Any]] | None = None) -> bytes:
    normalized_rows = [(str(key), _stringify(value)) for key, value in rows if _stringify(value)]
    notion_url = _row_value(normalized_rows, "Notion URL")
    record_type = _row_value(normalized_rows, "Record Type") or "Submission"
    receipt_no = _receipt_no(title, notion_url)
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    image_previews = _decode_images(images or [])

    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        leftMargin=0.62 * inch,
        rightMargin=0.62 * inch,
        topMargin=0.62 * inch,
        bottomMargin=0.62 * inch,
        title=title,
        author="Notion Sample Tracker",
    )
    styles = _styles()
    story: list[Any] = []
    story.extend(_header(title, record_type, receipt_no, generated_at, notion_url, styles))
    story.extend(_summary_cards(normalized_rows, styles))
    story.extend(_details_section(normalized_rows, styles))
    if image_previews:
        story.extend(_image_section(image_previews, styles))

    doc.build(story, onFirstPage=_footer, onLaterPages=_footer)
    return buffer.getvalue()


def _header(
    title: str,
    record_type: str,
    receipt_no: str,
    generated_at: str,
    notion_url: str,
    styles: dict[str, ParagraphStyle],
) -> list[Any]:
    left = [
        Paragraph("CAMM Sample Tracking", styles["eyebrow_light"]),
        Paragraph(_escape(title), styles["title"]),
        Paragraph(f"{_escape(record_type)} receipt", styles["subtitle"]),
    ]
    meta_rows = [
        [Paragraph("Receipt", styles["meta_label"]), Paragraph(_escape(receipt_no), styles["meta_value"])],
        [Paragraph("Generated", styles["meta_label"]), Paragraph(_escape(generated_at), styles["meta_value"])],
    ]
    meta = Table(meta_rows, colWidths=[0.62 * inch, 1.25 * inch], hAlign="RIGHT")
    meta.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                ("TOPPADDING", (0, 0), (-1, -1), 1),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ]
        )
    )
    right_items: list[Any] = [meta]
    qr = _qr_flowable(notion_url)
    if qr:
        right_items.extend([Spacer(1, 0.08 * inch), qr, Paragraph("Scan to open in Notion", styles["qr_note"])])

    table = Table([[left, right_items]], colWidths=[4.0 * inch, 2.25 * inch], hAlign="LEFT")
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), DARK),
                ("BOX", (0, 0), (-1, -1), 0.25, DARK),
                ("LEFTPADDING", (0, 0), (0, 0), 22),
                ("RIGHTPADDING", (0, 0), (0, 0), 14),
                ("LEFTPADDING", (1, 0), (1, 0), 8),
                ("RIGHTPADDING", (1, 0), (1, 0), 16),
                ("TOPPADDING", (0, 0), (-1, -1), 14),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 12),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LINEBELOW", (0, 0), (-1, -1), 4, ACCENT),
            ]
        )
    )
    return [table, Spacer(1, 0.22 * inch)]


def _summary_cards(rows: list[tuple[str, str]], styles: dict[str, ParagraphStyle]) -> list[Any]:
    card_fields = [
        ("Name", _row_value(rows, "Name")),
        ("Type", _row_value(rows, "Data Type") or _row_value(rows, "Sample Type")),
        ("Parent", _row_value(rows, "Sample") or _row_value(rows, "Parent Sample") or _row_value(rows, "Related Result")),
        ("Status", _row_value(rows, "Archive Status") or "Saved"),
    ]
    cells = []
    for label, value in card_fields:
        cells.append([Paragraph(_escape(label), styles["card_label"]), Paragraph(_escape(value or "-"), styles["card_value"])])
    table = Table([cells], colWidths=[1.54 * inch] * 4, hAlign="LEFT")
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), SOFT),
                ("BOX", (0, 0), (-1, -1), 0.5, LINE),
                ("INNERGRID", (0, 0), (-1, -1), 0.35, LINE),
                ("LEFTPADDING", (0, 0), (-1, -1), 12),
                ("RIGHTPADDING", (0, 0), (-1, -1), 12),
                ("TOPPADDING", (0, 0), (-1, -1), 9),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 9),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]
        )
    )
    return [table, Spacer(1, 0.24 * inch)]


def _details_section(rows: list[tuple[str, str]], styles: dict[str, ParagraphStyle]) -> list[Any]:
    hidden = {"Record Type"}
    link_labels = {"Notion URL", "Link"}
    normal_rows = [(label, value) for label, value in rows if label not in hidden and label not in link_labels]
    link_rows = [(label, value) for label, value in rows if label in link_labels]
    story: list[Any] = [Paragraph("Submission Details", styles["section"]), Spacer(1, 0.08 * inch)]

    detail_data = []
    for label, value in normal_rows:
        detail_data.append([Paragraph(_escape(label), styles["field_label"]), Paragraph(_escape(value), styles["field_value"])])
    if detail_data:
        table = Table(detail_data, colWidths=[1.55 * inch, 4.6 * inch], hAlign="LEFT", splitByRow=1)
        table.setStyle(_detail_table_style())
        story.append(table)
    if link_rows:
        story.extend([Spacer(1, 0.18 * inch), Paragraph("Links", styles["section_small"])])
        link_data = []
        for label, value in link_rows:
            link_data.append([Paragraph(_escape(label), styles["field_label"]), Paragraph(_link_text(value), styles["mono"])])
        link_table = Table(link_data, colWidths=[1.55 * inch, 4.6 * inch], hAlign="LEFT", splitByRow=1)
        link_table.setStyle(_detail_table_style())
        story.append(link_table)
    return story


def _image_section(images: list[dict[str, Any]], styles: dict[str, ParagraphStyle]) -> list[Any]:
    story: list[Any] = [Spacer(1, 0.24 * inch), Paragraph("Image Attachments", styles["section"]), Spacer(1, 0.1 * inch)]
    cells = []
    for item in images[:MAX_PREVIEW_IMAGES]:
        flowable = _image_flowable(item["bytes"])
        caption = Paragraph(_escape(item.get("name") or "Attachment"), styles["caption"])
        tile = Table([[flowable], [caption]], colWidths=[2.9 * inch], hAlign="CENTER")
        tile.setStyle(
            TableStyle(
                [
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 0),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                    ("TOPPADDING", (0, 0), (-1, -1), 0),
                    ("BOTTOMPADDING", (0, 0), (-1, 0), 4),
                    ("BOTTOMPADDING", (0, 1), (-1, -1), 0),
                ]
            )
        )
        cells.append(tile)
    rows = [cells[index : index + 2] for index in range(0, len(cells), 2)]
    for row in rows:
        while len(row) < 2:
            row.append("")
    table = Table(rows, colWidths=[3.0 * inch, 3.0 * inch], hAlign="LEFT")
    table.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 12),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 14),
            ]
        )
    )
    story.append(table)
    if len(images) > MAX_PREVIEW_IMAGES:
        story.append(Paragraph(f"{len(images) - MAX_PREVIEW_IMAGES} additional image(s) omitted from receipt preview.", styles["note"]))
    return story


def _detail_table_style() -> TableStyle:
    return TableStyle(
        [
            ("BACKGROUND", (0, 0), (-1, -1), PANEL),
            ("BOX", (0, 0), (-1, -1), 0.5, LINE),
            ("INNERGRID", (0, 0), (-1, -1), 0.25, LINE),
            ("LEFTPADDING", (0, 0), (-1, -1), 10),
            ("RIGHTPADDING", (0, 0), (-1, -1), 10),
            ("TOPPADDING", (0, 0), (-1, -1), 8),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ]
    )


def _styles() -> dict[str, ParagraphStyle]:
    base = getSampleStyleSheet()
    return {
        "title": ParagraphStyle("Title", parent=base["Normal"], fontName="Helvetica-Bold", fontSize=18, leading=22, textColor=colors.white),
        "subtitle": ParagraphStyle("Subtitle", parent=base["Normal"], fontSize=9, leading=12, textColor=colors.HexColor("#e9edf3")),
        "eyebrow_light": ParagraphStyle("EyebrowLight", parent=base["Normal"], fontName="Helvetica-Bold", fontSize=9, leading=12, textColor=colors.white),
        "meta_label": ParagraphStyle("MetaLabel", parent=base["Normal"], fontSize=7, leading=9, textColor=colors.HexColor("#d7dde8"), alignment=TA_RIGHT),
        "meta_value": ParagraphStyle("MetaValue", parent=base["Normal"], fontName="Helvetica-Bold", fontSize=8, leading=10, textColor=colors.white, alignment=TA_RIGHT),
        "qr_note": ParagraphStyle("QrNote", parent=base["Normal"], fontSize=6.8, leading=8, textColor=colors.HexColor("#e9edf3"), alignment=TA_CENTER),
        "card_label": ParagraphStyle("CardLabel", parent=base["Normal"], fontName="Helvetica-Bold", fontSize=7.5, leading=9, textColor=MUTED),
        "card_value": ParagraphStyle("CardValue", parent=base["Normal"], fontName="Helvetica-Bold", fontSize=10, leading=12, textColor=INK),
        "section": ParagraphStyle("Section", parent=base["Normal"], fontName="Helvetica-Bold", fontSize=13, leading=16, textColor=INK),
        "section_small": ParagraphStyle("SectionSmall", parent=base["Normal"], fontName="Helvetica-Bold", fontSize=10.5, leading=13, textColor=INK),
        "field_label": ParagraphStyle("FieldLabel", parent=base["Normal"], fontName="Helvetica-Bold", fontSize=8.5, leading=11, textColor=MUTED),
        "field_value": ParagraphStyle("FieldValue", parent=base["Normal"], fontSize=9.5, leading=12.5, textColor=INK),
        "mono": ParagraphStyle("Mono", parent=base["Normal"], fontName="Courier", fontSize=7.4, leading=9.5, textColor=INK, wordWrap="CJK"),
        "caption": ParagraphStyle("Caption", parent=base["Normal"], fontSize=8, leading=10, textColor=MUTED, alignment=TA_CENTER),
        "note": ParagraphStyle("Note", parent=base["Normal"], fontSize=8, leading=10, textColor=MUTED),
    }


def _footer(canvas, doc) -> None:
    canvas.saveState()
    canvas.setStrokeColor(LINE)
    canvas.setLineWidth(0.5)
    canvas.line(doc.leftMargin, 0.43 * inch, PAGE_W - doc.rightMargin, 0.43 * inch)
    canvas.setFont("Helvetica", 7.5)
    canvas.setFillColor(MUTED)
    canvas.drawString(doc.leftMargin, 0.25 * inch, "Generated by the Notion Sample Tracker web forms")
    canvas.drawRightString(PAGE_W - doc.rightMargin, 0.25 * inch, f"Page {doc.page}")
    canvas.restoreState()


def _qr_flowable(value: str) -> Image | None:
    if not value or qrcode is None:
        return None
    image = qrcode.make(value).convert("RGB")
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    buffer.seek(0)
    return Image(buffer, width=0.78 * inch, height=0.78 * inch, hAlign="RIGHT")


def _image_flowable(content: bytes) -> Image:
    image = PILImage.open(BytesIO(content)).convert("RGB")
    image.thumbnail((900, 650))
    buffer = BytesIO()
    image.save(buffer, format="JPEG", quality=86)
    buffer.seek(0)
    width, height = image.size
    max_w = 2.82 * inch
    max_h = 2.05 * inch
    scale = min(max_w / width, max_h / height, 1)
    return Image(buffer, width=width * scale, height=height * scale, hAlign="CENTER")


def _decode_images(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    decoded = []
    for item in items[: MAX_PREVIEW_IMAGES + 1]:
        if not isinstance(item, dict):
            continue
        data_url = str(item.get("data_url") or item.get("dataUrl") or "")
        if not data_url.startswith("data:image/") or "," not in data_url:
            continue
        try:
            content = base64.b64decode(data_url.split(",", 1)[1], validate=True)
        except Exception:
            continue
        if not content or len(content) > MAX_IMAGE_BYTES:
            continue
        decoded.append({"name": str(item.get("name") or "Attachment"), "bytes": content})
    return decoded


def _row_value(rows: list[tuple[str, str]], label: str) -> str:
    for key, value in rows:
        if key.lower() == label.lower():
            return value
    return ""


def _stringify(value: Any) -> str:
    if isinstance(value, list):
        return ", ".join(str(item) for item in value if str(item))
    return str(value or "")


def _receipt_no(title: str, notion_url: str) -> str:
    source = notion_url or title or datetime.now(timezone.utc).isoformat()
    cleaned = re.sub(r"[^A-Za-z0-9]+", "", source)
    return (cleaned[-10:] or "receipt").upper()


def _link_text(value: str) -> str:
    escaped = _escape(value)
    if value.startswith(("http://", "https://")):
        return f'<link href="{escaped}" color="#1d4ed8">{escaped}</link>'
    return escaped


def _escape(value: Any) -> str:
    text = str(value or "")
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\r", " ").replace("\n", "<br/>")
