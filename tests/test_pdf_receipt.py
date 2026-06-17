from base64 import b64encode
from io import BytesIO

from PIL import Image
from pypdf import PdfReader

from notion_sample_tracker.services.pdf_receipt import make_receipt_pdf


def _png_data_url() -> str:
    image = Image.new("RGB", (120, 80), (240, 124, 32))
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    return "data:image/png;base64," + b64encode(buffer.getvalue()).decode("ascii")


def test_receipt_pdf_has_sections_and_image_preview():
    pdf = make_receipt_pdf(
        "Result Submission - image0_LiNbO3",
        [
            ("Record Type", "Result"),
            ("Name", "image0_LiNbO3"),
            ("Data Type", "other"),
            ("Upload Method", "file"),
            ("Parent Entry", "sample"),
            ("Sample", "LiNbO3_film"),
            ("Characterisation", "Raman"),
            ("Link", "https://example.com/data"),
            ("Submission ID", "test-submission-id"),
            ("Notion URL", "https://notion.example/page"),
        ],
        images=[{"name": "preview.png", "data_url": _png_data_url()}],
    )

    reader = PdfReader(BytesIO(pdf))
    text = "\n".join(page.extract_text() or "" for page in reader.pages)

    assert len(reader.pages) >= 1
    assert "CAMM Sample Tracking" in text
    assert "Submission Details" in text
    assert "Image Attachments" in text
    assert "preview.png" in text
