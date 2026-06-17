from notion_sample_tracker.app import _sample_receipt
from notion_sample_tracker.models import SampleForm


def test_sample_receipt_includes_synthesis_and_processing_details():
    form = SampleForm(
        name="PTO54",
        sample_type="Root Sample",
        composition="PbTiO3",
        synthesis=["PLD"],
        synthesis_details="700 C growth comment",
        processing=["Anneal"],
        processing_details="Annealed for 2 hours",
    )

    receipt = _sample_receipt(form, {"url": "https://notion.example/sample"})
    rows = dict(receipt["rows"])

    assert rows["Synthesis"] == ["PLD"]
    assert rows["Synthesis Details"] == "700 C growth comment"
    assert rows["Processing"] == ["Anneal"]
    assert rows["Processing Details"] == "Annealed for 2 hours"
