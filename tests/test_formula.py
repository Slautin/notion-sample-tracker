from notion_sample_tracker.services.formula import FormulaParser


def test_formula_parser_normalizes_and_extracts_elements():
    parsed = FormulaParser().parse("Fe2O3")

    assert parsed.normalized_formula == "Fe2O3"
    assert parsed.elements == ["Fe", "O"]
    assert parsed.element_key == "FeO"


def test_formula_parser_rejects_empty_formula():
    parser = FormulaParser()

    try:
        parser.parse("")
    except ValueError as exc:
        assert "Composition is required" in str(exc)
    else:
        raise AssertionError("Expected ValueError")
