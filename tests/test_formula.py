import pytest

from notion_sample_tracker.services.formula import FormulaParser


@pytest.mark.parametrize(
    "typed, stored, elements",
    [
        ("Fe2O3", "Fe2O3", ["Fe", "O"]),
        ("Co3O4", "Co3O4", ["Co", "O"]),  # was Co14O19: percentages were cut off, not rounded
        ("Co6O8", "Co3O4", ["Co", "O"]),  # the smallest whole numbers
        ("PbTiO3", "TiPbO3", ["Ti", "Pb", "O"]),  # elements by electronegativity
        ("LiNbO3", "LiNbO3", ["Li", "Nb", "O"]),
        ("Ba(NO3)2", "BaN2O6", ["Ba", "N", "O"]),  # was Ba33(N8O25)2
        ("Fe2(SO4)3", "Fe2S3O12", ["Fe", "S", "O"]),  # was Fe10(SO4)3
        ("Ca(OH)2", "CaH2O2", ["Ca", "H", "O"]),  # was CaHO
        ("H45C45(NO3)2", "H45C45N2O6", ["H", "C", "N", "O"]),  # was H48C48O, without nitrogen
        ("Al0.7Sc0.3N", "Sc3Al7N10", ["Sc", "Al", "N"]),  # fractions become whole numbers
        ("Al7Sc3N10", "Sc3Al7N10", ["Sc", "Al", "N"]),  # the same material, the same form
        ("La0.67Sr0.33MnO3", "Sr33La67Mn100O300", ["Sr", "La", "Mn", "O"]),
        ("Pb(Zr0.52Ti0.48)O3", "Zr13Ti12Pb25O75", ["Zr", "Ti", "Pb", "O"]),
        ("(Zr0.1Ti0.2)3", "ZrTi2", ["Zr", "Ti"]),  # float noise of 0.1 * 3 is absorbed
        ("CuSO4·5H2O", "CuH10SO9", ["Cu", "H", "S", "O"]),  # hydrates are added up
        ("CuSO4*5H2O", "CuH10SO9", ["Cu", "H", "S", "O"]),
        ("Na", "Na", ["Na"]),  # sodium, not "N/A"
        ("NaCl", "NaCl", ["Na", "Cl"]),
        ("TaVCrMoW", "TaVCrMoW", ["Ta", "V", "Cr", "Mo", "W"]),
        (" Ti O2 ", "TiO2", ["Ti", "O"]),
        ("Ar", "Ar", ["Ar"]),  # no electronegativity: still accepted, and last when combined
    ],
)
def test_formulas_are_normalised_to_one_form_per_composition(typed, stored, elements):
    parsed = FormulaParser().parse(typed)

    assert parsed.normalized_formula == stored
    assert parsed.elements == elements
    assert parsed.element_key == "".join(elements)
    assert parsed.input_formula == typed


@pytest.mark.parametrize("typed", ["", "   ", "N/A", "n/a", "nan", "NaN"])
def test_a_missing_composition_is_refused(typed):
    with pytest.raises(ValueError, match="Composition is required"):
        FormulaParser().parse(typed)


@pytest.mark.parametrize("typed, message", [("XyZ", "Xy is not a chemical element"), ("Xx", "Xx is not a chemical element"), ("5", "invalid formula"), ("PbTiO3·", "invalid formula"), ("Ti-2O", "")])
def test_invalid_formulas_are_refused(typed, message):
    with pytest.raises(ValueError, match=message):
        FormulaParser().parse(typed)
