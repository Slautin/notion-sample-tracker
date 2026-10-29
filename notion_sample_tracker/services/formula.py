from __future__ import annotations

import math
import re
import warnings
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from fractions import Fraction

from notion_sample_tracker.periodictable.composition import Composition
from notion_sample_tracker.periodictable.periodic_table import Element, get_el_sp


@dataclass(frozen=True)
class ParsedFormula:
    input_formula: str
    normalized_formula: str
    elements: list[str]
    element_key: str


# Separators of a hydrate or adduct, as in CuSO4·5H2O or CuSO4*5H2O.
ADDUCT_SEPARATORS = re.compile(r"[·•*]")


class FormulaParser:
    """Normalises a chemical formula to one form per composition.

    Brackets and hydrate parts are expanded, the amounts become the smallest whole numbers in the same ratio, and
    elements are written in order of electronegativity, as the bundled periodic-table library writes formulas:
    PbTiO3 -> TiPbO3, Ba(NO3)2 -> BaN2O6, Co6O8 -> Co3O4, Al0.7Sc0.3N -> Sc3Al7N10, CuSO4·5H2O -> CuH10SO9.
    """

    def __init__(self, decimal_places: int = 6):
        # Amounts are read to this many decimal places: enough for any typed amount, and it absorbs the float
        # noise of multiplying out brackets (0.1 * 3 = 0.30000000000000004).
        self.decimal_places = decimal_places

    def parse(self, formula: str) -> ParsedFormula:
        cleaned = self._clean(formula)
        amounts = self._whole_numbers(self._amounts(cleaned))
        elements = sorted(amounts, key=_electronegativity_order)
        normalized = "".join(symbol + (str(amounts[symbol]) if amounts[symbol] != 1 else "") for symbol in elements)
        return ParsedFormula(
            input_formula=formula,
            normalized_formula=normalized,
            elements=elements,
            element_key="".join(elements),
        )

    @staticmethod
    def _clean(value: str) -> str:
        cleaned = re.sub(r"\s+", "", str(value or "")).replace("|", "")
        # "Na" is sodium: only these spellings mean that no composition was given.
        if not cleaned or cleaned.casefold() in {"nan", "n/a"}:
            raise ValueError("Composition is required")
        return cleaned

    def _amounts(self, formula: str) -> dict[str, Fraction]:
        """The exact amount of each element, adding up the parts of a hydrate or adduct."""
        totals: dict[str, Fraction] = {}
        for part in ADDUCT_SEPARATORS.split(formula):
            match = re.fullmatch(r"(\d+(?:\.\d+)?)?([A-Z(\[].*)", part)
            if not match:
                raise ValueError(f"{formula} is an invalid formula!")
            multiplier = self._exact(match.group(1) or "1")
            for symbol, amount in Composition(match.group(2)).get_el_amt_dict().items():
                # The library also accepts placeholder "dummy" species such as Xy; a sample has real elements only.
                if not Element.is_valid_symbol(symbol):
                    raise ValueError(f"{symbol} is not a chemical element (in {formula}).")
                totals[symbol] = totals.get(symbol, Fraction(0)) + multiplier * self._exact(amount)
        present = {symbol: amount for symbol, amount in totals.items() if amount > 0}
        if not present:
            raise ValueError(f"{formula} is an invalid formula!")
        return present

    def _exact(self, value: object) -> Fraction:
        try:
            return Fraction(Decimal(str(value)).quantize(Decimal(1).scaleb(-self.decimal_places)))
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(f"{value} is not a valid amount") from exc

    @staticmethod
    def _whole_numbers(amounts: dict[str, Fraction]) -> dict[str, int]:
        """The smallest whole numbers in the same ratio: Al0.7Sc0.3N -> 7, 3, 10 and Co6O8 -> 3, 4."""
        scale = math.lcm(*(amount.denominator for amount in amounts.values()))
        whole = {symbol: int(amount * scale) for symbol, amount in amounts.items()}
        divisor = math.gcd(*whole.values())
        return {symbol: count // divisor for symbol, count in whole.items()}


def _electronegativity_order(symbol: str) -> tuple[float, str]:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")  # the library warns for elements without a value
        electronegativity = get_el_sp(symbol).X
    # Elements without a value (some noble gases) go last; equal values are ordered by symbol.
    return (electronegativity if not math.isnan(electronegativity) else math.inf, symbol)
