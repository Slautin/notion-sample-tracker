from __future__ import annotations

from dataclasses import dataclass

from notion_sample_tracker.periodictable.composition import Composition


@dataclass(frozen=True)
class ParsedFormula:
    input_formula: str
    normalized_formula: str
    elements: list[str]
    element_key: str


class FormulaParser:
    def __init__(self, significant_figures: int = 3):
        self.significant_figures = significant_figures
        self.multiplier = 10.0 ** (significant_figures - 1)

    def parse(self, formula: str) -> ParsedFormula:
        cleaned = self._clean(formula)
        normalized = self._pretty_formula(cleaned)
        composition = Composition(normalized)
        elements = [element.symbol for element in composition.elements]
        return ParsedFormula(
            input_formula=formula,
            normalized_formula=normalized,
            elements=elements,
            element_key="".join(elements),
        )

    @staticmethod
    def _clean(value: str) -> str:
        cleaned = str(value or "").strip().replace("|", "")
        if not cleaned or cleaned.upper() in {"NAN", "NA", "N/A"}:
            raise ValueError("Composition is required")
        return cleaned

    @staticmethod
    def _extract_parentheses(value: str) -> list[str]:
        matches: list[str] = []
        start = 0
        end = 0
        while True:
            relative_start = value[start:].find("(")
            relative_end = value[end:].find(")")
            if relative_start == -1 or relative_end == -1:
                break
            matches.append(value[start + relative_start + 1 : end + relative_end])
            start += 1 + relative_start
            end += 1 + relative_end
        return matches

    def _fractional_formula(self, formula: str) -> str:
        composition = Composition(formula)
        parts: list[str] = []
        for element in composition.elements:
            fraction = round(composition.get_atomic_fraction(element), self.significant_figures)
            parts.append(f"{element.symbol}{fraction}")
        return "".join(parts)

    def _normalize_formula(self, formula: str) -> str:
        matches = self._extract_parentheses(formula)
        if not matches:
            return self._fractional_formula(formula)
        normalized = formula
        for match in matches:
            normalized = normalized.replace(match, self._fractional_formula(match))
        return normalized

    def _pretty_formula(self, formula: str) -> str:
        composition = Composition(self._normalize_formula(formula))
        percent_formula = []
        for element in composition.elements:
            percent = int(composition.get_atomic_fraction(element) * self.multiplier)
            percent_formula.append(f"{element.symbol}{percent}")
        return Composition("".join(percent_formula)).reduced_formula
