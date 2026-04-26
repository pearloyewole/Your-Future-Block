"""Compatibility helpers for the local MVP API contract.

This module bridges:
  - collaborator API shapes (window/scenario, DuckDB-backed services)
  - local MVP API shapes (year slider + scenario aliases + hazard toggles)
"""
from __future__ import annotations

from typing import Any

YEAR_TO_WINDOW = {
    2030: "2021-2040",
    2050: "2041-2060",
    2080: "2071-2090",
    2100: "2081-2100",
}

WINDOW_TO_YEAR = {v: k for k, v in YEAR_TO_WINDOW.items()}

SCENARIO_ALIASES = {
    "moderate": "ssp245",
    "high": "ssp370",
    "veryhigh": "ssp585",
    "ssp245": "ssp245",
    "ssp370": "ssp370",
    "ssp585": "ssp585",
    "ssp2-4.5": "ssp245",
    "ssp3-7.0": "ssp370",
    "ssp5-8.5": "ssp585",
}

SCENARIO_DISPLAY = {
    "ssp245": "SSP2-4.5",
    "ssp370": "SSP3-7.0",
    "ssp585": "SSP5-8.5",
}

SCENARIO_LABELS = [
    {"label": "Moderate Warming", "value": "ssp245", "display": "SSP2-4.5"},
    {"label": "High Warming", "value": "ssp370", "display": "SSP3-7.0"},
    {"label": "Very High Warming", "value": "ssp585", "display": "SSP5-8.5"},
]

HAZARD_ALIASES = {
    "combined": "overall",
    "overall": "overall",
    "heat": "heat",
    "wildfire": "wildfire",
    "flood": "flood",
}

SCORE_LABEL_BANDS = [
    {"min": 0, "max": 20, "label": "Very Low"},
    {"min": 21, "max": 40, "label": "Low"},
    {"min": 41, "max": 60, "label": "Moderate"},
    {"min": 61, "max": 80, "label": "High"},
    {"min": 81, "max": 100, "label": "Very High"},
]


def normalize_scenario(value: str | None) -> str:
    raw = (value or "ssp370").strip().lower().replace("_", "")
    resolved = SCENARIO_ALIASES.get(raw)
    if resolved:
        return resolved
    resolved = SCENARIO_ALIASES.get(raw.replace(".", "").replace("-", ""))
    if resolved:
        return resolved
    raise ValueError(
        f"Unsupported scenario '{value}'. Use SSP2-4.5, SSP3-7.0, or SSP5-8.5."
    )


def normalize_year(value: int | str | None) -> int:
    year = int(value or 2050)
    if year not in YEAR_TO_WINDOW:
        supported = ", ".join(str(y) for y in sorted(YEAR_TO_WINDOW))
        raise ValueError(f"Unsupported year '{value}'. Use one of: {supported}.")
    return year


def window_for_year(value: int | str | None) -> str:
    return YEAR_TO_WINDOW[normalize_year(value)]


def normalize_hazard(value: str | None) -> str:
    hazard = (value or "combined").strip().lower()
    resolved = HAZARD_ALIASES.get(hazard)
    if not resolved:
        raise ValueError(
            f"Unsupported hazard '{value}'. Use heat, flood, wildfire, or combined."
        )
    return resolved


def scenario_display(scenario: str) -> str:
    return SCENARIO_DISPLAY.get(scenario, scenario)


def api_config() -> dict[str, Any]:
    return {
        "years": sorted(YEAR_TO_WINDOW),
        "yearWindows": {str(y): w for y, w in YEAR_TO_WINDOW.items()},
        "scenarios": SCENARIO_LABELS,
        "scoreLabels": SCORE_LABEL_BANDS,
    }

