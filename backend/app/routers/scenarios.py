"""Scenarios endpoint -- enumerate the (window, scenario) pairs available.

The UI uses this to populate the year-slider and scenario-selector. The
synthetic and real DBs both have the same canonical labels, so the UI
doesn't need to know which build is being served.
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter

from ..db import get_db

log = logging.getLogger("risklens.api.scenarios")

router = APIRouter(tags=["scenarios"])

# Human labels shown in the UI for each climate window. Keep in sync with
# shared/scoring.py and pipelines/real/config.py CLIMATE_WINDOWS.
WINDOW_LABEL = {
    "1981-2010": "Historical (1981-2010)",
    "2021-2040": "Near-term (~2030)",
    "2041-2060": "Mid-century (~2050)",
    "2071-2090": "Late-century (~2080)",
    "2081-2100": "End-of-century (~2100)",
}
SCENARIO_LABEL = {
    "historical": "Historical",
    "ssp245":     "Lower / Moderate (SSP2-4.5)",
    "ssp370":     "High (SSP3-7.0)",
    "ssp585":     "Very High (SSP5-8.5)",
}


@router.get("/scenarios")
def list_scenarios() -> dict[str, Any]:
    """Return windows + scenarios actually present in the served DB."""
    db = get_db()
    try:
        rows = db.fetchall(
            "SELECT DISTINCT window_label, scenario FROM risk_cells "
            "ORDER BY window_label, scenario;"
        )
    except Exception as e:
        log.warning("scenarios lookup failed: %r", e)
        rows = []

    windows: list[dict[str, str]] = []
    seen_w: set[str] = set()
    scenarios: list[dict[str, str]] = []
    seen_s: set[str] = set()
    pairs: list[tuple[str, str]] = []

    for win, scen in rows:
        pairs.append((win, scen))
        if win not in seen_w:
            seen_w.add(win)
            windows.append({"id": win, "label": WINDOW_LABEL.get(win, win)})
        if scen not in seen_s:
            seen_s.add(scen)
            scenarios.append({"id": scen, "label": SCENARIO_LABEL.get(scen, scen)})

    return {
        "windows":   windows,
        "scenarios": scenarios,
        "pairs":     [{"window": w, "scenario": s} for w, s in pairs],
    }
