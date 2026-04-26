"""Risk lookup service.

Thin layer over DuckDB that turns one (cell_id, window, scenario) into a
CellRisk pydantic model. All SQL lives here so router code stays declarative.
"""
from __future__ import annotations

import json
import logging
from typing import Any

from ..db import get_db
from ..schemas import CellRisk, CellSummary

log = logging.getLogger("risklens.api.risk")


# ---------------------------------------------------------------------------
# Single cell + window + scenario
# ---------------------------------------------------------------------------
_CELL_SQL = """
SELECT c.cell_id, c.centroid_lat, c.centroid_lon, c.tract_fips,
       r.window_label, r.scenario,
       r.heat_score, r.wildfire_score, r.flood_score, r.overall_score,
       r.heat_label,  r.wildfire_label,  r.flood_label,  r.overall_label,
       r.drivers
FROM   cells c
JOIN   risk_cells r USING (cell_id)
WHERE  c.cell_id = ?
  AND  r.window_label = ?
  AND  r.scenario = ?
LIMIT 1;
"""


def fetch_cell_risk(cell_id: str, window: str, scenario: str) -> CellRisk | None:
    """Return a fully-populated CellRisk, or None if the row doesn't exist."""
    db = get_db()
    row = db.fetchone(_CELL_SQL, (cell_id, window, scenario))
    if not row:
        return None

    (cell_id, lat, lon, tract,
     win, scen,
     heat, wf, fl, overall,
     heat_lbl, wf_lbl, fl_lbl, overall_lbl,
     drivers_json) = row

    drivers: dict = {}
    if drivers_json:
        try:
            drivers = json.loads(drivers_json)
        except json.JSONDecodeError:
            log.warning("malformed drivers JSON for cell %s", cell_id)

    return CellRisk(
        cell_id=cell_id,
        centroid_lat=float(lat),
        centroid_lon=float(lon),
        tract_fips=tract,
        window=win,
        scenario=scen,
        heat_score=round(float(heat), 1),
        wildfire_score=round(float(wf), 1),
        flood_score=round(float(fl), 1),
        overall_score=round(float(overall), 1),
        heat_label=heat_lbl,
        wildfire_label=wf_lbl,
        flood_label=fl_lbl,
        overall_label=overall_lbl,
        drivers=drivers,
    )


# ---------------------------------------------------------------------------
# Bounding-box query for the map
# ---------------------------------------------------------------------------
_BBOX_SQL = """
SELECT c.cell_id, c.centroid_lat, c.centroid_lon,
       r.overall_score, r.overall_label
FROM   cells c
JOIN   risk_cells r USING (cell_id)
WHERE  r.window_label = ?
  AND  r.scenario = ?
  AND  c.centroid_lon BETWEEN ? AND ?
  AND  c.centroid_lat BETWEEN ? AND ?
LIMIT  ?;
"""

_HAZARD_COLUMNS = {
    "heat": ("heat_score", "heat_label"),
    "wildfire": ("wildfire_score", "wildfire_label"),
    "flood": ("flood_score", "flood_label"),
    "overall": ("overall_score", "overall_label"),
}

_CELL_CONTEXT_SQL = """
SELECT c.tract_fips, t.name
FROM cells c
LEFT JOIN tracts t ON t.tract_fips = c.tract_fips
WHERE c.cell_id = ?
LIMIT 1;
"""


def fetch_cells_in_bbox(
    min_lon: float, min_lat: float, max_lon: float, max_lat: float,
    window: str, scenario: str, limit: int = 5000,
) -> list[CellSummary]:
    """Lightweight viewport query for map rendering."""
    db = get_db()
    rows = db.fetchall(_BBOX_SQL, (window, scenario, min_lon, max_lon, min_lat, max_lat, limit))
    return [
        CellSummary(
            cell_id=r[0],
            centroid_lat=float(r[1]),
            centroid_lon=float(r[2]),
            overall_score=round(float(r[3]), 1),
            overall_label=r[4],
        )
        for r in rows
    ]


def fetch_cell_context(cell_id: str) -> dict[str, Any]:
    db = get_db()
    row = db.fetchone(_CELL_CONTEXT_SQL, (cell_id,))
    if not row:
        return {"tract_fips": None, "neighborhood": None}
    tract_fips, neighborhood = row
    return {
        "tract_fips": tract_fips,
        "neighborhood": neighborhood,
    }


def fetch_cells_in_bbox_for_hazard(
    min_lon: float, min_lat: float, max_lon: float, max_lat: float,
    window: str, scenario: str, hazard: str, limit: int = 5000,
) -> list[dict[str, Any]]:
    """Return map points with hazard-specific score and label."""
    if hazard not in _HAZARD_COLUMNS:
        raise ValueError(f"Unknown hazard '{hazard}'")
    score_col, label_col = _HAZARD_COLUMNS[hazard]
    sql = f"""
    SELECT c.cell_id, c.centroid_lat, c.centroid_lon, c.tract_fips, t.name,
           r.{score_col} AS score,
           r.{label_col} AS label
    FROM   cells c
    JOIN   risk_cells r USING (cell_id)
    LEFT JOIN tracts t ON t.tract_fips = c.tract_fips
    WHERE  r.window_label = ?
      AND  r.scenario = ?
      AND  c.centroid_lon BETWEEN ? AND ?
      AND  c.centroid_lat BETWEEN ? AND ?
    LIMIT  ?;
    """
    db = get_db()
    rows = db.fetchall(
        sql,
        (window, scenario, min_lon, max_lon, min_lat, max_lat, limit),
    )
    return [
        {
            "cell_id": r[0],
            "lat": float(r[1]),
            "lon": float(r[2]),
            "tract_fips": r[3],
            "neighborhood": r[4],
            "score": round(float(r[5]), 1),
            "label": r[6],
        }
        for r in rows
    ]
