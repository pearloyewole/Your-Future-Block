"""Direct cell access.

  GET /cells/{cell_id}?window=...&scenario=...     full risk row for one cell
  GET /cells?min_lon=&min_lat=&max_lon=&max_lat=&window=&scenario=
                                                    lightweight viewport query

Used by the map: a single click hits /cells/{id}, panning hits /cells (bbox).
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query

from ..schemas import CellRisk, CellsBBoxResponse
from ..services.risk import fetch_cell_risk, fetch_cells_in_bbox

log = logging.getLogger("risklens.api.cells")

router = APIRouter(prefix="/cells", tags=["cells"])


@router.get("/{cell_id}", response_model=CellRisk)
def get_cell(
    cell_id: str,
    window: str = Query("2041-2060", description="Climate window label"),
    scenario: str = Query("ssp370", description="Emissions scenario id"),
) -> CellRisk:
    risk = fetch_cell_risk(cell_id, window, scenario)
    if risk is None:
        raise HTTPException(
            status_code=404,
            detail=f"No risk row for cell={cell_id} window={window} scenario={scenario}",
        )
    return risk


@router.get("", response_model=CellsBBoxResponse)
def list_cells_in_bbox(
    min_lon: float = Query(...),
    min_lat: float = Query(...),
    max_lon: float = Query(...),
    max_lat: float = Query(...),
    window: str = Query("2041-2060"),
    scenario: str = Query("ssp370"),
    limit: int = Query(5000, ge=1, le=20000),
) -> CellsBBoxResponse:
    if max_lon <= min_lon or max_lat <= min_lat:
        raise HTTPException(status_code=400, detail="bbox must have max > min on both axes")
    rows = fetch_cells_in_bbox(min_lon, min_lat, max_lon, max_lat,
                               window, scenario, limit=limit)
    return CellsBBoxResponse(window=window, scenario=scenario, count=len(rows), cells=rows)
