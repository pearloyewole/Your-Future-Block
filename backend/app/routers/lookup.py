"""Address (or lat/lon) -> risk lookup.

Two entry points so the UI can use whichever it has:
  POST /lookup        body: {address, window, scenario}
  POST /lookup_point  body: {lat, lon, window, scenario}

Both return a LookupResponse. The endpoint is forgiving: out-of-region
or geocoder-down requests still return 200 with `note` set, instead of
500-ing -- the UI can render a graceful "we don't cover that address yet"
message.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter

from ..db import get_db
from ..schemas import LookupRequest, LookupResponse, PointLookupRequest
from ..services.explain import explain_risk
from ..services.geocode import geocode_address
from ..services.risk import fetch_cell_risk

log = logging.getLogger("risklens.api.lookup")

router = APIRouter(tags=["lookup"])


def _enrich_with_risk(
    response: LookupResponse,
    lat: float,
    lon: float,
    window: str,
    scenario: str,
    do_explain: bool,
) -> LookupResponse:
    """Common path: lat/lon -> cell -> risk -> (optional) explanation."""
    db = get_db()
    cell_id = db.cell_for_point(lat, lon)
    if cell_id is None:
        response.note = "Coordinate falls outside the served region."
        return response

    response.cell_id = cell_id
    risk = fetch_cell_risk(cell_id, window, scenario)
    if risk is None:
        response.note = (
            f"No risk row for cell {cell_id} at window={window} scenario={scenario}. "
            "The pipeline may not have produced this combination."
        )
        return response

    response.risk = risk
    if do_explain:
        try:
            response.explanation = explain_risk(risk)
        except Exception as e:
            log.warning("explain failed: %r", e)
            response.note = "explanation generation failed; risk numbers are still valid"
    return response


@router.post("/lookup", response_model=LookupResponse)
async def lookup_address(req: LookupRequest) -> LookupResponse:
    """Geocode the address, then return the matching block's risk row."""
    geo = await geocode_address(req.address)
    if geo is None:
        return LookupResponse(
            address=req.address,
            note="Address could not be geocoded (Census Geocoder returned no match).",
        )

    response = LookupResponse(
        address=req.address,
        matched_address=geo.matched_address,
        lat=geo.lat,
        lon=geo.lon,
        tract_fips=geo.tract_fips,
    )
    return _enrich_with_risk(
        response, geo.lat, geo.lon, req.window, req.scenario, req.explain
    )


@router.post("/lookup_point", response_model=LookupResponse)
def lookup_point(req: PointLookupRequest) -> LookupResponse:
    """Skip geocoding -- caller already has lat/lon (e.g. clicked the map)."""
    response = LookupResponse(
        address=f"({req.lat:.5f}, {req.lon:.5f})",
        lat=req.lat,
        lon=req.lon,
    )
    return _enrich_with_risk(
        response, req.lat, req.lon, req.window, req.scenario, req.explain
    )
