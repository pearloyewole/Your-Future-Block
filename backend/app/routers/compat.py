"""Local MVP API compatibility routes.

These endpoints mirror the Node prototype contract so local frontend logic can
run directly against the collaborator FastAPI backend.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from ..db import get_db
from ..services.compat import (
    api_config,
    normalize_hazard,
    normalize_scenario,
    normalize_year,
    scenario_display,
    window_for_year,
)
from ..services.explain import explain_risk
from ..services.geocode import GeocodeResult, geocode_address
from ..services.risk import (
    fetch_cell_context,
    fetch_cell_risk,
    fetch_cells_in_bbox_for_hazard,
)
from ..settings import settings

router = APIRouter(prefix="/api", tags=["compat"])


class GeocodeBody(BaseModel):
    address: str


class RiskBody(BaseModel):
    lat: float
    lon: float
    year: int = 2050
    scenario: str = "ssp370"
    explain: bool = True


class GeocodeRiskBody(BaseModel):
    address: str
    year: int = 2050
    scenario: str = "ssp370"
    explain: bool = True


@router.get("/health")
def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "service": "risklens-api-compat",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/config")
def config() -> dict[str, Any]:
    return api_config()


@router.post("/geocode")
async def geocode(req: GeocodeBody) -> dict[str, Any]:
    geo = await geocode_address(req.address)
    if geo is None:
        raise HTTPException(
            status_code=404,
            detail=(
                "Address could not be geocoded. Try a full LA-area address, "
                "for example: 200 N Spring St, Los Angeles, CA"
            ),
        )
    return _shape_geocode(req.address, geo)


@router.post("/risk")
def risk(req: RiskBody) -> dict[str, Any]:
    return _risk_for_point(
        lon=req.lon,
        lat=req.lat,
        year=req.year,
        scenario=req.scenario,
        explain=req.explain,
    )


@router.post("/geocode-risk")
async def geocode_risk(req: GeocodeRiskBody) -> dict[str, Any]:
    geo = await geocode_address(req.address)
    if geo is None:
        raise HTTPException(
            status_code=404,
            detail=(
                "Address could not be geocoded. Try a full LA-area address, "
                "for example: 200 N Spring St, Los Angeles, CA"
            ),
        )
    payload = _risk_for_point(
        lon=geo.lon,
        lat=geo.lat,
        year=req.year,
        scenario=req.scenario,
        explain=req.explain,
    )
    return {"geocoded": _shape_geocode(req.address, geo), "risk": payload}


@router.get("/map-cells")
def map_cells(
    year: int = Query(2050),
    scenario: str = Query("ssp370"),
    hazard: str = Query("combined"),
    min_lon: float | None = Query(None),
    min_lat: float | None = Query(None),
    max_lon: float | None = Query(None),
    max_lat: float | None = Query(None),
    limit: int = Query(10000, ge=1, le=50000),
) -> dict[str, Any]:
    normalized_year = normalize_year(year)
    window = window_for_year(normalized_year)
    normalized_scenario = normalize_scenario(scenario)
    normalized_hazard = normalize_hazard(hazard)

    if None in (min_lon, min_lat, max_lon, max_lat):
        min_lon, min_lat, max_lon, max_lat = settings.bbox

    rows = fetch_cells_in_bbox_for_hazard(
        min_lon=min_lon,
        min_lat=min_lat,
        max_lon=max_lon,
        max_lat=max_lat,
        window=window,
        scenario=normalized_scenario,
        hazard=normalized_hazard,
        limit=limit,
    )
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [r["lon"], r["lat"]]},
                "properties": {
                    "cell_id": r["cell_id"],
                    "neighborhood": r["neighborhood"],
                    "tract_fips": r["tract_fips"],
                    "hazard": normalized_hazard,
                    "score": r["score"],
                    "label": r["label"],
                },
            }
            for r in rows
        ],
    }


def _risk_for_point(
    *,
    lon: float,
    lat: float,
    year: int,
    scenario: str,
    explain: bool,
) -> dict[str, Any]:
    normalized_year = normalize_year(year)
    window = window_for_year(normalized_year)
    normalized_scenario = normalize_scenario(scenario)

    db = get_db()
    cell_id = db.cell_for_point(lat, lon)
    if cell_id is None:
        raise HTTPException(
            status_code=404,
            detail="Coordinate falls outside the served region.",
        )

    risk = fetch_cell_risk(cell_id, window, normalized_scenario)
    if risk is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No risk row for cell={cell_id} window={window} "
                f"scenario={normalized_scenario}."
            ),
        )

    context = fetch_cell_context(cell_id)
    drivers = risk.drivers or {}
    vulnerability = _as_pct(drivers.get("svi_overall"))
    resilience = _as_pct(drivers.get("community_resilience"))

    payload = {
        "year": normalized_year,
        "year_window": window,
        "scenario": normalized_scenario,
        "scenario_display": scenario_display(normalized_scenario),
        "cell_id": risk.cell_id,
        "neighborhood": context["neighborhood"] or context["tract_fips"] or "LA Area",
        "tract_fips": context["tract_fips"],
        "coordinates": {"lat": lat, "lon": lon},
        "modifiers": {
            "tree_canopy_pct": _as_float(drivers.get("tree_canopy_pct")),
            "impervious_pct": _as_float(drivers.get("impervious_pct")),
            "social_vulnerability": vulnerability,
            "resilience_idx": resilience,
        },
        "scores": {
            "heat": {"score": risk.heat_score, "label": risk.heat_label},
            "wildfire": {"score": risk.wildfire_score, "label": risk.wildfire_label},
            "flood": {"score": risk.flood_score, "label": risk.flood_label},
            "overall": {"score": risk.overall_score, "label": risk.overall_label},
        },
    }
    if explain:
        payload["explanation"] = explain_risk(risk)
    else:
        payload["explanation"] = None
    return payload


def _shape_geocode(input_address: str, geo: GeocodeResult) -> dict[str, Any]:
    return {
        "source": geo.source,
        "input_address": input_address,
        "matched_address": geo.matched_address,
        "lat": geo.lat,
        "lon": geo.lon,
        "tract_fips": geo.tract_fips,
        "county_fips": geo.county_fips,
        "state_fips": geo.state_fips,
    }


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return round(float(value), 1)
    except (TypeError, ValueError):
        return None


def _as_pct(value: Any) -> float | None:
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if 0 <= f <= 1:
        f = f * 100
    return round(f, 1)
