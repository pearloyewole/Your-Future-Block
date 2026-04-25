"""Pydantic request/response models for the RiskLens API.

Kept in one file so the UI team can read every shape the backend accepts and
returns in <30 seconds. None of these models touch DB-internal column names;
they are the public contract.
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Requests
# ---------------------------------------------------------------------------
class LookupRequest(BaseModel):
    """Address-based lookup. The default window/scenario is the demo headline."""
    address: str = Field(..., examples=["200 N Spring St, Los Angeles, CA"])
    window: str = Field("2041-2060", description="Climate window label, e.g. '2041-2060'")
    scenario: str = Field("ssp370", description="Emissions scenario id: ssp245|ssp370|ssp585")
    explain: bool = Field(True, description="If true, generate an LLM (or fallback) explanation")


class PointLookupRequest(BaseModel):
    """Direct lat/lon lookup -- useful when the user clicks the map."""
    lat: float
    lon: float
    window: str = "2041-2060"
    scenario: str = "ssp370"
    explain: bool = True


# ---------------------------------------------------------------------------
# Pieces
# ---------------------------------------------------------------------------
class CellRisk(BaseModel):
    """All hazard scores + driver snapshot for one (cell, window, scenario)."""
    cell_id: str
    centroid_lat: float
    centroid_lon: float
    tract_fips: str | None

    window: str
    scenario: str

    heat_score: float
    wildfire_score: float
    flood_score: float
    overall_score: float

    heat_label: str
    wildfire_label: str
    flood_label: str
    overall_label: str

    drivers: dict[str, Any] = Field(
        default_factory=dict,
        description="Snapshot of the raw inputs that produced these scores.",
    )


class CellSummary(BaseModel):
    """Lightweight row for map/bbox queries -- no drivers, no labels."""
    cell_id: str
    centroid_lat: float
    centroid_lon: float
    overall_score: float
    overall_label: str


# ---------------------------------------------------------------------------
# Responses
# ---------------------------------------------------------------------------
class LookupResponse(BaseModel):
    """The thing the UI renders after an address search."""
    address: str
    matched_address: str | None = None
    lat: float | None = None
    lon: float | None = None
    tract_fips: str | None = None

    cell_id: str | None = None
    risk: CellRisk | None = None
    explanation: str | None = None

    note: str | None = Field(
        None,
        description="Human-readable note when something soft-failed "
                    "(address out of region, no risk row, geocoder offline, ...).",
    )


class CellsBBoxResponse(BaseModel):
    window: str
    scenario: str
    count: int
    cells: list[CellSummary]
