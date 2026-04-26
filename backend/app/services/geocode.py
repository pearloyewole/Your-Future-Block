"""Address geocoding via the U.S. Census Geocoder.

Free, no API key needed for basic usage. Returns lat/lon plus tract FIPS
when the address is in the U.S. We use it both for typed addresses and
the autocomplete that the UI may add later.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

import httpx

from ..settings import REPO_ROOT, settings

log = logging.getLogger("risklens.api.geocode")


@dataclass
class GeocodeResult:
    address: str
    lat: float
    lon: float
    tract_fips: str | None
    matched_address: str | None
    source: str = "us_census_geocoder"
    county_fips: str | None = None
    state_fips: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "address":         self.address,
            "lat":             self.lat,
            "lon":             self.lon,
            "tract_fips":      self.tract_fips,
            "matched_address": self.matched_address,
            "source":          self.source,
            "county_fips":     self.county_fips,
            "state_fips":      self.state_fips,
        }


async def geocode_address(address: str) -> GeocodeResult | None:
    """Resolve a one-line U.S. address with local fallback for demo resilience."""
    base = settings.census_geocoder_base.rstrip("/")
    url = f"{base}/geographies/onelineaddress"
    params = {
        "address":   address,
        "benchmark": "Public_AR_Current",
        "vintage":   "Current_Current",
        "format":    "json",
    }
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            r = await client.get(url, params=params)
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        log.warning("Census geocoder failed for %r: %r", address, e)
        return _fallback_match(address)

    matches = (data.get("result") or {}).get("addressMatches") or []
    if not matches:
        return _fallback_match(address)
    m = matches[0]
    coords = m.get("coordinates") or {}
    lon = coords.get("x")
    lat = coords.get("y")
    if lon is None or lat is None:
        return _fallback_match(address)

    tract_fips: str | None = None
    county_fips: str | None = None
    state_fips: str | None = None
    geos = (m.get("geographies") or {}).get("Census Tracts") or []
    if geos:
        # GEOID is the 11-digit state+county+tract code, exactly what we key on.
        tract_fips = geos[0].get("GEOID")
        county_fips = geos[0].get("COUNTY")
        state_fips = geos[0].get("STATE")

    return GeocodeResult(
        address=address,
        lat=float(lat),
        lon=float(lon),
        tract_fips=tract_fips,
        matched_address=m.get("matchedAddress"),
        source="us_census_geocoder",
        county_fips=county_fips,
        state_fips=state_fips,
    )


@lru_cache(maxsize=1)
def _load_fallback_rows() -> list[dict[str, Any]]:
    path = REPO_ROOT / "backend/app/data/address_fallbacks.json"
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text())
    except Exception as e:
        log.warning("Could not parse fallback addresses from %s: %r", path, e)
        return []


def _fallback_match(address: str) -> GeocodeResult | None:
    rows = _load_fallback_rows()
    if not rows:
        return None

    needle = _normalize(address)
    exact = next((r for r in rows if _normalize(str(r.get("address", ""))) == needle), None)
    match = exact
    if match is None:
        match = next(
            (
                r
                for r in rows
                if needle in _normalize(str(r.get("address", "")))
                or _normalize(str(r.get("address", ""))) in needle
            ),
            None,
        )
    if match is None:
        return None

    tract = str(match.get("tract_fips") or "") or None
    return GeocodeResult(
        address=address,
        lat=float(match["lat"]),
        lon=float(match["lon"]),
        tract_fips=tract,
        matched_address=str(match.get("display_name") or match.get("address") or address),
        source="local_fallback",
        county_fips=tract[2:5] if tract and len(tract) >= 5 else None,
        state_fips=tract[0:2] if tract and len(tract) >= 2 else None,
    )


def _normalize(value: str) -> str:
    return " ".join(value.lower().strip().split())
