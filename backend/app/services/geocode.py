"""Address geocoding via the U.S. Census Geocoder.

Free, no API key needed for basic usage. Returns lat/lon plus tract FIPS
when the address is in the U.S. We use it both for typed addresses and
the autocomplete that the UI may add later.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import httpx

from ..settings import settings

log = logging.getLogger("risklens.api.geocode")

ONELINE_URL = (
    "{base}/locations/onelineaddress"
    "?address={addr}&benchmark=Public_AR_Current&format=json"
)
GEO_URL = (
    "{base}/geographies/onelineaddress"
    "?address={addr}&benchmark=Public_AR_Current&vintage=Current_Current&format=json"
)


@dataclass
class GeocodeResult:
    address: str
    lat: float
    lon: float
    tract_fips: str | None
    matched_address: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "address":         self.address,
            "lat":             self.lat,
            "lon":             self.lon,
            "tract_fips":      self.tract_fips,
            "matched_address": self.matched_address,
        }


async def geocode_address(address: str) -> GeocodeResult | None:
    """Resolve a one-line U.S. address. Returns None if no match."""
    base = settings.census_geocoder_base.rstrip("/")
    # We hit the geographies endpoint so we get tract FIPS in the same call.
    url = GEO_URL.format(base=base, addr=httpx.QueryParams({"_": address})["_"])
    # httpx encodes the param above; build the URL manually so the raw `address=`
    # query string is exactly what the Census endpoint wants.
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
        return None

    matches = (data.get("result") or {}).get("addressMatches") or []
    if not matches:
        return None
    m = matches[0]
    coords = m.get("coordinates") or {}
    lon = coords.get("x")
    lat = coords.get("y")
    if lon is None or lat is None:
        return None

    tract_fips: str | None = None
    geos = (m.get("geographies") or {}).get("Census Tracts") or []
    if geos:
        # GEOID is the 11-digit state+county+tract code, exactly what we key on.
        tract_fips = geos[0].get("GEOID")

    return GeocodeResult(
        address=address,
        lat=float(lat),
        lon=float(lon),
        tract_fips=tract_fips,
        matched_address=m.get("matchedAddress"),
    )
