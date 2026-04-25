"""00_grid.py -- Build the canonical spatial grid.

Pipeline:
1. Download CA tract shapefile (TIGER/Line) and filter to LA County (FIPS 06037).
2. Dissolve tracts to derive the LA County boundary.
3. Polyfill the boundary with H3 cells at config.H3_RES.
4. For each cell, compute polygon geometry + centroid, and assign tract_fips
   via centroid-in-polygon spatial join.

Outputs:
    data/processed/tracts.parquet
    data/processed/cells.parquet
"""
from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd
import h3
import pandas as pd
from shapely.geometry import Polygon, mapping, shape
from shapely.ops import unary_union
from tqdm import tqdm

from pipelines.real.io import (
    download, ensure_path_setup, has, load_gdf, log, save_gdf, unzip,
)
from pipelines.real.config import (
    H3_RES, LA_COUNTY_FIPS, LA_STATE_FIPS, RAW_DIR,
)

ensure_path_setup()

TIGER_TRACTS_URL = (
    "https://www2.census.gov/geo/tiger/TIGER2023/TRACT/"
    f"tl_2023_{LA_STATE_FIPS}_tract.zip"
)


# ---------------------------------------------------------------------------
# Step 1: tracts
# ---------------------------------------------------------------------------
def fetch_tracts() -> gpd.GeoDataFrame:
    """Download CA tracts and filter to LA County."""
    zip_path = download(TIGER_TRACTS_URL, RAW_DIR / "tl_2023_06_tract.zip")
    extracted = RAW_DIR / "tl_2023_06_tract"
    if not (extracted / "tl_2023_06_tract.shp").exists():
        unzip(zip_path, extracted)

    log("reading CA tracts shapefile")
    gdf = gpd.read_file(extracted / "tl_2023_06_tract.shp").to_crs(4326)
    la = gdf[gdf["COUNTYFP"] == LA_COUNTY_FIPS[2:]].copy()  # county part of FIPS

    # Schema-friendly column names matching db/schema.sql:
    out = la.rename(columns={
        "GEOID": "tract_fips",
        "STATEFP": "state_fips",
        "COUNTYFP": "county_fips",
        "NAMELSAD": "name",
        "ALAND": "aland",
        "AWATER": "awater",
    })[["tract_fips", "state_fips", "county_fips", "name", "aland", "awater", "geometry"]]

    log(f"LA County has {len(out):,} tracts")
    return out


# ---------------------------------------------------------------------------
# Step 2 + 3: build H3 grid from county boundary
# ---------------------------------------------------------------------------
def _shapely_to_h3_polys(geom):
    """Convert a (Multi)Polygon to a list of h3.LatLngPoly objects.

    h3-py v4 wants (lat, lng)-ordered coordinates; shapely gives (x, y) = (lon, lat).
    """
    polys = list(geom.geoms) if geom.geom_type == "MultiPolygon" else [geom]
    out = []
    for p in polys:
        ext = [(y, x) for x, y in p.exterior.coords]
        holes = [[(y, x) for x, y in r.coords] for r in p.interiors]
        out.append(h3.LatLngPoly(ext, *holes))
    return out


def polyfill(boundary_geom, res: int) -> set[str]:
    """Return the set of H3 cell indices covering boundary_geom."""
    log(f"polyfilling boundary at H3 resolution {res}")
    cells: set[str] = set()
    for poly in _shapely_to_h3_polys(boundary_geom):
        cells.update(h3.polygon_to_cells(poly, res))
    log(f"polyfill -> {len(cells):,} cells")
    return cells


def build_cells(tracts: gpd.GeoDataFrame, res: int) -> gpd.GeoDataFrame:
    """Polyfill the dissolved LA boundary with H3 cells, attach geom + tract."""
    boundary = unary_union(tracts.geometry.values)
    cell_ids = sorted(polyfill(boundary, res))

    # Build geometries in batches to keep memory bounded.
    log("materializing cell polygons + centroids")
    rows = []
    for cid in tqdm(cell_ids, unit="cells"):
        # h3 v4: cell_to_boundary returns list of (lat, lng) tuples
        boundary_pts = h3.cell_to_boundary(cid)
        poly = Polygon([(lng, lat) for lat, lng in boundary_pts])
        clat, clng = h3.cell_to_latlng(cid)
        rows.append((cid, clat, clng, poly))

    cells = gpd.GeoDataFrame(
        rows, columns=["cell_id", "centroid_lat", "centroid_lon", "geometry"],
        crs=4326,
    )

    # Assign tract_fips by centroid-in-polygon (faster + unambiguous than
    # using the full hex polygon, which can straddle two tracts).
    log("assigning tract_fips via centroid spatial join")
    centroids = gpd.GeoDataFrame(
        cells[["cell_id"]].copy(),
        geometry=gpd.points_from_xy(cells.centroid_lon, cells.centroid_lat),
        crs=4326,
    )
    sj = gpd.sjoin(centroids, tracts[["tract_fips", "geometry"]],
                   how="left", predicate="within")
    cells = cells.merge(sj[["cell_id", "tract_fips"]].drop_duplicates("cell_id"),
                        on="cell_id", how="left")

    n_unmatched = cells["tract_fips"].isna().sum()
    if n_unmatched:
        log(f"WARNING: {n_unmatched:,} cells outside any tract polygon "
            f"(likely water/coastal sliver; will use nearest tract)")
        # Fall back to nearest tract for the orphan cells.
        orphans = cells[cells["tract_fips"].isna()]
        nearest = gpd.sjoin_nearest(
            gpd.GeoDataFrame(orphans[["cell_id"]],
                             geometry=gpd.points_from_xy(orphans.centroid_lon,
                                                         orphans.centroid_lat),
                             crs=4326),
            tracts[["tract_fips", "geometry"]],
            how="left",
        )[["cell_id", "tract_fips"]].drop_duplicates("cell_id")
        cells = cells.set_index("cell_id")
        cells.loc[nearest["cell_id"], "tract_fips"] = nearest["tract_fips"].values
        cells = cells.reset_index()

    return cells[["cell_id", "centroid_lat", "centroid_lon", "tract_fips", "geometry"]]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main(force: bool = False) -> None:
    if has("cells") and has("tracts") and not force:
        log("cells.parquet and tracts.parquet exist; use --force to rebuild")
        return

    tracts = fetch_tracts()
    save_gdf(tracts, "tracts")

    cells = build_cells(tracts, H3_RES)
    save_gdf(cells, "cells")

    log(f"done: {len(cells):,} cells across {cells['tract_fips'].nunique():,} tracts")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--force", action="store_true", help="rebuild even if outputs exist")
    args = p.parse_args()
    main(force=args.force)
