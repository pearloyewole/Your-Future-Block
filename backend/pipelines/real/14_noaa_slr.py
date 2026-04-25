"""14_noaa_slr.py -- NOAA Sea Level Rise inundation.

For each cell, find the LOWEST SLR scenario (in feet, 0..10) that inundates
it. Cells never inundated within 10 ft get NULL.

NOAA distributes per-county shapefiles for each 1-foot SLR step. The naming
convention is e.g. 'CA_SouthCoast_slr_1ft.shp', shipped in zip files at
    https://coast.noaa.gov/slrdata/Depth_Rasters/CA/...

For LA we want the 'CA_SouthCoast' or 'CA_LA' bundle. To avoid hard-coding
brittle URLs, we look for any shapefile in data/raw/noaa_slr/ matching the
pattern '*slr*[0-9]*ft*.shp' and use the digits as the foot threshold.

Manual download: https://coast.noaa.gov/slrdata/

Output: data/processed/slr.parquet
        columns: cell_id, slr_inundated_ft (DOUBLE; NaN if not inundated <= 10ft)
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import box

from pipelines.real.io import (
    attach_polygon_attrs_to_cells, ensure_path_setup,
    has, load_gdf, log, save_df,
)
from pipelines.real.config import LA_BBOX, RAW_DIR

ensure_path_setup()

SLR_DIR = RAW_DIR / "noaa_slr"
SLR_PATTERN = re.compile(r"slr[_-]?(\d+)\s*ft", re.IGNORECASE)


def _discover_slr_shapefiles() -> dict[int, list[Path]]:
    """Group available SLR inundation shapefiles by foot threshold."""
    SLR_DIR.mkdir(parents=True, exist_ok=True)
    by_ft: dict[int, list[Path]] = {}
    for shp in SLR_DIR.rglob("*.shp"):
        m = SLR_PATTERN.search(shp.name)
        if m:
            ft = int(m.group(1))
            by_ft.setdefault(ft, []).append(shp)
    return dict(sorted(by_ft.items()))


def _load_slr_polys(by_ft: dict[int, list[Path]]) -> gpd.GeoDataFrame:
    """One unified GeoDataFrame with a column slr_ft per polygon."""
    rows: list[gpd.GeoDataFrame] = []
    minlon, minlat, maxlon, maxlat = LA_BBOX
    bbox = box(minlon, minlat, maxlon, maxlat)
    for ft, shps in by_ft.items():
        for shp in shps:
            log(f"reading {shp.relative_to(RAW_DIR)} (ft={ft})")
            g = gpd.read_file(shp).to_crs(4326)
            g = gpd.clip(g, bbox)
            if g.empty:
                continue
            g = g[["geometry"]].assign(slr_ft=ft)
            rows.append(g)
    if not rows:
        return gpd.GeoDataFrame(columns=["slr_ft", "geometry"], crs=4326)
    return pd.concat(rows, ignore_index=True)


def main(force: bool = False) -> None:
    if has("slr") and not force:
        log("slr.parquet exists; use --force to rebuild")
        return

    cells = load_gdf("cells")
    by_ft = _discover_slr_shapefiles()

    if not by_ft:
        log("WARNING: no NOAA SLR shapefiles found in backend/data/raw/noaa_slr/")
        log("Download per-foot inundation polygons for LA from "
            "https://coast.noaa.gov/slrdata/ and unzip into that directory.")
        log("Continuing with empty SLR layer (all cells = not inundated).")
        out = pd.DataFrame({
            "cell_id": cells["cell_id"],
            "slr_inundated_ft": pd.NA,
        })
        save_df(out, "slr")
        return

    polys = gpd.GeoDataFrame(_load_slr_polys(by_ft), crs=4326)
    log(f"SLR polygons (LA bbox): {len(polys):,}")

    # We want the LOWEST ft threshold that touches each cell. Spatial-join
    # all polygons, then take min(slr_ft) per cell_id.
    sj = gpd.sjoin(cells[["cell_id", "geometry"]],
                   polys[["slr_ft", "geometry"]],
                   how="left", predicate="intersects")
    out = (sj.groupby("cell_id", as_index=False)["slr_ft"]
             .min().rename(columns={"slr_ft": "slr_inundated_ft"}))
    out = (cells[["cell_id"]].merge(out, on="cell_id", how="left"))

    save_df(out, "slr")
    n_in = out["slr_inundated_ft"].notna().sum()
    log(f"cells inundated within 10 ft: {n_in:,} / {len(out):,}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--force", action="store_true")
    args = p.parse_args()
    main(force=args.force)
