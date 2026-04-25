"""11_calfire_fhsz.py -- CAL FIRE Fire Hazard Severity Zones (FHSZ).

For each H3 cell, assign the highest FHSZ class that intersects it. Also
compute the distance (m, EPSG:3310) to the nearest Very High zone, used as
a proximity modifier in the wildfire score.

CAL FIRE publishes the statewide FHSZ as a shapefile via the California State
Geoportal / OSFM. The dataset has been re-issued multiple times; we accept
either the SRA or the LRA file -- whatever the user puts in data/raw/ -- so
this script tolerates URL drift.

Manual download fallback (if auto-download fails):
    https://osfm.fire.ca.gov/what-we-do/community-wildfire-preparedness-and-mitigation/fire-hazard-severity-zones
    Save the shapefile zip to: backend/data/raw/calfire_fhsz.zip

Output: data/processed/fhsz.parquet
        columns: cell_id, fhsz_class, dist_to_fhsz_vh_m
"""
from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from pipelines.real.io import (
    attach_polygon_attrs_to_cells, download, ensure_path_setup,
    has, load_gdf, log, save_df, unzip,
)
from pipelines.real.config import LA_BBOX, RAW_DIR

ensure_path_setup()

# Try this first; fall back to manual placement if it 404s (URLs drift).
FHSZ_URLS = [
    "https://osfm.fire.ca.gov/media/k0bnpgam/fhszsra22_1.zip",
    "https://osfm.fire.ca.gov/media/cppffuzd/fhszlra24_1.zip",
]

# Order matters: max-class join uses this ranking.
FHSZ_CLASS_ORDER = ["Very High", "High", "Moderate", "None"]


def _try_download_fhsz() -> list[Path]:
    """Attempt to download CAL FIRE FHSZ zips. Return list of local zip paths."""
    paths: list[Path] = []
    for url in FHSZ_URLS:
        dest = RAW_DIR / Path(url).name
        try:
            paths.append(download(url, dest))
        except Exception as e:
            log(f"  could not fetch {url}: {e!r}")
    if not paths:
        # Look for any file the user may have dropped in manually.
        manual = list(RAW_DIR.glob("*fhsz*.zip")) + list(RAW_DIR.glob("FHSZ*.zip"))
        if manual:
            paths.extend(manual)
    if not paths:
        raise SystemExit(
            "No FHSZ zip available. Download manually from CAL FIRE OSFM and "
            "place in backend/data/raw/ (any filename containing 'fhsz')."
        )
    return paths


def _load_fhsz(paths: list[Path]) -> gpd.GeoDataFrame:
    """Read all FHSZ shapefiles, normalize the class column, clip to LA bbox."""
    gdfs: list[gpd.GeoDataFrame] = []
    for zp in paths:
        ext_dir = RAW_DIR / zp.stem
        if not ext_dir.exists():
            unzip(zp, ext_dir)
        for shp in ext_dir.rglob("*.shp"):
            log(f"reading {shp.relative_to(RAW_DIR)}")
            g = gpd.read_file(shp).to_crs(4326)
            class_col = next(
                (c for c in g.columns if c.upper() in {"HAZ_CLASS", "FHSZ", "CLASS",
                                                        "HAZARDCLAS", "HAZARDCL",
                                                        "HAZ_CODE", "FHSZ_7CLAS"}),
                None,
            )
            if not class_col:
                log(f"  no class column found in {shp.name}; cols={list(g.columns)}")
                continue
            # CAL FIRE shapefiles inconsistently encode hazard as either text
            # ("VERY HIGH"/"High"/"Moderate") or integer codes (1=Mod, 2=High,
            # 3=Very High). Normalize both.
            raw = g[class_col].astype(str).str.strip().str.upper()
            mapping = {
                "1": "Moderate", "2": "High", "3": "Very High",
                "MODERATE": "Moderate", "HIGH": "High", "VERY HIGH": "Very High",
                "MOD": "Moderate", "VH": "Very High",
            }
            g["fhsz_class"] = raw.map(mapping)
            g = g[g["fhsz_class"].isin(FHSZ_CLASS_ORDER[:-1])][["fhsz_class", "geometry"]]
            gdfs.append(g)

    if not gdfs:
        raise SystemExit("No usable FHSZ shapefiles found")

    fhsz = pd.concat(gdfs, ignore_index=True)
    fhsz = gpd.GeoDataFrame(fhsz, geometry="geometry", crs=4326)

    # Clip to LA bbox to keep distance computation manageable.
    minlon, minlat, maxlon, maxlat = LA_BBOX
    bbox = gpd.GeoSeries.from_xy([], []).total_bounds  # noqa: just for type
    from shapely.geometry import box
    clip = box(minlon, minlat, maxlon, maxlat)
    fhsz = gpd.clip(fhsz, clip)
    log(f"FHSZ polygons in LA bbox: {len(fhsz):,}")
    return fhsz


def _distance_to_very_high(cells: gpd.GeoDataFrame, fhsz: gpd.GeoDataFrame) -> pd.DataFrame:
    """Compute meters from each cell centroid to the nearest Very High zone."""
    vh = fhsz[fhsz["fhsz_class"] == "Very High"]
    if vh.empty:
        return pd.DataFrame({"cell_id": cells["cell_id"], "dist_to_fhsz_vh_m": np.nan})

    # EPSG:3310 (California Albers, meters) for accurate distances.
    cells_m = cells.set_geometry(
        gpd.points_from_xy(cells.centroid_lon, cells.centroid_lat), crs=4326
    ).to_crs(3310)
    vh_m = vh.to_crs(3310)

    nearest = gpd.sjoin_nearest(cells_m[["cell_id", "geometry"]],
                                vh_m[["geometry"]],
                                how="left", distance_col="dist_to_fhsz_vh_m")
    return (nearest[["cell_id", "dist_to_fhsz_vh_m"]]
            .drop_duplicates("cell_id"))


def main(force: bool = False) -> None:
    if has("fhsz") and not force:
        log("fhsz.parquet exists; use --force to rebuild")
        return

    cells = load_gdf("cells")
    paths = _try_download_fhsz()
    fhsz = _load_fhsz(paths)

    classes = attach_polygon_attrs_to_cells(
        cells, fhsz, ["fhsz_class"],
        how="max-class", class_order=FHSZ_CLASS_ORDER,
        fill={"fhsz_class": "None"},
    )
    dists = _distance_to_very_high(cells, fhsz)
    out = classes.merge(dists, on="cell_id", how="left")

    save_df(out, "fhsz")
    log("class distribution:\n" + out["fhsz_class"].value_counts().to_string())


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--force", action="store_true")
    args = p.parse_args()
    main(force=args.force)
