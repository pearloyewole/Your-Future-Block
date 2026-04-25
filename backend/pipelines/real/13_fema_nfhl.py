"""13_fema_nfhl.py -- FEMA National Flood Hazard Layer for LA County.

Output per cell:
    flood_zone   : best (most-restrictive) FEMA zone touching the cell
    in_100yr     : in a SFHA (zones A*, V*)
    in_500yr     : in a 0.2% chance zone (X500 / shaded X)

Source: FEMA NFHL county GDB. URL pattern is:
    https://hazards.fema.gov/nfhlv2/output/County/{COUNTYFIPS}C_{REVISION}.zip

LA County is FIPS 06037; the revision changes periodically. We probe a few
known suffix patterns and fall back to manual placement.

Output: data/processed/nfhl.parquet
        columns: cell_id, flood_zone, in_100yr, in_500yr
"""
from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import box

from pipelines.real.io import (
    attach_polygon_attrs_to_cells, download, ensure_path_setup,
    has, load_gdf, log, save_df, unzip,
)
from pipelines.real.config import LA_BBOX, RAW_DIR

ensure_path_setup()

NFHL_CANDIDATES = [
    # Known recent revisions; if these 404 we fall back to manual.
    "https://hazards.fema.gov/nfhlv2/output/County/06037C_20240620.zip",
    "https://hazards.fema.gov/nfhlv2/output/County/06037C_20231215.zip",
    "https://hazards.fema.gov/nfhlv2/output/County/06037C_20221207.zip",
]

# Most-restrictive first; used by max-class join.
ZONE_ORDER = [
    "VE", "V", "AE", "AO", "AH", "A", "A99",
    "X500",      # 0.2 pct annual chance / shaded X
    "X",         # minimal flood hazard
    "D",         # undetermined
    "OPEN WATER", "AREA NOT INCLUDED",
]
HUNDRED_YR = {"VE", "V", "AE", "AO", "AH", "A", "A99"}
FIVE_HUNDRED_YR = {"X500"}


def _try_download_nfhl() -> Path:
    for url in NFHL_CANDIDATES:
        dest = RAW_DIR / Path(url).name
        try:
            return download(url, dest)
        except Exception as e:
            log(f"  could not fetch {url}: {e!r}")
    manual = list(RAW_DIR.glob("06037C*.zip"))
    if manual:
        return manual[0]
    raise SystemExit(
        "No FEMA NFHL zip available. Get one for LA County (FIPS 06037) from "
        "https://msc.fema.gov/portal/advanceSearch and place in backend/data/raw/"
    )


def _load_flood_zones(zip_path: Path) -> gpd.GeoDataFrame:
    ext = RAW_DIR / zip_path.stem
    if not ext.exists():
        unzip(zip_path, ext)

    # The S_FLD_HAZ_AR (flood hazard areas) layer lives in the GDB.
    gdb = next(ext.rglob("*.gdb"), None)
    if gdb is None:
        raise SystemExit(f"No .gdb found in {ext}")
    log(f"reading S_FLD_HAZ_AR from {gdb.name}")
    g = gpd.read_file(gdb, layer="S_FLD_HAZ_AR").to_crs(4326)

    # Normalize zone column. FEMA uses FLD_ZONE; X500 is encoded as
    # FLD_ZONE='X' AND ZONE_SUBTY ILIKE '0.2%'.
    g["flood_zone"] = g["FLD_ZONE"].astype(str).str.upper().str.strip()
    if "ZONE_SUBTY" in g.columns:
        sub = g["ZONE_SUBTY"].astype(str).str.upper().fillna("")
        g.loc[(g["flood_zone"] == "X") & (sub.str.contains("0.2")), "flood_zone"] = "X500"

    minlon, minlat, maxlon, maxlat = LA_BBOX
    g = gpd.clip(g, box(minlon, minlat, maxlon, maxlat))
    return g[["flood_zone", "geometry"]]


def main(force: bool = False) -> None:
    if has("nfhl") and not force:
        log("nfhl.parquet exists; use --force to rebuild")
        return

    cells = load_gdf("cells")
    zip_path = _try_download_nfhl()
    flood = _load_flood_zones(zip_path)

    classes = attach_polygon_attrs_to_cells(
        cells, flood, ["flood_zone"],
        how="max-class", class_order=ZONE_ORDER,
        fill={"flood_zone": "X"},
    )
    classes["in_100yr"] = classes["flood_zone"].isin(HUNDRED_YR)
    classes["in_500yr"] = classes["flood_zone"].isin(FIVE_HUNDRED_YR | HUNDRED_YR)

    save_df(classes, "nfhl")
    log("zone distribution:\n" + classes["flood_zone"].value_counts().to_string())


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--force", action="store_true")
    args = p.parse_args()
    main(force=args.force)
