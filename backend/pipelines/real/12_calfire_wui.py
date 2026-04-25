"""12_calfire_wui.py -- California Wildland Urban Interface (WUI).

Tag each cell with the most-severe WUI class it intersects:
    Interface  > Intermix  > None

Source: California Open Data Portal "Wildland Urban Interface" dataset.
URLs change; we try a few candidates, then fall back to a manual file in
data/raw/ matching glob '*wui*.zip' or '*WUI*.zip'.

Output: data/processed/wui.parquet
        columns: cell_id, wui_class
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

WUI_URLS = [
    # data.ca.gov resource (URL has historically rotated)
    "https://data.ca.gov/dataset/wildland-urban-interface/resource/"
    "/download/wildlandurbaninterface.zip",
]

WUI_CLASS_ORDER = ["Interface", "Intermix", "None"]


def _try_download_wui() -> list[Path]:
    paths: list[Path] = []
    for url in WUI_URLS:
        dest = RAW_DIR / "calfire_wui.zip"
        try:
            paths.append(download(url, dest))
        except Exception as e:
            log(f"  could not fetch {url}: {e!r}")
    if not paths:
        manual = list(RAW_DIR.glob("*wui*.zip")) + list(RAW_DIR.glob("*WUI*.zip"))
        if manual:
            paths.extend(manual)
    if not paths:
        raise SystemExit(
            "No WUI zip available. Download from data.ca.gov 'Wildland Urban "
            "Interface' and place in backend/data/raw/ (filename containing 'wui')."
        )
    return paths


def _load_wui(paths: list[Path]) -> gpd.GeoDataFrame:
    gdfs: list[gpd.GeoDataFrame] = []
    for zp in paths:
        ext = RAW_DIR / zp.stem
        if not ext.exists():
            unzip(zp, ext)
        for shp in ext.rglob("*.shp"):
            log(f"reading {shp.relative_to(RAW_DIR)}")
            g = gpd.read_file(shp).to_crs(4326)
            class_col = next(
                (c for c in g.columns if c.upper() in {"WUICLASS10", "WUICLASS",
                                                        "WUI_CLASS", "TYPE"}),
                None,
            )
            if class_col is None:
                continue
            mapping = {
                "11": "Intermix", "12": "Intermix", "13": "Intermix",
                "21": "Interface", "22": "Interface", "23": "Interface",
                "INTERMIX": "Intermix", "INTERFACE": "Interface",
            }
            g["wui_class"] = (g[class_col].astype(str).str.upper()
                              .map(mapping).fillna("None"))
            g = g[g["wui_class"].isin(["Intermix", "Interface"])][
                ["wui_class", "geometry"]]
            gdfs.append(g)

    if not gdfs:
        raise SystemExit("No usable WUI shapefiles found")

    wui = pd.concat(gdfs, ignore_index=True)
    wui = gpd.GeoDataFrame(wui, geometry="geometry", crs=4326)
    minlon, minlat, maxlon, maxlat = LA_BBOX
    wui = gpd.clip(wui, box(minlon, minlat, maxlon, maxlat))
    log(f"WUI polygons in LA bbox: {len(wui):,}")
    return wui


def main(force: bool = False) -> None:
    if has("wui") and not force:
        log("wui.parquet exists; use --force to rebuild")
        return

    cells = load_gdf("cells")
    paths = _try_download_wui()
    wui = _load_wui(paths)

    out = attach_polygon_attrs_to_cells(
        cells, wui, ["wui_class"],
        how="max-class", class_order=WUI_CLASS_ORDER,
        fill={"wui_class": "None"},
    )

    save_df(out, "wui")
    log("class distribution:\n" + out["wui_class"].value_counts().to_string())


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--force", action="store_true")
    args = p.parse_args()
    main(force=args.force)
