"""21_la_metro_gtfs.py -- LA Metro GTFS transit stop density per cell (stretch).

Counts the number of bus/rail stops within 400 m (~1/4 mile, ADA accessibility
default) of each cell centroid.

GTFS bundle URL changes occasionally; we try a few candidates and fall back
to any local *.zip in data/raw/gtfs/.

Output: data/processed/gtfs.parquet
        columns: cell_id, transit_stops_400m
"""
from __future__ import annotations

import argparse
import io
import zipfile
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from pipelines.real.io import download, ensure_path_setup, has, load_gdf, log, save_df
from pipelines.real.config import LA_BBOX, RAW_DIR

ensure_path_setup()

GTFS_DIR = RAW_DIR / "gtfs"
GTFS_DIR.mkdir(parents=True, exist_ok=True)

GTFS_URLS = [
    "https://gitlab.com/LACMTA/gtfs_bus/-/raw/master/gtfs_bus.zip",
    "https://gitlab.com/LACMTA/gtfs_rail/-/raw/master/gtfs_rail.zip",
]


def _try_download() -> list[Path]:
    paths: list[Path] = []
    for url in GTFS_URLS:
        dest = GTFS_DIR / Path(url).name
        try:
            paths.append(download(url, dest))
        except Exception as e:
            log(f"  could not fetch {url}: {e!r}")
    if not paths:
        paths = list(GTFS_DIR.rglob("*.zip"))
    return paths


def _stops_from_gtfs(zips: list[Path]) -> pd.DataFrame:
    """Concatenate stops.txt from each GTFS zip; dedupe by stop_id."""
    rows: list[pd.DataFrame] = []
    for zp in zips:
        try:
            with zipfile.ZipFile(zp) as z:
                with z.open("stops.txt") as f:
                    df = pd.read_csv(io.TextIOWrapper(f, encoding="utf-8"))
            df = df[["stop_id", "stop_lat", "stop_lon"]]
            df["stop_id"] = zp.stem + "::" + df["stop_id"].astype(str)
            rows.append(df)
        except Exception as e:
            log(f"  could not read stops.txt from {zp.name}: {e!r}")
    if not rows:
        return pd.DataFrame(columns=["stop_id", "stop_lat", "stop_lon"])
    stops = pd.concat(rows, ignore_index=True).drop_duplicates(["stop_lat", "stop_lon"])
    minlon, minlat, maxlon, maxlat = LA_BBOX
    stops = stops[
        (stops["stop_lon"].between(minlon, maxlon))
        & (stops["stop_lat"].between(minlat, maxlat))
    ]
    return stops


def _synthetic(cells: pd.DataFrame) -> pd.DataFrame:
    log("synthetic GTFS (drop LA Metro GTFS zips into data/raw/gtfs/ for real values)")
    rng = np.random.default_rng(21)
    lat = cells["centroid_lat"].to_numpy()
    lon = cells["centroid_lon"].to_numpy()
    d_dt = np.hypot(lat - 34.05, lon + 118.25)
    base = np.clip(20 * np.exp(-d_dt * 10), 0, None)
    counts = rng.poisson(base)
    return pd.DataFrame({
        "cell_id": cells["cell_id"].values,
        "transit_stops_400m": counts.astype("int32"),
    })


def main(force: bool = False) -> None:
    if has("gtfs") and not force:
        log("gtfs.parquet exists; use --force to rebuild")
        return

    cells = load_gdf("cells")
    zips = _try_download()
    if not zips:
        save_df(_synthetic(cells), "gtfs")
        return

    stops = _stops_from_gtfs(zips)
    if stops.empty:
        save_df(_synthetic(cells), "gtfs")
        return
    log(f"unique transit stops in LA bbox: {len(stops):,}")

    # 400 m buffer around each cell centroid -> count intersecting stops.
    cell_pts = gpd.GeoDataFrame(
        cells[["cell_id"]].copy(),
        geometry=gpd.points_from_xy(cells.centroid_lon, cells.centroid_lat),
        crs=4326,
    ).to_crs(3310)
    buffers = cell_pts.copy()
    buffers["geometry"] = buffers.buffer(400)

    stop_pts = gpd.GeoDataFrame(
        stops[["stop_id"]].copy(),
        geometry=gpd.points_from_xy(stops.stop_lon, stops.stop_lat),
        crs=4326,
    ).to_crs(3310)

    sj = gpd.sjoin(buffers, stop_pts, how="left", predicate="contains")
    counts = (sj.groupby("cell_id", as_index=False)["stop_id"].count()
                .rename(columns={"stop_id": "transit_stops_400m"}))
    out = (cells[["cell_id"]].merge(counts, on="cell_id", how="left")
                              .fillna({"transit_stops_400m": 0}))
    out["transit_stops_400m"] = out["transit_stops_400m"].astype("int32")

    save_df(out, "gtfs")
    log(f"cells with >0 nearby stops: {(out['transit_stops_400m'] > 0).sum():,}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--force", action="store_true")
    args = p.parse_args()
    main(force=args.force)
