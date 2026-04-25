"""16_nlcd.py -- NLCD impervious surface % and tree canopy cover %.

NLCD CONUS rasters (~30 m, 16+ GB CONUS-wide) are the source of truth, but
they're impractical to auto-download for a hackathon. We support three modes:

1. Local raster mode: drop the LA-clipped GeoTIFFs into:
       data/raw/nlcd/impervious_la.tif   (USGS NLCD impervious surface)
       data/raw/nlcd/canopy_la.tif       (USGS NLCD tree canopy)
   We sample each cell centroid (and optionally average over the cell hex).

   Get them by:
     - https://www.mrlc.gov/data  ->  NLCD 2021 Percent Impervious + Tree Canopy
     - Clip to LA bbox in QGIS/gdalwarp:
         gdalwarp -te -119 33.6 -117.5 34.9 -t_srs EPSG:4326 in.tif out.tif

2. Synthetic mode: plausible canopy / impervious from cell centroid.
   Inland low-density: low impervious, more canopy.
   Urban core (downtown LA): very high impervious, very low canopy.
   Foothills: high canopy, low impervious.

3. Hybrid: real for whichever raster exists, synthetic for the other.

Output: data/processed/nlcd.parquet
        columns: cell_id, impervious_pct, tree_canopy_pct
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from pipelines.real.io import ensure_path_setup, has, load_gdf, log, save_df
from pipelines.real.config import RAW_DIR

ensure_path_setup()

NLCD_DIR = RAW_DIR / "nlcd"
NLCD_DIR.mkdir(parents=True, exist_ok=True)
IMPERVIOUS_TIF = NLCD_DIR / "impervious_la.tif"
CANOPY_TIF = NLCD_DIR / "canopy_la.tif"


def _sample_raster(path: Path, cells: pd.DataFrame) -> np.ndarray:
    """Return per-cell raster value (nearest pixel at centroid). NaN if outside."""
    import rasterio
    from rasterio.warp import transform as rio_transform

    with rasterio.open(path) as src:
        crs = src.crs
        nodata = src.nodata
        arr = src.read(1)
        transform = src.transform
        h, w = arr.shape

    lats = cells["centroid_lat"].to_numpy()
    lons = cells["centroid_lon"].to_numpy()
    if crs and not crs.is_geographic:
        xs, ys = rio_transform("EPSG:4326", crs, lons.tolist(), lats.tolist())
        xs = np.array(xs); ys = np.array(ys)
    else:
        xs, ys = lons, lats
    inv = ~transform
    cols, rows = inv * (xs, ys)
    rows = rows.astype(int); cols = cols.astype(int)
    valid = (rows >= 0) & (rows < h) & (cols >= 0) & (cols < w)

    out = np.full(len(cells), np.nan, dtype="float32")
    out[valid] = arr[rows[valid], cols[valid]].astype("float32")
    if nodata is not None:
        out[out == nodata] = np.nan
    return out


def _synthetic_landcover(cells: pd.DataFrame) -> pd.DataFrame:
    log("synthetic NLCD mode (drop GeoTIFFs into data/raw/nlcd/ for real values)")
    rng = np.random.default_rng(11)
    lat = cells["centroid_lat"].to_numpy()
    lon = cells["centroid_lon"].to_numpy()

    # Distance from LA downtown (34.05, -118.25) -> proxy for urbanization.
    d_dt = np.hypot(lat - 34.05, lon + 118.25)
    urban = np.clip(1 - d_dt * 4, 0, 1)               # 1 near DTLA -> 0 in foothills

    impervious = np.clip(
        85 * urban + 20 + rng.normal(0, 8, size=len(cells)),
        0, 100,
    )
    canopy = np.clip(
        45 * (1 - urban) + 8 + rng.normal(0, 6, size=len(cells))
        + 25 * np.maximum(0, lat - 34.15),            # more canopy in foothills
        0, 90,
    )
    return pd.DataFrame({
        "cell_id": cells["cell_id"].values,
        "impervious_pct": impervious.astype("float32"),
        "tree_canopy_pct": canopy.astype("float32"),
    })


def main(force: bool = False) -> None:
    if has("nlcd") and not force:
        log("nlcd.parquet exists; use --force to rebuild")
        return

    cells = load_gdf("cells")[["cell_id", "centroid_lat", "centroid_lon"]]

    have_imp = IMPERVIOUS_TIF.exists()
    have_can = CANOPY_TIF.exists()
    if have_imp or have_can:
        log(f"NLCD rasters: impervious={have_imp}, canopy={have_can}")
        synthetic = _synthetic_landcover(cells)
        out = pd.DataFrame({"cell_id": cells["cell_id"].values})
        try:
            out["impervious_pct"] = (
                _sample_raster(IMPERVIOUS_TIF, cells) if have_imp
                else synthetic["impervious_pct"].values
            )
        except Exception as e:
            log(f"impervious sampling failed ({e!r}); using synthetic")
            out["impervious_pct"] = synthetic["impervious_pct"].values
        try:
            out["tree_canopy_pct"] = (
                _sample_raster(CANOPY_TIF, cells) if have_can
                else synthetic["tree_canopy_pct"].values
            )
        except Exception as e:
            log(f"canopy sampling failed ({e!r}); using synthetic")
            out["tree_canopy_pct"] = synthetic["tree_canopy_pct"].values
        # NaNs from edge cells: backfill from synthetic.
        for col in ("impervious_pct", "tree_canopy_pct"):
            mask = pd.isna(out[col])
            if mask.any():
                out.loc[mask, col] = synthetic.loc[mask.values, col].values
    else:
        out = _synthetic_landcover(cells)

    save_df(out, "nlcd")
    log(
        f"impervious: mean={out['impervious_pct'].mean():.1f}%, "
        f"canopy: mean={out['tree_canopy_pct'].mean():.1f}%"
    )


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--force", action="store_true")
    args = p.parse_args()
    main(force=args.force)
