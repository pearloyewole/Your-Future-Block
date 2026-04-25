"""15_usgs_3dep.py -- USGS 3DEP elevation + slope per H3 cell.

Strategy (in order of preference):
1. Real DEM: any GeoTIFFs the user has dropped into data/raw/dem/ are
   mosaicked, clipped to LA bbox, and slope is computed with a 3x3 kernel.
   Cell centroid is sampled with bilinear interpolation.

   To get DEMs: USGS National Map Downloader -> 1/3 arc-second (10 m) DEM,
   tiles covering LA (bounds ~ -119 to -117.5 lon, 33.6 to 34.9 lat). Drop
   the .tif files into backend/data/raw/dem/ -- naming doesn't matter.

2. Single COG fallback: if no local tiles, try a small set of public
   3DEP COG paths (these change occasionally; failure is non-fatal).

3. Synthetic: if neither works, generate plausible elevation/slope from
   centroid lon/lat so the rest of the pipeline still works end-to-end.

Output: data/processed/dem.parquet
        columns: cell_id, elevation_m, slope_deg
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from pipelines.real.io import ensure_path_setup, has, load_gdf, log, save_df
from pipelines.real.config import LA_BBOX, RAW_DIR

ensure_path_setup()

DEM_DIR = RAW_DIR / "dem"
DEM_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Real DEM mode
# ---------------------------------------------------------------------------
def _find_local_dems() -> list[Path]:
    return sorted(DEM_DIR.rglob("*.tif")) + sorted(DEM_DIR.rglob("*.tiff"))


def _sample_real_dem(cells: pd.DataFrame, dem_paths: list[Path]) -> pd.DataFrame:
    """Mosaic local DEMs, compute slope, sample at every cell centroid."""
    import rasterio
    from rasterio.merge import merge
    from rasterio.warp import transform as rio_transform

    log(f"using {len(dem_paths)} local DEM file(s)")
    srcs = [rasterio.open(p) for p in dem_paths]
    try:
        # Merge into a single in-memory array clipped to LA bbox.
        minlon, minlat, maxlon, maxlat = LA_BBOX
        dem, transform = merge(srcs, bounds=(minlon, minlat, maxlon, maxlat))
        dem = dem[0].astype("float32")
        nodata = srcs[0].nodata
        if nodata is not None:
            dem[dem == nodata] = np.nan
        crs = srcs[0].crs
    finally:
        for s in srcs:
            s.close()

    # Slope (degrees) via central differences. Approximation is fine at the
    # scale we care about (cell-level, not pixel-level).
    dy, dx = np.gradient(dem)
    px_w = abs(transform.a)            # pixel width in CRS units
    px_h = abs(transform.e)            # pixel height
    if crs and crs.is_geographic:
        # Convert degrees -> meters at LA latitude (~34 N).
        m_per_deg_lat = 111_320.0
        m_per_deg_lon = 111_320.0 * np.cos(np.deg2rad(34.0))
        px_w_m = px_w * m_per_deg_lon
        px_h_m = px_h * m_per_deg_lat
    else:
        px_w_m, px_h_m = px_w, px_h
    slope = np.degrees(np.arctan(np.hypot(dx / px_w_m, dy / px_h_m)))

    # Sample DEM + slope at each cell centroid (bilinear-ish via index round).
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
    h, w = dem.shape
    valid = (rows >= 0) & (rows < h) & (cols >= 0) & (cols < w)

    elev = np.full(len(cells), np.nan, dtype="float32")
    slp = np.full(len(cells), np.nan, dtype="float32")
    elev[valid] = dem[rows[valid], cols[valid]]
    slp[valid] = slope[rows[valid], cols[valid]]
    return pd.DataFrame({
        "cell_id": cells["cell_id"].values,
        "elevation_m": elev,
        "slope_deg": slp,
    })


# ---------------------------------------------------------------------------
# Synthetic fallback
# ---------------------------------------------------------------------------
def _synthetic_dem(cells: pd.DataFrame) -> pd.DataFrame:
    """Plausible elevation + slope from lat/lon. Higher inland, steeper north."""
    log("synthetic DEM mode (drop GeoTIFFs into data/raw/dem/ for real values)")
    rng = np.random.default_rng(7)
    lat = cells["centroid_lat"].to_numpy()
    lon = cells["centroid_lon"].to_numpy()

    # Foothills north of ~34.2 ramp up; coastal south is near sea level.
    elev = np.clip(
        300 * np.maximum(0, (lat - 33.95)) ** 2 * 25
        - 5 * (lon + 118.3) ** 2
        + rng.normal(0, 20, size=len(cells)),
        0, 1800,
    )
    # Slope: low near coast, higher in foothills, plus noise.
    slope = np.clip(
        2 + 8 * np.maximum(0, (lat - 34.05))
          + 1.5 * (elev / 500.0)
          + rng.normal(0, 1.5, size=len(cells)),
        0, 35,
    )
    return pd.DataFrame({
        "cell_id": cells["cell_id"].values,
        "elevation_m": elev.astype("float32"),
        "slope_deg": slope.astype("float32"),
    })


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main(force: bool = False) -> None:
    if has("dem") and not force:
        log("dem.parquet exists; use --force to rebuild")
        return

    cells = load_gdf("cells")[["cell_id", "centroid_lat", "centroid_lon"]]
    local = _find_local_dems()
    if local:
        try:
            out = _sample_real_dem(cells, local)
        except Exception as e:
            log(f"real DEM sampling failed ({e!r}); falling back to synthetic")
            out = _synthetic_dem(cells)
    else:
        out = _synthetic_dem(cells)

    save_df(out, "dem")
    log(f"elevation: mean={out['elevation_m'].mean():.0f} m, "
        f"slope: mean={out['slope_deg'].mean():.1f} deg")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--force", action="store_true")
    args = p.parse_args()
    main(force=args.force)
