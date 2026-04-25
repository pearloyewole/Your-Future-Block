"""Synthetic pipeline -- offline, deterministic, no network.

Produces `backend/data/processed/risklens.synthetic.duckdb` with the same
schema as the real pipeline so the API and UI work identically against
either DB.

Usage:
    python -m pipelines.synthetic.build
    DUCKDB_PATH=backend/data/processed/risklens.synthetic.duckdb \
        uvicorn backend.app.api:app --reload

The synthetic data is *deterministic plausible*: derived from cell
centroid lat/lon with a seeded RNG so every build produces identical
numbers. It is NOT random and NOT a stand-in for real data -- it is a
demo-safe baseline that lets the rest of the stack (scoring, API, UI,
LLM) be developed and demoed without any external downloads.

This file intentionally has zero overlap with `pipelines/real/`.
Both pipelines depend only on `shared/` (schema, scoring, DuckDB loader).
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Resolve `from shared...` without requiring `pip install -e .`
BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from shared.duckdb_loader import load_into_duckdb, reset_db  # noqa: E402
from shared.scoring import (  # noqa: E402
    BASELINE_WINDOW,
    FUTURE_SCENARIOS,
    FUTURE_WINDOWS,
    HISTORICAL_SCENARIO,
    compute_scores,
    load_weights,
)

# --------------------------------------------------------------------- config
SEED = 42
DUCKDB_PATH = Path(
    os.environ.get(
        "SYNTHETIC_DUCKDB_PATH",
        BACKEND_DIR / "data" / "processed" / "risklens.synthetic.duckdb",
    )
)

# A small synthetic LA-shaped bounding box and a low H3 resolution so the
# whole build runs in a few seconds. We don't need real H3 here -- a flat
# lat/lon grid is enough for a demo, and avoids the h3 dependency at runtime.
LA_BBOX = (-118.7, 33.7, -117.7, 34.5)   # minlon, minlat, maxlon, maxlat
GRID_NX = 60                             # ~1.6 km steps east-west
GRID_NY = 50                             # ~1.6 km steps north-south
# (60 x 50 = 3,000 cells -- tiny, demo-only. The real pipeline produces ~95K.)


# --------------------------------------------------------------------- grid
def build_cells_and_tracts() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (cells_gdf, tracts_gdf) as plain pandas with WKB-ready geometry."""
    import geopandas as gpd
    from shapely.geometry import Polygon, box

    minlon, minlat, maxlon, maxlat = LA_BBOX
    dx = (maxlon - minlon) / GRID_NX
    dy = (maxlat - minlat) / GRID_NY

    rows = []
    for i in range(GRID_NX):
        for j in range(GRID_NY):
            lon0, lat0 = minlon + i * dx, minlat + j * dy
            lon1, lat1 = lon0 + dx, lat0 + dy
            cid = f"syn_{i:03d}_{j:03d}"
            poly = box(lon0, lat0, lon1, lat1)
            rows.append((cid, (lat0 + lat1) / 2, (lon0 + lon1) / 2,
                         f"06037{i % 100:04d}00", poly))

    cells = gpd.GeoDataFrame(
        rows, columns=["cell_id", "centroid_lat", "centroid_lon", "tract_fips", "geometry"],
        crs=4326,
    )

    # One synthetic tract per cell-column. Tract polygons cover the whole
    # column from south to north so tract-level joins behave normally.
    tract_rows = []
    for i in range(GRID_NX):
        tid = f"06037{i % 100:04d}00"
        if any(r[0] == tid for r in tract_rows):
            continue
        tract_rows.append((
            tid, "06", "037", f"Synthetic Tract {i}",
            float(dx * dy * 100), 0.0,
            Polygon([
                (minlon + i * dx, minlat),
                (minlon + (i + 1) * dx, minlat),
                (minlon + (i + 1) * dx, maxlat),
                (minlon + i * dx, maxlat),
            ]),
        ))
    tracts = gpd.GeoDataFrame(
        tract_rows,
        columns=["tract_fips", "state_fips", "county_fips", "name",
                 "aland", "awater", "geometry"],
        crs=4326,
    )

    print(f"[synth] grid: {len(cells):,} cells, {len(tracts):,} tracts")
    return cells, tracts


# --------------------------------------------------------------------- attrs
def build_cell_attrs(cells: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """Per-cell modifiers: spatially smooth-ish so the map doesn't look noisy."""
    n = len(cells)
    lat = cells["centroid_lat"].to_numpy()
    lon = cells["centroid_lon"].to_numpy()

    # Distance from synthetic "downtown" (Boyle Heights-ish): higher impervious,
    # lower canopy near it. Distance from coast (south/west): coastal flood risk.
    dt_lat, dt_lon = 34.05, -118.20
    coast_lat = 33.75
    d_downtown = np.hypot(lat - dt_lat, (lon - dt_lon) * 0.85)
    d_coast    = np.maximum(0, lat - coast_lat)

    impervious = np.clip(85 - 80 * d_downtown + rng.normal(0, 4, n), 5, 95)
    canopy     = np.clip(5 + 70 * d_downtown + rng.normal(0, 4, n), 0, 80)

    # Wildfire baseline: fhsz higher in the north (mountains). WUI on the rim.
    fhsz_score = np.clip((lat - 34.0) * 220 + rng.normal(0, 10, n), 0, 100)
    fhsz_class = np.where(
        fhsz_score < 25, "None",
        np.where(fhsz_score < 50, "Moderate",
                 np.where(fhsz_score < 75, "High", "Very High"))
    )
    wui_class = np.where(
        (fhsz_score > 40) & (fhsz_score < 75), "Interface",
        np.where(fhsz_score >= 75, "Intermix", "None")
    )

    # Flood baseline: AE near coast, X otherwise.
    coastal_band = d_coast < 0.05
    flood_zone = np.where(coastal_band, "AE", "X")
    in_100yr = coastal_band
    slr_inundated_ft = np.where(d_coast < 0.02, 3.0,
                          np.where(d_coast < 0.04, 6.0, np.nan))

    # Terrain: flat near coast, steeper in north.
    elevation_m = np.clip((lat - 33.7) * 600 + rng.normal(0, 20, n), 0, 800)
    slope_deg   = np.clip((lat - 34.0) * 30 + rng.normal(0, 2, n), 0, 30)

    # Vulnerability: higher near downtown.
    svi_overall = np.clip(0.8 - 0.6 * d_downtown + rng.normal(0, 0.05, n), 0.05, 0.95)

    return pd.DataFrame({
        "cell_id":             cells["cell_id"].values,
        "elevation_m":         elevation_m,
        "slope_deg":           slope_deg,
        "impervious_pct":      impervious,
        "tree_canopy_pct":     canopy,
        "fhsz_class":          fhsz_class,
        "wui_class":           wui_class,
        "dist_to_fhsz_vh_m":   np.clip(rng.normal(2000, 800, n), 0, 5000),
        "fires_5km_30yr":      rng.integers(0, 4, size=n),
        "flood_zone":          flood_zone,
        "in_100yr":            in_100yr,
        "in_500yr":            (d_coast < 0.07),
        "slr_inundated_ft":    slr_inundated_ft,
        "dist_to_coast_m":     d_coast * 111_000.0,
        "svi_overall":         svi_overall,
        "pct_age_65plus":      np.clip(15 + rng.normal(0, 4, n), 5, 35),
        "pct_no_vehicle":      np.clip(svi_overall * 25 + rng.normal(0, 3, n), 0, 60),
        "pct_below_poverty":   np.clip(svi_overall * 30 + rng.normal(0, 4, n), 0, 60),
        "pct_disability":      np.clip(8 + rng.normal(0, 2, n), 2, 25),
        "median_income":       np.clip(85_000 - 60_000 * svi_overall + rng.normal(0, 5_000, n),
                                       25_000, 200_000),
        "nri_heat_eal":        rng.uniform(0, 5, n),
        "nri_wildfire_eal":    fhsz_score * 0.05,
        "nri_riverine_eal":    rng.uniform(0, 2, n),
        "nri_coastal_eal":     np.where(coastal_band, rng.uniform(2, 8, n), 0),
        "community_resilience": np.clip(0.5 + rng.normal(0, 0.1, n), 0, 1),
        "daytime_workers":     rng.integers(0, 500, n),
        "transit_stops_400m":  rng.integers(0, 8, n),
    })


# --------------------------------------------------------------------- climate
def build_climate(cells: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """Latitude-driven baseline + scenario/window warming factor."""
    lat = cells["centroid_lat"].to_numpy()
    lon = cells["centroid_lon"].to_numpy()
    cell_ids = cells["cell_id"].to_numpy()

    base_heat = np.clip(12 + 6 * np.sin((lat - 33.6) * 6)
                        + 3 * np.cos((lon + 118) * 4)
                        + rng.normal(0, 1, len(cells)), 2, 30)
    base_warm = np.clip(base_heat * 0.4 + rng.normal(0, 1, len(cells)), 0, 20)
    base_pr   = 350 + 80 * np.sin((lat - 33.6) * 8) + rng.normal(0, 30, len(cells))
    base_p99  = 35 + rng.normal(0, 4, len(cells))
    base_cdd  = 90 + rng.normal(0, 10, len(cells))

    scenario_factor = {"ssp245": 1.0, "ssp370": 1.6, "ssp585": 2.2}
    window_year = {
        "2021-2040": 2030, "2041-2060": 2050,
        "2071-2090": 2080, "2081-2100": 2090,
    }

    rows: list[tuple] = []
    # Historical baseline
    for cid, h, wn, pr, p99, cdd in zip(
        cell_ids, base_heat, base_warm, base_pr, base_p99, base_cdd
    ):
        rows.append((cid, BASELINE_WINDOW, HISTORICAL_SCENARIO,
                     float(h), float(wn), float(pr), float(p99), float(cdd)))

    for window in FUTURE_WINDOWS:
        t = (window_year[window] - 2010) / 100.0
        for scen in FUTURE_SCENARIOS:
            sf = scenario_factor[scen]
            heat = base_heat + 25 * t * sf + rng.normal(0, 1, len(cells))
            warm = base_warm + 18 * t * sf + rng.normal(0, 1, len(cells))
            pr_a = base_pr * (1 - 0.05 * t * sf) + rng.normal(0, 15, len(cells))
            p99  = base_p99 * (1 + 0.10 * t * sf) + rng.normal(0, 2, len(cells))
            cdd  = base_cdd * (1 + 0.08 * t * sf) + rng.normal(0, 5, len(cells))
            for cid, h, wn, pa, p9, cd in zip(cell_ids, heat, warm, pr_a, p99, cdd):
                rows.append((cid, window, scen,
                             float(h), float(wn), float(pa), float(p9), float(cd)))

    return pd.DataFrame(rows, columns=[
        "cell_id", "window", "scenario",
        "heat_days", "warm_nights", "pr_annual_mm", "pr_p99_mm", "cdd_max",
    ])


# --------------------------------------------------------------------- main
def main(reset: bool = True) -> None:
    if reset:
        reset_db(DUCKDB_PATH)

    rng = np.random.default_rng(SEED)
    weights = load_weights()

    cells, tracts = build_cells_and_tracts()
    attrs = build_cell_attrs(cells, rng)
    climate = build_climate(cells, rng)

    print(f"[synth] scoring {len(cells):,} cells x "
          f"{climate['window'].nunique()} windows x "
          f"{climate['scenario'].nunique()} scenarios")
    risk = compute_scores(attrs, climate, weights)
    print(f"[synth] risk rows: {len(risk):,}")
    print(f"[synth]   heat:     mean={risk['heat_score'].mean():.1f}, "
          f"max={risk['heat_score'].max():.1f}")
    print(f"[synth]   wildfire: mean={risk['wildfire_score'].mean():.1f}, "
          f"max={risk['wildfire_score'].max():.1f}")
    print(f"[synth]   flood:    mean={risk['flood_score'].mean():.1f}, "
          f"max={risk['flood_score'].max():.1f}")

    print(f"[synth] writing {DUCKDB_PATH}")
    load_into_duckdb(
        DUCKDB_PATH,
        pipeline="synthetic",
        cells=cells,
        tracts=tracts,
        cell_attrs=attrs,
        cell_climate=climate,
        risk_cells=risk,
        notes=f"seeded={SEED} grid={GRID_NX}x{GRID_NY} bbox={LA_BBOX}",
    )
    print("[synth] done.")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--no-reset", action="store_true",
                   help="append to existing DB instead of wiping it first")
    args = p.parse_args()
    main(reset=not args.no_reset)
