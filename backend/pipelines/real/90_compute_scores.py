"""90_compute_scores.py -- Real pipeline's final ETL step.

Joins all per-layer parquets produced by 11..21 into one wide cell_attrs
table, then delegates scoring and DuckDB loading to the shared modules so
both pipelines produce identical schemas.

Idempotent: blow away the real DuckDB and re-run.
"""
from __future__ import annotations

import argparse

import pandas as pd

from pipelines.real.io import ensure_path_setup, load_df, load_gdf, log
from pipelines.real.config import DUCKDB_PATH

from shared.duckdb_loader import load_into_duckdb
from shared.scoring import compute_scores, load_weights

ensure_path_setup()


# ---------------------------------------------------------------------------
# Per-layer join (pipeline-specific: knows which parquets exist)
# ---------------------------------------------------------------------------
def build_cell_attrs() -> pd.DataFrame:
    """Wide table: one row per cell, all static modifiers + tract-level joined."""
    cells = load_gdf("cells")[["cell_id", "centroid_lat", "centroid_lon", "tract_fips"]]

    # Per-cell layers (no tract_fips needed):
    fhsz  = load_df("fhsz")    # cell_id, fhsz_class, dist_to_fhsz_vh_m
    wui   = load_df("wui")     # cell_id, wui_class
    nfhl  = load_df("nfhl")    # cell_id, flood_zone, in_100yr, in_500yr
    slr   = load_df("slr")     # cell_id, slr_inundated_ft
    dem   = load_df("dem")     # cell_id, elevation_m, slope_deg
    nlcd  = load_df("nlcd")    # cell_id, impervious_pct, tree_canopy_pct
    lodes = load_df("lodes")   # cell_id, daytime_workers
    gtfs  = load_df("gtfs")    # cell_id, transit_stops_400m

    # Per-tract layers (joined via tract_fips):
    svi = load_df("svi")       # tract_fips, svi_overall
    acs = load_df("acs")       # tract_fips, pct_age_65plus, pct_below_poverty, pct_no_vehicle
    nri = load_df("nri")       # tract_fips, nri_*_eal, community_resilience

    out = (
        cells
        .merge(fhsz,  on="cell_id", how="left")
        .merge(wui,   on="cell_id", how="left")
        .merge(nfhl,  on="cell_id", how="left")
        .merge(slr,   on="cell_id", how="left")
        .merge(dem,   on="cell_id", how="left")
        .merge(nlcd,  on="cell_id", how="left")
        .merge(lodes, on="cell_id", how="left")
        .merge(gtfs,  on="cell_id", how="left")
        .merge(svi,   on="tract_fips", how="left")
        .merge(acs,   on="tract_fips", how="left")
        .merge(nri,   on="tract_fips", how="left")
    )

    # Defaults for any layer that wasn't joined.
    out["fhsz_class"] = out["fhsz_class"].fillna("None")
    out["wui_class"]  = out["wui_class"].fillna("None")
    out["flood_zone"] = out["flood_zone"].fillna("X")
    out["in_100yr"]   = out["in_100yr"].fillna(False).astype(bool)
    out["in_500yr"]   = out["in_500yr"].fillna(False).astype(bool)

    log(f"cell_attrs: {len(out):,} rows, {len(out.columns)} columns")
    return out


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main(force: bool = False) -> None:
    weights = load_weights()

    log("loading per-layer parquets")
    attrs = build_cell_attrs()
    climate = load_df("climate")

    log("computing scores")
    risk = compute_scores(attrs, climate, weights)
    log(f"risk rows: {len(risk):,}  "
        f"({risk['scenario'].nunique()} scenarios x "
        f"{risk['window'].nunique()} windows x "
        f"{risk['cell_id'].nunique():,} cells)")
    log(f"  heat:     mean={risk['heat_score'].mean():.1f}, max={risk['heat_score'].max():.1f}")
    log(f"  wildfire: mean={risk['wildfire_score'].mean():.1f}, max={risk['wildfire_score'].max():.1f}")
    log(f"  flood:    mean={risk['flood_score'].mean():.1f}, max={risk['flood_score'].max():.1f}")
    log(f"  overall:  mean={risk['overall_score'].mean():.1f}, max={risk['overall_score'].max():.1f}")

    log(f"loading into {DUCKDB_PATH}")
    cells_gdf  = load_gdf("cells")
    tracts_gdf = load_gdf("tracts")
    load_into_duckdb(
        DUCKDB_PATH,
        pipeline="real",
        cells=cells_gdf,
        tracts=tracts_gdf,
        cell_attrs=attrs,
        cell_climate=climate,
        risk_cells=risk,
        notes="real-mode build (CMIP6 + CAL FIRE + FEMA + CDC SVI + ...)",
    )
    log("done.")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--force", action="store_true",
                   help="rebuild risk_cells even if DB already populated")
    args = p.parse_args()
    main(force=args.force)
