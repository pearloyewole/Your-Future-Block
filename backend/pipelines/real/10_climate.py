"""10_climate.py -- Real per-cell climate projection metrics from CMIP6.

This script is part of the REAL pipeline. It has no synthetic fallback by
design: if you want a deterministic, offline build, run
`python -m pipelines.synthetic.build` instead, which is a completely
separate code path producing the same DuckDB schema.

Two real sources are supported, switched via env var CLIMATE_SOURCE or
the --source flag:

    nex-gddp   (default) NASA NEX-GDDP-CMIP6 on anonymous S3.
                         ~25 km global. Stable, well-documented S3 paths.
    loca2                Cal-Adapt LOCA2-Hybrid on AWS (~3 km California).
                         STUB: requires verifying current Cal-Adapt zarr paths
                         against https://analytics.cal-adapt.org/data/.

For each (cell, window, scenario) combination we compute:
    heat_days       mean annual count of days where tasmax > 35 C
    warm_nights     mean annual count of days where tasmin > 20 C
    pr_annual_mm    mean annual precipitation total (mm)
    pr_p99_mm       99th percentile of daily precipitation (mm)
    cdd_max         mean annual longest run of consecutive dry (<1 mm) days

Output: data/processed/climate.parquet
        columns: cell_id, window, scenario,
                 heat_days, warm_nights, pr_annual_mm, pr_p99_mm, cdd_max
"""
from __future__ import annotations

import argparse
import random

import numpy as np
import pandas as pd
import xarray as xr

from pipelines.real.io import ensure_path_setup, has, load_gdf, log, save_df
from pipelines.real.config import (
    BASELINE_WINDOW, CLIMATE_SOURCE, CLIMATE_VARS, CLIMATE_WINDOWS,
    FUTURE_SCENARIOS, HEAT_DAY_THRESHOLD_K, HISTORICAL_SCENARIO,
    LA_BBOX, NEX_GDDP_BUCKET, NEX_GDDP_GRID, NEX_GDDP_MEMBER,
    NEX_GDDP_MODELS, SCENARIOS, WARM_NIGHT_THRESHOLD_K,
    YEARS_SAMPLED_PER_WINDOW,
)

ensure_path_setup()


# ===========================================================================
# Source 1: NEX-GDDP-CMIP6 from anonymous S3 (default)
# ===========================================================================
def _nex_gddp_url(model: str, scenario: str, var: str, year: int) -> str:
    """S3 URL for one (model, scenario, var, year) NetCDF file.

    Anonymous bucket: s3://nex-gddp-cmip6/NEX-GDDP-CMIP6/...
    """
    return (
        f"s3://{NEX_GDDP_BUCKET}/NEX-GDDP-CMIP6/{model}/{scenario}/"
        f"{NEX_GDDP_MEMBER}/{var}/"
        f"{var}_day_{model}_{scenario}_{NEX_GDDP_MEMBER}_{NEX_GDDP_GRID}_{year}.nc"
    )


def _years_for_window(window: str) -> list[int]:
    """Sample years from a window. With YEARS_SAMPLED_PER_WINDOW=None, full window."""
    lo, hi = CLIMATE_WINDOWS[window]
    full = list(range(lo, hi + 1))
    if YEARS_SAMPLED_PER_WINDOW is None or YEARS_SAMPLED_PER_WINDOW >= len(full):
        return full
    rng = random.Random(f"{window}-sample")
    return sorted(rng.sample(full, YEARS_SAMPLED_PER_WINDOW))


def _open_nex_var(model: str, scenario: str, var: str, years: list[int]) -> xr.Dataset:
    """Open daily files for `years`, clip to LA bbox, concat along time.

    Uses anonymous s3fs so no AWS credentials required. We slice to LA_BBOX
    BEFORE calling .load() so each year's working set is ~1 MB instead of
    the full ~250 MB CONUS-scale file.
    """
    import s3fs  # local import: only needed in nex-gddp mode
    fs = s3fs.S3FileSystem(anon=True)
    paths = [_nex_gddp_url(model, scenario, var, y) for y in years]
    log(f"  opening {len(paths)} files for {model}/{scenario}/{var}")

    minlon, minlat, maxlon, maxlat = LA_BBOX
    ds_list: list[xr.Dataset] = []
    for p in paths:
        try:
            with fs.open(p) as fh:
                ds = _open_dataset_with_fallbacks(fh)
                # NEX-GDDP uses 0-360 longitudes; normalize before slicing.
                if float(ds.lon.min()) >= 0:
                    ds = ds.assign_coords(
                        lon=(((ds.lon + 180) % 360) - 180)
                    ).sortby("lon")
                ds = ds.sel(
                    lat=slice(minlat, maxlat),
                    lon=slice(minlon, maxlon),
                ).load()
            ds_list.append(ds)
        except FileNotFoundError:
            log(f"  WARNING: missing {p}")
            continue
    if not ds_list:
        raise RuntimeError(f"No NEX-GDDP files found for {model}/{scenario}/{var}")
    return xr.concat(ds_list, dim="time")


def _open_dataset_with_fallbacks(fh) -> xr.Dataset:
    """Try multiple xarray engines because environment packaging can vary."""
    errors: list[str] = []
    for engine in ("h5netcdf", "netcdf4", None):
        try:
            if engine is None:
                return xr.open_dataset(fh)
            return xr.open_dataset(fh, engine=engine)
        except Exception as e:
            errors.append(f"{engine or 'auto'}={e!r}")
            try:
                fh.seek(0)
            except Exception:
                pass
    raise RuntimeError("could not open NetCDF with available engines: " + "; ".join(errors))


def _compute_metrics(tasmax: xr.DataArray | None,
                     tasmin: xr.DataArray | None,
                     pr: xr.DataArray | None) -> xr.Dataset:
    """Reduce a daily time-series stack into per-pixel climatology metrics."""
    out_vars: dict[str, xr.DataArray] = {}

    if tasmax is not None:
        years_n = float(np.unique(tasmax["time.year"].values).size)
        heat_days_per_year = (tasmax > HEAT_DAY_THRESHOLD_K).sum("time") / years_n
        out_vars["heat_days"] = heat_days_per_year

    if tasmin is not None:
        years_n = float(np.unique(tasmin["time.year"].values).size)
        warm_nights = (tasmin > WARM_NIGHT_THRESHOLD_K).sum("time") / years_n
        out_vars["warm_nights"] = warm_nights

    if pr is not None:
        # NEX-GDDP precipitation is kg m-2 s-1 -> mm/day
        pr_mm = pr * 86400.0
        years_n = float(np.unique(pr_mm["time.year"].values).size)
        out_vars["pr_annual_mm"] = pr_mm.sum("time") / years_n
        out_vars["pr_p99_mm"] = pr_mm.quantile(0.99, dim="time").drop_vars("quantile")

        # cdd_max: mean across years of the longest consecutive dry-days run.
        # "Dry" defined as < 1 mm/day. Done per-year for stability, then averaged.
        def _max_run(arr: np.ndarray) -> int:
            best = run = 0
            for x in arr:
                run = run + 1 if x else 0
                if run > best:
                    best = run
            return best
        years = pr_mm.groupby("time.year")
        max_runs = years.reduce(
            lambda block, axis: np.apply_along_axis(_max_run, axis, block),
            dim="time",
        )
        out_vars["cdd_max"] = max_runs.mean("year")

    return xr.Dataset(out_vars)


def nex_gddp_climate(cells: pd.DataFrame) -> pd.DataFrame:
    """Build the climate.parquet rows from NEX-GDDP-CMIP6 on S3."""
    log(f"NEX-GDDP-CMIP6: {len(NEX_GDDP_MODELS)} models, "
        f"{len(SCENARIOS)} scenarios, {len(CLIMATE_WINDOWS)} windows")

    cell_ids = cells["cell_id"].to_numpy()
    cell_lats = xr.DataArray(cells["centroid_lat"].to_numpy(), dims="cell")
    cell_lons = xr.DataArray(cells["centroid_lon"].to_numpy(), dims="cell")

    rows: list[tuple] = []

    for window, (lo, hi) in CLIMATE_WINDOWS.items():
        # historical scenario only spans up to 2014; use only baseline window for it
        scenarios_here = (
            [HISTORICAL_SCENARIO] if window == BASELINE_WINDOW else FUTURE_SCENARIOS
        )
        for scen in scenarios_here:
            log(f"window={window} scenario={scen}")
            years = _years_for_window(window)
            per_model_metrics: list[xr.Dataset] = []
            for model in NEX_GDDP_MODELS:
                try:
                    var_arrays: dict[str, xr.DataArray] = {}
                    for var in CLIMATE_VARS:
                        ds = _open_nex_var(model, scen, var, years)
                        var_arrays[var] = ds[var]
                    metrics = _compute_metrics(
                        var_arrays.get("tasmax"),
                        var_arrays.get("tasmin"),
                        var_arrays.get("pr"),
                    )
                    per_model_metrics.append(metrics)
                except Exception as e:
                    log(f"  skip {model}: {e!r}")
                    continue
            if not per_model_metrics:
                log(f"  no models succeeded for {window}/{scen}; skipping")
                continue

            ensemble = xr.concat(per_model_metrics, dim="model").mean(
                "model", skipna=True
            )

            # nearest-neighbour sample at each cell centroid.
            sampled = ensemble.sel(
                lat=cell_lats, lon=cell_lons, method="nearest"
            ).load()

            n = len(cell_ids)
            for i in range(n):
                rows.append((
                    cell_ids[i], window, scen,
                    float(sampled["heat_days"].values[i]) if "heat_days" in sampled else np.nan,
                    float(sampled["warm_nights"].values[i]) if "warm_nights" in sampled else np.nan,
                    float(sampled["pr_annual_mm"].values[i]) if "pr_annual_mm" in sampled else np.nan,
                    float(sampled["pr_p99_mm"].values[i]) if "pr_p99_mm" in sampled else np.nan,
                    float(sampled["cdd_max"].values[i]) if "cdd_max" in sampled else np.nan,
                ))

    return pd.DataFrame(rows, columns=[
        "cell_id", "window", "scenario",
        "heat_days", "warm_nights", "pr_annual_mm", "pr_p99_mm", "cdd_max",
    ])


# ===========================================================================
# Source 2: LOCA2-Hybrid (Cal-Adapt) -- STUB
# ===========================================================================
def loca2_climate(cells: pd.DataFrame) -> pd.DataFrame:
    """Cal-Adapt LOCA2-Hybrid downscaled CMIP6 (~3 km, California-only).

    Wired up but not yet validated: Cal-Adapt's S3 zarr layout has changed
    historically. To enable, set the CADCAT_* constants in config.py and
    confirm against the live catalog at https://analytics.cal-adapt.org/data/.
    """
    raise NotImplementedError(
        "LOCA2-Hybrid loader not implemented; verify Cal-Adapt zarr paths "
        "and adapt nex_gddp_climate(). Use --source nex-gddp."
    )


# ===========================================================================
# Entry point
# ===========================================================================
def main(force: bool = False, source: str | None = None) -> None:
    if has("climate") and not force:
        log("climate.parquet exists; use --force to rebuild")
        return

    cells = load_gdf("cells")[["cell_id", "centroid_lat", "centroid_lon"]]
    src = source or CLIMATE_SOURCE
    log(f"climate source = {src}")

    if src == "nex-gddp":
        df = nex_gddp_climate(cells)
    elif src == "loca2":
        df = loca2_climate(cells)
    else:
        raise SystemExit(
            f"unknown --source {src!r}. Real pipeline supports 'nex-gddp' or 'loca2'. "
            f"For an offline build use `python -m pipelines.synthetic.build`."
        )

    save_df(df, "climate")
    log(f"climate rows: {len(df):,}  "
        f"({df['scenario'].nunique()} scenarios x {df['window'].nunique()} windows "
        f"x {df['cell_id'].nunique():,} cells)")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--force", action="store_true")
    p.add_argument("--source", choices=["nex-gddp", "loca2"],
                   help="override CLIMATE_SOURCE env var")
    args = p.parse_args()
    main(force=args.force, source=args.source)
