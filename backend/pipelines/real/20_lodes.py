"""20_lodes.py -- LEHD LODES daytime worker counts (stretch).

Pulls California Workplace Area Characteristics (WAC) at the Census BLOCK
level, sums total jobs per cell via spatial-block lookup (block centroid in
H3 cell), and writes per-cell daytime_workers.

LODES URL pattern (year may need bumping over time):
    https://lehd.ces.census.gov/data/lodes/LODES8/ca/wac/ca_wac_S000_JT00_2021.csv.gz

If the download fails (or h3 conversion of every block in CA is too heavy),
we generate plausible synthetic worker counts so downstream scoring still
works.

Output: data/processed/lodes.parquet
        columns: cell_id, daytime_workers
"""
from __future__ import annotations

import argparse
import gzip
from pathlib import Path

import h3
import numpy as np
import pandas as pd

from pipelines.real.io import download, ensure_path_setup, has, load_gdf, log, save_df
from pipelines.real.config import H3_RES, LA_COUNTY_FIPS, RAW_DIR

ensure_path_setup()

LODES_URLS = [
    "https://lehd.ces.census.gov/data/lodes/LODES8/ca/wac/ca_wac_S000_JT00_2021.csv.gz",
    "https://lehd.ces.census.gov/data/lodes/LODES8/ca/wac/ca_wac_S000_JT00_2020.csv.gz",
]

# LODES block GEOID is 15 digits: state(2) + county(3) + tract(6) + block(4).
# We don't have block centroids -- to keep this hackathon-light we use a
# tract-level distribution and split jobs evenly across cells in the tract.


def _try_download() -> Path | None:
    for url in LODES_URLS:
        dest = RAW_DIR / Path(url).name
        try:
            return download(url, dest)
        except Exception as e:
            log(f"  could not fetch {url}: {e!r}")
    manual = list(RAW_DIR.glob("ca_wac*.csv.gz"))
    return manual[0] if manual else None


def _synthetic(cells: pd.DataFrame) -> pd.DataFrame:
    log("synthetic LODES (drop ca_wac CSV.gz into data/raw/ for real values)")
    rng = np.random.default_rng(20)
    lat = cells["centroid_lat"].to_numpy()
    lon = cells["centroid_lon"].to_numpy()
    d_dt = np.hypot(lat - 34.05, lon + 118.25)
    base = np.clip(800 * np.exp(-d_dt * 12), 0, None)
    workers = rng.poisson(base + 5)
    return pd.DataFrame({
        "cell_id": cells["cell_id"].values,
        "daytime_workers": workers.astype("int32"),
    })


def main(force: bool = False) -> None:
    if has("lodes") and not force:
        log("lodes.parquet exists; use --force to rebuild")
        return

    cells = load_gdf("cells")
    src = _try_download()
    if src is None:
        save_df(_synthetic(cells), "lodes")
        return

    log(f"reading {src.name}")
    with gzip.open(src, "rt") as f:
        wac = pd.read_csv(f, dtype={"w_geocode": str}, usecols=["w_geocode", "C000"])
    # Filter to LA County by GEOID prefix (state+county = 06037).
    la_prefix = LA_COUNTY_FIPS  # 06037
    wac = wac[wac["w_geocode"].str.startswith(la_prefix)].copy()
    wac["tract_fips"] = wac["w_geocode"].str[:11]
    by_tract = wac.groupby("tract_fips", as_index=False)["C000"].sum().rename(
        columns={"C000": "tract_workers"}
    )

    # Distribute tract worker totals across H3 cells in that tract, weighted
    # equally. This is a simplification; with block centroids we could weight
    # by block centroid -> H3 polyfill, but tract-uniform is a reasonable
    # starting point for "where are workers concentrated?"
    cells_per_tract = (cells.groupby("tract_fips")["cell_id"].count()
                            .rename("n_cells").reset_index())
    merged = (cells[["cell_id", "tract_fips"]]
              .merge(by_tract, on="tract_fips", how="left")
              .merge(cells_per_tract, on="tract_fips", how="left"))
    merged["daytime_workers"] = (
        merged["tract_workers"] / merged["n_cells"]
    ).fillna(0).round().astype("int32")

    out = merged[["cell_id", "daytime_workers"]]
    save_df(out, "lodes")
    log(f"total LA daytime workers (sum of cells): {out['daytime_workers'].sum():,}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--force", action="store_true")
    args = p.parse_args()
    main(force=args.force)
