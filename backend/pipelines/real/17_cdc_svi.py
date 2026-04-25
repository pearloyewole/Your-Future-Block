"""17_cdc_svi.py -- CDC/ATSDR Social Vulnerability Index by Census tract.

Source: https://www.atsdr.cdc.gov/place-health/php/svi/svi-data-documentation-download.html
We pull the latest California tract-level CSV (no auth required) and join to
LA County tracts.

We use a small, well-documented set of columns:
    RPL_THEMES   -> overall percentile rank (0..1) -> svi_overall
The rest is left to ACS / NRI for richer demographics.

Output: data/processed/svi.parquet
        columns: tract_fips, svi_overall
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from pipelines.real.io import download, ensure_path_setup, has, log, save_df
from pipelines.real.config import LA_COUNTY_FIPS, LA_STATE_FIPS, RAW_DIR

ensure_path_setup()

SVI_URLS = [
    # CDC currently hosts state CSVs at this pattern; if it 404s, fall back to manual.
    f"https://svi.cdc.gov/Documents/Data/2022/csv/states/California.csv",
    f"https://svi.cdc.gov/Documents/Data/2020/csv/states/California.csv",
]


def _try_download() -> Path | None:
    for url in SVI_URLS:
        dest = RAW_DIR / Path(url).name
        try:
            return download(url, dest)
        except Exception as e:
            log(f"  could not fetch {url}: {e!r}")
    manual = list(RAW_DIR.glob("*California*.csv")) + list(RAW_DIR.glob("*svi*.csv"))
    return manual[0] if manual else None


def _synthetic_svi(tract_fips: pd.Series) -> pd.DataFrame:
    log("synthetic SVI mode (drop a CDC SVI California CSV in data/raw/ for real values)")
    rng = np.random.default_rng(13)
    return pd.DataFrame({
        "tract_fips": tract_fips.values,
        "svi_overall": np.clip(rng.beta(2.0, 2.0, size=len(tract_fips)), 0, 1),
    })


def main(force: bool = False) -> None:
    if has("svi") and not force:
        log("svi.parquet exists; use --force to rebuild")
        return

    # Get the canonical tract list from our cells/tracts dataset.
    from pipelines.real.io import load_gdf
    tracts = load_gdf("tracts")["tract_fips"]

    src = _try_download()
    if src is None:
        out = _synthetic_svi(tracts)
        save_df(out, "svi")
        return

    log(f"reading {src.name}")
    df = pd.read_csv(src, dtype={"FIPS": str, "STCNTY": str})
    fips_col = next((c for c in df.columns if c.upper() in {"FIPS", "GEOID", "TRACT"}), None)
    rpl_col = next((c for c in df.columns if c.upper() in {"RPL_THEMES", "RPL_THEME"}), None)
    if fips_col is None or rpl_col is None:
        log(f"WARNING: SVI CSV missing expected cols; have {list(df.columns)[:10]}...")
        out = _synthetic_svi(tracts)
        save_df(out, "svi")
        return

    df = df[[fips_col, rpl_col]].rename(
        columns={fips_col: "tract_fips", rpl_col: "svi_overall"}
    )
    df["tract_fips"] = df["tract_fips"].astype(str).str.zfill(11)
    # CDC encodes "missing" as -999.
    df["svi_overall"] = pd.to_numeric(df["svi_overall"], errors="coerce")
    df.loc[df["svi_overall"] < 0, "svi_overall"] = np.nan
    # Restrict to LA County.
    df = df[df["tract_fips"].str.startswith(LA_STATE_FIPS + "037")]
    log(f"SVI tract rows for LA: {len(df):,}")

    # Reindex to our canonical tract list; impute missing tracts with the
    # county median so downstream joins never produce NaN.
    out = pd.DataFrame({"tract_fips": tracts.unique()})
    out = out.merge(df, on="tract_fips", how="left")
    median = out["svi_overall"].median(skipna=True)
    out["svi_overall"] = out["svi_overall"].fillna(median)
    save_df(out, "svi")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--force", action="store_true")
    args = p.parse_args()
    main(force=args.force)
