"""19_fema_nri.py -- FEMA National Risk Index by Census tract.

Pulls the tract-level NRI CSV (one row per tract, ~150 cols) and keeps the
hazard expected-annual-loss (EAL) values we care about plus the community
resilience score.

NRI Tract CSV: https://hazards.fema.gov/nri/data-resources -> "Tract" CSV bundle.
The actual download URL changes per release; we try a couple of common ones,
otherwise fall back to a manual file in data/raw/.

Output: data/processed/nri.parquet
        columns: tract_fips,
                 nri_heat_eal, nri_wildfire_eal, nri_riverine_eal, nri_coastal_eal,
                 community_resilience
"""
from __future__ import annotations

import argparse
import io
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

from pipelines.real.io import download, ensure_path_setup, has, load_gdf, log, save_df
from pipelines.real.config import LA_STATE_FIPS, RAW_DIR

ensure_path_setup()

NRI_URLS = [
    # Known release bundles; if all fail, look in raw/ for manually-placed CSV.
    "https://hazards.fema.gov/nri/data-resources/files/NRI_Table_CensusTracts.zip",
    "https://hazards.fema.gov/nri/Content/StaticDocuments/DataDownload/NRI_Table_CensusTracts.zip",
]

# NRI uses different column suffixes per release; try several. EAL = Expected Annual Loss.
COL_CANDIDATES = {
    "tract_fips":            ["TRACTFIPS", "STCOFIPS", "GEOID", "TRACT"],
    "nri_heat_eal":          ["HWAV_EALT", "HEAT_EALT", "HWAV_EAL"],
    "nri_wildfire_eal":      ["WFIR_EALT", "WFIR_EAL"],
    "nri_riverine_eal":      ["RFLD_EALT", "RFLD_EAL"],
    "nri_coastal_eal":       ["CFLD_EALT", "CFLD_EAL"],
    "community_resilience":  ["RESL_VALUE", "RESL_SCORE", "RESL_RATNG"],
}


def _try_download() -> Path | None:
    for url in NRI_URLS:
        dest = RAW_DIR / Path(url).name
        try:
            return download(url, dest)
        except Exception as e:
            log(f"  could not fetch {url}: {e!r}")
    manual = list(RAW_DIR.glob("NRI*Tract*.csv")) + list(RAW_DIR.glob("NRI*Tract*.zip"))
    return manual[0] if manual else None


def _load_csv(path: Path) -> pd.DataFrame:
    if path.suffix == ".zip":
        with zipfile.ZipFile(path) as z:
            csv_name = next(n for n in z.namelist() if n.lower().endswith(".csv"))
            with z.open(csv_name) as f:
                return pd.read_csv(io.TextIOWrapper(f, encoding="latin-1"),
                                   low_memory=False)
    return pd.read_csv(path, encoding="latin-1", low_memory=False)


def _pick(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols_upper = {c.upper(): c for c in df.columns}
    for cand in candidates:
        if cand.upper() in cols_upper:
            return cols_upper[cand.upper()]
    return None


def _synthetic(tract_fips: pd.Series) -> pd.DataFrame:
    log("synthetic NRI mode (place NRI_Table_CensusTracts.zip in data/raw/ for real)")
    rng = np.random.default_rng(19)
    n = len(tract_fips)
    return pd.DataFrame({
        "tract_fips":           tract_fips.values,
        "nri_heat_eal":         rng.gamma(2, 50_000, n),
        "nri_wildfire_eal":     rng.gamma(1.5, 20_000, n),
        "nri_riverine_eal":     rng.gamma(1.5, 15_000, n),
        "nri_coastal_eal":      rng.gamma(1.0, 5_000, n),
        "community_resilience": np.clip(rng.normal(50, 12, n), 0, 100),
    })


def main(force: bool = False) -> None:
    if has("nri") and not force:
        log("nri.parquet exists; use --force to rebuild")
        return

    tracts = load_gdf("tracts")["tract_fips"]
    src = _try_download()
    if src is None:
        save_df(_synthetic(tracts), "nri")
        return

    df = _load_csv(src)
    log(f"NRI rows total: {len(df):,}; columns: {len(df.columns)}")

    # Map to our schema.
    col_map = {k: _pick(df, v) for k, v in COL_CANDIDATES.items()}
    missing = [k for k, v in col_map.items() if v is None]
    if missing:
        log(f"  WARNING: missing NRI columns {missing}; using synthetic for those")

    if col_map["tract_fips"] is None:
        log("NRI has no tract FIPS column; using synthetic entirely")
        save_df(_synthetic(tracts), "nri")
        return

    out = pd.DataFrame({"tract_fips": df[col_map["tract_fips"]].astype(str).str.zfill(11)})
    out = out[out["tract_fips"].str.startswith(LA_STATE_FIPS + "037")].copy()

    syn = _synthetic(out["tract_fips"])
    for k, src_col in col_map.items():
        if k == "tract_fips":
            continue
        if src_col is None:
            out[k] = syn[k].values
        else:
            out[k] = pd.to_numeric(df.loc[out.index, src_col], errors="coerce")
            out[k] = out[k].fillna(syn[k].values)

    out = (pd.DataFrame({"tract_fips": tracts.unique()})
             .merge(out, on="tract_fips", how="left"))
    for c in out.columns:
        if c == "tract_fips":
            continue
        out[c] = out[c].fillna(out[c].median())

    save_df(out, "nri")
    log(f"NRI rows for LA tracts: {len(out):,}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--force", action="store_true")
    args = p.parse_args()
    main(force=args.force)
