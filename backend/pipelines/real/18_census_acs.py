"""18_census_acs.py -- ACS 5-year demographics for LA County tracts.

Pulls a small set of vulnerability-relevant variables from the Census API and
derives clean rate columns (% age 65+, % no vehicle, % below poverty).

Requires CENSUS_API_KEY. Without it we fall back to synthetic values.

ACS variables used (2022 ACS 5-year):
    B01003_001E  total population
    B01001_020E..B01001_025E  males 65+
    B01001_044E..B01001_049E  females 65+
    B17001_002E  population in poverty
    B25044_003E + B25044_010E  households with no vehicle (owner + renter)
    B25044_001E  total households

Output: data/processed/acs.parquet
        columns: tract_fips, pct_age_65plus, pct_below_poverty, pct_no_vehicle
"""
from __future__ import annotations

import argparse
import os

import numpy as np
import pandas as pd
import requests

from pipelines.real.io import ensure_path_setup, has, load_gdf, log, save_df
from pipelines.real.config import LA_COUNTY_FIPS, LA_STATE_FIPS

ensure_path_setup()

ACS_YEAR = 2022
ACS_DATASET = f"https://api.census.gov/data/{ACS_YEAR}/acs/acs5"

# Friendly aliases mapped to the underlying ACS variable codes.
VARS = {
    "total_pop":      "B01003_001E",
    "pov_pop":        "B17001_002E",
    "hh_total":       "B25044_001E",
    "hh_no_veh_own":  "B25044_003E",
    "hh_no_veh_rent": "B25044_010E",
    # Age 65+ pieces (sum of male 65+ + female 65+):
    "m_65_66": "B01001_020E", "m_67_69": "B01001_021E", "m_70_74": "B01001_022E",
    "m_75_79": "B01001_023E", "m_80_84": "B01001_024E", "m_85up":  "B01001_025E",
    "f_65_66": "B01001_044E", "f_67_69": "B01001_045E", "f_70_74": "B01001_046E",
    "f_75_79": "B01001_047E", "f_80_84": "B01001_048E", "f_85up":  "B01001_049E",
}


def _fetch_acs(api_key: str) -> pd.DataFrame:
    """Return a long DataFrame of ACS values for LA County tracts."""
    get = ",".join(["NAME"] + list(VARS.values()))
    params = {
        "get": get,
        "for": "tract:*",
        "in": f"state:{LA_STATE_FIPS} county:{LA_COUNTY_FIPS[2:]}",
        "key": api_key,
    }
    log(f"GET {ACS_DATASET}")
    r = requests.get(ACS_DATASET, params=params, timeout=60)
    r.raise_for_status()
    rows = r.json()
    df = pd.DataFrame(rows[1:], columns=rows[0])

    # Rename ACS codes back to friendly aliases.
    inv = {v: k for k, v in VARS.items()}
    df = df.rename(columns={c: inv.get(c, c) for c in df.columns})

    # Cast numerics; ACS uses negative sentinels (-666666666 etc) for suppressed.
    for k in VARS:
        df[k] = pd.to_numeric(df[k], errors="coerce")
        df.loc[df[k] < 0, k] = np.nan

    df["tract_fips"] = df["state"] + df["county"] + df["tract"]
    return df


def _derive(df: pd.DataFrame) -> pd.DataFrame:
    age65 = df[[
        "m_65_66", "m_67_69", "m_70_74", "m_75_79", "m_80_84", "m_85up",
        "f_65_66", "f_67_69", "f_70_74", "f_75_79", "f_80_84", "f_85up",
    ]].sum(axis=1, skipna=False)

    no_veh = df[["hh_no_veh_own", "hh_no_veh_rent"]].sum(axis=1, skipna=False)

    out = pd.DataFrame({
        "tract_fips":         df["tract_fips"],
        "pct_age_65plus":     100 * age65 / df["total_pop"],
        "pct_below_poverty":  100 * df["pov_pop"] / df["total_pop"],
        "pct_no_vehicle":     100 * no_veh / df["hh_total"],
    })
    for c in ("pct_age_65plus", "pct_below_poverty", "pct_no_vehicle"):
        out[c] = out[c].clip(lower=0, upper=100)
    return out


def _synthetic_acs(tract_fips: pd.Series) -> pd.DataFrame:
    log("synthetic ACS mode (set CENSUS_API_KEY in .env for real values)")
    rng = np.random.default_rng(17)
    n = len(tract_fips)
    return pd.DataFrame({
        "tract_fips":         tract_fips.values,
        "pct_age_65plus":     np.clip(rng.normal(13, 5, n), 1, 40),
        "pct_below_poverty":  np.clip(rng.gamma(2, 8, n), 0, 60),
        "pct_no_vehicle":     np.clip(rng.gamma(2, 5, n), 0, 50),
    })


def main(force: bool = False) -> None:
    if has("acs") and not force:
        log("acs.parquet exists; use --force to rebuild")
        return

    tracts = load_gdf("tracts")["tract_fips"]
    api_key = os.environ.get("CENSUS_API_KEY", "").strip()

    if not api_key:
        out = _synthetic_acs(tracts)
    else:
        try:
            raw = _fetch_acs(api_key)
            out = _derive(raw)
            # Reindex to canonical tract list, impute missing.
            out = (pd.DataFrame({"tract_fips": tracts.unique()})
                     .merge(out, on="tract_fips", how="left"))
            for c in ("pct_age_65plus", "pct_below_poverty", "pct_no_vehicle"):
                out[c] = out[c].fillna(out[c].median())
        except Exception as e:
            log(f"ACS API call failed ({e!r}); falling back to synthetic")
            out = _synthetic_acs(tracts)

    save_df(out, "acs")
    log(f"ACS rows: {len(out):,}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--force", action="store_true")
    args = p.parse_args()
    main(force=args.force)
