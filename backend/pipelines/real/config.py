"""Project-wide constants. Imported by every pipeline script.

Edit `LA_BBOX` or `H3_RES` to change study area / resolution; everything
downstream picks the change up automatically.
"""
from __future__ import annotations

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[3]
BACKEND_DIR = REPO_ROOT / "backend"
SHARED_DIR = BACKEND_DIR / "shared"

DATA_DIR = Path(os.environ.get("RISKLENS_DATA_DIR", BACKEND_DIR / "data"))
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
# Real pipeline writes its own DB file. The API picks which one to serve via
# DUCKDB_PATH env var. Override here only if you need a non-standard layout.
DUCKDB_PATH = Path(
    os.environ.get(
        "REAL_DUCKDB_PATH", PROCESSED_DIR / "risklens.real.duckdb"
    )
)
SCHEMA_PATH = SHARED_DIR / "schema.sql"
WEIGHTS_PATH = SHARED_DIR / "weights.yaml"

for p in (RAW_DIR, PROCESSED_DIR):
    p.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Study area: Los Angeles County
# ---------------------------------------------------------------------------
LA_COUNTY_FIPS = "06037"   # state 06 (CA), county 037 (LA)
LA_STATE_FIPS = "06"

# WGS84 bounding box used to clip every raster/zarr read.
# Padded ~0.1° beyond county bounds so cell centroids near the edge still
# get a valid sampled value (avoids NaN cliffs).
LA_BBOX = (-119.0, 33.6, -117.5, 34.9)   # (minlon, minlat, maxlon, maxlat)

# ---------------------------------------------------------------------------
# Spatial grid
# ---------------------------------------------------------------------------
# H3 resolution table (https://h3geo.org/docs/core-library/restable):
#   res 8 -> ~0.74 km^2 per hex (good for county-wide overview)
#   res 9 -> ~0.105 km^2 per hex (~150 m edge -- "block-level" feel) <-- DEFAULT
#   res 10 -> ~0.015 km^2 per hex (city-block; ~5x more cells; slow)
H3_RES = 9

# ---------------------------------------------------------------------------
# Climate windows and emission scenarios
# ---------------------------------------------------------------------------
# Climate science treats projections as multi-decade averages. We label by
# the midpoint year so the UI slider can show "2030 / 2050 / 2080 / 2100".
CLIMATE_WINDOWS: dict[str, tuple[int, int]] = {
    "1981-2010": (1981, 2010),     # baseline
    "2021-2040": (2021, 2040),     # ~"2030"
    "2041-2060": (2041, 2060),     # ~"2050"
    "2071-2090": (2071, 2090),     # ~"2080"
    "2081-2100": (2081, 2100),     # ~"2100"
}
BASELINE_WINDOW = "1981-2010"
FUTURE_WINDOWS = [w for w in CLIMATE_WINDOWS if w != BASELINE_WINDOW]

SCENARIOS = ["historical", "ssp245", "ssp370", "ssp585"]
HISTORICAL_SCENARIO = "historical"
FUTURE_SCENARIOS = [s for s in SCENARIOS if s != HISTORICAL_SCENARIO]

# Mapping of UI labels -> (scenario, default future window)
UI_TO_SCENARIO = {
    "Lower / Moderate":  "ssp245",
    "High":              "ssp370",
    "Very High":         "ssp585",
}

# ---------------------------------------------------------------------------
# Climate model ensemble
# ---------------------------------------------------------------------------
# NEX-GDDP-CMIP6 is the default source (anonymous S3, well-documented paths,
# global ~25 km grid). For LA we want the ENSEMBLE-MEAN signal, not a single
# model — so we pull ~5 well-validated models and average them.
#
# To upgrade to California-specific 3 km LOCA2-Hybrid, set
# CLIMATE_SOURCE=loca2 and verify Cal-Adapt zarr paths in 10_climate_loca2.py.
CLIMATE_SOURCE = os.environ.get("CLIMATE_SOURCE", "nex-gddp")  # "nex-gddp" | "loca2"

NEX_GDDP_BUCKET = "nex-gddp-cmip6"
NEX_GDDP_MODELS = [
    "ACCESS-CM2",
    "CNRM-ESM2-1",
    "EC-Earth3",
    "MPI-ESM1-2-HR",
    "MIROC6",
]
NEX_GDDP_MEMBER = "r1i1p1f1"
NEX_GDDP_GRID = "gn"

# Variables we actually need.
#   tasmax (K) -> heat days
#   tasmin (K) -> warm nights
#   pr (kg/m2/s -> mm/day after *86400) -> annual mean + 99th pct
CLIMATE_VARS = ["tasmax", "tasmin", "pr"]

# Thresholds for derived metrics (in dataset native units).
HEAT_DAY_THRESHOLD_K = 273.15 + 35.0      # 35 C
WARM_NIGHT_THRESHOLD_K = 273.15 + 20.0    # 20 C

# When sampling a 20-year window, fully reading every year is overkill for
# climatology. We subsample to this many years per window per model to keep
# pipeline runtime sane on a laptop. Set to None for full window.
YEARS_SAMPLED_PER_WINDOW = 5
