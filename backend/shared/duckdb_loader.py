"""Shared DuckDB loader used by both pipelines.

Both `pipelines/synthetic/build.py` and `pipelines/real/build.py` end with a
call to `load_into_duckdb(db_path, ...)` to populate the canonical schema.
The API is then pointed at whichever .duckdb file you want to serve via
the `DUCKDB_PATH` env var.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import duckdb
import geopandas as gpd
import pandas as pd

from .scoring import SCHEMA_PATH

log = logging.getLogger("risklens.shared.duckdb")
logging.basicConfig(level=logging.INFO, format="[%(name)s] %(message)s")
DEFAULT_DUCKDB_HOME = Path(
    os.environ.get("RISKLENS_DUCKDB_HOME", SCHEMA_PATH.parent.parent / "data/.duckdb")
)
DEFAULT_DUCKDB_HOME.mkdir(parents=True, exist_ok=True)


def _split_schema_statements(sql: str) -> list[str]:
    """Split on `-- ###` markers, drop empty/comment-only chunks."""
    out: list[str] = []
    for chunk in sql.split("-- ###"):
        chunk = chunk.strip()
        if not chunk:
            continue
        # A chunk that is only SQL line comments has no executable content.
        non_comment = [
            line for line in chunk.splitlines()
            if line.strip() and not line.strip().startswith("--")
        ]
        if not non_comment:
            continue
        out.append(chunk)
    return out


def _adapt_for_duckdb(stmt: str) -> str | None:
    """DuckDB-flavored rewrites of the canonical schema. Returns None to skip."""
    s = stmt.upper()
    if s.startswith("CREATE EXTENSION"):
        return None
    if "USING GIST" in s or "USING RTREE" in s:
        return stmt.split(" USING ")[0] + ";"
    return stmt


@contextmanager
def duckdb_conn(db_path: Path, read_only: bool = False) -> Iterator[duckdb.DuckDBPyConnection]:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(
        str(db_path),
        read_only=read_only,
        config={"home_directory": str(DEFAULT_DUCKDB_HOME)},
    )
    con.execute("INSTALL spatial; LOAD spatial;")
    try:
        yield con
    finally:
        con.close()


def init_schema(db_path: Path) -> None:
    """Create all canonical tables in the target DuckDB file. Idempotent."""
    sql = SCHEMA_PATH.read_text()
    with duckdb_conn(db_path) as con:
        for raw in _split_schema_statements(sql):
            stmt = _adapt_for_duckdb(raw)
            if stmt:
                con.execute(stmt)
    log.info("schema ready at %s", db_path)


def reset_db(db_path: Path) -> None:
    db_path = Path(db_path)
    if db_path.exists():
        db_path.unlink()
        log.info("deleted %s", db_path)
    wal = db_path.with_suffix(db_path.suffix + ".wal")
    if wal.exists():
        wal.unlink()


def _git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], text=True, stderr=subprocess.DEVNULL
        ).strip()
    except Exception:
        return "unknown"


def load_into_duckdb(
    db_path: Path,
    *,
    pipeline: str,
    cells: gpd.GeoDataFrame,
    tracts: gpd.GeoDataFrame,
    cell_attrs: pd.DataFrame,
    cell_climate: pd.DataFrame,
    risk_cells: pd.DataFrame,
    notes: str = "",
) -> None:
    """Wipe + repopulate every runtime table from in-memory frames."""
    init_schema(db_path)

    cells_df = pd.DataFrame({
        "cell_id":      cells["cell_id"].astype(str),
        "centroid_lat": cells["centroid_lat"].astype(float),
        "centroid_lon": cells["centroid_lon"].astype(float),
        "tract_fips":   cells["tract_fips"].astype(str),
        "geom_wkb":     cells.geometry.to_wkb(),
    })
    tracts_df = pd.DataFrame({
        "tract_fips":  tracts["tract_fips"].astype(str),
        "state_fips":  tracts["state_fips"].astype(str),
        "county_fips": tracts["county_fips"].astype(str),
        "name":        tracts["name"].astype(str),
        "aland":       tracts["aland"].astype(float),
        "awater":      tracts["awater"].astype(float),
        "geom_wkb":    tracts.geometry.to_wkb(),
    })

    attrs_cols_schema = [
        "cell_id", "elevation_m", "slope_deg", "impervious_pct", "tree_canopy_pct",
        "fhsz_class", "wui_class", "dist_to_fhsz_vh_m", "fires_5km_30yr",
        "flood_zone", "in_100yr", "in_500yr", "slr_inundated_ft", "dist_to_coast_m",
        "svi_overall", "pct_age_65plus", "pct_no_vehicle", "pct_below_poverty",
        "pct_disability", "median_income",
        "nri_heat_eal", "nri_wildfire_eal", "nri_riverine_eal", "nri_coastal_eal",
        "community_resilience", "daytime_workers", "transit_stops_400m",
    ]
    attrs_df = cell_attrs.reindex(columns=attrs_cols_schema).copy()
    # Booleans need to be real booleans, not numpy.object_ NaNs.
    for bcol in ("in_100yr", "in_500yr"):
        attrs_df[bcol] = attrs_df[bcol].fillna(False).astype(bool)

    climate_df = cell_climate.rename(columns={"window": "window_label"}).copy()
    risk_df = risk_cells.rename(columns={"window": "window_label"}).copy()

    with duckdb_conn(db_path) as con:
        for tbl in ("provenance", "risk_cells", "cell_climate", "cell_attrs", "cells", "tracts"):
            con.execute(f"DELETE FROM {tbl}")

        con.register("cells_df", cells_df)
        con.execute("""
            INSERT INTO cells (cell_id, centroid_lat, centroid_lon, tract_fips, geom)
            SELECT cell_id, centroid_lat, centroid_lon, tract_fips,
                   ST_GeomFromWKB(geom_wkb)
            FROM cells_df
        """)
        con.unregister("cells_df")

        con.register("tracts_df", tracts_df)
        con.execute("""
            INSERT INTO tracts (tract_fips, state_fips, county_fips, name, aland, awater, geom)
            SELECT tract_fips, state_fips, county_fips, name, aland, awater,
                   ST_GeomFromWKB(geom_wkb)
            FROM tracts_df
        """)
        con.unregister("tracts_df")

        con.register("attrs_df", attrs_df)
        con.execute(
            f"INSERT INTO cell_attrs SELECT {','.join(attrs_cols_schema)} FROM attrs_df"
        )
        con.unregister("attrs_df")

        con.register("climate_df", climate_df)
        con.execute("""
            INSERT INTO cell_climate
              (cell_id, window_label, scenario,
               heat_days, warm_nights, pr_annual_mm, pr_p99_mm, cdd_max)
            SELECT cell_id, window_label, scenario,
                   heat_days, warm_nights, pr_annual_mm, pr_p99_mm, cdd_max
            FROM climate_df
        """)
        con.unregister("climate_df")

        con.register("risk_df", risk_df)
        con.execute("""
            INSERT INTO risk_cells
              (cell_id, window_label, scenario,
               heat_score, wildfire_score, flood_score, overall_score,
               heat_label,  wildfire_label,  flood_label,  overall_label,
               drivers)
            SELECT cell_id, window_label, scenario,
                   heat_score, wildfire_score, flood_score, overall_score,
                   heat_label,  wildfire_label,  flood_label,  overall_label,
                   drivers
            FROM risk_df
        """)
        con.unregister("risk_df")

        windows = sorted(cell_climate["window"].unique().tolist())
        scenarios = sorted(cell_climate["scenario"].unique().tolist())
        con.execute(
            "INSERT INTO provenance "
            "(pipeline, built_at, git_sha, cell_count, tract_count, windows, scenarios, notes) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                pipeline,
                datetime.now(timezone.utc),
                _git_sha(),
                int(len(cells)),
                int(len(tracts)),
                json.dumps(windows),
                json.dumps(scenarios),
                notes,
            ),
        )

        for tbl in ("tracts", "cells", "cell_attrs", "cell_climate", "risk_cells", "provenance"):
            n = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            log.info("  %-14s %d rows", tbl, n)
