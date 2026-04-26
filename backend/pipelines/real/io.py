"""Shared I/O helpers used by every pipeline script.

Conventions:
- All processed layers are written as GeoParquet (or plain Parquet) under
  PROCESSED_DIR. Filenames match the pipeline step (e.g. 'cells.parquet',
  'fhsz.parquet').
- Every GeoDataFrame is reprojected to EPSG:4326 before writing.
- DuckDB is opened lazily with the spatial extension loaded.
"""
from __future__ import annotations

import hashlib
import os
import shutil
import sys
import zipfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable

import duckdb
import geopandas as gpd
import pandas as pd
import requests
from tqdm import tqdm

from pipelines.real.config import DUCKDB_PATH, PROCESSED_DIR, RAW_DIR, SCHEMA_PATH

DUCKDB_HOME = Path(
    os.environ.get(
        "RISKLENS_DUCKDB_HOME",
        Path(__file__).resolve().parents[2] / "data/.duckdb",
    )
)
DUCKDB_HOME.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Logging (lightweight, no external deps)
# ---------------------------------------------------------------------------
def log(msg: str) -> None:
    print(f"[risklens] {msg}", flush=True)


def die(msg: str, code: int = 1) -> None:
    print(f"[risklens] FATAL: {msg}", file=sys.stderr, flush=True)
    sys.exit(code)


# ---------------------------------------------------------------------------
# HTTP downloads (resumable, with progress bar)
# ---------------------------------------------------------------------------
def download(url: str, dest: Path, *, force: bool = False, expected_sha256: str | None = None) -> Path:
    """Stream a file to disk. Skips if it exists unless force=True."""
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and not force:
        log(f"have {dest.name} ({dest.stat().st_size/1e6:.1f} MB), skipping download")
        return dest

    log(f"downloading {url} -> {dest}")
    tmp = dest.with_suffix(dest.suffix + ".part")
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with open(tmp, "wb") as f, tqdm(
            total=total, unit="B", unit_scale=True, unit_divisor=1024,
            disable=total == 0, desc=dest.name
        ) as bar:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)
                    bar.update(len(chunk))
    tmp.rename(dest)

    if expected_sha256:
        got = sha256_file(dest)
        if got != expected_sha256:
            die(f"sha256 mismatch on {dest}: got {got}, expected {expected_sha256}")
    return dest


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def unzip(src: Path, dest_dir: Path) -> Path:
    """Extract a zip archive into dest_dir (created if missing)."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(src) as z:
        z.extractall(dest_dir)
    return dest_dir


# ---------------------------------------------------------------------------
# GeoParquet helpers
# ---------------------------------------------------------------------------
def processed_path(name: str) -> Path:
    """Resolve 'foo' -> PROCESSED_DIR/foo.parquet (idempotent if user passed full name)."""
    p = PROCESSED_DIR / name
    if p.suffix != ".parquet":
        p = p.with_suffix(".parquet")
    return p


def save_gdf(gdf: gpd.GeoDataFrame, name: str) -> Path:
    """Write a GeoDataFrame as GeoParquet in EPSG:4326. Returns output path."""
    out = processed_path(name)
    out.parent.mkdir(parents=True, exist_ok=True)
    if "geometry" in gdf.columns and gdf.geometry.notna().any():
        if gdf.crs is None:
            log(f"WARNING: {name} GeoDataFrame has no CRS, assuming EPSG:4326")
            gdf = gdf.set_crs(4326)
        elif gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(4326)
    gdf.to_parquet(out, index=False)
    log(f"wrote {out.name} ({len(gdf):,} rows, {out.stat().st_size/1e6:.1f} MB)")
    return out


def save_df(df: pd.DataFrame, name: str) -> Path:
    out = processed_path(name)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    log(f"wrote {out.name} ({len(df):,} rows, {out.stat().st_size/1e6:.1f} MB)")
    return out


def load_gdf(name: str) -> gpd.GeoDataFrame:
    """Load a previously-saved GeoParquet by short name."""
    p = processed_path(name)
    if not p.exists():
        die(f"missing {p}; run the pipeline step that produces it first")
    return gpd.read_parquet(p)


def load_df(name: str) -> pd.DataFrame:
    p = processed_path(name)
    if not p.exists():
        die(f"missing {p}; run the pipeline step that produces it first")
    return pd.read_parquet(p)


def has(name: str) -> bool:
    return processed_path(name).exists()


# ---------------------------------------------------------------------------
# Spatial join: attach polygon attributes to cells
# ---------------------------------------------------------------------------
def attach_polygon_attrs_to_cells(
    cells: gpd.GeoDataFrame,
    polys: gpd.GeoDataFrame,
    cols: list[str],
    *,
    how: str = "max-class",
    class_order: list[str] | None = None,
    fill: dict | None = None,
) -> pd.DataFrame:
    """Spatial-join polygon attributes onto cells.

    Parameters
    ----------
    cells : GeoDataFrame with `cell_id`, polygon geometry.
    polys : GeoDataFrame with categorical/numeric attributes in `cols`.
    how :
        "max-class"  -> for one categorical column, pick the highest-ranked
                        class (per `class_order`) intersecting the cell.
        "first"      -> arbitrary first match (use when polys are mutually
                        exclusive, e.g. flood zones).
        "any"        -> boolean: did the cell intersect any polygon?
    fill : default values when no polygon intersects a cell.
    """
    if cells.crs is None:
        cells = cells.set_crs(4326)
    if polys.crs is None:
        polys = polys.set_crs(4326)
    polys = polys.to_crs(cells.crs)

    joined = gpd.sjoin(cells[["cell_id", "geometry"]], polys[cols + ["geometry"]],
                       how="left", predicate="intersects")

    if how == "max-class":
        if not class_order or len(cols) != 1:
            raise ValueError("max-class requires exactly one column and class_order")
        col = cols[0]
        rank = {c: i for i, c in enumerate(class_order)}
        joined["_rank"] = joined[col].map(rank).fillna(-1)
        out = (joined.sort_values("_rank", ascending=False)
                     .drop_duplicates("cell_id", keep="first")
                     [["cell_id"] + cols])
    elif how == "first":
        out = joined.drop_duplicates("cell_id", keep="first")[["cell_id"] + cols]
    elif how == "any":
        out = (joined.assign(_hit=joined[cols[0]].notna())
                     .groupby("cell_id", as_index=False)["_hit"].any()
                     .rename(columns={"_hit": cols[0]}))
    else:
        raise ValueError(f"unknown how={how}")

    if fill:
        out = out.copy()
        for k, v in fill.items():
            if k in out.columns:
                out[k] = out[k].fillna(v)

    return out


# ---------------------------------------------------------------------------
# DuckDB
# ---------------------------------------------------------------------------
@contextmanager
def duckdb_conn(read_only: bool = False):
    """Yield a DuckDB connection with the spatial extension loaded."""
    con = duckdb.connect(
        str(DUCKDB_PATH),
        read_only=read_only,
        config={"home_directory": str(DUCKDB_HOME)},
    )
    con.execute("INSTALL spatial; LOAD spatial;")
    try:
        yield con
    finally:
        con.close()


def init_db() -> None:
    """Apply backend/db/schema.sql. Safe to re-run; uses CREATE TABLE IF NOT EXISTS."""
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    sql = SCHEMA_PATH.read_text()
    with duckdb_conn() as con:
        con.execute(sql)
    log(f"db ready: {DUCKDB_PATH}")


def reset_db() -> None:
    """Delete the DuckDB file. Use --reset on the orchestrator to do this."""
    if DUCKDB_PATH.exists():
        DUCKDB_PATH.unlink()
        log(f"deleted {DUCKDB_PATH}")
    wal = DUCKDB_PATH.with_suffix(DUCKDB_PATH.suffix + ".wal")
    if wal.exists():
        wal.unlink()


# ---------------------------------------------------------------------------
# Misc
# ---------------------------------------------------------------------------
def chunked(seq: Iterable, size: int):
    buf = []
    for x in seq:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


def ensure_path_setup() -> None:
    """Add `backend/` to sys.path so `python pipelines/real/00_grid.py` works.

    Lets the script resolve both `from pipelines.real...` and `from shared...`
    without requiring `pip install -e .` or `python -m`.
    """
    backend = Path(__file__).resolve().parents[2]   # backend/pipelines/real -> backend
    if str(backend) not in sys.path:
        sys.path.insert(0, str(backend))
