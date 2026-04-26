"""Dual-backend database layer.

Goal: pipeline scripts and the runtime API both call ``get_db()`` and use the
same ``execute / executemany / fetch_*`` interface, so the same code runs against
either DuckDB (hackathon-friendly) or Postgres+PostGIS (production-friendly).
"""
from __future__ import annotations

import logging
import os
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

from .settings import REPO_ROOT, settings

log = logging.getLogger(__name__)

SCHEMA_PATH = REPO_ROOT / "backend/shared/schema.sql"


# --------------------------------------------------------------------- helpers
def _split_schema_statements(sql: str) -> list[str]:
    """Split on `-- ###` markers, drop empty/comment-only chunks."""
    out: list[str] = []
    for chunk in sql.split("-- ###"):
        chunk = chunk.strip()
        if not chunk:
            continue
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
        # Keep the index but strip the access-method clause, which DuckDB doesn't grok in
        # the same syntax (DuckDB spatial uses RTREE but as a separate command form).
        return stmt.split(" USING ")[0] + ";"
    return stmt


def _adapt_for_postgres(stmt: str) -> str | None:
    return stmt  # canonical form is already postgres-flavored


# --------------------------------------------------------------------- protocol
class Database:
    """Minimal sync DB interface used by pipeline + API.

    Subclasses implement the backend-specific bits.
    """

    backend: str

    def init_schema(self) -> None:
        raise NotImplementedError

    @contextmanager
    def conn(self) -> Iterator[Any]:
        raise NotImplementedError

    def execute(self, sql: str, params: Sequence | dict | None = None) -> None:
        with self.conn() as cx:
            cx.execute(sql, params or [])

    def executemany(self, sql: str, rows: Iterable[Sequence]) -> None:
        with self.conn() as cx:
            cx.executemany(sql, list(rows))

    def fetchone(self, sql: str, params: Sequence | dict | None = None) -> tuple | None:
        with self.conn() as cx:
            cur = cx.execute(sql, params or [])
            return cur.fetchone()

    def fetchall(self, sql: str, params: Sequence | dict | None = None) -> list[tuple]:
        with self.conn() as cx:
            cur = cx.execute(sql, params or [])
            return cur.fetchall()

    def write_geodataframe(self, gdf, table: str, *, mode: str = "replace") -> None:
        """Bulk write a GeoDataFrame to ``table``. ``mode`` is replace|append."""
        raise NotImplementedError

    def cell_for_point(self, lat: float, lon: float) -> str | None:
        raise NotImplementedError


# --------------------------------------------------------------------- DuckDB
class DuckDBDatabase(Database):
    backend = "duckdb"

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._read_only = False
        self._duckdb_home = Path(
            os.environ.get("RISKLENS_DUCKDB_HOME", REPO_ROOT / "backend/data/.duckdb")
        )
        self._duckdb_home.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def conn(self) -> Iterator[Any]:
        import duckdb

        if not hasattr(self._local, "c"):
            self._local.c = duckdb.connect(
                str(self.path),
                read_only=self._read_only,
                config={"home_directory": str(self._duckdb_home)},
            )
            self._local.c.execute("INSTALL spatial; LOAD spatial;")
        yield self._local.c

    def set_read_only(self, ro: bool) -> None:
        self._read_only = ro
        if hasattr(self._local, "c"):
            try:
                self._local.c.close()
            finally:
                del self._local.c

    def init_schema(self) -> None:
        sql = SCHEMA_PATH.read_text()
        with self.conn() as cx:
            for raw in _split_schema_statements(sql):
                stmt = _adapt_for_duckdb(raw)
                if stmt:
                    cx.execute(stmt)
        log.info("DuckDB schema initialized at %s", self.path)

    def write_geodataframe(self, gdf, table: str, *, mode: str = "replace") -> None:
        import geopandas as gpd  # noqa: F401

        if "geom" not in gdf.columns and gdf.geometry.name != "geom":
            gdf = gdf.rename_geometry("geom")
        df = gdf.copy()
        df["geom"] = df["geom"].to_wkb()
        with self.conn() as cx:
            cx.register("_stage", df)
            cols = ", ".join(c for c in df.columns if c != "geom")
            select = f"SELECT {cols}, ST_GeomFromWKB(geom) AS geom FROM _stage"
            if mode == "replace":
                cx.execute(f"CREATE OR REPLACE TABLE {table} AS {select};")
            elif mode == "append":
                cx.execute(f"INSERT INTO {table} {select};")
            else:
                raise ValueError(mode)
            cx.unregister("_stage")

    def cell_for_point(self, lat: float, lon: float) -> str | None:
        row = self.fetchone(
            "SELECT cell_id FROM cells WHERE ST_Contains(geom, ST_Point(?, ?)) LIMIT 1;",
            (lon, lat),
        )
        return row[0] if row else None


# --------------------------------------------------------------------- Postgres
class PostgresDatabase(Database):
    backend = "postgres"

    def __init__(self, url: str) -> None:
        from sqlalchemy import create_engine

        self.engine = create_engine(url, pool_size=5, pool_pre_ping=True, future=True)

    @contextmanager
    def conn(self) -> Iterator[Any]:
        from sqlalchemy import text

        with self.engine.begin() as cx:
            class _Adapter:
                def execute(self, sql, params=None):
                    return cx.execute(text(sql), params or {})

                def executemany(self, sql, rows):
                    return cx.execute(text(sql), list(rows))

            yield _Adapter()

    def init_schema(self) -> None:
        sql = SCHEMA_PATH.read_text()
        with self.conn() as cx:
            for raw in _split_schema_statements(sql):
                stmt = _adapt_for_postgres(raw)
                if stmt:
                    cx.execute(stmt)
        log.info("Postgres schema initialized")

    def write_geodataframe(self, gdf, table: str, *, mode: str = "replace") -> None:
        if_exists = "replace" if mode == "replace" else "append"
        gdf.to_postgis(table, self.engine, if_exists=if_exists, index=False)

    def cell_for_point(self, lat: float, lon: float) -> str | None:
        from sqlalchemy import text

        with self.engine.connect() as cx:
            row = cx.execute(
                text(
                    "SELECT cell_id FROM cells "
                    "WHERE ST_Contains(geom, ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)) LIMIT 1"
                ),
                {"lon": lon, "lat": lat},
            ).first()
        return row[0] if row else None


# --------------------------------------------------------------------- factory
_db_singleton: Database | None = None


def get_db() -> Database:
    global _db_singleton
    if _db_singleton is not None:
        return _db_singleton
    if settings.risklens_db_backend == "duckdb":
        _db_singleton = DuckDBDatabase(settings.duckdb_path)
    elif settings.risklens_db_backend == "postgres":
        _db_singleton = PostgresDatabase(settings.database_url)
    else:
        raise ValueError(f"Unknown backend {settings.risklens_db_backend}")
    return _db_singleton


def reset_db_singleton() -> None:
    """For tests / pipeline scripts that change settings between runs."""
    global _db_singleton
    _db_singleton = None
