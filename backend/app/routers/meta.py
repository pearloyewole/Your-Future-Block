"""Meta endpoints: health + provenance.

These exist so the demo can answer two questions out loud:
  1. Is the API up?
  2. Is it serving the synthetic build or the real build?
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException

from ..db import get_db
from ..settings import settings

log = logging.getLogger("risklens.api.meta")

router = APIRouter(tags=["meta"])


def _read_provenance() -> dict[str, Any] | None:
    """Best-effort read of the provenance row written at pipeline build time."""
    db = get_db()
    try:
        row = db.fetchone(
            "SELECT pipeline, built_at, git_sha, cell_count, tract_count, "
            "windows, scenarios, notes FROM provenance LIMIT 1;"
        )
    except Exception as e:  # table missing, file missing, etc.
        log.warning("provenance lookup failed: %r", e)
        return None
    if not row:
        return None
    pipeline, built_at, git_sha, cells, tracts, windows, scenarios, notes = row
    if isinstance(built_at, datetime):
        built_at = built_at.isoformat()
    return {
        "pipeline":    pipeline,
        "built_at":    built_at,
        "git_sha":     git_sha,
        "cell_count":  cells,
        "tract_count": tracts,
        "windows":     json.loads(windows) if windows else [],
        "scenarios":   json.loads(scenarios) if scenarios else [],
        "notes":       notes,
    }


@router.get("/healthz")
def healthz() -> dict[str, Any]:
    """Liveness + provenance probe. Always returns 200 unless the process is dead."""
    duckdb_exists = settings.duckdb_path.exists()
    return {
        "status":      "ok",
        "duckdb_path": str(settings.duckdb_path),
        "duckdb_size_mb": round(settings.duckdb_path.stat().st_size / 1e6, 2)
                          if duckdb_exists else None,
        "duckdb_exists": duckdb_exists,
        "provenance":  _read_provenance(),
    }


@router.get("/provenance")
def provenance() -> dict[str, Any]:
    """Full provenance row, or 404 if the DB hasn't been built yet."""
    p = _read_provenance()
    if p is None:
        raise HTTPException(
            status_code=404,
            detail=(
                "No provenance row found. Build the DB first: "
                "`python -m pipelines.synthetic.build` or run the real pipeline."
            ),
        )
    return p
