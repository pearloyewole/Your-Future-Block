"""RiskLens FastAPI app.

The API is pipeline-agnostic: it knows nothing about whether the DuckDB
file under DUCKDB_PATH was produced by the synthetic or real pipeline.
Both write the same canonical schema (backend/shared/schema.sql), so the
same SQL works for both.

Run against synthetic DB:
    DUCKDB_PATH=backend/data/processed/risklens.synthetic.duckdb \
        uvicorn app.api:app --reload --port 8000

Run against real DB:
    DUCKDB_PATH=backend/data/processed/risklens.real.duckdb \
        uvicorn app.api:app --reload --port 8001
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .routers import cells, compat, lookup, meta, scenarios
from .settings import settings

logging.basicConfig(level=settings.log_level, format="[%(name)s] %(message)s")
log = logging.getLogger("risklens.api")
STATIC_DIR = Path(__file__).resolve().parent / "static"

app = FastAPI(
    title="RiskLens API",
    description=(
        "Block-level climate exposure for Los Angeles. Heat, wildfire, "
        "and flood risk by year and emissions scenario."
    ),
    version="0.1.0",
)

# CORS: open during the hackathon; the UI runs on whatever port Vite picks.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(meta.router)
app.include_router(scenarios.router)
app.include_router(lookup.router)
app.include_router(cells.router)
app.include_router(compat.router)

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/", include_in_schema=False)
def root() -> FileResponse:
    if not STATIC_DIR.exists():
        raise RuntimeError("Missing static UI directory at backend/app/static")
    return FileResponse(STATIC_DIR / "index.html")


@app.on_event("startup")
def _startup_log() -> None:
    log.info("RiskLens API booting; serving DuckDB %s", settings.duckdb_path)
    if not settings.duckdb_path.exists():
        log.warning(
            "DUCKDB_PATH does not exist: %s -- run a pipeline first "
            "(e.g. `make build-synth`)", settings.duckdb_path,
        )


__all__ = ["app", "json"]
