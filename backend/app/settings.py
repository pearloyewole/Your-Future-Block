"""Process-wide settings, sourced from environment / .env."""
from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(REPO_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # backend selection
    risklens_db_backend: Literal["duckdb", "postgres"] = "duckdb"

    # duckdb -- which build the API serves. Defaults to synthetic so the
    # server boots even if the real pipeline hasn't run yet. Override with
    # DUCKDB_PATH=backend/data/processed/risklens.real.duckdb to serve real.
    duckdb_path: Path = REPO_ROOT / "backend/data/processed/risklens.synthetic.duckdb"

    # postgres
    database_url: str = "postgresql+psycopg://risklens:risklens@localhost:5432/risklens"

    # cache
    redis_url: str | None = None

    # ROI
    roi_name: str = "la_county"
    roi_bbox: str = "-119.0,33.5,-117.5,34.9"
    h3_resolution: int = 9

    # external APIs
    census_api_key: str | None = None
    census_geocoder_base: str = "https://geocoding.geo.census.gov/geocoder"

    # LLM
    llm_provider: Literal["anthropic", "openai", "none"] = "none"
    anthropic_api_key: str | None = None
    openai_api_key: str | None = None
    llm_model: str = "claude-3-5-sonnet-latest"

    # Cal-Adapt
    caladapt_s3_bucket: str = "cadcat"
    caladapt_s3_prefix: str = "loca2-hybrid"

    log_level: str = "INFO"

    # ----- helpers -----
    @property
    def bbox(self) -> tuple[float, float, float, float]:
        a, b, c, d = (float(x) for x in self.roi_bbox.split(","))
        return a, b, c, d  # min_lon, min_lat, max_lon, max_lat

    @property
    def data_raw(self) -> Path:
        p = REPO_ROOT / "backend/data/raw"
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def data_processed(self) -> Path:
        p = REPO_ROOT / "backend/data/processed"
        p.mkdir(parents=True, exist_ok=True)
        return p


settings = Settings()
