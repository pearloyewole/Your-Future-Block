-- RiskLens canonical schema. Source of truth for both pipelines.
-- Compatible with PostGIS (Postgres 16 + postgis 3) and DuckDB (with the spatial extension).
-- Statements are separated by the line-marker below so the Python loader can rewrite each
-- one independently for the target backend.

-- ###
CREATE TABLE IF NOT EXISTS tracts (
  tract_fips    VARCHAR PRIMARY KEY,
  state_fips    VARCHAR,
  county_fips   VARCHAR,
  name          VARCHAR,
  aland         DOUBLE,
  awater        DOUBLE,
  geom          GEOMETRY
);

-- ###
CREATE TABLE IF NOT EXISTS cells (
  cell_id       VARCHAR PRIMARY KEY,
  centroid_lat  DOUBLE,
  centroid_lon  DOUBLE,
  tract_fips    VARCHAR,
  geom          GEOMETRY
);

-- ###
CREATE INDEX IF NOT EXISTS cells_tract_idx ON cells (tract_fips);

-- ###
CREATE TABLE IF NOT EXISTS cell_attrs (
  cell_id              VARCHAR PRIMARY KEY,
  -- terrain
  elevation_m          DOUBLE,
  slope_deg            DOUBLE,
  -- land cover
  impervious_pct       DOUBLE,
  tree_canopy_pct      DOUBLE,
  -- wildfire baseline
  fhsz_class           VARCHAR,
  wui_class            VARCHAR,
  dist_to_fhsz_vh_m    DOUBLE,
  fires_5km_30yr       INTEGER,
  -- flood baseline
  flood_zone           VARCHAR,
  in_100yr             BOOLEAN,
  in_500yr             BOOLEAN,
  slr_inundated_ft     DOUBLE,
  dist_to_coast_m      DOUBLE,
  -- vulnerability (from tract)
  svi_overall          DOUBLE,
  pct_age_65plus       DOUBLE,
  pct_no_vehicle       DOUBLE,
  pct_below_poverty    DOUBLE,
  pct_disability       DOUBLE,
  median_income        DOUBLE,
  -- FEMA NRI (tract-level expected annual loss + resilience)
  nri_heat_eal         DOUBLE,
  nri_wildfire_eal     DOUBLE,
  nri_riverine_eal     DOUBLE,
  nri_coastal_eal      DOUBLE,
  community_resilience DOUBLE,
  -- daytime exposure (stretch)
  daytime_workers      INTEGER,
  transit_stops_400m   INTEGER
);

-- ###
CREATE TABLE IF NOT EXISTS cell_climate (
  cell_id        VARCHAR,
  window_label   VARCHAR,            -- '1981-2010' | '2021-2040' | '2041-2060' | '2071-2090' | '2081-2100'
  scenario       VARCHAR,            -- 'historical' | 'ssp245' | 'ssp370' | 'ssp585'
  heat_days      DOUBLE,             -- mean annual count tasmax > 35C
  warm_nights    DOUBLE,             -- mean annual count tasmin > 20C
  pr_annual_mm   DOUBLE,
  pr_p99_mm      DOUBLE,
  cdd_max        DOUBLE,             -- mean annual longest run of consecutive dry days
  PRIMARY KEY (cell_id, window_label, scenario)
);

-- ###
CREATE TABLE IF NOT EXISTS risk_cells (
  cell_id          VARCHAR,
  window_label     VARCHAR,
  scenario         VARCHAR,
  heat_score       DOUBLE,
  wildfire_score   DOUBLE,
  flood_score      DOUBLE,
  overall_score    DOUBLE,
  heat_label       VARCHAR,
  wildfire_label   VARCHAR,
  flood_label      VARCHAR,
  overall_label    VARCHAR,
  drivers          VARCHAR,                -- JSON-encoded snapshot of driver values for the LLM
  computed_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (cell_id, window_label, scenario)
);

-- ###
CREATE INDEX IF NOT EXISTS risk_cells_lookup ON risk_cells (cell_id, window_label, scenario);

-- ###
-- Provenance: written once at the end of each pipeline build so the API
-- (and the demo) can answer "which build is this DB?" without guessing.
CREATE TABLE IF NOT EXISTS provenance (
  pipeline      VARCHAR PRIMARY KEY,    -- 'synthetic' | 'real'
  built_at      TIMESTAMP,
  git_sha       VARCHAR,
  cell_count    INTEGER,
  tract_count   INTEGER,
  windows       VARCHAR,                -- JSON array of window labels included
  scenarios     VARCHAR,                -- JSON array of scenarios included
  notes         VARCHAR
);
