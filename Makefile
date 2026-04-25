# RiskLens convenience targets.
# All paths are relative to the repo root. Run from there.
#
#   make build-synth     # build the synthetic safety-net DB (offline, ~seconds)
#   make build-real      # build the real-data DB (network, slow, can fail)
#   make api-synth       # serve the synthetic DB on :8000
#   make api-real        # serve the real DB on :8001
#   make api-both        # run both APIs in parallel
#   make clean-synth     # delete the synthetic DB
#   make clean-real      # delete the real DB

PY            ?= python
UVICORN       ?= uvicorn

PROCESSED_DIR := backend/data/processed
SYNTH         := $(PROCESSED_DIR)/risklens.synthetic.duckdb
REAL          := $(PROCESSED_DIR)/risklens.real.duckdb

# Pydantic-settings reads env vars; we set them inline so each target is self-contained.
.PHONY: help build-synth build-real api-synth api-real api-both \
        clean-synth clean-real clean test lint

help:
	@echo "RiskLens targets:"
	@echo "  build-synth    build $(SYNTH)"
	@echo "  build-real     build $(REAL)"
	@echo "  api-synth      serve synthetic DB on :8000"
	@echo "  api-real       serve real DB on :8001"
	@echo "  api-both       run both APIs side-by-side"
	@echo "  clean-synth    rm $(SYNTH)"
	@echo "  clean-real     rm $(REAL)"
	@echo "  clean          rm both DuckDB files"
	@echo "  test           pytest backend/tests"
	@echo "  lint           ruff check ."

# ----- builds ---------------------------------------------------------------
build-synth:
	cd backend && $(PY) -m pipelines.synthetic.build

build-real:
	cd backend && $(PY) -m pipelines.real.build

# ----- API ------------------------------------------------------------------
api-synth:
	DUCKDB_PATH=$(SYNTH) $(UVICORN) backend.app.api:app --reload --port 8000

api-real:
	DUCKDB_PATH=$(REAL) $(UVICORN) backend.app.api:app --reload --port 8001

api-both:
	@echo "Starting both APIs. Synthetic on :8000, real on :8001."
	@$(MAKE) -j 2 api-synth api-real

# ----- cleanup --------------------------------------------------------------
clean-synth:
	rm -f $(SYNTH) $(SYNTH).wal

clean-real:
	rm -f $(REAL) $(REAL).wal

clean: clean-synth clean-real

# ----- dev ------------------------------------------------------------------
test:
	pytest backend/tests

lint:
	ruff check backend
