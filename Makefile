# RiskLens Makefile -- thin wrapper around the two pipelines and the API.
# Both pipelines write to distinct DuckDB files; the API serves whichever
# one DUCKDB_PATH points at.

PY     ?= python
SYNTH  := backend/data/processed/risklens.synthetic.duckdb
REAL   := backend/data/processed/risklens.real.duckdb

.PHONY: help
help: ## list available targets
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ---------- synthetic (offline, deterministic, demo-safe) ----------
.PHONY: build-synth
build-synth: ## build the synthetic DuckDB (no network, ~10s)
	$(PY) -m pipelines.synthetic.build

.PHONY: api-synth
api-synth: ## serve API against synthetic DB on :8000
	cd backend && DUCKDB_PATH=$(abspath $(SYNTH)) \
		uvicorn app.api:app --reload --port 8000

# ---------- real (network-heavy, slow) ----------
.PHONY: build-real
build-real: ## run the real pipeline end-to-end (slow, requires downloads)
	cd backend && $(PY) -m pipelines.real.00_grid && \
	             $(PY) -m pipelines.real.10_climate --source nex-gddp && \
	             $(PY) -m pipelines.real.11_calfire_fhsz && \
	             $(PY) -m pipelines.real.13_fema_nfhl && \
	             $(PY) -m pipelines.real.17_cdc_svi && \
	             $(PY) -m pipelines.real.19_fema_nri && \
	             $(PY) -m pipelines.real.90_compute_scores

.PHONY: api-real
api-real: ## serve API against real DB on :8001
	cd backend && DUCKDB_PATH=$(abspath $(REAL)) \
		uvicorn app.api:app --reload --port 8001

# ---------- both at once ----------
.PHONY: api-both
api-both: ## run both APIs side-by-side (synthetic on :8000, real on :8001)
	$(MAKE) -j 2 api-synth api-real

# ---------- housekeeping ----------
.PHONY: clean-synth
clean-synth: ## delete the synthetic DB
	rm -f $(SYNTH) $(SYNTH).wal

.PHONY: clean-real
clean-real: ## delete the real DB
	rm -f $(REAL) $(REAL).wal

.PHONY: clean-all
clean-all: clean-synth clean-real ## delete both DBs
	rm -rf backend/data/processed/*.parquet

.PHONY: install
install: ## install python deps (editable)
	$(PY) -m pip install -e .[dev]

.PHONY: lint
lint: ## ruff lint
	ruff check backend
