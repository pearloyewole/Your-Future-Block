# RiskLens LA - Dual Backend Tracks

This branch keeps two backend tracks running in parallel:

- **Track A (Node MVP foundation):** small REST API for `/api/geocode`, `/api/risk`, `/api/config`, `/api/map-cells` (plus `/api/geocode-risk` and `/api/health`)
- **Track B (FastAPI + real-data pipeline):** collaborator architecture with DuckDB and real-data pipeline work

## Track A: Node MVP API (default for `la-risk-explorer-main`)

1. Start Node server (port `8787`):
   - `npm start`
2. Start frontend (port `8080`):
   - `cd la-risk-explorer-main && npm run dev`
3. Open:
   - `http://127.0.0.1:8080/atlas`

`la-risk-explorer-main` dev proxy points `/api` to `http://127.0.0.1:8787` by default.  
To override proxy target:

- `VITE_API_PROXY_TARGET=http://127.0.0.1:8000 npm run dev`

## Track B: FastAPI / real-data path (kept intact)

- Existing FastAPI app and real-data pipeline files remain in `backend/`.
- Run FastAPI path as before (for example `make api-synth`) on `127.0.0.1:8000`.
- This track can continue advancing independently while frontend demos use the Node path.
