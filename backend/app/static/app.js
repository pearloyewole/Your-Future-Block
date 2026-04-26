const yearSlider = document.querySelector("#year-slider");
const yearLabel = document.querySelector("#year-label");
const scenarioSelect = document.querySelector("#scenario");
const hazardSelect = document.querySelector("#hazard");
const addressInput = document.querySelector("#address");
const runBtn = document.querySelector("#run-btn");
const statusEl = document.querySelector("#status");

const heatScoreEl = document.querySelector("#heat-score");
const wildfireScoreEl = document.querySelector("#wildfire-score");
const floodScoreEl = document.querySelector("#flood-score");
const overallScoreEl = document.querySelector("#overall-score");
const metaEl = document.querySelector("#meta-line");
const explanationEl = document.querySelector("#explanation");

let map;
let mapLayer;
let marker;
let config;
let selectedCellId = null;

const SELECTED_BLOCK_OUTLINE = "#2563eb";
const DEFAULT_BLOCK_OUTLINE = "#35564f";

setupMap();
await loadConfig();
setYearLabel();
await runAnalysis();

runBtn.addEventListener("click", async () => {
  await runAnalysis();
});

yearSlider.addEventListener("input", () => {
  setYearLabel();
});

yearSlider.addEventListener("change", async () => {
  await runAnalysis();
});

scenarioSelect.addEventListener("change", async () => {
  await runAnalysis();
});

hazardSelect.addEventListener("change", async () => {
  await refreshMapCells(selectedCellId);
});

addressInput.addEventListener("keydown", async (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    await runAnalysis();
  }
});

function setupMap() {
  map = L.map("map", { zoomControl: true }).setView([34.0522, -118.2437], 10);
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
    attribution: "&copy; OpenStreetMap contributors"
  }).addTo(map);
}

async function loadConfig() {
  const response = await fetch("/api/config");
  const payload = await response.json();
  if (!response.ok) throw new Error(payload?.detail || "Could not load config.");
  config = payload;
}

function selectedYear() {
  const years = config?.years ?? [2030, 2050, 2080, 2100];
  const index = Number(yearSlider.value);
  return years[index] ?? 2050;
}

function selectedScenario() {
  return scenarioSelect.value;
}

function selectedHazard() {
  return hazardSelect.value;
}

function setYearLabel() {
  const year = selectedYear();
  const windowRange = config?.yearWindows?.[String(year)] ?? "";
  yearLabel.textContent = `${year} (${windowRange})`;
}

async function runAnalysis() {
  const address = addressInput.value.trim();
  if (!address) return;

  try {
    setStatus("Analyzing address...");
    const payload = {
      address,
      year: selectedYear(),
      scenario: selectedScenario()
    };

    const response = await fetch("/api/geocode-risk", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload)
    });
    const data = await response.json();
    if (!response.ok) throw new Error(data.detail || "Analysis failed.");

    selectedCellId = data.risk.cell_id ?? null;
    renderResult(data);
    await refreshMapCells(selectedCellId);
    setStatus("Done.");
  } catch (error) {
    setStatus(error.message || "Could not run analysis.");
  }
}

async function refreshMapCells(activeCellId = null) {
  try {
    if (activeCellId) {
      selectedCellId = activeCellId;
    }
    const year = selectedYear();
    const scenario = selectedScenario();
    const hazard = selectedHazard();
    const bbox = map.getBounds();
    const params = new URLSearchParams({
      year: String(year),
      scenario,
      hazard,
      min_lon: String(bbox.getWest()),
      min_lat: String(bbox.getSouth()),
      max_lon: String(bbox.getEast()),
      max_lat: String(bbox.getNorth())
    });
    const response = await fetch(`/api/map-cells?${params.toString()}`);
    const layerData = await response.json();
    if (!response.ok) throw new Error(layerData.detail || "Map layer failed.");

    if (mapLayer) {
      map.removeLayer(mapLayer);
    }

    mapLayer = L.geoJSON(layerData, {
      pointToLayer: (feature, latlng) => {
        return L.circleMarker(latlng, markerStyle(feature));
      },
      onEachFeature: (feature, layer) => {
        const p = feature.properties;
        layer.bindPopup(
          `<strong>${p.neighborhood || "Cell"}</strong><br/>Score: ${p.score} (${p.label})`
        );
        layer.on("click", () => {
          selectedCellId = p.cell_id;
          applySelectedBlockStyle();
        });
      }
    }).addTo(map);
    applySelectedBlockStyle();
  } catch (error) {
    setStatus(error.message || "Could not refresh map.");
  }
}

function renderResult(data) {
  const { geocoded, risk } = data;

  heatScoreEl.textContent = formatScore(risk.scores.heat);
  wildfireScoreEl.textContent = formatScore(risk.scores.wildfire);
  floodScoreEl.textContent = formatScore(risk.scores.flood);
  overallScoreEl.textContent = formatScore(risk.scores.overall);

  metaEl.textContent = `${risk.neighborhood} | Tract ${risk.tract_fips || "unknown"} | ${risk.year_window} | ${risk.scenario_display}`;
  explanationEl.textContent = risk.explanation || "Explanation unavailable.";

  if (marker) map.removeLayer(marker);
  marker = L.marker([geocoded.lat, geocoded.lon]).addTo(map);
  marker.bindPopup(geocoded.matched_address).openPopup();
  map.setView([geocoded.lat, geocoded.lon], 12, { animate: true });
}

function formatScore(value) {
  return `${value.score} (${value.label})`;
}

function setStatus(text) {
  statusEl.textContent = text;
}

function scoreColor(score) {
  if (score <= 20) return "#2a8f5d";
  if (score <= 40) return "#73b767";
  if (score <= 60) return "#dfc84f";
  if (score <= 80) return "#de8a42";
  return "#ce4a3c";
}

function markerStyle(feature) {
  const score = feature.properties.score;
  const isSelected = Boolean(selectedCellId) && feature.properties.cell_id === selectedCellId;
  return {
    radius: isSelected ? 8 : 5,
    color: isSelected ? SELECTED_BLOCK_OUTLINE : DEFAULT_BLOCK_OUTLINE,
    weight: isSelected ? 3 : 1,
    fillOpacity: isSelected ? 0.86 : 0.72,
    fillColor: scoreColor(score)
  };
}

function applySelectedBlockStyle() {
  if (!mapLayer) return;
  mapLayer.eachLayer((layer) => {
    if (!layer.feature || typeof layer.setStyle !== "function") return;
    layer.setStyle(markerStyle(layer.feature));
    if (layer.feature.properties.cell_id === selectedCellId && typeof layer.bringToFront === "function") {
      layer.bringToFront();
    }
  });
}
