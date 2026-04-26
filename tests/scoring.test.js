import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  localHeatAmplifier,
  normalizeScenario,
  normalizeYear,
  scoreLabel
} from "../src/scoring.js";
import { RiskEngine } from "../src/riskEngine.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");
const weights = JSON.parse(
  readFileSync(path.join(root, "config/weights.json"), "utf8")
);
const cells = JSON.parse(
  readFileSync(path.join(root, "data/risk_cells.geojson"), "utf8")
);

test("normalizeScenario resolves aliases", () => {
  assert.equal(normalizeScenario("SSP3-7.0", weights.scenarioAliases), "ssp370");
  assert.equal(normalizeScenario("moderate", weights.scenarioAliases), "ssp245");
  assert.equal(normalizeScenario("veryhigh", weights.scenarioAliases), "ssp585");
});

test("normalizeYear rejects unsupported values", () => {
  assert.throws(() => normalizeYear(2040, [2030, 2050]), /Unsupported year/);
});

test("scoreLabel bins values into expected labels", () => {
  assert.equal(scoreLabel(9), "Very Low");
  assert.equal(scoreLabel(33), "Low");
  assert.equal(scoreLabel(58), "Moderate");
  assert.equal(scoreLabel(77), "High");
  assert.equal(scoreLabel(99), "Very High");
});

test("localHeatAmplifier increases with imperviousness and lower canopy", () => {
  const cooler = localHeatAmplifier({
    imperviousPct: 30,
    treeCanopyPct: 40,
    buildingDensityIdx: 30
  });
  const hotter = localHeatAmplifier({
    imperviousPct: 80,
    treeCanopyPct: 8,
    buildingDensityIdx: 80
  });
  assert.ok(hotter > cooler);
});

test("risk engine returns bounded scores and labels", () => {
  const engine = new RiskEngine({ featureCollection: cells, weights });
  const result = engine.scoreForPoint({
    lon: -118.2437,
    lat: 34.0522,
    year: 2050,
    scenario: "ssp370"
  });

  for (const hazard of ["heat", "wildfire", "flood", "overall"]) {
    const value = result.scores[hazard].score;
    assert.ok(value >= 0 && value <= 100);
    assert.equal(typeof result.scores[hazard].label, "string");
  }
});
