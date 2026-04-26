import {
  clamp0to100,
  floodLocalPhysical,
  localHeatAmplifier,
  normalizeScenario,
  normalizeYear,
  round1,
  scoreLabel
} from "./scoring.js";
import { haversineMeters, pointInPolygon, polygonCentroid } from "./geo.js";

export class RiskEngine {
  constructor({ featureCollection, weights }) {
    this.weights = weights;
    this.features = featureCollection.features.map((feature) => ({
      ...feature,
      _centroid: polygonCentroid(feature.geometry.coordinates[0])
    }));
    this.validYears = Object.keys(weights.yearWindows).map((y) => Number(y));
  }

  getConfig() {
    return {
      years: this.validYears,
      yearWindows: this.weights.yearWindows,
      scenarios: [
        { label: "Moderate Warming", value: "ssp245", display: "SSP2-4.5" },
        { label: "High Warming", value: "ssp370", display: "SSP3-7.0" },
        { label: "Very High Warming", value: "ssp585", display: "SSP5-8.5" }
      ],
      scoreLabels: [
        { min: 0, max: 20, label: "Very Low" },
        { min: 21, max: 40, label: "Low" },
        { min: 41, max: 60, label: "Moderate" },
        { min: 61, max: 80, label: "High" },
        { min: 81, max: 100, label: "Very High" }
      ]
    };
  }

  resolveScenario(inputScenario) {
    return normalizeScenario(inputScenario, this.weights.scenarioAliases);
  }

  resolveYear(inputYear) {
    return normalizeYear(inputYear, this.validYears);
  }

  resolveCell(lon, lat) {
    const point = [lon, lat];
    const containing = this.features.find((feature) =>
      pointInPolygon(point, feature.geometry.coordinates[0])
    );
    if (containing) return containing;

    let nearest = this.features[0];
    let bestDistance = Infinity;
    for (const feature of this.features) {
      const distance = haversineMeters(point, feature._centroid);
      if (distance < bestDistance) {
        bestDistance = distance;
        nearest = feature;
      }
    }
    return nearest;
  }

  scoreForPoint({ lon, lat, year, scenario }) {
    const resolvedYear = this.resolveYear(year);
    const resolvedScenario = this.resolveScenario(scenario);
    const feature = this.resolveCell(lon, lat);

    const scores = this.computeHazardScores({
      feature,
      year: resolvedYear,
      scenario: resolvedScenario
    });

    return {
      year: resolvedYear,
      year_window: this.weights.yearWindows[String(resolvedYear)],
      scenario: resolvedScenario,
      scenario_display: scenarioDisplay(resolvedScenario),
      cell_id: feature.properties.cell_id,
      neighborhood: feature.properties.neighborhood,
      tract_fips: feature.properties.tract_fips,
      coordinates: { lat, lon },
      modifiers: {
        tree_canopy_pct: feature.properties.tree_canopy_pct,
        impervious_pct: feature.properties.impervious_pct,
        social_vulnerability: feature.properties.social_vulnerability,
        resilience_idx: feature.properties.resilience_idx
      },
      scores,
      explanation: buildExplanation({
        neighborhood: feature.properties.neighborhood,
        year: resolvedYear,
        scenario: scenarioDisplay(resolvedScenario),
        ...scores,
        treeCanopy: feature.properties.tree_canopy_pct,
        impervious: feature.properties.impervious_pct,
        socialVulnerability: feature.properties.social_vulnerability
      })
    };
  }

  computeHazardScores({ feature, year, scenario }) {
    const properties = feature.properties;
    const heatW = this.weights.hazardSpecific.heat;
    const wildfireW = this.weights.hazardSpecific.wildfire;
    const floodW = this.weights.hazardSpecific.flood;

    const projectedHeat = properties.heat_projection[String(year)][scenario];
    const heatAmplifier = localHeatAmplifier({
      imperviousPct: properties.impervious_pct,
      treeCanopyPct: properties.tree_canopy_pct,
      buildingDensityIdx: properties.building_density_idx
    });
    const heatScore = clamp0to100(
      projectedHeat * heatW.futureExposure +
        heatAmplifier * heatW.localAmplifier +
        properties.social_vulnerability * heatW.socialVulnerability
    );

    const wildfireFuture =
      properties.wildfire_climate_modifier[String(year)][scenario];
    const wildfireScore = clamp0to100(
      properties.wildfire_baseline * wildfireW.baseline +
        wildfireFuture * wildfireW.futureStress +
        properties.slope_idx * wildfireW.terrain +
        properties.social_vulnerability * wildfireW.socialVulnerability
    );

    const coastal = properties.coastal_flood_projection[String(year)][scenario];
    const inland = properties.inland_flood_projection[String(year)][scenario];
    const floodPhysical = floodLocalPhysical({
      drainageStressIdx: properties.drainage_stress_idx,
      imperviousPct: properties.impervious_pct
    });
    const floodScore = clamp0to100(
      Math.max(coastal, inland) * floodW.coastalOrInland +
        floodPhysical * floodW.localPhysical +
        properties.social_vulnerability * floodW.socialVulnerability
    );

    const overallW = this.weights.overall;
    const rawOverall =
      heatScore * overallW.heat +
      wildfireScore * overallW.wildfire +
      floodScore * overallW.flood;
    const overallScore = clamp0to100(
      rawOverall - properties.resilience_idx * 0.05
    );

    return {
      heat: withLabel(heatScore),
      wildfire: withLabel(wildfireScore),
      flood: withLabel(floodScore),
      overall: withLabel(overallScore)
    };
  }

  mapCells({ year, scenario, hazard }) {
    const resolvedYear = this.resolveYear(year);
    const resolvedScenario = this.resolveScenario(scenario);
    const hazardKey = normalizeHazard(hazard);

    return {
      type: "FeatureCollection",
      features: this.features.map((feature) => {
        const scores = this.computeHazardScores({
          feature,
          year: resolvedYear,
          scenario: resolvedScenario
        });
        const selected = scores[hazardKey];
        return {
          type: "Feature",
          geometry: feature.geometry,
          properties: {
            cell_id: feature.properties.cell_id,
            neighborhood: feature.properties.neighborhood,
            tract_fips: feature.properties.tract_fips,
            hazard: hazardKey,
            score: selected.score,
            label: selected.label
          }
        };
      })
    };
  }
}

function withLabel(score) {
  const rounded = round1(score);
  return {
    score: rounded,
    label: scoreLabel(rounded)
  };
}

function normalizeHazard(hazard) {
  const value = String(hazard ?? "overall").toLowerCase();
  if (value === "combined") return "overall";
  if (["heat", "wildfire", "flood", "overall"].includes(value)) return value;
  throw new Error(
    `Unsupported hazard "${hazard}". Use heat, flood, wildfire, or combined.`
  );
}

function scenarioDisplay(scenario) {
  if (scenario === "ssp245") return "SSP2-4.5";
  if (scenario === "ssp370") return "SSP3-7.0";
  if (scenario === "ssp585") return "SSP5-8.5";
  return scenario;
}

function buildExplanation({
  neighborhood,
  year,
  scenario,
  heat,
  wildfire,
  flood,
  overall,
  treeCanopy,
  impervious,
  socialVulnerability
}) {
  const topHazard = [
    { name: "heat", value: heat.score },
    { name: "wildfire", value: wildfire.score },
    { name: "flood", value: flood.score }
  ].sort((a, b) => b.value - a.value)[0];

  return `In ${neighborhood}, the leading projected climate exposure by ${year} under ${scenario} is ${topHazard.name}. Heat is influenced by ${impervious}% impervious cover and ${treeCanopy}% tree canopy, while social vulnerability (${socialVulnerability}/100) can increase impact severity during extreme events. This screening-level result combines hazard projections and local modifiers; it is not a house-specific prediction. Overall exposure is ${overall.score}/100 (${overall.label}).`;
}
