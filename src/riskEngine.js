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

const SCENARIOS = ["ssp245", "ssp370", "ssp585"];
const HAZARDS = ["heat", "wildfire", "flood", "overall"];

export class RiskEngine {
  constructor({ featureCollection, weights }) {
    this.weights = weights;
    this.features = featureCollection.features.map((feature) => ({
      ...feature,
      _centroid: polygonCentroid(feature.geometry.coordinates[0])
    }));
    this.validYears = Object.keys(weights.yearWindows).map((y) => Number(y));
    this.distributions = this.buildDistributions();
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

  scoreForPoint({ lon, lat, year, scenario, state = "CA", propertyType = "homeowner" }) {
    const resolvedYear = this.resolveYear(year);
    const resolvedScenario = this.resolveScenario(scenario);
    const feature = this.resolveCell(lon, lat);

    const scores = this.computeHazardScores({
      feature,
      year: resolvedYear,
      scenario: resolvedScenario
    });
    const scoreValues = {
      heat: scores.heat.score,
      wildfire: scores.wildfire.score,
      flood: scores.flood.score,
      overall: scores.overall.score
    };
    const context = buildCommunityContext(feature.properties);
    const comparison = this.buildComparison({
      scores: scoreValues,
      year: resolvedYear,
      scenario: resolvedScenario
    });
    const communityImpact = buildCommunityImpact({
      scores: scoreValues,
      labels: {
        heat: scores.heat.label,
        wildfire: scores.wildfire.label,
        flood: scores.flood.label
      },
      context
    });
    const riskProfile = classifyRiskProfile(
      scoreValues.heat,
      scoreValues.wildfire,
      scoreValues.flood
    );
    const insuranceGuidance = buildInsuranceGuidance({
      riskProfile,
      state: normalizeState(state),
      propertyType,
      inFemaFloodZone:
        toBoolean(feature.properties.in_fema_flood_zone) || scoreValues.flood >= 60,
      inFireHazardZone:
        toBoolean(feature.properties.in_fire_hazard_zone) ||
        toNumber(feature.properties.wildfire_baseline) >= 35
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
      comparison,
      community_impact: communityImpact,
      insurance_guidance: insuranceGuidance,
      explanation: buildExplanation({
        neighborhood: feature.properties.neighborhood,
        topHazard: communityImpact.main_concern,
        likelyDisruptions: communityImpact.likely_disruptions
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

  buildDistributions() {
    const distributions = {};

    for (const year of this.validYears) {
      const yearKey = String(year);
      distributions[yearKey] = {};
      for (const scenario of SCENARIOS) {
        const values = {
          heat: [],
          wildfire: [],
          flood: [],
          overall: []
        };

        for (const feature of this.features) {
          const scores = this.computeHazardScores({ feature, year, scenario });
          values.heat.push(scores.heat.score);
          values.wildfire.push(scores.wildfire.score);
          values.flood.push(scores.flood.score);
          values.overall.push(scores.overall.score);
        }

        distributions[yearKey][scenario] = {};
        for (const hazard of HAZARDS) {
          const sorted = [...values[hazard]].sort((a, b) => a - b);
          distributions[yearKey][scenario][hazard] = {
            min: round1(percentile(sorted, 0)),
            p5: round1(percentile(sorted, 5)),
            median: round1(percentile(sorted, 50)),
            p95: round1(percentile(sorted, 95)),
            max: round1(percentile(sorted, 100)),
            values: sorted
          };
        }
      }
    }

    return distributions;
  }

  buildComparison({ scores, year, scenario }) {
    const dist = this.distributions[String(year)][scenario];
    return {
      heat: comparisonForHazard(scores.heat, dist.heat),
      wildfire: comparisonForHazard(scores.wildfire, dist.wildfire),
      flood: comparisonForHazard(scores.flood, dist.flood)
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

function buildExplanation({ neighborhood, topHazard, likelyDisruptions }) {
  const disruptionSnippet = likelyDisruptions.slice(0, 2).join(", ");
  return `In ${neighborhood}, the main climate concern is ${topHazard.toLowerCase()}. Expected disruptions include ${disruptionSnippet}. This is a screening-level estimate, not a house-specific prediction.`;
}

function comparisonForHazard(selectedScore, distribution) {
  return {
    la_median: distribution.median,
    lowest_risk_benchmark: distribution.p5,
    percent_above_median: percentDifference(selectedScore, distribution.median),
    percentile: percentileRank(selectedScore, distribution.values)
  };
}

function percentile(values, p) {
  if (!values.length) return 0;
  if (p <= 0) return values[0];
  if (p >= 100) return values[values.length - 1];

  const index = ((values.length - 1) * p) / 100;
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  const weight = index - lower;
  return values[lower] * (1 - weight) + values[upper] * weight;
}

function percentileRank(selectedScore, allScores) {
  if (!allScores.length) return 0;
  const belowOrEqual = allScores.filter((score) => score <= selectedScore).length;
  return Math.round((belowOrEqual / allScores.length) * 100);
}

function percentDifference(selectedScore, benchmarkScore) {
  if (!benchmarkScore) return null;
  return Math.round(((selectedScore - benchmarkScore) / benchmarkScore) * 100);
}

function buildCommunityContext(properties) {
  const socialVulnerability = toNumber(properties.social_vulnerability, 50);
  const treeCanopy = toNumber(properties.tree_canopy_pct, 15);
  const impervious = toNumber(properties.impervious_pct, 60);

  return {
    social_vulnerability_score: socialVulnerability,
    tree_canopy_pct: treeCanopy,
    impervious_pct: impervious,
    elderly_pct: toNumber(
      properties.elderly_pct,
      clamp0to100(6 + socialVulnerability * 0.12)
    ),
    children_pct: toNumber(
      properties.children_pct,
      clamp0to100(12 + socialVulnerability * 0.14)
    ),
    no_vehicle_pct: toNumber(
      properties.no_vehicle_pct,
      clamp0to100(4 + socialVulnerability * 0.12)
    ),
    low_income_pct: toNumber(
      properties.low_income_pct,
      clamp0to100(12 + socialVulnerability * 0.42)
    ),
    outdoor_worker_pct: toNumber(
      properties.outdoor_worker_pct,
      clamp0to100(4 + (100 - treeCanopy) * 0.06)
    ),
    distance_to_hospital_miles: toNumber(
      properties.distance_to_hospital_miles,
      round1(0.8 + socialVulnerability * 0.025)
    ),
    distance_to_cooling_center_miles: toNumber(
      properties.distance_to_cooling_center_miles,
      round1(0.7 + impervious * 0.012)
    )
  };
}

function buildCommunityImpact({ scores, labels, context }) {
  const topHazard = [
    { id: "heat", score: scores.heat },
    { id: "wildfire", score: scores.wildfire },
    { id: "flood", score: scores.flood }
  ].sort((a, b) => b.score - a.score)[0].id;

  if (topHazard === "heat") {
    return buildHeatImpact(scores.heat, labels.heat, context);
  }
  if (topHazard === "wildfire") {
    return buildWildfireImpact(scores.wildfire, labels.wildfire, context);
  }
  return buildFloodImpact(scores.flood, labels.flood, context);
}

function buildHeatImpact(score, label, context) {
  const disruptions = [];
  const groups = [];

  if (score >= 60) {
    disruptions.push(
      "more dangerous heat days",
      "higher cooling costs",
      "greater stress on the electric grid",
      "hotter bus stops, sidewalks, and outdoor work areas"
    );
  }
  if (context.tree_canopy_pct < 15) {
    disruptions.push("less shade and stronger urban heat island effects");
  }
  if (context.impervious_pct > 60) {
    disruptions.push("more heat retained by pavement and buildings");
  }

  if (context.elderly_pct > 12) groups.push("older adults");
  if (context.children_pct > 20) groups.push("children");
  if (context.low_income_pct > 35) groups.push("lower-income households");
  if (context.outdoor_worker_pct > 7) groups.push("outdoor workers");
  if (context.no_vehicle_pct > 10) groups.push("people without reliable cars");

  return {
    main_concern: "Extreme heat",
    hazard_score: round1(score),
    hazard_label: label,
    what_this_means:
      "Extreme heat is the main concern here. Residents may face more dangerous heat days, higher cooling costs, and stronger stress on outdoor workers, older adults, transit riders, and households without reliable cooling.",
    likely_disruptions: uniqueList(disruptions),
    vulnerable_groups: uniqueList(groups)
  };
}

function buildWildfireImpact(score, label, context) {
  const disruptions = [
    "smoke exposure and poor air-quality days",
    "evacuation warnings",
    "road closures",
    "power shutoffs",
    "insurance availability constraints",
    "school or work disruptions"
  ];

  if (score < 60) {
    disruptions.splice(1, 2);
  }

  const groups = [
    "people with asthma or heart conditions",
    "older adults",
    "children",
    "outdoor workers"
  ];
  if (context.no_vehicle_pct > 10) groups.push("people without cars");
  if (context.low_income_pct > 35) {
    groups.push("households with limited ability to relocate temporarily");
  }

  return {
    main_concern: "Wildfire and smoke",
    hazard_score: round1(score),
    hazard_label: label,
    what_this_means:
      "Wildfire-related disruption can affect this area even if flames do not directly reach the block. Smoke, evacuation warnings, closures, and power interruptions can still affect daily life.",
    likely_disruptions: uniqueList(disruptions),
    vulnerable_groups: uniqueList(groups)
  };
}

function buildFloodImpact(score, label, context) {
  const disruptions = [
    "road closures",
    "ground-floor property damage",
    "basement or garage flooding",
    "mold risk after storms",
    "transit delays",
    "storm drain overflow",
    "business interruption"
  ];

  if (score < 40) {
    disruptions.splice(0, 2);
  }

  const groups = [
    "renters",
    "small businesses",
    "people without cars",
    "older adults",
    "households with limited savings for repairs"
  ];
  if (context.low_income_pct > 35) {
    groups.push("lower-income households");
  }

  return {
    main_concern: "Flooding",
    hazard_score: round1(score),
    hazard_label: label,
    what_this_means:
      "This area may experience flooding disruption during heavy rainfall or coastal events. Everyday impacts can include road closures, transit delays, property damage, and cleanup costs.",
    likely_disruptions: uniqueList(disruptions),
    vulnerable_groups: uniqueList(groups)
  };
}

function classifyRiskProfile(heat, wildfire, flood) {
  if (flood >= 70 && wildfire < 60) return "flood_heavy";
  if (wildfire >= 70 && flood < 60) return "wildfire_heavy";
  if (flood >= 60 && wildfire >= 60) return "flood_and_wildfire_heavy";
  if (heat >= 70 && flood < 60 && wildfire < 60) return "heat_heavy";
  return "mixed_or_moderate";
}

function buildInsuranceGuidance({
  riskProfile,
  state,
  propertyType,
  inFemaFloodZone,
  inFireHazardZone
}) {
  const coverageSections = [];
  const coverageToAsk = [];

  if (
    inFemaFloodZone ||
    ["flood_heavy", "flood_and_wildfire_heavy"].includes(riskProfile)
  ) {
    const items = [
      "National Flood Insurance Program coverage",
      "Private flood insurance options",
      "Building coverage and contents coverage"
    ];
    if (propertyType === "renter") {
      items.push("Separate flood coverage for renters' belongings");
    }
    coverageSections.push({
      title: "Flood coverage to ask about",
      items
    });
    coverageToAsk.push(
      "NFIP flood insurance",
      "private flood insurance",
      "building and contents flood coverage"
    );
  }

  if (
    state === "CA" &&
    (inFireHazardZone ||
      ["wildfire_heavy", "flood_and_wildfire_heavy"].includes(riskProfile))
  ) {
    coverageSections.push({
      title: "Wildfire / fire coverage to ask about",
      items: [
        "Standard homeowners or renters insurance fire coverage",
        "California FAIR Plan if traditional coverage is unavailable",
        "Difference in Conditions policy to complement FAIR Plan where needed"
      ]
    });
    coverageToAsk.push(
      "standard homeowners/renters fire coverage",
      "California FAIR Plan fallback",
      "Difference in Conditions (DIC) policy"
    );
  }

  if (riskProfile === "heat_heavy") {
    coverageSections.push({
      title: "Heat-related resilience costs to ask about",
      items: [
        "Equipment-breakdown riders for HVAC systems",
        "Temporary loss-of-use coverage terms during extreme heat outages"
      ]
    });
  }

  return {
    risk_profile: riskProfile,
    state,
    property_type: propertyType,
    coverage_sections: coverageSections,
    coverage_to_ask_about: uniqueList(coverageToAsk),
    disclaimer:
      "Educational only. Not personal financial, legal, or insurance advice. Coverage availability, pricing, exclusions, and requirements vary by property and insurer. Compare policies with a licensed insurance agent."
  };
}

function normalizeState(state) {
  const normalized = String(state ?? "").toUpperCase().trim();
  if (!normalized) return "CA";
  if (normalized === "06") return "CA";
  return normalized;
}

function toNumber(value, fallback = 0) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return parsed;
}

function toBoolean(value) {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value > 0;
  if (typeof value === "string") {
    const normalized = value.toLowerCase().trim();
    return normalized === "true" || normalized === "1" || normalized === "yes";
  }
  return false;
}

function uniqueList(items) {
  return [...new Set(items)];
}
