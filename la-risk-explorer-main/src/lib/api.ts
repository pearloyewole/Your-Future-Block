import type { RiskMapPoint } from "@/components/atlas/LAMap";

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? "";

export type Scenario = "ssp245" | "ssp370" | "ssp585";
export type Year = 2030 | 2050 | 2080 | 2100;
export type HazardLayer = "heat" | "wildfire" | "flood";
export type RiskMapCellGeometry =
  | { type: "Point"; coordinates: [number, number] }
  | { type: "Polygon"; coordinates: [Array<[number, number]>] }
  | { type: "MultiPolygon"; coordinates: Array<Array<Array<[number, number]>>> };

export interface GeocodeResult {
  source: string;
  input_address: string;
  matched_address: string | null;
  lat: number;
  lon: number;
  tract_fips: string | null;
  county_fips: string | null;
  state_fips: string | null;
}

export interface RiskSummary {
  score: number;
  label: string;
}

export interface HazardComparison {
  la_median: number;
  lowest_risk_benchmark: number;
  percent_above_median: number | null;
  percentile: number;
}

export interface CommunityImpact {
  main_concern: string;
  hazard_score: number;
  hazard_label: string;
  what_this_means: string;
  likely_disruptions: string[];
  vulnerable_groups: string[];
}

export interface InsuranceGuidanceSection {
  title: string;
  items: string[];
}

export interface InsuranceGuidance {
  risk_profile: string;
  state: string;
  property_type: string;
  coverage_sections: InsuranceGuidanceSection[];
  coverage_to_ask_about: string[];
  disclaimer: string;
}

export interface RiskPayload {
  year: number;
  year_window: string;
  scenario: Scenario;
  scenario_display: string;
  cell_id: string;
  neighborhood: string;
  tract_fips: string | null;
  coordinates: { lat: number; lon: number };
  modifiers: {
    tree_canopy_pct: number | null;
    impervious_pct: number | null;
    social_vulnerability: number | null;
    resilience_idx: number | null;
  };
  scores: {
    heat: RiskSummary;
    wildfire: RiskSummary;
    flood: RiskSummary;
    overall: RiskSummary;
  };
  comparison: {
    heat: HazardComparison;
    wildfire: HazardComparison;
    flood: HazardComparison;
  };
  community_impact: CommunityImpact;
  insurance_guidance: InsuranceGuidance;
  explanation: string | null;
}

export interface GeocodeRiskResponse {
  geocoded: GeocodeResult;
  risk: RiskPayload;
}

interface ConfigResponse {
  years: number[];
  yearWindows: Record<string, string>;
}

export interface RiskMapCellFeature {
  type: "Feature";
  geometry: RiskMapCellGeometry;
  properties: {
    cell_id: string;
    neighborhood: string | null;
    tract_fips: string | null;
    hazard: string;
    score: number;
    label: string;
  };
}

export interface RiskMapCellsResponse {
  type: "FeatureCollection";
  features: RiskMapCellFeature[];
}

export async function fetchConfig(): Promise<ConfigResponse> {
  return requestJson<ConfigResponse>("/api/config");
}

export async function fetchGeocodeRisk(params: {
  address: string;
  year: Year;
  scenario: Scenario;
}): Promise<GeocodeRiskResponse> {
  return requestJson<GeocodeRiskResponse>("/api/geocode-risk", {
    method: "POST",
    body: JSON.stringify(params),
  });
}

export async function fetchRiskForPoint(params: {
  lat: number;
  lon: number;
  year: Year;
  scenario: Scenario;
}): Promise<RiskPayload> {
  return requestJson<RiskPayload>("/api/risk", {
    method: "POST",
    body: JSON.stringify(params),
  });
}

export async function fetchMapCells(params: {
  year: Year;
  scenario: Scenario;
  hazard: HazardLayer;
}): Promise<RiskMapPoint[]> {
  const payload = await fetchMapCellsGeoJson(params);
  return mapCellsToPoints(payload);
}

export async function fetchMapCellsGeoJson(params: {
  year: Year;
  scenario: Scenario;
  hazard: HazardLayer;
}): Promise<RiskMapCellsResponse> {
  const query = new URLSearchParams({
    year: String(params.year),
    scenario: params.scenario,
    hazard: params.hazard,
    limit: "5000",
  });
  return requestJson<RiskMapCellsResponse>(`/api/map-cells?${query.toString()}`);
}

export function mapCellsToPoints(payload: RiskMapCellsResponse): RiskMapPoint[] {
  return payload.features.map(mapCellFeatureToPoint);
}

export function mapCellFeatureToPoint(feature: RiskMapCellFeature): RiskMapPoint {
  const [lon, lat] = centerFromGeometry(feature.geometry);
  return {
    cellId: feature.properties.cell_id,
    lat,
    lon,
    score: feature.properties.score,
    label: feature.properties.label,
    neighborhood: feature.properties.neighborhood,
    tractFips: feature.properties.tract_fips,
  };
}

async function requestJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    ...init,
    headers: {
      "content-type": "application/json",
      ...(init?.headers ?? {}),
    },
  });
  const json = await response.json();
  if (!response.ok) {
    const detail = (json?.detail ?? json?.error ?? "Request failed") as string;
    throw new Error(detail);
  }
  return json as T;
}

function centerFromGeometry(geometry: RiskMapCellGeometry): [number, number] {
  if (geometry.type === "Point") {
    return geometry.coordinates;
  }

  if (geometry.type === "Polygon") {
    const ring = geometry.coordinates[0];
    if (ring.length === 0) return [-118.2437, 34.0522];
    return averageCoords(ring);
  }

  if (geometry.type === "MultiPolygon") {
    const firstPoly = geometry.coordinates[0]?.[0] ?? [];
    if (firstPoly.length === 0) return [-118.2437, 34.0522];
    return averageCoords(firstPoly);
  }

  return [-118.2437, 34.0522];
}

function averageCoords(coords: Array<[number, number]>): [number, number] {
  const total = coords.reduce(
    (acc, [lon, lat]) => {
      return { lon: acc.lon + lon, lat: acc.lat + lat };
    },
    { lon: 0, lat: 0 }
  );

  return [total.lon / coords.length, total.lat / coords.length];
}
