import { useEffect, useMemo, useState, type ReactNode } from "react";
import { Link } from "react-router-dom";
import { ArrowLeft, Droplets, Flame, Layers, Thermometer } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { LAMap, type RiskMapPoint } from "@/components/atlas/LAMap";
import { LAInteractiveMap } from "@/components/atlas/LAInteractiveMap";
import { LA3DMap } from "@/components/atlas/LA3DMap";
import {
  fetchConfig,
  fetchGeocodeRisk,
  fetchMapCellsGeoJson,
  fetchRiskForPoint,
  mapCellsToPoints,
  type GeocodeResult,
  type HazardLayer,
  type RiskMapCellsResponse,
  type RiskPayload,
  type Scenario,
  type Year,
} from "@/lib/api";
import { toneFromScore } from "@/lib/riskTone";

const YEARS: Year[] = [2030, 2050, 2080, 2100];
const HAZARDS: Array<{ id: HazardLayer; label: string }> = [
  { id: "combined", label: "Combined" },
  { id: "heat", label: "Heat" },
  { id: "wildfire", label: "Wildfire" },
  { id: "flood", label: "Flood" },
];
const SCENARIOS: Array<{ id: Scenario; title: string; description: string }> = [
  {
    id: "ssp245",
    title: "Lower warming (middle-of-the-road emissions)",
    description: "Emissions rise slower and then level off over time."
  },
  {
    id: "ssp370",
    title: "High warming",
    description: "Higher emissions path with stronger warming impacts."
  },
  {
    id: "ssp585",
    title: "Very high warming",
    description: "Most severe warming path used to stress-test future risk."
  }
];
const DEFAULT_SCENARIO: Scenario = "ssp585";
const SCORE_TONE_VARS = ["--risk-1", "--risk-2", "--risk-3", "--risk-4", "--risk-5"] as const;

export default function Atlas() {
  const [address, setAddress] = useState("");
  const [year, setYear] = useState<Year>(2050);
  const [scenario, setScenario] = useState<Scenario>(DEFAULT_SCENARIO);
  const [hazardLayer, setHazardLayer] = useState<HazardLayer>("heat");
  const [mapView, setMapView] = useState<"interactive" | "stylized" | "3d">("3d");

  const [yearWindows, setYearWindows] = useState<Record<string, string>>({});
  const [mapPoints, setMapPoints] = useState<RiskMapPoint[]>([]);
  const [mapCells, setMapCells] = useState<RiskMapCellsResponse | null>(null);
  const [risk, setRisk] = useState<RiskPayload | null>(null);
  const [geocoded, setGeocoded] = useState<GeocodeResult | null>(null);
  const [selectedCellId, setSelectedCellId] = useState<string | null>(null);
  const [status, setStatus] = useState<string>("Loading...");
  const [loading, setLoading] = useState(false);
  const [focusVersion, setFocusVersion] = useState(0);
  const selectedScenarioMeta = SCENARIOS.find((item) => item.id === scenario) ?? SCENARIOS[2];

  useEffect(() => {
    const run = async () => {
      try {
        const config = await fetchConfig();
        setYearWindows(config.yearWindows);
      } catch (error) {
        setStatus(error instanceof Error ? error.message : "Could not load API config.");
      }
    };
    void run();
  }, []);

  useEffect(() => {
    const run = async () => {
      try {
        const cells = await fetchMapCellsGeoJson({
          year,
          scenario,
          hazard: hazardLayer,
        });
        setMapCells(cells);
        setMapPoints(mapCellsToPoints(cells));
      } catch (error) {
        setStatus(error instanceof Error ? error.message : "Could not load map layer.");
      }
    };
    void run();
  }, [year, scenario, hazardLayer]);

  useEffect(() => {
    if (!geocoded) return;
    if (address.trim()) {
      void analyzeAddress();
      return;
    }
    void refreshSelectedPointRisk();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [year, scenario]);

  const selectedLat = geocoded?.lat ?? risk?.coordinates.lat ?? null;
  const selectedLon = geocoded?.lon ?? risk?.coordinates.lon ?? null;
  const hasAnalysis = Boolean(risk);
  const activeCellId = selectedCellId ?? normalizeCellId(risk?.cell_id);

  const nearby = useMemo(() => {
    if (!risk) return [];
    const origin = risk.coordinates;
    const currentCellId = String(risk.cell_id);
    return mapPoints
      .filter((p) => String(p.cellId) !== currentCellId)
      .map((point) => ({ point, d: distanceKm(origin.lat, origin.lon, point.lat, point.lon) }))
      .sort((a, b) => a.d - b.d)
      .slice(0, 6);
  }, [mapPoints, risk]);

  async function analyzeAddress() {
    if (!address.trim()) {
      setStatus("Enter an address to analyze.");
      return;
    }

    setLoading(true);
    setStatus("Analyzing address...");
    try {
      const payload = await fetchGeocodeRisk({
        address,
        year,
        scenario,
      });
      setGeocoded(payload.geocoded);
      setRisk(payload.risk);
      setSelectedCellId(normalizeCellId(payload.risk.cell_id));
      setFocusVersion((v) => v + 1);
      setStatus("Done.");
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Address analysis failed.");
    } finally {
      setLoading(false);
    }
  }

  async function handleSelectPoint(point: RiskMapPoint) {
    const normalizedPointCellId = normalizeCellId(point.cellId);
    setSelectedCellId(normalizedPointCellId);
    setAddress("");
    setLoading(true);
    setStatus("Loading selected block...");
    try {
      const payload = await fetchRiskForPoint({
        lat: point.lat,
        lon: point.lon,
        year,
        scenario,
      });
      setRisk(payload);
      setSelectedCellId(normalizeCellId(payload.cell_id) ?? normalizedPointCellId);
      setGeocoded({
        source: "map_selection",
        input_address: address,
        matched_address: point.neighborhood ?? payload.neighborhood,
        lat: point.lat,
        lon: point.lon,
        tract_fips: point.tractFips,
        county_fips: null,
        state_fips: null,
      });
      setFocusVersion((v) => v + 1);
      setStatus(`Selected ${point.neighborhood ?? "map point"}.`);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Could not load selected block.");
    } finally {
      setLoading(false);
    }
  }

  async function refreshSelectedPointRisk() {
    if (!geocoded) return;
    setLoading(true);
    setStatus("Updating selected block...");
    try {
      const payload = await fetchRiskForPoint({
        lat: geocoded.lat,
        lon: geocoded.lon,
        year,
        scenario,
      });
      setRisk(payload);
      setSelectedCellId(normalizeCellId(payload.cell_id));
      setStatus("Updated selected block.");
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Could not update selected block.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="min-h-screen bg-background">
      <header className="flex items-center justify-between border-b border-border bg-card/60 px-6 py-3 backdrop-blur">
        <div className="flex items-center gap-3">
          <Link to="/" className="inline-flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground">
            <ArrowLeft className="h-4 w-4" /> Back
          </Link>
          <span className="h-4 w-px bg-border" />
          <h1 className="font-display text-lg font-bold tracking-tight">Your Future Block</h1>
        </div>
      </header>

      <main className="relative isolate h-[calc(100vh-57px)] overflow-hidden bg-secondary/40">
        <section className="absolute inset-0 z-0">
          {mapView === "3d" && mapCells ? (
            <LA3DMap
              cells={mapCells}
              selectedCellId={activeCellId}
              selectedLat={selectedLat}
              selectedLon={selectedLon}
              focusVersion={focusVersion}
              onSelectPoint={handleSelectPoint}
            />
          ) : mapView === "interactive" ? (
            <LAInteractiveMap
              points={mapPoints}
              selectedCellId={activeCellId}
              selectedLat={selectedLat}
              selectedLon={selectedLon}
              focusVersion={focusVersion}
              onSelectPoint={handleSelectPoint}
            />
          ) : (
            <LAMap
              points={mapPoints}
              selectedCellId={activeCellId}
              selectedLat={selectedLat}
              selectedLon={selectedLon}
              onSelectPoint={handleSelectPoint}
            />
          )}

          {hasAnalysis ? (
            <div className="pointer-events-none absolute left-5 top-5 z-[1100] flex flex-col gap-2">
            <div className="pointer-events-auto rounded-xl border border-border bg-card/90 px-4 py-3 shadow-soft backdrop-blur">
              <p className="mb-2 font-mono text-[10px] uppercase tracking-wider text-muted-foreground">Projection year</p>
              <div className="grid grid-cols-2 gap-2">
                {YEARS.map((y) => (
                  <button
                    key={y}
                    onClick={() => setYear(y)}
                    className={`rounded-md border px-2 py-1.5 text-sm font-semibold transition ${
                      y === year
                        ? "border-foreground bg-foreground text-background"
                        : "border-border bg-secondary/70 hover:border-foreground/40"
                    }`}
                  >
                    {y}
                  </button>
                ))}
              </div>
              <p className="text-xs text-muted-foreground">{yearWindows[String(year)] ?? ""}</p>
            </div>
            <div className="pointer-events-auto rounded-xl border border-border bg-card/90 px-4 py-2 shadow-soft backdrop-blur">
              <div className="mb-1 flex items-center gap-1.5">
                <Layers className="h-3 w-3 text-muted-foreground" />
                <p className="font-mono text-[10px] uppercase tracking-wider text-muted-foreground">Map layer</p>
              </div>
              <p className="text-xs font-semibold">{HAZARDS.find((h) => h.id === hazardLayer)?.label}</p>
              <p className="text-xs text-muted-foreground">
                View: {mapView === "interactive" ? "Interactive" : mapView === "stylized" ? "Stylized" : "3D chunks"}
              </p>
              <p className="text-xs text-muted-foreground">
                Scenario: {selectedScenarioMeta.title}
              </p>
              <p className="text-[11px] text-muted-foreground">{mapPoints.length.toLocaleString()} points loaded</p>
            </div>
            </div>
          ) : null}

        </section>

        <aside className="absolute bottom-0 right-0 z-[1200] h-[54%] w-full overflow-y-auto border-t border-border bg-card/95 shadow-2xl backdrop-blur md:top-0 md:h-full md:w-[420px] md:border-l md:border-t-0">
          <div className="border-b border-border p-6">
            <p className="font-mono text-[10px] uppercase tracking-wider text-muted-foreground">Address</p>
            <div className="mt-2 flex gap-2">
              <Input value={address} onChange={(e) => setAddress(e.target.value)} />
              <Button onClick={() => void analyzeAddress()} disabled={loading}>
                {loading ? "..." : "Analyze"}
              </Button>
            </div>
            <p className="mt-2 text-xs text-muted-foreground">{status}</p>
          </div>

          {hasAnalysis ? (
            <>
          <div className="border-b border-border p-6">
            <p className="font-mono text-[10px] uppercase tracking-wider text-muted-foreground">Selected block</p>
            <h2 className="mt-1 font-display text-2xl font-bold">{risk?.neighborhood ?? "No selection"}</h2>
            <p className="mt-3 text-sm text-muted-foreground">
              Screening-level output. Not a house-level prediction.
            </p>
          </div>

          <div className="border-b border-border p-6">
            <p className="mb-3 font-mono text-[10px] uppercase tracking-wider text-muted-foreground">Per-hazard scores + LA comparison</p>
            <div className="space-y-2 text-sm">
              <ScoreRow
                icon={<Thermometer className="h-4 w-4" />}
                label="Heat"
                value={risk?.scores.heat}
                median={risk?.comparison.heat.la_median}
                benchmark={risk?.comparison.heat.lowest_risk_benchmark}
                percentile={risk?.comparison.heat.percentile}
                percentAboveMedian={risk?.comparison.heat.percent_above_median}
              />
              <ScoreRow
                icon={<Flame className="h-4 w-4" />}
                label="Wildfire"
                value={risk?.scores.wildfire}
                median={risk?.comparison.wildfire.la_median}
                benchmark={risk?.comparison.wildfire.lowest_risk_benchmark}
                percentile={risk?.comparison.wildfire.percentile}
                percentAboveMedian={risk?.comparison.wildfire.percent_above_median}
              />
              <ScoreRow
                icon={<Droplets className="h-4 w-4" />}
                label="Flood"
                value={risk?.scores.flood}
                median={risk?.comparison.flood.la_median}
                benchmark={risk?.comparison.flood.lowest_risk_benchmark}
                percentile={risk?.comparison.flood.percentile}
                percentAboveMedian={risk?.comparison.flood.percent_above_median}
              />
            </div>
          </div>

          <div className="border-b border-border p-6">
            <p className="mb-3 font-mono text-[10px] uppercase tracking-wider text-muted-foreground">Map controls</p>
            <p className="mb-2 text-xs text-muted-foreground">Hazard layer</p>
            <div className="grid grid-cols-2 gap-2">
              {HAZARDS.map((item) => (
                <button
                  key={item.id}
                  onClick={() => setHazardLayer(item.id)}
                  className={`rounded-lg border px-3 py-2 text-sm font-semibold transition ${
                    hazardLayer === item.id
                      ? "border-foreground bg-foreground text-background"
                      : "border-border bg-secondary/60 hover:border-foreground/40"
                  }`}
                >
                  {item.label}
                </button>
              ))}
            </div>
            <p className="mb-2 mt-4 text-xs text-muted-foreground">Map view</p>
            <div className="grid grid-cols-3 gap-2">
              <button
                onClick={() => setMapView("3d")}
                className={`rounded-lg border px-3 py-2 text-sm font-semibold transition ${
                  mapView === "3d"
                    ? "border-foreground bg-foreground text-background"
                    : "border-border bg-secondary/60 hover:border-foreground/40"
                }`}
              >
                3D
              </button>
              <button
                onClick={() => setMapView("interactive")}
                className={`rounded-lg border px-3 py-2 text-sm font-semibold transition ${
                  mapView === "interactive"
                    ? "border-foreground bg-foreground text-background"
                    : "border-border bg-secondary/60 hover:border-foreground/40"
                }`}
              >
                Interactive
              </button>
              <button
                onClick={() => setMapView("stylized")}
                className={`rounded-lg border px-3 py-2 text-sm font-semibold transition ${
                  mapView === "stylized"
                    ? "border-foreground bg-foreground text-background"
                    : "border-border bg-secondary/60 hover:border-foreground/40"
                }`}
              >
                Stylized
              </button>
            </div>
          </div>

          <div className="border-b border-border p-6">
            <p className="mb-2 font-mono text-[10px] uppercase tracking-wider text-muted-foreground">Community Impact Explainer</p>
            {risk ? (
              <div className="space-y-3">
                <p className="font-display text-base leading-snug">
                  {risk.community_impact.main_concern}: {risk.community_impact.hazard_label}
                </p>
                <p className="text-sm text-muted-foreground">{risk.community_impact.what_this_means}</p>

                <div>
                  <p className="font-mono text-[10px] uppercase tracking-wider text-muted-foreground">Likely disruptions</p>
                  <ul className="mt-1 space-y-1">
                    {risk.community_impact.likely_disruptions.map((item) => (
                      <li key={item} className="text-sm">
                        • {item}
                      </li>
                    ))}
                  </ul>
                </div>

                <div>
                  <p className="font-mono text-[10px] uppercase tracking-wider text-muted-foreground">Groups that may be more affected</p>
                  <p className="mt-1 text-sm">
                    {risk.community_impact.vulnerable_groups.length
                      ? risk.community_impact.vulnerable_groups.join(", ")
                      : "No specific group stands out in this snapshot."}
                  </p>
                </div>
              </div>
            ) : (
              <p className="text-sm text-muted-foreground">
                Analyze an address to get neighborhood-level impact context.
              </p>
            )}
          </div>

          <div className="border-b border-border p-6">
            <p className="mb-2 font-mono text-[10px] uppercase tracking-wider text-muted-foreground">Coverage To Ask About</p>
            {risk ? (
              <div className="space-y-3">
                <p className="text-sm text-muted-foreground">
                  Risk profile: <span className="font-semibold text-foreground">{risk.insurance_guidance.risk_profile.replaceAll("_", " ")}</span>
                </p>

                {risk.insurance_guidance.coverage_sections.map((section) => (
                  <div key={section.title}>
                    <p className="text-sm font-semibold">{section.title}</p>
                    <ul className="mt-1 space-y-1">
                      {section.items.map((item) => (
                        <li key={item} className="text-sm">
                          • {item}
                        </li>
                      ))}
                    </ul>
                  </div>
                ))}

                {risk.insurance_guidance.coverage_sections.length === 0 ? (
                  <p className="text-sm text-muted-foreground">
                    No special coverage flags were triggered for this risk profile.
                  </p>
                ) : null}

                <p className="text-xs text-muted-foreground">
                  {risk.insurance_guidance.disclaimer}
                </p>
              </div>
            ) : (
              <p className="text-sm text-muted-foreground">
                Analyze an address to see educational coverage guidance.
              </p>
            )}
          </div>

          <div className="border-b border-border p-6">
            <p className="mb-2 font-mono text-[10px] uppercase tracking-wider text-muted-foreground">Summary</p>
            <p className="font-display text-base leading-snug">
              {risk?.explanation ?? "Analyze an address to get a plain-English summary."}
            </p>
          </div>

          <div className="border-b border-border p-6">
            <p className="mb-3 font-mono text-[10px] uppercase tracking-wider text-muted-foreground">Nearby cells</p>
            <ul className="space-y-1.5">
              {nearby.map(({ point, d }) => (
                <li key={point.cellId}>
                  <button
                    onClick={() => void handleSelectPoint(point)}
                    className="flex w-full items-center justify-between rounded-lg border border-border bg-secondary/40 px-3 py-2 text-left text-sm transition hover:border-foreground/40 hover:bg-secondary"
                  >
                    <span className="font-medium">{point.neighborhood ?? point.cellId}</span>
                    <span className="font-mono text-xs tabular-nums">
                      {point.score} · {d.toFixed(1)} km
                    </span>
                  </button>
                </li>
              ))}
            </ul>
          
          </div>

          <div className="p-6">
            <p className="mb-3 font-mono text-[10px] uppercase tracking-wider text-muted-foreground">Emissions scenario</p>
            <div className="space-y-2">
              {SCENARIOS.map((item) => (
                <button
                  key={item.id}
                  onClick={() => setScenario(item.id)}
                  className={`w-full rounded-lg border px-3 py-2 text-left text-sm transition ${
                    scenario === item.id
                      ? "border-foreground bg-foreground text-background"
                      : "border-border bg-secondary/60 hover:border-foreground/40"
                  }`}
                >
                  <p className="font-semibold">{item.title}</p>
                  <p className={`text-xs ${scenario === item.id ? "text-background/85" : "text-muted-foreground"}`}>
                    {item.description}
                  </p>
                </button>
              ))}
            </div>
          </div>
            </>
          ) : (
            <div className="p-6">
              <p className="text-sm text-muted-foreground">
                Enter an address above to unlock risk scores, map controls, neighborhood comparison, and community guidance.
              </p>
            </div>
          )}
        </aside>
      </main>
    </div>
  );
}

function ScoreRow({
  icon,
  label,
  value,
  median,
  benchmark,
  percentile,
  percentAboveMedian,
}: {
  icon: ReactNode;
  label: string;
  value: { score: number; label: string } | undefined;
  median?: number;
  benchmark?: number;
  percentile?: number;
  percentAboveMedian?: number | null;
}) {
  const score = value?.score ?? 0;
  const toneVar = SCORE_TONE_VARS[toneFromScore(score) - 1];
  const toneColor = `hsl(var(${toneVar}))`;
  const toneBg = `hsl(var(${toneVar}) / 0.16)`;

  return (
    <div
      className="rounded-lg border px-3 py-2"
      style={{
        borderColor: `hsl(var(${toneVar}) / 0.35)`,
        backgroundColor: `hsl(var(${toneVar}) / 0.08)`
      }}
    >
      <div className="flex items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <span style={{ color: toneColor }}>{icon}</span>
          <span className="font-semibold">{label}</span>
        </div>
        <span
          className="rounded px-2 py-1 font-mono text-xs"
          style={{ color: toneColor, backgroundColor: toneBg }}
        >
          {value ? `${Math.round(value.score)} (${value.label})` : "--"}
        </span>
      </div>
      {typeof median === "number" &&
      typeof benchmark === "number" &&
      typeof percentile === "number" ? (
        <p className="mt-2 text-xs text-muted-foreground">
          {formatPercentDelta(percentAboveMedian ?? null)} vs LA median ({Math.round(median)}), percentile {percentile}, benchmark {Math.round(benchmark)}.
        </p>
      ) : null}
    </div>
  );
}

function distanceKm(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const r = 6371;
  const toRad = (v: number) => (v * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * r * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function formatPercentDelta(value: number | null): string {
  if (value === null) return "N/A";
  if (value === 0) return "At parity";
  return value > 0 ? `${value}% higher` : `${Math.abs(value)}% lower`;
}

function normalizeCellId(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  const normalized = String(value).trim();
  return normalized.length > 0 ? normalized : null;
}
