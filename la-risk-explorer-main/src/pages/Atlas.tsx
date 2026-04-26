import { useEffect, useMemo, useState, type ReactNode } from "react";
import { Link } from "react-router-dom";
import { ArrowLeft, Droplets, Flame, Layers, Thermometer } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { LAMap, type RiskMapPoint } from "@/components/atlas/LAMap";
import { LAInteractiveMap } from "@/components/atlas/LAInteractiveMap";
import {
  fetchConfig,
  fetchGeocodeRisk,
  fetchMapCells,
  fetchRiskForPoint,
  type GeocodeResult,
  type HazardLayer,
  type RiskPayload,
  type Year,
} from "@/lib/api";

const YEARS: Year[] = [2030, 2050, 2080, 2100];
const HAZARDS: Array<{ id: HazardLayer; label: string }> = [
  { id: "heat", label: "Heat" },
  { id: "wildfire", label: "Wildfire" },
  { id: "flood", label: "Flood" },
];
const RISK_SENSITIVE_SCENARIO = "ssp585" as const;
const RISK_SENSITIVE_SCENARIO_LABEL = "SSP5-8.5 (very high emissions)";

export default function Atlas() {
  const [address, setAddress] = useState("200 N Spring St, Los Angeles, CA");
  const [year, setYear] = useState<Year>(2050);
  const [hazardLayer, setHazardLayer] = useState<HazardLayer>("heat");
  const [mapView, setMapView] = useState<"interactive" | "stylized">("interactive");

  const [yearWindows, setYearWindows] = useState<Record<string, string>>({});
  const [mapPoints, setMapPoints] = useState<RiskMapPoint[]>([]);
  const [risk, setRisk] = useState<RiskPayload | null>(null);
  const [geocoded, setGeocoded] = useState<GeocodeResult | null>(null);
  const [status, setStatus] = useState<string>("Loading...");
  const [loading, setLoading] = useState(false);

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
        const points = await fetchMapCells({
          year,
          scenario: RISK_SENSITIVE_SCENARIO,
          hazard: hazardLayer,
        });
        setMapPoints(points);
      } catch (error) {
        setStatus(error instanceof Error ? error.message : "Could not load map layer.");
      }
    };
    void run();
  }, [year, hazardLayer]);

  useEffect(() => {
    void analyzeAddress();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [year]);

  const selectedCellId = risk?.cell_id ?? null;
  const selectedLat = geocoded?.lat ?? risk?.coordinates.lat ?? null;
  const selectedLon = geocoded?.lon ?? risk?.coordinates.lon ?? null;

  const nearby = useMemo(() => {
    if (!risk) return [];
    const origin = risk.coordinates;
    return mapPoints
      .filter((p) => p.cellId !== risk.cell_id)
      .map((point) => ({ point, d: distanceKm(origin.lat, origin.lon, point.lat, point.lon) }))
      .sort((a, b) => a.d - b.d)
      .slice(0, 6);
  }, [mapPoints, risk]);

  async function analyzeAddress() {
    setLoading(true);
    setStatus("Analyzing address...");
    try {
      const payload = await fetchGeocodeRisk({
        address,
        year,
        scenario: RISK_SENSITIVE_SCENARIO,
      });
      setGeocoded(payload.geocoded);
      setRisk(payload.risk);
      setStatus(`Done. Source: ${payload.geocoded.source}.`);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Address analysis failed.");
    } finally {
      setLoading(false);
    }
  }

  async function handleSelectPoint(point: RiskMapPoint) {
    setLoading(true);
    setStatus("Loading selected block...");
    try {
      const payload = await fetchRiskForPoint({
        lat: point.lat,
        lon: point.lon,
        year,
        scenario: RISK_SENSITIVE_SCENARIO,
      });
      setRisk(payload);
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
      setStatus(`Selected ${point.neighborhood ?? "map point"}.`);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Could not load selected block.");
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
          <h1 className="font-display text-lg font-bold tracking-tight">LA Risk Explorer · Live Node API</h1>
        </div>
        <Badge variant="outline" className="font-mono text-xs">
          Node /api integration
        </Badge>
      </header>

      <main className="relative isolate h-[calc(100vh-57px)] overflow-hidden bg-secondary/40">
        <section className="absolute inset-0 z-0">
          {mapView === "interactive" ? (
            <LAInteractiveMap
              points={mapPoints}
              selectedCellId={selectedCellId}
              selectedLat={selectedLat}
              selectedLon={selectedLon}
              onSelectPoint={handleSelectPoint}
            />
          ) : (
            <LAMap
              points={mapPoints}
              selectedCellId={selectedCellId}
              selectedLat={selectedLat}
              selectedLon={selectedLon}
              onSelectPoint={handleSelectPoint}
            />
          )}

          <div className="pointer-events-none absolute left-5 top-5 z-[1100] flex flex-col gap-2">
            <div className="pointer-events-auto rounded-xl border border-border bg-card/90 px-4 py-3 shadow-soft backdrop-blur">
              <p className="font-mono text-[10px] uppercase tracking-wider text-muted-foreground">Projection year</p>
              <p className="font-display text-3xl font-bold leading-none">{year}</p>
              <p className="text-xs text-muted-foreground">{yearWindows[String(year)] ?? ""}</p>
            </div>
            <div className="pointer-events-auto rounded-xl border border-border bg-card/90 px-4 py-2 shadow-soft backdrop-blur">
              <div className="mb-1 flex items-center gap-1.5">
                <Layers className="h-3 w-3 text-muted-foreground" />
                <p className="font-mono text-[10px] uppercase tracking-wider text-muted-foreground">Map layer</p>
              </div>
              <p className="text-xs font-semibold">{HAZARDS.find((h) => h.id === hazardLayer)?.label}</p>
              <p className="text-xs text-muted-foreground">
                View: {mapView === "interactive" ? "Interactive" : "Stylized"}
              </p>
              <p className="text-[11px] text-muted-foreground">{mapPoints.length.toLocaleString()} points loaded</p>
            </div>
          </div>

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

          <div className="border-b border-border p-6">
            <p className="font-mono text-[10px] uppercase tracking-wider text-muted-foreground">Selected block</p>
            <h2 className="mt-1 font-display text-2xl font-bold">{risk?.neighborhood ?? "No selection"}</h2>
            <p className="mt-1 text-sm text-muted-foreground">
              {risk
                ? `Tract ${risk.tract_fips ?? "unknown"} · High-risk pathway: ${RISK_SENSITIVE_SCENARIO_LABEL}`
                : "Analyze an address or click a map point."}
            </p>
            <p className="mt-3 text-sm text-muted-foreground">
              Combined score view removed. Compare hazard-specific scores below.
            </p>
          </div>

          <div className="border-b border-border p-6">
            <p className="mb-3 font-mono text-[10px] uppercase tracking-wider text-muted-foreground">Year</p>
            <div className="grid grid-cols-4 gap-2">
              {YEARS.map((y) => (
                <button
                  key={y}
                  onClick={() => setYear(y)}
                  className={`rounded-lg border px-2 py-2 text-sm font-semibold transition ${
                    y === year
                      ? "border-foreground bg-foreground text-background"
                      : "border-border bg-secondary/60 hover:border-foreground/40"
                  }`}
                >
                  {y}
                </button>
              ))}
            </div>
            <p className="mt-2 text-xs text-muted-foreground">{yearWindows[String(year)] ?? ""}</p>
          </div>

          <div className="border-b border-border p-6">
            <p className="mb-2 font-mono text-[10px] uppercase tracking-wider text-muted-foreground">Scenario</p>
            <p className="text-sm font-semibold">{RISK_SENSITIVE_SCENARIO_LABEL}</p>
            <p className="mt-1 text-xs text-muted-foreground">
              Locked to the most risk-sensitive pathway for awareness.
            </p>
          </div>

          <div className="border-b border-border p-6">
            <p className="mb-3 font-mono text-[10px] uppercase tracking-wider text-muted-foreground">Map hazard layer</p>
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
            <p className="mb-3 mt-4 font-mono text-[10px] uppercase tracking-wider text-muted-foreground">Map view</p>
            <div className="grid grid-cols-2 gap-2">
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
            <p className="mb-3 font-mono text-[10px] uppercase tracking-wider text-muted-foreground">Per-hazard scores</p>
            <div className="space-y-2 text-sm">
              <ScoreRow icon={<Thermometer className="h-4 w-4" />} label="Heat" value={risk?.scores.heat} />
              <ScoreRow icon={<Flame className="h-4 w-4" />} label="Wildfire" value={risk?.scores.wildfire} />
              <ScoreRow icon={<Droplets className="h-4 w-4" />} label="Flood" value={risk?.scores.flood} />
            </div>
          </div>

          <div className="border-b border-border p-6">
            <p className="mb-2 font-mono text-[10px] uppercase tracking-wider text-muted-foreground">What's driving this</p>
            <p className="font-display text-base leading-snug">
              {risk?.explanation ?? "Analyze an address to get a plain-English explanation."}
            </p>
          </div>

          <div className="p-6">
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
            <Button asChild variant="outline" className="mt-4 w-full">
              <Link to="/">About the project</Link>
            </Button>
          </div>
        </aside>
      </main>
    </div>
  );
}

function ScoreRow({
  icon,
  label,
  value,
}: {
  icon: ReactNode;
  label: string;
  value: { score: number; label: string } | undefined;
}) {
  return (
    <div className="flex items-center justify-between rounded-lg border border-border bg-secondary/30 px-3 py-2">
      <div className="flex items-center gap-2">
        {icon}
        <span className="font-semibold">{label}</span>
      </div>
      <span className="font-mono text-xs">
        {value ? `${Math.round(value.score)} (${value.label})` : "--"}
      </span>
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
