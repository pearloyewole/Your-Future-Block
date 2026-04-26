export type Hazard = "heat" | "fire" | "flood";
export type Year = 2025 | 2030 | 2050 | 2100;

export interface Block {
  id: string;
  name: string;
  blurb: string;
  /** SVG coords in a 800x520 viewbox */
  x: number;
  y: number;
  scores: Record<Year, Record<Hazard, number>>; // 0-100 per hazard per year
}

/** Composite risk = weighted average of active hazards (0-100). */
export function compositeScore(b: Block, year: Year, layers: Record<Hazard, boolean>): number {
  const active = (Object.keys(layers) as Hazard[]).filter((h) => layers[h]);
  if (active.length === 0) return 0;
  const sum = active.reduce((s, h) => s + b.scores[year][h], 0);
  return Math.round(sum / active.length);
}

export function severityBand(score: number): { label: string; tone: 1 | 2 | 3 | 4 | 5 } {
  if (score < 25) return { label: "Low", tone: 1 };
  if (score < 45) return { label: "Moderate", tone: 2 };
  if (score < 60) return { label: "Elevated", tone: 3 };
  if (score < 78) return { label: "High", tone: 4 };
  return { label: "Extreme", tone: 5 };
}

export const YEARS: Year[] = [2025, 2030, 2050, 2100];

export const HAZARD_META: Record<Hazard, { label: string; varName: string; description: string }> = {
  heat: { label: "Heat", varName: "--heat", description: "Days/yr above 95°F & nighttime heat stress." },
  fire: { label: "Wildfire", varName: "--fire", description: "Wildland-urban interface burn probability." },
  flood: { label: "Flood", varName: "--flood", description: "Coastal surge, sea-level rise & flash flooding." },
};

/** Plausible (illustrative) fixtures across LA neighborhoods. */
export const BLOCKS: Block[] = [
  {
    id: "venice", name: "Venice", blurb: "Coastal flats just inland of the boardwalk.",
    x: 175, y: 360,
    scores: {
      2025: { heat: 22, fire: 10, flood: 48 },
      2030: { heat: 28, fire: 12, flood: 58 },
      2050: { heat: 41, fire: 16, flood: 78 },
      2100: { heat: 60, fire: 20, flood: 96 },
    },
  },
  {
    id: "santa-monica", name: "Santa Monica", blurb: "Bluff-edge city with a vulnerable beach plain.",
    x: 150, y: 320,
    scores: {
      2025: { heat: 18, fire: 14, flood: 38 },
      2030: { heat: 24, fire: 17, flood: 47 },
      2050: { heat: 36, fire: 22, flood: 66 },
      2100: { heat: 54, fire: 28, flood: 88 },
    },
  },
  {
    id: "malibu", name: "Malibu", blurb: "Canyon-coast corridor, repeat wildfire path.",
    x: 70, y: 280,
    scores: {
      2025: { heat: 30, fire: 72, flood: 36 },
      2030: { heat: 36, fire: 79, flood: 44 },
      2050: { heat: 50, fire: 88, flood: 60 },
      2100: { heat: 66, fire: 94, flood: 78 },
    },
  },
  {
    id: "topanga", name: "Topanga", blurb: "Steep chaparral canyon, single-road egress.",
    x: 130, y: 240,
    scores: {
      2025: { heat: 38, fire: 76, flood: 14 },
      2030: { heat: 45, fire: 82, flood: 18 },
      2050: { heat: 60, fire: 90, flood: 24 },
      2100: { heat: 76, fire: 96, flood: 30 },
    },
  },
  {
    id: "pacoima", name: "Pacoima", blurb: "San Fernando Valley, intense urban heat island.",
    x: 360, y: 130,
    scores: {
      2025: { heat: 58, fire: 30, flood: 12 },
      2030: { heat: 67, fire: 36, flood: 14 },
      2050: { heat: 82, fire: 46, flood: 18 },
      2100: { heat: 95, fire: 58, flood: 24 },
    },
  },
  {
    id: "highland-park", name: "Highland Park", blurb: "Hill-edge LA, mixed heat & fire pressure.",
    x: 470, y: 220,
    scores: {
      2025: { heat: 44, fire: 38, flood: 16 },
      2030: { heat: 52, fire: 44, flood: 19 },
      2050: { heat: 68, fire: 56, flood: 24 },
      2100: { heat: 84, fire: 70, flood: 32 },
    },
  },
  {
    id: "downtown", name: "Downtown LA", blurb: "Dense core, low albedo, river-adjacent.",
    x: 470, y: 280,
    scores: {
      2025: { heat: 50, fire: 12, flood: 28 },
      2030: { heat: 58, fire: 14, flood: 33 },
      2050: { heat: 74, fire: 18, flood: 44 },
      2100: { heat: 90, fire: 24, flood: 58 },
    },
  },
  {
    id: "boyle-heights", name: "Boyle Heights", blurb: "East of the LA River, freeway-locked.",
    x: 525, y: 295,
    scores: {
      2025: { heat: 52, fire: 14, flood: 30 },
      2030: { heat: 60, fire: 17, flood: 36 },
      2050: { heat: 76, fire: 22, flood: 48 },
      2100: { heat: 92, fire: 28, flood: 62 },
    },
  },
  {
    id: "san-pedro", name: "San Pedro", blurb: "Port-adjacent bluffs, rising tide exposure.",
    x: 480, y: 460,
    scores: {
      2025: { heat: 26, fire: 18, flood: 42 },
      2030: { heat: 32, fire: 22, flood: 51 },
      2050: { heat: 46, fire: 30, flood: 70 },
      2100: { heat: 62, fire: 38, flood: 90 },
    },
  },
  {
    id: "long-beach", name: "Long Beach", blurb: "Low-lying coastal grid + LA River outfall.",
    x: 580, y: 470,
    scores: {
      2025: { heat: 34, fire: 12, flood: 50 },
      2030: { heat: 41, fire: 14, flood: 60 },
      2050: { heat: 56, fire: 19, flood: 80 },
      2100: { heat: 72, fire: 26, flood: 98 },
    },
  },
];

/** Crude pixel distance for "compare nearby" sort. */
export function distance(a: Block, b: Block) {
  return Math.hypot(a.x - b.x, a.y - b.y);
}

export function explain(b: Block, year: Year, layers: Record<Hazard, boolean>): string {
  const active = (Object.keys(layers) as Hazard[]).filter((h) => layers[h]);
  if (active.length === 0) return "Toggle a hazard layer to see what's driving risk here.";
  const ranked = active
    .map((h) => ({ h, v: b.scores[year][h], base: b.scores[2025][h] }))
    .sort((a, z) => z.v - a.v);
  const top = ranked[0];
  const delta = top.v - top.base;
  const dir = delta >= 0 ? "rises" : "falls";
  const yrTxt = year === 2025 ? "today" : `by ${year}`;
  const driver =
    top.h === "heat"
      ? "more extreme-heat days and hotter nights"
      : top.h === "fire"
      ? "drier fuels and longer fire weather windows"
      : "sea-level rise, king tides, and intensified storm runoff";
  return `In ${b.name}, ${HAZARD_META[top.h].label.toLowerCase()} risk ${dir} from ${top.base} ${yrTxt === "today" ? "now" : "to " + top.v} ${yrTxt}, driven by ${driver}.`;
}