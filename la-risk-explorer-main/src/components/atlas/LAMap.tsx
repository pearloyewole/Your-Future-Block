import { useMemo } from "react";
import { toneForRisk } from "@/lib/riskTone";

export interface RiskMapPoint {
  cellId: string;
  lat: number;
  lon: number;
  score: number;
  label: string;
  neighborhood: string | null;
  tractFips: string | null;
  percentAboveMedian: number | null;
}

interface Props {
  points: RiskMapPoint[];
  selectedCellId: string | null;
  selectedLat: number | null;
  selectedLon: number | null;
  onSelectPoint: (point: RiskMapPoint) => void;
}

const MIN_LON = -119.0;
const MAX_LON = -117.5;
const MIN_LAT = 33.5;
const MAX_LAT = 34.9;

const RISK_VARS = ["--risk-1", "--risk-2", "--risk-3", "--risk-4", "--risk-5"] as const;

export function LAMap({
  points,
  selectedCellId,
  selectedLat,
  selectedLon,
  onSelectPoint,
}: Props) {
  const projected = useMemo(
    () =>
      points.map((point) => {
        const { x, y } = project(point.lon, point.lat);
        const tone = toneForRisk({
          score: point.score,
          percentAboveMedian: point.percentAboveMedian,
        });
        return { point, x, y, tone };
      }),
    [points]
  );

  const selectedMarker =
    selectedLat !== null && selectedLon !== null ? project(selectedLon, selectedLat) : null;
  const viewBox = useMemo(() => {
    if (!selectedMarker) return "0 0 800 520";

    const zoomWidth = 320;
    const zoomHeight = 220;
    const minX = clamp(selectedMarker.x - zoomWidth / 2, 0, 800 - zoomWidth);
    const minY = clamp(selectedMarker.y - zoomHeight / 2, 0, 520 - zoomHeight);
    return `${minX} ${minY} ${zoomWidth} ${zoomHeight}`;
  }, [selectedMarker]);

  return (
    <svg
      viewBox={viewBox}
      className="h-full w-full"
      role="img"
      aria-label="Stylized Los Angeles map with live climate risk points"
    >
      <rect width="800" height="520" fill="hsl(var(--map-water))" />
      <path
        d="M 0,180 C 60,150 120,170 180,200 L 240,160 C 300,120 360,110 430,130 L 520,120 C 600,130 680,160 800,150 L 800,420 C 720,440 640,430 560,460 L 480,500 C 420,520 360,510 300,480 L 220,470 C 160,450 100,430 40,420 L 0,420 Z"
        fill="hsl(var(--map-land))"
        stroke="hsl(var(--map-stroke) / 0.35)"
        strokeWidth="1.2"
      />
      <path
        d="M 240,140 C 320,120 420,118 520,130 L 540,200 C 460,210 360,205 280,200 Z"
        fill="hsl(var(--map-land-2))"
        opacity="0.7"
      />
      <path
        d="M 380,140 C 420,200 470,260 500,310 C 530,370 560,420 600,470"
        stroke="hsl(var(--map-river))"
        strokeWidth="2"
        fill="none"
        opacity="0.85"
      />

      {projected.map(({ point, x, y, tone }) => {
        const isSelected = point.cellId === selectedCellId;
        const colorVar = RISK_VARS[tone - 1];
        const radius = 3.5 + (point.score / 100) * 8;
        return (
          <g
            key={point.cellId}
            className="cursor-pointer"
            onClick={() => onSelectPoint(point)}
          >
            {isSelected ? (
              <circle
                cx={x}
                cy={y}
                r={radius + 7}
                fill="none"
                stroke={`hsl(var(${colorVar}))`}
                strokeWidth="1.5"
                opacity="0.6"
              />
            ) : null}
            <circle
              cx={x}
              cy={y}
              r={radius}
              fill={`hsl(var(${colorVar}))`}
              fillOpacity={isSelected ? 0.92 : 0.72}
              stroke={isSelected ? "hsl(var(--foreground))" : "hsl(var(--background))"}
              strokeWidth={isSelected ? 1.8 : 1}
            />
          </g>
        );
      })}

      {selectedMarker ? (
        <g>
          <circle
            cx={selectedMarker.x}
            cy={selectedMarker.y}
            r={7}
            fill="none"
            stroke="hsl(var(--foreground))"
            strokeWidth="2"
          />
          <circle
            cx={selectedMarker.x}
            cy={selectedMarker.y}
            r={2.4}
            fill="hsl(var(--foreground))"
          />
        </g>
      ) : null}

      {projected.length === 0 ? (
        <text
          x="400"
          y="260"
          textAnchor="middle"
          fontSize="14"
          fill="hsl(var(--muted-foreground))"
        >
          No map points available for this selection.
        </text>
      ) : null}
    </svg>
  );
}

function project(lon: number, lat: number): { x: number; y: number } {
  const xNorm = (lon - MIN_LON) / (MAX_LON - MIN_LON);
  const yNorm = (lat - MIN_LAT) / (MAX_LAT - MIN_LAT);
  const x = 30 + Math.max(0, Math.min(1, xNorm)) * 740;
  const y = 500 - Math.max(0, Math.min(1, yNorm)) * 460;
  return { x, y };
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}
