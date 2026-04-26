import { useEffect, useMemo } from "react";
import { CircleMarker, MapContainer, Popup, TileLayer, useMap } from "react-leaflet";
import type { RiskMapPoint } from "./LAMap";
import { toneForRisk } from "@/lib/riskTone";

interface Props {
  points: RiskMapPoint[];
  selectedCellId: string | null;
  selectedLat: number | null;
  selectedLon: number | null;
  focusVersion: number;
  onSelectPoint: (point: RiskMapPoint) => void;
}

export function LAInteractiveMap({
  points,
  selectedCellId,
  selectedLat,
  selectedLon,
  focusVersion,
  onSelectPoint,
}: Props) {
  const center = useMemo<[number, number]>(() => {
    if (selectedLat !== null && selectedLon !== null) return [selectedLat, selectedLon];
    if (points.length === 0) return [34.0522, -118.2437];

    const totals = points.reduce(
      (acc, point) => ({ lat: acc.lat + point.lat, lon: acc.lon + point.lon }),
      { lat: 0, lon: 0 }
    );
    return [totals.lat / points.length, totals.lon / points.length];
  }, [points, selectedLat, selectedLon]);

  return (
    <MapContainer center={center} zoom={10} scrollWheelZoom className="h-full w-full">
      <TileLayer
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
      />

      <FitToData
        points={points}
        selectedLat={selectedLat}
        selectedLon={selectedLon}
        focusVersion={focusVersion}
      />

      {points.map((point) => {
        const tone = toneForRisk({
          score: point.score,
          percentAboveMedian: point.percentAboveMedian,
        });
        const color = scoreColor(tone);
        const isSelected = point.cellId === selectedCellId;
        const radius = 5 + point.score / 14;
        return (
          <CircleMarker
            key={point.cellId}
            center={[point.lat, point.lon]}
            radius={radius}
            pathOptions={{
              color: isSelected ? "#0f172a" : "#ffffff",
              weight: isSelected ? 2.4 : 1.2,
              fillColor: color,
              fillOpacity: isSelected ? 0.95 : 0.75,
            }}
            eventHandlers={{
              click: () => onSelectPoint(point),
            }}
          >
            <Popup>
              <div className="space-y-1">
                <div className="font-semibold">{point.neighborhood ?? "Map cell"}</div>
                <div className="text-xs">Score: {point.score} ({point.label})</div>
                <div className="text-xs">
                  {point.percentAboveMedian === null
                    ? "LA median comparison: N/A"
                    : `LA median comparison: ${point.percentAboveMedian > 0 ? "+" : ""}${point.percentAboveMedian}%`}
                </div>
                <div className="text-xs text-muted-foreground">Tract: {point.tractFips ?? "unknown"}</div>
              </div>
            </Popup>
          </CircleMarker>
        );
      })}

      {selectedLat !== null && selectedLon !== null ? (
        <CircleMarker
          center={[selectedLat, selectedLon]}
          radius={6}
          pathOptions={{
            color: "#020617",
            weight: 2,
            fillColor: "#ffffff",
            fillOpacity: 0.95,
          }}
        />
      ) : null}
    </MapContainer>
  );
}

function FitToData({
  points,
  selectedLat,
  selectedLon,
  focusVersion,
}: {
  points: RiskMapPoint[];
  selectedLat: number | null;
  selectedLon: number | null;
  focusVersion: number;
}) {
  const map = useMap();

  useEffect(() => {
    if (selectedLat !== null && selectedLon !== null) {
      map.flyTo([selectedLat, selectedLon], 14.7, { animate: true, duration: 0.9 });
      return;
    }

    const coords = points.map((point) => [point.lat, point.lon] as [number, number]);
    if (coords.length === 0) return;
    if (coords.length === 1) {
      map.setView(coords[0], 11, { animate: true });
      return;
    }

    map.fitBounds(coords, { padding: [40, 40], animate: true, maxZoom: 12 });
  }, [map, points, selectedLat, selectedLon, focusVersion]);

  return null;
}

function scoreColor(tone: 1 | 2 | 3 | 4 | 5): string {
  if (tone === 1) return "#2a8f5d";
  if (tone === 2) return "#73b767";
  if (tone === 3) return "#dfc84f";
  if (tone === 4) return "#de8a42";
  return "#ce4a3c";
}
