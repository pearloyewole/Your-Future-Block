import { useEffect, useMemo, useRef } from "react";
import maplibregl, {
  type GeoJSONSource,
  type MapGeoJSONFeature,
  type StyleSpecification,
} from "maplibre-gl";
import type { RiskMapCellFeature, RiskMapCellGeometry, RiskMapCellsResponse } from "@/lib/api";
import type { RiskMapPoint } from "./LAMap";

interface Props {
  cells: RiskMapCellsResponse;
  selectedCellId: string | null;
  selectedLat: number | null;
  selectedLon: number | null;
  focusVersion: number;
  onSelectPoint: (point: RiskMapPoint) => void;
}

const SOURCE_ID = "risk-cells";
const EXTRUSION_LAYER_ID = "risk-cells-3d";
const OUTLINE_LAYER_ID = "risk-cells-outline";
const SELECTED_LAYER_ID = "risk-cells-selected";
const OSM_STYLE: StyleSpecification = {
  version: 8,
  sources: {
    "osm-raster": {
      type: "raster",
      tiles: [
        "https://a.tile.openstreetmap.org/{z}/{x}/{y}.png",
        "https://b.tile.openstreetmap.org/{z}/{x}/{y}.png",
        "https://c.tile.openstreetmap.org/{z}/{x}/{y}.png",
      ],
      tileSize: 256,
      attribution:
        '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap contributors</a>',
    },
  },
  layers: [
    {
      id: "osm-raster-layer",
      type: "raster",
      source: "osm-raster",
      minzoom: 0,
      maxzoom: 22,
    },
  ],
};

export function LA3DMap({
  cells,
  selectedCellId,
  selectedLat,
  selectedLon,
  focusVersion,
  onSelectPoint,
}: Props) {
  const mapContainerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const onSelectPointRef = useRef(onSelectPoint);
  const selectedLatRef = useRef<number | null>(selectedLat);
  const selectedLonRef = useRef<number | null>(selectedLon);
  const pointIndex = useMemo(() => {
    const index = new Map<string, RiskMapPoint>();
    for (const feature of cells.features) {
      const point = mapFeatureToPoint(feature);
      index.set(point.cellId, point);
    }
    return index;
  }, [cells]);
  const pointIndexRef = useRef(pointIndex);
  const fittedRef = useRef(false);

  onSelectPointRef.current = onSelectPoint;
  selectedLatRef.current = selectedLat;
  selectedLonRef.current = selectedLon;
  pointIndexRef.current = pointIndex;

  useEffect(() => {
    if (!mapContainerRef.current || mapRef.current) return;

    const map = new maplibregl.Map({
      container: mapContainerRef.current,
      style: OSM_STYLE,
      center: [-118.2437, 34.0522],
      zoom: 9.3,
      pitch: 48,
      bearing: -18,
      antialias: true,
    });
    mapRef.current = map;

      map.on("load", () => {
      map.addSource(SOURCE_ID, {
        type: "geojson",
        data: cells,
      });

      map.addLayer({
        id: EXTRUSION_LAYER_ID,
        type: "fill-extrusion",
        source: SOURCE_ID,
        paint: {
          "fill-extrusion-color": [
            "step",
            ["coalesce", ["to-number", ["get", "percent_above_median"]], 0],
            "#2a8f5d",
            -30,
            "#73b767",
            -10,
            "#dfc84f",
            10,
            "#de8a42",
            30,
            "#ce4a3c",
          ],
          "fill-extrusion-height": [
            "step",
            ["to-number", ["get", "score"], 0],
            260,
            20,
            650,
            40,
            1200,
            60,
            1800,
            80,
            2600,
          ],
          "fill-extrusion-base": 0,
          "fill-extrusion-opacity": 0.62,
        },
      });

      map.addLayer({
        id: OUTLINE_LAYER_ID,
        type: "line",
        source: SOURCE_ID,
        paint: {
          "line-color": "rgba(255,255,255,0.55)",
          "line-width": 1,
        },
      });

      map.addLayer({
        id: SELECTED_LAYER_ID,
        type: "line",
        source: SOURCE_ID,
        filter: ["==", ["get", "cell_id"], ""],
        paint: {
          "line-color": "#0f172a",
          "line-width": 3,
        },
      });

      map.on("mousemove", EXTRUSION_LAYER_ID, () => {
        map.getCanvas().style.cursor = "pointer";
      });
      map.on("mouseleave", EXTRUSION_LAYER_ID, () => {
        map.getCanvas().style.cursor = "";
      });

        map.on("click", EXTRUSION_LAYER_ID, (event) => {
        const feature = event.features?.[0];
        if (!feature) return;

        const cellId = String(feature.properties?.cell_id ?? "");
        if (!cellId) return;

        const point =
          pointIndexRef.current.get(cellId) ??
          mapFeatureToPoint(normalizeMapFeature(feature));
          onSelectPointRef.current(point);
        });

        const readyLat = selectedLatRef.current;
        const readyLon = selectedLonRef.current;
        if (readyLat !== null && readyLon !== null) {
          flyToSelection(map, readyLat, readyLon);
        }
      });

    return () => {
      map.remove();
      mapRef.current = null;
    };
  }, [cells]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !map.isStyleLoaded()) return;
    const source = map.getSource(SOURCE_ID) as GeoJSONSource | undefined;
    if (!source) return;
    source.setData(cells as GeoJSON.FeatureCollection);

    if (!fittedRef.current && selectedLat === null && selectedLon === null) {
      const bounds = boundsFromFeatures(cells.features);
      if (bounds) {
        map.fitBounds(bounds, { padding: 50, maxZoom: 10.8, animate: true });
        fittedRef.current = true;
      }
    }
  }, [cells, selectedLat, selectedLon]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !map.isStyleLoaded()) return;
    if (!map.getLayer(SELECTED_LAYER_ID)) return;

    map.setFilter(SELECTED_LAYER_ID, [
      "==",
      ["get", "cell_id"],
      selectedCellId ?? "",
    ] as unknown as maplibregl.FilterSpecification);
  }, [selectedCellId]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !map.isStyleLoaded()) return;
    if (selectedLat === null || selectedLon === null) return;
    flyToSelection(map, selectedLat, selectedLon);
  }, [selectedLat, selectedLon, focusVersion]);

  return <div ref={mapContainerRef} className="h-full w-full" />;
}

function boundsFromFeatures(
  features: RiskMapCellFeature[]
): maplibregl.LngLatBoundsLike | null {
  const coords: Array<[number, number]> = [];
  for (const feature of features) {
    coords.push(...flattenGeometry(feature.geometry));
  }
  if (coords.length === 0) return null;

  let minLon = coords[0][0];
  let maxLon = coords[0][0];
  let minLat = coords[0][1];
  let maxLat = coords[0][1];

  for (const [lon, lat] of coords) {
    minLon = Math.min(minLon, lon);
    maxLon = Math.max(maxLon, lon);
    minLat = Math.min(minLat, lat);
    maxLat = Math.max(maxLat, lat);
  }

  return [
    [minLon, minLat],
    [maxLon, maxLat],
  ];
}

function flattenGeometry(geometry: RiskMapCellGeometry): Array<[number, number]> {
  if (geometry.type === "Point") return [geometry.coordinates];
  if (geometry.type === "Polygon") return geometry.coordinates[0] ?? [];
  if (geometry.type === "MultiPolygon") return geometry.coordinates[0]?.[0] ?? [];
  return [];
}

function mapFeatureToPoint(feature: RiskMapCellFeature): RiskMapPoint {
  const [lon, lat] = centerFromGeometry(feature.geometry);
  return {
    cellId: feature.properties.cell_id,
    lat,
    lon,
    score: Number(feature.properties.score ?? 0),
    label: String(feature.properties.label ?? "Unknown"),
    neighborhood: feature.properties.neighborhood,
    tractFips: feature.properties.tract_fips,
    percentAboveMedian: feature.properties.percent_above_median ?? null,
  };
}

function centerFromGeometry(geometry: RiskMapCellGeometry): [number, number] {
  const coords = flattenGeometry(geometry);
  if (coords.length === 0) return [-118.2437, 34.0522];

  const total = coords.reduce(
    (acc, [lon, lat]) => ({ lon: acc.lon + lon, lat: acc.lat + lat }),
    { lon: 0, lat: 0 }
  );

  return [total.lon / coords.length, total.lat / coords.length];
}

function normalizeMapFeature(feature: MapGeoJSONFeature): RiskMapCellFeature {
  const geometry = feature.geometry as RiskMapCellGeometry;
  return {
    type: "Feature",
    geometry,
    properties: {
      cell_id: String(feature.properties?.cell_id ?? ""),
      neighborhood: (feature.properties?.neighborhood as string | null) ?? null,
      tract_fips: (feature.properties?.tract_fips as string | null) ?? null,
      hazard: String(feature.properties?.hazard ?? ""),
      score: Number(feature.properties?.score ?? 0),
      label: String(feature.properties?.label ?? "Unknown"),
      la_median: Number(feature.properties?.la_median ?? 0) || null,
      percent_above_median:
        Number(feature.properties?.percent_above_median ?? 0) ||
        (feature.properties?.percent_above_median === 0 ? 0 : null),
    },
  };
}

function flyToSelection(map: maplibregl.Map, lat: number, lon: number) {
  map.flyTo({
    center: [lon, lat],
    zoom: 14.6,
    pitch: 62,
    bearing: -18,
    speed: 0.9,
    essential: true,
  });
}
