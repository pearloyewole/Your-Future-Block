const EARTH_RADIUS_M = 6371008.8;

export function pointInPolygon(point, ring) {
  const [x, y] = point;
  let inside = false;

  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const [xi, yi] = ring[i];
    const [xj, yj] = ring[j];

    const intersects =
      yi > y !== yj > y &&
      x < ((xj - xi) * (y - yi)) / (yj - yi + Number.EPSILON) + xi;

    if (intersects) inside = !inside;
  }

  return inside;
}

export function polygonCentroid(ring) {
  let twiceArea = 0;
  let x = 0;
  let y = 0;

  for (let i = 0; i < ring.length - 1; i += 1) {
    const [x0, y0] = ring[i];
    const [x1, y1] = ring[i + 1];
    const f = x0 * y1 - x1 * y0;
    twiceArea += f;
    x += (x0 + x1) * f;
    y += (y0 + y1) * f;
  }

  if (Math.abs(twiceArea) < Number.EPSILON) {
    return ring[0];
  }

  const factor = 1 / (3 * twiceArea);
  return [x * factor, y * factor];
}

export function haversineMeters(a, b) {
  const [lon1, lat1] = a;
  const [lon2, lat2] = b;

  const toRad = (value) => (value * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const lat1Rad = toRad(lat1);
  const lat2Rad = toRad(lat2);

  const sinDLat = Math.sin(dLat / 2);
  const sinDLon = Math.sin(dLon / 2);
  const c =
    sinDLat * sinDLat +
    Math.cos(lat1Rad) * Math.cos(lat2Rad) * sinDLon * sinDLon;
  const angular = 2 * Math.atan2(Math.sqrt(c), Math.sqrt(1 - c));
  return EARTH_RADIUS_M * angular;
}
