import { createServer } from "node:http";
import { readFileSync } from "node:fs";
import { promises as fs } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { CensusGeocoder } from "./geocode.js";
import { RiskEngine } from "./riskEngine.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const projectRoot = path.resolve(__dirname, "..");
const publicDir = path.join(projectRoot, "public");

const riskCells = JSON.parse(
  readFileSync(path.join(projectRoot, "data/risk_cells.geojson"), "utf8")
);
const fallbackAddresses = JSON.parse(
  readFileSync(path.join(projectRoot, "data/address_fallbacks.json"), "utf8")
);
const fallbackCities = JSON.parse(
  readFileSync(path.join(projectRoot, "data/city_fallbacks.json"), "utf8")
);
const weights = JSON.parse(
  readFileSync(path.join(projectRoot, "config/weights.json"), "utf8")
);

const geocoder = new CensusGeocoder({ fallbackAddresses, fallbackCities });
const riskEngine = new RiskEngine({ featureCollection: riskCells, weights });
const CORS_ORIGIN = process.env.CORS_ORIGIN ?? "*";

const server = createServer(async (req, res) => {
  try {
    if (!req.url || !req.method) {
      sendJson(res, 400, { error: "Invalid request." }, req);
      return;
    }

    const url = new URL(req.url, "http://localhost");

    if (req.method === "OPTIONS") {
      sendNoContent(res, req);
      return;
    }

    if (req.method === "GET" && url.pathname === "/api/health") {
      sendJson(res, 200, {
        status: "ok",
        service: "risklens-la-mvp",
        timestamp: new Date().toISOString()
      }, req);
      return;
    }

    if (req.method === "GET" && url.pathname === "/api/config") {
      sendJson(res, 200, riskEngine.getConfig(), req);
      return;
    }

    if (req.method === "POST" && url.pathname === "/api/geocode") {
      const body = await readJsonBody(req);
      const result = await geocoder.geocodeAddress(body.address);
      sendJson(res, 200, result, req);
      return;
    }

    if (req.method === "POST" && url.pathname === "/api/risk") {
      const body = await readJsonBody(req);
      const result = riskEngine.scoreForPoint({
        lon: Number(body.lon),
        lat: Number(body.lat),
        year: body.year ?? 2050,
        scenario: body.scenario ?? "ssp585",
        state: body.state ?? "CA",
        propertyType: body.property_type ?? "homeowner"
      });
      sendJson(res, 200, result, req);
      return;
    }

    if (req.method === "POST" && url.pathname === "/api/geocode-risk") {
      const body = await readJsonBody(req);
      const geocoded = await geocoder.geocodeAddress(body.address);
      const risk = riskEngine.scoreForPoint({
        lon: geocoded.lon,
        lat: geocoded.lat,
        year: body.year ?? 2050,
        scenario: body.scenario ?? "ssp585",
        state: mapStateFipsToAbbrev(geocoded.state_fips) ?? body.state ?? "CA",
        propertyType: body.property_type ?? "homeowner"
      });
      sendJson(res, 200, { geocoded, risk }, req);
      return;
    }

    if (req.method === "GET" && url.pathname === "/api/map-cells") {
      const year = Number(url.searchParams.get("year") ?? 2050);
      const scenario = url.searchParams.get("scenario") ?? "ssp585";
      const hazard = url.searchParams.get("hazard") ?? "combined";
      const layer = riskEngine.mapCells({ year, scenario, hazard });
      sendJson(res, 200, layer, req);
      return;
    }

    await serveStatic(req, res, url.pathname);
  } catch (error) {
    sendJson(res, 400, { error: error.message ?? "Unexpected server error." }, req);
  }
});

const PORT = Number(process.env.PORT ?? 8787);
const HOST = process.env.HOST ?? "0.0.0.0";
server.listen(PORT, HOST, () => {
  // eslint-disable-next-line no-console
  console.log(`RiskLens LA MVP running on http://${HOST}:${PORT}`);
});

async function readJsonBody(req) {
  const chunks = [];
  for await (const chunk of req) {
    chunks.push(chunk);
  }
  if (chunks.length === 0) return {};

  const raw = Buffer.concat(chunks).toString("utf8");
  try {
    return JSON.parse(raw);
  } catch {
    throw new Error("Body must be valid JSON.");
  }
}

function sendJson(res, statusCode, value, req) {
  const body = JSON.stringify(value, null, 2);
  const origin = req?.headers?.origin ?? "";
  const allowOrigin = resolveCorsOrigin(origin);
  res.writeHead(statusCode, {
    "access-control-allow-origin": allowOrigin,
    "access-control-allow-methods": "GET,POST,OPTIONS",
    "access-control-allow-headers": "content-type,authorization",
    "access-control-max-age": "86400",
    "vary": "Origin",
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(body)
  });
  res.end(body);
}

function sendNoContent(res, req) {
  const origin = req?.headers?.origin ?? "";
  const allowOrigin = resolveCorsOrigin(origin);
  res.writeHead(204, {
    "access-control-allow-origin": allowOrigin,
    "access-control-allow-methods": "GET,POST,OPTIONS",
    "access-control-allow-headers": "content-type,authorization",
    "access-control-max-age": "86400",
    "vary": "Origin"
  });
  res.end();
}

async function serveStatic(req, res, pathname) {
  const targetPath = pathname === "/" ? "/index.html" : pathname;
  const resolvedPath = path.resolve(publicDir, `.${targetPath}`);
  if (!resolvedPath.startsWith(publicDir)) {
    sendJson(res, 403, { error: "Forbidden." });
    return;
  }

  try {
    const content = await fs.readFile(resolvedPath);
    res.writeHead(200, { "content-type": contentType(resolvedPath) });
    res.end(content);
  } catch {
    sendJson(res, 404, { error: `Route not found: ${req.method} ${pathname}` });
  }
}

function contentType(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  if (ext === ".html") return "text/html; charset=utf-8";
  if (ext === ".js") return "text/javascript; charset=utf-8";
  if (ext === ".css") return "text/css; charset=utf-8";
  if (ext === ".json") return "application/json; charset=utf-8";
  return "application/octet-stream";
}

function mapStateFipsToAbbrev(stateFips) {
  if (!stateFips) return null;
  if (String(stateFips) === "06") return "CA";
  return null;
}

function resolveCorsOrigin(requestOrigin) {
  if (CORS_ORIGIN === "*") return "*";
  const allowed = CORS_ORIGIN.split(",")
    .map((item) => item.trim())
    .filter(Boolean);
  if (requestOrigin && allowed.includes(requestOrigin)) return requestOrigin;
  return allowed[0] ?? "*";
}
