export class CensusGeocoder {
  constructor({ fallbackAddresses = [], fallbackCities = [] } = {}) {
    this.fallbackAddresses = fallbackAddresses;
    this.fallbackCities = fallbackCities;
  }

  async geocodeAddress(address) {
    const trimmed = String(address ?? "").trim();
    if (!trimmed) {
      throw new Error("Address is required.");
    }

    const liveResult = await this.tryCensus(trimmed);
    if (liveResult) return liveResult;

    const liveLocationOnlyResult = await this.tryCensusLocationOnly(trimmed);
    if (liveLocationOnlyResult) return liveLocationOnlyResult;

    const fallbackResult = this.tryFallback(trimmed);
    if (fallbackResult) return fallbackResult;

    const cityFallback = this.tryCityFallback(trimmed);
    if (cityFallback) return cityFallback;

    throw new Error(
      "Address lookup failed. Try a full U.S. street address (number + street + city + state), e.g. 200 N Spring St, Los Angeles, CA."
    );
  }

  async tryCensus(address) {
    const endpoint = new URL(
      "https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress"
    );
    endpoint.searchParams.set("address", address);
    endpoint.searchParams.set("benchmark", "Public_AR_Current");
    endpoint.searchParams.set("vintage", "Current_Current");
    endpoint.searchParams.set("format", "json");

    try {
      const response = await fetch(endpoint, {
        headers: { accept: "application/json" }
      });
      if (!response.ok) return null;
      const payload = await response.json();
      const match = payload?.result?.addressMatches?.[0];
      if (!match) return null;

      const geographies = match.geographies ?? {};
      const tracts = geographies["Census Tracts"] ?? geographies["Census Tract"];
      const tract = Array.isArray(tracts) ? tracts[0] : null;
      const state = tract?.STATE ?? "";
      const county = tract?.COUNTY ?? "";
      const tractCode = tract?.TRACT ?? "";
      const tractFips = `${state}${county}${tractCode}`;

      return {
        source: "us_census_geocoder",
        input_address: address,
        matched_address: match.matchedAddress,
        lat: match.coordinates.y,
        lon: match.coordinates.x,
        tract_fips: tractFips || null,
        county_fips: county || null,
        state_fips: state || null
      };
    } catch {
      return null;
    }
  }

  async tryCensusLocationOnly(address) {
    const endpoint = new URL(
      "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
    );
    endpoint.searchParams.set("address", address);
    endpoint.searchParams.set("benchmark", "Public_AR_Current");
    endpoint.searchParams.set("format", "json");

    try {
      const response = await fetch(endpoint, {
        headers: { accept: "application/json" }
      });
      if (!response.ok) return null;
      const payload = await response.json();
      const match = payload?.result?.addressMatches?.[0];
      if (!match) return null;

      return {
        source: "us_census_geocoder_location_only",
        input_address: address,
        matched_address: match.matchedAddress ?? null,
        lat: match.coordinates?.y ?? null,
        lon: match.coordinates?.x ?? null,
        tract_fips: null,
        county_fips: null,
        state_fips: null
      };
    } catch {
      return null;
    }
  }

  tryFallback(address) {
    const normalized = address.toLowerCase();
    const direct = this.fallbackAddresses.find(
      (item) => item.address.toLowerCase() === normalized
    );
    if (direct) return shapeFallbackResult(address, direct);

    const includes = this.fallbackAddresses.find((item) => {
      const candidate = item.address.toLowerCase();
      return normalized.includes(candidate) || candidate.includes(normalized);
    });
    if (includes) return shapeFallbackResult(address, includes);

    return null;
  }

  tryCityFallback(address) {
    const normalizedAddress = normalize(address);
    for (const city of this.fallbackCities) {
      const normalizedCity = normalize(city.city);
      const normalizedState = normalize(city.state ?? "");

      const cityMatch =
        normalizedAddress === normalizedCity ||
        normalizedAddress.includes(`${normalizedCity},${normalizedState}`) ||
        normalizedAddress.includes(`${normalizedCity} ${normalizedState}`) ||
        normalizedAddress.includes(`${normalizedCity}, ca`) ||
        normalizedAddress.includes(`${normalizedCity} ca`) ||
        normalizedAddress.includes(normalizedCity);

      if (!cityMatch) continue;

      return {
        source: "local_city_fallback",
        input_address: address,
        matched_address: city.display_name ?? `${city.city}, ${city.state ?? "CA"}`,
        lat: city.lat,
        lon: city.lon,
        tract_fips: null,
        county_fips: city.county_fips ?? null,
        state_fips: city.state_fips ?? "06"
      };
    }

    return null;
  }
}

function shapeFallbackResult(inputAddress, value) {
  const tractFips = value.tract_fips ?? null;
  return {
    source: "local_fallback",
    input_address: inputAddress,
    matched_address: value.display_name ?? value.address,
    lat: value.lat,
    lon: value.lon,
    tract_fips: tractFips,
    county_fips: tractFips ? tractFips.slice(2, 5) : null,
    state_fips: tractFips ? tractFips.slice(0, 2) : null
  };
}

function normalize(value) {
  return String(value ?? "")
    .toLowerCase()
    .trim()
    .replace(/\s+/g, " ")
    .replace(/[.]/g, "");
}
