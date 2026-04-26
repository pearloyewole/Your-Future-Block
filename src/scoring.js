export function clamp0to100(value) {
  return Math.max(0, Math.min(100, value));
}

export function round1(value) {
  return Math.round(value * 10) / 10;
}

export function scoreLabel(score) {
  if (score <= 20) return "Very Low";
  if (score <= 40) return "Low";
  if (score <= 60) return "Moderate";
  if (score <= 80) return "High";
  return "Very High";
}

export function normalizeScenario(input, aliases) {
  const raw = String(input ?? "ssp370")
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/_/g, "");

  const normalized =
    aliases[raw] ??
    aliases[raw.replace(/\./g, "")] ??
    aliases[raw.replace(/-/g, "")];

  if (!normalized) {
    throw new Error(
      `Unsupported scenario "${input}". Use SSP2-4.5, SSP3-7.0, or SSP5-8.5.`
    );
  }
  return normalized;
}

export function normalizeYear(input, validYears) {
  const year = Number(input);
  if (!validYears.includes(year)) {
    throw new Error(
      `Unsupported year "${input}". Use one of: ${validYears.join(", ")}.`
    );
  }
  return year;
}

export function localHeatAmplifier({
  imperviousPct,
  treeCanopyPct,
  buildingDensityIdx
}) {
  const treePenalty = 100 - treeCanopyPct;
  return clamp0to100(
    imperviousPct * 0.5 + treePenalty * 0.35 + buildingDensityIdx * 0.15
  );
}

export function floodLocalPhysical({ drainageStressIdx, imperviousPct }) {
  return clamp0to100(drainageStressIdx * 0.7 + imperviousPct * 0.3);
}
