export type RiskTone = 1 | 2 | 3 | 4 | 5;

export function toneFromScore(score: number): RiskTone {
  if (score < 20) return 1;
  if (score < 40) return 2;
  if (score < 60) return 3;
  if (score < 80) return 4;
  return 5;
}

export function toneFromPercentAboveMedian(
  percentAboveMedian: number | null | undefined
): RiskTone | null {
  if (percentAboveMedian === null || percentAboveMedian === undefined) return null;
  if (Number.isNaN(percentAboveMedian)) return null;

  if (percentAboveMedian <= -30) return 1;
  if (percentAboveMedian < -10) return 2;
  if (percentAboveMedian < 10) return 3;
  if (percentAboveMedian < 30) return 4;
  return 5;
}

export function toneForRisk(input: {
  score: number;
  percentAboveMedian: number | null | undefined;
}): RiskTone {
  return toneFromPercentAboveMedian(input.percentAboveMedian) ?? toneFromScore(input.score);
}
