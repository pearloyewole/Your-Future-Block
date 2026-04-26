import { Year, YEARS } from "@/data/laBlocks";

interface Props {
  values: Record<Year, number>;
  colorVar: string; // CSS var name like "--heat"
  highlightYear: Year;
  active: boolean;
}

export function Sparkline({ values, colorVar, highlightYear, active }: Props) {
  const w = 160;
  const h = 40;
  const pad = 4;
  const xs = YEARS.map((y, i) => pad + (i * (w - pad * 2)) / (YEARS.length - 1));
  const ys = YEARS.map((y) => h - pad - (values[y] / 100) * (h - pad * 2));
  const path = xs.map((x, i) => `${i === 0 ? "M" : "L"}${x},${ys[i]}`).join(" ");
  const areaPath = `${path} L${xs[xs.length - 1]},${h - pad} L${xs[0]},${h - pad} Z`;
  const stroke = `hsl(var(${colorVar}))`;
  const opacity = active ? 1 : 0.25;

  return (
    <svg viewBox={`0 0 ${w} ${h}`} className="h-10 w-full" style={{ opacity }}>
      <path d={areaPath} fill={stroke} fillOpacity={0.15} />
      <path d={path} fill="none" stroke={stroke} strokeWidth={1.75} />
      {YEARS.map((y, i) => (
        <circle
          key={y}
          cx={xs[i]}
          cy={ys[i]}
          r={y === highlightYear ? 3.2 : 1.8}
          fill={stroke}
          stroke="hsl(var(--card))"
          strokeWidth={y === highlightYear ? 1.5 : 0}
        />
      ))}
    </svg>
  );
}