import { Link } from "react-router-dom";
import { ArrowRight } from "lucide-react";

/* ----------------------------- shared atoms ----------------------------- */

const SectionLabel = ({ children }: { children: React.ReactNode }) => (
  <p className="font-mono text-[11px] uppercase tracking-[0.18em] text-accent">{children}</p>
);

const Marker = ({ children }: { children: React.ReactNode }) => (
  <em className="relative not-italic font-display italic">
    <span
      aria-hidden
      className="absolute inset-x-[-0.1em] bottom-[0.06em] top-[0.18em] -z-10 bg-highlight"
    />
    {children}
  </em>
);

const Underline = ({ children }: { children: React.ReactNode }) => (
  <span className="relative inline-block">
    {children}
    <span
      aria-hidden
      className="absolute inset-x-0 -bottom-1 h-[3px] bg-accent"
    />
  </span>
);

/* --------------------------- stylized heatmap --------------------------- */

const HeatmapStrip = () => {
  // Procedural-ish grid of warm/cool blocks, divided by a "river"
  const rows = 14;
  const cols = 32;
  const colors = [
    "hsl(205 80% 35%)",
    "hsl(195 70% 45%)",
    "hsl(170 60% 50%)",
    "hsl(95 55% 55%)",
    "hsl(55 85% 55%)",
    "hsl(30 90% 55%)",
    "hsl(10 85% 50%)",
    "hsl(358 78% 45%)",
  ];
  // deterministic pseudo-random
  const rand = (i: number, j: number) => {
    const x = Math.sin(i * 12.9898 + j * 78.233) * 43758.5453;
    return x - Math.floor(x);
  };
  return (
    <div className="overflow-hidden rounded-md border border-border shadow-soft">
      <svg viewBox={`0 0 ${cols * 20} ${rows * 20}`} className="block h-auto w-full">
        {Array.from({ length: rows }).map((_, r) =>
          Array.from({ length: cols }).map((_, c) => {
            // river column band
            const riverDist = Math.abs(c - cols / 2 + Math.sin(r * 0.6) * 1.2);
            if (riverDist < 1.4) {
              return (
                <rect
                  key={`${r}-${c}`}
                  x={c * 20}
                  y={r * 20}
                  width={20}
                  height={20}
                  fill="hsl(215 65% 22%)"
                />
              );
            }
            const noise = rand(r, c);
            const heat = Math.min(1, Math.max(0, 0.55 - riverDist * 0.04 + noise * 0.7));
            const idx = Math.min(colors.length - 1, Math.floor(heat * colors.length));
            return (
              <rect
                key={`${r}-${c}`}
                x={c * 20}
                y={r * 20}
                width={20}
                height={20}
                fill={colors[idx]}
              />
            );
          }),
        )}
      </svg>
    </div>
  );
};

/* --------------------------- page ---------------------------- */

const Index = () => {
  return (
    <div className="min-h-screen bg-background text-foreground">
      {/* Top bar */}
      <header className="border-b border-border/60">
        <div className="mx-auto flex max-w-7xl items-center justify-between px-6 py-4">
          <span className="font-display text-base italic">Your Future Block</span>
          <Link
            to="/atlas"
            className="font-mono text-[11px] uppercase tracking-[0.18em] text-muted-foreground transition hover:text-accent"
          >
            Launch atlas →
          </Link>
        </div>
      </header>

      {/* 01 — Hero */}
      <section className="mx-auto max-w-7xl px-6 pb-20 pt-14 md:pt-20">
        <SectionLabel>A climate awareness project</SectionLabel>

        <h1 className="mt-8 font-display text-[12vw] font-bold leading-[0.95] tracking-tight md:text-8xl lg:text-9xl">
          Your <em className="font-display italic">Future</em> Block
        </h1>

        <div className="mt-10 grid gap-6 md:grid-cols-[1.2fr_1fr] md:items-end">
          <p className="font-display text-3xl leading-tight md:text-4xl">
            What will your community look<br className="hidden md:block" /> like <Marker>tomorrow?</Marker>
          </p>
          <div className="md:text-right">
            <div className="flex flex-wrap items-center gap-4 md:justify-end">
              <span className="inline-flex items-center gap-1.5 font-mono text-[11px] uppercase tracking-[0.16em] text-muted-foreground">
                <span className="h-2 w-2 rounded-full bg-heat" /> Heat
              </span>
              <span className="inline-flex items-center gap-1.5 font-mono text-[11px] uppercase tracking-[0.16em] text-muted-foreground">
                <span className="h-2 w-2 rounded-full bg-fire" /> Wildfire
              </span>
              <span className="inline-flex items-center gap-1.5 font-mono text-[11px] uppercase tracking-[0.16em] text-muted-foreground">
                <span className="h-2 w-2 rounded-full bg-flood" /> Flood
              </span>
            </div>
            <p className="mt-2 font-mono text-[11px] uppercase tracking-[0.16em] text-muted-foreground md:text-right">
              2030 · 2050 · 2100 projections
            </p>
          </div>
        </div>

        <div className="mt-12">
          <HeatmapStrip />
          <p className="mt-3 font-mono text-xs text-muted-foreground">
            Fig. 1 — Modeled surface heat overlaid against residential blocks. Source: CMIP6 downscaled projections.
          </p>
        </div>
      </section>

      {/* 02 — The stat */}
      <section className="border-t border-border/60">
        <div className="mx-auto max-w-7xl px-6 py-24 text-center">
          <SectionLabel>Nature Sustainability · 2026</SectionLabel>
          <p className="mx-auto mt-10 max-w-4xl font-display text-4xl leading-tight md:text-6xl">
            <em className="italic">"Climate change</em> <Underline>doesn't affect me…"</Underline>
          </p>

          <div className="mx-auto mt-16 grid max-w-5xl gap-10 border-t border-border pt-10 md:grid-cols-3">
            {[
              { n: "60", l: "Datasets analyzed" },
              { n: "70,337", l: "Participants surveyed" },
              { n: "81/83", l: "Datasets where people rated their own risk lower than others'" },
            ].map((s) => (
              <div key={s.l} className="text-left">
                <p className="font-display text-6xl font-bold leading-none md:text-7xl">{s.n}</p>
                <p className="mt-3 max-w-[14rem] font-mono text-xs uppercase tracking-[0.14em] text-muted-foreground">
                  {s.l}
                </p>
              </div>
            ))}
          </div>

          <p className="mx-auto mt-16 max-w-2xl font-mono text-xs leading-relaxed text-muted-foreground">
            Sandlund, Bjälkebring &amp; Bergquist. Meta-analytical evidence of a self–other discrepancy in
            climate-related risk perceptions. <em>Nat Sustain</em> 9, 377–384 (2026).
          </p>
        </div>
      </section>

      {/* 03 — The question */}
      <section className="border-t border-border/60 bg-secondary/40">
        <div className="mx-auto max-w-7xl px-6 py-24">
          <SectionLabel>02 — The question</SectionLabel>
          <h2 className="mt-8 max-w-5xl font-display text-5xl font-bold leading-[1.05] tracking-tight md:text-6xl lg:text-7xl">
            What if we could <Marker>understand,</Marker> <Marker>quantify,</Marker> and{" "}
            <Marker>communicate</Marker> what our communities will look like…
          </h2>

          <div className="mt-16 grid gap-10 border-t border-border pt-10 md:grid-cols-3">
            {[
              { tag: "Tomorrow", body: "Today's exposure, mapped block-by-block." },
              { tag: "In 30 years", body: "Mid-century projections under multiple emissions paths." },
              { tag: "A century from now", body: "The long view — for the kids on this street." },
            ].map((c) => (
              <div key={c.tag}>
                <p className="font-mono text-[11px] uppercase tracking-[0.18em] text-accent">{c.tag}</p>
                <p className="mt-3 font-display text-xl leading-snug">{c.body}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* 04 — The platform (dark) */}
      <section className="bg-foreground text-background">
        <div className="mx-auto max-w-7xl px-6 py-24">
          <p className="font-mono text-[11px] uppercase tracking-[0.18em] text-accent">03 — The platform</p>
          <div className="mt-8 grid gap-12 md:grid-cols-2 md:gap-16">
            <div>
              <h2 className="font-display text-6xl font-bold leading-[1.0] tracking-tight md:text-7xl">
                Your Future<br /> Block
              </h2>
              <p className="mt-8 max-w-md font-display text-xl italic leading-snug text-background/70">
                A platform for consolidating and bringing awareness to climate risk for community members.
              </p>
            </div>
            <div>
              <p className="font-display text-2xl leading-snug md:text-3xl">
                <em className="italic">Your Future Block</em> lets anyone enter an LA address and see how{" "}
                <span className="text-heat font-semibold not-italic">heat</span>,{" "}
                <span className="text-fire font-semibold not-italic">wildfire</span>, and{" "}
                <span className="text-flood font-semibold not-italic">flood</span> exposure may change by{" "}
                <span className="font-mono">2030</span>, <span className="font-mono">2050</span>, and{" "}
                <span className="font-mono">2100</span> under different emissions scenarios.
              </p>

              <div className="mt-12 border-t border-background/15 pt-6">
                <p className="font-mono text-[11px] uppercase tracking-[0.18em] text-background/55">
                  Heat exposure · 2025 → 2100 (illustrative)
                </p>
                <div
                  className="mt-3 h-2 w-full rounded-full"
                  style={{
                    background:
                      "linear-gradient(90deg, hsl(200 70% 70%), hsl(50 90% 60%), hsl(28 90% 55%), hsl(358 78% 48%))",
                  }}
                />
                <div className="mt-2 flex justify-between font-mono text-[11px] text-background/55">
                  <span>Today</span>
                  <span>+1.5°C</span>
                  <span>+2.5°C</span>
                  <span>+4°C</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* 05 — Method */}
      <section className="border-t border-border/60">
        <div className="mx-auto max-w-7xl px-6 py-24">
          <SectionLabel>04 — Method</SectionLabel>
          <div className="mt-8 grid gap-12 md:grid-cols-2">
            <h2 className="font-display text-6xl font-bold leading-[1.0] tracking-tight md:text-7xl">
              Public data,<br /> honestly<br /> assembled.
            </h2>
            <p className="self-end max-w-md font-display text-xl leading-snug text-muted-foreground">
              Every layer is sourced, every assumption is named. The goal isn't a verdict — it's a conversation
              your block can actually have.
            </p>
          </div>

          <div className="mt-14 grid gap-x-12 gap-y-10 border-t border-border pt-10 md:grid-cols-2">
            {[
              { i: "i.", t: "Downscaled climate projections", b: "CMIP6 scenarios localized to neighborhood-scale grids across the LA basin." },
              { i: "ii.", t: "Official hazard maps", b: "FEMA flood zones, CAL FIRE severity zones, and urban heat island layers." },
              { i: "iii.", t: "Neighborhood vulnerability", b: "Census-tract demographics blended in to surface where impact compounds." },
              { i: "iv.", t: "A score, in plain English", b: "Transparent risk score with a paragraph anyone can read — no jargon." },
            ].map((m, idx) => (
              <div
                key={m.i}
                className={`pb-8 ${idx % 2 === 0 ? "md:border-r md:border-border md:pr-12" : ""} ${
                  idx < 2 ? "md:border-b md:border-border" : ""
                }`}
              >
                <p className="font-mono text-sm text-accent">{m.i}</p>
                <h3 className="mt-3 font-display text-2xl font-bold leading-tight">{m.t}</h3>
                <p className="mt-3 max-w-md text-base leading-relaxed text-muted-foreground">{m.b}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* 06 — CTA */}
      <section className="border-t border-border/60">
        <div className="mx-auto max-w-7xl px-6 py-28 text-center">
          <SectionLabel>Now in preview · Los Angeles</SectionLabel>
          <h2 className="mx-auto mt-10 max-w-3xl font-display text-5xl font-bold leading-[1.05] tracking-tight md:text-7xl">
            See your block.<br />
            <em className="italic">Then see it in 2050.</em>
          </h2>
          <p className="mx-auto mt-8 max-w-xl font-display text-xl leading-snug text-muted-foreground">
            Open the interactive atlas and explore heat, wildfire, and flood projections for any LA neighborhood.
          </p>

          <div className="mt-10 flex flex-wrap items-center justify-center gap-3">
            <Link
              to="/atlas"
              className="inline-flex items-center gap-2 bg-accent px-7 py-4 font-mono text-sm uppercase tracking-[0.16em] text-accent-foreground transition hover:bg-accent/90"
            >
              Launch the atlas <ArrowRight className="h-4 w-4" />
            </Link>
            <a
              href="#"
              className="inline-flex items-center gap-2 border border-foreground px-7 py-4 font-mono text-sm uppercase tracking-[0.16em] text-foreground transition hover:bg-foreground hover:text-background"
            >
              Read the brief
            </a>
          </div>

          <p className="mt-8 font-mono text-xs text-muted-foreground">
            A Hacktech 2026 project · Built with public data
          </p>
        </div>
      </section>

      {/* Footer */}
      <footer className="border-t border-border/60">
        <div className="mx-auto flex max-w-7xl flex-col items-center justify-between gap-2 px-6 py-6 md:flex-row">
          <span className="font-display text-sm italic">Your Future Block</span>
          <span className="font-mono text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
            © 2026 · All rights reserved
          </span>
          <span className="font-mono text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
            Los Angeles, CA
          </span>
        </div>
      </footer>
    </div>
  );
};

export default Index;
