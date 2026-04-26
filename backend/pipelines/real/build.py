"""Orchestrator for the real-data pipeline.

This is the entrypoint used by:
  - `make build-real`
  - `python -m pipelines.real.build`

It runs the numbered real pipeline steps in order, then writes
`risklens.real.duckdb` via 90_compute_scores.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass


@dataclass(frozen=True)
class Step:
    id: str
    module: str
    description: str


STEP_ORDER: list[Step] = [
    Step("00_grid", "pipelines.real.00_grid", "Build LA tract + H3 cell grid"),
    Step("10_climate", "pipelines.real.10_climate", "Compute CMIP6 climate metrics"),
    Step("11_calfire_fhsz", "pipelines.real.11_calfire_fhsz", "Attach CAL FIRE FHSZ"),
    Step("12_calfire_wui", "pipelines.real.12_calfire_wui", "Attach CAL FIRE WUI"),
    Step("13_fema_nfhl", "pipelines.real.13_fema_nfhl", "Attach FEMA NFHL flood zones"),
    Step("14_noaa_slr", "pipelines.real.14_noaa_slr", "Attach NOAA sea level rise"),
    Step("15_usgs_3dep", "pipelines.real.15_usgs_3dep", "Attach USGS elevation + slope"),
    Step("16_nlcd", "pipelines.real.16_nlcd", "Attach NLCD impervious + tree canopy"),
    Step("17_cdc_svi", "pipelines.real.17_cdc_svi", "Load CDC SVI tract vulnerability"),
    Step("18_census_acs", "pipelines.real.18_census_acs", "Load ACS tract vulnerability"),
    Step("19_fema_nri", "pipelines.real.19_fema_nri", "Load FEMA NRI tract risk"),
    Step("20_lodes", "pipelines.real.20_lodes", "Attach LODES daytime workers"),
    Step("21_la_metro_gtfs", "pipelines.real.21_la_metro_gtfs", "Attach LA Metro exposure"),
    Step("90_compute_scores", "pipelines.real.90_compute_scores", "Compute + load risk DB"),
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--force",
        action="store_true",
        help="Pass --force to each step to rebuild outputs.",
    )
    p.add_argument(
        "--source",
        choices=["nex-gddp", "loca2"],
        default=None,
        help="Climate source for 10_climate (overrides CLIMATE_SOURCE).",
    )
    p.add_argument(
        "--from-step",
        choices=[s.id for s in STEP_ORDER],
        default=STEP_ORDER[0].id,
        help="Start from this step id (inclusive).",
    )
    p.add_argument(
        "--to-step",
        choices=[s.id for s in STEP_ORDER],
        default=STEP_ORDER[-1].id,
        help="Stop at this step id (inclusive).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print step commands without executing.",
    )
    return p.parse_args()


def selected_steps(from_step: str, to_step: str) -> list[Step]:
    ids = [s.id for s in STEP_ORDER]
    i = ids.index(from_step)
    j = ids.index(to_step)
    if i > j:
        raise SystemExit(f"--from-step {from_step} cannot be after --to-step {to_step}")
    return STEP_ORDER[i : j + 1]


def command_for(step: Step, args: argparse.Namespace) -> list[str]:
    cmd = [sys.executable, "-m", step.module]
    if args.force:
        cmd.append("--force")
    if step.id == "10_climate" and args.source:
        cmd.extend(["--source", args.source])
    return cmd


def main() -> None:
    args = parse_args()
    steps = selected_steps(args.from_step, args.to_step)
    print(f"[risklens.real.build] running {len(steps)} step(s)")
    for step in steps:
        cmd = command_for(step, args)
        print(f"[risklens.real.build] {step.id}: {step.description}")
        print(f"[risklens.real.build]   $ {' '.join(cmd)}")
        if args.dry_run:
            continue
        subprocess.run(cmd, check=True)
    print("[risklens.real.build] done.")


if __name__ == "__main__":
    main()
