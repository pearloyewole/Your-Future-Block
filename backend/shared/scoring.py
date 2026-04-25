"""Pure scoring math, shared by both pipelines.

Inputs:
    attrs   : DataFrame keyed by cell_id with all per-cell static modifiers
              (canopy, impervious, fhsz_class, wui_class, flood_zone,
               slr_inundated_ft, slope_deg, svi_overall, ...).
    climate : DataFrame keyed by (cell_id, window, scenario) with the
              five climate metrics (heat_days, warm_nights, pr_annual_mm,
              pr_p99_mm, cdd_max). Must include the historical baseline.
    weights : dict loaded from shared/weights.yaml.

Output:
    risk_df : DataFrame with one row per (cell_id, window, scenario) future
              combination, containing per-hazard scores (0..100), labels,
              and a JSON `drivers` snapshot for the LLM.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

WEIGHTS_PATH = Path(__file__).parent / "weights.yaml"
SCHEMA_PATH = Path(__file__).parent / "schema.sql"

BASELINE_WINDOW = "1981-2010"
HISTORICAL_SCENARIO = "historical"
FUTURE_WINDOWS = ["2021-2040", "2041-2060", "2071-2090", "2081-2100"]
FUTURE_SCENARIOS = ["ssp245", "ssp370", "ssp585"]


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------
def load_weights() -> dict:
    return yaml.safe_load(WEIGHTS_PATH.read_text())


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def normalize(x, lo: float, hi: float) -> np.ndarray:
    """Clamp linear scaling of x into 0..100. NaN -> 0."""
    arr = np.asarray(x, dtype="float64")
    arr = np.where(np.isnan(arr), 0.0, arr)
    if hi == lo:
        return np.clip(arr, 0, 100)
    return np.clip((arr - lo) / (hi - lo) * 100.0, 0.0, 100.0)


def label_one(score: float, labels: list[list]) -> str:
    for lo, hi, name in labels:
        if lo <= score <= hi:
            return name
    return "Unknown"


def label_vec(scores: np.ndarray, labels: list[list]) -> np.ndarray:
    out = np.empty(len(scores), dtype=object)
    for i, s in enumerate(scores):
        out[i] = label_one(float(s), labels)
    return out


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------
def compute_scores(attrs: pd.DataFrame, climate: pd.DataFrame, weights: dict) -> pd.DataFrame:
    """Return a DataFrame with one row per (cell, window, scenario)."""
    base = climate[
        (climate["window"] == BASELINE_WINDOW)
        & (climate["scenario"] == HISTORICAL_SCENARIO)
    ][["cell_id", "heat_days", "warm_nights", "pr_p99_mm", "pr_annual_mm"]].rename(
        columns={
            "heat_days":     "heat_days_base",
            "warm_nights":   "warm_nights_base",
            "pr_p99_mm":     "pr_p99_mm_base",
            "pr_annual_mm":  "pr_annual_mm_base",
        }
    )

    fut = climate[
        (climate["window"].isin(FUTURE_WINDOWS))
        & (climate["scenario"].isin(FUTURE_SCENARIOS))
    ].merge(base, on="cell_id", how="left").merge(attrs, on="cell_id", how="left")

    # ---- Heat ---------------------------------------------------------------
    h = weights["heat"]
    extra_heat_days = (fut["heat_days"] - fut["heat_days_base"]).clip(lower=0)
    extra_warm_nights = (fut["warm_nights"] - fut["warm_nights_base"]).clip(lower=0)

    fut_score = 0.5 * normalize(extra_heat_days,
                                *h["bounds"]["extra_heat_days_vs_baseline"]) \
              + 0.5 * normalize(extra_warm_nights,
                                *h["bounds"]["extra_warm_nights_vs_baseline"])

    canopy_def = (100 - fut["tree_canopy_pct"].fillna(50)).clip(0, 100)
    local_score = 0.6 * normalize(canopy_def,
                                  *h["bounds"]["canopy_deficit_pct"]) \
                + 0.4 * normalize(fut["impervious_pct"].fillna(50),
                                  *h["bounds"]["impervious_pct"])

    vuln_score = normalize(fut["svi_overall"].fillna(0.5),
                           *h["bounds"]["svi_overall"])

    heat_score = (
        h["future_exposure"] * fut_score
        + h["local_amplifier"] * local_score
        + h["social_vulnerability"] * vuln_score
    )

    # ---- Wildfire -----------------------------------------------------------
    wf = weights["wildfire"]
    fhsz_score = fut["fhsz_class"].map(wf["fhsz_score"]).fillna(0).astype(float).values
    wui_bump = fut["wui_class"].map(wf["wui_bump"]).fillna(0).astype(float).values
    base_wf = np.clip(fhsz_score + wui_bump, 0, 100)

    pr_delta_pct = ((fut["pr_annual_mm_base"] - fut["pr_annual_mm"])
                    / fut["pr_annual_mm_base"].replace(0, np.nan) * 100).fillna(0)
    future_stress_raw = extra_heat_days + np.clip(pr_delta_pct, 0, 100) * 0.5
    fut_climate_stress = normalize(future_stress_raw,
                                   *wf["bounds"]["future_stress"])

    terrain_score = normalize(fut["slope_deg"].fillna(0),
                              *wf["bounds"]["slope_deg"])
    wf_vuln = normalize(fut["svi_overall"].fillna(0.5),
                        *wf["bounds"]["svi_overall"])

    wildfire_score = (
        wf["baseline_hazard"] * base_wf
        + wf["future_climate_stress"] * fut_climate_stress
        + wf["terrain"] * terrain_score
        + wf["social_vulnerability"] * wf_vuln
    )

    # ---- Flood --------------------------------------------------------------
    fl = weights["flood"]

    coastal_lookup = {
        (None if k is None else int(k)): float(v)
        for k, v in fl["coastal"]["inundation_score"].items()
    }

    def _coastal_one(ft):
        if pd.isna(ft):
            return coastal_lookup[None]
        return coastal_lookup.get(int(ft), coastal_lookup[None])

    coastal_arr = fut["slr_inundated_ft"].apply(_coastal_one).values

    fema_score = fut["flood_zone"].map(fl["inland"]["fema_zone_score"]).fillna(5).astype(float).values
    pr_p99_pct = ((fut["pr_p99_mm"] - fut["pr_p99_mm_base"])
                  / fut["pr_p99_mm_base"].replace(0, np.nan) * 100).fillna(0)
    pr_p99_score = normalize(pr_p99_pct, *fl["inland"]["bounds"]["precip_p99_delta_pct"])
    impervious_bump = fl["inland"]["impervious_weight"] * fut["impervious_pct"].fillna(0)
    inland_arr = np.clip(fema_score + impervious_bump.values * 0.5 + pr_p99_score * 0.3, 0, 100)

    flood_score = np.maximum(coastal_arr, inland_arr)

    # ---- Overall ------------------------------------------------------------
    o = weights["overall"]
    overall = o["heat"] * heat_score + o["wildfire"] * wildfire_score + o["flood"] * flood_score

    labels = weights["labels"]
    out = pd.DataFrame({
        "cell_id": fut["cell_id"].values,
        "window":  fut["window"].values,
        "scenario": fut["scenario"].values,
        "heat_score": heat_score,
        "wildfire_score": wildfire_score,
        "flood_score": flood_score,
        "overall_score": overall,
        "heat_label": label_vec(heat_score, labels),
        "wildfire_label": label_vec(wildfire_score, labels),
        "flood_label": label_vec(flood_score, labels),
        "overall_label": label_vec(overall, labels),
    })

    driver_cols = [
        "tree_canopy_pct", "impervious_pct", "fhsz_class", "wui_class",
        "flood_zone", "in_100yr", "in_500yr", "slr_inundated_ft",
        "elevation_m", "slope_deg", "svi_overall", "pct_age_65plus",
        "pct_no_vehicle", "pct_below_poverty",
        "heat_days", "heat_days_base", "warm_nights", "warm_nights_base",
        "pr_p99_mm", "pr_p99_mm_base",
        "daytime_workers", "transit_stops_400m",
    ]
    driver_cols = [c for c in driver_cols if c in fut.columns]
    drivers_df = fut[driver_cols].copy()
    out["drivers"] = [
        json.dumps({k: (None if (isinstance(v, float) and np.isnan(v)) else
                       (bool(v) if isinstance(v, (bool, np.bool_)) else v))
                    for k, v in row.items()},
                   default=lambda x: float(x) if isinstance(x, (np.floating,)) else
                                     int(x) if isinstance(x, (np.integer,)) else str(x))
        for row in drivers_df.to_dict("records")
    ]
    return out
