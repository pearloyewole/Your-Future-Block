
from __future__ import annotations

import logging
from typing import Any

from ..schemas import CellRisk
from ..settings import settings

log = logging.getLogger("risklens.api.explain")


# ---------------------------------------------------------------------------
# Deterministic fallback
# ---------------------------------------------------------------------------
def _label_phrase(score: float) -> str:
    if score >= 81: return "very high"
    if score >= 61: return "high"
    if score >= 41: return "moderate"
    if score >= 21: return "low"
    return "very low"


def _scenario_phrase(scen: str) -> str:
    return {
        "ssp245": "a moderate-emissions scenario (SSP2-4.5)",
        "ssp370": "a high-emissions scenario (SSP3-7.0)",
        "ssp585": "a very high-emissions scenario (SSP5-8.5)",
    }.get(scen, scen)


def _window_phrase(win: str) -> str:
    return {
        "2021-2040": "around 2030",
        "2041-2060": "around 2050",
        "2071-2090": "around 2080",
        "2081-2100": "around 2100",
    }.get(win, win)


def fallback_explanation(risk: CellRisk) -> str:
    """A clear, deterministic paragraph -- no LLM required."""
    d = risk.drivers or {}
    win = _window_phrase(risk.window)
    scen = _scenario_phrase(risk.scenario)

    # Identify the dominant hazard.
    hazards = sorted(
        [("heat", risk.heat_score), ("wildfire", risk.wildfire_score),
         ("flood", risk.flood_score)],
        key=lambda t: t[1], reverse=True,
    )
    top, top_score = hazards[0]

    parts: list[str] = []
    parts.append(
        f"By {win} under {scen}, this block's overall climate exposure is "
        f"{_label_phrase(risk.overall_score)} ({risk.overall_score:.0f}/100)."
    )

    # Why the top hazard?
    if top == "heat":
        canopy = d.get("tree_canopy_pct")
        imperv = d.get("impervious_pct")
        delta_days = (
            (d.get("heat_days") or 0) - (d.get("heat_days_base") or 0)
        )
        why = []
        if delta_days > 5:
            why.append(f"about {delta_days:.0f} more extreme-heat days per year than the 1981-2010 baseline")
        if canopy is not None and canopy < 15:
            why.append(f"low tree canopy ({canopy:.0f}%)")
        if imperv is not None and imperv > 60:
            why.append(f"high impervious surface ({imperv:.0f}%)")
        if why:
            parts.append(
                "Heat is the dominant risk: " + ", ".join(why) + "."
            )
        else:
            parts.append("Heat is the dominant risk for this block.")

    elif top == "wildfire":
        fhsz = d.get("fhsz_class")
        wui = d.get("wui_class")
        slope = d.get("slope_deg")
        why = []
        if fhsz and fhsz != "None":
            why.append(f"a CAL FIRE hazard zone of {fhsz}")
        if wui and wui != "None":
            why.append(f"wildland-urban interface ({wui})")
        if slope is not None and slope > 10:
            why.append(f"steep terrain (~{slope:.0f}\u00b0 slope)")
        if why:
            parts.append("Wildfire is the dominant risk: " + ", ".join(why) + ".")
        else:
            parts.append("Wildfire is the dominant risk for this block.")

    else:  # flood
        zone = d.get("flood_zone")
        slr = d.get("slr_inundated_ft")
        why = []
        if zone and zone not in ("X", "D"):
            why.append(f"FEMA flood zone {zone}")
        if slr:
            why.append(f"projected coastal inundation at {slr:.0f} ft of sea level rise")
        if why:
            parts.append("Flooding is the dominant risk: " + ", ".join(why) + ".")
        else:
            parts.append("Flooding is the dominant risk for this block.")

    # Vulnerability framing.
    svi = d.get("svi_overall")
    if svi is not None and svi >= 0.6:
        parts.append(
            "Social vulnerability is elevated here, which can amplify the human impact "
            "of any hazard event."
        )

    return " ".join(parts)


# ---------------------------------------------------------------------------
# LLM-powered explanation
# ---------------------------------------------------------------------------
_PROMPT_SYSTEM = (
    "You are a climate-risk explainer for a public-facing block-level visualizer. "
    "Given a JSON snapshot of risk scores and their underlying drivers for a single "
    "block, write ONE paragraph (<=120 words) for a non-expert reader. "
    "Be concrete: name the dominant hazard, cite 1-3 specific drivers, mention the "
    "year and emissions scenario in plain language. Do NOT make claims the data "
    "does not support. Do NOT predict damages or insurance outcomes. Do NOT use "
    "bullet points or headings. Output the paragraph only."
)


def _user_prompt(risk: CellRisk) -> str:
    return (
        f"Window: {risk.window}\n"
        f"Scenario: {risk.scenario}\n"
        f"Overall score: {risk.overall_score:.0f}/100 ({risk.overall_label})\n"
        f"Heat: {risk.heat_score:.0f} ({risk.heat_label})\n"
        f"Wildfire: {risk.wildfire_score:.0f} ({risk.wildfire_label})\n"
        f"Flood: {risk.flood_score:.0f} ({risk.flood_label})\n"
        f"Drivers (JSON): {risk.drivers}"
    )


def _explain_anthropic(risk: CellRisk) -> str:
    import anthropic
    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
    msg = client.messages.create(
        model=settings.llm_model,
        max_tokens=300,
        system=_PROMPT_SYSTEM,
        messages=[{"role": "user", "content": _user_prompt(risk)}],
    )
    parts = [b.text for b in msg.content if getattr(b, "type", None) == "text"]
    return "".join(parts).strip()


def _explain_openai(risk: CellRisk) -> str:
    import openai
    client = openai.OpenAI(api_key=settings.openai_api_key)
    resp = client.chat.completions.create(
        model=settings.llm_model,
        max_tokens=300,
        messages=[
            {"role": "system", "content": _PROMPT_SYSTEM},
            {"role": "user",   "content": _user_prompt(risk)},
        ],
    )
    return (resp.choices[0].message.content or "").strip()


def explain_risk(risk: CellRisk) -> str:
    """Try the configured LLM, always fall back to the deterministic paragraph."""
    provider = settings.llm_provider
    try:
        if provider == "anthropic" and settings.anthropic_api_key:
            return _explain_anthropic(risk)
        if provider == "openai" and settings.openai_api_key:
            return _explain_openai(risk)
    except Exception as e:
        log.warning("LLM (%s) explanation failed: %r; using fallback", provider, e)
    return fallback_explanation(risk)
