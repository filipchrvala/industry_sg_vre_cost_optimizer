"""Render the investment dashboard as a single self-contained HTML file.

Charts are inline SVG rather than a JavaScript charting library. The dashboard is
handed to people who open it from a shared drive or an email attachment, often
inside a network that will not fetch a CDN, and a chart that silently fails to
draw is worse than no chart. Inline SVG also survives print-to-PDF, which is how
these end up in a board pack.
"""

from __future__ import annotations

import html
import json
import math
from datetime import datetime, timezone
from typing import Any, Sequence

# Sequential scale, dark for weak results through to strong. Readable in
# greyscale and safe for the most common form of colour blindness.
HEATMAP_COLOURS = [
    (13, 27, 62),
    (23, 62, 110),
    (26, 106, 140),
    (35, 148, 138),
    (95, 184, 111),
    (176, 210, 71),
    (253, 231, 55),
]

CSS = """
:root {
  --bg: #f5f6f8;
  --panel: #ffffff;
  --ink: #14181f;
  --muted: #5c6675;
  --line: #dfe3e9;
  --accent: #1a6a8c;
  --good: #2f8f5b;
  --warn: #b4761e;
  --bad: #b23a3a;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  background: var(--bg);
  color: var(--ink);
  font: 15px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
}
.wrap { max-width: 1180px; margin: 0 auto; padding: 32px 24px 64px; }
header { border-bottom: 2px solid var(--ink); padding-bottom: 16px; margin-bottom: 28px; }
.kicker {
  font-size: 11px; letter-spacing: 0.16em; text-transform: uppercase;
  color: var(--accent); font-weight: 650; margin: 0 0 8px;
}
h1 { font-size: 26px; margin: 0 0 6px; letter-spacing: -0.01em; }
.sub { color: var(--muted); font-size: 14px; }
.source-box { border-left: 3px solid var(--accent); padding: 2px 0 2px 16px; margin-bottom: 16px; }
.source-box strong { display: block; font-size: 16px; margin-bottom: 4px; }
h2 {
  font-size: 15px; text-transform: uppercase; letter-spacing: 0.08em;
  color: var(--muted); margin: 36px 0 14px; font-weight: 600;
}
.panel {
  background: var(--panel); border: 1px solid var(--line); border-radius: 10px;
  padding: 20px 22px; margin-bottom: 18px;
}
.cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); gap: 14px; }
.card { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 16px 18px; }
.card .label { font-size: 12px; text-transform: uppercase; letter-spacing: 0.06em; color: var(--muted); }
.card .value { font-size: 27px; font-weight: 650; margin-top: 6px; letter-spacing: -0.02em; }
.card .note { font-size: 12.5px; color: var(--muted); margin-top: 4px; }
.value.good { color: var(--good); }
.value.warn { color: var(--warn); }
.value.bad { color: var(--bad); }
table { width: 100%; border-collapse: collapse; font-size: 14px; }
th, td { text-align: left; padding: 9px 10px; border-bottom: 1px solid var(--line); }
th { font-size: 12px; text-transform: uppercase; letter-spacing: 0.05em; color: var(--muted); font-weight: 600; }
td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; }
tr.best td { background: #eaf5ef; font-weight: 600; }
.legend { display: flex; align-items: center; gap: 8px; font-size: 12.5px; color: var(--muted); margin-top: 10px; }
.swatch { display: flex; }
.swatch span { width: 26px; height: 12px; display: block; }
.note-block { font-size: 13.5px; color: var(--muted); margin-top: 10px; }
.badge {
  display: inline-block; font-size: 12px; padding: 3px 9px; border-radius: 999px;
  border: 1px solid var(--line); background: #f0f3f6; color: var(--muted); margin-right: 6px;
}
.badge.ok { background: #eaf5ef; border-color: #bfe0cd; color: var(--good); }
.badge.warn { background: #fbf2e3; border-color: #ebd7b0; color: var(--warn); }
figure { margin: 0; }
figcaption { font-size: 13px; color: var(--muted); margin-top: 8px; }
.hm-cell { cursor: pointer; }
.hm-cell:focus { outline: none; }
.hm-cell:hover .hm-fill { stroke: #14181f; stroke-width: 1.2; }
.hm-pick-head {
  display: flex; justify-content: space-between; align-items: baseline;
  gap: 12px; flex-wrap: wrap; margin: 18px 0 12px;
}
.hm-pick-head strong { font-size: 16px; }
@media print {
  body { background: #fff; }
  .panel, .card { break-inside: avoid; }
  .hm-cell { cursor: default; }
}
"""


def esc(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def fmt_money(value: Any, decimals: int = 0) -> str:
    if not is_number(value):
        return "—"
    return f"{value:,.{decimals}f} €".replace(",", " ")


def fmt_num(value: Any, decimals: int = 1, unit: str = "") -> str:
    if not is_number(value):
        return "—"
    text = f"{value:,.{decimals}f}".replace(",", " ")
    return f"{text} {unit}".strip()


def fmt_years(value: Any) -> str:
    if not is_number(value):
        return "—"
    return f"{value:.1f} r"


def fmt_mwh(value: Any, decimals: int = 1) -> str:
    if not is_number(value):
        return "—"
    return f"{value:,.{decimals}f} €/MWh".replace(",", " ")


def fmt_dt(value: Any) -> str:
    if not value:
        return "—"
    try:
        stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return str(value)
    return stamp.strftime("%d.%m.%Y %H:%M")


def colour_for(fraction: float) -> str:
    fraction = min(max(fraction, 0.0), 1.0)
    scaled = fraction * (len(HEATMAP_COLOURS) - 1)
    low = int(math.floor(scaled))
    high = min(low + 1, len(HEATMAP_COLOURS) - 1)
    t = scaled - low
    r = round(HEATMAP_COLOURS[low][0] + (HEATMAP_COLOURS[high][0] - HEATMAP_COLOURS[low][0]) * t)
    g = round(HEATMAP_COLOURS[low][1] + (HEATMAP_COLOURS[high][1] - HEATMAP_COLOURS[low][1]) * t)
    b = round(HEATMAP_COLOURS[low][2] + (HEATMAP_COLOURS[high][2] - HEATMAP_COLOURS[low][2]) * t)
    return f"rgb({r},{g},{b})"


def card(
    label: str,
    value: str,
    note: str = "",
    tone: str = "",
    live: str = "",
    rec_only: bool = False,
) -> str:
    tone_class = f" {tone}" if tone else ""
    note_html = f'<div class="note">{esc(note)}</div>' if (note or live) else ""
    attrs = ""
    if live:
        attrs += f' data-live="{esc(live)}"'
    if rec_only:
        attrs += ' data-rec-only="1"'
        attrs += f' data-original="{esc(value)}"'
        attrs += f' data-original-note="{esc(note)}"'
        attrs += f' data-original-tone="{esc(tone)}"'
    return (
        f'<div class="card"{attrs}><div class="label">{esc(label)}</div>'
        f'<div class="value{tone_class}">{esc(value)}</div>{note_html}</div>'
    )


def detail_row(label: str, value: str, live: str = "", rec_only: bool = False) -> str:
    attrs = ""
    if live:
        attrs += f' data-live="{esc(live)}"'
    if rec_only:
        attrs += ' data-rec-only="1"'
        attrs += f' data-original="{esc(value)}"'
    return f"<tr{attrs}><td>{esc(label)}</td><td class='num'>{esc(value)}</td></tr>"


def _axis_index(axis: Sequence[Any], value: Any) -> int:
    if not axis:
        return 0
    if not is_number(value):
        return 0
    return min(range(len(axis)), key=lambda i: abs(float(axis[i]) - float(value)))


def _grid_get(grid: Any, j: int, i: int) -> Any:
    if not isinstance(grid, list) or j >= len(grid):
        return None
    row = grid[j]
    if not isinstance(row, list) or i >= len(row):
        return None
    return row[i]


def _objective_view(heatmap: dict[str, Any] | None) -> dict[str, Any]:
    obj = str((heatmap or {}).get("objective") or "max_npv").lower()
    if obj in {"shortest_payback", "min_payback", "payback"}:
        return {
            "id": "shortest_payback",
            "metric": "simple_payback_years",
            "invert": True,
            "title": "Jednoduchá návratnosť podľa veľkosti systému (roky)",
            "cells": "Hodnoty v bunkách sú návratnosť v rokoch. Svetlejšia bunka = kratšia návratnosť.",
            "legend": "years",
            "label": "najkratšia návratnosť",
        }
    return {
        "id": "max_npv",
        "metric": "npv_eur",
        "invert": False,
        "title": "Čistá súčasná hodnota podľa veľkosti systému (€)",
        "cells": "Hodnoty v bunkách sú NPV v tisícoch eur. Svetlejšia bunka = vyššie NPV.",
        "legend": "money",
        "label": "najvyššie NPV",
    }


def render_heatmap(
    heatmap: dict[str, Any],
    metric: str | None = None,
) -> str:
    """Draw the PV x battery grid; a click shows the economics of that size."""
    view = _objective_view(heatmap)
    metric = metric or view["metric"]
    axes = heatmap.get("axes") or {}
    pv_axis: list[float] = list(axes.get("pv_kwp") or [])
    bat_axis: list[float] = list(axes.get("battery_kwh") or [])
    grids = heatmap.get("grids") or {}
    grid = grids.get(metric) or []
    if not pv_axis or not bat_axis or not grid:
        return '<div class="panel">Mapa veľkostí v tomto behu nie je k dispozícii.</div>'

    values = [v for row in grid for v in row if is_number(v)]
    if not values:
        return '<div class="panel">Mapa veľkostí nevrátila žiadne konečné výsledky.</div>'
    low, high = min(values), max(values)
    span = high - low if high > low else 1.0

    cell_w, cell_h = 62, 34
    left, top = 108, 46
    width = left + cell_w * len(pv_axis) + 24
    height = top + cell_h * len(bat_axis) + 62

    recommended = heatmap.get("recommended") or {}
    best_pv, best_bat = recommended.get("pv_kwp"), recommended.get("battery_kwh")
    rec_i = _axis_index(pv_axis, best_pv)
    rec_j = _axis_index(bat_axis, best_bat)

    parts = [
        f'<svg id="heatmap-svg" viewBox="0 0 {width} {height}" width="100%" role="img" '
        f'aria-label="Kliknite na kombináciu FVE a batérie" '
        f'style="font: 11px sans-serif; max-width:{width}px">'
    ]
    parts.append(
        f'<text x="{left}" y="18" font-size="12" font-weight="600" fill="#14181f">'
        f'{esc(view["title"])}</text>'
    )
    parts.append(
        f'<text x="{left}" y="34" font-size="11" fill="#5c6675">Veľkosť FVE (kWp) →</text>'
    )

    for i, kwp in enumerate(pv_axis):
        x = left + i * cell_w + cell_w / 2
        parts.append(
            f'<text x="{x:.1f}" y="{top - 6}" text-anchor="middle" fill="#5c6675">'
            f"{kwp:,.0f}</text>".replace(",", " ")
        )

    for j, kwh in enumerate(bat_axis):
        y = top + j * cell_h
        parts.append(
            f'<text x="{left - 10}" y="{y + cell_h / 2 + 4:.1f}" text-anchor="end" '
            f'fill="#5c6675">{kwh:,.0f} kWh</text>'.replace(",", " ")
        )
        for i, kwp in enumerate(pv_axis):
            x = left + i * cell_w
            value = _grid_get(grid, j, i)
            if not is_number(value):
                parts.append(
                    f'<rect x="{x}" y="{y}" width="{cell_w - 2}" height="{cell_h - 2}" '
                    f'fill="#eef0f3" stroke="#fff"/>'
                )
                continue
            fraction = (value - low) / span
            if view["invert"]:
                fraction = 1.0 - fraction
            fill = colour_for(fraction)
            text_fill = "#ffffff" if fraction < 0.55 else "#14181f"
            if view["legend"] == "years":
                title = f"{kwp:,.0f} kWp / {kwh:,.0f} kWh: {value:.1f} r".replace(",", " ")
                label = f"{value:.1f}"
            else:
                title = f"{kwp:,.0f} kWp / {kwh:,.0f} kWh: {value:,.0f} € NPV".replace(",", " ")
                label = (
                    f"{value / 1000:,.0f}k".replace(",", " ")
                    if abs(value) >= 1000
                    else f"{value:,.0f}".replace(",", " ")
                )
            parts.append(
                f'<g class="hm-cell" data-i="{i}" data-j="{j}" tabindex="0" role="button" '
                f'aria-label="{esc(title)}">'
                f'<rect class="hm-fill" x="{x}" y="{y}" width="{cell_w - 2}" height="{cell_h - 2}" '
                f'fill="{fill}" stroke="#fff"><title>{esc(title)}</title></rect>'
                f'<text x="{x + cell_w / 2 - 1:.1f}" y="{y + cell_h / 2 + 4:.1f}" '
                f'text-anchor="middle" fill="{text_fill}">{label}</text></g>'
            )
            if (
                is_number(best_pv) and is_number(best_bat)
                and abs(kwp - best_pv) < 1e-6 and abs(kwh - best_bat) < 1e-6
            ):
                parts.append(
                    f'<rect x="{x}" y="{y}" width="{cell_w - 2}" height="{cell_h - 2}" '
                    f'fill="none" stroke="#b23a3a" stroke-width="2.5" pointer-events="none"/>'
                )

    parts.append(
        f'<rect id="hm-cursor" x="0" y="0" width="{cell_w - 2}" height="{cell_h - 2}" '
        f'fill="none" stroke="#163a56" stroke-width="2.5" pointer-events="none"/>'
    )
    parts.append(
        f'<text x="{left}" y="{height - 26}" fill="#5c6675">'
        f'Zvislá os je kapacita batérie (kWh). Červený okraj je odporúčanie podľa cieľa '
        f'„{esc(view["label"])}“, modrá značka vybranú bunku. {esc(view["cells"])}</text>'
    )
    parts.append("</svg>")

    swatch = "".join(
        f'<span style="background:{colour_for(i / 6)}"></span>' for i in range(7)
    )
    if view["legend"] == "years":
        legend_left, legend_right = (fmt_years(high), fmt_years(low)) if view["invert"] else (fmt_years(low), fmt_years(high))
    else:
        legend_left, legend_right = fmt_money(low), fmt_money(high)
    legend = (
        f'<div class="legend"><span>{legend_left}</span>'
        f'<span class="swatch">{swatch}</span><span>{legend_right}</span></div>'
    )
    payload = {
        "pv_kwp": pv_axis,
        "battery_kwh": bat_axis,
        "grids": {
            key: grids.get(key) or []
            for key in (
                "annual_savings_eur",
                "npv_eur",
                "simple_payback_years",
                "discounted_payback_years",
                "total_capex_eur",
                "self_consumption_pct",
                "operating_cost_baseline_eur",
                "operating_cost_optimized_eur",
                "battery_cycles_per_year",
                "battery_life_years",
                "cashflow_after_om_eur",
            )
        },
        "recommended": {"i": rec_i, "j": rec_j},
        "cell": {"w": cell_w, "h": cell_h, "left": left, "top": top},
        "current": heatmap.get("current_scenario") or {},
        "objective_label": view["label"],
    }
    data = json.dumps(payload, ensure_ascii=False).replace("<", "\\u003c")
    source = heatmap.get("pv_profile_source")
    note = (
        "Každá bunka je úplná ekonomická simulácia voči prognóze výroby. "
        if source == "ai_forecast"
        else "Prognóza v tomto behu nebola k dispozícii; bunky používajú syntetický profil. "
    )
    return (
        '<div class="panel"><figure>' + "".join(parts) + f"</figure>{legend}"
        '<div class="hm-pick-head">'
        '<strong id="hm-title">Vybraná konfigurácia</strong>'
        f'<span class="note" id="hm-badge">Odporúčanie: {esc(view["label"])}</span>'
        "</div>"
        '<div class="cards" id="hm-cards"></div>'
        '<table id="hm-compare" style="margin-top:14px"></table>'
        f'<div class="note-block">{esc(note)}'
        "Kliknite na bunku — karty hore aj Podrobnosti sa prepíšu na túto veľkosť. "
        "Každá kombinácia má vlastnú úplnú simuláciu odberu, cien, FVE, batérie a MRK.</div>"
        f'<script type="application/json" id="heatmap-data">{data}</script></div>'
    )


COLOUR_WITHOUT = "#b23a3a"
COLOUR_WITH = "#1a6a8c"


def _axis_max(values: Sequence[float]) -> float:
    high = max((v for v in values if is_number(v)), default=0.0)
    if high <= 0:
        return 1.0
    magnitude = 10 ** max(0, int(math.floor(math.log10(high))))
    step = magnitude if high / magnitude > 2 else magnitude / 2
    return math.ceil(high / step) * step


def render_consumption(consumption: dict[str, Any] | None, fallback: dict[str, Any] | None) -> str:
    """Energy bought from the grid, with and without the proposed plant."""
    data = consumption or {}
    monthly = data.get("monthly") or {}
    daily = data.get("daily") or {}
    totals = data.get("totals") or {}

    if not monthly.get("x") and fallback:
        series = fallback.get("series") or []
        if len(series) >= 2 and fallback.get("x"):
            daily = {
                "x": fallback["x"],
                "without": series[0].get("values") or [],
                "with": series[1].get("values") or [],
                "unit": series[0].get("unit") or "kWh/deň",
            }

    if not monthly.get("x") and not daily.get("x"):
        return ""

    parts = ['<div class="panel">']
    if totals.get("without_kwh") is not None:
        parts.append(
            '<div class="cards" style="margin-bottom:18px">'
            + card("Bez FVE a batérie", fmt_num(totals["without_kwh"] / 1000.0, 0, "MWh"), "Nákup zo siete za obdobie")
            + card("S FVE a batériou", fmt_num((totals.get("with_kwh") or 0) / 1000.0, 0, "MWh"), "Nákup zo siete po nasadení", "good")
            + card(
                "Ušetrená energia",
                fmt_num((totals.get("saved_kwh") or 0) / 1000.0, 0, "MWh"),
                f"{fmt_num(totals.get('saved_pct'), 0, '%')} menej zo siete" if totals.get("saved_pct") is not None else "",
                "good",
            )
            + "</div>"
        )

    if monthly.get("x") and monthly.get("without"):
        parts.append(_grouped_bars(monthly, title="Mesačná spotreba zo siete"))
    if daily.get("x") and daily.get("without"):
        parts.append(_line_pair(daily, title="Denná spotreba zo siete"))

    parts.append(
        '<figcaption>Červená je nákup zo siete bez FVE a batérie. Modrá je ten istý odber '
        'po nasadení navrhnutého systému. Rozdiel medzi nimi je energia, ktorú už '
        'nezaplatíte dodávateľovi.</figcaption>'
    )
    parts.append("</div>")
    return "".join(parts)


def _grouped_bars(series: dict[str, Any], *, title: str) -> str:
    labels = series.get("x") or []
    without = [float(v) if is_number(v) else 0.0 for v in (series.get("without") or [])]
    with_ = [float(v) if is_number(v) else 0.0 for v in (series.get("with") or [])]
    n = min(len(labels), len(without), len(with_))
    if n == 0:
        return ""
    without, with_, labels = without[:n], with_[:n], labels[:n]
    high = _axis_max(without + with_)

    width, height = 1080, 280
    pad_l, pad_r, pad_t, pad_b = 72, 16, 28, 48
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b
    group = plot_w / n
    bar_w = min(22.0, group * 0.32)

    parts = [
        f'<svg viewBox="0 0 {width} {height}" width="100%" role="img" '
        f'aria-label="{esc(title)}" style="font: 11px sans-serif">'
        f'<text x="{pad_l}" y="16" font-size="13" font-weight="600" fill="#14181f">{esc(title)} (kWh)</text>'
    ]
    for k in range(5):
        y = pad_t + plot_h * k / 4
        value = high * (1 - k / 4)
        parts.append(
            f'<line x1="{pad_l}" y1="{y:.1f}" x2="{width - pad_r}" y2="{y:.1f}" stroke="#e6e9ee"/>'
        )
        parts.append(
            f'<text x="{pad_l - 8}" y="{y + 4:.1f}" text-anchor="end" fill="#5c6675">'
            f"{value:,.0f}</text>".replace(",", " ")
        )

    for i, (label, a, b) in enumerate(zip(labels, without, with_)):
        x0 = pad_l + i * group + group / 2
        ha = plot_h * (a / high)
        hb = plot_h * (b / high)
        parts.append(
            f'<rect x="{x0 - bar_w - 1:.1f}" y="{pad_t + plot_h - ha:.1f}" width="{bar_w:.1f}" '
            f'height="{ha:.1f}" fill="{COLOUR_WITHOUT}"><title>{esc(label)} bez: {a:,.0f} kWh</title></rect>'.replace(",", " ")
        )
        parts.append(
            f'<rect x="{x0 + 1:.1f}" y="{pad_t + plot_h - hb:.1f}" width="{bar_w:.1f}" '
            f'height="{hb:.1f}" fill="{COLOUR_WITH}"><title>{esc(label)} s FVE+bat: {b:,.0f} kWh</title></rect>'.replace(",", " ")
        )
        parts.append(
            f'<text x="{x0:.1f}" y="{height - 28}" text-anchor="middle" fill="#5c6675">{esc(label)}</text>'
        )

    parts.append(
        f'<rect x="{pad_l}" y="{height - 14}" width="11" height="11" fill="{COLOUR_WITHOUT}"/>'
        f'<text x="{pad_l + 16}" y="{height - 4}" fill="#14181f">Bez FVE a batérie</text>'
        f'<rect x="{pad_l + 200}" y="{height - 14}" width="11" height="11" fill="{COLOUR_WITH}"/>'
        f'<text x="{pad_l + 216}" y="{height - 4}" fill="#14181f">S FVE a batériou</text>'
    )
    parts.append("</svg>")
    return "".join(parts)


def _line_pair(series: dict[str, Any], *, title: str) -> str:
    without = [float(v) if is_number(v) else 0.0 for v in (series.get("without") or [])]
    with_ = [float(v) if is_number(v) else 0.0 for v in (series.get("with") or [])]
    n = min(len(without), len(with_))
    if n < 2:
        return ""
    without, with_ = without[:n], with_[:n]
    high = _axis_max(without + with_)

    width, height = 1080, 260
    pad_l, pad_r, pad_t, pad_b = 72, 16, 28, 36
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b

    def xy(i: int, value: float) -> tuple[float, float]:
        x = pad_l + plot_w * i / (n - 1)
        y = pad_t + plot_h * (1 - value / high)
        return x, y

    without_pts = [xy(i, v) for i, v in enumerate(without)]
    with_pts = [xy(i, v) for i, v in enumerate(with_)]
    fill = " ".join(
        [f"{x:.1f},{y:.1f}" for x, y in without_pts]
        + [f"{x:.1f},{y:.1f}" for x, y in reversed(with_pts)]
    )
    without_line = " ".join(f"{x:.1f},{y:.1f}" for x, y in without_pts)
    with_line = " ".join(f"{x:.1f},{y:.1f}" for x, y in with_pts)

    parts = [
        f'<svg viewBox="0 0 {width} {height}" width="100%" role="img" '
        f'aria-label="{esc(title)}" style="font: 11px sans-serif; margin-top:12px">'
        f'<text x="{pad_l}" y="16" font-size="13" font-weight="600" fill="#14181f">{esc(title)} (kWh/deň)</text>'
    ]
    for k in range(5):
        y = pad_t + plot_h * k / 4
        value = high * (1 - k / 4)
        parts.append(
            f'<line x1="{pad_l}" y1="{y:.1f}" x2="{width - pad_r}" y2="{y:.1f}" stroke="#e6e9ee"/>'
        )
        parts.append(
            f'<text x="{pad_l - 8}" y="{y + 4:.1f}" text-anchor="end" fill="#5c6675">'
            f"{value:,.0f}</text>".replace(",", " ")
        )
    parts.append(f'<polygon points="{fill}" fill="{COLOUR_WITH}" fill-opacity="0.12"/>')
    parts.append(
        f'<polyline fill="none" stroke="{COLOUR_WITHOUT}" stroke-width="1.5" points="{without_line}"/>'
    )
    parts.append(
        f'<polyline fill="none" stroke="{COLOUR_WITH}" stroke-width="1.7" points="{with_line}"/>'
    )
    parts.append(
        f'<rect x="{pad_l}" y="{height - 12}" width="11" height="11" fill="{COLOUR_WITHOUT}"/>'
        f'<text x="{pad_l + 16}" y="{height - 2}" fill="#14181f">Bez FVE a batérie</text>'
        f'<rect x="{pad_l + 200}" y="{height - 12}" width="11" height="11" fill="{COLOUR_WITH}"/>'
        f'<text x="{pad_l + 216}" y="{height - 2}" fill="#14181f">S FVE a batériou</text>'
    )
    parts.append("</svg>")
    return "".join(parts)


COLOUR_PRICE = "#1a6a8c"
COLOUR_PRICE_W = "#b4761e"


def _axis_span(values: Sequence[float]) -> tuple[float, float]:
    nums = [float(v) for v in values if is_number(v)]
    if not nums:
        return 0.0, 1.0
    lo, hi = min(nums), max(nums)
    if lo == hi:
        pad = abs(lo) * 0.15 or 10.0
        return lo - pad, hi + pad
    span = hi - lo
    return lo - 0.08 * span, hi + 0.08 * span


def _y_of(value: float, lo: float, hi: float, pad_t: float, plot_h: float) -> float:
    return pad_t + plot_h * (1 - (value - lo) / (hi - lo))


def _price_bars(labels: list[str], mean: list[float], weighted: list[float], *, title: str) -> str:
    n = min(len(labels), len(mean), len(weighted))
    if n == 0:
        return ""
    labels, mean, weighted = labels[:n], mean[:n], weighted[:n]
    lo, hi = _axis_span(mean + weighted)
    width, height = 1080, 280
    pad_l, pad_r, pad_t, pad_b = 72, 16, 28, 48
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b
    group = plot_w / n
    bar_w = min(20.0, group * 0.32)
    zero_y = _y_of(0.0, lo, hi, pad_t, plot_h) if lo < 0 < hi else pad_t + plot_h

    parts = [
        f'<svg viewBox="0 0 {width} {height}" width="100%" role="img" '
        f'aria-label="{esc(title)}" style="font: 11px sans-serif">'
        f'<text x="{pad_l}" y="16" font-size="13" font-weight="600" fill="#14181f">{esc(title)}</text>'
    ]
    for k in range(5):
        value = hi - (hi - lo) * k / 4
        y = pad_t + plot_h * k / 4
        parts.append(
            f'<line x1="{pad_l}" y1="{y:.1f}" x2="{width - pad_r}" y2="{y:.1f}" stroke="#e6e9ee"/>'
        )
        parts.append(
            f'<text x="{pad_l - 8}" y="{y + 4:.1f}" text-anchor="end" fill="#5c6675">'
            f"{value:,.0f}</text>".replace(",", " ")
        )
    if lo < 0 < hi:
        parts.append(
            f'<line x1="{pad_l}" y1="{zero_y:.1f}" x2="{width - pad_r}" y2="{zero_y:.1f}" '
            f'stroke="#9aa3ad" stroke-dasharray="3 3"/>'
        )
    for i, (label, a, b) in enumerate(zip(labels, mean, weighted)):
        x0 = pad_l + i * group + group / 2
        ya = _y_of(a, lo, hi, pad_t, plot_h)
        yb = _y_of(b, lo, hi, pad_t, plot_h)
        parts.append(
            f'<rect x="{x0 - bar_w - 1:.1f}" y="{min(ya, zero_y):.1f}" width="{bar_w:.1f}" '
            f'height="{abs(zero_y - ya):.1f}" fill="{COLOUR_PRICE}">'
            f"<title>{esc(label)} priemer: {a:,.1f} €/MWh</title></rect>".replace(",", " ")
        )
        parts.append(
            f'<rect x="{x0 + 1:.1f}" y="{min(yb, zero_y):.1f}" width="{bar_w:.1f}" '
            f'height="{abs(zero_y - yb):.1f}" fill="{COLOUR_PRICE_W}">'
            f"<title>{esc(label)} vážený: {b:,.1f} €/MWh</title></rect>".replace(",", " ")
        )
        parts.append(
            f'<text x="{x0:.1f}" y="{height - 28}" text-anchor="middle" fill="#5c6675">{esc(label)}</text>'
        )
    parts.append(
        f'<rect x="{pad_l}" y="{height - 14}" width="11" height="11" fill="{COLOUR_PRICE}"/>'
        f'<text x="{pad_l + 16}" y="{height - 4}" fill="#14181f">Jednoduchý priemer</text>'
        f'<rect x="{pad_l + 200}" y="{height - 14}" width="11" height="11" fill="{COLOUR_PRICE_W}"/>'
        f'<text x="{pad_l + 216}" y="{height - 4}" fill="#14181f">Vážený odberom</text>'
    )
    parts.append("</svg>")
    return "".join(parts)


def _price_line(labels: list[str], values: list[float], *, title: str) -> str:
    n = min(len(labels), len(values))
    if n < 2:
        return ""
    labels, values = labels[:n], [float(v) if is_number(v) else 0.0 for v in values[:n]]
    lo, hi = _axis_span(values)
    width, height = 1080, 240
    pad_l, pad_r, pad_t, pad_b = 72, 16, 28, 36
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b
    pts = []
    for i, value in enumerate(values):
        x = pad_l + plot_w * i / (n - 1)
        y = _y_of(value, lo, hi, pad_t, plot_h)
        pts.append((x, y))
    line = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
    parts = [
        f'<svg viewBox="0 0 {width} {height}" width="100%" role="img" '
        f'aria-label="{esc(title)}" style="font: 11px sans-serif; margin-top:12px">'
        f'<text x="{pad_l}" y="16" font-size="13" font-weight="600" fill="#14181f">{esc(title)}</text>'
    ]
    for k in range(5):
        value = hi - (hi - lo) * k / 4
        y = pad_t + plot_h * k / 4
        parts.append(
            f'<line x1="{pad_l}" y1="{y:.1f}" x2="{width - pad_r}" y2="{y:.1f}" stroke="#e6e9ee"/>'
        )
        parts.append(
            f'<text x="{pad_l - 8}" y="{y + 4:.1f}" text-anchor="end" fill="#5c6675">'
            f"{value:,.0f}</text>".replace(",", " ")
        )
    if lo < 0 < hi:
        zy = _y_of(0.0, lo, hi, pad_t, plot_h)
        parts.append(
            f'<line x1="{pad_l}" y1="{zy:.1f}" x2="{width - pad_r}" y2="{zy:.1f}" '
            f'stroke="#9aa3ad" stroke-dasharray="3 3"/>'
        )
    parts.append(f'<polyline fill="none" stroke="{COLOUR_PRICE}" stroke-width="2" points="{line}"/>')
    step = max(1, n // 8)
    for i, (label, (x, _)) in enumerate(zip(labels, pts)):
        if i % step == 0 or i == n - 1:
            parts.append(
                f'<text x="{x:.1f}" y="{height - 8}" text-anchor="middle" fill="#5c6675">{esc(label)}</text>'
            )
    parts.append("</svg>")
    return "".join(parts)


def render_prices(prices: dict[str, Any] | None) -> str:
    data = prices or {}
    if not data.get("available"):
        return (
            '<div class="panel"><span class="badge warn">Ceny sa nezobrazili</span>'
            f"{esc(data.get('source_detail') or 'Zlúčený profil odberu a cien chýba.')}</div>"
        )
    p10_p50_p90 = data.get("p10_p50_p90_eur_per_mwh") or [None, None, None]
    while len(p10_p50_p90) < 3:
        p10_p50_p90.append(None)
    cards = "".join(
        [
            card("Priemerná cena", fmt_mwh(data.get("mean_eur_per_mwh")), "Jednoduchý priemer intervalu"),
            card(
                "Vážená odberom",
                fmt_mwh(data.get("weighted_mean_eur_per_mwh")),
                "Cena, ktorú závod skutočne platí",
            ),
            card("Minimum / maximum", f"{fmt_mwh(data.get('min_eur_per_mwh'))} / {fmt_mwh(data.get('max_eur_per_mwh'))}"),
            card("P10 / P50 / P90", f"{fmt_mwh(p10_p50_p90[0])} / {fmt_mwh(p10_p50_p90[1])} / {fmt_mwh(p10_p50_p90[2])}"),
        ]
    )
    source_badge = "ok" if data.get("source") in {"okte_dam", "load_csv", "prices_csv"} else "warn"
    monthly = data.get("monthly") or {}
    hourly = data.get("hourly") or {}
    month_rows = ""
    for label, mean, weighted in zip(
        monthly.get("x") or [],
        monthly.get("mean_eur_per_mwh") or [],
        monthly.get("weighted_eur_per_mwh") or [],
    ):
        month_rows += (
            f"<tr><td>{esc(label)}</td><td class='num'>{esc(fmt_mwh(mean))}</td>"
            f"<td class='num'>{esc(fmt_mwh(weighted))}</td></tr>"
        )
    table = ""
    if month_rows:
        table = (
            "<table><thead><tr><th>Mesiac</th><th class='num'>Priemer</th>"
            "<th class='num'>Vážený odberom</th></tr></thead>"
            f"<tbody>{month_rows}</tbody></table>"
        )
    return (
        '<div class="panel">'
        f'<span class="badge {source_badge}">{esc(data.get("source_label") or "Ceny")}</span>'
        f'<div class="source-box" style="margin-top:12px">'
        f"<strong>{esc(data.get('source_label'))}</strong>"
        f"{esc(data.get('source_detail'))}"
        f"<div class='note-block'>Obdobie {esc(fmt_dt(data.get('period_start')))} – "
        f"{esc(fmt_dt(data.get('period_end')))} · {esc(fmt_num(data.get('priced_rows'), 0))} intervalov "
        f"s cenou · pokrytie {esc(fmt_num(data.get('coverage_pct'), 1, '%'))}</div>"
        "</div>"
        f'<div class="cards">{cards}</div>'
        + _price_bars(
            list(monthly.get("x") or []),
            list(monthly.get("mean_eur_per_mwh") or []),
            list(monthly.get("weighted_eur_per_mwh") or []),
            title="Mesačná cena elektriny (€/MWh)",
        )
        + _price_line(
            list(hourly.get("x") or []),
            list(hourly.get("mean_eur_per_mwh") or []),
            title="Priemerný denný profil ceny (€/MWh)",
        )
        + (f'<div style="margin-top:16px">{table}</div>' if table else "")
        + '<figcaption>Jednotka €/MWh je trhová kotácia denného trhu. Vážený priemer zohľadňuje, '
        "v ktorých hodinách závod skutočne odoberá — to je relevantnejšie číslo pre opex "
        "než jednoduchý priemer.</figcaption>"
        "</div>"
    )


def render_forecast_panel(calibration: dict[str, Any] | None) -> str:
    """State plainly how far the production forecast is backed by measurement."""
    if not calibration:
        return (
            '<div class="panel"><span class="badge warn">Bez kalibrácie</span>'
            "Výroba vychádza z modelu počasia Open-Meteo cez fyzikálny a AI reťazec. "
            "V tomto behu nie je k dispozícii pozemné meranie.</div>"
        )

    available = bool(calibration.get("calibration_available"))
    station = calibration.get("station") or {}
    bias = calibration.get("bias") or {}

    if not available:
        reason = calibration.get("skip_reason", "no measurement available")
        return (
            f'<div class="panel"><span class="badge warn">Bez kalibrácie</span>'
            f"Výroba stojí len na modeli Open-Meteo: {esc(reason)}. "
            f"Výnos berte ako modelový odhad, nie ako nameranú hodnotu.</div>"
        )

    rows = [
        ("Referenčná stanica", f"{station.get('name', '—')} ({fmt_num(station.get('distance_km'), 1, 'km')})"),
        ("Nameraná odchýlka modelu počasia", fmt_num(bias.get("bias_pct"), 1, "%")),
        ("Stredná absolútna chyba", fmt_num(bias.get("mae_w_m2"), 1, "W/m²")),
        ("Korelácia s meraním", fmt_num(bias.get("correlation"), 3)),
        ("Použitá korekcia žiarenia", fmt_num(calibration.get("irradiance_scale_factor"), 4)),
        ("Porovnané denné kroky", fmt_num(bias.get("daylight_steps"), 0)),
    ]
    body = "".join(
        f"<tr><td>{esc(label)}</td><td class='num'>{esc(value)}</td></tr>" for label, value in rows
    )
    return (
        '<div class="panel"><span class="badge ok">Kalibrované voči SHMÚ</span>'
        f"<table>{body}</table>"
        '<div class="note-block">Open-Meteo je modelový produkt a jeho odchýlka žiarenia '
        "sa líši podľa lokality. Korekcia vyššie vychádza z nameraného globálneho "
        "žiarenia, ktoré zverejňuje Slovenský hydrometeorologický ústav.</div></div>"
    )


def render_equipment(ranking: dict[str, Any] | None) -> str:
    ranking = ranking or {}
    items = (
        ranking.get("top_recommendations")
        or ranking.get("top")
        or ranking.get("ranked")
        or []
    )
    if not items:
        return ""
    head = (
        "<tr><th>Výrobca</th><th>Model</th><th class='num'>Wp</th>"
        "<th class='num'>Účinnosť</th><th class='num'>€/Wp</th>"
        "<th class='num'>Moduly</th><th class='num'>Plocha</th><th class='num'>Skóre</th></tr>"
    )
    rows = []
    for i, m in enumerate(items[:8]):
        rows.append(
            "<tr{cls}><td>{man}</td><td>{mod}</td><td class='num'>{wp}</td>"
            "<td class='num'>{eff}</td><td class='num'>{eur}</td>"
            "<td class='num'>{cnt}</td><td class='num'>{area}</td>"
            "<td class='num'>{score}</td></tr>".format(
                cls=' class="best"' if i == 0 else "",
                man=esc(m.get("manufacturer")),
                mod=esc(m.get("model")),
                wp=fmt_num(m.get("power_wp"), 0),
                eff=fmt_num(m.get("efficiency_pct"), 1, "%"),
                eur=fmt_num(m.get("eur_per_wp"), 3),
                cnt=fmt_num(m.get("module_count_estimate"), 0),
                area=fmt_num(m.get("array_area_m2_estimate"), 0, "m²"),
                score=fmt_num(m.get("score"), 3),
            )
        )
    return (
        f'<div class="panel"><table>{head}{"".join(rows)}</table>'
        '<div class="note-block">Zoradené pre túto lokalitu podľa energetickej hustoty, '
        "ceny za watt, odolnosti voči tieneniu a účinnosti. Zvýraznený riadok je najlepšia zhoda.</div></div>"
    )


def render_heatmap_table(heatmap: dict[str, Any]) -> str:
    """Kept for callers; the interactive panel already shows this comparison."""
    return ""


_HEATMAP_JS = r"""
(function () {
  function init() {
  var node = document.getElementById("heatmap-data");
  if (!node) return;
  var data;
  try { data = JSON.parse(node.textContent); } catch (err) { return; }
  var grids = data.grids || {};
  var rec = data.recommended || { i: 0, j: 0 };
  var cell = data.cell || { w: 62, h: 34, left: 108, top: 46 };

  function num(v) { return typeof v === "number" && isFinite(v); }
  function money(v) {
    if (!num(v)) return "—";
    return Math.round(v).toLocaleString("sk-SK") + " €";
  }
  function years(v) { return num(v) ? v.toFixed(1) + " r" : "—"; }
  function pct(v) { return num(v) ? v.toFixed(1) + " %" : "—"; }
  function kw(v, unit) { return num(v) ? Math.round(v).toLocaleString("sk-SK") + " " + unit : "—"; }
  function dpbText(v) { return num(v) ? years(v) : "Nevráti sa pri diskontovaní"; }
  function cyclesText(kwh, cycles) {
    if (!num(kwh) || kwh <= 0) return "Bez batérie";
    return num(cycles) ? Math.round(cycles).toLocaleString("sk-SK") : "—";
  }
  function lifeText(kwh, life) {
    if (!num(kwh) || kwh <= 0) return "Neaplikuje sa";
    return years(life);
  }
  function pbTone(v) {
    if (!num(v)) return "";
    if (v <= 8) return "good";
    if (v <= 12) return "warn";
    return "bad";
  }
  function npvTone(v) { return num(v) && v > 0 ? "good" : "bad"; }
  function cellVal(key, i, j) {
    var g = grids[key] || [];
    return (g[j] && g[j][i] != null) ? g[j][i] : null;
  }
  function setCard(key, text, note, tone) {
    var el = document.querySelector('.card[data-live="' + key + '"]');
    if (!el) return;
    var val = el.querySelector(".value");
    if (val) {
      val.textContent = text;
      val.className = "value" + (tone ? " " + tone : "");
    }
    var n = el.querySelector(".note");
    if (n && note != null) n.textContent = note;
  }
  function setDetail(key, text) {
    var el = document.querySelector('#live-details [data-live="' + key + '"] td.num');
    if (el) el.textContent = text;
  }
  function restoreRecOnly() {
    document.querySelectorAll("[data-rec-only='1']").forEach(function (el) {
      var val = el.querySelector(".value") || el.querySelector("td.num");
      if (!val) return;
      val.textContent = el.getAttribute("data-original") || "—";
      if (el.classList.contains("card")) {
        var tone = el.getAttribute("data-original-tone") || "";
        val.className = "value" + (tone ? " " + tone : "");
        var n = el.querySelector(".note");
        if (n) n.textContent = el.getAttribute("data-original-note") || "";
      }
    });
  }
  function dashRecOnly() {
    document.querySelectorAll("[data-rec-only='1']").forEach(function (el) {
      var val = el.querySelector(".value") || el.querySelector("td.num");
      if (!val) return;
      val.textContent = "—";
      if (el.classList.contains("card")) {
        val.className = "value";
        var n = el.querySelector(".note");
        if (n) n.textContent = "Len pre odporúčanú veľkosť";
      }
    });
  }
  function paint(i, j) {
    var kwp = (data.pv_kwp || [])[i];
    var kwh = (data.battery_kwh || [])[j];
    var sav = cellVal("annual_savings_eur", i, j);
    var npv = cellVal("npv_eur", i, j);
    var pb = cellVal("simple_payback_years", i, j);
    var dpb = cellVal("discounted_payback_years", i, j);
    var capex = cellVal("total_capex_eur", i, j);
    var sc = cellVal("self_consumption_pct", i, j);
    var opexToday = cellVal("operating_cost_baseline_eur", i, j);
    var opexPlant = cellVal("operating_cost_optimized_eur", i, j);
    var cycles = cellVal("battery_cycles_per_year", i, j);
    var life = cellVal("battery_life_years", i, j);
    var cash = cellVal("cashflow_after_om_eur", i, j);
    var isRec = i === rec.i && j === rec.j;
    var recSav = cellVal("annual_savings_eur", rec.i, rec.j);
    var recNpv = cellVal("npv_eur", rec.i, rec.j);
    var recPb = cellVal("simple_payback_years", rec.i, rec.j);
    var recCapex = cellVal("total_capex_eur", rec.i, rec.j);
    var sizeNote = "Simulácia " + kw(kwp, "kWp") + " FVE + " + kw(kwh, "kWh") + " batéria";
    var cfg = document.getElementById("live-config");
    if (cfg) cfg.textContent = sizeNote;
    var title = document.getElementById("hm-title");
    if (title) title.textContent =
      "Vybraná konfigurácia: " + kw(kwp, "kWp") + " FVE + " + kw(kwh, "kWh") + " batéria";
    var badge = document.getElementById("hm-badge");
    if (badge) badge.textContent = isRec
      ? ("Odporúčanie: " + (data.objective_label || "vybraný cieľ"))
      : "Porovnanie voči odporúčaniu";
    var cards = [
      ["FVE", kw(kwp, "kWp"), ""],
      ["Batéria", kw(kwh, "kWh"), ""],
      ["Ročná úspora", money(sav), "Simulácia tejto veľkosti"],
      ["Celkový CAPEX", money(capex), "Investícia pri tejto veľkosti"],
      ["Jednoduchá návratnosť", years(pb), ""],
      ["Čistá súčasná hodnota", money(npv), "Za dobu amortizácie"],
      ["Vlastná spotreba", pct(sc), "Podiel vyrobenej energie spotrebovanej na mieste"]
    ];
    var hmCards = document.getElementById("hm-cards");
    if (hmCards) hmCards.innerHTML = cards.map(function (c) {
      return '<div class="card"><div class="label">' + c[0] + '</div><div class="value">' + c[1] +
        '</div>' + (c[2] ? '<div class="note">' + c[2] + '</div>' : '') + '</div>';
    }).join("");
    function row(label, a, b, d) {
      var tone = d && d.cls ? ' class="num value ' + d.cls + '"' : ' class="num"';
      return "<tr><td>" + label + "</td><td class='num'>" + a + "</td><td class='num'>" + b +
        "</td><td" + tone + ">" + (d ? d.text : "—") + "</td></tr>";
    }
    var dSav = (!isRec && num(sav) && num(recSav))
      ? { text: ((sav - recSav) >= 0 ? "+" : "") + money(sav - recSav), cls: sav >= recSav ? "good" : "bad" }
      : { text: "—", cls: "" };
    var dCap = (!isRec && num(capex) && num(recCapex))
      ? { text: ((capex - recCapex) >= 0 ? "+" : "") + money(capex - recCapex), cls: capex <= recCapex ? "good" : "" }
      : { text: "—", cls: "" };
    var dNpv = (!isRec && num(npv) && num(recNpv))
      ? { text: ((npv - recNpv) >= 0 ? "+" : "") + money(npv - recNpv), cls: npv >= recNpv ? "good" : "bad" }
      : { text: "—", cls: "" };
    var dPb = (!isRec && num(pb) && num(recPb))
      ? { text: ((pb - recPb) >= 0 ? "+" : "") + (pb - recPb).toFixed(1) + " r", cls: pb <= recPb ? "good" : "bad" }
      : { text: "—", cls: "" };
    var cmp = document.getElementById("hm-compare");
    if (cmp) cmp.innerHTML =
      "<thead><tr><th></th><th class='num'>Vybrané</th><th class='num'>Odporúčané</th><th class='num'>Rozdiel</th></tr></thead><tbody>" +
      row("Ročná úspora", money(sav), money(recSav), dSav) +
      row("CAPEX", money(capex), money(recCapex), dCap) +
      row("NPV", money(npv), money(recNpv), dNpv) +
      row("Návratnosť", years(pb), years(recPb), dPb) +
      "</tbody>";
    var cursor = document.getElementById("hm-cursor");
    if (cursor) {
      cursor.setAttribute("x", cell.left + i * cell.w);
      cursor.setAttribute("y", cell.top + j * cell.h);
    }
    setCard("savings", money(sav), sizeNote, "good");
    setCard("capex", money(capex), "FVE, úložisko a inštalácia", "");
    setCard("payback", years(pb), isRec ? null : "Bez diskontovania", pbTone(pb));
    setCard("npv", money(npv), isRec ? null : "Za dobu amortizácie", npvTone(npv));
    setDetail("opex-today", money(opexToday));
    setDetail("opex-plant", money(opexPlant));
    setDetail("dpb", dpbText(dpb));
    setDetail("cycles", cyclesText(kwh, cycles));
    setDetail("life", lifeText(kwh, life));
    setDetail("cashflow", money(cash));
    var note = document.getElementById("live-details-note");
    if (isRec) {
      restoreRecOnly();
      if (note) note.hidden = true;
    } else {
      dashRecOnly();
      if (note) note.hidden = false;
    }
  }
  document.querySelectorAll(".hm-cell").forEach(function (el) {
    function go() { paint(Number(el.getAttribute("data-i")), Number(el.getAttribute("data-j"))); }
    el.addEventListener("click", go);
    el.addEventListener("keydown", function (ev) {
      if (ev.key === "Enter" || ev.key === " ") { ev.preventDefault(); go(); }
    });
  });
  paint(rec.i, rec.j);
  }
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
"""


def build_html(
    payload: dict[str, Any],
    *,
    heatmap: dict[str, Any] | None,
    calibration: dict[str, Any] | None,
    ranking: dict[str, Any] | None,
    site_name: str = "",
) -> str:
    kpis = payload.get("decision_kpis") or {}
    generated = datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M UTC")

    payback = kpis.get("simple_payback_years")
    payback_tone = "good" if is_number(payback) and payback <= 8 else (
        "warn" if is_number(payback) and payback <= 12 else "bad"
    )
    npv = kpis.get("npv_operating_eur")
    npv_tone = "good" if is_number(npv) and npv > 0 else "bad"
    view = _objective_view(heatmap)
    objective_note = f"Tento beh hľadal {view['label']}."
    rec = (heatmap or {}).get("recommended") or {}
    if rec:
        kpis = dict(kpis)
        if rec.get("annual_savings_eur") is not None:
            kpis["operating_savings_annual_estimate_eur"] = rec.get("annual_savings_eur")
        if rec.get("total_capex_eur") is not None:
            kpis["total_capex_eur"] = rec.get("total_capex_eur")
        if rec.get("simple_payback_years") is not None:
            kpis["simple_payback_years"] = rec.get("simple_payback_years")
            payback = rec.get("simple_payback_years")
        if rec.get("npv_eur") is not None:
            kpis["npv_operating_eur"] = rec.get("npv_eur")
            npv = rec.get("npv_eur")
        if rec.get("operating_cost_baseline_eur") is not None:
            kpis["operating_cost_baseline_eur"] = rec.get("operating_cost_baseline_eur")
        if rec.get("operating_cost_optimized_eur") is not None:
            kpis["operating_cost_with_pv_battery_eur"] = rec.get("operating_cost_optimized_eur")
        if rec.get("discounted_payback_years") is not None:
            kpis["discounted_payback_years"] = rec.get("discounted_payback_years")
        if rec.get("battery_cycles_per_year") is not None:
            kpis["battery_annual_equivalent_cycles_est"] = rec.get("battery_cycles_per_year")
        if rec.get("battery_life_years") is not None:
            kpis["battery_estimated_life_years_effective"] = rec.get("battery_life_years")
        if rec.get("cashflow_after_om_eur") is not None:
            kpis["finance_annual_net_cashflow_after_finance_eur"] = rec.get("cashflow_after_om_eur")
        if rec.get("battery_kwh") is not None:
            kpis["battery_kwh"] = rec.get("battery_kwh")
    payback_tone = "good" if is_number(payback) and payback <= 8 else (
        "warn" if is_number(payback) and payback <= 12 else "bad"
    )
    npv_tone = "good" if is_number(npv) and npv > 0 else "bad"
    rec_size_note = ""
    if is_number(rec.get("pv_kwp")) or is_number(rec.get("battery_kwh")):
        rec_size_note = (
            f"Simulácia {fmt_num(rec.get('pv_kwp'), 0, 'kWp')} FVE + "
            f"{fmt_num(rec.get('battery_kwh'), 0, 'kWh')} batéria"
        )

    kpi_cards = [
        card(
            "Ročná úspora",
            fmt_money(kpis.get("operating_savings_annual_estimate_eur")),
            rec_size_note or "Úspora prevádzkových nákladov za rok",
            "good",
            live="savings",
        ),
        card("Celkový CAPEX", fmt_money(kpis.get("total_capex_eur")), "FVE, úložisko a inštalácia", live="capex"),
        card("Jednoduchá návratnosť", fmt_years(payback), objective_note if view["id"] == "shortest_payback" else "Bez diskontovania", payback_tone, live="payback"),
        card("Čistá súčasná hodnota", fmt_money(npv), objective_note if view["id"] == "max_npv" else "Za dobu amortizácie", npv_tone, live="npv"),
    ]
    if view["id"] == "shortest_payback":
        kpi_cards[0], kpi_cards[2] = kpi_cards[2], kpi_cards[0]
    kpi_cards += [
        card(
            "Vnútorné výnosové percento",
            fmt_num(kpis.get("irr_pct"), 1, "%"),
            "Z neleverovaných peňažných tokov",
            "good" if is_number(kpis.get("irr_pct")) and kpis.get("irr_pct") > 8 else "",
            live="irr",
            rec_only=True,
        ),
        card(
            "Úspora pri P90",
            fmt_money(kpis.get("p90_annual_savings_eur")),
            "Prekročená v 9 z 10 scenárov",
            live="p90",
            rec_only=True,
        ),
        card(
            "Uvoľnená rezervovaná kapacita",
            fmt_num(kpis.get("rv_downsizing_potential_kw"), 0, "kW"),
            "Zmluvná kapacita, ktorú možno znížiť",
            live="rv",
            rec_only=True,
        ),
    ]
    cards = "".join(kpi_cards)

    sections = [
        "<h2>Rozhodovacie zhrnutie</h2>",
        f'<div class="panel" style="margin-bottom:14px"><span class="badge ok">'
        f'Cieľ návrhu: {esc(view["label"])}</span>'
        f'<span class="badge" id="live-config">{esc(rec_size_note or "Odporúčaná veľkosť")}</span></div>',
        f'<div class="cards">{cards}</div>',
        "<h2>Ceny elektriny použité vo výpočte</h2>",
        render_prices(payload.get("prices")),
    ]

    consumption_html = render_consumption(
        payload.get("consumption"), payload.get("single_chart")
    )
    if consumption_html:
        sections += [
            "<h2>Spotreba energie zo siete</h2>",
            consumption_html,
        ]

    if heatmap:
        sections += [
            "<h2>Veľkosť systému a návratnosť</h2>",
            render_heatmap(heatmap),
            render_heatmap_table(heatmap),
        ]

    sections += ["<h2>Pôvod prognózy výroby</h2>", render_forecast_panel(calibration)]

    equipment = render_equipment(ranking)
    if equipment:
        sections += ["<h2>Odporúčané moduly</h2>", equipment]

    baseline = kpis.get("operating_cost_baseline_eur")
    optimised = kpis.get("operating_cost_with_pv_battery_eur")
    dpb = kpis.get("discounted_payback_years")
    if not is_number(dpb):
        capex = rec.get("total_capex_eur") or kpis.get("total_capex_eur")
        annual = rec.get("annual_savings_eur") or kpis.get("operating_savings_annual_estimate_eur")
        dr = kpis.get("discount_rate") or (heatmap or {}).get("discount_rate") or 0.075
        years = int(kpis.get("analysis_horizon_years") or (heatmap or {}).get("amortization_years") or 15)
        if is_number(capex) and is_number(annual) and annual > 0 and capex > 0:
            cum = 0.0
            for t in range(1, max(int(years) + 10, 40) + 1):
                inc = annual / (1.0 + float(dr)) ** t
                prev = cum
                cum += inc
                if cum >= capex:
                    dpb = t - 1 + (capex - prev) / inc
                    break
    if not is_number(dpb):
        dpb_text = "Nevráti sa pri diskontovaní"
    else:
        dpb_text = fmt_years(dpb)
    bat_kwh = rec.get("battery_kwh")
    if bat_kwh is None:
        bat_kwh = kpis.get("battery_kwh")
    no_battery = not is_number(bat_kwh) or float(bat_kwh) <= 1e-6
    cycles = kpis.get("battery_annual_equivalent_cycles_est")
    if no_battery:
        cycles_text = "Bez batérie"
        life_text = "Neaplikuje sa"
    else:
        cycles_text = fmt_num(cycles, 0)
        life_text = fmt_years(kpis.get("battery_estimated_life_years_effective"))
    cash = kpis.get("finance_annual_net_cashflow_after_finance_eur")
    if not is_number(cash) and is_number(kpis.get("operating_savings_annual_estimate_eur")):
        cash = kpis.get("operating_savings_annual_estimate_eur")
    assumptions = [
        ("Prevádzkové náklady dnes", fmt_money(baseline), "opex-today", False),
        ("Prevádzkové náklady s FVE a úložiskom", fmt_money(optimised), "opex-plant", False),
        ("Diskontovaná návratnosť", dpb_text, "dpb", False),
        ("IRR pri P90", fmt_num(kpis.get("p90_irr_pct"), 1, "%"), "p90-irr", True),
        (
            "NPV pri P50 / P90",
            f"{fmt_money(kpis.get('p50_npv_eur'))} / {fmt_money(kpis.get('p90_npv_eur'))}",
            "p50-p90-npv",
            True,
        ),
        (
            "Pravdepodobnosť NPV > 0",
            fmt_num((kpis.get("probability_npv_positive") or 0) * 100, 0, "%")
            if is_number(kpis.get("probability_npv_positive"))
            else "—",
            "npv-prob",
            True,
        ),
        ("Úspora pri P50", fmt_money(kpis.get("p50_annual_savings_eur")), "p50-sav", True),
        ("Cyklov batérie za rok", cycles_text, "cycles", False),
        ("Očakávaná životnosť batérie", life_text, "life", False),
        (
            "Obchodná marža za rok",
            fmt_money(kpis.get("trading_only_annual_margin_eur_estimate")),
            "trading",
            True,
        ),
        ("Čistý cashflow (úspora − O&M)", fmt_money(cash), "cashflow", False),
    ]
    detail_rows = "".join(
        detail_row(label, value, live, rec_only) for label, value, live, rec_only in assumptions
    )
    sections += [
        "<h2>Podrobnosti</h2>",
        '<div class="panel"><table id="live-details">'
        f"{detail_rows}</table>"
        '<div class="note-block" id="live-details-note" hidden>'
        "P50/P90, IRR, rezervovaná kapacita a obchodná marža sú z Monte Carlo "
        "len pre odporúčanú veľkosť. Ostatné čísla sú z úplnej simulácie vybranej bunky."
        "</div></div>",
    ]

    flags = payload.get("quality_flags") or {}
    badges = []
    if flags.get("catalog_url_outage_detected"):
        badges.append('<span class="badge warn">Katalóg bol nedostupný, použité lokálne ceny zariadení</span>')
    source = flags.get("price_source")
    if source == "okte_dam":
        badges.append('<span class="badge ok">Ceny z verejného trhu OKTE ISOT DAM</span>')
    elif source == "load_csv":
        badges.append('<span class="badge ok">Ceny zo stĺpca v profile odberu</span>')
    elif source == "prices_csv":
        badges.append('<span class="badge ok">Ceny zo samostatného cenníka</span>')
    elif flags.get("historical_prices_in_csv"):
        badges.append('<span class="badge ok">Historické ceny v vstupnom súbore</span>')
    else:
        badges.append('<span class="badge warn">Zdroj cien sa nepodarilo potvrdiť</span>')
    if badges:
        sections += [
            "<h2>Kvalita vstupov</h2>",
            f'<div class="panel">{"".join(badges)}</div>',
        ]

    title = f"Investičný podklad: FVE a batériové úložisko{f' — {site_name}' if site_name else ''}"
    return (
        "<!doctype html><html lang='sk'><head><meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width, initial-scale=1'>"
        f"<title>{esc(title)}</title><style>{CSS}</style></head><body><div class='wrap'>"
        f"<header><div class='kicker'>UC3.2. SEED</div>"
        f"<h1>{esc(title)}</h1>"
        f"<div class='sub'>Vygenerované {esc(generated)}</div></header>"
        + "".join(sections)
        + f"<script>{_HEATMAP_JS}</script>"
        + "</div></body></html>"
    )
