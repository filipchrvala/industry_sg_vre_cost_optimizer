"""Render the investment dashboard as a single self-contained HTML file.

Charts are inline SVG rather than a JavaScript charting library. The dashboard is
handed to people who open it from a shared drive or an email attachment, often
inside a network that will not fetch a CDN, and a chart that silently fails to
draw is worse than no chart. Inline SVG also survives print-to-PDF, which is how
these end up in a board pack.
"""

from __future__ import annotations

import html
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
h1 { font-size: 26px; margin: 0 0 6px; letter-spacing: -0.01em; }
.sub { color: var(--muted); font-size: 14px; }
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
@media print {
  body { background: #fff; }
  .panel, .card { break-inside: avoid; }
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


def card(label: str, value: str, note: str = "", tone: str = "") -> str:
    tone_class = f" {tone}" if tone else ""
    note_html = f'<div class="note">{esc(note)}</div>' if note else ""
    return (
        f'<div class="card"><div class="label">{esc(label)}</div>'
        f'<div class="value{tone_class}">{esc(value)}</div>{note_html}</div>'
    )


def render_heatmap(heatmap: dict[str, Any], metric: str = "npv_eur") -> str:
    """Draw the PV x battery grid as an SVG table of coloured cells."""
    axes = heatmap.get("axes") or {}
    pv_axis: list[float] = axes.get("pv_kwp") or []
    bat_axis: list[float] = axes.get("battery_kwh") or []
    grid = (heatmap.get("grids") or {}).get(metric) or []
    if not pv_axis or not bat_axis or not grid:
        return '<div class="panel">Heatmap not available for this run.</div>'

    values = [v for row in grid for v in row if is_number(v)]
    if not values:
        return '<div class="panel">Heatmap produced no finite results.</div>'
    low, high = min(values), max(values)
    span = high - low if high > low else 1.0

    cell_w, cell_h = 62, 34
    left, top = 108, 46
    width = left + cell_w * len(pv_axis) + 24
    height = top + cell_h * len(bat_axis) + 62

    recommended = heatmap.get("recommended") or {}
    best_pv, best_bat = recommended.get("pv_kwp"), recommended.get("battery_kwh")

    parts = [
        f'<svg viewBox="0 0 {width} {height}" width="100%" role="img" '
        f'aria-label="Net present value across PV and battery sizes" '
        f'style="font: 11px sans-serif; max-width:{width}px">'
    ]
    parts.append(
        f'<text x="{left}" y="18" font-size="12" font-weight="600" fill="#14181f">'
        f'Net present value by system size (€)</text>'
    )
    parts.append(
        f'<text x="{left}" y="34" font-size="11" fill="#5c6675">PV size (kWp) →</text>'
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
            value = grid[j][i] if i < len(grid[j]) else None
            if not is_number(value):
                parts.append(
                    f'<rect x="{x}" y="{y}" width="{cell_w - 2}" height="{cell_h - 2}" '
                    f'fill="#eef0f3" stroke="#fff"/>'
                )
                continue
            fraction = (value - low) / span
            fill = colour_for(fraction)
            text_fill = "#ffffff" if fraction < 0.55 else "#14181f"
            parts.append(
                f'<rect x="{x}" y="{y}" width="{cell_w - 2}" height="{cell_h - 2}" '
                f'fill="{fill}" stroke="#fff"><title>{kwp:,.0f} kWp / {kwh:,.0f} kWh: '
                f'{value:,.0f} €</title></rect>'.replace(",", " ")
            )
            label = f"{value / 1000:,.0f}k".replace(",", " ") if abs(value) >= 1000 else f"{value:,.0f}".replace(",", " ")
            parts.append(
                f'<text x="{x + cell_w / 2 - 1:.1f}" y="{y + cell_h / 2 + 4:.1f}" '
                f'text-anchor="middle" fill="{text_fill}">{label}</text>'
            )
            if (
                is_number(best_pv) and is_number(best_bat)
                and abs(kwp - best_pv) < 1e-6 and abs(kwh - best_bat) < 1e-6
            ):
                parts.append(
                    f'<rect x="{x}" y="{y}" width="{cell_w - 2}" height="{cell_h - 2}" '
                    f'fill="none" stroke="#b23a3a" stroke-width="2.5"/>'
                )

    parts.append(
        f'<text x="{left}" y="{height - 26}" fill="#5c6675">'
        f'Battery size (kWh) on the vertical axis. Red outline marks the recommended '
        f'combination. Values in thousands of euro.</text>'
    )
    parts.append("</svg>")

    swatch = "".join(
        f'<span style="background:{colour_for(i / 6)}"></span>' for i in range(7)
    )
    legend = (
        f'<div class="legend"><span>{fmt_money(low)}</span>'
        f'<span class="swatch">{swatch}</span><span>{fmt_money(high)}</span></div>'
    )
    return f'<div class="panel"><figure>{"".join(parts)}</figure>{legend}</div>'


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


def render_forecast_panel(calibration: dict[str, Any] | None) -> str:
    """State plainly how far the production forecast is backed by measurement."""
    if not calibration:
        return (
            '<div class="panel"><span class="badge warn">Not calibrated</span>'
            "Production comes from the Open-Meteo weather model through the physics "
            "and AI chain, with no ground measurement in this run.</div>"
        )

    available = bool(calibration.get("calibration_available"))
    station = calibration.get("station") or {}
    bias = calibration.get("bias") or {}

    if not available:
        reason = calibration.get("skip_reason", "no measurement available")
        return (
            f'<div class="panel"><span class="badge warn">Not calibrated</span>'
            f"Production rests on the Open-Meteo model alone: {esc(reason)}. "
            f"Treat the yield as a model estimate rather than a measured one.</div>"
        )

    rows = [
        ("Reference station", f"{station.get('name', '—')} ({fmt_num(station.get('distance_km'), 1, 'km')} away)"),
        ("Measured bias of the weather model", fmt_num(bias.get("bias_pct"), 1, "%")),
        ("Mean absolute error", fmt_num(bias.get("mae_w_m2"), 1, "W/m²")),
        ("Correlation with measurement", fmt_num(bias.get("correlation"), 3)),
        ("Applied irradiance scale", fmt_num(calibration.get("irradiance_scale_factor"), 4)),
        ("Daylight steps compared", fmt_num(bias.get("daylight_steps"), 0)),
    ]
    body = "".join(
        f"<tr><td>{esc(label)}</td><td class='num'>{esc(value)}</td></tr>" for label, value in rows
    )
    return (
        '<div class="panel"><span class="badge ok">Calibrated against SHMÚ</span>'
        f"<table>{body}</table>"
        '<div class="note-block">Open-Meteo is a model product, and its irradiance bias '
        "differs by location. The correction above is derived from measured global "
        "radiation published by the Slovak Hydrometeorological Institute.</div></div>"
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
        "<tr><th>Manufacturer</th><th>Model</th><th class='num'>Wp</th>"
        "<th class='num'>Efficiency</th><th class='num'>€/Wp</th>"
        "<th class='num'>Modules</th><th class='num'>Area</th><th class='num'>Score</th></tr>"
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
        '<div class="note-block">Ranked for this site by energy density, cost per watt, '
        "shading tolerance and efficiency. The highlighted row is the best match.</div></div>"
    )


def render_heatmap_table(heatmap: dict[str, Any]) -> str:
    recommended = heatmap.get("recommended") or {}
    current = heatmap.get("current_scenario") or {}
    rows = [
        ("Recommended PV", fmt_num(recommended.get("pv_kwp"), 0, "kWp")),
        ("Recommended battery", fmt_num(recommended.get("battery_kwh"), 0, "kWh")),
        ("Sized scenario PV", fmt_num(current.get("pv_kwp"), 0, "kWp")),
        ("Sized scenario battery", fmt_num(current.get("battery_kwh"), 0, "kWh")),
        ("Annual saving at the optimum", fmt_money(recommended.get("annual_savings_eur"))),
        ("CAPEX at the optimum", fmt_money(recommended.get("total_capex_eur"))),
        ("Payback at the optimum", fmt_years(recommended.get("simple_payback_years"))),
    ]
    body = "".join(
        f"<tr><td>{esc(k)}</td><td class='num'>{esc(v)}</td></tr>" for k, v in rows
    )
    source = heatmap.get("pv_profile_source")
    note = (
        "Every cell is a full economic simulation against the AI production forecast."
        if source == "ai_forecast"
        else "Forecast unavailable for this run; cells fall back to a synthetic profile."
    )
    return f'<div class="panel"><table>{body}</table><div class="note-block">{esc(note)}</div></div>'


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

    cards = "".join(
        [
            card(
                "Annual saving",
                fmt_money(kpis.get("operating_savings_annual_estimate_eur")),
                "Operating cost avoided per year",
                "good",
            ),
            card("Total CAPEX", fmt_money(kpis.get("total_capex_eur")), "PV, storage and installation"),
            card("Simple payback", fmt_years(payback), "Undiscounted", payback_tone),
            card("Net present value", fmt_money(npv), "Over the amortisation period", npv_tone),
            card(
                "Internal rate of return",
                fmt_num(kpis.get("irr_pct"), 1, "%"),
                "On the unlevered cashflows",
                "good" if is_number(kpis.get("irr_pct")) and kpis.get("irr_pct") > 8 else "",
            ),
            card(
                "Saving at P90",
                fmt_money(kpis.get("p90_annual_savings_eur")),
                "Exceeded in 9 outcomes out of 10",
            ),
            card(
                "Peak capacity freed",
                fmt_num(kpis.get("rv_downsizing_potential_kw"), 0, "kW"),
                "Contracted capacity that could be released",
            ),
        ]
    )

    sections = [
        f"<h2>Decision summary</h2>",
        f'<div class="cards">{cards}</div>',
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
            "<h2>System size and return</h2>",
            render_heatmap(heatmap),
            render_heatmap_table(heatmap),
        ]

    sections += ["<h2>Production forecast provenance</h2>", render_forecast_panel(calibration)]

    equipment = render_equipment(ranking)
    if equipment:
        sections += ["<h2>Recommended modules</h2>", equipment]

    baseline = kpis.get("operating_cost_baseline_eur")
    optimised = kpis.get("operating_cost_with_pv_battery_eur")
    assumptions = [
        ("Operating cost today", fmt_money(baseline)),
        ("Operating cost with PV and storage", fmt_money(optimised)),
        ("Discounted payback", fmt_years(kpis.get("discounted_payback_years"))),
        ("IRR at P90", fmt_num(kpis.get("p90_irr_pct"), 1, "%")),
        ("NPV at P50 / P90", f"{fmt_money(kpis.get('p50_npv_eur'))} / {fmt_money(kpis.get('p90_npv_eur'))}"),
        ("Probability NPV > 0", fmt_num((kpis.get("probability_npv_positive") or 0) * 100, 0, "%") if is_number(kpis.get("probability_npv_positive")) else "—"),
        ("Saving at P50", fmt_money(kpis.get("p50_annual_savings_eur"))),
        ("Battery cycles per year", fmt_num(kpis.get("battery_annual_equivalent_cycles_est"), 0)),
        ("Expected battery life", fmt_years(kpis.get("battery_estimated_life_years_effective"))),
        (
            "Trading margin per year",
            fmt_money(kpis.get("trading_only_annual_margin_eur_estimate")),
        ),
        ("Net cashflow after financing", fmt_money(kpis.get("finance_annual_net_cashflow_after_finance_eur"))),
    ]
    detail_rows = "".join(
        f"<tr><td>{esc(k)}</td><td class='num'>{esc(v)}</td></tr>" for k, v in assumptions
    )
    sections += ["<h2>Detail</h2>", f'<div class="panel"><table>{detail_rows}</table></div>']

    flags = payload.get("quality_flags") or {}
    badges = []
    if flags.get("catalog_url_outage_detected"):
        badges.append('<span class="badge warn">Catalog source unreachable, local prices used</span>')
    if flags.get("historical_prices_in_csv"):
        badges.append('<span class="badge ok">Metered prices from the input file</span>')
    else:
        badges.append('<span class="badge warn">Prices modelled, not metered</span>')
    if badges:
        sections += [
            "<h2>Data quality</h2>",
            f'<div class="panel">{"".join(badges)}</div>',
        ]

    title = f"Investment case: PV and battery storage{f' — {site_name}' if site_name else ''}"
    return (
        "<!doctype html><html lang='sk'><head><meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width, initial-scale=1'>"
        f"<title>{esc(title)}</title><style>{CSS}</style></head><body><div class='wrap'>"
        f"<header><h1>{esc(title)}</h1>"
        f"<div class='sub'>Generated {esc(generated)} · UC3.2 cost optimizer</div></header>"
        + "".join(sections)
        + "</div></body></html>"
    )
