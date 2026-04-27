"""
CFO dashboard – číta výstup z DashboardPiece (dashboard_data.json).

Spustenie (z priečinka pitonak_mrk):
  streamlit run streamlit_app.py

Voliteľne:
  set STREAMLIT_DASHBOARD_JSON=C:\\cesta\\k\\dashboard_data.json
"""
from __future__ import annotations

import json
import math
import os
from pathlib import Path

import pandas as pd
import streamlit as st
import yaml

DEFAULT_DASHBOARD = (
    Path(__file__).resolve().parent / "tests" / "DashboardPiece_Outputs" / "dashboard_data.json"
)
DEFAULT_REPORT = (
    Path(__file__).resolve().parent / "tests" / "SimulatePiece_Outputs" / "mrk_savings_report.json"
)
DEFAULT_SCENARIO = Path(__file__).resolve().parent / "user_input" / "scenario.yaml"


def _load_dashboard(path: Path) -> dict:
    if not path.is_file():
        raise FileNotFoundError(str(path))
    return json.loads(path.read_text(encoding="utf-8"))


def _load_optional_json(path: Path) -> dict | None:
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _load_optional_yaml(path: Path) -> dict | None:
    if not path.is_file():
        return None
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _render_auto_controls(scenario_path: Path) -> None:
    scenario = _load_optional_yaml(scenario_path) or {}
    eq = scenario.get("equipment") or {}
    auto = eq.get("auto") or {}
    constraints = eq.get("constraints") or {}

    st.sidebar.markdown("---")
    st.sidebar.subheader("Auto návrh (užívateľ)")
    with st.sidebar.expander("Parametre auto optimalizácie", expanded=False):
        scope = st.selectbox(
            "Scope riešenia",
            options=["pv_and_battery", "pv_only", "battery_only"],
            index=["pv_and_battery", "pv_only", "battery_only"].index(
                str(eq.get("system_scope", "pv_and_battery"))
                if str(eq.get("system_scope", "pv_and_battery")) in ("pv_and_battery", "pv_only", "battery_only")
                else "pv_and_battery"
            ),
        )
        objective = st.selectbox(
            "Cieľ optimalizácie",
            options=["max_npv", "shortest_payback"],
            index=0 if str(auto.get("objective", "max_npv")) == "max_npv" else 1,
        )
        target_pb = st.number_input(
            "Cieľová návratnosť (roky)",
            min_value=1.0,
            max_value=25.0,
            value=float(auto.get("target_payback_years", 9.0)),
            step=0.5,
        )
        min_pv = st.number_input(
            "Min. FVE (kWp)",
            min_value=0.0,
            max_value=10000.0,
            value=float(auto.get("min_pv_kwp", 300.0)),
            step=50.0,
        )
        min_bat = st.number_input(
            "Min. batéria (kWh)",
            min_value=0.0,
            max_value=20000.0,
            value=float(auto.get("min_battery_kwh", 300.0)),
            step=50.0,
        )
        max_capex = st.number_input(
            "CAPEX strop (€)",
            min_value=50000.0,
            max_value=50000000.0,
            value=float(constraints.get("max_capex_eur", 800000.0)),
            step=50000.0,
        )
        require_pv = st.checkbox("Vyžadovať FVE", value=bool(auto.get("require_pv", True)))
        require_bat = st.checkbox("Vyžadovať batériu", value=bool(auto.get("require_battery", True)))
        if st.button("Uložiť do scenario.yaml", use_container_width=True):
            scenario.setdefault("equipment", {})
            scenario["equipment"]["selection_mode"] = "auto"
            scenario["equipment"]["system_scope"] = scope
            scenario["equipment"].setdefault("constraints", {})
            scenario["equipment"]["constraints"]["max_capex_eur"] = float(max_capex)
            scenario["equipment"].setdefault("auto", {})
            scenario["equipment"]["auto"].update(
                {
                    "objective": objective,
                    "target_payback_years": float(target_pb),
                    "min_pv_kwp": float(min_pv),
                    "min_battery_kwh": float(min_bat),
                    "require_pv": bool(require_pv),
                    "require_battery": bool(require_bat),
                }
            )
            scenario_path.write_text(yaml.safe_dump(scenario, sort_keys=False, allow_unicode=True), encoding="utf-8")
            st.success(f"Uložené: `{scenario_path}`")
            st.caption("Potom spusti `python run_workflow.py` pre nový návrh.")


def _load_profile_df(report: dict) -> pd.DataFrame | None:
    artifacts = report.get("artifacts", {}) or {}
    csv_path = artifacts.get("baseline_vs_optimized_profile_csv")
    if not csv_path:
        return None
    p = Path(csv_path)
    if not p.is_file():
        return None

    df = pd.read_csv(p)
    if "datetime" not in df.columns:
        return None

    df["čas"] = pd.to_datetime(df["datetime"], errors="coerce")
    out = pd.DataFrame({"čas": df["čas"]})
    if "baseline_energy_kwh_interval" in df.columns:
        out["Bez FVE a batérie"] = df["baseline_energy_kwh_interval"]
    if "optimized_energy_kwh_interval" in df.columns:
        out["S FVE a batériou"] = df["optimized_energy_kwh_interval"]
    out = out.dropna(subset=["čas"])
    if out.shape[1] < 2:
        return None
    return out


def _downsample_df(df: pd.DataFrame, max_rows: int) -> pd.DataFrame:
    if len(df) <= max_rows:
        return df
    step = max(1, len(df) // max_rows)
    return df.iloc[::step].reset_index(drop=True)


def _resolve_plot_window(df: pd.DataFrame) -> tuple[pd.Timestamp, pd.Timestamp]:
    min_dt = df["čas"].min()
    max_dt = df["čas"].max()
    default_start = max(min_dt, max_dt - pd.Timedelta(days=30))
    picked = st.sidebar.date_input(
        "Zobrazené obdobie (posun + zoom)",
        value=(default_start.date(), max_dt.date()),
        min_value=min_dt.date(),
        max_value=max_dt.date(),
    )
    if isinstance(picked, tuple) and len(picked) == 2:
        start_dt = pd.Timestamp(picked[0])
        end_dt = pd.Timestamp(picked[1]) + pd.Timedelta(days=1) - pd.Timedelta(minutes=1)
        if end_dt < start_dt:
            start_dt, end_dt = end_dt, start_dt
        return start_dt, end_dt
    return default_start, max_dt


def main() -> None:
    st.set_page_config(page_title="MRK – CFO dashboard", layout="wide")
    st.title("MRK analýza – CFO prehľad")
    st.caption("Zdroj: výstup workflow (`dashboard_data.json`).")

    default_path = os.environ.get("STREAMLIT_DASHBOARD_JSON", str(DEFAULT_DASHBOARD))
    path_str = st.sidebar.text_input("Cesta k dashboard_data.json", value=default_path)
    dashboard_path = Path(path_str)
    report_path = Path(
        os.environ.get("STREAMLIT_REPORT_JSON", str(DEFAULT_REPORT))
    )
    scenario_path = Path(os.environ.get("STREAMLIT_SCENARIO_YAML", str(DEFAULT_SCENARIO)))

    try:
        data = _load_dashboard(dashboard_path)
    except FileNotFoundError:
        st.error(f"Súbor neexistuje: `{dashboard_path}`. Najprv spusti `python run_workflow.py`.")
        st.stop()

    report = _load_optional_json(report_path) or {}
    scenario = _load_optional_yaml(scenario_path) or {}
    _render_auto_controls(scenario_path)

    fmt = data.get("format", "")
    if fmt != "cfo_finance_dashboard_v1":
        st.warning(f"Neočakávaný formát `{fmt}` – zobrazenie môže byť neúplné.")

    st.sidebar.markdown(f"**Vygenerované:** `{data.get('generated_at_utc', '—')}`")

    kpi = data.get("decision_kpis") or {}
    profile_df = _load_profile_df(report)

    st.subheader("Kľúčové ukazovatele")
    if profile_df is not None and not profile_df.empty:
        dt_from = profile_df["čas"].min()
        dt_to = profile_df["čas"].max()
        sample_days = report.get("executive_summary", {}).get("days_in_sample")
        if sample_days is not None:
            st.caption(
                f"KPI sú počítané za obdobie `{dt_from:%Y-%m-%d %H:%M}` až `{dt_to:%Y-%m-%d %H:%M}` "
                f"({sample_days:.2f} dňa)."
            )
        else:
            st.caption(
                f"KPI sú počítané za obdobie `{dt_from:%Y-%m-%d %H:%M}` až `{dt_to:%Y-%m-%d %H:%M}`."
            )
    else:
        st.caption("KPI obdobie: nepodarilo sa načítať profilové časové dáta z reportu.")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Náklady baseline (€)", f"{kpi.get('operating_cost_baseline_eur', 0):,.0f}")
    c2.metric("Náklady s FVE+batériou (€)", f"{kpi.get('operating_cost_with_pv_battery_eur', 0):,.0f}")
    c3.metric("Úspora za obdobie (€)", f"{kpi.get('operating_savings_period_eur', 0):,.0f}")
    c4.metric("Ročný prepočet úspory (€)", f"{kpi.get('operating_savings_annual_estimate_eur', 0):,.0f}")

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("CAPEX celkom (€)", f"{kpi.get('total_capex_eur', 0):,.0f}")
    c6.metric("Jednoduchá návratnosť (r.)", f"{kpi.get('simple_payback_years', '—')}")
    c7.metric("Diskontovaná návratnosť (r.)", f"{kpi.get('discounted_payback_years', '—')}")
    c8.metric("NPV (€)", f"{kpi.get('npv_operating_eur', 0):,.0f}")

    c9, c10 = st.columns(2)
    c9.metric("P50 ročná úspora (€)", f"{kpi.get('p50_annual_savings_eur', 0):,.0f}")
    c10.metric("P90 ročná úspora (€)", f"{kpi.get('p90_annual_savings_eur', 0):,.0f}")
    ann_factor = report.get("executive_summary", {}).get("annualization_factor")
    if ann_factor is not None:
        st.caption(
            f"Ročný prepočet je sekundárny odhad: úspora za obdobie × `{ann_factor:.4f}`."
        )

    st.subheader("RV / MRK príležitosť")
    c11, c12 = st.columns(2)
    c11.metric("Potenciál zníženia RV (kW)", f"{kpi.get('rv_downsizing_potential_kw', 0):,.1f}")
    c12.metric("Úspora fixného RV (obdobie, €)", f"{kpi.get('rv_fixed_fee_savings_period_eur', 0):,.0f}")

    st.subheader("Navrhnutá technológia (FVE + batéria)")
    equipment = report.get("equipment", {})
    resolved = equipment.get("resolved", {})
    hw = equipment.get("hardware_recommendation", {})
    pv_online_top1 = hw.get("pv_online_selected_rank1") or {}
    pv_selected = (hw.get("pv", {}) or {}).get("selected_rank_1", {})
    pv_primary = pv_online_top1 if pv_online_top1 else pv_selected
    battery_selected = hw.get("battery", {}) or {}
    inverter_selected = (hw.get("inverter", {}) or {}).get("selected_rank_1", {})

    c13, c14 = st.columns(2)
    c13.metric("FVE inštalovaný výkon (kWp)", f"{resolved.get('installed_kwp', 0):,.1f}")
    c14.metric("Batéria kapacita (kWh)", f"{resolved.get('energy_kwh', 0):,.1f}")

    c15, c16 = st.columns(2)
    c15.metric(
        "FVE moduly (ks)",
        f"{pv_primary.get('module_count_estimate', pv_primary.get('module_count', '—'))}",
    )
    c16.metric("Batéria max. výkon (kW)", f"{battery_selected.get('max_power_kw', '—')}")
    c17, c18 = st.columns(2)
    c17.metric("FVE meniče (ks)", inverter_selected.get("count", "—"))
    c18.metric("Meniče AC spolu (kW)", f"{inverter_selected.get('total_ac_kw', '—')}")

    if pv_primary or battery_selected:
        pv_source = "online top-1" if pv_online_top1 else "interný katalóg"
        st.markdown(
            f"**FVE model (zdroj: {pv_source}):** {pv_primary.get('manufacturer', '—')} {pv_primary.get('model', '')}  \n"
            f"**Batéria model:** {battery_selected.get('manufacturer', '—')} "
            f"{battery_selected.get('product_line', '')}"
        )
        if inverter_selected:
            st.markdown(
                f"**Menič model:** {inverter_selected.get('manufacturer', '—')} "
                f"{inverter_selected.get('model', '')}  \n"
                f"**DC/AC ratio:** {inverter_selected.get('dc_ac_ratio', '—')}"
            )
    else:
        st.info("Detaily návrhu FVE/batérie neboli nájdené v `mrk_savings_report.json`.")

    st.subheader("Technická skladba a CAPEX transparentnosť")
    pv_cfg = scenario.get("pv", {}) if isinstance(scenario, dict) else {}
    bat_cfg = scenario.get("battery", {}) if isinstance(scenario, dict) else {}
    capex_inputs = report.get("capex_inputs", {}) or {}

    installed_kwp = float(resolved.get("installed_kwp", 0.0) or 0.0)
    energy_kwh = float(resolved.get("energy_kwh", 0.0) or 0.0)
    pv_spec = float(pv_cfg.get("specific_capex_eur_per_kwp", 0.0) or 0.0)
    bat_spec = float(bat_cfg.get("specific_capex_eur_per_kwh", 0.0) or 0.0)

    batt_unit_kwh = float(battery_selected.get("nominal_kwh", 0.0) or 0.0)
    batt_unit_p_kw = float(battery_selected.get("max_power_kw", 0.0) or 0.0)
    batt_units = int(math.ceil(energy_kwh / batt_unit_kwh)) if batt_unit_kwh > 0 else 0
    batt_total_power = batt_units * batt_unit_p_kw

    c17, c18, c19 = st.columns(3)
    c17.metric("Batériové jednotky (odhad ks)", batt_units if batt_units > 0 else "—")
    c18.metric("Kapacita 1 bat. jednotky (kWh)", f"{batt_unit_kwh:,.1f}" if batt_unit_kwh > 0 else "—")
    c19.metric("Celkový bat. výkon (kW, odhad)", f"{batt_total_power:,.1f}" if batt_total_power > 0 else "—")

    st.caption(
        "Meniče sú v reporte už explicitne navrhnuté (model + počet ks + AC výkon spolu + DC/AC ratio). "
        "Ide o orientačný automatický výber z online katalógu; finálny BOM treba potvrdiť realizačným návrhom."
    )

    pv_capex = float(capex_inputs.get("pv_capex_eur", 0.0) or 0.0)
    bat_capex = float(capex_inputs.get("battery_capex_eur", 0.0) or 0.0)
    st.markdown(
        f"**CAPEX zdroj (výpočet):**  \n"
        f"- FVE CAPEX = `installed_kwp × pv.specific_capex_eur_per_kwp` = `{installed_kwp:.1f} × {pv_spec:.1f}` = `{pv_capex:,.2f} €`  \n"
        f"- Batéria CAPEX = `energy_kwh × battery.specific_capex_eur_per_kwh` = `{energy_kwh:.1f} × {bat_spec:.1f}` = `{bat_capex:,.2f} €`  \n"
        f"- Spolu CAPEX = `{pv_capex + bat_capex:,.2f} €`"
    )

    st.subheader("Priebeh spotreby energie: celé obdobie")
    if profile_df is not None and not profile_df.empty:
        start_dt, end_dt = _resolve_plot_window(profile_df)
        filtered = profile_df[(profile_df["čas"] >= start_dt) & (profile_df["čas"] <= end_dt)]
        if filtered.empty:
            st.info("V zvolenom rozsahu nie sú žiadne dáta.")
            return
        max_pts = st.sidebar.slider("Max. bodov v grafe (výkon)", 500, 20000, 4000, 500)
        plot_df = _downsample_df(filtered, max_pts)
        st.line_chart(plot_df.set_index("čas"), height=420)
    else:
        chart = data.get("single_chart") or {}
        xs = chart.get("x") or []
        series = chart.get("series") or []
        if not xs or not series:
            st.info("V JSON chýbajú dáta pre graf (spusti workflow po aktualizácii SimulatePiece).")
        else:
            max_pts = st.sidebar.slider("Max. bodov v grafe (výkon)", 500, 20000, 4000, 500)
            plot_df = pd.DataFrame({"čas": pd.to_datetime(xs)})
            for s in series:
                name = s.get("name", "séria")
                vals = s.get("values") or []
                if len(vals) == len(xs):
                    plot_df[name] = vals
            plot_df = _downsample_df(plot_df, max_pts)
            st.line_chart(plot_df.set_index("čas"), height=420)


if __name__ == "__main__":
    main()
