from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from domino.base_piece import BasePiece

from .models import InputModel, OutputModel


class DashboardPiece(BasePiece):
    """Build finance-focused dashboard payload for CFO decisions."""

    def piece_function(self, input_data: InputModel) -> OutputModel:
        rep_path = Path(input_data.report_json)
        kpi_path = Path(input_data.kpi_results_csv)
        inv_path = Path(input_data.investment_evaluation_csv)
        if not rep_path.is_file():
            raise FileNotFoundError(f"Report JSON not found: {rep_path}")
        if not kpi_path.is_file():
            raise FileNotFoundError(f"KPI CSV not found: {kpi_path}")
        if not inv_path.is_file():
            raise FileNotFoundError(f"Investment CSV not found: {inv_path}")

        rep = json.loads(rep_path.read_text(encoding="utf-8"))
        kpi_df = pd.read_csv(kpi_path)
        inv_df = pd.read_csv(inv_path)

        exec_ = rep.get("executive_summary") or {}
        mrk = rep.get("mrk_and_rv") or {}
        unc = rep.get("uncertainty_assessment") or {}
        inv = (inv_df.to_dict(orient="records") or [{}])[0]
        art = rep.get("artifacts") or {}
        profile_path = Path(art.get("baseline_vs_optimized_profile_csv") or "")
        chart = {"title": "Priebeh spotreby energie: baseline vs FVE+batéria", "x": [], "series": []}
        if profile_path.is_file():
            prof = pd.read_csv(profile_path)
            chart = {
                "title": "Priebeh spotreby energie: baseline vs FVE+batéria",
                "x": prof["datetime"].astype(str).tolist(),
                "series": [
                    {
                        "name": "Bez FVE a batérie",
                        "unit": "kWh/interval",
                        "values": pd.to_numeric(
                            prof["baseline_energy_kwh_interval"], errors="coerce"
                        ).fillna(0.0).round(4).tolist(),
                    },
                    {
                        "name": "S FVE a batériou",
                        "unit": "kWh/interval",
                        "values": pd.to_numeric(
                            prof["optimized_energy_kwh_interval"], errors="coerce"
                        ).fillna(0.0).round(4).tolist(),
                    },
                ],
            }

        payload = {
            "format": "cfo_finance_dashboard_v1",
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "decision_kpis": {
                "operating_cost_baseline_eur": exec_.get("operating_cost_baseline_eur"),
                "operating_cost_with_pv_battery_eur": exec_.get("operating_cost_pv_battery_eur"),
                "operating_savings_period_eur": exec_.get("operating_savings_eur_period"),
                "operating_savings_annual_estimate_eur": exec_.get("operating_savings_eur_per_year_estimate"),
                "total_capex_eur": inv.get("total_capex_eur"),
                "simple_payback_years": inv.get("simple_payback_years"),
                "discounted_payback_years": inv.get("discounted_payback_years"),
                "npv_operating_eur": inv.get("npv_operating_eur"),
                "p50_annual_savings_eur": unc.get("p50_annual_savings_eur"),
                "p90_annual_savings_eur": unc.get("p90_annual_savings_eur"),
                "rv_downsizing_potential_kw": mrk.get("rv_downsizing_potential_kw"),
                "rv_fixed_fee_savings_period_eur": mrk.get("estimated_fixed_rv_fee_savings_if_resized_eur_for_period"),
            },
            "single_chart": chart,
            "quality_flags": {
                "report_schema_version": ((rep.get("meta") or {}).get("schema_version")),
                "catalog_url_outage_detected": (((rep.get("equipment") or {}).get("catalog_sync_status") or {}).get("url_outage_detected")),
                "historical_prices_in_csv": ((rep.get("input_quality") or {}).get("historical_prices_in_csv")),
            },
        }

        out_dir = Path(self.results_path or rep_path.parent)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_json = out_dir / "dashboard_data.json"
        out_json.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        return OutputModel(dashboard_data_json=str(out_json))
