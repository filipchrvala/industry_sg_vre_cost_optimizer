from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
import traceback

import pandas as pd
from domino.base_piece import BasePiece

from .models import InputModel, OutputModel

try:
    from common import onedata_io as od
except ModuleNotFoundError:
    try:
        from pieces.common import onedata_io as od
    except ModuleNotFoundError:
        od = None


class DashboardPiece(BasePiece):
    """Build finance-focused dashboard payload for CFO decisions."""

    def piece_function(self, input_data: InputModel, secrets_data=None) -> OutputModel:
        _stage = None
        _piece_out = None
        _run_id = None
        if od is not None:
            input_data, _stage = od.stage_inputs(input_data, secrets_data)
            _run_id = od.resolve_run_id(input_data, secrets_data, generate=False)
        rep_path = Path((input_data.report_json or "").strip())
        kpi_path = Path((input_data.kpi_results_csv or "").strip())
        inv_path = Path((input_data.investment_evaluation_csv or "").strip())
        out_dir = Path(self.results_path or rep_path.parent)
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "dashboard_started.txt").write_text(
            f"report_json={rep_path}\nkpi_results_csv={kpi_path}\n"
            f"investment_evaluation_csv={inv_path}\nresults_path={self.results_path}\n",
            encoding="utf-8",
        )
        log_path = out_dir / "dashboard.log"

        def _log(msg: str) -> None:
            text = f"[DashboardPiece] {msg}"
            print(text, flush=True)
            with log_path.open("a", encoding="utf-8") as f:
                f.write(text + "\n")

        _log(f"Input report_json={rep_path}")
        _log(f"Input kpi_results_csv={kpi_path}")
        _log(f"Input investment_evaluation_csv={inv_path}")
        if not rep_path.is_file():
            raise FileNotFoundError(f"Report JSON not found: {rep_path}")
        if not kpi_path.is_file():
            raise FileNotFoundError(f"KPI CSV not found: {kpi_path}")
        if not inv_path.is_file():
            raise FileNotFoundError(f"Investment CSV not found: {inv_path}")

        try:
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
                dt_col = "datetime" if "datetime" in prof.columns else prof.columns[0]
                base_col = next(
                    (c for c in prof.columns if "baseline" in c.lower() and "kwh" in c.lower()),
                    "baseline_energy_kwh_interval",
                )
                opt_col = next(
                    (c for c in prof.columns if "optim" in c.lower() and "kwh" in c.lower()),
                    "optimized_energy_kwh_interval",
                )
                chart = {
                    "title": "Priebeh spotreby energie: baseline vs FVE+batéria",
                    "x": prof[dt_col].astype(str).tolist() if dt_col in prof.columns else [],
                    "series": [
                        {
                            "name": "Bez FVE a batérie",
                            "unit": "kWh/interval",
                            "values": pd.to_numeric(
                                prof[base_col] if base_col in prof.columns else 0,
                                errors="coerce",
                            )
                            .fillna(0.0)
                            .round(4)
                            .tolist(),
                        },
                        {
                            "name": "S FVE a batériou",
                            "unit": "kWh/interval",
                            "values": pd.to_numeric(
                                prof[opt_col] if opt_col in prof.columns else 0,
                                errors="coerce",
                            )
                            .fillna(0.0)
                            .round(4)
                            .tolist(),
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
                    "trading_only_annual_margin_eur_estimate": ((rep.get("trading_only_analysis") or {}).get("annual_margin_eur_estimate")),
                    "battery_annual_equivalent_cycles_est": ((rep.get("battery_lifetime_assessment") or {}).get("annual_equivalent_cycles_est")),
                    "battery_estimated_life_years_effective": ((rep.get("battery_lifetime_assessment") or {}).get("estimated_life_years_effective")),
                    "finance_annual_net_cashflow_after_finance_eur": ((rep.get("finance_layer") or {}).get("annual_net_cashflow_after_finance_eur")),
                    "finance_npv_after_finance_eur": ((rep.get("finance_layer") or {}).get("npv_after_finance_eur")),
                },
                "single_chart": chart,
                "battery_lifetime_assessment": (rep.get("battery_lifetime_assessment") or {}),
                "c_rate_sweep": (rep.get("c_rate_sweep") or []),
                "trading_only_analysis": (rep.get("trading_only_analysis") or {}),
                "finance_layer": (rep.get("finance_layer") or {}),
                "quality_flags": {
                    "report_schema_version": ((rep.get("meta") or {}).get("schema_version")),
                    "catalog_url_outage_detected": (((rep.get("equipment") or {}).get("catalog_sync_status") or {}).get("url_outage_detected")),
                    "historical_prices_in_csv": ((rep.get("input_quality") or {}).get("historical_prices_in_csv")),
                },
            }

            out_json = out_dir / "dashboard_data.json"
            out_json.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
            _log(f"Wrote dashboard JSON: {out_json}; kpi_rows={len(kpi_df)}")
            _piece_out = OutputModel(dashboard_data_json=str(out_json))
        except Exception as exc:
            (out_dir / "dashboard_error.txt").write_text(traceback.format_exc(), encoding="utf-8")
            _log(f"ERROR during dashboard assembly: {exc}")
            if od is not None:
                od.cleanup_on_error(self.results_path, secrets_data, "DashboardPiece", _stage, run_id=_run_id)
            raise
        if od is not None and _piece_out is not None:
            return od.finish_piece(
                _piece_out, self.results_path, secrets_data, "DashboardPiece", _stage, run_id=_run_id
            )
        if _stage is not None:
            _stage.cleanup()
        return _piece_out
