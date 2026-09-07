from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
import traceback

import pandas as pd
from domino.base_piece import BasePiece

from .models import InputModel, OutputModel
from .render import build_html

try:
    from common import onedata_io as od
except ModuleNotFoundError:
    try:
        from pieces.common import onedata_io as od
    except ModuleNotFoundError:
        od = None


class DashboardPiece(BasePiece):
    """Build finance-focused dashboard payload for CFO decisions."""

    @staticmethod
    def _read_json(path: str | None) -> dict | None:
        """Optional side inputs must never sink the dashboard.

        The heatmap, calibration report and equipment ranking each enrich the
        output but none is essential, and a run without SHMÚ coverage or with a
        catalog outage should still produce a report.
        """
        if not path:
            return None
        p = Path(str(path).strip())
        if not p.is_file() or p.stat().st_size == 0:
            return None
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return None
        return data if isinstance(data, dict) else None

    @staticmethod
    def _build_consumption(profile_path: Path, dispatch_path: Path) -> dict:
        """Daily and monthly grid energy with and without the proposed plant.

        The comparison the board asks for is energy bought from the grid, not
        site load. Site load barely changes; what changes is how much of it the
        meter still sees. Interval kWh is summed to the day and the month so
        the chart shows consumption, not a 15-minute mean that looks like noise.
        """
        empty = {
            "daily": {"x": [], "without": [], "with": []},
            "monthly": {"x": [], "without": [], "with": []},
            "totals": {},
            "legacy_chart": {"title": "", "x": [], "series": []},
        }
        frame = DashboardPiece._read_profile(profile_path)
        if frame is None:
            frame = DashboardPiece._read_dispatch(dispatch_path)
        if frame is None or frame.empty:
            return empty

        frame = frame.copy()
        frame["datetime"] = pd.to_datetime(frame["datetime"], errors="coerce")
        frame = frame.dropna(subset=["datetime"]).sort_values("datetime")
        frame["without_kwh"] = pd.to_numeric(frame["without_kwh"], errors="coerce").fillna(0.0).clip(lower=0.0)
        frame["with_kwh"] = pd.to_numeric(frame["with_kwh"], errors="coerce").fillna(0.0).clip(lower=0.0)

        daily = (
            frame.assign(day=frame["datetime"].dt.floor("D"))
            .groupby("day", as_index=False)[["without_kwh", "with_kwh"]]
            .sum()
        )
        monthly = (
            frame.assign(month=frame["datetime"].dt.to_period("M").dt.to_timestamp())
            .groupby("month", as_index=False)[["without_kwh", "with_kwh"]]
            .sum()
        )

        without_total = float(frame["without_kwh"].sum())
        with_total = float(frame["with_kwh"].sum())
        totals = {
            "without_kwh": round(without_total, 1),
            "with_kwh": round(with_total, 1),
            "saved_kwh": round(without_total - with_total, 1),
            "saved_pct": round((1.0 - with_total / without_total) * 100.0, 1) if without_total > 0 else None,
            "days": int(len(daily)),
        }

        legacy = {
            "title": "Spotreba zo siete: bez FVE a batérie vs s FVE a batériou",
            "resolution": "daily_sum_kwh",
            "x": daily["day"].dt.strftime("%Y-%m-%d").tolist(),
            "series": [
                {
                    "name": "Bez FVE a batérie",
                    "unit": "kWh/deň",
                    "values": daily["without_kwh"].round(1).tolist(),
                },
                {
                    "name": "S FVE a batériou",
                    "unit": "kWh/deň",
                    "values": daily["with_kwh"].round(1).tolist(),
                },
            ],
        }
        return {
            "daily": {
                "x": daily["day"].dt.strftime("%Y-%m-%d").tolist(),
                "without": daily["without_kwh"].round(1).tolist(),
                "with": daily["with_kwh"].round(1).tolist(),
                "unit": "kWh/deň",
            },
            "monthly": {
                "x": monthly["month"].dt.strftime("%Y-%m").tolist(),
                "without": monthly["without_kwh"].round(1).tolist(),
                "with": monthly["with_kwh"].round(1).tolist(),
                "unit": "kWh/mesiac",
            },
            "totals": totals,
            "legacy_chart": legacy,
        }

    @staticmethod
    def _read_profile(path: Path) -> pd.DataFrame | None:
        if not path.is_file():
            return None
        raw = pd.read_csv(path)
        if "datetime" not in raw.columns:
            return None
        without = next(
            (c for c in ("baseline_energy_kwh_interval", "baseline_kwh") if c in raw.columns),
            None,
        )
        with_ = next(
            (c for c in ("optimized_energy_kwh_interval", "optimized_kwh") if c in raw.columns),
            None,
        )
        if without is None or with_ is None:
            return None
        return raw[["datetime", without, with_]].rename(
            columns={without: "without_kwh", with_: "with_kwh"}
        )

    @staticmethod
    def _read_dispatch(path: Path) -> pd.DataFrame | None:
        """Fall back to BatterySim dispatch when SimulatePiece left no profile."""
        if not path.is_file():
            return None
        raw = pd.read_csv(path)
        if "datetime" not in raw.columns or "load_kw" not in raw.columns:
            return None
        if "pv_battery_grid_kw" not in raw.columns:
            return None
        dt_h = 0.25
        if len(raw) >= 2:
            stamps = pd.to_datetime(raw["datetime"], errors="coerce")
            step = stamps.diff().dt.total_seconds().median()
            if pd.notna(step) and step > 0:
                dt_h = float(step) / 3600.0
        load = pd.to_numeric(raw["load_kw"], errors="coerce").fillna(0.0).clip(lower=0.0)
        grid = pd.to_numeric(raw["pv_battery_grid_kw"], errors="coerce").fillna(0.0).clip(lower=0.0)
        return pd.DataFrame(
            {
                "datetime": raw["datetime"],
                "without_kwh": load * dt_h,
                "with_kwh": grid * dt_h,
            }
        )

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
            consumption = self._build_consumption(
                profile_path=profile_path,
                dispatch_path=Path((input_data.battery_dispatch_csv or "").strip() or ""),
            )
            # Kept for older consumers of dashboard_data.json.
            chart = consumption.get("legacy_chart") or {
                "title": "Spotreba zo siete: bez FVE a batérie vs s FVE a batériou",
                "x": [],
                "series": [],
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
                    "p50_npv_eur": unc.get("p50_npv_eur"),
                    "p90_npv_eur": unc.get("p90_npv_eur"),
                    "irr_pct": unc.get("irr_pct"),
                    "p90_irr_pct": unc.get("p90_irr_pct"),
                    "probability_npv_positive": unc.get("probability_npv_positive"),
                    "uncertainty_method": unc.get("method"),
                    "rv_downsizing_potential_kw": mrk.get("rv_downsizing_potential_kw"),
                    "rv_fixed_fee_savings_period_eur": mrk.get("estimated_fixed_rv_fee_savings_if_resized_eur_for_period"),
                    "trading_only_annual_margin_eur_estimate": ((rep.get("trading_only_analysis") or {}).get("annual_margin_eur_estimate")),
                    "battery_annual_equivalent_cycles_est": ((rep.get("battery_lifetime_assessment") or {}).get("annual_equivalent_cycles_est")),
                    "battery_estimated_life_years_effective": ((rep.get("battery_lifetime_assessment") or {}).get("estimated_life_years_effective")),
                    "finance_annual_net_cashflow_after_finance_eur": ((rep.get("finance_layer") or {}).get("annual_net_cashflow_after_finance_eur")),
                    "finance_npv_after_finance_eur": ((rep.get("finance_layer") or {}).get("npv_after_finance_eur")),
                },
                "single_chart": chart,
                "consumption": consumption,
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

            heatmap = self._read_json(input_data.heatmap_json)
            calibration = self._read_json(input_data.calibration_json)
            ranking = self._read_json(input_data.catalog_ranked_recommendation_json)
            if heatmap:
                payload["sizing_heatmap"] = heatmap
            if calibration:
                payload["forecast_calibration"] = calibration
            if ranking:
                payload["equipment_ranking"] = ranking

            out_json = out_dir / "dashboard_data.json"
            out_json.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
            _log(f"Wrote dashboard JSON: {out_json}; kpi_rows={len(kpi_df)}")

            site_name = str(((rep.get("meta") or {}).get("site_name") or "")).strip()
            out_html = out_dir / "dashboard.html"
            out_html.write_text(
                build_html(
                    payload,
                    heatmap=heatmap,
                    calibration=calibration,
                    ranking=ranking,
                    site_name=site_name,
                ),
                encoding="utf-8",
            )
            _log(f"Wrote dashboard HTML: {out_html}")
            self.display_result = {"file_type": "html", "file_path": str(out_html)}

            _piece_out = OutputModel(
                dashboard_data_json=str(out_json), dashboard_html=str(out_html)
            )
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
