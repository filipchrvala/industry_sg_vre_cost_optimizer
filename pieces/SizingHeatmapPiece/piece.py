"""Sweep PV and battery sizes and report the economics of every combination.

SizingOptimizationPiece returns a single recommended pair. That is the answer to
"what should we build", but it hides how sharp the optimum is. A board asked to
approve capital wants to see whether the recommendation sits on a plateau, where
adding storage stops paying, and how much of the benefit a cheaper half-sized
system would still capture.

Every cell runs the same full economic simulation the recommendation came from,
against the same AI production forecast rescaled to that cell's array size, so
the grid and the headline result cannot disagree.
"""

from __future__ import annotations

import json
import traceback
from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from domino.base_piece import BasePiece

try:
    from pieces.simulate_import import load_simulate_module
except ModuleNotFoundError:
    from simulate_import import load_simulate_module

from .models import InputModel, OutputModel

try:
    from common import onedata_io as od
except ModuleNotFoundError:
    try:
        from pieces.common import onedata_io as od
    except ModuleNotFoundError:
        od = None


class SizingHeatmapPiece(BasePiece):
    """Build the PV x battery economics grid behind the sizing recommendation."""

    def piece_function(self, input_data: InputModel, secrets_data=None) -> OutputModel:
        _stage = None
        _run_id = None
        if od is not None:
            input_data, _stage = od.stage_inputs(input_data, secrets_data)
            _run_id = od.resolve_run_id(input_data, secrets_data, generate=False)

        csv_path = Path(input_data.load_csv)
        scenario_path = Path(input_data.scenario_yaml)
        out_dir = Path(self.results_path or scenario_path.parent)
        out_dir.mkdir(parents=True, exist_ok=True)
        log_path = out_dir / "sizing_heatmap.log"

        def _log(msg: str) -> None:
            text = f"[SizingHeatmapPiece] {msg}"
            print(text, flush=True)
            try:
                with log_path.open("a", encoding="utf-8") as fh:
                    fh.write(text + "\n")
            except Exception:
                pass

        try:
            if not csv_path.is_file():
                raise FileNotFoundError(f"Load CSV not found: {csv_path}")
            if not scenario_path.is_file():
                raise FileNotFoundError(f"Scenario YAML not found: {scenario_path}")

            sim = load_simulate_module()
            cfg = yaml.safe_load(scenario_path.read_text(encoding="utf-8")) or {}
            sim._apply_system_scope(cfg)
            df = sim.load_consumption_csv(csv_path)
            dt_h = sim.infer_timestep_hours(df)

            reference_kwp = float((cfg.get("pv") or {}).get("installed_kwp", 0.0) or 0.0)
            pv_profile = sim.load_pv_profile_per_kwp(
                input_data.virtual_solar_csv, df, reference_kwp=reference_kwp
            )
            profile_source = "ai_forecast" if pv_profile is not None else "synthetic_fallback"
            _log(f"PV profile source: {profile_source}")

            bounds = self._bounds(sim, cfg, df, dt_h, input_data.technical_limits_json)
            max_kwp = float(bounds.get("max_kwp") or 0.0)
            max_kwh = float(bounds.get("max_kwh") or 0.0)
            _log(f"Sweep bounds: max_kwp={max_kwp:.1f}, max_kwh={max_kwh:.1f}")

            kwp_axis = self._axis(max_kwp, int(input_data.pv_steps))
            kwh_axis = self._axis(max_kwh, int(input_data.battery_steps))

            analysis = cfg.get("analysis") or {}
            years = int(analysis.get("amortization_years", 12))
            discount_rate = float(analysis.get("discount_rate", 0.08))
            objective = str(
                ((cfg.get("equipment") or {}).get("auto") or {}).get("objective", "max_npv")
            ).lower()

            rows: list[dict] = []
            grids = {
                key: [[None] * len(kwp_axis) for _ in kwh_axis]
                for key in (
                    "annual_savings_eur",
                    "npv_eur",
                    "simple_payback_years",
                    "total_capex_eur",
                    "self_consumption_pct",
                )
            }

            best = None
            for j, kwh in enumerate(kwh_axis):
                for i, kwp in enumerate(kwp_axis):
                    cell = self._evaluate(
                        sim, cfg, df, kwp, kwh, pv_profile, years, discount_rate, objective, dt_h
                    )
                    rows.append(cell)
                    for key in grids:
                        grids[key][j][i] = cell.get(key)
                    if cell["feasible"] and (best is None or cell["score"] < best["score"]):
                        best = cell

            if best is None:
                # Nothing cleared the objective, so report the largest saving
                # rather than an empty recommendation.
                best = max(rows, key=lambda r: r.get("annual_savings_eur") or -1e18)

            _log(
                f"Evaluated {len(rows)} combinations; best {best['kwp']:.0f} kWp / "
                f"{best['kwh']:.0f} kWh"
            )

            payload = {
                "format": "uc32_sizing_heatmap_v1",
                "objective": objective,
                "pv_profile_source": profile_source,
                "amortization_years": years,
                "discount_rate": discount_rate,
                "axes": {
                    "pv_kwp": kwp_axis,
                    "battery_kwh": kwh_axis,
                },
                "bounds": bounds,
                "grids": grids,
                "recommended": {
                    "pv_kwp": best["kwp"],
                    "battery_kwh": best["kwh"],
                    "annual_savings_eur": best.get("annual_savings_eur"),
                    "npv_eur": best.get("npv_eur"),
                    "simple_payback_years": best.get("simple_payback_years"),
                    "total_capex_eur": best.get("total_capex_eur"),
                },
                "current_scenario": {
                    "pv_kwp": reference_kwp,
                    "battery_kwh": float((cfg.get("battery") or {}).get("energy_kwh", 0.0) or 0.0),
                },
            }

            heatmap_json = out_dir / "sizing_heatmap.json"
            heatmap_csv = out_dir / "sizing_heatmap.csv"
            heatmap_json.write_text(
                json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
            )
            pd.DataFrame(rows).to_csv(heatmap_csv, index=False)
            self.display_result = {"file_type": "json", "file_path": str(heatmap_json)}

            piece_out = OutputModel(
                message=(
                    f"Swept {len(kwp_axis)}x{len(kwh_axis)} sizes on the {profile_source}; "
                    f"best {best['kwp']:.0f} kWp / {best['kwh']:.0f} kWh"
                ),
                heatmap_json=str(heatmap_json),
                heatmap_csv=str(heatmap_csv),
                recommended_kwp=float(best["kwp"]),
                recommended_kwh=float(best["kwh"]),
            )
        except Exception as exc:
            (out_dir / "sizing_heatmap_error.txt").write_text(
                traceback.format_exc(), encoding="utf-8"
            )
            _log(f"ERROR: {exc}")
            if od is not None:
                od.cleanup_on_error(
                    self.results_path, secrets_data, "SizingHeatmapPiece", _stage, run_id=_run_id
                )
            raise

        if od is not None:
            return od.finish_piece(
                piece_out, self.results_path, secrets_data, "SizingHeatmapPiece", _stage,
                run_id=_run_id,
            )
        if _stage is not None:
            _stage.cleanup()
        return piece_out

    @staticmethod
    def _bounds(sim, cfg: dict, df: pd.DataFrame, dt_h: float, limits_path: str | None) -> dict:
        bounds = dict(sim.technical_bounds_kwp_kwh(cfg, df, dt_h))
        if limits_path and Path(str(limits_path)).is_file():
            upstream = json.loads(Path(str(limits_path)).read_text(encoding="utf-8")) or {}
            for key in ("max_kwp", "max_kwh"):
                if upstream.get(key) is not None:
                    bounds[key] = float(upstream[key])
            bounds["source"] = "TechnicalLimitsPiece"
        else:
            bounds["source"] = "derived_from_load"
        return bounds

    @staticmethod
    def _axis(maximum: float, steps: int) -> list[float]:
        """Axis from zero to the technical limit, rounded to readable sizes.

        Zero is included on both axes so the grid also answers "PV only" and
        "battery only", which are the comparisons a reader reaches for first.
        """
        if maximum <= 0:
            return [0.0]
        raw = np.linspace(0.0, maximum, steps)
        magnitude = 10 ** max(0, int(np.floor(np.log10(max(maximum / steps, 1.0)))))
        rounded = sorted({float(round(v / magnitude) * magnitude) for v in raw})
        return [v for v in rounded if v >= 0]

    @staticmethod
    def _evaluate(
        sim,
        cfg: dict,
        df: pd.DataFrame,
        kwp: float,
        kwh: float,
        pv_profile,
        years: int,
        discount_rate: float,
        objective: str,
        dt_h: float,
    ) -> dict:
        import copy

        trial = copy.deepcopy(cfg)
        trial.setdefault("pv", {})["installed_kwp"] = float(kwp)
        trial.setdefault("battery", {})["energy_kwh"] = float(kwh)
        trial["use_pv"] = kwp > 0
        trial["use_battery"] = kwh > 0

        cell = {
            "kwp": float(kwp),
            "kwh": float(kwh),
            "feasible": False,
            "score": float("inf"),
            "annual_savings_eur": None,
            "npv_eur": None,
            "simple_payback_years": None,
            "total_capex_eur": None,
            "self_consumption_pct": None,
        }
        if kwp <= 0 and kwh <= 0:
            cell.update({"annual_savings_eur": 0.0, "npv_eur": 0.0, "total_capex_eur": 0.0})
            return cell

        try:
            bundle = sim._sim_bundle(trial, df, pv_profile_per_kwp=pv_profile)
            score, fin = sim._score_financials(
                bundle, dr=discount_rate, years=years, objective=objective
            )
        except Exception:
            return cell

        cell.update(
            {
                "score": float(score),
                "feasible": bool(np.isfinite(score)),
                "annual_savings_eur": fin.get("annual_operating_savings_eur"),
                "npv_eur": fin.get("npv_eur"),
                "simple_payback_years": fin.get("simple_payback_years"),
                "total_capex_eur": fin.get("total_capex_eur"),
            }
        )

        if pv_profile is not None and kwp > 0:
            produced = np.asarray(pv_profile, dtype=float) * kwp
            load = df["load_kw"].astype(float).to_numpy()
            generated = float(produced.sum() * dt_h)
            if generated > 0:
                self_used = float(np.minimum(produced, load).sum() * dt_h)
                cell["self_consumption_pct"] = round(self_used / generated * 100.0, 1)

        return cell
