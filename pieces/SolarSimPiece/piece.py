from __future__ import annotations

import importlib.util
from pathlib import Path
import traceback

import pandas as pd
import yaml
from domino.base_piece import BasePiece

from .models import InputModel, OutputModel


def _load_simulate_module():
    sim_path = Path(__file__).resolve().parents[1] / "SimulatePiece" / "piece.py"
    spec = importlib.util.spec_from_file_location("simulate_piece_module", sim_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load SimulatePiece module from {sim_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SolarSimPiece(BasePiece):
    """Create virtual PV production CSV from selected scenario."""

    def piece_function(self, input_data: InputModel) -> OutputModel:
        csv_path = Path(input_data.load_csv)
        scenario_path = Path(input_data.scenario_yaml)
        out_dir = Path(self.results_path or scenario_path.parent)
        out_dir.mkdir(parents=True, exist_ok=True)
        log_path = out_dir / "solar_sim.log"

        def _log(msg: str) -> None:
            text = f"[SolarSimPiece] {msg}"
            print(text, flush=True)
            with log_path.open("a", encoding="utf-8") as f:
                f.write(text + "\n")

        _log(f"Input load_csv={csv_path}")
        _log(f"Input scenario_yaml={scenario_path}")
        if not csv_path.is_file():
            raise FileNotFoundError(f"Load CSV not found: {csv_path}")
        if not scenario_path.is_file():
            raise FileNotFoundError(f"Scenario YAML not found: {scenario_path}")

        try:
            sim = _load_simulate_module()
            cfg = yaml.safe_load(scenario_path.read_text(encoding="utf-8")) or {}
            pv = cfg.get("pv") or {}
            installed_kwp = float(pv.get("installed_kwp", 0.0))
            yield_kwp = float(pv.get("yield_kwh_per_kwp_year", 1000.0))
            df = sim.load_consumption_csv(csv_path)
            pv_kw = sim.synthetic_pv_kw(df["datetime"], installed_kwp, yield_kwh_per_kwp_year=yield_kwp)
            out_df = pd.DataFrame({"datetime": df["datetime"], "pv_kw": pv_kw})
            _log(f"Computed virtual solar rows={len(out_df)}, installed_kwp={installed_kwp}")
        except Exception as exc:
            (out_dir / "solar_sim_error.txt").write_text(traceback.format_exc(), encoding="utf-8")
            _log(f"ERROR during solar simulation: {exc}")
            raise

        out_csv = out_dir / "virtual_solar.csv"
        out_df.to_csv(out_csv, index=False)
        _log(f"Wrote output: {out_csv}")
        return OutputModel(message="Solar simulation finished", virtual_solar_csv=str(out_csv))
