from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml
from domino.base_piece import BasePiece

from pieces.SimulatePiece.piece import load_consumption_csv, synthetic_pv_kw

from .models import InputModel, OutputModel


class SolarSimPiece(BasePiece):
    """Create virtual PV production CSV from selected scenario."""

    def piece_function(self, input_data: InputModel) -> OutputModel:
        csv_path = Path(input_data.load_csv)
        scenario_path = Path(input_data.scenario_yaml)
        if not csv_path.is_file():
            raise FileNotFoundError(f"Load CSV not found: {csv_path}")
        if not scenario_path.is_file():
            raise FileNotFoundError(f"Scenario YAML not found: {scenario_path}")

        cfg = yaml.safe_load(scenario_path.read_text(encoding="utf-8")) or {}
        pv = cfg.get("pv") or {}
        installed_kwp = float(pv.get("installed_kwp", 0.0))
        yield_kwp = float(pv.get("yield_kwh_per_kwp_year", 1000.0))

        df = load_consumption_csv(csv_path)
        pv_kw = synthetic_pv_kw(df["datetime"], installed_kwp, yield_kwh_per_kwp_year=yield_kwp)
        out_df = pd.DataFrame({"datetime": df["datetime"], "pv_kw": pv_kw})

        out_dir = Path(self.results_path or scenario_path.parent)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_csv = out_dir / "virtual_solar.csv"
        out_df.to_csv(out_csv, index=False)
        return OutputModel(message="Solar simulation finished", virtual_solar_csv=str(out_csv))
