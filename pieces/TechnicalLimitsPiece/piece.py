from __future__ import annotations

import json
from pathlib import Path

from domino.base_piece import BasePiece

from pieces.SimulatePiece.piece import infer_timestep_hours, load_consumption_csv, technical_bounds_kwp_kwh

from .models import InputModel, OutputModel


class TechnicalLimitsPiece(BasePiece):
    """Calculate technical bounds from scenario constraints."""

    def piece_function(self, input_data: InputModel) -> OutputModel:
        csv_path = Path(input_data.load_csv)
        scenario_path = Path(input_data.scenario_yaml)
        if not csv_path.is_file():
            raise FileNotFoundError(f"Load CSV not found: {csv_path}")
        if not scenario_path.is_file():
            raise FileNotFoundError(f"Scenario YAML not found: {scenario_path}")

        import yaml

        cfg = yaml.safe_load(scenario_path.read_text(encoding="utf-8")) or {}
        df = load_consumption_csv(csv_path)
        dt_h = infer_timestep_hours(df)
        bounds = technical_bounds_kwp_kwh(cfg, df, dt_h)

        out_dir = Path(self.results_path or scenario_path.parent)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_json = out_dir / "technical_limits.json"
        out_json.write_text(json.dumps(bounds, indent=2, ensure_ascii=False), encoding="utf-8")
        return OutputModel(
            message="Technical limits calculated",
            technical_limits_json=str(out_json),
            scenario_yaml=str(scenario_path),
        )
