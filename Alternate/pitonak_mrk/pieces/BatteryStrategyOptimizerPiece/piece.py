from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import yaml
from domino.base_piece import BasePiece

from pieces.SimulatePiece.piece import build_price_series, load_consumption_csv

from .models import InputModel, OutputModel


class BatteryStrategyOptimizerPiece(BasePiece):
    """Build simple price-driven strategy thresholds for battery operation."""

    def piece_function(self, input_data: InputModel) -> OutputModel:
        csv_path = Path(input_data.load_csv)
        scenario_path = Path(input_data.scenario_yaml)
        if not csv_path.is_file():
            raise FileNotFoundError(f"Load CSV not found: {csv_path}")
        if not scenario_path.is_file():
            raise FileNotFoundError(f"Scenario YAML not found: {scenario_path}")

        cfg = yaml.safe_load(scenario_path.read_text(encoding="utf-8")) or {}
        df = load_consumption_csv(csv_path)
        price = build_price_series(df, cfg).values.astype(float)
        rec = {
            "charge_below_eur_per_kwh": round(float(np.quantile(price, 0.30)), 6),
            "discharge_above_eur_per_kwh": round(float(np.quantile(price, 0.75)), 6),
            "expensive_hour_threshold_eur_per_kwh": round(float(np.percentile(price, 70.0)), 6),
            "strategy_note": "Thresholds aligned to dispatch logic in SimulatePiece.",
        }

        out_dir = Path(self.results_path or scenario_path.parent)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_json = out_dir / "battery_strategy_recommendation.json"
        out_json.write_text(json.dumps(rec, indent=2, ensure_ascii=False), encoding="utf-8")
        return OutputModel(message="Battery strategy optimized", battery_strategy_recommendation_json=str(out_json))
