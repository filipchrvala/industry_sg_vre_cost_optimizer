from __future__ import annotations

import copy
import json
from pathlib import Path

import yaml
from domino.base_piece import BasePiece

from pieces.SimulatePiece.piece import _auto_optimize_sizes, _apply_system_scope, load_consumption_csv

from .models import InputModel, OutputModel


class SizingOptimizationPiece(BasePiece):
    """Resolve final scenario sizing (manual or auto)."""

    def piece_function(self, input_data: InputModel) -> OutputModel:
        csv_path = Path(input_data.load_csv)
        scenario_path = Path(input_data.scenario_yaml)
        tl_path = Path(input_data.technical_limits_json)
        if not csv_path.is_file():
            raise FileNotFoundError(f"Load CSV not found: {csv_path}")
        if not scenario_path.is_file():
            raise FileNotFoundError(f"Scenario YAML not found: {scenario_path}")
        if not tl_path.is_file():
            raise FileNotFoundError(f"Technical limits JSON not found: {tl_path}")

        cfg = yaml.safe_load(scenario_path.read_text(encoding="utf-8")) or {}
        _apply_system_scope(cfg)
        df = load_consumption_csv(csv_path)

        eq = cfg.get("equipment") or {}
        mode = str(eq.get("selection_mode", "manual")).lower()
        auto_log = None
        final_cfg = copy.deepcopy(cfg)
        if mode == "auto":
            final_cfg, auto_log = _auto_optimize_sizes(final_cfg, df)

        out_dir = Path(self.results_path or scenario_path.parent)
        out_dir.mkdir(parents=True, exist_ok=True)
        sized_yaml = out_dir / "scenario_sized.yaml"
        sized_yaml.write_text(yaml.safe_dump(final_cfg, allow_unicode=True, sort_keys=False), encoding="utf-8")

        out_json = out_dir / "sizing_optimization.json"
        out_json.write_text(
            json.dumps({"selection_mode": mode, "auto_optimization": auto_log}, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        return OutputModel(
            message="Sizing optimization finished",
            sized_scenario_yaml=str(sized_yaml),
            sizing_optimization_json=str(out_json),
        )
