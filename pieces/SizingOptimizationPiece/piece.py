from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import traceback

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


class SizingOptimizationPiece(BasePiece):
    """Resolve final scenario sizing (manual or auto)."""

    def piece_function(self, input_data: InputModel) -> OutputModel:
        csv_path = Path(input_data.load_csv)
        scenario_path = Path(input_data.scenario_yaml)
        tl_path = Path(input_data.technical_limits_json)
        out_dir = Path(self.results_path or scenario_path.parent)
        out_dir.mkdir(parents=True, exist_ok=True)
        log_path = out_dir / "sizing_optimization.log"

        def _log(msg: str) -> None:
            text = f"[SizingOptimizationPiece] {msg}"
            print(text, flush=True)
            with log_path.open("a", encoding="utf-8") as f:
                f.write(text + "\n")

        _log(f"Input load_csv={csv_path}")
        _log(f"Input scenario_yaml={scenario_path}")
        _log(f"Input technical_limits_json={tl_path}")
        if not csv_path.is_file():
            raise FileNotFoundError(f"Load CSV not found: {csv_path}")
        if not scenario_path.is_file():
            raise FileNotFoundError(f"Scenario YAML not found: {scenario_path}")
        if not tl_path.is_file():
            raise FileNotFoundError(f"Technical limits JSON not found: {tl_path}")

        try:
            sim = _load_simulate_module()
            cfg = yaml.safe_load(scenario_path.read_text(encoding="utf-8")) or {}
            sim._apply_system_scope(cfg)
            df = sim.load_consumption_csv(csv_path)

            eq = cfg.get("equipment") or {}
            mode = str(eq.get("selection_mode", "manual")).lower()
            auto_log = None
            final_cfg = copy.deepcopy(cfg)
            if mode == "auto":
                final_cfg, auto_log = sim._auto_optimize_sizes(final_cfg, df)
            _log(f"Resolved selection_mode={mode}, rows={len(df)}")
        except Exception as exc:
            (out_dir / "sizing_optimization_error.txt").write_text(traceback.format_exc(), encoding="utf-8")
            _log(f"ERROR during sizing optimization: {exc}")
            raise

        sized_yaml = out_dir / "scenario_sized.yaml"
        sized_yaml.write_text(yaml.safe_dump(final_cfg, allow_unicode=True, sort_keys=False), encoding="utf-8")

        out_json = out_dir / "sizing_optimization.json"
        out_json.write_text(
            json.dumps({"selection_mode": mode, "auto_optimization": auto_log}, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        _log(f"Wrote outputs: {sized_yaml}, {out_json}")
        return OutputModel(
            message="Sizing optimization finished",
            sized_scenario_yaml=str(sized_yaml),
            sizing_optimization_json=str(out_json),
        )
