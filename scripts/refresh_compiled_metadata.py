"""Refresh .domino/compiled_metadata.json input/output schemas from piece models."""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "pieces"))

COMPILED = ROOT / ".domino" / "compiled_metadata.json"
PIECES_DIR = ROOT / "pieces"

PIECE_NAMES = [
    "UserInputPiece",
    "CatalogSyncPiece",
    "TechnicalLimitsPiece",
    "SizingOptimizationPiece",
    "CatalogRankerPiece",
    "SolarSimPiece",
    "BatteryStrategyOptimizerPiece",
    "BatterySimPiece",
    "SimulatePiece",
    "KPIPiece",
    "InvestmentEvalPiece",
    "DashboardPiece",
]


def main() -> None:
    compiled = json.loads(COMPILED.read_text(encoding="utf-8")) if COMPILED.is_file() else {}
    for name in PIECE_NAMES:
        mod = importlib.import_module(f"{name}.models")
        inp = mod.InputModel.model_json_schema()
        out = mod.OutputModel.model_json_schema()
        entry = compiled.setdefault(name, {})
        entry["name"] = name
        entry["input_schema"] = inp
        entry["output_schema"] = out
        meta_path = PIECES_DIR / name / "metadata.json"
        if meta_path.is_file():
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            for key in ("description", "dependency", "tags", "style", "container_resources"):
                if key in meta:
                    entry[key] = meta[key]
        print("updated", name)

    COMPILED.parent.mkdir(parents=True, exist_ok=True)
    COMPILED.write_text(json.dumps(compiled, indent=4), encoding="utf-8")
    print("wrote", COMPILED)


if __name__ == "__main__":
    main()
