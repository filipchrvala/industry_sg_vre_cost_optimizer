"""Revert Test.customization piece names to pre-rename (Domino-stable)."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CUSTOM = ROOT / "Test.customization"

# new_name -> old_name (reverse of update_test_customization.py)
REVERSE = [
    ("DashboardDataPiece", "DashboardPiece"),
    ("InvestmentEvaluationPiece", "InvestmentEvalPiece"),
    ("ComputeKPIsPiece", "KPIPiece"),
    ("SimulateMRKScenarioPiece", "SimulatePiece"),
    ("BatterySimulationPiece", "BatterySimPiece"),
    ("SolarSimulationPiece", "SolarSimPiece"),
    ("BatteryStrategyPiece", "BatteryStrategyOptimizerPiece"),
    ("BatterySimulationPi_", "BatterySimPi_"),
    ("SimulateMRKScenarioPi_", "SimulatePi_"),
    ("ComputeKPIsPiece_", "KPIPiece_"),
    ("InvestmentEvaluation_", "InvestmentEval_"),
    ("BatteryStrategyPi_", "BatteryStrategyOptimizerPi_"),
    ("SolarSimulationPi_", "SolarSimPi_"),
]


def main() -> None:
    raw = CUSTOM.read_text(encoding="utf-8")
    for old, new in REVERSE:
        raw = raw.replace(old, new)
    data = json.loads(raw)
    # Fix node data names in workflowNodes
    for node in data.get("workflowNodes", []):
        name = node.get("data", {}).get("name", "")
        for new, old in REVERSE:
            if name == new:
                node["data"]["name"] = old
                style = node["data"].get("style", {})
                style["module"] = old
                style["label"] = old
    # workflowPieces keys are UUID-based; update name field inside entries
    for entry in data.get("workflowPieces", {}).values():
        n = entry.get("name", "")
        for new, old in REVERSE:
            if n == new:
                entry["name"] = old
                entry.setdefault("style", {})["module"] = old
                entry.setdefault("style", {})["label"] = old
                entry["source_url"] = entry["source_url"].replace(
                    f"/pieces/{new}", f"/pieces/{old}"
                )
    CUSTOM.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(f"Reverted names in {CUSTOM}")


if __name__ == "__main__":
    main()
