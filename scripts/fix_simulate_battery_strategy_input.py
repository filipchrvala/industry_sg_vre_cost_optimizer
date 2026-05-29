"""Sync SimulatePiece input_schema + battery strategy wiring in Test.customization."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CUSTOM = ROOT / "Test.customization"
COMPILED = ROOT / ".domino" / "compiled_metadata.json"

SIM_ID = "109_36d206ac-15f9-42ea-9549-b712756722b0"
STRAT_ID = "107_0e90313e-cdac-44d2-b202-1d61eb55b8fa"
STRAT_UPSTREAM = "BatteryStrategyOptimizerPi_0e90313ecdac44d2b2021d61eb55b8fa"


def main() -> None:
    data = json.loads(CUSTOM.read_text(encoding="utf-8"))
    compiled = json.loads(COMPILED.read_text(encoding="utf-8"))
    sim_meta = compiled["SimulatePiece"]

    # 1) workflowPieces entry: fresh input_schema from compiled metadata
    entry = data["workflowPieces"][SIM_ID]
    entry["input_schema"] = sim_meta["input_schema"]
    entry["output_schema"] = sim_meta["output_schema"]
    entry["description"] = sim_meta.get("description", entry.get("description", ""))

    # 2) workflowPiecesData: wire battery strategy upstream
    wpd = data.setdefault("workflowPiecesData", {})
    sim_wpd = wpd.setdefault(SIM_ID, {})
    inputs = sim_wpd.setdefault("inputs", {})
    inputs["battery_strategy_recommendation_json"] = {
        "fromUpstream": True,
        "upstreamId": STRAT_UPSTREAM,
        "upstreamArgument": "battery_strategy_recommendation_json",
        "upstreamValue": (
            "BatteryStrategyOptimizerPiece (0e90313e) - Battery Strategy Recommendation Json"
        ),
        "value": "",
    }

    # 3) edge BatteryStrategyOptimizer -> Simulate (graph line)
    edges = data.setdefault("workflowEdges", [])
    edge_id = (
        f"reactflow__edge-{STRAT_ID}source-{STRAT_ID}-"
        f"{SIM_ID}target-{SIM_ID}"
    )
    if not any(e.get("source") == STRAT_ID and e.get("target") == SIM_ID for e in edges):
        edges.append(
            {
                "source": STRAT_ID,
                "sourceHandle": f"source-{STRAT_ID}",
                "target": SIM_ID,
                "targetHandle": f"target-{SIM_ID}",
                "id": edge_id,
                "markerEnd": {"type": "arrowclosed", "width": 20, "height": 20},
            }
        )

    CUSTOM.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    props = list(entry["input_schema"]["properties"].keys())
    print(f"Updated {CUSTOM}")
    print(f"SimulatePiece input_schema: {props}")
    print("battery_strategy wired:", "battery_strategy_recommendation_json" in inputs)


if __name__ == "__main__":
    main()
