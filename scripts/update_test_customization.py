"""Update Test.customization to match renamed Domino pieces."""
from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CUSTOM = ROOT / "Test.customization"
PIECES = ROOT / "pieces"

# Order matters (longer names first).
TEXT_REPLACEMENTS = [
    ("BatteryStrategyOptimizerPiece", "BatteryStrategyPiece"),
    ("InvestmentEvalPiece", "InvestmentEvaluationPiece"),
    ("SolarSimPiece", "SolarSimulationPiece"),
    ("BatterySimPiece", "BatterySimulationPiece"),
    ("SimulatePiece", "SimulateMRKScenarioPiece"),
    ("KPIPiece", "ComputeKPIsPiece"),
    ("DashboardPiece", "DashboardDataPiece"),
    # Human labels -> unified names
    ("User Input", "UserInputPiece"),
    ("Catalog Sync", "CatalogSyncPiece"),
    ("Technical Limits", "TechnicalLimitsPiece"),
    ("Sizing Optimization", "SizingOptimizationPiece"),
    ("Catalog Ranker", "CatalogRankerPiece"),
    ("Solar Simulation", "SolarSimulationPiece"),
    ("Battery Strategy", "BatteryStrategyPiece"),
    ("Battery Simulation", "BatterySimulationPiece"),
    ("Simulate MRK Scenario", "SimulateMRKScenarioPiece"),
    ("Compute KPIs", "ComputeKPIsPiece"),
    ("Investment Evaluation", "InvestmentEvaluationPiece"),
    ("Dashboard Data", "DashboardDataPiece"),
]

# Domino upstreamId prefixes (truncated UI ids).
UPSTREAM_ID_REPLACEMENTS = [
    ("BatteryStrategyOptimizer", "BatteryStrategy"),
    ("SolarSimPi_", "SolarSimulationPi_"),
    ("SimulatePi_", "SimulateMRKScenarioPi_"),
    ("KPIPiece_", "ComputeKPIsPiece_"),
    ("Investment_", "InvestmentEvaluation_"),
    ("BatterySimPi_", "BatterySimulationPi_"),
]


def _load_piece_metadata() -> dict[str, dict]:
    out: dict[str, dict] = {}
    for meta_path in sorted(PIECES.glob("*/metadata.json")):
        data = json.loads(meta_path.read_text(encoding="utf-8"))
        name = data["name"]
        out[name] = data
    return out


def _piece_entry_from_repo(name: str, meta: dict, source_image: str) -> dict:
    """Build workflowPieces entry skeleton from repo metadata + domino compile hints."""
    dep = meta.get("dependency") or {"requirements_file": "requirements_0.txt"}
    style = meta.get("style") or {}
    label = name
    module = name
    icon = style.get("icon_class_name", "fa-solid:cube")
    return {
        "id": 100,
        "name": name,
        "description": meta.get("description", ""),
        "dependency": dep,
        "source_image": source_image,
        "input_schema": {},  # filled below from compiled if needed
        "output_schema": {},
        "secrets_schema": None,
        "container_resources": {
            "requests": {"cpu": 100, "memory": 128},
            "limits": {"cpu": 100, "memory": 128},
            "use_gpu": False,
        },
        "tags": [],
        "style": {
            "module": module,
            "label": label,
            "nodeType": "default",
            "nodeStyle": {"backgroundColor": "#ebebeb"},
            "useIcon": True,
            "iconClassName": icon,
            "iconStyle": {"cursor": "pointer"},
        },
        "source_url": f"https://github.com/filipchrvala/industry_sg_vre_cost_optimizer/tree/main/pieces/{name}",
        "repository_url": "https://github.com/filipchrvala/industry_sg_vre_cost_optimizer",
        "repository_id": 14,
    }


def main() -> None:
    raw = CUSTOM.read_text(encoding="utf-8")
    for old, new in TEXT_REPLACEMENTS:
        raw = raw.replace(old, new)
    for old, new in UPSTREAM_ID_REPLACEMENTS:
        raw = raw.replace(old, new)

    data = json.loads(raw)

    # Node id suffix -> piece name (stable UUIDs from export)
    suffix_to_piece = {
        "6c7d1d1b-ebc0-41cf-94af-98c9378610e0": "UserInputPiece",
        "ee96043b-6cb0-42f1-934e-ed0b8a7f8f78": "CatalogSyncPiece",
        "02ae53ae-10a2-48c5-8894-70f2596f47b8": "TechnicalLimitsPiece",
        "eeb3f1b8-5db3-436f-98f9-2c9812b1d6a9": "SizingOptimizationPiece",
        "4b358e11-57eb-402a-baac-ed9d720b7e70": "CatalogRankerPiece",
        "24e79064-faab-4489-8a31-9f72cad03311": "SolarSimulationPiece",
        "0e90313e-cdac-44d2-b202-1d61eb55b8fa": "BatteryStrategyPiece",
        "193e02b8-a3d7-4ad9-aa99-10e76b77eddf": "BatterySimulationPiece",
        "36d206ac-15f9-42ea-9549-b712756722b0": "SimulateMRKScenarioPiece",
        "6b1a6e0b-0f1b-49e0-abb1-7fd089ede078": "ComputeKPIsPiece",
        "39d0e970-8af5-4378-8ac2-20a04f604dad": "InvestmentEvaluationPiece",
        "83e9f675-7a46-4797-a32c-8a42899d2a66": "DashboardDataPiece",
    }

    compiled_path = ROOT / ".domino" / "compiled_metadata.json"
    compiled = json.loads(compiled_path.read_text(encoding="utf-8")) if compiled_path.is_file() else {}

    # Fix compiled keys if still old
    key_renames = [
        ("BatteryStrategyOptimizerPiece", "BatteryStrategyPiece"),
        ("InvestmentEvalPiece", "InvestmentEvaluationPiece"),
        ("SolarSimPiece", "SolarSimulationPiece"),
        ("BatterySimPiece", "BatterySimulationPiece"),
        ("SimulatePiece", "SimulateMRKScenarioPiece"),
        ("KPIPiece", "ComputeKPIsPiece"),
        ("DashboardPiece", "DashboardDataPiece"),
    ]
    for old, new in key_renames:
        if old in compiled and new not in compiled:
            compiled[new] = compiled.pop(old)
            compiled[new]["name"] = new
            if "style" in compiled[new]:
                compiled[new]["style"]["node_label"] = new

    source_image = "ghcr.io/filipchrvala/industry_sg_vre_cost_optimizer:0.1.9-group0"
    if (ROOT / "config.toml").is_file():
        ver = re.search(r'VERSION\s*=\s*"([^"]+)"', (ROOT / "config.toml").read_text(encoding="utf-8"))
        if ver:
            source_image = f"ghcr.io/filipchrvala/industry_sg_vre_cost_optimizer:{ver.group(1)}-group0"

    repo_meta = _load_piece_metadata()

    for node_id, piece_name in suffix_to_piece.items():
        if node_id not in data.get("workflowPieces", {}):
            continue
        entry = data["workflowPieces"][node_id]
        meta = repo_meta.get(piece_name, {})
        if piece_name in compiled:
            entry.update(
                {
                    "name": piece_name,
                    "description": compiled[piece_name].get("description", entry.get("description", "")),
                    "input_schema": compiled[piece_name].get("input_schema", entry.get("input_schema", {})),
                    "output_schema": compiled[piece_name].get("output_schema", entry.get("output_schema", {})),
                    "source_image": source_image,
                }
            )
        else:
            entry["name"] = piece_name
            entry["description"] = meta.get("description", entry.get("description", ""))

        style = entry.setdefault("style", {})
        style["module"] = piece_name
        style["label"] = piece_name
        entry["source_url"] = (
            f"https://github.com/filipchrvala/industry_sg_vre_cost_optimizer/tree/main/pieces/{piece_name}"
        )

        if node_id in data.get("workflowNodes", []):
            pass
        for node in data.get("workflowNodes", []):
            if node.get("id") == node_id:
                node["data"]["name"] = piece_name
                ns = node["data"].setdefault("style", {})
                ns["module"] = piece_name
                ns["label"] = piece_name

    # Simulate: wire battery strategy input
    sim_id = "109_36d206ac-15f9-42ea-9549-b712756722b0"
    strat_id = "107_0e90313e-cdac-44d2-b202-1d61eb55b8fa"
    wpd = data.setdefault("workflowPiecesData", {})
    if sim_id in wpd:
        inputs = wpd[sim_id].setdefault("inputs", {})
        inputs["battery_strategy_recommendation_json"] = {
            "fromUpstream": True,
            "upstreamId": "BatteryStrategyPi_0e90313ecdac44d2b2021d61eb55b8fa",
            "upstreamArgument": "battery_strategy_recommendation_json",
            "upstreamValue": "BatteryStrategyPiece (0e90313e) - Battery Strategy Recommendation Json",
            "value": "",
        }

    edges = data.setdefault("workflowEdges", [])
    edge_id = (
        "reactflow__edge-107_0e90313e-cdac-44d2-b202-1d61eb55b8fa"
        "source-107_0e90313e-cdac-44d2-b202-1d61eb55b8fa-"
        f"{sim_id}target-{sim_id}"
    )
    if not any(e.get("source") == strat_id and e.get("target") == sim_id for e in edges):
        edges.append(
            {
                "source": strat_id,
                "sourceHandle": f"source-{strat_id}",
                "target": sim_id,
                "targetHandle": f"target-{sim_id}",
                "id": edge_id,
                "markerEnd": {"type": "arrowclosed", "width": 20, "height": 20},
            }
        )

    # Persist compiled metadata fixes
    if compiled_path.is_file():
        compiled_path.write_text(
            json.dumps(compiled, indent=4, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    CUSTOM.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(f"Updated {CUSTOM}")


if __name__ == "__main__":
    main()
