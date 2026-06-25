"""Sync Test.customization with published piece names, schemas, and wiring."""
from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CUSTOM = ROOT / "Test.customization"
COMPILED = ROOT / ".domino" / "compiled_metadata.json"
CONFIG = ROOT / "config.toml"

# Domino workflow node id -> published piece name
NODE_PIECES: dict[str, str] = {
    "101_6c7d1d1b-ebc0-41cf-94af-98c9378610e0": "UserInputPiece",
    "102_ee96043b-6cb0-42f1-934e-ed0b8a7f8f78": "CatalogSyncPiece",
    "103_02ae53ae-10a2-48c5-8894-70f2596f47b8": "TechnicalLimitsPiece",
    "104_eeb3f1b8-5db3-436f-98f9-2c9812b1d6a9": "SizingOptimizationPiece",
    "105_4b358e11-57eb-402a-baac-ed9d720b7e70": "CatalogRankerPiece",
    "106_24e79064-faab-4489-8a31-9f72cad03311": "SolarSimPiece",
    "107_0e90313e-cdac-44d2-b202-1d61eb55b8fa": "BatteryStrategyOptimizerPiece",
    "108_193e02b8-a3d7-4ad9-aa99-10e76b77eddf": "BatterySimPiece",
    "109_36d206ac-15f9-42ea-9549-b712756722b0": "SimulatePiece",
    "110_6b1a6e0b-0f1b-49e0-abb1-7fd089ede078": "KPIPiece",
    "111_39d0e970-8af5-4378-8ac2-20a04f604dad": "InvestmentEvalPiece",
    "112_83e9f675-7a46-4797-a32c-8a42899d2a66": "DashboardPiece",
}

SIM_ID = "109_36d206ac-15f9-42ea-9549-b712756722b0"
STRAT_ID = "107_0e90313e-cdac-44d2-b202-1d61eb55b8fa"
BATSIM_ID = "108_193e02b8-a3d7-4ad9-aa99-10e76b77eddf"
SOLAR_ID = "106_24e79064-faab-4489-8a31-9f72cad03311"
DASH_ID = "112_83e9f675-7a46-4797-a32c-8a42899d2a66"
KPI_ID = "110_6b1a6e0b-0f1b-49e0-abb1-7fd089ede078"
INV_ID = "111_39d0e970-8af5-4378-8ac2-20a04f604dad"

HIGH_MEMORY_MB = {"SimulatePiece": 1024, "BatterySimPiece": 2048, "CatalogRankerPiece": 512}

# Must match Domino UI / Airflow task_id prefixes (not domino-py compile names).
DOMINO_TASK_PREFIX: dict[str, str] = {
    "UserInputPiece": "UserInputP",
    "CatalogSyncPiece": "CatalogSyn",
    "TechnicalLimitsPiece": "TechnicalL",
    "SizingOptimizationPiece": "SizingOpti",
    "CatalogRankerPiece": "CatalogRan",
    "SolarSimPiece": "SolarSimPi",
    "BatteryStrategyOptimizerPiece": "BatteryStr",
    "BatterySimPiece": "BatterySim",
    "SimulatePiece": "SimulatePi",
    "KPIPiece": "KPIPiece",
    "InvestmentEvalPiece": "Investment",
    "DashboardPiece": "DashboardP",
}


def _upstream_id(piece_name: str, node_id: str) -> str:
    """Domino UI task_id / upstreamId prefix + node uuid without dashes."""
    prefix = DOMINO_TASK_PREFIX[piece_name]
    uuid = node_id.split("_", 1)[1].replace("-", "")
    return f"{prefix}_{uuid}"


def _node_id_from_upstream_id(upstream_id: str) -> str | None:
    if not upstream_id or len(upstream_id) < 32:
        return None
    suffix = upstream_id[-32:]
    for node_id in NODE_PIECES:
        if node_id.split("_", 1)[1].replace("-", "") == suffix:
            return node_id
    return None


def _edge_dict(source: str, target: str) -> dict:
    return {
        "source": source,
        "sourceHandle": f"source-{source}",
        "target": target,
        "targetHandle": f"target-{target}",
        "id": f"reactflow__edge-{source}source-{source}-{target}target-{target}",
        "markerEnd": {"type": "arrowclosed", "width": 20, "height": 20},
    }


def normalize_upstream_ids_and_edges(data: dict) -> None:
    """Align upstreamId values and graph edges with Domino UI task_id rules."""
    wpd = data.setdefault("workflowPiecesData", {})
    for node_id, piece_name in NODE_PIECES.items():
        inputs = wpd.setdefault(node_id, {}).setdefault("inputs", {})
        for spec in inputs.values():
            if not spec.get("fromUpstream"):
                continue
            src_node = _node_id_from_upstream_id(spec.get("upstreamId", ""))
            if not src_node:
                continue
            src_piece = NODE_PIECES[src_node]
            spec["upstreamId"] = _upstream_id(src_piece, src_node)

    edges = data.setdefault("workflowEdges", [])
    seen = {(e.get("source"), e.get("target")) for e in edges}
    for node_id in NODE_PIECES:
        for spec in wpd.get(node_id, {}).get("inputs", {}).values():
            if not spec.get("fromUpstream"):
                continue
            src_node = _node_id_from_upstream_id(spec.get("upstreamId", ""))
            if not src_node or src_node == node_id:
                continue
            pair = (src_node, node_id)
            if pair not in seen:
                edges.append(_edge_dict(src_node, node_id))
                seen.add(pair)


def _short_label(piece_name: str, node_suffix: str) -> str:
    return f"{piece_name} ({node_suffix[:8]})"


def _container_resources(piece_name: str) -> dict:
    meta_path = ROOT / "pieces" / piece_name / "metadata.json"
    if meta_path.is_file():
        cr = json.loads(meta_path.read_text(encoding="utf-8")).get("container_resources")
        if cr:
            return {
                "requests": cr.get("requests", {"cpu": 100, "memory": 128}),
                "limits": cr.get("limits", {"cpu": 500, "memory": 512}),
                "use_gpu": False,
            }
    mem = HIGH_MEMORY_MB.get(piece_name, 128)
    return {
        "requests": {"cpu": 100, "memory": 128},
        "limits": {"cpu": 500, "memory": mem},
        "use_gpu": False,
    }


def _version_image() -> str:
    ver = "0.1.26"
    if CONFIG.is_file():
        m = re.search(r'VERSION\s*=\s*"([^"]+)"', CONFIG.read_text(encoding="utf-8"))
        if m:
            ver = m.group(1)
    return f"ghcr.io/filipchrvala/industry_sg_vre_cost_optimizer:{ver}-group0"


def main() -> None:
    data = json.loads(CUSTOM.read_text(encoding="utf-8"))
    compiled = json.loads(COMPILED.read_text(encoding="utf-8"))
    source_image = _version_image()
    wpd = data.setdefault("workflowPiecesData", {})

    # --- sync workflowPieces + nodes ---
    for node_id, piece_name in NODE_PIECES.items():
        if node_id not in data.get("workflowPieces", {}):
            continue

        meta = compiled.get(piece_name, {})
        entry = data["workflowPieces"][node_id]
        entry["name"] = piece_name
        entry["source_image"] = source_image
        entry["description"] = meta.get("description", entry.get("description", ""))
        if meta.get("input_schema"):
            entry["input_schema"] = meta["input_schema"]
            props = entry["input_schema"].get("properties", {})
            props.pop("output_dir", None)
            req = entry["input_schema"].get("required", [])
            entry["input_schema"]["required"] = [name for name in req if name != "output_dir"]
        if meta.get("output_schema"):
            entry["output_schema"] = meta["output_schema"]
        entry["container_resources"] = _container_resources(piece_name)
        style = entry.setdefault("style", {})
        style["module"] = piece_name
        style["label"] = piece_name
        entry["source_url"] = (
            f"https://github.com/filipchrvala/industry_sg_vre_cost_optimizer/tree/main/pieces/{piece_name}"
        )

        wpd_node = wpd.setdefault(node_id, {})
        wpd_node.setdefault("containerResources", {})
        wpd_node["containerResources"]["memory"] = entry["container_resources"]["limits"]["memory"]
        wpd_node["containerResources"]["cpu"] = entry["container_resources"]["limits"].get("cpu", 500)
        wpd_node["containerResources"]["useGpu"] = False

        for node in data.get("workflowNodes", []):
            if node.get("id") == node_id:
                node["data"]["name"] = piece_name
                ns = node["data"].setdefault("style", {})
                ns["module"] = piece_name
                ns["label"] = piece_name

    # --- BatterySim inputs (explicit wiring) ---
    battery_inputs = {
        "load_csv": ("101_6c7d1d1b-ebc0-41cf-94af-98c9378610e0", "UserInputPiece", "load_csv", "Load Csv"),
        "scenario_yaml": ("104_eeb3f1b8-5db3-436f-98f9-2c9812b1d6a9", "SizingOptimizationPiece", "sized_scenario_yaml", "Sized Scenario Yaml"),
        "virtual_solar_csv": (SOLAR_ID, "SolarSimPiece", "virtual_solar_csv", "Virtual Solar Csv"),
        "battery_strategy_recommendation_json": (
            STRAT_ID,
            "BatteryStrategyOptimizerPiece",
            "battery_strategy_recommendation_json",
            "Battery Strategy Recommendation Json",
        ),
    }
    battery_wpd_inputs = wpd.setdefault(BATSIM_ID, {}).setdefault("inputs", {})
    for arg, (src_id, pname, up_arg, label) in battery_inputs.items():
        suffix = src_id.split("_", 1)[1][:8]
        battery_wpd_inputs[arg] = {
            "fromUpstream": True,
            "upstreamId": _upstream_id(pname, src_id),
            "upstreamArgument": up_arg,
            "upstreamValue": f"{_short_label(pname, suffix)} - {label}",
            "value": "",
        }

    # --- Simulate inputs (explicit wiring) ---
    sim_inputs = {
        "load_csv": ("101_6c7d1d1b-ebc0-41cf-94af-98c9378610e0", "UserInputPiece", "load_csv", "Load Csv"),
        "scenario_yaml": ("104_eeb3f1b8-5db3-436f-98f9-2c9812b1d6a9", "SizingOptimizationPiece", "sized_scenario_yaml", "Sized Scenario Yaml"),
        "virtual_solar_csv": (SOLAR_ID, "SolarSimPiece", "virtual_solar_csv", "Virtual Solar Csv"),
        "battery_dispatch_csv": (BATSIM_ID, "BatterySimPiece", "battery_dispatch_csv", "Battery Dispatch Csv"),
        "battery_summary_csv": (BATSIM_ID, "BatterySimPiece", "battery_summary_csv", "Battery Summary Csv"),
        "ranked_catalog_json": ("105_4b358e11-57eb-402a-baac-ed9d720b7e70", "CatalogRankerPiece", "catalog_ranked_recommendation_json", "Catalog Ranked Recommendation Json"),
        "inverter_catalog_json": ("102_ee96043b-6cb0-42f1-934e-ed0b8a7f8f78", "CatalogSyncPiece", "inverter_catalog_json", "Inverter Catalog Json"),
        "battery_catalog_json": ("102_ee96043b-6cb0-42f1-934e-ed0b8a7f8f78", "CatalogSyncPiece", "battery_catalog_json", "Battery Catalog Json"),
        "catalog_manifest_json": ("102_ee96043b-6cb0-42f1-934e-ed0b8a7f8f78", "CatalogSyncPiece", "catalog_manifest_json", "Catalog Manifest Json"),
    }
    sim_wpd_inputs = wpd.setdefault(SIM_ID, {}).setdefault("inputs", {})
    for arg, (src_id, pname, up_arg, label) in sim_inputs.items():
        suffix = src_id.split("_", 1)[1][:8]
        sim_wpd_inputs[arg] = {
            "fromUpstream": True,
            "upstreamId": _upstream_id(pname, src_id),
            "upstreamArgument": up_arg,
            "upstreamValue": f"{_short_label(pname, suffix)} - {label}",
            "value": "",
        }
    sim_wpd_inputs.pop("battery_strategy_recommendation_json", None)
    sim_wpd_inputs.pop("output_dir", None)

    # --- Dashboard inputs ---
    dash_inputs = {
        "report_json": (SIM_ID, "SimulatePiece", "report_json", "Report Json"),
        "kpi_results_csv": (KPI_ID, "KPIPiece", "kpi_results_csv", "Kpi Results Csv"),
        "investment_evaluation_csv": (INV_ID, "InvestmentEvalPiece", "investment_evaluation_csv", "Investment Evaluation Csv"),
    }
    dash_wpd = wpd.setdefault(DASH_ID, {}).setdefault("inputs", {})
    for arg, (src_id, pname, up_arg, label) in dash_inputs.items():
        suffix = src_id.split("_", 1)[1][:8]
        dash_wpd[arg] = {
            "fromUpstream": True,
            "upstreamId": _upstream_id(pname, src_id),
            "upstreamArgument": up_arg,
            "upstreamValue": f"{_short_label(pname, suffix)} - {label}",
            "value": "",
        }

    # --- edges: only data-flow edges (no duplicate Strategy->Sim if only arg mapping) ---
    # Domino still needs graph edges for upstream resolution
    required_edges = [
        ("101_6c7d1d1b-ebc0-41cf-94af-98c9378610e0", BATSIM_ID),
        ("104_eeb3f1b8-5db3-436f-98f9-2c9812b1d6a9", BATSIM_ID),
        (SOLAR_ID, BATSIM_ID),
        (STRAT_ID, BATSIM_ID),
        ("101_6c7d1d1b-ebc0-41cf-94af-98c9378610e0", SIM_ID),
        ("104_eeb3f1b8-5db3-436f-98f9-2c9812b1d6a9", SIM_ID),
        (SOLAR_ID, SIM_ID),
        (BATSIM_ID, SIM_ID),
        ("105_4b358e11-57eb-402a-baac-ed9d720b7e70", SIM_ID),
        ("102_ee96043b-6cb0-42f1-934e-ed0b8a7f8f78", SIM_ID),
        (SIM_ID, KPI_ID),
        (SIM_ID, INV_ID),
        (SIM_ID, DASH_ID),
        (KPI_ID, INV_ID),
        (KPI_ID, DASH_ID),
        (INV_ID, DASH_ID),
    ]

    def _edge(s: str, t: str) -> dict:
        return {
            "source": s,
            "sourceHandle": f"source-{s}",
            "target": t,
            "targetHandle": f"target-{t}",
            "id": f"reactflow__edge-{s}source-{s}-{t}target-{t}",
            "markerEnd": {"type": "arrowclosed", "width": 20, "height": 20},
        }

    # Remove Strategy->Sim duplicate edges beyond one; remove Strategy->BatterySim if present
    edges = data.get("workflowEdges", [])
    cleaned = []
    seen_pairs: set[tuple[str, str]] = set()
    valid_sim_sources = {src for src, tgt in required_edges if tgt == SIM_ID}
    valid_battery_sources = {src for src, tgt in required_edges if tgt == BATSIM_ID}
    for e in edges:
        pair = (e.get("source", ""), e.get("target", ""))
        if pair in seen_pairs:
            continue
        if e.get("target") == BATSIM_ID and e.get("source") not in valid_battery_sources:
            continue
        # Keep Simulate incoming edges only for sources that are actually mapped to Simulate inputs.
        if e.get("target") == SIM_ID and e.get("source") not in valid_sim_sources:
            continue
        seen_pairs.add(pair)
        cleaned.append(e)

    for s, t in required_edges:
        if (s, t) not in seen_pairs:
            cleaned.append(_edge(s, t))
            seen_pairs.add((s, t))

    data["workflowEdges"] = cleaned

    normalize_upstream_ids_and_edges(data)

    CUSTOM.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    sim_schema = data["workflowPieces"][SIM_ID]["input_schema"]["properties"]
    print(f"Synced {CUSTOM}")
    print(f"image={source_image}")
    print(f"Simulate inputs: {list(sim_schema.keys())}")


if __name__ == "__main__":
    main()
