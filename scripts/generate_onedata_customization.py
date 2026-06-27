"""Generate test_cost_optimizer_onedata.customization and local variant for GitHub/Domino."""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from sync_test_customization import NODE_PIECES, _upstream_id, _short_label, normalize_upstream_ids_and_edges  # noqa: E402

CUSTOM = ROOT / "Test.customization"
OUT_ONEDATA = ROOT / "test_cost_optimizer_onedata.customization"
OUT_LOCAL = ROOT / "test_cost_optimizer_local.customization"
OUT_SPICE = ROOT / "test_cost_optimizer_onedata.spice.customization"
CONFIG = ROOT / "config.toml"
CONFIG_SPICE = ROOT / "config.spice.toml"
COMPILED = ROOT / ".domino" / "compiled_metadata.json"

USER_ID = "101_6c7d1d1b-ebc0-41cf-94af-98c9378610e0"

def _secrets_schema() -> dict:
    sys.path.insert(0, str(ROOT / "pieces"))
    from common.onedata_defaults import (
        DEFAULT_ONEDATA_TOKEN,
        DEFAULT_ONEZONE_HOST,
        DEFAULT_OUTPUT_DIR,
    )

    return {
        "title": "SecretsModel",
        "type": "object",
        "properties": {
            "onedata_onezone_host": {
                "anyOf": [{"type": "string"}, {"type": "null"}],
                "default": DEFAULT_ONEZONE_HOST,
                "description": "OneData Onezone host (default in code)",
                "title": "Onedata Onezone Host",
            },
            "onedata_token": {
                "anyOf": [{"type": "string"}, {"type": "null"}],
                "default": DEFAULT_ONEDATA_TOKEN,
                "description": "OneData access token (default in code; Domino secrets optional)",
                "title": "Onedata Token",
            },
            "onedata_output_dir": {
                "anyOf": [{"type": "string"}, {"type": "null"}],
                "default": DEFAULT_OUTPUT_DIR,
                "description": "Base dir for per-run outputs: <dir>/<run_id>/<PieceName>/",
                "title": "Onedata Output Dir",
            },
        },
    }

ONEDATA_INPUTS = {
    "load_csv": "onedata:///FilipsSpace/cost_optimizer/inputs/load_and_prices.csv",
    "prices_csv": "",
    "scenario_yaml": "onedata:///FilipsSpace/cost_optimizer/inputs/scenario.yaml",
}

LOCAL_INPUTS = {
    "load_csv": "/home/shared_storage/cost_optimizer/inputs/load_and_prices.csv",
    "prices_csv": "",
    "scenario_yaml": "/home/shared_storage/cost_optimizer/inputs/scenario.yaml",
}


def _version_image(*, spice: bool = False) -> str:
    cfg = CONFIG_SPICE if spice and CONFIG_SPICE.is_file() else CONFIG
    ver = "0.1.39"
    if cfg.is_file():
        m = re.search(r'VERSION\s*=\s*"([^"]+)"', cfg.read_text(encoding="utf-8"))
        if m:
            ver = m.group(1)
    if spice:
        return f"harbor.testbed.spice-platform.eu/partner/uc3/industry_sg_vre_cost_optimizer:{ver}-group0"
    return f"ghcr.io/filipchrvala/industry_sg_vre_cost_optimizer:{ver}-group0"


def _wire_run_id(data: dict) -> None:
    wpd = data.setdefault("workflowPiecesData", {})
    user_suffix = USER_ID.split("_", 1)[1][:8]
    for node_id in NODE_PIECES:
        if node_id == USER_ID:
            continue
        piece_name = NODE_PIECES[node_id]
        inputs = wpd.setdefault(node_id, {}).setdefault("inputs", {})
        inputs["run_id"] = {
            "fromUpstream": True,
            "upstreamId": _upstream_id("UserInputPiece", USER_ID),
            "upstreamArgument": "run_id",
            "upstreamValue": f"{_short_label('UserInputPiece', user_suffix)} - Run Id",
            "value": "",
        }


def _apply_inputs(data: dict, user_inputs: dict[str, str]) -> None:
    wpd = data.setdefault("workflowPiecesData", {})
    ui = wpd.setdefault(USER_ID, {}).setdefault("inputs", {})
    for key, val in user_inputs.items():
        ui[key] = {
            "fromUpstream": False,
            "upstreamId": "",
            "upstreamArgument": "",
            "upstreamValue": "",
            "value": val,
        }


def build(mode: str, *, spice: bool = False) -> dict:
    subprocess.run([sys.executable, str(ROOT / "scripts" / "sync_test_customization.py")], check=True)
    data = json.loads(CUSTOM.read_text(encoding="utf-8"))
    compiled = json.loads(COMPILED.read_text(encoding="utf-8")) if COMPILED.is_file() else {}

    source_image = _version_image(spice=spice)
    gitlab_base = "https://gitlab.spice-platform.eu/use-cases/uc3/UC3.2_Industry_Sg_Vre_Cost_Optimizer"
    github_base = "https://github.com/filipchrvala/industry_sg_vre_cost_optimizer"
    repo_base = gitlab_base if spice else github_base

    for node_id, piece_name in NODE_PIECES.items():
        entry = data.get("workflowPieces", {}).get(node_id)
        if not entry:
            continue
        entry["source_image"] = source_image
        entry["secrets_schema"] = _secrets_schema()
        meta = compiled.get(piece_name, {})
        if meta.get("input_schema"):
            entry["input_schema"] = meta["input_schema"]
        if meta.get("output_schema"):
            entry["output_schema"] = meta["output_schema"]
        entry["repository_url"] = repo_base
        entry["source_url"] = f"{repo_base}/tree/main/pieces/{piece_name}"

    _wire_run_id(data)
    normalize_upstream_ids_and_edges(data)
    if mode == "onedata":
        _apply_inputs(data, ONEDATA_INPUTS)
    else:
        _apply_inputs(data, LOCAL_INPUTS)
        for node_id in data.get("workflowPieces", {}):
            data["workflowPieces"][node_id]["secrets_schema"] = None

    return data


def main() -> None:
    onedata = build("onedata")
    OUT_ONEDATA.write_text(json.dumps(onedata, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(f"Wrote {OUT_ONEDATA}")

    spice = build("onedata", spice=True)
    OUT_SPICE.write_text(json.dumps(spice, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(f"Wrote {OUT_SPICE} (Harbor images for GitLab)")

    local = build("local")
    OUT_LOCAL.write_text(json.dumps(local, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(f"Wrote {OUT_LOCAL}")


if __name__ == "__main__":
    main()
