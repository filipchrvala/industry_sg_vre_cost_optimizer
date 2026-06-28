"""Export SPICE/GitHub workflow JSON from test_cost_optimizer_onedata.customization."""

from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CUSTOM = ROOT / "test_cost_optimizer_onedata.customization"
CONFIG = ROOT / "config.toml"
OUT_GHCR = ROOT / "test_cost_optimizer_onedata.json"
OUT_SPICE = ROOT / "test_cost_optimizer_onedata.spice.json"
WORKFLOWS_PACK = (
    Path(__file__).resolve().parents[2]
    / "uc3-domino-gitlab-sync"
    / "cost_optimizer_onedata"
    / "UC3.2_cost_optimizer_onedata.json"
)


def _version() -> str:
    text = CONFIG.read_text(encoding="utf-8") if CONFIG.is_file() else ""
    match = re.search(r'VERSION\s*=\s*"([^"]+)"', text)
    return match.group(1) if match else "0.1.39"


def _image(*, spice: bool, version: str) -> str:
    if spice:
        return (
            f"harbor.testbed.spice-platform.eu/partner/uc3/"
            f"industry_sg_vre_cost_optimizer:{version}-group0"
        )
    return f"ghcr.io/filipchrvala/industry_sg_vre_cost_optimizer:{version}-group0"


def _repo_urls(*, spice: bool) -> tuple[str, str]:
    if spice:
        base = "https://gitlab.spice-platform.eu/use-cases/uc3/UC3.2_Industry_Sg_Vre_Cost_Optimizer"
    else:
        base = "https://github.com/filipchrvala/industry_sg_vre_cost_optimizer"
    return base, base


def export(*, spice: bool) -> dict:
    data = json.loads(CUSTOM.read_text(encoding="utf-8"))
    version = _version()
    image = _image(spice=spice, version=version)
    repo_url, repo_base = _repo_urls(spice=spice)
    for piece in data.get("workflowPieces", {}).values():
        piece["source_image"] = image
        piece["repository_url"] = repo_url
        name = piece.get("name", "")
        if name:
            piece["source_url"] = f"{repo_base}/tree/main/pieces/{name}"
    if spice:
        payload = dict(data)
    else:
        payload = {**data, "source_image": image}
    return payload


def write(path: Path, payload: dict) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    print(f"Wrote {path} ({path.stat().st_size} bytes)")


def main() -> int:
    write(OUT_GHCR, export(spice=False))
    spice = export(spice=True)
    write(OUT_SPICE, spice)
    if WORKFLOWS_PACK.parent.is_dir():
        write(WORKFLOWS_PACK, spice)
    else:
        print(f"Skip workflows pack (missing {WORKFLOWS_PACK.parent})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
