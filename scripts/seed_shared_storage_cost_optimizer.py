"""Seed local Domino shared storage for cost optimizer workflow (no OneData)."""

from __future__ import annotations

import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "user_input"
DEST = Path("/home/shared_storage/cost_optimizer/inputs")

FILES = ("load_and_prices.csv", "scenario.yaml", "load.csv", "prices.csv")


def main() -> None:
    DEST.mkdir(parents=True, exist_ok=True)
    for name in FILES:
        src = SRC / name
        if src.is_file():
            shutil.copy2(src, DEST / name)
            print("copied", src, "->", DEST / name)
    print("Done. Use test_cost_optimizer_local.customization in local Domino.")


if __name__ == "__main__":
    main()
