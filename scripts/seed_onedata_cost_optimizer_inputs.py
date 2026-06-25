"""Upload cost_optimizer inputs to OneData (requires ONEDATA_TOKEN)."""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pieces"))

from common import onedata_io as od  # noqa: E402
from common.onedata_defaults import DEFAULT_INPUT_DIR  # noqa: E402

SRC = ROOT / "user_input"
FILES = {
    "load_and_prices.csv": f"{DEFAULT_INPUT_DIR}/load_and_prices.csv",
    "scenario.yaml": f"{DEFAULT_INPUT_DIR}/scenario.yaml",
}


def main() -> None:
    token = os.environ.get("ONEDATA_TOKEN", "").strip()
    if not token:
        print("Set ONEDATA_TOKEN before running.")
        sys.exit(1)
    secrets = {
        "onedata_onezone_host": os.environ.get("ONEDATA_ONEZONE_HOST", "data.spice-platform.eu"),
        "onedata_token": token,
        "onedata_output_dir": os.environ.get(
            "ONEDATA_OUTPUT_BASE", "onedata:///FilipsSpace/cost_optimizer/outputs"
        ),
    }
    od.configure_onedata(secrets, force=True)
    for local_name, remote in FILES.items():
        src = SRC / local_name
        if not src.is_file():
            print("skip missing", src)
            continue
        od.write_bytes(remote, src.read_bytes())
        print("uploaded", remote)
    print("Inputs ready under", DEFAULT_INPUT_DIR)


if __name__ == "__main__":
    main()
