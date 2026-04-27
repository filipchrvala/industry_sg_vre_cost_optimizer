#!/usr/bin/env python3
"""Local runner using SimulatePiece naming style.

  python run_mrk_analysis.py
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pieces.SimulatePiece.models import InputModel
from pieces.SimulatePiece.piece import SimulatePiece


def main() -> int:
    p = argparse.ArgumentParser(description="MRK / PV / battery savings on historical load")
    p.add_argument("--csv", type=Path, default=ROOT / "user_input" / "load_and_prices.csv")
    p.add_argument("--scenario", type=Path, default=ROOT / "user_input" / "scenario.yaml")
    p.add_argument("--output", type=Path, default=ROOT / "output")
    args = p.parse_args()

    piece = SimulatePiece()
    r = piece.piece_function(
        InputModel(
            load_csv=str(args.csv),
            scenario_yaml=str(args.scenario),
            output_dir=str(args.output),
        )
    )
    print("Written:", r.report_json)
    rep = json.loads(Path(r.report_json).read_text(encoding="utf-8"))
    b = rep["scenarios"]["baseline"]["total_operating_eur"]
    both = rep["scenarios"].get("pv_and_battery")
    if both:
        print(f"Baseline operating cost (period): {b:,.0f} EUR")
        print(f"PV+battery operating cost (period): {both['total_operating_eur']:,.0f} EUR")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
