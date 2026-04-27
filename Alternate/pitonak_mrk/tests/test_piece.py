from __future__ import annotations

from pathlib import Path

import pytest

from pieces.SimulatePiece.models import InputModel
from pieces.SimulatePiece.piece import SimulatePiece


def test_simulate_piece_writes_report(tmp_path: Path):
    root = Path(__file__).resolve().parent.parent
    csv_p = root / "user_input" / "load_and_prices.csv"
    scen_p = root / "user_input" / "scenario.yaml"
    if not csv_p.is_file() or not scen_p.is_file():
        pytest.skip("sample user_input missing")

    piece = SimulatePiece()
    piece.results_path = str(tmp_path / "domino_out")
    out = piece.piece_function(InputModel(load_csv=str(csv_p), scenario_yaml=str(scen_p), output_dir=""))
    assert Path(out.report_json).is_file()
    assert "finished" in out.message.lower()
