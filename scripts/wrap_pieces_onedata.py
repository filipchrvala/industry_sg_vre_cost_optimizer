"""Wrap piece_function with OneData stage/finish boundary."""

from __future__ import annotations

import re
import textwrap
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

OD_IMPORT = textwrap.dedent(
    '''
    try:
        from common import onedata_io as od
    except ModuleNotFoundError:
        try:
            from pieces.common import onedata_io as od
        except ModuleNotFoundError:
            od = None
    '''
).strip("\n")

SKIP = {"KPIPiece"}

PIECES: dict[str, bool] = {
    "UserInputPiece": True,
    "CatalogSyncPiece": False,
    "TechnicalLimitsPiece": False,
    "SizingOptimizationPiece": False,
    "CatalogRankerPiece": False,
    "SolarSimPiece": False,
    "BatteryStrategyOptimizerPiece": False,
    "BatterySimPiece": False,
    "SimulatePiece": False,
    "InvestmentEvalPiece": False,
    "DashboardPiece": False,
}


def _find_piece_function_end(lines: list[str], start: int) -> int:
    """Return line index where class-level next method starts (4-space indent def)."""
    for i in range(start + 1, len(lines)):
        line = lines[i]
        if re.match(r"^    def \w+", line) and not line.strip().startswith("def piece_function"):
            return i
    return len(lines)


def wrap_piece(name: str, entry: bool) -> None:
    if name in SKIP:
        return
    path = ROOT / "pieces" / name / "piece.py"
    src = path.read_text(encoding="utf-8")
    if "finish_piece" in src:
        print("skip", name)
        return

    if "onedata_io as od" not in src:
        src = src.replace(
            "from .models import InputModel, OutputModel",
            "from .models import InputModel, OutputModel\n\n" + OD_IMPORT,
        )

    src = src.replace(
        "def piece_function(self, input_data: InputModel) -> OutputModel:",
        "def piece_function(self, input_data: InputModel, secrets_data=None) -> OutputModel:",
    )

    gen = "True" if entry else "False"
    prelude = (
        "        _stage = None\n"
        "        _piece_out = None\n"
        "        _run_id = None\n"
        "        if od is not None:\n"
        "            input_data, _stage = od.stage_inputs(input_data, secrets_data)\n"
        f"            _run_id = od.resolve_run_id(input_data, secrets_data, generate={gen})\n"
    )

    marker = "def piece_function(self, input_data: InputModel, secrets_data=None) -> OutputModel:\n"
    if marker not in src:
        raise RuntimeError(f"{name}: signature missing")
    src = src.replace(marker, marker + prelude, 1)

    src = src.replace("return OutputModel(", "_piece_out = OutputModel(")

    cleanup = (
        f'            if od is not None:\n'
        f'                od.cleanup_on_error(self.results_path, secrets_data, "{name}", _stage, run_id=_run_id)\n'
    )
    if cleanup.strip() not in src:
        src = src.replace("            raise\n", cleanup + "            raise\n")

    epilogue = (
        f"        if od is not None and _piece_out is not None:\n"
        f"            return od.finish_piece(\n"
        f"                _piece_out, self.results_path, secrets_data, \"{name}\", _stage, run_id=_run_id\n"
        f"            )\n"
        f"        if _stage is not None:\n"
        f"            _stage.cleanup()\n"
        f"        return _piece_out\n"
    )

    lines = src.splitlines()
    start = next(i for i, l in enumerate(lines) if "def piece_function" in l)
    end = _find_piece_function_end(lines, start)
    lines[end:end] = epilogue.splitlines()
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("wrapped", name)


if __name__ == "__main__":
    for n, e in PIECES.items():
        wrap_piece(n, e)
