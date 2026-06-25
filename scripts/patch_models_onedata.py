"""Add OneDataSecretsModel + run_id to all piece models.py files."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PIECES = [
    "UserInputPiece",
    "CatalogSyncPiece",
    "TechnicalLimitsPiece",
    "SizingOptimizationPiece",
    "CatalogRankerPiece",
    "SolarSimPiece",
    "BatteryStrategyOptimizerPiece",
    "BatterySimPiece",
    "SimulatePiece",
    "KPIPiece",
    "InvestmentEvalPiece",
    "DashboardPiece",
]

IMPORT_BLOCK = """from pieces.common.onedata_models import OneDataSecretsModel, RunIdInputMixin

"""

for name in PIECES:
    path = ROOT / "pieces" / name / "models.py"
    if not path.is_file():
        print("skip", path)
        continue
    text = path.read_text(encoding="utf-8")
    if "OneDataSecretsModel" in text:
        print("already", name)
        continue
  # Insert import after pydantic imports
    if "from pieces.common.onedata_models" not in text:
        text = re.sub(
            r"(from pydantic import[^\n]+\n)",
            r"\1\n" + IMPORT_BLOCK,
            text,
            count=1,
        )
    text = text.replace("class InputModel(BaseModel):", "class InputModel(RunIdInputMixin):")
    if "class SecretsModel" not in text:
        text = text.replace(
            "class OutputModel",
            "class SecretsModel(OneDataSecretsModel):\n    pass\n\n\nclass OutputModel",
            1,
        )
    if name == "UserInputPiece" and "run_id: str" not in text:
        text = text.replace(
            "class OutputModel(BaseModel):",
            "class OutputModel(BaseModel):",
        )
        text = text.replace(
            "    scenario_yaml: str\n",
            "    scenario_yaml: str\n    run_id: str = \"\"\n",
            1,
        )
    path.write_text(text, encoding="utf-8")
    print("patched models", name)
