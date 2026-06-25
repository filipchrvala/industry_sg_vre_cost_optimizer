"""Verify all piece models import the way Domino organize does (pieces/ on sys.path)."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PIECES = ROOT / "pieces"
sys.path.insert(0, str(PIECES))

NAMES = [
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

for name in NAMES:
    mod = importlib.import_module(f"{name}.models")
    assert hasattr(mod, "InputModel"), name
    assert hasattr(mod, "OutputModel"), name
    assert hasattr(mod, "SecretsModel"), name
    print("OK", name)

print("All piece models import successfully (Domino organize path).")
