"""Shared import helper for pieces that call SimulateMRKScenarioPiece helpers."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

_SIMULATE_MODULE_CANDIDATES = (
    "pieces.SimulateMRKScenarioPiece.piece",
    "pieces.SimulatePiece.piece",
)


def load_simulate_module():
    """Import simulate helpers; supports legacy SimulatePiece in older Domino images."""
    repo_root = Path(__file__).resolve().parents[1]
    repo_s = str(repo_root)
    if repo_s not in sys.path:
        sys.path.insert(0, repo_s)

    last_err: ModuleNotFoundError | None = None
    for module_name in _SIMULATE_MODULE_CANDIDATES:
        try:
            return importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            last_err = exc

    raise ModuleNotFoundError(
        "Could not import simulate helper module. Tried: "
        f"{', '.join(_SIMULATE_MODULE_CANDIDATES)}. "
        "Publish a new Domino image (e.g. 0.1.10-group0) that includes the renamed pieces."
    ) from last_err
