"""Verify every piece model imports the way Domino organize does.

Domino puts `pieces/` on sys.path and imports `<PieceName>.models`, which is a
different import root than a local `pytest` run. A piece that works locally can
still fail to organize, and the failure only appears at image build time.

The list is discovered from the filesystem rather than hardcoded, so a new piece
is covered the moment it exists.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PIECES = ROOT / "pieces"
sys.path.insert(0, str(PIECES))

# Pieces ported from UC3.4 keep their upstream shape, where secrets are not
# used; requiring a SecretsModel of them would mean editing vendored code.
SECRETS_OPTIONAL = {
    "DataNormalizationPiece",
    "DataPreprocessingPiece",
    "InferencePiece",
    "ModelDeciderPiece",
    "OpenMeteoPVDataPiece",
    "PVOUTErrorCorrectionModelTrainPiece",
    "PVOUTPredictionModelTrainPiece",
    "PvoutModelFeatureSelectPiece",
    "PvoutStagedInferenceSpecPiece",
    "PvoutToVirtualSolarPiece",
}


def discover() -> list[str]:
    return sorted(
        d.name
        for d in PIECES.iterdir()
        if d.is_dir() and (d / "models.py").is_file() and (d / "metadata.json").is_file()
    )


def main() -> int:
    failures: list[str] = []
    for name in discover():
        try:
            mod = importlib.import_module(f"{name}.models")
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{name}: import failed: {exc}")
            continue

        for attr in ("InputModel", "OutputModel"):
            if not hasattr(mod, attr):
                failures.append(f"{name}: missing {attr}")
        if not hasattr(mod, "SecretsModel") and name not in SECRETS_OPTIONAL:
            failures.append(f"{name}: missing SecretsModel")

        print("OK", name)

    if failures:
        print("\nFailures:")
        for failure in failures:
            print(f"  - {failure}")
        return 1

    print(f"\nAll {len(discover())} piece models import successfully (Domino organize path).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
