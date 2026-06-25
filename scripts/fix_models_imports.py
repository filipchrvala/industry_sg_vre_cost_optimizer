"""Fix models.py imports for Domino organize (pieces/ on sys.path, not pieces package)."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PIECES = ROOT / "pieces"

OLD = "from pieces.common.onedata_models import OneDataSecretsModel, RunIdInputMixin"
NEW = """try:
    from common.onedata_models import OneDataSecretsModel, RunIdInputMixin
except ModuleNotFoundError:
    from pieces.common.onedata_models import OneDataSecretsModel, RunIdInputMixin"""

for models in PIECES.glob("*/models.py"):
    text = models.read_text(encoding="utf-8")
    if OLD not in text:
        continue
    models.write_text(text.replace(OLD, NEW), encoding="utf-8")
    print("fixed", models.parent.name)
