"""Let tests import pieces without a Domino installation."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for path in (ROOT, ROOT / "pieces"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from pieces.local_compat import install_domino_stub  # noqa: E402

install_domino_stub()
