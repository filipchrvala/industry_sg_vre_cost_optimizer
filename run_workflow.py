#!/usr/bin/env python3
"""MRK workflow orchestrator (pitonak-like structure).

  python run_workflow.py
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from workflow.orchestrator import main

if __name__ == "__main__":
    raise SystemExit(main())
