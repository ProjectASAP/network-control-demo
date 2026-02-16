#!/usr/bin/env python3
from __future__ import annotations

import runpy
import sys
from pathlib import Path


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root))
    runpy.run_path(str(root / "run_rtt_sweep_epoch_with_solver.py"), run_name="__main__")
