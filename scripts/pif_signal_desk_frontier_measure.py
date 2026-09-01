#!/usr/bin/env python3
"""Measure A1 predictions against Gold C and freeze the relative gates."""

import json
import sys
from pathlib import Path

PIF_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIF_ROOT))

from research_factory.signal_desk_frontier import measure_frontier  # noqa: E402


def main() -> int:
    root = PIF_ROOT / "work/signal-desk-rebuild/gold-authoring-v2"
    receipt, gates = measure_frontier(
        manifest_path=PIF_ROOT / "work/signal-desk-rebuild/benchmark/partial-manifest.json",
        gold_c_root=root / "results/development/C",
        prediction_root=root / "results/development/A1",
    )
    artifacts = root / "artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)
    (artifacts / "frontier-ceiling.json").write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (artifacts / "frozen-gates.json").write_text(
        json.dumps(gates, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
