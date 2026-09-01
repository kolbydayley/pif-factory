#!/usr/bin/env python3
"""Evaluate the complete development blind-audit slice without exposing items."""

import json
import sys
from pathlib import Path

PIF_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIF_ROOT))

from research_factory.signal_desk_gold_audit import evaluate_dev_audit  # noqa: E402


def main() -> int:
    root = PIF_ROOT / "work/signal-desk-rebuild/gold-authoring-v2"
    receipt = evaluate_dev_audit(
        manifest_path=PIF_ROOT / "work/signal-desk-rebuild/benchmark/partial-manifest.json",
        result_root=root / "results/development",
    )
    output = root / "artifacts/gold-audit-dev.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0 if receipt["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
