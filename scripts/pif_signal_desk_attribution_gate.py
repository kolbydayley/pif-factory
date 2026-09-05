#!/usr/bin/env python3
"""Run the sanitized Signal Desk speaker-attribution completeness gate."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from research_factory.signal_desk_attribution_gate import audit_gold_c, repair_gold_c  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--result-root", type=Path, required=True)
    parser.add_argument("--project-root", type=Path, default=ROOT)
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--repair-output", type=Path, help="write filtered repaired C view and quarantine receipt")
    args = parser.parse_args()
    if args.repair_output:
        receipt = repair_gold_c(manifest_path=args.manifest, result_root=args.result_root,
                                output_root=args.repair_output, project_root=args.project_root)
    else:
        receipt = audit_gold_c(manifest_path=args.manifest, result_root=args.result_root, project_root=args.project_root)
    encoded = json.dumps(receipt, indent=2, sort_keys=True) + "\n"
    if args.receipt:
        args.receipt.parent.mkdir(parents=True, exist_ok=True)
        args.receipt.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0 if receipt.get("passed", True) else 2


if __name__ == "__main__":
    raise SystemExit(main())
