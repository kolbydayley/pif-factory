#!/usr/bin/env python3
"""Evaluate the complete development blind-audit slice without exposing items."""

import json
import sys
from pathlib import Path

PIF_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIF_ROOT))

from research_factory.signal_desk_gold_audit import (  # noqa: E402
    evaluate_dev_audit,
    select_dev_audit_windows,
)
from research_factory.signal_desk_gold_atomicity import evaluate_atomicity_review  # noqa: E402
from research_factory.signal_desk_rebuild_gold import (  # noqa: E402
    select_blind_gold_audit_windows,
)


def main() -> int:
    root = PIF_ROOT / "work/signal-desk-rebuild/gold-authoring-v2"
    manifest_path = PIF_ROOT / "work/signal-desk-rebuild/benchmark/partial-manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    development_ids = {
        str(row["window_id"])
        for row in manifest["windows"]
        if row["split"] == "development"
    }
    blind_ids = set(select_blind_gold_audit_windows(manifest)) & development_ids
    c_outputs = {
        path.stem: json.loads(path.read_text(encoding="utf-8"))
        for path in (root / "results/development/C").glob("*.json")
    }
    audit_plan = select_dev_audit_windows(
        manifest,
        {window_id: len(output["events"]) for window_id, output in c_outputs.items()},
        initial_window_ids=sorted(blind_ids),
    )
    receipt = evaluate_dev_audit(
        manifest_path=manifest_path,
        result_root=root / "results/development",
        expected_windows=len(audit_plan["window_ids"]),
        blind_window_ids=audit_plan["window_ids"],
        initial_windows=len(blind_ids),
    )
    output = root / "artifacts/gold-audit-dev.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    atomicity = evaluate_atomicity_review(result_root=root / "results/development")
    atomicity_output = root / "artifacts/gold-atomicity-review-dev.json"
    atomicity_output.write_text(
        json.dumps(atomicity, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps({
        "blind_audit": receipt,
        "atomicity_review": atomicity,
    }, indent=2, sort_keys=True))
    return 0 if receipt["passed"] and atomicity["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
