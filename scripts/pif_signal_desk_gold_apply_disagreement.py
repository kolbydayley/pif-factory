#!/usr/bin/env python3
"""Apply the completed private GPT-5.5 dev disagreement review and re-audit."""
from __future__ import annotations

import json
import sys
from pathlib import Path

PIF_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIF_ROOT))

from research_factory.signal_desk_gold_audit import evaluate_dev_audit  # noqa: E402
from research_factory.signal_desk_gold_disagreement import build_cases  # noqa: E402
from research_factory.signal_desk_gold_disagreement_apply import apply_decisions  # noqa: E402
from research_factory.signal_desk_rebuild_gold import select_blind_gold_audit_windows  # noqa: E402


def main() -> int:
    root = PIF_ROOT / "work/signal-desk-rebuild/gold-authoring-v2"
    manifest_path = PIF_ROOT / "work/signal-desk-rebuild/benchmark/partial-manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    development_ids = {str(row["window_id"]) for row in manifest["windows"]
                       if row.get("split") == "development"}
    audit = json.loads((root / "artifacts/gold-audit-dev.json").read_text(encoding="utf-8"))
    selected = [str(x) for x in audit["audit_selection"]["window_ids"] if str(x) in development_ids]
    output_root = root / "results/development-adjudicated"
    receipt = apply_decisions(
        manifest_path=manifest_path, result_root=root / "results/development",
        project_root=PIF_ROOT, private_root=root,
        selected_window_ids=selected,
        decisions_root=root / "private-gpt55-disagreement-review",
        output_root=output_root,
    )
    (root / "artifacts/gold-adjudicated-truth-dev.json").write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    blind_ids = set(select_blind_gold_audit_windows(manifest)) & development_ids
    semantic = json.loads((root / "artifacts/gold-semantic-review-dev.json").read_text(encoding="utf-8"))
    atomicity = json.loads((root / "artifacts/gold-atomicity-adjudication-dev.json").read_text(encoding="utf-8"))
    corrected = evaluate_dev_audit(
        manifest_path=manifest_path, result_root=output_root,
        initial_window_ids=sorted(blind_ids), initial_windows=len(blind_ids),
        project_root=PIF_ROOT,
        semantic_reversal_adjudications={str(k): str(v) for k, v in semantic["decisions"].items()},
        atomicity_adjudications={str(k): str(v["verdict"]) for k, v in atomicity["decisions"].items()},
    )
    (root / "artifacts/gold-audit-dev-adjudicated.json").write_text(
        json.dumps(corrected, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps({"adjudication": receipt, "audit": corrected}, indent=2, sort_keys=True))
    return 0 if corrected["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
