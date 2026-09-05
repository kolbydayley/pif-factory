#!/usr/bin/env python3
"""Apply the completed private GPT-5.5 dev disagreement review and re-audit."""
from __future__ import annotations

import json
import hashlib
import sys
from pathlib import Path

PIF_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIF_ROOT))

from research_factory.signal_desk_gold_audit import (  # noqa: E402
    evaluate_adjudicated_gold_reliability,
)
from research_factory.signal_desk_gold_disagreement import (  # noqa: E402
    build_cases,
    validate_decisions,
)
from research_factory.signal_desk_gold_disagreement_apply import apply_decisions  # noqa: E402


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _tree_sha256(paths: list[Path], *, root: Path) -> str:
    """Hash the exact input file set without exposing its contents."""

    entries = [
        {"path": str(path.relative_to(root)), "sha256": _file_sha256(path)}
        for path in sorted(paths)
    ]
    return hashlib.sha256(
        json.dumps(entries, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _load_decisions(decisions_root: Path, expected_ids: set[str]) -> dict[str, dict]:
    decisions: dict[str, dict] = {}
    for path in sorted(decisions_root.glob("*.output.json")):
        value = json.loads(path.read_text(encoding="utf-8"))
        file_ids = [str(row.get("case_id") or "") for row in value.get("decisions", [])]
        loaded = validate_decisions(value, file_ids)
        for case_id, decision in loaded.items():
            if case_id not in expected_ids:
                raise RuntimeError(f"decision file contains unknown case: {case_id}")
            if case_id in decisions:
                raise RuntimeError(f"duplicate adjudication decision: {case_id}")
            decisions[case_id] = decision
    if set(decisions) != expected_ids:
        raise RuntimeError(f"adjudication decisions cover {len(decisions)}/{len(expected_ids)} cases")
    return decisions


def main() -> int:
    root = PIF_ROOT / "work/signal-desk-rebuild/gold-authoring-v2"
    manifest_path = PIF_ROOT / "work/signal-desk-rebuild/benchmark/partial-manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    audit = json.loads((root / "artifacts/gold-audit-dev.json").read_text(encoding="utf-8"))
    selected = [str(x) for x in audit["audit_selection"]["window_ids"]]
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
    # The raw blind-audit receipt remains the source for catastrophic,
    # semantic, over-splitting, and power gates.  It is deliberately not
    # replaced by a comparison of corrected Gold-C to the same independent
    # audit: that would recount Gold-supported disagreements as failures.
    raw_audit_path = root / "artifacts/gold-audit-dev.json"
    raw_audit = json.loads(raw_audit_path.read_text(encoding="utf-8"))
    cases = build_cases(
        manifest_path=manifest_path,
        result_root=root / "results/development",
        project_root=PIF_ROOT,
        selected_window_ids=raw_audit["audit_selection"]["window_ids"],
    )
    decisions = _load_decisions(
        root / "private-gpt55-disagreement-review",
        {str(case["case_id"]) for case in cases},
    )
    selected_ids = set(raw_audit["audit_selection"]["window_ids"])
    gold_paths = [
        root / "results/development" / "C" / f"{window_id}.json"
        for window_id in sorted(selected_ids)
    ]
    audit_paths = [
        root / "results/development" / "AUDIT" / f"{window_id}.json"
        for window_id in sorted(selected_ids)
    ]
    semantic_path = root / "artifacts/gold-semantic-review-dev.json"
    atomicity_path = root / "artifacts/gold-atomicity-adjudication-dev.json"
    input_hashes = {
        "manifest_file_sha256": _file_sha256(manifest_path),
        "gold_selected_tree_sha256": _tree_sha256(gold_paths, root=root),
        "audit_selected_tree_sha256": _tree_sha256(audit_paths, root=root),
        "decisions_tree_sha256": _tree_sha256(
            list((root / "private-gpt55-disagreement-review").glob("*.output.json")),
            root=root,
        ),
        "raw_audit_receipt_sha256": _file_sha256(raw_audit_path),
        "semantic_review_receipt_sha256": _file_sha256(semantic_path),
        "atomicity_review_receipt_sha256": _file_sha256(atomicity_path),
        "case_descriptors_sha256": hashlib.sha256(
            json.dumps(cases, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    }
    adjudicated = evaluate_adjudicated_gold_reliability(
        gold_event_denominator=int(raw_audit["critical_errors"]["event_denominator"]),
        cases=cases,
        decisions=decisions,
        raw_audit_receipt=raw_audit,
        input_hashes=input_hashes,
    )
    output_path = root / "artifacts/gold-audit-dev-adjudicated.json"
    output_path.write_text(json.dumps(adjudicated, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"adjudication": receipt, "audit": adjudicated}, indent=2, sort_keys=True))
    return 0 if adjudicated["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
