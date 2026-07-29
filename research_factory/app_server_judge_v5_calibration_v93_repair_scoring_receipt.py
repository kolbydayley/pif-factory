from __future__ import annotations

"""Correctly separate v91 canary stability from the v92 repair result."""

import argparse
import json
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_judge_v5_calibration_v26_diagnostic import _load_json, _write_immutable
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record_matches,
    _verify_record,
)
from .app_server_judge_v5_calibration_v91_fresh_luna_diagnostic import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V91_ROOT,
)
from .app_server_judge_v5_calibration_v92_speaker_repair import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V92_ROOT,
)
from .app_server_judge_v5_diagnostic import _record
from .util import now_iso


V93_SCORE_VERSION = "pif_app_server_judge_v5_4_v93_repair_scoring_v1"
V93_AUDIT_VERSION = "pif_app_server_judge_v5_4_v93_repair_scoring_audit_v1"
V93_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v93_terminal_v1"
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V92_ROOT.parent / "judge-calibration-v5_4-v93-repair-scoring-receipt"
).resolve()


class JudgeV5CalibrationV93Error(RuntimeError):
    """The v93 repair scoring receipt cannot preserve its predecessors."""


def _validate_predecessors(*, v92_root: Path, v91_root: Path) -> dict[str, Any]:
    paths = {
        "v92_terminal": v92_root / "terminal.json",
        "v92_spec": v92_root / "speaker-repair-spec.json",
        "v92_score": v92_root / "speaker-repair-score.json",
        "v92_repair": v92_root / "speaker-repair-output.private.json",
        "v92_reconciled": v92_root / "reconciled-output.private.json",
        "v92_audit": v92_root / "reconciliation-audit.json",
        "v91_terminal": v91_root / "terminal.json",
        "v91_spec": v91_root / "fresh-luna-spec.json",
        "v91_score": v91_root / "fresh-luna-score.json",
        "v91_output": v91_root / "fresh-luna-output.private.json",
        "v91_canary": v91_root / "permutation-canary-output.private.json",
        "v91_truth": v91_root / "fresh-luna-truth.private.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    t92 = values["v92_terminal"]
    sp92 = values["v92_spec"]
    s92 = values["v92_score"]
    a92 = values["v92_audit"]
    t91 = values["v91_terminal"]
    sp91 = values["v91_spec"]
    s91 = values["v91_score"]
    repair_rows = values["v92_repair"].get("decisions") or []
    if (
        t92.get("state") != "inactive"
        or t92.get("repair_completed") is not True
        or t92.get("diagnostic_passed") is not False
        or t92.get("usage_status") != "complete"
        or t92.get("accounting_complete") is not True
        or t92.get("semantic_retry_count") != 0
        or t92.get("production_mutated") is not False
        or len(repair_rows) != 1
        or repair_rows[0].get("field_status") != "incorrect"
        or not repair_rows[0].get("source_evidence_spans")
        or a92.get("residual_mismatch_count") != 2
        or sorted(row.get("field") for row in a92.get("residual_mismatches") or [])
        != ["certainty", "target"]
        or s92.get("metrics", {}).get("exact_count") != 13
        or s92.get("metrics", {}).get("permutation_canary_exact_count") != 3
        or not _record_matches(t92.get("score"), paths["v92_score"])
        or not _record_matches(t92.get("repair_output"), paths["v92_repair"])
        or not _record_matches(t92.get("reconciled_output"), paths["v92_reconciled"])
        or not _record_matches(t92.get("reconciliation_audit"), paths["v92_audit"])
        or not all(_verify_record(row) for row in sp92.get("runtime_files") or [])
        or t91.get("state") != "inactive"
        or t91.get("bounded_observable_repair_authorized") is not True
        or t91.get("usage_status") != "complete"
        or t91.get("production_mutated") is not False
        or s91.get("metrics", {}).get("permutation_canary_exact_count") != 4
        or s91.get("metrics", {}).get("observable_repair_trigger_count") != 1
        or not _record_matches(t91.get("score"), paths["v91_score"])
        or not _record_matches(t91.get("output"), paths["v91_output"])
        or not _record_matches(t91.get("canary_output"), paths["v91_canary"])
        or not _record_matches(sp91.get("frozen_inputs", {}).get("truth"), paths["v91_truth"])
        or not all(_verify_record(row) for row in sp91.get("runtime_files") or [])
    ):
        raise JudgeV5CalibrationV93Error("v91/v92 repair-scoring contract drifted")
    return {
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
    }


def build_v93_score(predecessor: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    values = predecessor["values"]
    truth = {row["task_id"]: row for row in values["v91_truth"]["tasks"]}
    final = {row["task_id"]: row for row in values["v92_reconciled"]["decisions"]}
    if set(truth) != set(final):
        raise JudgeV5CalibrationV93Error("v93 final primary coverage drifted")
    incorrect = [row for row in truth.values() if row["expected_status"] == "incorrect"]
    correct = [row for row in truth.values() if row["expected_status"] == "correct"]
    exact = sum(final[task_id]["field_status"] == row["expected_status"] for task_id, row in truth.items())
    incorrect_exact = sum(final[row["task_id"]]["field_status"] == "incorrect" for row in incorrect)
    correct_exact = sum(final[row["task_id"]]["field_status"] == "correct" for row in correct)
    primary_abstentions = sum(row["field_status"] == "abstain" for row in final.values())
    primary_evidence = sum(bool(row["source_evidence_spans"]) for row in final.values())
    original_canary = values["v91_score"]["metrics"]
    residuals = values["v92_audit"]["residual_mismatches"]
    trigger_cleared = (
        primary_abstentions == 0
        and primary_evidence == 15
        and len(residuals) == 2
        and original_canary["permutation_canary_exact_count"] == 4
    )
    score = {
        "schema_version": V93_SCORE_VERSION,
        "passed": exact == 15,
        "metrics": {
            "task_count": 15,
            "incorrect_count": len(incorrect),
            "correct_count": len(correct),
            "repaired_exact_count": exact,
            "repaired_exact_rate": round(exact / 15, 6),
            "repaired_incorrect_sensitivity": round(incorrect_exact / len(incorrect), 6),
            "repaired_correct_specificity": round(correct_exact / len(correct), 6),
            "repaired_primary_abstention_count": primary_abstentions,
            "repaired_primary_evidence_complete_count": primary_evidence,
            "original_permutation_canary_exact_count": original_canary[
                "permutation_canary_exact_count"
            ],
            "original_permutation_canary_exact_rate": original_canary[
                "permutation_canary_exact_rate"
            ],
            "original_canary_abstention_count": original_canary["canary_abstention_count"],
            "residual_mismatch_count": len(residuals),
        },
        "observable_repair_trigger_cleared": trigger_cleared,
        "residual_reference_audit_authorized": trigger_cleared and 1 <= len(residuals) <= 3,
        "fresh_full_development_calibration_authorized": exact == 15,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "scoring_contract": (
            "original primary/canary pair measures pre-repair stability; the stronger repair supersedes the "
            "triggered primary decision and is scored separately without comparing it to the old canary"
        ),
    }
    audit = {
        "schema_version": V93_AUDIT_VERSION,
        "created_at": now_iso(),
        "old_repaired_vs_old_canary_exact_count": values["v92_score"]["metrics"][
            "permutation_canary_exact_count"
        ],
        "original_pre_repair_canary_exact_count": original_canary[
            "permutation_canary_exact_count"
        ],
        "repair_decision_status": values["v92_repair"]["decisions"][0]["field_status"],
        "repair_has_exact_evidence": bool(
            values["v92_repair"]["decisions"][0]["source_evidence_spans"]
        ),
        "residual_mismatches": residuals,
        "semantic_decisions_changed_by_deterministic_code": False,
        "majority_voting_used": False,
        "privacy": "opaque_task_ids_field_enums_statuses_and_counts_only",
    }
    return score, audit


def freeze_v93(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v92_root: Path = DEFAULT_V92_ROOT,
    v91_root: Path = DEFAULT_V91_ROOT,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v93 terminal")
    predecessor = _validate_predecessors(v92_root=v92_root.resolve(), v91_root=v91_root.resolve())
    score, audit = build_v93_score(predecessor)
    if (
        score["observable_repair_trigger_cleared"] is not True
        or score["residual_reference_audit_authorized"] is not True
        or score["fresh_full_development_calibration_authorized"] is not False
    ):
        raise JudgeV5CalibrationV93Error("v93 corrected scoring gate did not authorize audit")
    score_path = root / "repair-scoring.json"
    audit_path = root / "repair-scoring-audit.json"
    _write_immutable(score_path, score)
    _write_immutable(audit_path, audit)
    terminal = {
        "schema_version": V93_TERMINAL_VERSION,
        "state": "completed",
        "terminal_at": now_iso(),
        "terminal_reason": "v93_repair_trigger_cleared_residual_reference_audit_authorized",
        "overall_evaluation_complete": False,
        "observable_repair_trigger_cleared": True,
        "residual_reference_audit_authorized": True,
        "fresh_full_development_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_attempt_started": False,
        "semantic_retry_count": 0,
        "accounting_complete": True,
        "usage_status": "complete",
        "usage": {
            "input_tokens": 0,
            "cached_input_tokens": 0,
            "output_tokens": 0,
            "reasoning_output_tokens": 0,
            "total_tokens": 0,
        },
        "score": _record(score_path),
        "audit": _record(audit_path),
        "predecessor": predecessor["records"],
    }
    _write_immutable(terminal_path, terminal)
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze corrected v93 repair scoring")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v93(output_dir=Path(args.output_dir))
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "residual_reference_audit_authorized": terminal[
                    "residual_reference_audit_authorized"
                ],
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
