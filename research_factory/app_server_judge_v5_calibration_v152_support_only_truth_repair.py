from __future__ import annotations

"""Repair the v150 support-only truth projection without model calls."""

import argparse
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v130_retained_alignment_owner as v130
from . import app_server_judge_v5_calibration_v150_fresh_alignment_diagnostic as v150
from . import app_server_judge_v5_calibration_v151_alignment_canary_recovery as v151
from .app_server_judge_v5_calibration_v26_diagnostic import _load_json, _write_immutable
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _validate_usage
from .util import now_iso


V152_TRUTH_VERSION = "pif_app_server_judge_v5_4_v152_support_only_truth_v1"
V152_AUDIT_VERSION = "pif_app_server_judge_v5_4_v152_truth_patch_audit_v1"
V152_PLAN_VERSION = "pif_app_server_judge_v5_4_v152_alignment_adjudication_plan_v1"
V152_SCORE_VERSION = "pif_app_server_judge_v5_4_v152_corrected_score_v1"
V152_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v152_terminal_v1"
DEFAULT_OUTPUT_ROOT = (
    v151.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v152-support-only-truth-repair"
).resolve()


class JudgeV5CalibrationV152Error(RuntimeError):
    """The immutable v151 truth-repair contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v151() -> dict[str, Any]:
    root = v151.DEFAULT_OUTPUT_ROOT
    paths = {
        "spec": root / "alignment-canary-recovery-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "fresh-alignment-score.json",
        "primary": root / "reused-primary-normalized.private.json",
        "canary": root / "alignment-canary-normalized.private.json",
        "raw": root / "alignment-canary-raw.private.json",
        "projection_audit": root / "exact-span-projection-audit.json",
    }
    values = {name: _load_json(path, f"v151 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    expected_usage = {
        "input_tokens": 51004,
        "cached_input_tokens": 0,
        "output_tokens": 24094,
        "reasoning_output_tokens": 4962,
        "total_tokens": 75098,
    }
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v151_alignment_quality_gate_not_passed"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage_status") != "complete"
        or terminal.get("usage") != expected_usage
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("production_mutated") is not False
        or terminal.get("fresh_full_development_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("v150_primary_turns_replayed") is not False
        or score.get("passed") is not False
        or score.get("failed_checks")
        != [
            "alignment_f1",
            "equivalence_partition_exact_case_rate",
            "order_bias",
            "permutation_canary_exact_rate",
            "unpaired_exact_case_rate",
        ]
        or spec.get("turn_plan") != list(v151.TURN_NAMES)
        or spec.get("replayed_primary_turn_count") != 0
        or spec.get("retry_count_per_turn") != 0
    ):
        raise JudgeV5CalibrationV152Error("v151 terminal contract drifted")
    if (
        terminal.get("score") != _record(paths["score"])
        or terminal.get("primary_output") != _record(paths["primary"])
        or terminal.get("canary_output") != _record(paths["canary"])
        or terminal.get("raw_outputs") != _record(paths["raw"])
        or terminal.get("projection_audit") != _record(paths["projection_audit"])
        or not all(_verify_record(record) for record in spec.get("runtime_files") or [])
    ):
        raise JudgeV5CalibrationV152Error("v151 artifact record drifted")

    attempts = {}
    usage = {field: 0 for field in USAGE_FIELDS}
    for turn_name in v151.TURN_NAMES:
        turn_root = root / "turns" / turn_name.replace("_", "-")
        records = {
            name: _record(turn_root / filename)
            for name, filename in {
                "capacity": "capacity.json",
                "sidecar": "sidecar.json",
                "output": "output.private.json",
            }.items()
        }
        if any(not _verify_record(record) for record in records.values()):
            raise JudgeV5CalibrationV152Error("v151 attempt coverage drifted")
        measured = _validate_usage(
            _load_json(Path(records["sidecar"]["path"]), "v151 sidecar")
        )
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        attempts[turn_name] = records
    if usage != expected_usage:
        raise JudgeV5CalibrationV152Error("v151 usage aggregate drifted")

    source = v151._validate_v150_failure()
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "usage": usage,
        "source": source,
    }


def repair_support_only_truth(source: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    truth = deepcopy(source["source"]["truth"])
    reference = source["source"]["source"]["v148"]["values"]["reference"]
    changes = []
    for row in truth["cases"]:
        case_id = str(row["case_id"])
        supported = {
            witness_id
            for witness_id, verdict in reference["cases"][case_id]["proposition"].items()
            if verdict == "supported"
        }
        expected = row["expected"]
        old_groups = deepcopy(expected["equivalence_groups"])
        old_unpaired = list(expected["unpaired_witness_ids"])
        expected["equivalence_groups"] = [
            sorted(set(group) & supported)
            for group in old_groups
            if set(group) & supported
        ]
        paired = {
            witness_id
            for pair in expected["pairs"]
            for witness_id in pair["witness_ids"]
        }
        if not paired <= supported:
            raise JudgeV5CalibrationV152Error("v152 truth pair references hidden witness")
        expected["unpaired_witness_ids"] = sorted(supported - paired)
        if (
            old_groups != expected["equivalence_groups"]
            or old_unpaired != expected["unpaired_witness_ids"]
        ):
            changes.append(
                {
                    "case_id": case_id,
                    "removed_hidden_group_id_count": sum(map(len, old_groups))
                    - sum(map(len, expected["equivalence_groups"])),
                    "removed_hidden_unpaired_id_count": len(old_unpaired)
                    - len(expected["unpaired_witness_ids"]),
                }
            )
    truth["schema_version"] = V152_TRUTH_VERSION
    truth["unpaired_case_count"] = sum(
        bool(row["expected"]["unpaired_witness_ids"]) for row in truth["cases"]
    )
    if (
        len(changes) != 3
        or any(
            row["removed_hidden_group_id_count"] != 1
            or row["removed_hidden_unpaired_id_count"] != 1
            for row in changes
        )
        or truth["unpaired_case_count"] != 3
    ):
        raise JudgeV5CalibrationV152Error("v152 support-only truth patch drifted")
    audit = {
        "schema_version": V152_AUDIT_VERSION,
        "created_at": now_iso(),
        "defect_class": "truth_projection_retained_witnesses_hidden_from_model_input",
        "patched_case_count": len(changes),
        "removed_hidden_group_id_count": sum(
            row["removed_hidden_group_id_count"] for row in changes
        ),
        "removed_hidden_unpaired_id_count": sum(
            row["removed_hidden_unpaired_id_count"] for row in changes
        ),
        "semantic_model_decisions_changed": False,
        "source_text_used_for_patch": False,
        "patch_rule": "restrict_expected_partition_and_unpaired_ids_to_frozen_supported_witnesses",
        "changes": sorted(changes, key=lambda row: row["case_id"]),
    }
    return truth, audit


def build_v152_plan(
    *, primary: Mapping[str, Any], canary: Mapping[str, Any], truth: Mapping[str, Any]
) -> dict[str, Any]:
    expected = {str(row["case_id"]): row for row in truth["cases"]}
    primary_rows = {str(row["case_id"]): row for row in primary["cases"]}
    canary_rows = {str(row["case_id"]): row for row in canary["cases"]}
    disagreements = []
    for case_id in sorted(expected):
        primary_owner = v130._owner_projection(primary_rows[case_id])
        canary_owner = v130._owner_projection(canary_rows[case_id])
        if primary_owner == canary_owner:
            continue
        disagreements.append(
            {
                "case_id": case_id,
                "relation_bucket": expected[case_id]["relation_bucket"],
                "shape": expected[case_id]["shape"],
                "trigger": "observable_permutation_output_changed",
                "primary_truth_exact": v130._project_alignment(primary_rows[case_id])
                == expected[case_id]["expected"],
                "canary_truth_exact": v130._project_alignment(canary_rows[case_id])
                == expected[case_id]["expected"],
            }
        )
    bucket_counts: dict[str, int] = {}
    shape_counts: dict[str, int] = {}
    for row in disagreements:
        bucket_counts[row["relation_bucket"]] = bucket_counts.get(row["relation_bucket"], 0) + 1
        shape_counts[row["shape"]] = shape_counts.get(row["shape"], 0) + 1
    if (
        len(disagreements) != 4
        or bucket_counts != {"non_equivalent": 2, "partial": 2}
        or shape_counts
        != {"single_event_pairs": 2, "merged_and_split_boundaries": 2}
    ):
        raise JudgeV5CalibrationV152Error("v152 disagreement isolation drifted")
    return {
        "schema_version": V152_PLAN_VERSION,
        "created_at": now_iso(),
        "observable_disagreement_case_count": len(disagreements),
        "relation_bucket_counts": dict(sorted(bucket_counts.items())),
        "shape_counts": dict(sorted(shape_counts.items())),
        "cases": disagreements,
        "adjudication_strategy": "one_capped_side_free_independent_re_evaluation",
        "anonymous_prior_candidates_are_diagnostic_not_votes": True,
        "majority_voting_used": False,
        "call_cap": 1,
        "retry_count_per_turn": 0,
        "fresh_full_development_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }


def run_v152(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v152 terminal")
    source = _validate_v151()
    truth, audit = repair_support_only_truth(source)
    primary = source["values"]["primary"]
    canary = source["values"]["canary"]
    score = v150.score_v150(primary=primary, canary=canary, truth=truth)
    expected_metrics = {
        "alignment_f1": 0.928571,
        "equivalence_partition_exact_case_rate": 1.0,
        "unpaired_exact_case_rate": 0.833333,
        "permutation_canary_exact_count": 8,
        "order_bias": 0.333333,
        "exact_case_rate": 0.833333,
    }
    if any(score["metrics"].get(key) != value for key, value in expected_metrics.items()):
        raise JudgeV5CalibrationV152Error("v152 corrected score drifted")
    plan = build_v152_plan(primary=primary, canary=canary, truth=truth)
    score = dict(score)
    score["schema_version"] = V152_SCORE_VERSION
    truth_path = root / "calibration-truth-v14-support-only.private.json"
    audit_path = root / "support-only-truth-patch-audit.json"
    score_path = root / "corrected-alignment-score.json"
    plan_path = root / "capped-alignment-adjudication-plan.json"
    _write_immutable(truth_path, truth)
    _write_stable_time(audit_path, audit, "created_at")
    _write_immutable(score_path, score)
    _write_stable_time(plan_path, plan, "created_at")
    zero_usage = {field: 0 for field in USAGE_FIELDS}
    terminal = {
        "schema_version": V152_TERMINAL_VERSION,
        "state": "inactive",
        "terminal_at": now_iso(),
        "terminal_reason": "inactive_incomplete_recovery_required",
        "development_terminal_reason": "v152_support_only_truth_repaired_capped_adjudication_required",
        "overall_evaluation_complete": False,
        "reference_truth_repaired": True,
        "reference_truth_frozen_for_next_attempt": True,
        "capped_side_free_alignment_adjudication_authorized": True,
        "fresh_full_development_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_attempt_started": False,
        "semantic_retry_count": 0,
        "usage_status": "complete",
        "accounting_complete": True,
        "usage": zero_usage,
        "predecessor_v151_usage": source["usage"],
        "predecessor_v150_usage": source["values"]["terminal"]["predecessor_v150_usage"],
        "truth": _record(truth_path),
        "truth_patch_audit": _record(audit_path),
        "corrected_score": _record(score_path),
        "adjudication_plan": _record(plan_path),
        "predecessor_v151_terminal": source["records"]["terminal"],
        "predecessor_v151_score": source["records"]["score"],
    }
    _write_immutable(terminal_path, terminal)
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Repair v150 support-only truth without model calls")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = run_v152(output_dir=Path(args.output_dir))
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "reference_truth_repaired": terminal["reference_truth_repaired"],
                "capped_side_free_alignment_adjudication_authorized": terminal[
                    "capped_side_free_alignment_adjudication_authorized"
                ],
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
