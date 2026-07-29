from __future__ import annotations

"""Zero-token recovery for the immutable v110 Sol reference audit."""

import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

from .app_server_judge_v5 import normalize_neutral_alignment_output
from .app_server_judge_v5_calibration_v26_diagnostic import (
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from . import app_server_judge_v5_calibration_v108_layered_diagnostic as v108
from . import app_server_judge_v5_calibration_v110_sol_reference_audit as v110
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _validate_usage
from .util import now_iso


V111_AUDIT_VERSION = "pif_app_server_judge_v5_4_v111_sol_reference_recovery_audit_v1"
V111_SCORE_VERSION = "pif_app_server_judge_v5_4_v111_sol_reference_recovery_score_v1"
V111_TAXONOMY_VERSION = "pif_app_server_judge_v5_4_v111_sol_reference_taxonomy_v1"
V111_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v111_terminal_v1"
DEFAULT_V110_ROOT = v110.DEFAULT_OUTPUT_ROOT
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V110_ROOT.parent / "judge-calibration-v5_4-v111-sol-reference-recovery"
).resolve()


class JudgeV5CalibrationV111Error(RuntimeError):
    """The immutable v110 evidence cannot be recovered without semantic replay."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _verify_nested_records(value: Any) -> bool:
    if isinstance(value, Mapping):
        if set(value) == {"path", "sha256", "size_bytes"}:
            return _verify_record(value)
        return all(_verify_nested_records(item) for item in value.values())
    if isinstance(value, list):
        return all(_verify_nested_records(item) for item in value)
    return True


def _validate_v110(v110_root: Path = DEFAULT_V110_ROOT) -> dict[str, Any]:
    root = v110_root.expanduser().resolve()
    paths = {
        "spec": root / "sol-reference-audit-spec.json",
        "selection": root / "owner-audit-selection.json",
        "capacity_audit": root / "capacity-policy-audit.json",
        "capacity_policy": root / "capacity-policy.json",
        "terminal": root / "terminal.json",
        "failure": root / "failure.json",
        "pointwise_checklist": root / "sol-pointwise-checklist-full.private.json",
        "pointwise_output": root / "sol-pointwise-output-full.private.json",
        "support": root / "sol-support-receipts.private.json",
        "alignment_output": root / "sol-alignment-output.private.json",
    }
    values = {name: _load_json(path, f"v110 {name}") for name, path in paths.items()}
    terminal, failure, spec = values["terminal"], values["failure"], values["spec"]
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage_status") != "complete"
        or terminal.get("reference_frozen") is not False
        or terminal.get("fresh_full_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or failure.get("classification") != "infrastructure_or_judge_attempt_failed"
        or failure.get("error_class") != "KeyError"
        or failure.get("failed_turn_name") != "sol_alignment_owner_shard_01"
        or failure.get("accounting_complete") is not True
        or failure.get("usage_status") != "complete"
        or failure.get("unknown_usage_turn_count") != 0
        or failure.get("retry_allowed_in_this_version") is not False
        or spec.get("model") != v110.MODEL
        or spec.get("reasoning_effort") != v110.EFFORT
        or spec.get("maximum_turn_count") != 5
        or spec.get("retry_count_per_turn") != 0
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV111Error("v110 terminal contract drifted")

    if not _verify_nested_records(spec.get("predecessor") or {}):
        raise JudgeV5CalibrationV111Error("v110 predecessor record drifted")
    if not _verify_nested_records(spec.get("frozen_inputs") or {}):
        raise JudgeV5CalibrationV111Error("v110 frozen input record drifted")
    if not _verify_nested_records(spec.get("runtime_files") or []):
        raise JudgeV5CalibrationV111Error("v110 runtime record drifted")

    turn_records: dict[str, dict[str, Any]] = {}
    usage = {field: 0 for field in USAGE_FIELDS}
    for turn_name in v110.TURN_NAMES:
        turn_root = root / "turns" / turn_name.replace("_", "-")
        turn_paths = {
            name: turn_root / filename
            for name, filename in {
                "capacity": "capacity.json",
                "input": "input.private.json",
                "prompt": "prompt.private.md",
                "schema": "schema.json",
                "sidecar": "sidecar.json",
                "output": "output.private.json",
            }.items()
        }
        if any(not path.is_file() for path in turn_paths.values()):
            raise JudgeV5CalibrationV111Error("v110 turn coverage is incomplete")
        sidecar = _load_json(turn_paths["sidecar"], "v110 sidecar")
        measured = _validate_usage(sidecar)
        if (
            sidecar.get("state") != "completed"
            or sidecar.get("usage_status") != "measured"
            or sidecar.get("usage_complete") is not True
        ):
            raise JudgeV5CalibrationV111Error("v110 sidecar is not completed and measured")
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        turn_records[turn_name] = {name: _record(path) for name, path in turn_paths.items()}
    if usage != terminal.get("usage") or usage != failure.get("usage"):
        raise JudgeV5CalibrationV111Error("v110 usage aggregate drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "turn_records": turn_records,
        "usage": usage,
        "predecessor": v110._validate_predecessor(),
    }


def normalize_v110_alignment(v110_evidence: Mapping[str, Any]) -> dict[str, Any]:
    cases = []
    root = Path(v110_evidence["root"])
    for turn_name in v110.ALIGNMENT_TURNS:
        turn_root = root / "turns" / turn_name.replace("_", "-")
        alignment_input = _load_json(turn_root / "input.private.json", "v110 alignment input")
        output = _load_json(turn_root / "output.private.json", "v110 alignment output")
        cases.extend(normalize_neutral_alignment_output(output, alignment_input)["cases"])
    if len(cases) != 12 or len({str(row["case_id"]) for row in cases}) != 12:
        raise JudgeV5CalibrationV111Error("v110 normalized alignment coverage drifted")
    return {
        "schema_version": "pif_app_server_judge_v5_4_v111_normalized_sol_alignment_v1",
        "cases": sorted(cases, key=lambda row: str(row["case_id"])),
        "mismatch_fields_projected_from_checklists": True,
        "origin_neutral": True,
    }


def build_sanitized_taxonomy(
    *, predecessor: Mapping[str, Any], sol_pointwise: Mapping[str, Any]
) -> dict[str, Any]:
    truth = predecessor["values"]["truth"]
    gpt_rows = {
        str(row["witness_id"]): row
        for row in predecessor["values"]["pointwise_output"]["units"]
    }
    sol_rows = {str(row["witness_id"]): row for row in sol_pointwise["units"]}
    witness_to_case = {
        witness_id: case_id
        for case_id, case in truth["cases"].items()
        for witness_id in case["proposition"]
    }
    counts = Counter()
    components = Counter()
    field_extra = Counter()
    field_missed = Counter()
    control_miss_shapes = Counter()
    for witness_id, gpt_row in gpt_rows.items():
        case_id = witness_to_case[witness_id]
        expected = v110._truth_pointwise_tuple(truth, case_id, witness_id)
        gpt = v110._pointwise_tuple(gpt_row)
        sol = v110._pointwise_tuple(sol_rows[witness_id])
        cohort = "control" if gpt == expected else "candidate"
        counts[(cohort, "total")] += 1
        counts[(cohort, "sol_truth_exact")] += int(sol == expected)
        counts[(cohort, "sol_gpt55_exact")] += int(sol == gpt)
        components[(cohort, "proposition_match_truth")] += int(sol[0] == expected[0])
        components[(cohort, "structured_match_truth")] += int(sol[1] == expected[1])
        components[(cohort, "field_set_match_truth")] += int(sol[2] == expected[2])
        for field in set(sol[2]) - set(expected[2]):
            field_extra[field] += 1
        for field in set(expected[2]) - set(sol[2]):
            field_missed[field] += 1
        if cohort == "control" and sol != expected:
            control_miss_shapes[str(truth["cases"][case_id]["shape"])] += 1
    return {
        "schema_version": V111_TAXONOMY_VERSION,
        "created_at": now_iso(),
        "sanitized": True,
        "cohorts": {
            cohort: {
                "counts": {
                    key: counts[(cohort, key)]
                    for key in ("total", "sol_truth_exact", "sol_gpt55_exact")
                },
                "component_matches": {
                    key: components[(cohort, key)]
                    for key in (
                        "proposition_match_truth",
                        "structured_match_truth",
                        "field_set_match_truth",
                    )
                },
            }
            for cohort in ("control", "candidate")
        },
        "control_miss_shapes": dict(sorted(control_miss_shapes.items())),
        "sol_extra_fields_vs_reference": dict(sorted(field_extra.items())),
        "sol_missed_fields_vs_reference": dict(sorted(field_missed.items())),
        "root_cause": "pointwise_structured_field_truth_or_minimal_root_rubric_disagreement",
        "proposition_support_consensus_count": 53,
        "private_content_exposed": False,
    }


def freeze_v111(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, v110_root: Path = DEFAULT_V110_ROOT
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v111 terminal")}
    evidence = _validate_v110(v110_root)
    normalized = normalize_v110_alignment(evidence)
    normalized_path = root / "sol-alignment-normalized.private.json"
    _write_immutable(normalized_path, normalized)
    pointwise = evidence["values"]["pointwise_output"]
    owner_score = v110.score_owner_audit(
        predecessor=evidence["predecessor"],
        sol_pointwise=pointwise,
        sol_alignment=normalized,
        alignment_selection=evidence["values"]["selection"],
    )
    owner_score = {**owner_score, "schema_version": V111_SCORE_VERSION}
    owner_score_path = root / "owner-audit-score.json"
    _write_immutable(owner_score_path, owner_score)
    truth_candidate = v110.apply_authorized_patch(
        predecessor=evidence["predecessor"],
        sol_pointwise=pointwise,
        sol_alignment=normalized,
        owner_score=owner_score,
    )
    truth_candidate_path = root / "calibration-truth-v9-candidate.private.json"
    _write_immutable(truth_candidate_path, truth_candidate)
    gpt_score = v108.score_v108(
        pointwise_output=evidence["predecessor"]["values"]["pointwise_output"],
        reconciled_alignment=evidence["predecessor"]["values"]["alignment"],
        expected=truth_candidate,
        observable_disagreements=evidence["predecessor"]["values"]["disagreements"],
    )
    gpt_score_path = root / "gpt55-score-against-v9-candidate.json"
    _write_immutable(gpt_score_path, gpt_score)
    taxonomy = build_sanitized_taxonomy(
        predecessor=evidence["predecessor"], sol_pointwise=pointwise
    )
    taxonomy_path = root / "sanitized-owner-disagreement-taxonomy.json"
    _write_stable_time(taxonomy_path, taxonomy, "created_at")
    audit = {
        "schema_version": V111_AUDIT_VERSION,
        "created_at": now_iso(),
        "v110_failure_root_cause": "raw_neutral_alignment_pair_shape_was_scored_as_normalized_pair_shape",
        "normalization": "existing_validated_neutral_alignment_projector_applied_per_original_shard",
        "v110": evidence["records"],
        "v110_turns": evidence["turn_records"],
        "v110_usage": evidence["usage"],
        "new_semantic_turn_count": 0,
        "new_usage": {field: 0 for field in USAGE_FIELDS},
        "owner_checks": owner_score["checks"],
        "reference_patch_authorized": owner_score["reference_patch_authorized"],
        "production_mutated": False,
    }
    audit_path = root / "v110-recovery-audit.json"
    _write_stable_time(audit_path, audit, "created_at")
    full_authorized = bool(owner_score["reference_patch_authorized"] and gpt_score["passed"])
    terminal = {
        "schema_version": V111_TERMINAL_VERSION,
        "state": "completed" if full_authorized else "inactive",
        "terminal_at": now_iso(),
        "terminal_reason": (
            "v111_reference_v9_frozen_fresh_full_calibration_authorized"
            if full_authorized
            else "inactive_incomplete_recovery_required"
        ),
        "development_terminal_reason": (
            "v111_sol_owner_control_and_reconciliation_passed"
            if full_authorized
            else "v110_owner_control_gate_not_passed_after_deterministic_schema_normalization"
        ),
        "overall_evaluation_complete": False,
        "reference_patch_authorized": bool(owner_score["reference_patch_authorized"]),
        "reference_frozen": bool(owner_score["reference_patch_authorized"]),
        "fresh_full_calibration_authorized": full_authorized,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_attempt_started": False,
        "semantic_retry_count": 0,
        "usage_status": "complete",
        "accounting_complete": True,
        "usage": {field: 0 for field in USAGE_FIELDS},
        "predecessor_v110_usage": evidence["usage"],
        "owner_score": _record(owner_score_path),
        "truth_candidate": _record(truth_candidate_path),
        "reconciled_gpt55_score": _record(gpt_score_path),
        "recovery_audit": _record(audit_path),
        "sanitized_taxonomy": _record(taxonomy_path),
        "owner_checks": owner_score["checks"],
        "owner_metrics": {
            key: value
            for key, value in owner_score.items()
            if key.endswith("count") or key.endswith("rate")
        },
        "failed_quality_gates": gpt_score["failed_checks"],
        "metrics": gpt_score["metrics"],
        "next_experiment_required": "independent_minimal_root_field_reference_owner_diagnostic",
    }
    _write_stable_time(terminal_path, terminal, "terminal_at")
    return {"root": root, "terminal": terminal, "owner_score": owner_score, "taxonomy": taxonomy}


def main() -> int:
    result = freeze_v111()
    terminal = result["terminal"]
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "reference_frozen": terminal["reference_frozen"],
                "fresh_full_calibration_authorized": terminal[
                    "fresh_full_calibration_authorized"
                ],
                "new_semantic_turn_count": 0,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
