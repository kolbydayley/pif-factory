from __future__ import annotations

"""Side-free Sol owner for the six retained alignment disputes."""

import argparse
import asyncio
import json
import math
from collections import Counter
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .app_server_judge_v5 import (
    CHECKLIST_FIELDS,
    build_neutral_alignment_input,
    build_neutral_alignment_prompt,
    neutral_alignment_base_instructions,
    neutral_alignment_output_schema,
    normalize_neutral_alignment_output,
    validate_pointwise_support_output,
)
from .app_server_judge_v5_calibration_v25_diagnostic import QUOTA_POINTS_PER_MILLION_TOKENS
from .app_server_judge_v5_calibration_v26_diagnostic import (
    MAX_TOKENS_PER_TURN,
    JudgeV5CalibrationV26DiagnosticAttemptFailed,
    _aggregate_usage,
    _client_factory,
    _freeze_turn_request,
    _get_or_run_turn,
    _load_json,
    _validate_scoreable_alignment_output,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import _record, _verify_record
from .app_server_judge_v5_calibration_v107_recovery_receipt import (
    _validate_v106 as _validate_v106_full,
)
from .app_server_judge_v5_calibration_v110_sol_reference_audit import (
    _project_alignment,
    _truth_alignment,
)
from .app_server_judge_v5_calibration_v121_retained_field_owner import _validate_v120
from .app_server_judge_v5_calibration_v129_singleton_proposition_repair import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V129_ROOT,
    _validate_v128,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V130_RECEIPT_VERSION = "pif_app_server_judge_v5_4_v130_support_receipts_v1"
V130_TRUTH_VERSION = "pif_app_server_judge_v5_4_v130_alignment_owner_truth_v1"
V130_SELECTION_VERSION = "pif_app_server_judge_v5_4_v130_selection_v1"
V130_SPEC_VERSION = "pif_app_server_judge_v5_4_v130_spec_v1"
V130_SCORE_VERSION = "pif_app_server_judge_v5_4_v130_score_v1"
V130_REFERENCE_VERSION = (
    "pif_app_server_judge_v5_4_calibration_truth_v10_retained_alignment_owner_frozen"
)
V130_FAILURE_VERSION = "pif_app_server_judge_v5_4_v130_failure_v1"
V130_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v130_terminal_v1"
V130_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V130_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V130_PHASE_ID = "judge_v5_4_v130_retained_alignment_owner"

MODEL = "gpt-5.6-sol"
EFFORT = "high"
PRIMARY_TURN = "retained_alignment_owner"
CANARY_TURN = "retained_alignment_owner_canary"
TURN_NAMES = (PRIMARY_TURN, CANARY_TURN)
CONTROL_COUNT = 6
DISPUTE_COUNT = 6
TIMEOUT_SECONDS = 1200.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V129_ROOT.parent / "judge-calibration-v5_4-v130-retained-alignment-owner"
).resolve()


class JudgeV5CalibrationV130Error(RuntimeError):
    """The v130 retained alignment-owner contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v129() -> dict[str, Any]:
    root = DEFAULT_V129_ROOT
    paths = {
        "spec": root / "singleton-proposition-repair-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "singleton-proposition-repair-score.json",
        "outputs": root / "singleton-proposition-repair-outputs.private.json",
        "final_owner": root / "final-proposition-owner-output.private.json",
        "reference": root / "calibration-truth-v10-retained-proposition-singleton.private.json",
        "truth": root / "singleton-proposition-repair-truth.private.json",
        "selection": root / "selection-audit.json",
    }
    values = {name: _load_json(path, f"v129 {name}") for name, path in paths.items()}
    terminal, score, spec, reference = (
        values["terminal"],
        values["score"],
        values["spec"],
        values["reference"],
    )
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v129_singleton_proposition_repair_passed_patch_authorized"
        or terminal.get("retained_proposition_reference_patch_authorized") is not True
        or terminal.get("proposition_reference_frozen") is not True
        or terminal.get("alignment_reference_frozen") is not False
        or terminal.get("fresh_diagnostic_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 62600
        or score.get("passed") is not True
        or score.get("failed_checks") != []
        or spec.get("model") != "gpt-5.6-terra"
        or spec.get("retry_count_per_turn") != 0
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
        or reference.get("proposition_reference_frozen") is not True
        or reference.get("alignment_reference_frozen") is not False
        or len(reference.get("cases") or {}) != 66
    ):
        raise JudgeV5CalibrationV130Error("v129 predecessor contract drifted")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5CalibrationV130Error("v129 runtime record drifted")
    for name, key in {
        "score": "score",
        "outputs": "outputs",
        "final_owner": "final_owner_output",
        "reference": "reference_candidate",
    }.items():
        record = terminal.get(key)
        if not isinstance(record, Mapping) or dict(record) != _record(paths[name]) or not _verify_record(record):
            raise JudgeV5CalibrationV130Error(f"v129 {name} record drifted")
    attempts = {}
    usage = {field: 0 for field in USAGE_FIELDS}
    for turn_name in spec["turn_plan"]:
        turn_root = root / "turns" / turn_name.replace("_", "-")
        records = {}
        for name, filename in {
            "capacity": "capacity.json",
            "sidecar": "sidecar.json",
            "output": "output.private.json",
        }.items():
            path = turn_root / filename
            if not path.is_file():
                raise JudgeV5CalibrationV130Error("v129 turn coverage is incomplete")
            records[name] = _record(path)
        measured = _validate_usage(_load_json(Path(records["sidecar"]["path"]), "v129 sidecar"))
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        attempts[turn_name] = records
    if usage != terminal["usage"]:
        raise JudgeV5CalibrationV130Error("v129 usage aggregate drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "usage": usage,
        "v128": _validate_v128(),
    }


def _load_v120_alignment_sources() -> dict[str, Any]:
    value = _validate_v120()
    terminal = value["values"]["terminal"]
    spec = value["values"]["spec"]
    records = {
        "support": terminal.get("support_receipts"),
        "pool": (spec.get("frozen_inputs") or {}).get("pool"),
    }
    if any(not isinstance(record, Mapping) or not _verify_record(record) for record in records.values()):
        raise JudgeV5CalibrationV130Error("v120 alignment source record drifted")
    value["support"] = _load_json(Path(records["support"]["path"]), "v120 support receipts")
    value["pool"] = _load_json(Path(records["pool"]["path"]), "v120 witness pool")
    value["alignment_source_records"] = {name: dict(record) for name, record in records.items()}
    return value


def _v126_and_v121(v129: Mapping[str, Any]) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    v126 = v129["v128"]["v127"]["v126"]
    v121 = v126["v125"]["v124"]["v122"]["v121"]
    return v126, v121


def _unique_exact_spans(spans: Sequence[Any], source: str, limit: int = 4) -> list[str]:
    result = []
    for span in spans:
        if isinstance(span, str) and span and span in source and span not in result:
            result.append(span)
        if len(result) == limit:
            break
    return result


def build_v130_support_receipts(
    v129: Mapping[str, Any], v120: Mapping[str, Any]
) -> dict[str, Any]:
    reference = v129["values"]["reference"]
    input_units = {
        str(row["witness_id"]): row for row in v120["values"]["pointwise_input"]["units"]
    }
    base = {str(row["witness_id"]): deepcopy(row) for row in v120["support"]["units"]}
    if set(base) != set(input_units) or len(base) != 52:
        raise JudgeV5CalibrationV130Error("v130 support receipt coverage drifted")

    migration_truth = {
        row["task_id"]: row
        for row in v129["v128"]["values"]["truth"]["tasks"]
        if row["role"] == "migration_owner"
    }
    migration_output = {
        row["task_id"]: row for row in v129["values"]["final_owner"]["decisions"]
    }
    proposition_owner = {
        row["witness_id"]: migration_output[task_id]
        for task_id, row in migration_truth.items()
    }
    if len(proposition_owner) != 6:
        raise JudgeV5CalibrationV130Error("v130 proposition owner coverage drifted")

    v126, v121 = _v126_and_v121(v129)
    field_truth = {row["task_id"]: row for row in v121["values"]["truth"]["tasks"]}
    field_output = {
        row["task_id"]: row for row in v126["values"]["reconciled"]["decisions"]
    }
    final_owner_source_ids = {
        str(row["source_task_id"])
        for row in v126["v125"]["values"]["truth"]["tasks"]
        if row["role"] == "final_owner_dispute"
    }
    projected_field_ids = {
        task_id
        for task_id, row in field_truth.items()
        if row["role"] != "matched_control" or task_id in final_owner_source_ids
    }
    if len(projected_field_ids) != 97:
        raise JudgeV5CalibrationV130Error("v130 projected field-owner coverage drifted")
    field_rows: dict[str, list[tuple[str, Mapping[str, Any]]]] = {}
    for task_id, truth_row in field_truth.items():
        if task_id not in projected_field_ids:
            continue
        decision = field_output.get(task_id)
        if decision is None:
            raise JudgeV5CalibrationV130Error("v130 final field owner coverage drifted")
        field_rows.setdefault(str(truth_row["witness_id"]), []).append(
            (str(truth_row["field"]), decision)
        )

    rows = []
    for witness_id, receipt in base.items():
        case_id = str(receipt["case_id"])
        case = reference["cases"][case_id]
        source = str(input_units[witness_id]["source_excerpt"])
        proposition = str(case["proposition"][witness_id])
        owner = proposition_owner.get(witness_id)
        if owner is not None:
            if owner["proposition_status"] != proposition:
                raise JudgeV5CalibrationV130Error("v130 proposition owner no longer matches reference")
            proposition_spans = _unique_exact_spans(owner["source_evidence_spans"], source)
        else:
            proposition_spans = _unique_exact_spans(receipt["proposition_evidence_spans"], source)
        if proposition == "supported" and not proposition_spans:
            raise JudgeV5CalibrationV130Error("v130 supported proposition has no exact evidence")

        issues = sorted(str(field) for field in case["field_issues"][witness_id])
        owner_spans = []
        for field, decision in sorted(
            field_rows.get(witness_id, []), key=lambda item: (item[0] not in issues, item[0])
        ):
            if field == "unsupported_inference" and witness_id in proposition_owner:
                wanted = "incorrect" if proposition == "unsupported" else "correct"
                if (field in issues) != (wanted == "incorrect"):
                    raise JudgeV5CalibrationV130Error(
                        "v130 proposition-to-unsupported-inference projection drifted"
                    )
                continue
            wanted = "incorrect" if field in issues else "correct"
            if decision["field_status"] != wanted:
                raise JudgeV5CalibrationV130Error("v130 field owner no longer matches reference")
            owner_spans.extend(decision["source_evidence_spans"])
        field_spans = _unique_exact_spans(
            owner_spans + list(receipt["field_evidence_spans"]), source
        )
        receipt.update(
            {
                "proposition_verdict": proposition,
                "proposition_evidence_spans": proposition_spans,
                "proposition_rationale": "Frozen proposition-only LLM owner projection.",
                "structured_field_verdict": "incorrect" if issues else "correct",
                "field_issue_fields": issues,
                "field_evidence_spans": field_spans,
                "field_rationale": "Frozen independent LLM field-owner projection.",
            }
        )
        rows.append(receipt)
    rows.sort(key=lambda row: (row["case_id"], row["witness_id"]))
    projected = {
        "schema_version": v120["support"]["schema_version"],
        "units": rows,
        "side_free": True,
        "claim_support_and_field_correctness_separate": True,
        "owner_projection_version": V130_RECEIPT_VERSION,
    }
    errors = validate_pointwise_support_output(
        {"units": rows}, v120["values"]["pointwise_input"]
    )
    if errors:
        raise JudgeV5CalibrationV130Error("v130 projected support receipts invalid: " + ";".join(errors))
    return projected


def _support_only_projection(
    row: Mapping[str, Any], proposition: Mapping[str, str]
) -> dict[str, Any]:
    projection = _project_alignment(row)
    supported = {witness_id for witness_id, status in proposition.items() if status == "supported"}
    all_witnesses = set(proposition)
    pairs = [pair for pair in projection["pairs"] if set(pair["witness_ids"]) <= supported]
    paired = {witness_id for pair in pairs for witness_id in pair["witness_ids"]}
    groups = []
    for group in projection["equivalence_groups"]:
        retained = sorted(set(group) & supported)
        if retained and retained not in groups:
            groups.append(retained)
    for witness_id in sorted(supported):
        if not any(witness_id in group for group in groups):
            groups.append([witness_id])
    for witness_id in sorted(all_witnesses - supported):
        groups.append([witness_id])
    groups.sort()
    return {
        "pairs": pairs,
        "equivalence_groups": groups,
        "unpaired_witness_ids": sorted(all_witnesses - paired),
    }


def _balanced_controls(
    candidates: Sequence[str], reference: Mapping[str, Any]
) -> list[str]:
    buckets: dict[str, list[str]] = {}
    for case_id in candidates:
        buckets.setdefault(str(reference["cases"][case_id]["shape"]), []).append(case_id)
    for shape, case_ids in buckets.items():
        case_ids.sort(key=lambda case_id: sha256_text(f"v130|control|{shape}|{case_id}"))
    selected = []
    while len(selected) < CONTROL_COUNT:
        progressed = False
        for shape in sorted(buckets):
            if buckets[shape] and len(selected) < CONTROL_COUNT:
                selected.append(buckets[shape].pop(0))
                progressed = True
        if not progressed:
            break
    if len(selected) != CONTROL_COUNT:
        raise JudgeV5CalibrationV130Error("v130 balanced control coverage drifted")
    return selected


def _support_only_input(
    pool: Mapping[str, Any], receipts: Mapping[str, Any], case_ids: Sequence[str]
) -> dict[str, Any]:
    value = build_neutral_alignment_input(pool, receipts, case_ids=case_ids)
    status = {str(row["witness_id"]): row["proposition_verdict"] for row in receipts["units"]}
    for case in value["cases"]:
        case["witnesses"] = [
            witness
            for witness in case["witnesses"]
            if status[str(witness["witness_id"])] == "supported"
        ]
        if not case["witnesses"]:
            raise JudgeV5CalibrationV130Error("v130 case has no support-positive witness")
    value["supported_witnesses_only"] = True
    value["unsupported_witnesses_structurally_excluded"] = True
    value["owner_protocol_version"] = V130_SPEC_VERSION
    return value


def build_v130_inputs(
    v129: Mapping[str, Any], v120: Mapping[str, Any], v106: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    reference = v129["values"]["reference"]
    receipts = build_v130_support_receipts(v129, v120)
    retained_ids = sorted(v120["values"]["truth"]["cases"])
    gpt55 = {str(row["case_id"]): row for row in v106["values"]["reconciled"]["cases"]}
    terra = {str(row["case_id"]): row for row in v120["values"]["alignment"]["cases"]}
    controls, disputes, truth_rows = [], [], []
    for case_id in retained_ids:
        case = reference["cases"][case_id]
        proposition = case["proposition"]
        current = _support_only_projection(
            {
                "alignment_pairs": case["pairs"],
                "equivalence_groups": case["equivalence_groups"],
                "unpaired_witness_ids": case["unpaired_witness_ids"],
            },
            proposition,
        )
        first = _support_only_projection(gpt55[case_id], proposition)
        second = _support_only_projection(terra[case_id], proposition)
        role = "unanimous_control" if current == first == second else "alignment_dispute"
        (controls if role == "unanimous_control" else disputes).append(case_id)
        truth_rows.append(
            {
                "case_id": case_id,
                "shape": case["shape"],
                "role": role,
                "current_reference": current,
                "proposition": deepcopy(proposition),
                "gpt55_matches_reference": first == current,
                "terra_matches_reference": second == current,
                "gpt55_matches_terra": first == second,
            }
        )
    if len(controls) != 12 or len(disputes) != DISPUTE_COUNT:
        raise JudgeV5CalibrationV130Error("v130 control/dispute split drifted")
    selected_controls = _balanced_controls(controls, reference)
    primary_ids = selected_controls + disputes
    source = _support_only_input(v120["pool"], receipts, primary_ids)
    case_source = {str(case["case_id"]): case for case in source["cases"]}
    primary = deepcopy(source)
    primary["permutation"] = "v130_primary_origin_neutral"
    primary["permuted_axes"] = ["case_order"]
    primary["cases"] = [
        case_source[case_id]
        for case_id in sorted(primary_ids, key=lambda value: sha256_text(f"v130|primary|{value}"))
    ]
    canary = deepcopy(source)
    canary["permutation"] = "v130_canary_origin_neutral"
    canary["permuted_axes"] = ["case_order", "witness_order"]
    canary["cases"] = []
    for case_id in sorted(disputes, key=lambda value: sha256_text(f"v130|canary|{value}")):
        case = deepcopy(case_source[case_id])
        case["witnesses"] = list(reversed(case["witnesses"]))
        canary["cases"].append(case)
    selected_truth = [row for row in truth_rows if row["case_id"] in set(primary_ids)]
    truth = {
        "schema_version": V130_TRUTH_VERSION,
        "case_count": 12,
        "control_count": CONTROL_COUNT,
        "dispute_count": DISPUTE_COUNT,
        "canary_case_count": DISPUTE_COUNT,
        "canary_case_ids": sorted(disputes),
        "cases": sorted(selected_truth, key=lambda row: row["case_id"]),
    }
    selection = {
        "schema_version": V130_SELECTION_VERSION,
        "created_at": now_iso(),
        "retained_case_count": 18,
        "unanimous_control_pool_count": 12,
        "selected_control_count": CONTROL_COUNT,
        "alignment_dispute_count": DISPUTE_COUNT,
        "dispute_source_counts": {
            "model_disagreement": sum(
                not row["gpt55_matches_terra"] for row in selected_truth if row["role"] == "alignment_dispute"
            ),
            "consensus_reference_dispute": sum(
                row["gpt55_matches_terra"] for row in selected_truth if row["role"] == "alignment_dispute"
            ),
        },
        "selected_control_shape_counts": dict(
            sorted(Counter(reference["cases"][case_id]["shape"] for case_id in selected_controls).items())
        ),
        "permutation_canary_count": DISPUTE_COUNT,
        "permutation_canary_contains_every_dispute": True,
        "support_positive_witnesses_only": True,
        "unsupported_witnesses_excluded_by_frozen_proposition_receipts": True,
        "prior_alignment_outputs_in_model_input": False,
        "fixture_truth_in_model_input": False,
        "system_identity_in_model_input": False,
        "selection_uses_source_text": False,
        "majority_voting_used": False,
        "privacy": "opaque_ids_and_aggregate_counts_only",
    }
    return primary, canary, truth, selection, receipts


def alignment_instructions_v130() -> str:
    return neutral_alignment_base_instructions() + (
        " Every visible witness has a frozen supported proposition verdict. Unsupported witnesses "
        "were excluded before this pass and must not be inferred or reconstructed. Align only the "
        "visible support-positive witnesses. Freeze one-to-one assignment before the checklist. "
        "For merge/split candidates, normalize each visible witness into its source-supported atomic "
        "propositions; identical atom sets with grouping-only differences are partial with exactly "
        "event_boundary and evidence different."
    )


def alignment_prompt_v130(value: Mapping[str, Any]) -> str:
    return (
        "All presented witnesses are support-positive. Align only these visible witnesses, freeze "
        "assignment, then complete the full checklist. Do not infer omitted witnesses.\n\n"
        + build_neutral_alignment_prompt(value)
    )


def _case_has_abstention(row: Mapping[str, Any]) -> bool:
    return any(
        decision == "abstain"
        for pair in row.get("alignment_pairs") or []
        for decision in (pair.get("checklist_decisions") or {}).values()
    )


def _owner_projection(row: Mapping[str, Any]) -> dict[str, Any]:
    checklists = []
    for pair in row.get("alignment_pairs") or []:
        checklists.append(
            {
                "witness_ids": sorted(str(value) for value in pair["witness_ids"]),
                "decisions": {
                    field: str((pair.get("checklist_decisions") or {})[field])
                    for field in CHECKLIST_FIELDS
                },
            }
        )
    checklists.sort(key=lambda item: tuple(item["witness_ids"]))
    return {"alignment": _project_alignment(row), "checklists": checklists}


def score_v130(
    primary: Mapping[str, Any], canary: Mapping[str, Any], truth: Mapping[str, Any]
) -> dict[str, Any]:
    primary_rows = {str(row["case_id"]): row for row in primary["cases"]}
    canary_rows = {str(row["case_id"]): row for row in canary["cases"]}
    expected = {str(row["case_id"]): row for row in truth["cases"]}
    if set(primary_rows) != set(expected) or set(canary_rows) != set(truth["canary_case_ids"]):
        raise JudgeV5CalibrationV130Error("v130 score coverage drifted")
    controls = [row for row in expected.values() if row["role"] == "unanimous_control"]
    disputes = [row for row in expected.values() if row["role"] == "alignment_dispute"]
    control_exact = sum(
        _support_only_projection(primary_rows[row["case_id"]], row["proposition"])
        == row["current_reference"]
        for row in controls
    )
    abstention_cases = sum(_case_has_abstention(row) for row in primary_rows.values())
    canary_exact = sum(
        _owner_projection(primary_rows[case_id]) == _owner_projection(canary_rows[case_id])
        for case_id in canary_rows
    )
    triggers = []
    for row in controls:
        if (
            _support_only_projection(primary_rows[row["case_id"]], row["proposition"])
            != row["current_reference"]
        ):
            triggers.append({"case_id": row["case_id"], "reason": "unanimous_control_mismatch"})
    for case_id, row in primary_rows.items():
        if _case_has_abstention(row):
            triggers.append({"case_id": case_id, "reason": "primary_abstention"})
    for case_id, row in canary_rows.items():
        if _owner_projection(primary_rows[case_id]) != _owner_projection(row):
            triggers.append({"case_id": case_id, "reason": "canary_disagreement"})
    checks = {
        "unanimous_control_exact_rate": control_exact == CONTROL_COUNT,
        "primary_abstention_count": abstention_cases == 0,
        "permutation_canary_exact_rate": canary_exact == DISPUTE_COUNT,
        "support_positive_input_contract": True,
        "schema_and_exact_evidence_validation": True,
    }
    passed = all(checks.values())
    return {
        "schema_version": V130_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": {
            "case_count": 12,
            "unanimous_control_count": len(controls),
            "unanimous_control_exact_count": control_exact,
            "alignment_dispute_count": len(disputes),
            "primary_abstention_case_count": abstention_cases,
            "permutation_canary_count": DISPUTE_COUNT,
            "permutation_canary_exact_count": canary_exact,
            "observable_repair_trigger_count": len({row["case_id"] for row in triggers}),
        },
        "observable_repair_triggers": triggers,
        "alignment_reference_frozen": passed,
        "fresh_diagnostic_authorized": passed,
        "fresh_full_calibration_authorized": False,
        "capped_repair_authorized": not passed and bool(triggers),
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "majority_voting_used": False,
    }


def reconcile_alignment_reference(
    *, current_reference: Mapping[str, Any], primary: Mapping[str, Any], truth: Mapping[str, Any]
) -> dict[str, Any]:
    candidate = deepcopy(current_reference)
    primary_rows = {str(row["case_id"]): row for row in primary["cases"]}
    dispute_ids = {
        str(row["case_id"]) for row in truth["cases"] if row["role"] == "alignment_dispute"
    }
    structural_changes = 0
    for case_id, case in candidate["cases"].items():
        original = _truth_alignment(case)
        projection = _support_only_projection(
            {
                "alignment_pairs": case["pairs"],
                "equivalence_groups": case["equivalence_groups"],
                "unpaired_witness_ids": case["unpaired_witness_ids"],
            },
            case["proposition"],
        )
        structural_changes += int(projection != original)
        case["pairs"] = deepcopy(projection["pairs"])
        case["equivalence_groups"] = deepcopy(projection["equivalence_groups"])
        case["unpaired_witness_ids"] = deepcopy(projection["unpaired_witness_ids"])
    for case_id in dispute_ids:
        case = candidate["cases"][case_id]
        projection = _support_only_projection(primary_rows[case_id], case["proposition"])
        case["pairs"] = deepcopy(projection["pairs"])
        case["equivalence_groups"] = deepcopy(projection["equivalence_groups"])
        case["unpaired_witness_ids"] = deepcopy(projection["unpaired_witness_ids"])
    candidate.update(
        {
            "schema_version": V130_REFERENCE_VERSION,
            "reference_version": "fixture_reference_v10_retained_alignment_owner_frozen",
            "retained_field_reference_patch_authorized": True,
            "proposition_reference_frozen": True,
            "alignment_reference_frozen": True,
            "reference_frozen": True,
            "retained_alignment_owner_case_count": DISPUTE_COUNT,
            "support_only_structural_projection_case_count": structural_changes,
            "fresh_diagnostic_authorized": True,
            "fresh_full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
        }
    )
    return candidate


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path, policy_path = root / "capacity-policy-audit.json", root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V130_CAPACITY_AUDIT_VERSION,
        "phase_id": V130_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V130_CAPACITY_POLICY_VERSION,
        "phase_id": V130_PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": list(TURN_NAMES),
        "minimum_remaining_reserve_percent": 20,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
        "phase_total_token_bound": bound,
        "projected_phase_quota_points": math.ceil(
            bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
        ),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v130(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v130 terminal")}
    v129 = _validate_v129()
    v120 = _load_v120_alignment_sources()
    v106 = _validate_v106_full()
    primary, canary, truth, selection, receipts = build_v130_inputs(v129, v120, v106)
    values = {
        "alignment-owner-input.private.json": primary,
        "alignment-owner-canary-input.private.json": canary,
        "alignment-owner-truth.private.json": truth,
        "support-receipts-v10.private.json": receipts,
    }
    for filename, value in values.items():
        _write_immutable(root / filename, value)
    _write_stable_time(root / "selection-audit.json", selection, "created_at")
    turns = []
    for turn_name, value in ((PRIMARY_TURN, primary), (CANARY_TURN, canary)):
        prompt, schema = alignment_prompt_v130(value), neutral_alignment_output_schema(value)
        paths = _freeze_turn_request(
            root=root, turn_name=turn_name, input_value=value, prompt=prompt, schema=schema
        )
        turns.append(
            {"turn_name": turn_name, "value": value, "prompt": prompt, "schema": schema, "paths": paths}
        )
    v126, v121 = _v126_and_v121(v129)
    predecessor = {
        **{f"v129_{name}": record for name, record in v129["records"].items()},
        "v129_attempts": v129["attempts"],
        **{f"v120_{name}": record for name, record in v120["records"].items()},
        "v120_attempts": v120["attempt_records"],
        **{f"v120_alignment_{name}": record for name, record in v120["alignment_source_records"].items()},
        **{f"v106_{name}": record for name, record in v106["records"].items()},
        "v126_reconciled_field_owner": v126["records"]["reconciled"],
        "v121_field_truth": v121["records"]["truth"],
    }
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V130_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "side_free_sol_owner_over_six_retained_alignment_disputes_and_six_controls",
        "case_count": 12,
        "unanimous_control_count": CONTROL_COUNT,
        "alignment_dispute_count": DISPUTE_COUNT,
        "permutation_canary_count": DISPUTE_COUNT,
        "turn_plan": list(TURN_NAMES),
        "support_positive_witnesses_only": True,
        "unsupported_witnesses_structurally_excluded": True,
        "prior_alignment_outputs_in_model_input": False,
        "fixture_truth_in_model_input": False,
        "system_identity_in_model_input": False,
        "majority_voting_used": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "six_exact_controls_zero_abstentions_and_six_of_six_origin_neutral_canary",
        "proposition_reference_frozen": True,
        "alignment_reference_frozen": False,
        "fresh_diagnostic_authorized": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v129_singleton_proposition_repair.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v121_retained_field_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v120_retained_case_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v107_recovery_receipt.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v110_sol_reference_audit.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_instruction_hash": sha256_text(alignment_instructions_v130()),
        "frozen_inputs": {
            "primary": _record(root / "alignment-owner-input.private.json"),
            "canary": _record(root / "alignment-owner-canary-input.private.json"),
            "truth": _record(root / "alignment-owner-truth.private.json"),
            "support_receipts": _record(root / "support-receipts-v10.private.json"),
            "selection": _record(root / "selection-audit.json"),
            "turns": [
                {
                    "turn_name": turn["turn_name"],
                    "input": _record(turn["paths"]["input"]),
                    "prompt": _record(turn["paths"]["prompt"]),
                    "schema": _record(turn["paths"]["schema"]),
                }
                for turn in turns
            ],
        },
        "privacy": "private_source_event_outputs_no_source_text_in_reports",
    }
    spec_path = root / "alignment-owner-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "truth": truth,
        "v129": v129,
    }


def _write_failure(root: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    attempts = [
        row for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            measured = _validate_usage(_load_json(Path(record["path"]), "v130 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V130_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": turn_name,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "accounting_complete": complete,
        "usage_status": "complete" if complete else "unknown",
        "usage": usage if complete else None,
        "known_usage_lower_bound": usage,
        "unknown_usage_turn_count": unknown,
        "attempts": attempts,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V130_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "proposition_reference_frozen": True,
        "alignment_reference_frozen": False,
        "fresh_diagnostic_authorized": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "accounting_complete": complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v130(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v130 terminal")
    frozen = freeze_v130(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    try:
        outputs, normalized, sidecars = [], [], []
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=alignment_instructions_v130(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(turn["value"]["cases"]),
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=turn["value"]: _validate_scoreable_alignment_output(candidate, item),
                )
                outputs.append(output)
                normalized.append(normalize_neutral_alignment_output(output, turn["value"]))
                sidecars.append(sidecar)
        paths = {
            "primary": root / "alignment-owner-output.private.json",
            "canary": root / "alignment-owner-canary-output.private.json",
            "primary_normalized": root / "alignment-owner-normalized.private.json",
            "canary_normalized": root / "alignment-owner-canary-normalized.private.json",
            "score": root / "alignment-owner-score.json",
            "reference": root / "calibration-truth-v10-retained-alignment-owner.private.json",
        }
        _write_immutable(paths["primary"], outputs[0])
        _write_immutable(paths["canary"], outputs[1])
        _write_immutable(paths["primary_normalized"], normalized[0])
        _write_immutable(paths["canary_normalized"], normalized[1])
        score = score_v130(normalized[0], normalized[1], frozen["truth"])
        _write_immutable(paths["score"], score)
        if score["passed"]:
            reference = reconcile_alignment_reference(
                current_reference=frozen["v129"]["values"]["reference"],
                primary=normalized[0],
                truth=frozen["truth"],
            )
            _write_immutable(paths["reference"], reference)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V130_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v130_retained_alignment_owner_passed_reference_frozen"
                if passed else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v130_retained_alignment_reference_frozen_fresh_diagnostic_authorized"
                if passed else "v130_retained_alignment_owner_repair_or_recovery_required"
            ),
            "overall_evaluation_complete": False,
            "proposition_reference_frozen": True,
            "alignment_reference_frozen": passed,
            "reference_frozen": passed,
            "fresh_diagnostic_authorized": passed,
            "fresh_full_calibration_authorized": False,
            "capped_repair_authorized": score["capped_repair_authorized"],
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(paths["score"]),
            "reference": _record(paths["reference"]) if passed else None,
            "primary_output": _record(paths["primary"]),
            "canary_output": _record(paths["canary"]),
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "predecessor_v129_usage": frozen["v129"]["usage"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v130 retained alignment owner")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v130(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "alignment_reference_frozen": terminal.get("alignment_reference_frozen", False),
                "fresh_diagnostic_authorized": terminal.get("fresh_diagnostic_authorized", False),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
