from __future__ import annotations

"""Final side-free alignment owner with a small origin-neutral canary."""

import argparse
import asyncio
import json
import math
from collections import defaultdict
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v108_layered_diagnostic as v108
from .app_server_judge_v5 import (
    CHECKLIST_FIELDS,
    neutral_alignment_output_schema,
    normalize_neutral_alignment_output,
)
from .app_server_judge_v5_calibration_v25_diagnostic import (
    QUOTA_POINTS_PER_MILLION_TOKENS,
)
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
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_calibration_v110_sol_reference_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V110_ROOT,
    _project_alignment,
    _truth_alignment,
)
from .app_server_judge_v5_calibration_v111_sol_reference_recovery import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V111_ROOT,
)
from .app_server_judge_v5_calibration_v116_capped_contested_repair import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V116_ROOT,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V117_INPUT_VERSION = "pif_app_server_judge_v5_4_v117_alignment_owner_input_v1"
V117_TRUTH_VERSION = "pif_app_server_judge_v5_4_v117_alignment_owner_truth_v1"
V117_SELECTION_VERSION = "pif_app_server_judge_v5_4_v117_alignment_selection_v1"
V117_SPEC_VERSION = "pif_app_server_judge_v5_4_v117_alignment_owner_spec_v1"
V117_SCORE_VERSION = "pif_app_server_judge_v5_4_v117_alignment_owner_score_v1"
V117_REFERENCE_VERSION = "pif_app_server_judge_v5_4_calibration_truth_v9_frozen"
V117_FAILURE_VERSION = "pif_app_server_judge_v5_4_v117_failure_v1"
V117_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v117_terminal_v1"
V117_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V117_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V117_PHASE_ID = "judge_v5_4_v117_alignment_reference_owner"

MODEL = "gpt-5.6-terra"
EFFORT = "high"
PRIMARY_TURN = "final_alignment_owner"
CANARY_TURN = "final_alignment_owner_canary"
TURN_NAMES = (PRIMARY_TURN, CANARY_TURN)
TIMEOUT_SECONDS = 1200.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V116_ROOT.parent / "judge-calibration-v5_4-v117-alignment-reference-owner"
).resolve()


class JudgeV5CalibrationV117Error(RuntimeError):
    """The v117 alignment-owner contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _iter_records(value: Any):
    if isinstance(value, Mapping):
        if {"path", "sha256", "size_bytes"}.issubset(value):
            yield value
        for item in value.values():
            yield from _iter_records(item)
    elif isinstance(value, list):
        for item in value:
            yield from _iter_records(item)


def _validate_v116() -> dict[str, Any]:
    root = DEFAULT_V116_ROOT
    paths = {
        "spec": root / "capped-repair-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "capped-repair-score.json",
        "reference": root / "pointwise-reference-v9-candidate.private.json",
        "reconciled": root / "reconciled-contested-field-output.private.json",
    }
    values = {name: _load_json(path, f"v116 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v116_capped_repair_passed_pointwise_patch_authorized"
        or terminal.get("pointwise_reference_patch_authorized") is not True
        or terminal.get("alignment_reference_frozen") is not False
        or terminal.get("fresh_full_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 25615
        or score.get("passed") is not True
        or score.get("failed_checks") != []
        or spec.get("model") != "gpt-5.6-terra"
        or spec.get("retry_count_per_turn") != 0
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
        or values["reference"].get("pointwise_reference_patch_authorized") is not True
        or values["reference"].get("alignment_reference_frozen") is not False
        or len(values["reference"].get("cases") or {}) != 18
    ):
        raise JudgeV5CalibrationV117Error("v116 predecessor contract drifted")
    if not all(_verify_record(record) for record in _iter_records(spec)):
        raise JudgeV5CalibrationV117Error("v116 frozen record drifted")
    turn_root = root / "turns" / "capped-contested-field-repair"
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
        raise JudgeV5CalibrationV117Error("v116 turn coverage is incomplete")
    usage = _validate_usage(_load_json(turn_paths["sidecar"], "v116 sidecar"))
    if usage != terminal["usage"]:
        raise JudgeV5CalibrationV117Error("v116 usage drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "turn_records": {name: _record(path) for name, path in turn_paths.items()},
        "usage": usage,
    }


def _validate_v111() -> dict[str, Any]:
    root = DEFAULT_V111_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "audit": root / "v110-recovery-audit.json",
        "score": root / "owner-audit-score.json",
        "normalized": root / "sol-alignment-normalized.private.json",
    }
    values = {name: _load_json(path, f"v111 {name}") for name, path in paths.items()}
    terminal, audit, score = values["terminal"], values["audit"], values["score"]
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("reference_patch_authorized") is not False
        or terminal.get("fresh_full_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("predecessor_v110_usage", {}).get("total_tokens") != 199677
        or audit.get("new_semantic_turn_count") != 0
        or audit.get("v110_usage", {}).get("total_tokens") != 199677
        or score.get("alignment_control_count") != 8
        or score.get("alignment_control_exact_count") != 7
        or score.get("alignment_candidate_count") != 4
        or score.get("alignment_consensus_patch_count") != 2
        or score.get("checks", {}).get("alignment_control_exact_rate") is not True
    ):
        raise JudgeV5CalibrationV117Error("v111 alignment evidence drifted")
    if not all(_verify_record(record) for record in _iter_records(audit)):
        raise JudgeV5CalibrationV117Error("v111 recovery record drifted")

    spec_record = audit["v110"]["spec"]
    v110_spec = _load_json(Path(spec_record["path"]), "v110 spec")
    gpt_record = (v110_spec.get("predecessor") or {}).get("alignment")
    if not isinstance(gpt_record, Mapping) or not _verify_record(gpt_record):
        raise JudgeV5CalibrationV117Error("v109 alignment record drifted")
    gpt_alignment = _load_json(Path(gpt_record["path"]), "v109 alignment")

    turn_inputs = []
    normalized_cases = []
    for turn_name in ("sol_alignment_owner_shard_00", "sol_alignment_owner_shard_01"):
        records = audit["v110_turns"][turn_name]
        input_value = _load_json(Path(records["input"]["path"]), f"{turn_name} input")
        raw_output = _load_json(Path(records["output"]["path"]), f"{turn_name} output")
        turn_inputs.append(input_value)
        normalized_cases.extend(
            normalize_neutral_alignment_output(raw_output, input_value)["cases"]
        )
    normalized_cases.sort(key=lambda row: row["case_id"])
    if normalized_cases != sorted(
        values["normalized"]["cases"], key=lambda row: row["case_id"]
    ):
        raise JudgeV5CalibrationV117Error("v111 normalized alignment drifted")
    selection_record = audit["v110"]["selection"]
    selection = _load_json(Path(selection_record["path"]), "v110 alignment selection")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {
            **{name: _record(path) for name, path in paths.items()},
            "gpt_alignment": _record(Path(gpt_record["path"])),
            "selection": _record(Path(selection_record["path"])),
        },
        "turn_inputs": turn_inputs,
        "gpt_alignment": gpt_alignment,
        "sol_alignment": {"cases": normalized_cases},
        "selection": selection,
        "v110_records": audit["v110"],
        "v110_turn_records": audit["v110_turns"],
    }


def _base_input(turn_inputs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    value = {key: deepcopy(item) for key, item in turn_inputs[0].items() if key != "cases"}
    value["owner_protocol_version"] = V117_INPUT_VERSION
    value["side_labels_present"] = False
    value["system_identity_present"] = False
    value["support_receipts_frozen"] = True
    value["rubric_and_decision_order_fixed"] = True
    return value


def build_v117_inputs(
    v116: Mapping[str, Any], v111: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    truth = v116["values"]["reference"]
    case_source = {
        str(case["case_id"]): deepcopy(case)
        for turn in v111["turn_inputs"]
        for case in turn["cases"]
    }
    audit_ids = [str(value) for value in v111["selection"]["audit"]]
    if set(case_source) != set(audit_ids) or len(audit_ids) != 12:
        raise JudgeV5CalibrationV117Error("v117 source case coverage drifted")

    gpt = {str(row["case_id"]): row for row in v111["gpt_alignment"]["cases"]}
    sol = {str(row["case_id"]): row for row in v111["sol_alignment"]["cases"]}
    controls = []
    disputes = []
    truth_rows = []
    for case_id in audit_ids:
        reference = _truth_alignment(truth["cases"][case_id])
        gpt_projection = _project_alignment(gpt[case_id])
        sol_projection = _project_alignment(sol[case_id])
        role = (
            "unanimous_control"
            if reference == gpt_projection == sol_projection
            else "alignment_dispute"
        )
        (controls if role == "unanimous_control" else disputes).append(case_id)
        truth_rows.append(
            {
                "case_id": case_id,
                "role": role,
                "current_reference": reference,
                "gpt55_matches_reference": gpt_projection == reference,
                "sol_matches_reference": sol_projection == reference,
                "gpt55_matches_sol": gpt_projection == sol_projection,
            }
        )
        for witness in case_source[case_id]["witnesses"]:
            witness_id = str(witness["witness_id"])
            if (
                witness["support_receipt"].get("proposition_verdict")
                != truth["cases"][case_id]["proposition"][witness_id]
            ):
                raise JudgeV5CalibrationV117Error(
                    "v117 frozen support receipt no longer matches pointwise reference"
                )
    if len(controls) != 7 or len(disputes) != 5:
        raise JudgeV5CalibrationV117Error("v117 control/dispute split drifted")

    primary = _base_input(v111["turn_inputs"])
    primary["permutation"] = "v117_primary_origin_neutral"
    primary["permuted_axes"] = ["case_order"]
    primary["cases"] = [
        case_source[case_id]
        for case_id in sorted(audit_ids, key=lambda value: sha256_text(f"v117|primary|{value}"))
    ]

    canary_control = min(controls, key=lambda value: sha256_text(f"v117|canary-control|{value}"))
    canary_ids = disputes + [canary_control]
    canary = _base_input(v111["turn_inputs"])
    canary["permutation"] = "v117_canary_origin_neutral"
    canary["permuted_axes"] = ["case_order", "witness_order"]
    canary["cases"] = []
    for case_id in sorted(
        canary_ids, key=lambda value: sha256_text(f"v117|canary|{value}")
    ):
        case = deepcopy(case_source[case_id])
        case["witnesses"] = list(reversed(case["witnesses"]))
        canary["cases"].append(case)

    private_truth = {
        "schema_version": V117_TRUTH_VERSION,
        "case_count": 12,
        "control_count": 7,
        "dispute_count": 5,
        "canary_case_count": 6,
        "canary_case_ids": sorted(canary_ids),
        "cases": sorted(truth_rows, key=lambda row: row["case_id"]),
    }
    selection = {
        "schema_version": V117_SELECTION_VERSION,
        "created_at": now_iso(),
        "case_count": 12,
        "unanimous_control_count": 7,
        "alignment_dispute_count": 5,
        "permutation_canary_count": 6,
        "permutation_canary_contains_every_dispute": True,
        "support_receipt_reference_mismatch_count": 0,
        "owner_is_side_free": True,
        "prior_alignment_outputs_in_model_input": False,
        "fixture_truth_in_model_input": False,
        "system_identity_in_model_input": False,
        "majority_voting_used": False,
        "privacy": "opaque_ids_and_aggregate_counts_only",
    }
    return primary, canary, private_truth, selection


def _case_has_abstention(row: Mapping[str, Any]) -> bool:
    return any(
        decision == "abstain"
        for pair in row.get("alignment_pairs") or []
        for decision in (pair.get("checklist_decisions") or {}).values()
    )


def _owner_projection(row: Mapping[str, Any]) -> dict[str, Any]:
    alignment = _project_alignment(row)
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
    return {"alignment": alignment, "checklists": checklists}


def score_v117(
    primary: Mapping[str, Any],
    canary: Mapping[str, Any],
    truth: Mapping[str, Any],
) -> dict[str, Any]:
    primary_rows = {str(row["case_id"]): row for row in primary["cases"]}
    canary_rows = {str(row["case_id"]): row for row in canary["cases"]}
    expected = {str(row["case_id"]): row for row in truth["cases"]}
    if set(primary_rows) != set(expected) or set(canary_rows) != set(
        truth["canary_case_ids"]
    ):
        raise JudgeV5CalibrationV117Error("v117 score coverage drifted")

    controls = [row for row in expected.values() if row["role"] == "unanimous_control"]
    disputes = [row for row in expected.values() if row["role"] == "alignment_dispute"]
    control_exact = sum(
        _project_alignment(primary_rows[row["case_id"]]) == row["current_reference"]
        for row in controls
    )
    abstention_cases = sum(_case_has_abstention(row) for row in primary_rows.values())
    canary_exact = sum(
        _owner_projection(primary_rows[case_id])
        == _owner_projection(canary_rows[case_id])
        for case_id in canary_rows
    )
    trigger_reasons: dict[str, set[str]] = defaultdict(set)
    for row in controls:
        case_id = row["case_id"]
        if _project_alignment(primary_rows[case_id]) != row["current_reference"]:
            trigger_reasons[case_id].add("unanimous_control_mismatch")
    for case_id, row in primary_rows.items():
        if _case_has_abstention(row):
            trigger_reasons[case_id].add("primary_abstention")
    for case_id, row in canary_rows.items():
        if _owner_projection(primary_rows[case_id]) != _owner_projection(row):
            trigger_reasons[case_id].add("canary_disagreement")
    checks = {
        "unanimous_control_exact_rate": control_exact == 7,
        "primary_abstention_count": abstention_cases == 0,
        "permutation_canary_exact_rate": canary_exact == 6,
        "schema_and_exact_evidence_validation": True,
    }
    passed = all(checks.values())
    trigger_count = len(trigger_reasons)
    return {
        "schema_version": V117_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": {
            "case_count": 12,
            "unanimous_control_count": len(controls),
            "unanimous_control_exact_count": control_exact,
            "alignment_dispute_count": len(disputes),
            "primary_abstention_case_count": abstention_cases,
            "permutation_canary_count": 6,
            "permutation_canary_exact_count": canary_exact,
            "observable_repair_trigger_count": trigger_count,
        },
        "observable_repair_triggers": [
            {"case_id": case_id, "reasons": sorted(reasons)}
            for case_id, reasons in sorted(trigger_reasons.items())
        ],
        "alignment_reference_frozen": passed,
        "fresh_full_calibration_authorized": passed,
        "capped_repair_authorized": not passed and 0 < trigger_count <= 12,
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
        str(row["case_id"])
        for row in truth["cases"]
        if row["role"] == "alignment_dispute"
    }
    for case_id in dispute_ids:
        projection = _project_alignment(primary_rows[case_id])
        case = candidate["cases"][case_id]
        case["pairs"] = deepcopy(projection["pairs"])
        case["equivalence_groups"] = deepcopy(projection["equivalence_groups"])
        case["unpaired_witness_ids"] = deepcopy(projection["unpaired_witness_ids"])
    candidate["schema_version"] = V117_REFERENCE_VERSION
    candidate["reference_version"] = "fixture_reference_v9_pointwise_and_alignment_owner_frozen"
    candidate["pointwise_reference_patch_authorized"] = True
    candidate["alignment_reference_frozen"] = True
    candidate["reference_frozen"] = True
    candidate["alignment_owner_case_count"] = 5
    candidate["fresh_full_calibration_authorized"] = True
    return candidate


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V117_CAPACITY_AUDIT_VERSION,
        "phase_id": V117_PHASE_ID,
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
        "schema_version": V117_CAPACITY_POLICY_VERSION,
        "phase_id": V117_PHASE_ID,
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


def freeze_v117(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v117 terminal")}
    v116 = _validate_v116()
    v111 = _validate_v111()
    primary, canary, truth, selection = build_v117_inputs(v116, v111)
    input_path = root / "alignment-owner-input.private.json"
    canary_path = root / "alignment-owner-canary-input.private.json"
    truth_path = root / "alignment-owner-truth.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(input_path, primary)
    _write_immutable(canary_path, canary)
    _write_immutable(truth_path, truth)
    _write_stable_time(selection_path, selection, "created_at")

    turns = []
    for turn_name, value in ((PRIMARY_TURN, primary), (CANARY_TURN, canary)):
        prompt = v108.build_alignment_prompt_v108(value)
        schema = neutral_alignment_output_schema(value)
        paths = _freeze_turn_request(
            root=root,
            turn_name=turn_name,
            input_value=value,
            prompt=prompt,
            schema=schema,
        )
        turns.append(
            {
                "turn_name": turn_name,
                "value": value,
                "prompt": prompt,
                "schema": schema,
                "paths": paths,
            }
        )
    predecessor = {
        **{f"v116_{name}": record for name, record in v116["records"].items()},
        "v116_turn": v116["turn_records"],
        **{f"v111_{name}": record for name, record in v111["records"].items()},
        "v110_records": v111["v110_records"],
        "v110_turn_records": v111["v110_turn_records"],
    }
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V117_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "side_free_alignment_owner_over_seven_controls_and_five_disputes_with_small_permutation_canary",
        "case_count": 12,
        "unanimous_control_count": 7,
        "alignment_dispute_count": 5,
        "permutation_canary_count": 6,
        "turn_plan": list(TURN_NAMES),
        "owner_is_side_free": True,
        "support_receipts_frozen": True,
        "prior_alignment_outputs_in_model_input": False,
        "fixture_truth_in_model_input": False,
        "majority_voting_used": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "seven_exact_controls_zero_abstentions_and_six_of_six_origin_neutral_canary",
        "pointwise_reference_patch_authorized": True,
        "alignment_reference_frozen": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v116_capped_contested_repair.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v111_sol_reference_recovery.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v110_sol_reference_audit.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v108_layered_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "input": _record(input_path),
            "canary": _record(canary_path),
            "truth": _record(truth_path),
            "selection": _record(selection_path),
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
        "v116": v116,
    }


def _write_failure(root: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    attempts = [
        row
        for row in _attempt_records(root)
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v117 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V117_FAILURE_VERSION,
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
        "schema_version": V117_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "pointwise_reference_patch_authorized": True,
        "alignment_reference_frozen": False,
        "reference_frozen": False,
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


async def run_v117(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v117 terminal")
    frozen = freeze_v117(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    try:
        outputs = []
        normalized = []
        sidecars = []
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=v108.alignment_instructions_v108(),
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
        primary_output_path = root / "alignment-owner-output.private.json"
        canary_output_path = root / "alignment-owner-canary-output.private.json"
        primary_normalized_path = root / "alignment-owner-normalized.private.json"
        canary_normalized_path = root / "alignment-owner-canary-normalized.private.json"
        _write_immutable(primary_output_path, outputs[0])
        _write_immutable(canary_output_path, outputs[1])
        _write_immutable(primary_normalized_path, normalized[0])
        _write_immutable(canary_normalized_path, normalized[1])
        score = score_v117(normalized[0], normalized[1], frozen["truth"])
        score_path = root / "alignment-owner-score.json"
        _write_immutable(score_path, score)
        reference_path = root / "fixture-reference-v9-frozen.private.json"
        if score["passed"]:
            reference = reconcile_alignment_reference(
                current_reference=frozen["v116"]["values"]["reference"],
                primary=normalized[0],
                truth=frozen["truth"],
            )
            _write_immutable(reference_path, reference)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V117_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v117_alignment_owner_passed_reference_frozen"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v117_reference_frozen_full_calibration_authorized"
                if passed
                else "v117_alignment_owner_repair_or_recovery_required"
            ),
            "overall_evaluation_complete": False,
            "pointwise_reference_patch_authorized": True,
            "alignment_reference_frozen": passed,
            "reference_frozen": passed,
            "fresh_full_calibration_authorized": passed,
            "capped_repair_authorized": score["capped_repair_authorized"],
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "reference": _record(reference_path) if passed else None,
            "primary_output": _record(primary_output_path),
            "canary_output": _record(canary_output_path),
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "predecessor_v116_usage": frozen["v116"]["usage"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, current_turn, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v117 alignment reference owner")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v117(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "reference_frozen": terminal.get("reference_frozen", False),
                "fresh_full_calibration_authorized": terminal.get(
                    "fresh_full_calibration_authorized", False
                ),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
