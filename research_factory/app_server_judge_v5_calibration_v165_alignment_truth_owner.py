from __future__ import annotations

"""Resolve the single v164 reference contradiction with a side-free owner."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v130_retained_alignment_owner as v130
from . import app_server_judge_v5_calibration_v157_exact_span_canary_recovery as v157
from . import app_server_judge_v5_calibration_v162_layer_corrected_field_adjudication as v162
from . import app_server_judge_v5_calibration_v164_equivalent_pair_verifier_diagnostic as v164
from .app_server_judge_v5 import neutral_alignment_output_schema
from .app_server_judge_v5_calibration_v25_diagnostic import QUOTA_POINTS_PER_MILLION_TOKENS
from .app_server_judge_v5_calibration_v26_diagnostic import (
    JudgeV5CalibrationV26DiagnosticAttemptFailed,
    _aggregate_usage,
    _client_factory,
    _freeze_turn_request,
    _get_or_run_turn,
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V165_SPEC_VERSION = "pif_app_server_judge_v5_4_v165_spec_v1"
V165_SCORE_VERSION = "pif_app_server_judge_v5_4_v165_score_v1"
V165_REFERENCE_VERSION = "pif_app_server_judge_v5_4_fixture_reference_v15"
V165_TRUTH_VERSION = "pif_app_server_judge_v5_4_full_truth_v165"
V165_PROTOCOL_VERSION = "pif_app_server_judge_v5_4_v165_alignment_protocol_v1"
V165_AUDIT_VERSION = "pif_app_server_judge_v5_4_v165_reference_patch_audit_v1"
V165_FAILURE_VERSION = "pif_app_server_judge_v5_4_v165_failure_v1"
V165_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v165_terminal_v1"
V165_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V165_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V165_PHASE_ID = "judge_v5_4_v165_alignment_truth_owner"

MODEL = "gpt-5.4"
EFFORT = "high"
PRIMARY_TURN = "alignment_truth_owner_primary"
CANARY_TURN = "alignment_truth_owner_canary"
TURN_NAMES = (PRIMARY_TURN, CANARY_TURN)
MAXIMUM_TOTAL_TOKENS_PER_TURN = 45_000
TIMEOUT_SECONDS = v164.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v164.DEFAULT_OUTPUT_ROOT.parent / "judge-calibration-v5_4-v165-alignment-truth-owner"
).resolve()


class JudgeV5CalibrationV165Error(RuntimeError):
    """The immutable v165 reference-owner contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v164() -> dict[str, Any]:
    root = v164.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "score": root / "equivalent-pair-verifier-diagnostic-score.json",
        "spec": root / "equivalent-pair-verifier-diagnostic-spec.json",
        "truth": root / "equivalent-pair-verifier-truth.private.json",
        "primary": root / "pair-primary-projected.private.json",
        "canary": root / "pair-canary-projected.private.json",
    }
    values = {name: _load_json(path, f"v164 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    attempts = [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    sidecars = []
    for row in attempts:
        record = row.get("sidecar")
        if not isinstance(record, Mapping):
            raise JudgeV5CalibrationV165Error("v164 measured sidecar coverage drifted")
        _verify_record(record)
        _validate_usage(_load_json(Path(record["path"]), "v164 sidecar"))
        sidecars.append(record)
    if (
        terminal.get("state") != "inactive"
        or terminal.get("development_terminal_reason")
        != "v164_equivalent_pair_verifier_quality_gate_not_passed"
        or terminal.get("fresh_full_replacement_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 193876
        or terminal.get("cumulative_calibration_usage", {}).get("total_tokens") != 3103996
        or score.get("failed_checks")
        != ["false_equivalent_detection_rate", "pair_exact_rate", "replacement_alignment_frozen_gates"]
        or score.get("metrics", {}).get("pair_exact_count") != 14
        or score.get("metrics", {}).get("permutation_exact_pair_count") != 15
        or score.get("metrics", {}).get("false_equivalent_pair_exact_count") != 0
        or len(attempts) != 6
        or len(sidecars) != 6
        or spec.get("maximum_turn_count") != 6
        or spec.get("retry_count_per_turn") != 0
    ):
        raise JudgeV5CalibrationV165Error("v164 quality terminal drifted")
    for record in spec["runtime_files"]:
        _verify_record(record)
    source = v164._validate_v163()
    data = v164.build_v164_inputs(source)
    if values["truth"]["pairs"] != [
        {key: deepcopy(row[key]) for key in ("pair_case_id", "original_case_id", "witness_ids", "expected")}
        for row in data["pairs"]
    ]:
        raise JudgeV5CalibrationV165Error("v164 pair truth drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "sidecars": sidecars,
        "source": source,
        "data": data,
        "cumulative_usage": terminal["cumulative_calibration_usage"],
    }


def _pair_expected(pair: Mapping[str, Any]) -> dict[str, Any]:
    witness_ids = sorted(str(value) for value in pair["witness_ids"])
    return {
        "pairs": [deepcopy(dict(pair))],
        "equivalence_groups": [witness_ids]
        if pair["relation"] == "equivalent"
        else [[witness_ids[0]], [witness_ids[1]]],
        "unpaired_witness_ids": [],
    }


def _owner_row(
    *, original_case: Mapping[str, Any], original_case_id: str,
    pair: Mapping[str, Any], role: str,
) -> dict[str, Any]:
    witness_ids = sorted(str(value) for value in pair["witness_ids"])
    witnesses = {str(row["witness_id"]): row for row in original_case["witnesses"]}
    owner_case_id = "ownercase_" + sha256_text(
        f"v165|{original_case_id}|{'|'.join(witness_ids)}"
    )[:24]
    return {
        "owner_case_id": owner_case_id,
        "original_case_id": original_case_id,
        "witness_ids": witness_ids,
        "role": role,
        "case": {
            "case_id": owner_case_id,
            "source_excerpt": original_case["source_excerpt"],
            "witnesses": [deepcopy(witnesses[key]) for key in witness_ids],
        },
        "expected": _pair_expected(pair),
    }


def build_v165_inputs(source: Mapping[str, Any]) -> dict[str, Any]:
    v159_data = source["source"]["v159_data"]
    primary_input = v164._combined_alignment_input(v159_data, "alignment_primary")
    source_cases = {str(row["case_id"]): row for row in primary_input["cases"]}
    expected = v159_data["alignment_diagnostic_expected"]
    targets = [
        row for row in source["data"]["pairs"]
        if row["expected"]["pairs"][0]["relation"] == "non_equivalent"
        and row["expected"]["pairs"][0]["mismatch_fields"] == ["stance"]
    ]
    if len(targets) != 1:
        raise JudgeV5CalibrationV165Error("v165 disputed stance target coverage drifted")
    target = targets[0]
    rows = [
        _owner_row(
            original_case=source_cases[target["original_case_id"]],
            original_case_id=target["original_case_id"],
            pair=target["expected"]["pairs"][0],
            role="disputed_reference_target",
        )
    ]
    used_cases = {target["original_case_id"]}
    candidates = []
    for case_id, case_expected in expected.items():
        for pair in case_expected["pairs"]:
            if len(pair["witness_ids"]) != 2 or case_id in used_cases:
                continue
            candidates.append((case_id, pair))
    candidates.sort(key=lambda row: sha256_text(f"v165|control|{row[0]}|{'|'.join(sorted(row[1]['witness_ids']))}"))
    partials = [row for row in candidates if row[1]["relation"] == "partial"]
    equivalents = [row for row in candidates if row[1]["relation"] == "equivalent"]
    selected = partials[:2]
    selected_case_ids = {row[0] for row in selected} | used_cases
    for row in equivalents:
        if row[0] in selected_case_ids:
            continue
        selected.append(row)
        selected_case_ids.add(row[0])
        if len(selected) == 5:
            break
    if (
        len(selected) != 5
        or sum(pair["relation"] == "partial" for _, pair in selected) != 2
        or sum(pair["relation"] == "equivalent" for _, pair in selected) != 3
    ):
        raise JudgeV5CalibrationV165Error("v165 settled control coverage drifted")
    rows.extend(
        _owner_row(
            original_case=source_cases[case_id],
            original_case_id=case_id,
            pair=pair,
            role="settled_control",
        )
        for case_id, pair in selected
    )
    rows.sort(key=lambda row: sha256_text(f"v165|owner-order|{row['owner_case_id']}"))
    primary = deepcopy(primary_input)
    primary["cases"] = [deepcopy(row["case"]) for row in rows]
    primary["permutation"] = "base"
    primary["permuted_axes"] = []
    canary = deepcopy(primary_input)
    canary["cases"] = []
    for row in reversed(rows):
        case = deepcopy(row["case"])
        case["witnesses"] = list(reversed(case["witnesses"]))
        canary["cases"].append(case)
    canary["permutation"] = "balanced_canary"
    canary["permuted_axes"] = ["anonymous_case_order", "anonymous_witness_order"]
    return {
        "rows": rows,
        "target_owner_case_id": next(row["owner_case_id"] for row in rows if row["role"] == "disputed_reference_target"),
        "target_v164_pair_case_id": target["pair_case_id"],
        "target_original_case_id": target["original_case_id"],
        "target_witness_ids": target["witness_ids"],
        "turns": [
            {"turn_name": PRIMARY_TURN, "turn_role": "owner_primary", "value": primary},
            {"turn_name": CANARY_TURN, "turn_role": "owner_canary", "value": canary},
        ],
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    audit = {
        "schema_version": V165_CAPACITY_AUDIT_VERSION,
        "phase_id": V165_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V165_CAPACITY_POLICY_VERSION,
        "phase_id": V165_PHASE_ID,
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
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "phase_total_token_bound": bound,
        "projected_phase_quota_points": math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v165(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v165 terminal")}
    source = _validate_v164()
    data = build_v165_inputs(source)
    turns = []
    for row in data["turns"]:
        prompt = v130.alignment_prompt_v130(row["value"])
        schema = neutral_alignment_output_schema(row["value"])
        paths = _freeze_turn_request(root=root, turn_name=row["turn_name"], input_value=row["value"], prompt=prompt, schema=schema)
        turns.append({**row, "prompt": prompt, "schema": schema, "paths": paths})
    truth_path = root / "alignment-truth-owner-input.private.json"
    _write_immutable(
        truth_path,
        {
            "schema_version": V165_SCORE_VERSION,
            "target_owner_case_id": data["target_owner_case_id"],
            "target_v164_pair_case_id": data["target_v164_pair_case_id"],
            "target_original_case_id": data["target_original_case_id"],
            "target_witness_ids": data["target_witness_ids"],
            "rows": [
                {key: deepcopy(row[key]) for key in ("owner_case_id", "original_case_id", "witness_ids", "role", "expected")}
                for row in data["rows"]
            ],
        },
    )
    predecessor = {
        **{f"v164_{name}": record for name, record in source["records"].items()},
        "v164_sidecars": source["sidecars"],
        "v162_reference": _record(v162.DEFAULT_OUTPUT_ROOT / "fixture-reference-v14-v162.private.json"),
        "v162_truth": _record(v162.DEFAULT_OUTPUT_ROOT / "full-calibration-truth-v162.private.json"),
        "cumulative_usage": source["cumulative_usage"],
    }
    capacity = _build_capacity_policy(root, predecessor)
    spec = {
        "schema_version": V165_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "single_side_free_reference_owner_with_balanced_permutation_and_settled_controls",
        "disputed_target_count": 1,
        "settled_control_count": 5,
        "partial_control_count": 2,
        "equivalent_control_count": 3,
        "turn_plan": list(TURN_NAMES),
        "minimum_turn_count": 2,
        "maximum_turn_count": 2,
        "retry_count_per_turn": 0,
        "target_truth_exposed_to_model": False,
        "reference_patch_requires_target_equivalent": True,
        "reference_patch_requires_all_controls_exact": True,
        "reference_patch_requires_permutation_exact": True,
        "reference_patch_requires_v164_rescore_all_gates": True,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [_record(Path(__file__)), *source["values"]["spec"]["runtime_files"]],
        "frozen_instructions": {"alignment_sha256": sha256_text(v130.alignment_instructions_v130())},
        "frozen_inputs": {
            "owner_truth": _record(truth_path),
            "v162_reference": predecessor["v162_reference"],
            "v162_truth": predecessor["v162_truth"],
            "turns": [
                {
                    "turn_name": turn["turn_name"],
                    "role": turn["turn_role"],
                    "input": _record(turn["paths"]["input"]),
                    "prompt": _record(turn["paths"]["prompt"]),
                    "schema": _record(turn["paths"]["schema"]),
                }
                for turn in turns
            ],
        },
        "privacy": "private_source_event_output_truth_and_mapping_sanitized_terminal_only",
    }
    spec_path = root / "alignment-truth-owner-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "data": data,
        "source": source,
    }


def _merged(outputs: Sequence[Mapping[str, Any]], turns: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return v164._merged_normalized(outputs, turns)


def _patch_projection(value: Mapping[str, Any], witness_ids: Sequence[str]) -> dict[str, Any]:
    patched = deepcopy(dict(value))
    wanted = tuple(sorted(str(item) for item in witness_ids))
    found = False
    for pair in patched["pairs"]:
        if tuple(sorted(pair["witness_ids"])) == wanted:
            pair["relation"] = "equivalent"
            pair["mismatch_fields"] = []
            found = True
    if not found:
        raise JudgeV5CalibrationV165Error("target pair absent from projection")
    merged = set(wanted)
    retained = []
    for group in patched["equivalence_groups"]:
        if merged.intersection(group):
            merged.update(group)
        else:
            retained.append(sorted(group))
    retained.append(sorted(merged))
    patched["equivalence_groups"] = sorted(retained)
    patched["unpaired_witness_ids"] = sorted(
        value for value in patched["unpaired_witness_ids"] if value not in merged
    )
    return patched


def _patched_v164_data(source: Mapping[str, Any], data: Mapping[str, Any]) -> dict[str, Any]:
    patched = deepcopy(source["data"])
    target_id = data["target_v164_pair_case_id"]
    for row in patched["pairs"]:
        if row["pair_case_id"] == target_id:
            row["expected"] = _patch_projection(row["expected"], data["target_witness_ids"])
            break
    else:
        raise JudgeV5CalibrationV165Error("v164 target pair absent")
    for row in patched["truth"]["alignment_cases"]:
        if row["case_id"] == data["target_original_case_id"]:
            row["expected"] = _patch_projection(row["expected"], data["target_witness_ids"])
            break
    else:
        raise JudgeV5CalibrationV165Error("v164 full truth target absent")
    return patched


def _retrospective_v164_score(
    *, source: Mapping[str, Any], data: Mapping[str, Any], patched_data: Mapping[str, Any]
) -> dict[str, Any]:
    primary = source["values"]["primary"]
    canary = source["values"]["canary"]
    expected = {row["pair_case_id"]: row["expected"] for row in patched_data["pairs"]}
    exact_count = sum(primary[key] == expected[key] for key in expected)
    permutation_exact_count = sum(primary[key] == canary[key] for key in expected)
    patched_cases = v164._patch_selected_cases(pair_results=primary, data=patched_data)
    full_expected = {
        str(row["case_id"]): row["expected"]
        for row in patched_data["truth"]["alignment_cases"]
    }
    replacement = {
        str(row["case_id"]): v130._project_alignment(row)
        for row in source["source"]["v159_source"]["source"]["values"]["reconciled"]["cases"]
    }
    replacement.update(patched_cases)
    full_metrics = v164.v158._projected_alignment_metrics(replacement, full_expected)
    checks = {
        "pair_exact_rate": exact_count == 15,
        "true_equivalent_preservation_rate": sum(
            value["pairs"][0]["relation"] == "equivalent" and primary[key] == value
            for key, value in expected.items()
        ) == 15,
        "permutation_exact_rate": permutation_exact_count == 15,
        "replacement_alignment_frozen_gates": all(
            full_metrics[key] >= 0.95
            for key in (
                "alignment_f1",
                "relation_accuracy",
                "equivalent_sensitivity",
                "equivalent_specificity",
                "mismatch_field_f1",
                "equivalence_partition_exact_case_rate",
                "unpaired_exact_case_rate",
            )
        ),
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": {
            "pair_count": len(expected),
            "pair_exact_count": exact_count,
            "true_equivalent_pair_count": 15,
            "true_equivalent_pair_exact_count": 15,
            "permutation_pair_count": len(expected),
            "permutation_exact_pair_count": permutation_exact_count,
            **full_metrics,
        },
    }


def score_v165(
    *, primary: Mapping[str, Any], canary: Mapping[str, Any],
    data: Mapping[str, Any], source: Mapping[str, Any],
) -> dict[str, Any]:
    rows = {row["owner_case_id"]: row for row in data["rows"]}
    if set(primary) != set(rows) or set(canary) != set(rows):
        raise JudgeV5CalibrationV165Error("v165 output coverage drifted")
    control_ids = sorted(key for key, row in rows.items() if row["role"] == "settled_control")
    target_id = data["target_owner_case_id"]
    primary_control_exact = sum(primary[key] == rows[key]["expected"] for key in control_ids)
    canary_control_exact = sum(canary[key] == rows[key]["expected"] for key in control_ids)
    permutation_exact = sum(primary[key] == canary[key] for key in rows)
    target_primary = primary[target_id]
    target_canary = canary[target_id]
    target_pair = target_primary["pairs"][0]
    target_equivalent = (
        target_pair["relation"] == "equivalent"
        and target_pair["mismatch_fields"] == []
        and target_primary["equivalence_groups"] == [sorted(data["target_witness_ids"])]
    )
    patched_data = _patched_v164_data(source, data)
    retrospective = _retrospective_v164_score(
        source=source, data=data, patched_data=patched_data
    ) if target_equivalent else None
    checks = {
        "primary_controls_exact": primary_control_exact == 5,
        "canary_controls_exact": canary_control_exact == 5,
        "permutation_exact": permutation_exact == 6,
        "target_owner_consistent": target_primary == target_canary,
        "target_reference_defect_confirmed": target_equivalent,
        "retrospective_v164_all_gates": bool(retrospective and retrospective["passed"]),
    }
    passed = all(checks.values())
    return {
        "schema_version": V165_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": {
            "primary_control_exact_count": primary_control_exact,
            "canary_control_exact_count": canary_control_exact,
            "control_count": 5,
            "permutation_exact_count": permutation_exact,
            "permutation_case_count": 6,
            "target_owner_consistent": target_primary == target_canary,
            "target_relation": target_pair["relation"],
            "target_mismatch_fields": target_pair["mismatch_fields"],
            "retrospective_v164_metrics": retrospective["metrics"] if retrospective else None,
        },
        "reference_patch_authorized": passed,
        "fresh_full_replacement_calibration_authorized": passed,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _patch_reference_case(case: Mapping[str, Any], witness_ids: Sequence[str]) -> dict[str, Any]:
    return _patch_projection(case, witness_ids)


def _write_corrected_reference(
    *, root: Path, data: Mapping[str, Any], source: Mapping[str, Any],
    primary_path: Path, canary_path: Path,
) -> dict[str, Path]:
    reference_path = v162.DEFAULT_OUTPUT_ROOT / "fixture-reference-v14-v162.private.json"
    truth_path = v162.DEFAULT_OUTPUT_ROOT / "full-calibration-truth-v162.private.json"
    reference = _load_json(reference_path, "v162 reference")
    full_truth = _load_json(truth_path, "v162 truth")
    case_id = data["target_original_case_id"]
    reference["cases"][case_id] = _patch_reference_case(
        reference["cases"][case_id], data["target_witness_ids"]
    )
    reference.update(
        {
            "schema_version": V165_REFERENCE_VERSION,
            "reference_version": "v15_v165",
            "v165_alignment_truth_patch_count": 1,
            "v165_reference_owner_model": MODEL,
            "fresh_full_calibration_authorized": True,
            "selection_authorized": False,
            "holdout_authorized": False,
        }
    )
    for row in full_truth["alignment_cases"]:
        if row["case_id"] == case_id:
            row["expected"] = _patch_projection(row["expected"], data["target_witness_ids"])
            break
    else:
        raise JudgeV5CalibrationV165Error("v162 full truth target absent")
    full_truth["schema_version"] = V165_TRUTH_VERSION
    full_truth["v165_alignment_truth_patch_count"] = 1
    output_reference = root / "fixture-reference-v15-v165.private.json"
    output_truth = root / "full-calibration-truth-v165.private.json"
    _write_immutable(output_reference, reference)
    _write_immutable(output_truth, full_truth)
    audit_path = root / "reference-patch-audit.json"
    _write_immutable(
        audit_path,
        {
            "schema_version": V165_AUDIT_VERSION,
            "patch_count": 1,
            "opaque_case_id": case_id,
            "opaque_witness_ids": sorted(data["target_witness_ids"]),
            "prior_relation": "non_equivalent",
            "prior_mismatch_fields": ["stance"],
            "new_relation": "equivalent",
            "new_mismatch_fields": [],
            "owner_model": MODEL,
            "owner_effort": EFFORT,
            "primary_output": _record(primary_path),
            "canary_output": _record(canary_path),
            "v164_primary": source["records"]["primary"],
            "v164_canary": source["records"]["canary"],
            "v164_terminal": source["records"]["terminal"],
            "old_reference": _record(reference_path),
            "old_truth": _record(truth_path),
            "new_reference": _record(output_reference),
            "new_truth": _record(output_truth),
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
        },
    )
    return {"reference": output_reference, "truth": output_truth, "audit": audit_path}


def _write_failure(root: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    attempts = [row for row in _attempt_records(root) if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))]
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            measured = _validate_usage(_load_json(Path(record["path"]), "v165 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    predecessor = _validate_v164()["cumulative_usage"]
    cumulative = _sum_usage(predecessor, usage)
    failure = {
        "schema_version": V165_FAILURE_VERSION,
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
        "predecessor_cumulative_usage": predecessor,
        "cumulative_known_usage_lower_bound": cumulative,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V165_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "reference_frozen": False,
        "fresh_full_replacement_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "accounting_complete": complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
        "predecessor_cumulative_usage": predecessor,
        "cumulative_known_usage_lower_bound": cumulative,
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v165(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v165 terminal")
    frozen = freeze_v165(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    outputs, sidecars = [], []
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=v130.alignment_instructions_v130(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=6,
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=turn["value"]: v157.validate_structurally_projectable_output(candidate, item),
                )
                projected, audit = v157.project_exact_spans_and_relation(output, turn["value"])
                turn_root = root / "turns" / current_turn.replace("_", "-")
                _write_immutable(turn_root / "structurally-projected.private.json", projected)
                _write_immutable(turn_root / "structural-projection-audit.json", audit)
                outputs.append(projected)
                sidecars.append(sidecar)
        primary = _merged([outputs[0]], [frozen["turns"][0]])
        canary = _merged([outputs[1]], [frozen["turns"][1]])
        primary_path = root / "owner-primary-projected.private.json"
        canary_path = root / "owner-canary-projected.private.json"
        _write_immutable(primary_path, primary)
        _write_immutable(canary_path, canary)
        score = score_v165(primary=primary, canary=canary, data=frozen["data"], source=frozen["source"])
        score_path = root / "alignment-truth-owner-score.json"
        _write_immutable(score_path, score)
        passed = bool(score["passed"])
        corrected = _write_corrected_reference(
            root=root, data=frozen["data"], source=frozen["source"],
            primary_path=primary_path, canary_path=canary_path,
        ) if passed else None
        protocol_path = root / "alignment-protocol-v165.json"
        if passed:
            _write_immutable(
                protocol_path,
                {
                    "schema_version": V165_PROTOCOL_VERSION,
                    "frozen_at": now_iso(),
                    "primary_alignment_model": "gpt-5.5",
                    "equivalent_pair_verifier_model": "gpt-5.6-sol",
                    "reference_owner_model": MODEL,
                    "reasoning_effort": EFFORT,
                    "reference": _record(corrected["reference"]),
                    "truth": _record(corrected["truth"]),
                    "reference_patch_audit": _record(corrected["audit"]),
                    "quality_gates_unchanged": True,
                    "fresh_full_replacement_calibration_authorized": True,
                    "selection_authorized": False,
                    "holdout_authorized": False,
                    "production_mutation_allowed": False,
                },
            )
        accounting = _aggregate_usage(sidecars)
        predecessor = frozen["source"]["cumulative_usage"]
        cumulative = _sum_usage(predecessor, accounting["usage"])
        terminal = {
            "schema_version": V165_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": "v165_reference_truth_corrected_full_calibration_authorized" if passed else "inactive_incomplete_recovery_required",
            "development_terminal_reason": "v165_alignment_truth_owner_passed" if passed else "v165_alignment_truth_owner_quality_gate_not_passed",
            "overall_evaluation_complete": False,
            "reference_frozen": passed,
            "fresh_full_replacement_calibration_authorized": passed,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "score": _record(score_path),
            "reference": _record(corrected["reference"]) if corrected else None,
            "truth": _record(corrected["truth"]) if corrected else None,
            "reference_patch_audit": _record(corrected["audit"]) if corrected else None,
            "protocol": _record(protocol_path) if passed else None,
            "predecessor_cumulative_usage": predecessor,
            "cumulative_calibration_usage": cumulative,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v165 alignment truth owner")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(run_v165(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds))
    print(json.dumps({
        "state": terminal["state"],
        "terminal_reason": terminal["terminal_reason"],
        "reference_frozen": terminal.get("reference_frozen", False),
        "fresh_full_replacement_calibration_authorized": terminal.get("fresh_full_replacement_calibration_authorized", False),
        "usage_status": terminal.get("usage_status"),
    }, sort_keys=True))
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
