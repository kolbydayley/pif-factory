from __future__ import annotations

"""Verify every primary-equivalent pair under two neutral permutations."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v130_retained_alignment_owner as v130
from . import app_server_judge_v5_calibration_v155_fresh_full_development as v155
from . import app_server_judge_v5_calibration_v157_exact_span_canary_recovery as v157
from . import app_server_judge_v5_calibration_v158_postprocess_quality_terminal as v158
from . import app_server_judge_v5_calibration_v159_replacement_model_diagnostic as v159
from . import app_server_judge_v5_calibration_v163_fresh_corrected_field_diagnostic as v163
from .app_server_judge_v5 import (
    neutral_alignment_output_schema,
    normalize_neutral_alignment_output,
)
from .app_server_judge_v5_calibration_v25_diagnostic import (
    QUOTA_POINTS_PER_MILLION_TOKENS,
)
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


V164_SPEC_VERSION = "pif_app_server_judge_v5_4_v164_spec_v1"
V164_SCORE_VERSION = "pif_app_server_judge_v5_4_v164_score_v1"
V164_PROTOCOL_VERSION = "pif_app_server_judge_v5_4_v164_alignment_verifier_protocol_v1"
V164_FAILURE_VERSION = "pif_app_server_judge_v5_4_v164_failure_v1"
V164_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v164_terminal_v1"
V164_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V164_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V164_PHASE_ID = "judge_v5_4_v164_equivalent_pair_verifier_diagnostic"

MODEL = "gpt-5.6-sol"
EFFORT = "high"
PRIMARY_TURNS = tuple(f"equivalent_pair_primary_{index:02d}" for index in range(3))
CANARY_TURNS = tuple(f"equivalent_pair_canary_{index:02d}" for index in range(3))
TURN_NAMES = PRIMARY_TURNS + CANARY_TURNS
PAIRS_PER_TURN = 5
MAXIMUM_TOTAL_TOKENS_PER_TURN = 45000
TIMEOUT_SECONDS = v163.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v163.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v164-equivalent-pair-verifier-diagnostic"
).resolve()


class JudgeV5CalibrationV164Error(RuntimeError):
    """The immutable v164 verifier contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v163() -> dict[str, Any]:
    root = v163.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "score": root / "fresh-corrected-field-diagnostic-score.json",
        "spec": root / "fresh-corrected-field-diagnostic-spec.json",
        "protocol": root / "field-protocol-v163.json",
        "field_output": root / "field-output.private.json",
    }
    values = {name: _load_json(path, f"v163 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    attempts = [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    sidecars = []
    for row in attempts:
        if not isinstance(row.get("sidecar"), Mapping):
            raise JudgeV5CalibrationV164Error("v163 measured sidecar coverage drifted")
        _verify_record(row["sidecar"])
        _validate_usage(_load_json(Path(row["sidecar"]["path"]), "v163 sidecar"))
        sidecars.append(row["sidecar"])
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v163_corrected_field_diagnostic_passed_alignment_verifier_authorized"
        or terminal.get("field_protocol_frozen") is not True
        or terminal.get("bounded_alignment_verifier_diagnostic_authorized") is not True
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 233682
        or terminal.get("cumulative_calibration_usage", {}).get("total_tokens")
        != 2910120
        or score.get("passed") is not True
        or score.get("failed_checks") != []
        or score.get("metrics", {}).get("field_overall_exact_count") != 11
        or score.get("metrics", {}).get("field_residual_exact_count") != 5
        or score.get("metrics", {}).get("field_control_exact_count") != 6
        or len(attempts) != 11
        or len(sidecars) != 11
        or spec.get("maximum_turn_count") != 11
        or spec.get("retry_count_per_turn") != 0
    ):
        raise JudgeV5CalibrationV164Error("v163 field protocol terminal drifted")
    for record in spec["runtime_files"]:
        _verify_record(record)
    source = v163._validate_v162()
    v159_source = v159._validate_v158()
    v159_data = v159.build_v159_inputs(v159_source)
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "sidecars": sidecars,
        "source": source,
        "v159_source": v159_source,
        "v159_data": v159_data,
        "cumulative_usage": terminal["cumulative_calibration_usage"],
    }


def _combined_alignment_input(data: Mapping[str, Any], role: str) -> dict[str, Any]:
    rows = [row for row in data["alignment_turns"] if row["turn_role"] == role]
    value = deepcopy(rows[0]["value"])
    value["cases"] = [deepcopy(case) for row in rows for case in row["value"]["cases"]]
    return value


def _v159_primary_projection(source: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    root = v159.DEFAULT_OUTPUT_ROOT
    raw = _load_json(root / "alignment-primary-projected.private.json", "v159 primary")
    alignment_input = _combined_alignment_input(source["v159_data"], "alignment_primary")
    normalized = normalize_neutral_alignment_output(raw, alignment_input)
    return {str(row["case_id"]): v130._project_alignment(row) for row in normalized["cases"]}


def build_v164_inputs(source: Mapping[str, Any]) -> dict[str, Any]:
    data = source["v159_data"]
    primary_input = _combined_alignment_input(data, "alignment_primary")
    source_cases = {str(row["case_id"]): row for row in primary_input["cases"]}
    primary = _v159_primary_projection(source)
    expected = data["alignment_diagnostic_expected"]
    pairs = []
    for case_id, projection in primary.items():
        witnesses = {str(row["witness_id"]): row for row in source_cases[case_id]["witnesses"]}
        expected_pairs = {
            tuple(sorted(row["witness_ids"])): row for row in expected[case_id]["pairs"]
        }
        for pair in projection["pairs"]:
            if pair["relation"] != "equivalent":
                continue
            witness_ids = tuple(sorted(pair["witness_ids"]))
            pair_case_id = "paircase_" + sha256_text(
                f"v164|{case_id}|{'|'.join(witness_ids)}"
            )[:24]
            wanted = expected_pairs[witness_ids]
            pairs.append(
                {
                    "pair_case_id": pair_case_id,
                    "original_case_id": case_id,
                    "witness_ids": list(witness_ids),
                    "case": {
                        "case_id": pair_case_id,
                        "source_excerpt": source_cases[case_id]["source_excerpt"],
                        "witnesses": [deepcopy(witnesses[key]) for key in witness_ids],
                    },
                    "expected": {
                        "pairs": [deepcopy(wanted)],
                        "equivalence_groups": [list(witness_ids)]
                        if wanted["relation"] == "equivalent"
                        else [[witness_ids[0]], [witness_ids[1]]],
                        "unpaired_witness_ids": [],
                    },
                }
            )
    pairs.sort(key=lambda row: sha256_text(f"v164|pair-order|{row['pair_case_id']}"))
    if len(pairs) != 15 or sum(row["expected"]["pairs"][0]["relation"] == "non_equivalent" for row in pairs) != 1:
        raise JudgeV5CalibrationV164Error("v164 equivalent-pair coverage drifted")
    shards = [pairs[index : index + PAIRS_PER_TURN] for index in range(0, 15, PAIRS_PER_TURN)]
    turns = []
    for turn_name, shard in zip(PRIMARY_TURNS, shards, strict=True):
        value = deepcopy(primary_input)
        value["cases"] = [deepcopy(row["case"]) for row in shard]
        value["permutation"] = "base"
        value["permuted_axes"] = []
        turns.append(
            {
                "turn_name": turn_name,
                "turn_role": "pair_primary",
                "pair_case_ids": [row["pair_case_id"] for row in shard],
                "value": value,
            }
        )
    for turn_name, shard in zip(CANARY_TURNS, shards, strict=True):
        value = deepcopy(primary_input)
        canary_cases = []
        for row in reversed(shard):
            case = deepcopy(row["case"])
            case["witnesses"] = list(reversed(case["witnesses"]))
            canary_cases.append(case)
        value["cases"] = canary_cases
        value["permutation"] = "balanced_canary"
        value["permuted_axes"] = ["anonymous_case_order", "anonymous_witness_order"]
        turns.append(
            {
                "turn_name": turn_name,
                "turn_role": "pair_canary",
                "pair_case_ids": [row["pair_case_id"] for row in shard],
                "value": value,
            }
        )
    return {
        "turns": turns,
        "pairs": pairs,
        "primary_projection": primary,
        "truth": data["truth"],
        "selected_case_ids": data["selected_case_ids"],
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    audit = {
        "schema_version": V164_CAPACITY_AUDIT_VERSION,
        "phase_id": V164_PHASE_ID,
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
        "schema_version": V164_CAPACITY_POLICY_VERSION,
        "phase_id": V164_PHASE_ID,
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
        "projected_phase_quota_points": math.ceil(
            bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
        ),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v164(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v164 terminal")}
    source = _validate_v163()
    data = build_v164_inputs(source)
    turns = []
    for row in data["turns"]:
        prompt = v130.alignment_prompt_v130(row["value"])
        schema = neutral_alignment_output_schema(row["value"])
        paths = _freeze_turn_request(
            root=root,
            turn_name=row["turn_name"],
            input_value=row["value"],
            prompt=prompt,
            schema=schema,
        )
        turns.append({**row, "prompt": prompt, "schema": schema, "paths": paths})
    pair_truth_path = root / "equivalent-pair-verifier-truth.private.json"
    _write_immutable(
        pair_truth_path,
        {
            "schema_version": V164_SCORE_VERSION,
            "pairs": [
                {key: deepcopy(row[key]) for key in ("pair_case_id", "original_case_id", "witness_ids", "expected")}
                for row in data["pairs"]
            ],
        },
    )
    predecessor = {
        **{f"v163_{name}": record for name, record in source["records"].items()},
        "v163_sidecars": source["sidecars"],
        "v159_primary": _record(v159.DEFAULT_OUTPUT_ROOT / "alignment-primary-projected.private.json"),
        "v159_canary": _record(v159.DEFAULT_OUTPUT_ROOT / "alignment-canary-projected.private.json"),
        "cumulative_usage": source["cumulative_usage"],
    }
    capacity = _build_capacity_policy(root, predecessor)
    spec = {
        "schema_version": V164_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "verify_every_primary_equivalent_pair_under_base_and_balanced_order",
        "primary_equivalent_pair_count": 15,
        "expected_true_equivalent_pair_count": 14,
        "expected_false_equivalent_pair_count": 1,
        "primary_turn_count": 3,
        "canary_turn_count": 3,
        "turn_plan": list(TURN_NAMES),
        "minimum_turn_count": len(TURN_NAMES),
        "maximum_turn_count": len(TURN_NAMES),
        "retry_count_per_turn": 0,
        "truth_labels_exposed_to_model": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)), *source["values"]["spec"]["runtime_files"]
        ],
        "frozen_instructions": {
            "alignment_sha256": sha256_text(v130.alignment_instructions_v130())
        },
        "frozen_inputs": {
            "pair_truth": _record(pair_truth_path),
            "field_protocol": source["records"]["protocol"],
            "reference": source["source"]["records"]["reference"],
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
    spec_path = root / "equivalent-pair-verifier-diagnostic-spec.json"
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


def _merged_normalized(
    outputs: Sequence[Mapping[str, Any]], turns: Sequence[Mapping[str, Any]]
) -> dict[str, dict[str, Any]]:
    raw = v155._merge_outputs(outputs, "cases")
    alignment_input = deepcopy(turns[0]["value"])
    alignment_input["cases"] = [deepcopy(case) for turn in turns for case in turn["value"]["cases"]]
    normalized = normalize_neutral_alignment_output(raw, alignment_input)
    return {str(row["case_id"]): v130._project_alignment(row) for row in normalized["cases"]}


def _patch_selected_cases(
    *, pair_results: Mapping[str, Mapping[str, Any]], data: Mapping[str, Any]
) -> dict[str, dict[str, Any]]:
    selected = deepcopy(data["primary_projection"])
    pair_map = {row["pair_case_id"]: row for row in data["pairs"]}
    for pair_case_id, result in pair_results.items():
        mapping = pair_map[pair_case_id]
        original = selected[mapping["original_case_id"]]
        key = tuple(sorted(mapping["witness_ids"]))
        replacement = result["pairs"][0]
        for index, pair in enumerate(original["pairs"]):
            if tuple(sorted(pair["witness_ids"])) == key:
                original["pairs"][index] = deepcopy(replacement)
                break
        else:
            raise JudgeV5CalibrationV164Error("v164 pair patch target drifted")
    for case in selected.values():
        groups = []
        paired = set()
        for pair in case["pairs"]:
            ids = sorted(pair["witness_ids"])
            paired.update(ids)
            groups.extend([ids] if pair["relation"] == "equivalent" else [[ids[0]], [ids[1]]])
        groups.extend([[witness_id] for witness_id in case["unpaired_witness_ids"] if witness_id not in paired])
        case["equivalence_groups"] = sorted(groups)
    return selected


def score_v164(
    *,
    primary_output: Mapping[str, Mapping[str, Any]],
    canary_output: Mapping[str, Mapping[str, Any]],
    data: Mapping[str, Any],
    source: Mapping[str, Any],
) -> dict[str, Any]:
    expected = {row["pair_case_id"]: row["expected"] for row in data["pairs"]}
    if set(primary_output) != set(expected) or set(canary_output) != set(expected):
        raise JudgeV5CalibrationV164Error("v164 score coverage drifted")
    exact = {key: primary_output[key] == expected[key] for key in expected}
    canary_exact = {key: primary_output[key] == canary_output[key] for key in expected}
    false_equivalent_ids = {
        key for key, value in expected.items() if value["pairs"][0]["relation"] != "equivalent"
    }
    true_equivalent_ids = set(expected) - false_equivalent_ids
    patched = _patch_selected_cases(pair_results=primary_output, data=data)
    full_expected = {
        str(row["case_id"]): row["expected"] for row in data["truth"]["alignment_cases"]
    }
    replacement = {
        str(row["case_id"]): v130._project_alignment(row)
        for row in source["v159_source"]["source"]["values"]["reconciled"]["cases"]
    }
    replacement.update(patched)
    full_metrics = v158._projected_alignment_metrics(replacement, full_expected)
    metrics = {
        "pair_count": len(expected),
        "pair_exact_count": sum(exact.values()),
        "false_equivalent_pair_count": len(false_equivalent_ids),
        "false_equivalent_pair_exact_count": sum(exact[key] for key in false_equivalent_ids),
        "true_equivalent_pair_count": len(true_equivalent_ids),
        "true_equivalent_pair_exact_count": sum(exact[key] for key in true_equivalent_ids),
        "permutation_exact_pair_count": sum(canary_exact.values()),
        "permutation_pair_count": len(canary_exact),
        "abstention_pair_count": 0,
        **full_metrics,
    }
    checks = {
        "pair_exact_rate": metrics["pair_exact_count"] == 15,
        "false_equivalent_detection_rate": metrics["false_equivalent_pair_exact_count"] == 1,
        "true_equivalent_preservation_rate": metrics["true_equivalent_pair_exact_count"] == 14,
        "permutation_exact_rate": metrics["permutation_exact_pair_count"] == 15,
        "replacement_alignment_frozen_gates": all(
            metrics[key] >= 0.95
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
    passed = all(checks.values())
    return {
        "schema_version": V164_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": metrics,
        "alignment_verifier_protocol_frozen": passed,
        "fresh_full_replacement_calibration_authorized": passed,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


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
            measured = _validate_usage(_load_json(Path(record["path"]), "v164 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    predecessor = _validate_v163()["cumulative_usage"]
    cumulative = _sum_usage(predecessor, usage)
    failure = {
        "schema_version": V164_FAILURE_VERSION,
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
        "schema_version": V164_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "alignment_verifier_protocol_frozen": False,
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


async def run_v164(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v164 terminal")
    frozen = freeze_v164(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    sidecars, primary_outputs, canary_outputs = [], [], []
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
                    batch_size=len(turn["value"]["cases"]),
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=turn["value"]: v157.validate_structurally_projectable_output(candidate, item),
                )
                projected, audit = v157.project_exact_spans_and_relation(output, turn["value"])
                turn_root = root / "turns" / current_turn.replace("_", "-")
                _write_immutable(turn_root / "structurally-projected.private.json", projected)
                _write_immutable(turn_root / "structural-projection-audit.json", audit)
                (primary_outputs if turn["turn_role"] == "pair_primary" else canary_outputs).append(projected)
                sidecars.append(sidecar)
        primary_turns = [row for row in frozen["turns"] if row["turn_role"] == "pair_primary"]
        canary_turns = [row for row in frozen["turns"] if row["turn_role"] == "pair_canary"]
        primary = _merged_normalized(primary_outputs, primary_turns)
        canary = _merged_normalized(canary_outputs, canary_turns)
        _write_immutable(root / "pair-primary-projected.private.json", primary)
        _write_immutable(root / "pair-canary-projected.private.json", canary)
        score = score_v164(primary_output=primary, canary_output=canary, data=frozen["data"], source=frozen["source"])
        score_path = root / "equivalent-pair-verifier-diagnostic-score.json"
        _write_immutable(score_path, score)
        passed = bool(score["passed"])
        protocol_path = root / "alignment-verifier-protocol-v164.json"
        if passed:
            _write_immutable(
                protocol_path,
                {
                    "schema_version": V164_PROTOCOL_VERSION,
                    "frozen_at": now_iso(),
                    "primary_alignment_model": "gpt-5.5",
                    "equivalent_pair_verifier_model": MODEL,
                    "reasoning_effort": EFFORT,
                    "verifier_trigger": "every_pair_primary_marks_equivalent",
                    "structural_projection_operations": ["retain_only_exact_source_substrings", "derive_relation_from_frozen_llm_checklist_precedence"],
                    "field_protocol": frozen["source"]["records"]["protocol"],
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
            "schema_version": V164_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": "v164_equivalent_pair_verifier_passed_full_calibration_authorized" if passed else "inactive_incomplete_recovery_required",
            "development_terminal_reason": "v164_equivalent_pair_verifier_diagnostic_passed" if passed else "v164_equivalent_pair_verifier_quality_gate_not_passed",
            "overall_evaluation_complete": False,
            "alignment_verifier_protocol_frozen": passed,
            "fresh_full_replacement_calibration_authorized": passed,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "score": _record(score_path),
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
    parser = argparse.ArgumentParser(description="Run v164 equivalent-pair verifier diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(run_v164(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds))
    print(json.dumps({"state": terminal["state"], "terminal_reason": terminal["terminal_reason"], "alignment_verifier_protocol_frozen": terminal.get("alignment_verifier_protocol_frozen", False), "fresh_full_replacement_calibration_authorized": terminal.get("fresh_full_replacement_calibration_authorized", False), "usage_status": terminal.get("usage_status")}, sort_keys=True))
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
