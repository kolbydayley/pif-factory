from __future__ import annotations

"""Terra pointwise diagnostic on 15 fresh retained cases plus support controls."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v108_layered_diagnostic as v108
from .app_server_judge_v5 import build_pointwise_support_input
from .app_server_judge_v5_calibration import make_v5_calibration_pool, pointwise_input_subset
from .app_server_judge_v5_calibration_v25_diagnostic import QUOTA_POINTS_PER_MILLION_TOKENS
from .app_server_judge_v5_calibration_v26_diagnostic import (
    MAX_TOKENS_PER_TURN,
    JudgeV5CalibrationV26DiagnosticAttemptFailed,
    _aggregate_usage,
    _client_factory,
    _freeze_turn_request,
    _get_or_run_turn,
    _load_json,
    _merge_outputs,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import _record, _verify_record
from .app_server_judge_v5_calibration_v120_retained_case_diagnostic import (
    _balanced_ids,
    _validate_v119,
)
from .app_server_judge_v5_calibration_v121_retained_field_owner import _validate_v120
from .app_server_judge_v5_calibration_v132_luna_singleton_alignment_owner import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V132_ROOT,
    _validate_v131,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V133_SELECTION_VERSION = "pif_app_server_judge_v5_4_v133_selection_v1"
V133_TRUTH_VERSION = "pif_app_server_judge_v5_4_v133_truth_v1"
V133_SPEC_VERSION = "pif_app_server_judge_v5_4_v133_spec_v1"
V133_SCORE_VERSION = "pif_app_server_judge_v5_4_v133_score_v1"
V133_FAILURE_VERSION = "pif_app_server_judge_v5_4_v133_failure_v1"
V133_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v133_terminal_v1"
V133_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V133_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V133_PHASE_ID = "judge_v5_4_v133_terra_pointwise_diagnostic"

MODEL = "gpt-5.6-terra"
EFFORT = "high"
PRIMARY_TURNS = tuple(f"terra_pointwise_shard_{index:02d}" for index in range(3))
CANARY_TURN = "terra_pointwise_order_canary"
TURN_NAMES = PRIMARY_TURNS + (CANARY_TURN,)
CASES_PER_SHARD = 6
CASE_COUNT = 18
FRESH_CASE_COUNT = 15
SUPPORT_CONTROL_COUNT = 3
CANARY_CASE_COUNT = 6
TIMEOUT_SECONDS = 1200.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V132_ROOT.parent / "judge-calibration-v5_4-v133-terra-pointwise-diagnostic"
).resolve()


class JudgeV5CalibrationV133Error(RuntimeError):
    """The v133 Terra pointwise diagnostic contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v132() -> dict[str, Any]:
    root = DEFAULT_V132_ROOT
    paths = {
        "spec": root / "luna-singleton-alignment-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "luna-singleton-alignment-score.json",
        "outputs": root / "luna-singleton-alignment-outputs.private.json",
        "reference": root / "calibration-truth-v10-retained-alignment-luna-singleton.private.json",
        "truth": root / "luna-singleton-alignment-truth.private.json",
        "selection": root / "selection-audit.json",
    }
    values = {name: _load_json(path, f"v132 {name}") for name, path in paths.items()}
    terminal, score, spec, reference = (
        values["terminal"], values["score"], values["spec"], values["reference"]
    )
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v132_luna_singleton_owner_passed_reference_frozen"
        or terminal.get("alignment_reference_frozen") is not True
        or terminal.get("reference_frozen") is not True
        or terminal.get("fresh_diagnostic_authorized") is not True
        or terminal.get("fresh_full_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 103943
        or score.get("passed") is not True
        or score.get("failed_checks") != []
        or score.get("metrics", {}).get("singleton_control_exact_count") != 3
        or score.get("metrics", {}).get("singleton_owner_abstention_count") != 0
        or spec.get("model") != "gpt-5.6-luna"
        or spec.get("retry_count_per_turn") != 0
        or spec.get("maximum_cases_per_turn") != 1
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
        or reference.get("alignment_reference_frozen") is not True
        or reference.get("fresh_diagnostic_authorized") is not True
        or reference.get("fresh_full_calibration_authorized") is not False
        or len(reference.get("cases") or {}) != 66
    ):
        raise JudgeV5CalibrationV133Error("v132 predecessor contract drifted")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5CalibrationV133Error("v132 runtime record drifted")
    for name, key in {"score": "score", "outputs": "outputs", "reference": "reference"}.items():
        record = terminal.get(key)
        if not isinstance(record, Mapping) or dict(record) != _record(paths[name]) or not _verify_record(record):
            raise JudgeV5CalibrationV133Error(f"v132 {name} record drifted")
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
                raise JudgeV5CalibrationV133Error("v132 turn coverage is incomplete")
            records[name] = _record(path)
        measured = _validate_usage(_load_json(Path(records["sidecar"]["path"]), "v132 sidecar"))
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        attempts[turn_name] = records
    if usage != terminal["usage"]:
        raise JudgeV5CalibrationV133Error("v132 usage aggregate drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "usage": usage,
        "v131": _validate_v131(),
    }


def build_v133_selection(
    v132: Mapping[str, Any], v119: Mapping[str, Any], v120: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    reference = v132["values"]["reference"]
    audited = set(v119["audited_case_ids"])
    repair_lineage = set(v120["values"]["truth"]["cases"])
    candidates = sorted(set(reference["cases"]) - audited - repair_lineage)
    if len(candidates) != 30:
        raise JudgeV5CalibrationV133Error("v133 untouched retained cohort drifted")
    fresh = _balanced_ids(candidates, reference, FRESH_CASE_COUNT, "v133-fresh")
    support_candidates = sorted(
        (
            case_id
            for case_id in audited
            if "unsupported" in reference["cases"][case_id]["proposition"].values()
        ),
        key=lambda case_id: sha256_text(f"v133|support-control|{case_id}"),
    )
    support_controls = support_candidates[:SUPPORT_CONTROL_COUNT]
    if len(support_controls) != SUPPORT_CONTROL_COUNT:
        raise JudgeV5CalibrationV133Error("v133 audited support-control coverage drifted")
    selected = sorted(
        fresh + support_controls,
        key=lambda case_id: sha256_text(f"v133|selected-order|{case_id}"),
    )
    canary = _balanced_ids(selected, reference, CANARY_CASE_COUNT, "v133-canary")
    unsupported_witness_count = sum(
        status == "unsupported"
        for case_id in selected
        for status in reference["cases"][case_id]["proposition"].values()
    )
    if unsupported_witness_count < SUPPORT_CONTROL_COUNT:
        raise JudgeV5CalibrationV133Error("v133 support-specificity coverage is insufficient")
    expected = deepcopy(reference)
    expected["schema_version"] = V133_TRUTH_VERSION
    expected["cases"] = {
        case_id: deepcopy(reference["cases"][case_id]) for case_id in selected
    }
    expected["canary_case_ids"] = list(canary)
    selection = {
        "schema_version": V133_SELECTION_VERSION,
        "created_at": now_iso(),
        "candidate_case_count": 30,
        "selected_case_count": CASE_COUNT,
        "fresh_case_count": FRESH_CASE_COUNT,
        "audited_support_control_count": SUPPORT_CONTROL_COUNT,
        "canary_case_count": CANARY_CASE_COUNT,
        "unsupported_witness_count": unsupported_witness_count,
        "selected": list(selected),
        "canary": list(canary),
        "v119_audited_overlap_count": len(set(selected) & audited),
        "v120_v132_repair_lineage_overlap_count": len(set(selected) & repair_lineage),
        "selection_uses_source_text": False,
        "selection_uses_only_frozen_shape_support_labels_and_provenance": True,
        "model_outputs_used_for_selection": False,
        "privacy": "opaque_case_ids_and_aggregate_counts_only",
    }
    return selection, expected


def _case_shards(case_ids: Sequence[str]) -> list[list[str]]:
    shards = [list(case_ids[index : index + CASES_PER_SHARD]) for index in range(0, len(case_ids), CASES_PER_SHARD)]
    if len(shards) != 3 or any(len(shard) != CASES_PER_SHARD for shard in shards):
        raise JudgeV5CalibrationV133Error("v133 shard coverage drifted")
    return shards


def _ratio(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 6) if denominator else 0.0


def _f1(tp: int, fp: int, fn: int) -> float:
    denominator = 2 * tp + fp + fn
    return round(2 * tp / denominator, 6) if denominator else 1.0


def score_v133(
    *, pointwise: Mapping[str, Any], canary: Mapping[str, Any], expected: Mapping[str, Any]
) -> dict[str, Any]:
    observed = {str(row["witness_id"]): row for row in pointwise["units"]}
    repeated = {str(row["witness_id"]): row for row in canary["units"]}
    support_tp = support_fn = support_tn = support_fp = 0
    structured_correct = field_tp = field_fp = field_fn = abstentions = 0
    witness_count = 0
    for case_id, case in expected["cases"].items():
        for witness_id, wanted_support in case["proposition"].items():
            row = observed[witness_id]
            witness_count += 1
            got_support = row["proposition_verdict"]
            abstentions += int(got_support == "abstain")
            if wanted_support == "supported" and got_support == "supported":
                support_tp += 1
            elif wanted_support == "supported":
                support_fn += 1
            elif got_support == "unsupported":
                support_tn += 1
            else:
                support_fp += 1
            structured_correct += int(
                row["structured_field_verdict"] == case["structured_fields"][witness_id]
            )
            wanted_fields = set(case["field_issues"][witness_id])
            got_fields = set(row["field_issue_fields"])
            field_tp += len(wanted_fields & got_fields)
            field_fp += len(got_fields - wanted_fields)
            field_fn += len(wanted_fields - got_fields)
    canary_case_exact = 0
    for case_id in expected["canary_case_ids"]:
        witness_ids = expected["cases"][case_id]["proposition"]
        exact = all(
            (
                observed[witness_id]["proposition_verdict"],
                observed[witness_id]["structured_field_verdict"],
                sorted(observed[witness_id]["field_issue_fields"]),
            )
            == (
                repeated[witness_id]["proposition_verdict"],
                repeated[witness_id]["structured_field_verdict"],
                sorted(repeated[witness_id]["field_issue_fields"]),
            )
            for witness_id in witness_ids
        )
        canary_case_exact += int(exact)
    metrics = {
        "case_count": len(expected["cases"]),
        "witness_count": witness_count,
        "support_sensitivity": _ratio(support_tp, support_tp + support_fn),
        "support_specificity": _ratio(support_tn, support_tn + support_fp),
        "structured_field_accuracy": _ratio(structured_correct, witness_count),
        "pointwise_field_issue_f1": _f1(field_tp, field_fp, field_fn),
        "abstention_count": abstentions,
        "canary_case_count": len(expected["canary_case_ids"]),
        "canary_case_exact_count": canary_case_exact,
        "order_bias": _ratio(len(expected["canary_case_ids"]) - canary_case_exact, len(expected["canary_case_ids"])),
    }
    checks = {
        "case_count": metrics["case_count"] == CASE_COUNT,
        "support_sensitivity": metrics["support_sensitivity"] >= 0.95,
        "support_specificity": metrics["support_specificity"] >= 0.95,
        "structured_field_accuracy": metrics["structured_field_accuracy"] >= 0.95,
        "pointwise_field_issue_f1": metrics["pointwise_field_issue_f1"] >= 0.95,
        "abstention_count": abstentions == 0,
        "canary_case_count": metrics["canary_case_count"] == CANARY_CASE_COUNT,
        "order_bias": metrics["order_bias"] == 0.0,
        "schema_and_exact_evidence_validation": True,
    }
    passed = all(checks.values())
    return {
        "schema_version": V133_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": metrics,
        "alignment_diagnostic_authorized": passed,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path, policy_path = root / "capacity-policy-audit.json", root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V133_CAPACITY_AUDIT_VERSION,
        "phase_id": V133_PHASE_ID,
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
        "schema_version": V133_CAPACITY_POLICY_VERSION,
        "phase_id": V133_PHASE_ID,
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


def freeze_v133(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v133 terminal")}
    v132, v119, v120 = _validate_v132(), _validate_v119(), _validate_v120()
    selection, expected = build_v133_selection(v132, v119, v120)
    pool, _, _ = make_v5_calibration_pool()
    pointwise_full = build_pointwise_support_input(pool)
    selected_input = pointwise_input_subset(pointwise_full, selection["selected"])
    shards = _case_shards(selection["selected"])
    _write_stable_time(root / "selection-audit.json", selection, "created_at")
    _write_immutable(root / "terra-pointwise-truth.private.json", expected)
    _write_immutable(root / "pointwise-input.private.json", selected_input)
    turns = []
    for turn_name, case_ids in zip(PRIMARY_TURNS, shards, strict=True):
        value = pointwise_input_subset(selected_input, case_ids)
        prompt, schema = v108.build_pointwise_checklist_prompt(value), v108.pointwise_checklist_schema(value)
        paths = _freeze_turn_request(
            root=root, turn_name=turn_name, input_value=value, prompt=prompt, schema=schema
        )
        turns.append({"turn_name": turn_name, "role": "primary", "value": value, "prompt": prompt, "schema": schema, "paths": paths})
    canary_value = pointwise_input_subset(selected_input, selection["canary"])
    canary_value["units"] = list(reversed(canary_value["units"]))
    canary_value["permutation_canary"] = True
    prompt, schema = v108.build_pointwise_checklist_prompt(canary_value), v108.pointwise_checklist_schema(canary_value)
    paths = _freeze_turn_request(
        root=root, turn_name=CANARY_TURN, input_value=canary_value, prompt=prompt, schema=schema
    )
    turns.append({"turn_name": CANARY_TURN, "role": "canary", "value": canary_value, "prompt": prompt, "schema": schema, "paths": paths})
    predecessor = {
        **{f"v132_{name}": record for name, record in v132["records"].items()},
        "v132_attempts": v132["attempts"],
        **{f"v119_{name}": record for name, record in v119["records"].items()},
        **{f"v120_{name}": record for name, record in v120["records"].items()},
    }
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V133_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "terra_pointwise_diagnostic_on_15_fresh_retained_cases_plus_3_audited_unsupported_controls",
        "candidate_case_count": 30,
        "case_count": CASE_COUNT,
        "fresh_case_count": FRESH_CASE_COUNT,
        "audited_support_control_count": SUPPORT_CONTROL_COUNT,
        "canary_case_count": CANARY_CASE_COUNT,
        "cases_per_shard": CASES_PER_SHARD,
        "turn_plan": list(TURN_NAMES),
        "prior_model_outputs_reused": False,
        "reference_truth_exposed_to_model": False,
        "explicit_pointwise_15_field_checklist": True,
        "retry_count_per_turn": 0,
        "promotion_rule": "all_support_field_abstention_exact_evidence_and_zero_order_bias_gates",
        "alignment_diagnostic_authorized": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v132_luna_singleton_alignment_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v120_retained_case_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v108_layered_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration.py"),
            _record(runtime_dir / "app_server_judge_v5.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_instruction_hash": sha256_text(v108.pointwise_checklist_instructions()),
        "frozen_inputs": {
            "selection": _record(root / "selection-audit.json"),
            "truth": _record(root / "terra-pointwise-truth.private.json"),
            "pointwise": _record(root / "pointwise-input.private.json"),
            "turns": [
                {
                    "turn_name": turn["turn_name"],
                    "role": turn["role"],
                    "input": _record(turn["paths"]["input"]),
                    "prompt": _record(turn["paths"]["prompt"]),
                    "schema": _record(turn["paths"]["schema"]),
                }
                for turn in turns
            ],
        },
        "privacy": "private_source_event_outputs_no_source_text_in_reports",
    }
    spec_path = root / "terra-pointwise-diagnostic-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "selected_input": selected_input,
        "expected": expected,
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v133 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V133_FAILURE_VERSION,
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
        "schema_version": V133_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "alignment_diagnostic_authorized": False,
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


async def run_v133(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v133 terminal")
    frozen = freeze_v133(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    try:
        primary_outputs, sidecars, canary_output = [], [], None
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=v108.pointwise_checklist_instructions(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(turn["value"]["units"]),
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=turn["value"]: v108.validate_pointwise_checklist_output(candidate, item),
                )
                sidecars.append(sidecar)
                if turn["role"] == "primary":
                    primary_outputs.append(output)
                else:
                    canary_output = output
        checklist = _merge_outputs(primary_outputs, "units")
        if v108.validate_pointwise_checklist_output(checklist, frozen["selected_input"]):
            raise JudgeV5CalibrationV133Error("v133 aggregate checklist invalid")
        pointwise = v108.project_pointwise_checklist(checklist)
        canary = v108.project_pointwise_checklist(canary_output)
        paths = {
            "checklist": root / "terra-pointwise-checklist.private.json",
            "pointwise": root / "terra-pointwise-output.private.json",
            "canary": root / "terra-pointwise-canary-output.private.json",
            "score": root / "terra-pointwise-diagnostic-score.json",
        }
        _write_immutable(paths["checklist"], checklist)
        _write_immutable(paths["pointwise"], pointwise)
        _write_immutable(paths["canary"], canary)
        score = score_v133(pointwise=pointwise, canary=canary, expected=frozen["expected"])
        _write_immutable(paths["score"], score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V133_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v133_terra_pointwise_diagnostic_passed_alignment_authorized"
                if passed else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v133_terra_pointwise_quality_gates_passed"
                if passed else "v133_terra_pointwise_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "pointwise_diagnostic_passed": passed,
            "alignment_diagnostic_authorized": passed,
            "fresh_full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(paths["score"]),
            "pointwise_output": _record(paths["pointwise"]),
            "canary_output": _record(paths["canary"]),
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v133 Terra pointwise diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v133(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "pointwise_diagnostic_passed": terminal.get("pointwise_diagnostic_passed", False),
                "alignment_diagnostic_authorized": terminal.get("alignment_diagnostic_authorized", False),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
