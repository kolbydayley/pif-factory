from __future__ import annotations

"""Singleton GPT-5.4 owner for the sole v130 alignment order disagreement."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .app_server_judge_v5 import neutral_alignment_output_schema, normalize_neutral_alignment_output
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
from .app_server_judge_v5_calibration_v130_retained_alignment_owner import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V130_ROOT,
    _case_has_abstention,
    _support_only_projection,
    _validate_v129,
    alignment_instructions_v130,
    alignment_prompt_v130,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V131_TRUTH_VERSION = "pif_app_server_judge_v5_4_v131_singleton_alignment_truth_v1"
V131_SELECTION_VERSION = "pif_app_server_judge_v5_4_v131_selection_v1"
V131_SPEC_VERSION = "pif_app_server_judge_v5_4_v131_spec_v1"
V131_SCORE_VERSION = "pif_app_server_judge_v5_4_v131_score_v1"
V131_OUTPUT_VERSION = "pif_app_server_judge_v5_4_v131_singleton_alignment_output_v1"
V131_REFERENCE_VERSION = (
    "pif_app_server_judge_v5_4_calibration_truth_v10_retained_alignment_singleton_frozen"
)
V131_FAILURE_VERSION = "pif_app_server_judge_v5_4_v131_failure_v1"
V131_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v131_terminal_v1"
V131_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V131_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V131_PHASE_ID = "judge_v5_4_v131_singleton_alignment_repair"

MODEL = "gpt-5.4"
EFFORT = "high"
CONTROL_TURNS = tuple(f"singleton_alignment_control_{index:02d}" for index in range(3))
OWNER_TURN = "singleton_alignment_owner"
TURN_NAMES = CONTROL_TURNS + (OWNER_TURN,)
CONTROL_COUNT = 3
TIMEOUT_SECONDS = 1200.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V130_ROOT.parent / "judge-calibration-v5_4-v131-singleton-alignment-repair"
).resolve()


class JudgeV5CalibrationV131Error(RuntimeError):
    """The v131 singleton alignment repair contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v130() -> dict[str, Any]:
    root = DEFAULT_V130_ROOT
    paths = {
        "spec": root / "alignment-owner-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "alignment-owner-score.json",
        "truth": root / "alignment-owner-truth.private.json",
        "input": root / "alignment-owner-input.private.json",
        "canary_input": root / "alignment-owner-canary-input.private.json",
        "primary": root / "alignment-owner-output.private.json",
        "canary": root / "alignment-owner-canary-output.private.json",
        "primary_normalized": root / "alignment-owner-normalized.private.json",
        "canary_normalized": root / "alignment-owner-canary-normalized.private.json",
        "selection": root / "selection-audit.json",
        "support": root / "support-receipts-v10.private.json",
    }
    values = {name: _load_json(path, f"v130 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v130_retained_alignment_owner_repair_or_recovery_required"
        or terminal.get("proposition_reference_frozen") is not True
        or terminal.get("alignment_reference_frozen") is not False
        or terminal.get("fresh_diagnostic_authorized") is not False
        or terminal.get("fresh_full_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 95626
        or score.get("passed") is not False
        or score.get("failed_checks") != ["permutation_canary_exact_rate"]
        or score.get("metrics", {}).get("unanimous_control_exact_count") != 6
        or score.get("metrics", {}).get("primary_abstention_case_count") != 0
        or score.get("metrics", {}).get("permutation_canary_exact_count") != 5
        or score.get("metrics", {}).get("observable_repair_trigger_count") != 1
        or spec.get("model") != "gpt-5.6-sol"
        or spec.get("retry_count_per_turn") != 0
        or spec.get("support_positive_witnesses_only") is not True
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV131Error("v130 predecessor contract drifted")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5CalibrationV131Error("v130 runtime record drifted")
    for name, key in {"score": "score", "primary": "primary_output", "canary": "canary_output"}.items():
        record = terminal.get(key)
        if not isinstance(record, Mapping) or dict(record) != _record(paths[name]) or not _verify_record(record):
            raise JudgeV5CalibrationV131Error(f"v130 {name} record drifted")
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
                raise JudgeV5CalibrationV131Error("v130 turn coverage is incomplete")
            records[name] = _record(path)
        measured = _validate_usage(_load_json(Path(records["sidecar"]["path"]), "v130 sidecar"))
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        attempts[turn_name] = records
    if usage != terminal["usage"]:
        raise JudgeV5CalibrationV131Error("v130 usage aggregate drifted")
    normalized_primary = normalize_neutral_alignment_output(values["primary"], values["input"])
    normalized_canary = normalize_neutral_alignment_output(values["canary"], values["canary_input"])
    if normalized_primary != values["primary_normalized"] or normalized_canary != values["canary_normalized"]:
        raise JudgeV5CalibrationV131Error("v130 normalized output drifted")
    triggers = score["observable_repair_triggers"]
    if len(triggers) != 1 or triggers[0].get("reason") != "canary_disagreement":
        raise JudgeV5CalibrationV131Error("v130 singleton trigger drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "usage": usage,
        "trigger_case_id": str(triggers[0]["case_id"]),
        "v129": _validate_v129(),
    }


def _select_controls(truth_rows: Sequence[Mapping[str, Any]]) -> list[str]:
    buckets: dict[str, list[str]] = {}
    for row in truth_rows:
        if row["role"] == "unanimous_control":
            buckets.setdefault(str(row["shape"]), []).append(str(row["case_id"]))
    for shape, values in buckets.items():
        values.sort(key=lambda case_id: sha256_text(f"v131|control|{shape}|{case_id}"))
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
        raise JudgeV5CalibrationV131Error("v131 singleton control coverage drifted")
    return selected


def _singleton_value(base: Mapping[str, Any], case: Mapping[str, Any], turn_name: str) -> dict[str, Any]:
    value = {key: deepcopy(item) for key, item in base.items() if key != "cases"}
    rendered = deepcopy(case)
    rendered["witnesses"] = sorted(
        rendered["witnesses"],
        key=lambda witness: sha256_text(
            f"v131|witness-order|{turn_name}|{witness['witness_id']}"
        ),
    )
    value["cases"] = [rendered]
    value["permutation"] = "v131_singleton_origin_neutral"
    value["permuted_axes"] = ["isolated_case", "hash_bound_witness_order"]
    value["singleton_context"] = True
    return value


def build_v131_inputs(
    v130: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    source = {str(case["case_id"]): case for case in v130["values"]["input"]["cases"]}
    truth_rows = {str(row["case_id"]): row for row in v130["values"]["truth"]["cases"]}
    trigger = v130["trigger_case_id"]
    if trigger not in source or truth_rows[trigger]["role"] != "alignment_dispute":
        raise JudgeV5CalibrationV131Error("v131 owner source coverage drifted")
    control_ids = _select_controls(list(truth_rows.values()))
    rows = []
    for turn_name, case_id in zip(CONTROL_TURNS, control_ids, strict=True):
        rows.append(
            {
                "turn_name": turn_name,
                "role": "singleton_control",
                "case_id": case_id,
                "value": _singleton_value(v130["values"]["input"], source[case_id], turn_name),
            }
        )
    rows.append(
        {
            "turn_name": OWNER_TURN,
            "role": "singleton_owner",
            "case_id": trigger,
            "value": _singleton_value(v130["values"]["input"], source[trigger], OWNER_TURN),
        }
    )
    truth = {
        "schema_version": V131_TRUTH_VERSION,
        "case_count": 4,
        "singleton_control_count": CONTROL_COUNT,
        "singleton_owner_count": 1,
        "cases": [
            {
                "turn_name": row["turn_name"],
                "case_id": row["case_id"],
                "role": row["role"],
                "current_reference": deepcopy(truth_rows[row["case_id"]]["current_reference"]),
                "proposition": deepcopy(truth_rows[row["case_id"]]["proposition"]),
            }
            for row in rows
        ],
    }
    selection = {
        "schema_version": V131_SELECTION_VERSION,
        "created_at": now_iso(),
        "observable_repair_trigger_count": 1,
        "trigger_reason": "v130_permutation_canary_disagreement",
        "singleton_control_count": CONTROL_COUNT,
        "singleton_owner_count": 1,
        "maximum_cases_per_turn": 1,
        "support_positive_witnesses_only": True,
        "selection_uses_source_text": False,
        "prior_alignment_outputs_in_model_input": False,
        "fixture_truth_in_model_input": False,
        "trigger_reason_in_model_input": False,
        "system_identity_in_model_input": False,
        "singleton_owner_is_decisive": True,
        "majority_voting_used": False,
        "privacy": "opaque_ids_and_aggregate_counts_only",
    }
    return rows, truth, selection


def score_v131(
    outputs: Mapping[str, Mapping[str, Any]], truth: Mapping[str, Any]
) -> dict[str, Any]:
    expected = {str(row["turn_name"]): row for row in truth["cases"]}
    if set(outputs) != set(expected):
        raise JudgeV5CalibrationV131Error("v131 score turn coverage drifted")
    controls = [row for row in expected.values() if row["role"] == "singleton_control"]
    owner = next(row for row in expected.values() if row["role"] == "singleton_owner")
    control_exact = 0
    abstentions = 0
    for turn_name, row in expected.items():
        cases = outputs[turn_name].get("cases") or []
        if len(cases) != 1 or str(cases[0].get("case_id")) != row["case_id"]:
            raise JudgeV5CalibrationV131Error("v131 singleton output coverage drifted")
        abstentions += int(_case_has_abstention(cases[0]))
        if row["role"] == "singleton_control":
            control_exact += int(
                _support_only_projection(cases[0], row["proposition"])
                == row["current_reference"]
            )
    owner_abstention = _case_has_abstention(outputs[owner["turn_name"]]["cases"][0])
    checks = {
        "singleton_control_exact_rate": control_exact == CONTROL_COUNT,
        "singleton_owner_abstention_count": not owner_abstention,
        "all_turn_abstention_count": abstentions == 0,
        "singleton_context_by_construction": True,
        "schema_and_exact_evidence_validation": True,
    }
    passed = all(checks.values())
    return {
        "schema_version": V131_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": {
            "case_count": 4,
            "singleton_control_count": CONTROL_COUNT,
            "singleton_control_exact_count": control_exact,
            "singleton_owner_count": 1,
            "singleton_owner_abstention_count": int(owner_abstention),
            "all_turn_abstention_count": abstentions,
            "inherited_v130_stable_canary_count": 5,
            "inherited_v130_exact_control_count": 6,
        },
        "alignment_reference_frozen": passed,
        "fresh_diagnostic_authorized": passed,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "singleton_owner_is_decisive": True,
        "majority_voting_used": False,
    }


def reconcile_alignment_reference(
    *, v130: Mapping[str, Any], outputs: Mapping[str, Mapping[str, Any]], truth: Mapping[str, Any]
) -> dict[str, Any]:
    candidate = deepcopy(v130["v129"]["values"]["reference"])
    v130_truth = {str(row["case_id"]): row for row in v130["values"]["truth"]["cases"]}
    v130_primary = {
        str(row["case_id"]): row for row in v130["values"]["primary_normalized"]["cases"]
    }
    trigger = v130["trigger_case_id"]
    structural_changes = 0
    for case in candidate["cases"].values():
        before = {
            "pairs": deepcopy(case["pairs"]),
            "equivalence_groups": deepcopy(case["equivalence_groups"]),
            "unpaired_witness_ids": deepcopy(case["unpaired_witness_ids"]),
        }
        projection = _support_only_projection(
            {
                "alignment_pairs": case["pairs"],
                "equivalence_groups": case["equivalence_groups"],
                "unpaired_witness_ids": case["unpaired_witness_ids"],
            },
            case["proposition"],
        )
        structural_changes += int(projection != before)
        case["pairs"] = deepcopy(projection["pairs"])
        case["equivalence_groups"] = deepcopy(projection["equivalence_groups"])
        case["unpaired_witness_ids"] = deepcopy(projection["unpaired_witness_ids"])
    stable_ids = {
        case_id
        for case_id, row in v130_truth.items()
        if row["role"] == "alignment_dispute" and case_id != trigger
    }
    if len(stable_ids) != 5:
        raise JudgeV5CalibrationV131Error("v131 stable owner coverage drifted")
    for case_id in stable_ids:
        case = candidate["cases"][case_id]
        projection = _support_only_projection(v130_primary[case_id], case["proposition"])
        case["pairs"] = deepcopy(projection["pairs"])
        case["equivalence_groups"] = deepcopy(projection["equivalence_groups"])
        case["unpaired_witness_ids"] = deepcopy(projection["unpaired_witness_ids"])
    owner_turn = next(row for row in truth["cases"] if row["role"] == "singleton_owner")["turn_name"]
    owner_row = outputs[owner_turn]["cases"][0]
    case = candidate["cases"][trigger]
    projection = _support_only_projection(owner_row, case["proposition"])
    case["pairs"] = deepcopy(projection["pairs"])
    case["equivalence_groups"] = deepcopy(projection["equivalence_groups"])
    case["unpaired_witness_ids"] = deepcopy(projection["unpaired_witness_ids"])
    candidate.update(
        {
            "schema_version": V131_REFERENCE_VERSION,
            "reference_version": "fixture_reference_v10_retained_alignment_singleton_frozen",
            "retained_field_reference_patch_authorized": True,
            "proposition_reference_frozen": True,
            "alignment_reference_frozen": True,
            "reference_frozen": True,
            "retained_alignment_owner_case_count": 6,
            "retained_alignment_stable_v130_case_count": 5,
            "retained_alignment_singleton_owner_count": 1,
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
        "schema_version": V131_CAPACITY_AUDIT_VERSION,
        "phase_id": V131_PHASE_ID,
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
        "schema_version": V131_CAPACITY_POLICY_VERSION,
        "phase_id": V131_PHASE_ID,
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


def freeze_v131(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v131 terminal")}
    v130 = _validate_v130()
    rows, truth, selection = build_v131_inputs(v130)
    _write_immutable(root / "singleton-alignment-truth.private.json", truth)
    _write_stable_time(root / "selection-audit.json", selection, "created_at")
    turns = []
    for row in rows:
        prompt, schema = alignment_prompt_v130(row["value"]), neutral_alignment_output_schema(row["value"])
        paths = _freeze_turn_request(
            root=root, turn_name=row["turn_name"], input_value=row["value"], prompt=prompt, schema=schema
        )
        turns.append({**row, "prompt": prompt, "schema": schema, "paths": paths})
    predecessor = {
        **{f"v130_{name}": record for name, record in v130["records"].items()},
        "v130_attempts": v130["attempts"],
        **{f"v129_{name}": record for name, record in v130["v129"]["records"].items()},
    }
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V131_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "four_isolated_singleton_gpt54_alignment_turns_for_three_controls_and_one_v130_trigger",
        "singleton_control_count": CONTROL_COUNT,
        "singleton_owner_count": 1,
        "maximum_cases_per_turn": 1,
        "turn_plan": list(TURN_NAMES),
        "support_positive_witnesses_only": True,
        "prior_alignment_outputs_in_model_input": False,
        "fixture_truth_in_model_input": False,
        "trigger_reason_in_model_input": False,
        "majority_voting_used": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "three_exact_singleton_controls_one_decisive_owner_and_zero_abstentions",
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
            _record(runtime_dir / "app_server_judge_v5_calibration_v130_retained_alignment_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v129_singleton_proposition_repair.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_instruction_hash": sha256_text(alignment_instructions_v130()),
        "frozen_inputs": {
            "truth": _record(root / "singleton-alignment-truth.private.json"),
            "selection": _record(root / "selection-audit.json"),
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
    spec_path = root / "singleton-alignment-repair-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "truth": truth,
        "v130": v130,
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v131 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V131_FAILURE_VERSION,
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
        "schema_version": V131_TERMINAL_VERSION,
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


async def run_v131(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v131 terminal")
    frozen = freeze_v131(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    try:
        outputs, sidecars = {}, []
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
                    batch_size=1,
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=turn["value"]: _validate_scoreable_alignment_output(candidate, item),
                )
                outputs[current_turn] = normalize_neutral_alignment_output(output, turn["value"])
                sidecars.append(sidecar)
        outputs_path = root / "singleton-alignment-repair-outputs.private.json"
        _write_immutable(outputs_path, {"schema_version": V131_OUTPUT_VERSION, "turns": outputs})
        score = score_v131(outputs, frozen["truth"])
        score_path = root / "singleton-alignment-repair-score.json"
        _write_immutable(score_path, score)
        reference_path = root / "calibration-truth-v10-retained-alignment-singleton.private.json"
        if score["passed"]:
            reference = reconcile_alignment_reference(v130=frozen["v130"], outputs=outputs, truth=frozen["truth"])
            _write_immutable(reference_path, reference)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V131_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v131_singleton_alignment_repair_passed_reference_frozen"
                if passed else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v131_retained_alignment_reference_frozen_fresh_diagnostic_authorized"
                if passed else "v131_singleton_alignment_repair_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "proposition_reference_frozen": True,
            "alignment_reference_frozen": passed,
            "reference_frozen": passed,
            "fresh_diagnostic_authorized": passed,
            "fresh_full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "outputs": _record(outputs_path),
            "reference": _record(reference_path) if passed else None,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "predecessor_v130_usage": frozen["v130"]["usage"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v131 singleton alignment repair")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v131(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
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
