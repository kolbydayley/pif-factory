from __future__ import annotations

"""Recover v155 by projecting its redundant relation enum from the LLM checklist."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v130_retained_alignment_owner as v130
from . import app_server_judge_v5_calibration_v153_capped_alignment_adjudication as v153
from . import app_server_judge_v5_calibration_v155_fresh_full_development as v155
from .app_server_judge_v5 import (
    build_disagreement_adjudication_input,
    expected_relation_from_checklist,
    neutral_alignment_output_schema,
    reconcile_neutral_alignment,
    validate_neutral_alignment_output,
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


V156_AUDIT_VERSION = "pif_app_server_judge_v5_4_v156_relation_projection_audit_v1"
V156_SPEC_VERSION = "pif_app_server_judge_v5_4_v156_spec_v1"
V156_SCORE_VERSION = "pif_app_server_judge_v5_4_v156_score_v1"
V156_PROTOCOL_VERSION = "pif_app_server_judge_v5_4_v156_frozen_protocol_v1"
V156_FAILURE_VERSION = "pif_app_server_judge_v5_4_v156_failure_v1"
V156_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v156_terminal_v1"
V156_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V156_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V156_PHASE_ID = "judge_v5_4_v156_relation_projection_recovery"

MODEL = v155.ALIGNMENT_MODEL
ADJUDICATOR_MODEL = v155.ADJUDICATOR_MODEL
EFFORT = v155.EFFORT
PRIMARY_TURNS = tuple(f"recovery_alignment_shard_{index:02d}" for index in range(1, 11))
CANARY_TURNS = (
    "recovery_alignment_canary_00",
    "recovery_alignment_canary_01",
)
ADJUDICATION_TURN = "recovery_alignment_disagreement_adjudication"
TURN_NAMES = PRIMARY_TURNS + CANARY_TURNS + (ADJUDICATION_TURN,)
MAXIMUM_TOTAL_TOKENS_PER_TURN = v155.MAXIMUM_TOTAL_TOKENS_PER_TURN
TIMEOUT_SECONDS = v155.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v155.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v156-relation-projection-recovery"
).resolve()


class JudgeV5CalibrationV156Error(RuntimeError):
    """The immutable v156 recovery contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def project_relation_from_checklist(
    output: Mapping[str, Any], alignment_input: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    before_errors = validate_neutral_alignment_output(output, alignment_input)
    nonprojectable = [
        error
        for error in before_errors
        if not error.endswith("_relation_precedence_mismatch")
    ]
    if nonprojectable:
        raise JudgeV5CalibrationV156Error(
            "alignment output has a nonprojectable validation error"
        )

    projected = deepcopy(output)
    changed = []
    pair_count = 0
    for case_index, case in enumerate(projected["cases"]):
        for pair_index, pair in enumerate(case["alignment_pairs"]):
            pair_count += 1
            relation = expected_relation_from_checklist(pair["checklist"])
            if pair["relation"] != relation:
                changed.append(
                    {
                        "case_index": case_index,
                        "pair_index": pair_index,
                        "before_relation": pair["relation"],
                        "projected_relation": relation,
                    }
                )
                pair["relation"] = relation

    after_errors = validate_neutral_alignment_output(projected, alignment_input)
    if after_errors:
        raise JudgeV5CalibrationV156Error(
            "relation projection did not produce a valid alignment output"
        )
    audit = {
        "schema_version": V156_AUDIT_VERSION,
        "pair_count": pair_count,
        "relation_projection_count": len(changed),
        "projected_rows": changed,
        "input_validation_error_count": len(before_errors),
        "input_validation_errors": before_errors,
        "output_validation_error_count": 0,
        "semantic_checklist_decisions_changed": False,
        "witness_assignments_changed": False,
        "equivalence_groups_changed": False,
        "unpaired_witnesses_changed": False,
        "evidence_spans_changed": False,
        "rationales_changed": False,
        "deterministic_operation": "derive_relation_from_frozen_llm_checklist_precedence",
    }
    return projected, audit


def validate_relation_projectable_output(
    output: Any, alignment_input: Mapping[str, Any]
) -> list[str]:
    errors = validate_neutral_alignment_output(output, alignment_input)
    if any(not error.endswith("_relation_precedence_mismatch") for error in errors):
        return errors
    try:
        project_relation_from_checklist(output, alignment_input)
    except Exception:
        return ["relation_projection_failed"]
    return []


def _validate_v155_failure() -> dict[str, Any]:
    root = v155.DEFAULT_OUTPUT_ROOT
    paths = {
        "spec": root / "fresh-full-development-spec.json",
        "terminal": root / "terminal.json",
        "failure": root / "failure.json",
        "pool": root / "shared-witness-pool.private.json",
        "pointwise": root / "pointwise-input-full.private.json",
        "truth": root / "full-calibration-truth.private.json",
        "selection": root / "field-selection-audit.json",
        "support": root / "support-output-full.private.json",
        "support_canary": root / "support-canary-output.private.json",
        "support_receipts": root / "support-receipts.private.json",
        "fields": root / "field-output.private.json",
        "field_repeats": root / "field-repeat-output.private.json",
        "failed_alignment_input": root
        / "turns/full-alignment-shard-00/input.private.json",
        "failed_alignment_output": root
        / "turns/full-alignment-shard-00/output.private.json",
        "failed_alignment_sidecar": root
        / "turns/full-alignment-shard-00/sidecar.json",
        "failed_alignment_capacity": root
        / "turns/full-alignment-shard-00/capacity.json",
    }
    values = {name: _load_json(path, f"v155 {name}") for name, path in paths.items()}
    terminal, failure, spec = values["terminal"], values["failure"], values["spec"]
    expected_usage = {
        "input_tokens": 991174,
        "cached_input_tokens": 61440,
        "output_tokens": 43261,
        "reasoning_output_tokens": 16060,
        "total_tokens": 1034435,
    }
    expected_attempts = set(
        v155.SUPPORT_PRIMARY_TURNS
        + v155.SUPPORT_CANARY_TURNS
        + v155.FIELD_PRIMARY_TURNS
        + v155.FIELD_REPEAT_TURNS
        + (v155.ALIGNMENT_PRIMARY_TURNS[0],)
    )
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage_status") != "complete"
        or terminal.get("usage") != expected_usage
        or terminal.get("development_judge_frozen") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or failure.get("classification") != "infrastructure_or_judge_attempt_failed"
        or failure.get("failed_turn_name") != v155.ALIGNMENT_PRIMARY_TURNS[0]
        or failure.get("error_class") != "JudgeV5CalibrationV26DiagnosticError"
        or failure.get("retry_allowed_in_this_version") is not False
        or failure.get("accounting_complete") is not True
        or failure.get("unknown_usage_turn_count") != 0
        or failure.get("usage") != expected_usage
        or spec.get("turn_plan") != list(v155.TURN_NAMES)
        or spec.get("retry_count_per_turn") != 0
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
        or terminal.get("failure") != _record(paths["failure"])
    ):
        raise JudgeV5CalibrationV156Error("v155 failed-attempt contract drifted")
    if not all(_verify_record(record) for record in spec.get("runtime_files") or []):
        raise JudgeV5CalibrationV156Error("v155 runtime record drifted")
    for key in ("pool", "pointwise", "truth", "selection"):
        if spec["frozen_inputs"].get(key) != _record(paths[key]):
            raise JudgeV5CalibrationV156Error("v155 frozen input record drifted")
    if any(
        not _verify_record(row[key])
        for row in spec["frozen_inputs"]["static_turns"]
        for key in ("input", "prompt", "schema")
    ):
        raise JudgeV5CalibrationV156Error("v155 static request record drifted")

    attempt_rows = failure.get("attempts") or []
    if (
        len(attempt_rows) != len(expected_attempts)
        or {row.get("turn_name") for row in attempt_rows} != expected_attempts
    ):
        raise JudgeV5CalibrationV156Error("v155 attempt coverage drifted")
    usage = {field: 0 for field in USAGE_FIELDS}
    attempts = {}
    for row in attempt_rows:
        turn_name = str(row["turn_name"])
        records = {key: row.get(key) for key in ("capacity", "sidecar", "output")}
        if any(not isinstance(record, Mapping) or not _verify_record(record) for record in records.values()):
            raise JudgeV5CalibrationV156Error("v155 attempt record drifted")
        measured = _validate_usage(
            _load_json(Path(records["sidecar"]["path"]), "v155 measured sidecar")
        )
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        attempts[turn_name] = records
    if usage != expected_usage:
        raise JudgeV5CalibrationV156Error("v155 measured usage drifted")

    relation_errors = validate_neutral_alignment_output(
        values["failed_alignment_output"], values["failed_alignment_input"]
    )
    if relation_errors != ["case_2_pair_0_relation_precedence_mismatch"]:
        raise JudgeV5CalibrationV156Error("v155 alignment failure signature drifted")
    projected, projection_audit = project_relation_from_checklist(
        values["failed_alignment_output"], values["failed_alignment_input"]
    )
    if projection_audit["relation_projection_count"] != 1:
        raise JudgeV5CalibrationV156Error("v155 relation projection count drifted")

    unstarted = (
        v155.ALIGNMENT_PRIMARY_TURNS[1:]
        + v155.ALIGNMENT_CANARY_TURNS
        + (v155.ADJUDICATION_TURN,)
    )
    for turn_name in unstarted:
        turn_root = root / "turns" / turn_name.replace("_", "-")
        if any(
            (turn_root / filename).exists()
            for filename in ("capacity.json", "sidecar.json", "output.private.json")
        ):
            raise JudgeV5CalibrationV156Error("v155 unstarted turn artifact appeared")

    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "usage": usage,
        "projected_alignment_00": projected,
        "projection_audit": projection_audit,
    }


def _build_turns(source: Mapping[str, Any]) -> list[dict[str, Any]]:
    pool = source["values"]["pool"]
    receipts = source["values"]["support_receipts"]
    truth = source["values"]["truth"]
    case_shards = v155.calibration_case_shards(pool)
    turns = []
    for turn_name, case_ids in zip(PRIMARY_TURNS, case_shards[1:], strict=True):
        value = v155._support_positive_alignment_input(
            pool, receipts, case_ids=case_ids, permutation="base"
        )
        turns.append(
            {
                "turn_name": turn_name,
                "turn_role": "alignment_primary",
                "case_ids": list(case_ids),
                "value": value,
            }
        )
    canary_ids = truth["alignment_canary_case_ids"]
    for turn_name, case_ids in zip(
        CANARY_TURNS, (canary_ids[:6], canary_ids[6:]), strict=True
    ):
        value = v155._support_positive_alignment_input(
            pool, receipts, case_ids=case_ids, permutation="balanced_canary"
        )
        turns.append(
            {
                "turn_name": turn_name,
                "turn_role": "alignment_canary",
                "case_ids": list(case_ids),
                "value": value,
            }
        )
    if len(turns) != 12 or any(len(row["case_ids"]) != 6 for row in turns):
        raise JudgeV5CalibrationV156Error("v156 turn coverage drifted")
    return turns


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    audit = {
        "schema_version": V156_CAPACITY_AUDIT_VERSION,
        "phase_id": V156_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
            "v155_failed_alignment_total_tokens": 39564,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V156_CAPACITY_POLICY_VERSION,
        "phase_id": V156_PHASE_ID,
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


def freeze_v156(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v156 terminal")}
    source = _validate_v155_failure()
    projected_path = root / "reused-alignment-shard-00-projected.private.json"
    projection_audit_path = root / "reused-alignment-shard-00-projection-audit.json"
    _write_immutable(projected_path, source["projected_alignment_00"])
    _write_immutable(projection_audit_path, source["projection_audit"])

    turns = []
    for row in _build_turns(source):
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

    predecessor = {
        **{f"v155_{name}": record for name, record in source["records"].items()},
        "v155_attempts": source["attempts"],
        "v155_usage": source["usage"],
    }
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V156_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "adjudicator_model": ADJUDICATOR_MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "reuse_all_46_measured_v155_turns_project_redundant_relation_and_run_only_remaining_alignment",
        "v155_completed_turn_count_reused": 46,
        "v155_turn_count_replayed": 0,
        "fresh_primary_alignment_turn_count": 10,
        "fresh_canary_turn_count": 2,
        "maximum_adjudication_call_count": 1,
        "turn_plan": list(TURN_NAMES),
        "minimum_turn_count": len(TURN_NAMES) - 1,
        "maximum_turn_count": len(TURN_NAMES),
        "retry_count_per_turn": 0,
        "relation_is_deterministically_derived_from_llm_checklist": True,
        "llm_semantic_decisions_changed_by_projection": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v155_fresh_full_development.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v153_capped_alignment_adjudication.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v130_retained_alignment_owner.py"),
            _record(runtime_dir / "app_server_judge_v5.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
            _record(runtime_dir / "util.py"),
        ],
        "frozen_instructions": {
            "alignment_sha256": sha256_text(v130.alignment_instructions_v130()),
            "adjudication_sha256": sha256_text(v153.adjudication_instructions_v153()),
        },
        "frozen_inputs": {
            "pool": source["records"]["pool"],
            "truth": source["records"]["truth"],
            "support": source["records"]["support"],
            "support_canary": source["records"]["support_canary"],
            "support_receipts": source["records"]["support_receipts"],
            "fields": source["records"]["fields"],
            "field_repeats": source["records"]["field_repeats"],
            "reused_alignment_input": source["records"]["failed_alignment_input"],
            "reused_alignment_output": source["records"]["failed_alignment_output"],
            "projected_alignment_output": _record(projected_path),
            "projection_audit": _record(projection_audit_path),
            "turns": [
                {
                    "turn_name": turn["turn_name"],
                    "role": turn["turn_role"],
                    "case_ids": turn["case_ids"],
                    "input": _record(turn["paths"]["input"]),
                    "prompt": _record(turn["paths"]["prompt"]),
                    "schema": _record(turn["paths"]["schema"]),
                }
                for turn in turns
            ],
        },
        "privacy": "private_source_event_output_truth_and_mapping_sanitized_terminal_only",
    }
    spec_path = root / "relation-projection-recovery-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "source": source,
        "projected_alignment_00": source["projected_alignment_00"],
    }


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


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
            measured = _validate_usage(
                _load_json(Path(record["path"]), "v156 measured sidecar")
            )
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    predecessor_usage = _validate_v155_failure()["usage"]
    cumulative = _sum_usage(predecessor_usage, usage)
    failure = {
        "schema_version": V156_FAILURE_VERSION,
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
        "predecessor_v155_usage": predecessor_usage,
        "cumulative_known_usage_lower_bound": cumulative,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V156_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "development_judge_frozen": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "accounting_complete": complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
        "predecessor_v155_usage": predecessor_usage,
        "cumulative_known_usage_lower_bound": cumulative,
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


def _project_and_write(
    *, root: Path, turn_name: str, output: Mapping[str, Any], alignment_input: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    projected, audit = project_relation_from_checklist(output, alignment_input)
    turn_root = root / "turns" / turn_name.replace("_", "-")
    _write_immutable(turn_root / "relation-projected.private.json", projected)
    _write_immutable(turn_root / "relation-projection-audit.json", audit)
    return projected, audit


async def run_v156(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v156 terminal")
    frozen = freeze_v156(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    sidecars = []
    projection_audits = []
    try:
        primary_outputs = [frozen["projected_alignment_00"]]
        canary_outputs = []
        canary_inputs = []
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
                    output_validator=lambda candidate, item=turn["value"]: validate_relation_projectable_output(candidate, item),
                )
                projected, audit = _project_and_write(
                    root=root,
                    turn_name=current_turn,
                    output=output,
                    alignment_input=turn["value"],
                )
                sidecars.append(sidecar)
                projection_audits.append(audit)
                if turn["turn_role"] == "alignment_primary":
                    primary_outputs.append(projected)
                else:
                    canary_outputs.append(projected)
                    canary_inputs.append(turn["value"])

            base_output = v155._merge_outputs(primary_outputs, "cases")
            pool = frozen["source"]["values"]["pool"]
            receipts = frozen["source"]["values"]["support_receipts"]
            base_input = v155._support_positive_alignment_input(
                pool,
                receipts,
                case_ids=[case["case_id"] for case in pool["cases"]],
                permutation="base",
            )
            if validate_neutral_alignment_output(base_output, base_input):
                raise JudgeV5CalibrationV156Error("v156 aggregate base alignment is invalid")
            _write_immutable(root / "alignment-base-projected.private.json", base_output)

            canary_output = v155._merge_outputs(canary_outputs, "cases")
            canary_input = deepcopy(canary_inputs[0])
            canary_input["cases"] = [
                deepcopy(case) for value in canary_inputs for case in value["cases"]
            ]
            if validate_neutral_alignment_output(canary_output, canary_input):
                raise JudgeV5CalibrationV156Error("v156 aggregate canary alignment is invalid")
            _write_immutable(root / "alignment-canary-projected.private.json", canary_output)

            disagreements = build_disagreement_adjudication_input(
                base_input=base_input,
                base_output=base_output,
                canary_input=canary_input,
                canary_output=canary_output,
                support_receipts=receipts,
            )
            _write_immutable(
                root / "alignment-observable-disagreements.private.json", disagreements
            )
            adjudication_output = None
            adjudication_alignment = None
            if disagreements["adjudication_required"]:
                packet = v153._balance_anonymous_candidates(deepcopy(disagreements))
                adjudication_alignment = v153.adjudication_alignment_input(
                    base_input=base_input, adjudication_input=packet
                )
                prompt = v153.build_disagreement_adjudication_prompt(
                    adjudication_input=packet,
                    adjudication_alignment=adjudication_alignment,
                )
                schema = neutral_alignment_output_schema(adjudication_alignment)
                request_paths = _freeze_turn_request(
                    root=root,
                    turn_name=ADJUDICATION_TURN,
                    input_value={
                        "packet": packet,
                        "alignment_input": adjudication_alignment,
                    },
                    prompt=prompt,
                    schema=schema,
                )
                current_turn = ADJUDICATION_TURN
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=ADJUDICATION_TURN,
                    paths=request_paths,
                    prompt=prompt,
                    schema=schema,
                    base_instructions=v153.adjudication_instructions_v153(),
                    model=ADJUDICATOR_MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(adjudication_alignment["cases"]),
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate: validate_relation_projectable_output(candidate, adjudication_alignment),
                )
                adjudication_output, audit = _project_and_write(
                    root=root,
                    turn_name=ADJUDICATION_TURN,
                    output=output,
                    alignment_input=adjudication_alignment,
                )
                sidecars.append(sidecar)
                projection_audits.append(audit)

            reconciled = reconcile_neutral_alignment(
                base_output=base_output,
                base_input=base_input,
                canary_output=canary_output,
                canary_input=canary_input,
                support_receipts=receipts,
                adjudication_output=adjudication_output,
                adjudication_input=adjudication_alignment,
            )
            _write_immutable(root / "alignment-reconciled.private.json", reconciled)

        truth = frozen["source"]["values"]["truth"]
        final_canary_exact = (
            len(truth["alignment_canary_case_ids"])
            if not reconciled["unresolved_cases_abstained"]
            else 0
        )
        score = v155.score_v155(
            support=frozen["source"]["values"]["support"],
            support_canary=frozen["source"]["values"]["support_canary"],
            fields=frozen["source"]["values"]["fields"],
            field_repeats=frozen["source"]["values"]["field_repeats"],
            alignment=reconciled,
            truth=truth,
            raw_alignment_disagreement_count=disagreements["disagreement_case_count"],
            final_canary_exact_count=final_canary_exact,
        )
        score = {**score, "schema_version": V156_SCORE_VERSION}
        score_path = root / "full-calibration-score.json"
        _write_immutable(score_path, score)
        passed = bool(score["passed"])
        protocol_path = root / "development-judge-protocol-v156.json"
        if passed:
            protocol = {
                "schema_version": V156_PROTOCOL_VERSION,
                "frozen_at": now_iso(),
                "support_model": v155.SUPPORT_MODEL,
                "field_model": v155.FIELD_MODEL,
                "alignment_model": MODEL,
                "adjudicator_model": ADJUDICATOR_MODEL,
                "reasoning_effort": EFFORT,
                "alignment_instructions_sha256": sha256_text(v130.alignment_instructions_v130()),
                "adjudication_instructions_sha256": sha256_text(v153.adjudication_instructions_v153()),
                "relation_projection_operation": "derive_relation_from_frozen_llm_checklist_precedence",
                "semantic_checklist_decisions_changed": False,
                "v155_turns_reused": 46,
                "v155_turns_replayed": 0,
                "quality_gates_unchanged": True,
                "reference": frozen["source"]["values"]["spec"]["frozen_inputs"]["reference"],
                "truth": frozen["source"]["records"]["truth"],
                "selection_authorized": True,
                "holdout_authorized": False,
                "production_mutation_allowed": False,
            }
            _write_immutable(protocol_path, protocol)

        accounting = _aggregate_usage(sidecars)
        incremental_usage = accounting["usage"]
        predecessor_usage = frozen["source"]["usage"]
        cumulative_usage = _sum_usage(predecessor_usage, incremental_usage)
        projection_summary = {
            "turn_count": len(projection_audits) + 1,
            "relation_projection_count": source_projection_count
            if (source_projection_count := frozen["source"]["projection_audit"]["relation_projection_count"])
            else 0,
            "fresh_relation_projection_count": sum(
                row["relation_projection_count"] for row in projection_audits
            ),
            "semantic_checklist_decisions_changed": False,
        }
        projection_summary["relation_projection_count"] += projection_summary[
            "fresh_relation_projection_count"
        ]
        terminal = {
            "schema_version": V156_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": "v156_full_development_calibration_passed_selection_authorized"
            if passed
            else "inactive_incomplete_recovery_required",
            "development_terminal_reason": "v156_full_development_calibration_passed"
            if passed
            else "v156_full_development_calibration_quality_gate_not_passed",
            "overall_evaluation_complete": False,
            "development_judge_frozen": passed,
            "selection_authorized": passed,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "v155_turns_replayed": False,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "projection_summary": projection_summary,
            "score": _record(score_path),
            "protocol": _record(protocol_path) if passed else None,
            "reference": frozen["source"]["values"]["spec"]["frozen_inputs"]["reference"],
            "truth": frozen["source"]["records"]["truth"],
            "predecessor_v155_usage": predecessor_usage,
            "cumulative_calibration_usage": cumulative_usage,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v156 relation-projection recovery")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v156(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "development_judge_frozen": terminal.get("development_judge_frozen", False),
                "selection_authorized": terminal.get("selection_authorized", False),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
