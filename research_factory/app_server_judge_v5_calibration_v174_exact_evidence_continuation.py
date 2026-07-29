from __future__ import annotations

"""Continue v173 after its one fully measured exact-evidence validation failure."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v130_retained_alignment_owner as v130
from . import app_server_judge_v5_calibration_v143_corrected_layered_diagnostic as v143
from . import app_server_judge_v5_calibration_v146_singleton_reference_owner as v146
from . import app_server_judge_v5_calibration_v155_fresh_full_development as v155
from . import app_server_judge_v5_calibration_v168_capacity_recovery as v168
from . import app_server_judge_v5_calibration_v173_full_negative_confirmation as v173
from .app_server_judge_v5_calibration_v25_diagnostic import (
    QUOTA_POINTS_PER_MILLION_TOKENS,
)
from .app_server_judge_v5_calibration_v26_diagnostic import (
    JudgeV5CalibrationV26DiagnosticAttemptFailed,
    JudgeV5CalibrationV26DiagnosticError,
    _aggregate_usage,
    _client_factory,
    _freeze_turn_request,
    _get_or_run_turn,
    _load_json,
    _validate_completed_turn,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import (
    output_schema as field_output_schema,
    validate_output as validate_field_output,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V174_SPEC_VERSION = "pif_app_server_judge_v5_4_v174_spec_v1"
V174_SCORE_VERSION = "pif_app_server_judge_v5_4_v174_score_v1"
V174_PROTOCOL_VERSION = "pif_app_server_judge_v5_4_v174_development_protocol_v1"
V174_FAILURE_VERSION = "pif_app_server_judge_v5_4_v174_failure_v1"
V174_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v174_terminal_v1"
V174_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V174_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V174_PHASE_ID = "judge_v5_4_v174_exact_evidence_continuation"

REUSED_TURN_NAMES = v173.TURN_NAMES[:6]
FAILED_TURN_NAME = v173.TURN_NAMES[6]
SOURCE_REMAINING_TURN_NAMES = v173.TURN_NAMES[6:]
TURN_NAMES = tuple(
    f"full_negative_confirmation_continuation_{index:02d}" for index in range(6, 15)
)
MODEL = v173.MODEL
EFFORT = v173.EFFORT
MAXIMUM_TOTAL_TOKENS_PER_TURN = v173.MAXIMUM_TOTAL_TOKENS_PER_TURN
TIMEOUT_SECONDS = v173.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v173.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v174-exact-evidence-continuation"
).resolve()


class JudgeV5CalibrationV174Error(RuntimeError):
    """The immutable v174 continuation contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _turn_paths_from_spec(
    spec: Mapping[str, Any], turn_name: str
) -> tuple[dict[str, Path], dict[str, Any]]:
    rows = {
        str(row["turn_name"]): row for row in spec["frozen_inputs"]["turns"]
    }
    if turn_name not in rows:
        raise JudgeV5CalibrationV174Error("v173 frozen turn request disappeared")
    row = rows[turn_name]
    for key in ("input", "prompt", "schema"):
        _verify_record(row[key])
    paths = {
        "input": Path(row["input"]["path"]),
        "prompt": Path(row["prompt"]["path"]),
        "schema": Path(row["schema"]["path"]),
    }
    turn_root = paths["input"].parent
    paths.update(
        {
            "capacity": turn_root / "capacity.json",
            "sidecar": turn_root / "sidecar.json",
            "output": turn_root / "output.private.json",
        }
    )
    value = {
        "input": _load_json(paths["input"], f"v173 {turn_name} input"),
        "prompt": paths["prompt"].read_text(encoding="utf-8"),
        "schema": _load_json(paths["schema"], f"v173 {turn_name} schema"),
    }
    return paths, value


def _validate_v173_failure() -> dict[str, Any]:
    root = v173.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "failure": root / "failure.json",
        "spec": root / "full-negative-confirmation-spec.json",
        "policy": root / "capacity-policy.json",
        "audit": root / "capacity-policy-audit.json",
        "selection": root / "full-negative-confirmation-selection-audit.json",
    }
    values = {name: _load_json(path, f"v173 {name}") for name, path in paths.items()}
    terminal, failure, spec = values["terminal"], values["failure"], values["spec"]
    expected_usage = {
        "input_tokens": 148152,
        "cached_input_tokens": 32384,
        "output_tokens": 3992,
        "reasoning_output_tokens": 3220,
        "total_tokens": 152144,
    }
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
        or failure.get("failed_turn_name") != FAILED_TURN_NAME
        or failure.get("error_class") != "JudgeV5CalibrationV26DiagnosticError"
        or failure.get("retry_allowed_in_this_version") is not False
        or failure.get("unknown_usage_turn_count") != 0
        or failure.get("usage") != expected_usage
        or spec.get("retry_count_per_turn") != 0
    ):
        raise JudgeV5CalibrationV174Error("v173 failure contract drifted")
    source = v173._validate_v172_reference()
    expected_cumulative = _sum_usage(source["cumulative_usage"], expected_usage)
    if terminal.get("cumulative_known_usage_lower_bound") != expected_cumulative:
        raise JudgeV5CalibrationV174Error("v173 cumulative accounting drifted")
    for record in spec["runtime_files"]:
        _verify_record(record)
    for key, value in spec["frozen_inputs"].items():
        if key == "turns":
            continue
        if isinstance(value, Mapping) and {"path", "sha256", "size_bytes"} <= set(value):
            _verify_record(value)
    attempts = [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    if len(attempts) != 7:
        raise JudgeV5CalibrationV174Error("v173 attempt coverage drifted")
    attempt_by_dir = {}
    for attempt in attempts:
        for key in ("capacity", "sidecar", "output"):
            record = attempt.get(key)
            if not isinstance(record, Mapping):
                raise JudgeV5CalibrationV174Error(f"v173 {key} record is missing")
            _verify_record(record)
        sidecar = _load_json(Path(attempt["sidecar"]["path"]), "v173 sidecar")
        _validate_usage(sidecar)
        attempt_by_dir[Path(attempt["sidecar"]["path"]).parent.name] = attempt
    reused_outputs = []
    reused_records = []
    turn_values = {}
    for turn_name in v173.TURN_NAMES[:7]:
        request_paths, value = _turn_paths_from_spec(spec, turn_name)
        turn_values[turn_name] = value
        directory = turn_name.replace("_", "-")
        if directory not in attempt_by_dir:
            raise JudgeV5CalibrationV174Error("v173 attempt identity drifted")
        if turn_name in REUSED_TURN_NAMES:
            output, _ = _validate_completed_turn(
                paths=request_paths,
                prompt=value["prompt"],
                schema=value["schema"],
                base_instructions=v146.base_instructions_v146(),
                model=MODEL,
                effort=EFFORT,
                policy_path=paths["policy"],
                output_validator=lambda candidate, item=value["input"]: validate_field_output(candidate, item),
            )
            reused_outputs.append(output)
            reused_records.append(attempt_by_dir[directory]["output"])
        else:
            try:
                _validate_completed_turn(
                    paths=request_paths,
                    prompt=value["prompt"],
                    schema=value["schema"],
                    base_instructions=v146.base_instructions_v146(),
                    model=MODEL,
                    effort=EFFORT,
                    policy_path=paths["policy"],
                    output_validator=lambda candidate, item=value["input"]: validate_field_output(candidate, item),
                )
            except JudgeV5CalibrationV26DiagnosticError as exc:
                if str(exc) != "v26 output invalid: decision_0_evidence":
                    raise JudgeV5CalibrationV174Error(
                        "v173 failed output classification drifted"
                    ) from exc
            else:
                raise JudgeV5CalibrationV174Error("v173 failed output became valid")
    if len(reused_outputs) != 6 or len(reused_records) != 6:
        raise JudgeV5CalibrationV174Error("v173 reusable prefix drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "source": source,
        "reused_outputs": reused_outputs,
        "reused_records": reused_records,
        "turn_values": turn_values,
        "cumulative_usage": expected_cumulative,
    }


def exact_single_evidence_prompt(value: Mapping[str, Any]) -> str:
    return (
        "Return the one independent field decision for the opaque task_id. Return exactly one "
        "source_evidence_span, copied byte-for-byte as a contiguous substring of source_excerpt; "
        "do not paraphrase it and do not add a second span. The span must directly support the "
        "requested field decision. Do not emit a whole-event verdict.\n\n"
        + json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        + "\n"
    )


def exact_single_evidence_schema(value: Mapping[str, Any]) -> dict[str, Any]:
    schema = deepcopy(field_output_schema(value))
    evidence = schema["properties"]["decisions"]["items"]["properties"][
        "source_evidence_spans"
    ]
    evidence["minItems"] = 1
    evidence["maxItems"] = 1
    return schema


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    projected = math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    measured_sidecars = [row["sidecar"] for row in predecessor["attempts"]]
    measured_totals = [
        _validate_usage(_load_json(Path(record["path"]), "v173 measured sidecar"))[
            "total_tokens"
        ]
        for record in measured_sidecars
    ]
    audit = {
        "schema_version": V174_CAPACITY_AUDIT_VERSION,
        "phase_id": V174_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v173_terminal": predecessor["records"]["terminal"],
        "v173_failure": predecessor["records"]["failure"],
        "v173_measured_sidecars": measured_sidecars,
        "measured_basis": {
            "v173_measured_turn_count": len(measured_sidecars),
            "v173_maximum_total_tokens": max(measured_totals),
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": 20,
            "maximum_live_used_percent_for_launch": 80 - projected,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V174_CAPACITY_POLICY_VERSION,
        "phase_id": V174_PHASE_ID,
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
        "projected_phase_quota_points": projected,
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v174(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v174 terminal")}
    predecessor = _validate_v173_failure()
    v173_spec = predecessor["values"]["spec"]
    prior_turn_rows = {
        row["turn_name"]: row for row in v173_spec["frozen_inputs"]["turns"]
    }
    turns = []
    for turn_name, source_turn_name in zip(TURN_NAMES, SOURCE_REMAINING_TURN_NAMES, strict=True):
        prior_input = _load_json(
            Path(prior_turn_rows[source_turn_name]["input"]["path"]),
            f"v173 {source_turn_name} input",
        )
        prompt = exact_single_evidence_prompt(prior_input)
        schema = exact_single_evidence_schema(prior_input)
        request_paths = _freeze_turn_request(
            root=root,
            turn_name=turn_name,
            input_value=prior_input,
            prompt=prompt,
            schema=schema,
        )
        turns.append(
            {
                "turn_name": turn_name,
                "source_turn_name": source_turn_name,
                "value": prior_input,
                "prompt": prompt,
                "schema": schema,
                "paths": request_paths,
            }
        )
    continuation_path = root / "exact-evidence-continuation-audit.json"
    _write_stable_time(
        continuation_path,
        {
            "schema_version": V174_SPEC_VERSION,
            "created_at": now_iso(),
            "v173_valid_reused_turn_count": len(REUSED_TURN_NAMES),
            "v173_failed_turn_name": FAILED_TURN_NAME,
            "v173_failed_output_reused": False,
            "v173_failed_output_error_class": "nonexact_additional_evidence_span",
            "v173_failed_output_first_span_exact": True,
            "v173_failed_output_second_span_nonexact": True,
            "remaining_fresh_turn_count": len(TURN_NAMES),
            "semantic_rubric_changed": False,
            "serialization_constraint": "exactly_one_nonempty_exact_source_substring",
            "selection_uses_truth_labels": False,
            "selection_uses_source_semantics": False,
            "selection_authorized": False,
            "holdout_authorized": False,
        },
        "created_at",
    )
    capacity = _build_capacity_policy(root, predecessor)
    source = predecessor["source"]
    v170_spec = source["v170"]["values"]["spec"]
    spec = {
        "schema_version": V174_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "phase_id": V174_PHASE_ID,
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "reuse_six_valid_v173_prefix_outputs_then_fresh_exact_single_evidence_remaining_turns",
        "reused_valid_turn_count": len(REUSED_TURN_NAMES),
        "fresh_turn_count": len(TURN_NAMES),
        "turn_plan": list(TURN_NAMES),
        "retry_count_per_turn": 0,
        "v173_failed_turn_replayed_in_place": False,
        "v173_failed_output_reused": False,
        "quality_gates_unchanged": True,
        "semantic_rubric_changed": False,
        "development_judge_frozen": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "reference_truth_exposed_to_model": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor["records"],
        "runtime_files": [_record(Path(__file__)), *v173_spec["runtime_files"]],
        "frozen_instructions": {
            "support_sha256": sha256_text(v143.support_base_instructions_v143()),
            "primary_field_sha256": v170_spec["frozen_instructions"]["field_sha256"],
            "confirmation_field_sha256": sha256_text(v146.base_instructions_v146()),
            "exact_single_evidence_prompt_prefix_sha256": sha256_text(
                exact_single_evidence_prompt({"schema_version": "hash_sentinel", "tasks": []})
            ),
            "alignment_sha256": v170_spec["frozen_instructions"]["alignment_sha256"],
        },
        "frozen_inputs": {
            "continuation_audit": _record(continuation_path),
            "reference": source["records"]["reference"],
            "truth": source["records"]["truth"],
            "primary_fields": source["v170"]["records"]["fields"],
            "field_repeats": source["v170"]["records"]["field_repeats"],
            "support": source["v170"]["records"]["support"],
            "support_canary": source["v170_scoring_records"]["support_canary"],
            "final_alignment": source["v170"]["records"]["final_base"],
            "final_alignment_canary": source["v170_scoring_records"]["final_canary"],
            "field_protocol": v170_spec["frozen_inputs"]["field_protocol"],
            "alignment_protocol": v170_spec["frozen_inputs"]["alignment_protocol"],
            "v173_attempts": predecessor["attempts"],
            "reused_v173_outputs": predecessor["reused_records"],
            "turns": [
                {
                    "turn_name": row["turn_name"],
                    "source_turn_name": row["source_turn_name"],
                    "input": _record(row["paths"]["input"]),
                    "prompt": _record(row["paths"]["prompt"]),
                    "schema": _record(row["paths"]["schema"]),
                }
                for row in turns
            ],
        },
        "privacy": "private_source_event_output_truth_and_mapping_sanitized_terminal_only",
    }
    spec_path = root / "exact-evidence-continuation-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "predecessor": predecessor,
    }


def _write_failure(
    root: Path,
    predecessor: Mapping[str, Any],
    turn_name: Optional[str],
    error_class: str,
) -> dict[str, Any]:
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v174 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    cumulative = _sum_usage(predecessor["cumulative_usage"], usage)
    failure = {
        "schema_version": V174_FAILURE_VERSION,
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
        "predecessor_cumulative_usage": predecessor["cumulative_usage"],
        "cumulative_known_usage_lower_bound": cumulative,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V174_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "development_judge_frozen": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "accounting_complete": complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
        "cumulative_known_usage_lower_bound": cumulative,
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v174(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v174 terminal")
    frozen = freeze_v174(output_dir=root, timeout_seconds=timeout_seconds)
    predecessor = frozen["predecessor"]
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
                    base_instructions=v146.base_instructions_v146(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=1,
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=turn["value"]: validate_field_output(candidate, item),
                )
                outputs.append(output)
                sidecars.append(sidecar)
        source = predecessor["source"]
        v170_values = source["v170"]["values"]
        all_confirmations = [*predecessor["reused_outputs"], *outputs]
        final_fields = v173.build_final_field_output(
            primary=v170_values["fields"], confirmations=all_confirmations
        )
        final_fields_path = root / "field-output-confirmed.private.json"
        _write_immutable(final_fields_path, final_fields)
        support_canary_record = frozen["spec"]["frozen_inputs"]["support_canary"]
        final_canary_record = frozen["spec"]["frozen_inputs"]["final_alignment_canary"]
        _verify_record(support_canary_record)
        _verify_record(final_canary_record)
        support_canary = _load_json(Path(support_canary_record["path"]), "support canary")
        final_canary = _load_json(Path(final_canary_record["path"]), "final canary")
        base_cases = {
            str(row["case_id"]): v130._project_alignment(row)
            for row in v170_values["final_base"]["cases"]
        }
        canary_cases = {
            str(row["case_id"]): v130._project_alignment(row)
            for row in final_canary["cases"]
        }
        canary_exact = sum(base_cases[key] == canary_cases[key] for key in canary_cases)
        score = v155.score_v155(
            support=v170_values["support"],
            support_canary=support_canary,
            fields=final_fields,
            field_repeats=v170_values["field_repeats"],
            alignment=v170_values["final_base"],
            truth=source["values"]["truth"],
            raw_alignment_disagreement_count=len(canary_cases) - canary_exact,
            final_canary_exact_count=canary_exact,
        )
        score["schema_version"] = V174_SCORE_VERSION
        score["reused_valid_confirmation_count"] = len(predecessor["reused_outputs"])
        score["fresh_confirmation_count"] = len(outputs)
        score_path = root / "exact-evidence-continuation-score.json"
        _write_immutable(score_path, score)
        output_path = root / "exact-evidence-continuation-output.private.json"
        _write_immutable(
            output_path,
            {
                "schema_version": V174_SPEC_VERSION,
                "turn_outputs": {
                    turn["turn_name"]: output
                    for turn, output in zip(frozen["turns"], outputs, strict=True)
                },
            },
        )
        passed = bool(score["passed"])
        protocol_path = root / "development-judge-protocol-v174.json"
        if passed:
            _write_immutable(
                protocol_path,
                {
                    "schema_version": V174_PROTOCOL_VERSION,
                    "frozen_at": now_iso(),
                    "support_model": v168.SUPPORT_MODEL,
                    "primary_field_model": v168.FIELD_MODEL,
                    "negative_field_confirmation_model": MODEL,
                    "alignment_model": v168.ALIGNMENT_MODEL,
                    "equivalent_pair_verifier_model": v168.VERIFIER_MODEL,
                    "reasoning_effort": EFFORT,
                    "support_instructions_sha256": sha256_text(v143.support_base_instructions_v143()),
                    "primary_field_instructions_sha256": frozen["spec"]["frozen_instructions"]["primary_field_sha256"],
                    "negative_confirmation_instructions_sha256": sha256_text(v146.base_instructions_v146()),
                    "alignment_instructions_sha256": frozen["spec"]["frozen_instructions"]["alignment_sha256"],
                    "negative_confirmation_route": "every_primary_field_status_incorrect",
                    "negative_confirmation_evidence_serialization": "exactly_one_exact_source_substring",
                    "negative_confirmation_uses_truth": False,
                    "reference": source["records"]["reference"],
                    "truth": source["records"]["truth"],
                    "field_protocol": frozen["spec"]["frozen_inputs"]["field_protocol"],
                    "alignment_protocol": frozen["spec"]["frozen_inputs"]["alignment_protocol"],
                    "all_primary_equivalent_pairs_verified": True,
                    "strict_canary_no_repair_call": True,
                    "retry_count_per_turn": 0,
                    "quality_gates_unchanged": True,
                    "selection_authorized": True,
                    "holdout_authorized": False,
                    "production_mutation_allowed": False,
                },
            )
        accounting = _aggregate_usage(sidecars)
        cumulative = _sum_usage(predecessor["cumulative_usage"], accounting["usage"])
        terminal = {
            "schema_version": V174_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v174_full_development_calibration_passed_selection_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v174_full_development_calibration_passed"
                if passed
                else "v174_full_development_calibration_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "development_judge_frozen": passed,
            "selection_authorized": passed,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_retry_count": 0,
            "reused_valid_confirmation_count": len(predecessor["reused_outputs"]),
            "fresh_confirmation_count": len(outputs),
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "score": _record(score_path),
            "protocol": _record(protocol_path) if passed else None,
            "reference": source["records"]["reference"],
            "truth": source["records"]["truth"],
            "final_fields": _record(final_fields_path),
            "predecessor_cumulative_usage": predecessor["cumulative_usage"],
            "cumulative_calibration_usage": cumulative,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, predecessor, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, predecessor, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v174 exact-evidence continuation")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v174(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
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
