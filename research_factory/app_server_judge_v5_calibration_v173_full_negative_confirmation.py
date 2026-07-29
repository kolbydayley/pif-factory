from __future__ import annotations

"""Run the full side-free confirmation of every primary negative field decision."""

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
from . import app_server_judge_v5_calibration_v149_fresh_corrected_field_diagnostic as v149
from . import app_server_judge_v5_calibration_v155_fresh_full_development as v155
from . import app_server_judge_v5_calibration_v168_capacity_recovery as v168
from . import app_server_judge_v5_calibration_v170_verifier_continuation as v170
from . import app_server_judge_v5_calibration_v172_speaker_truth_owner as v172
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


V173_SPEC_VERSION = "pif_app_server_judge_v5_4_v173_spec_v1"
V173_SCORE_VERSION = "pif_app_server_judge_v5_4_v173_score_v1"
V173_PROTOCOL_VERSION = "pif_app_server_judge_v5_4_v173_development_protocol_v1"
V173_FAILURE_VERSION = "pif_app_server_judge_v5_4_v173_failure_v1"
V173_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v173_terminal_v1"
V173_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V173_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V173_PHASE_ID = "judge_v5_4_v173_full_negative_confirmation"

TURN_NAMES = tuple(f"full_negative_confirmation_{index:02d}" for index in range(15))
MODEL = "gpt-5.4"
EFFORT = "high"
MAXIMUM_TOTAL_TOKENS_PER_TURN = 28_000
TIMEOUT_SECONDS = v172.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v172.DEFAULT_OUTPUT_ROOT.parent / "judge-calibration-v5_4-v173-full-negative-confirmation"
).resolve()


class JudgeV5CalibrationV173Error(RuntimeError):
    """The immutable v173 full-confirmation contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v172_reference() -> dict[str, Any]:
    root = v172.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "score": root / "speaker-truth-owner-score.json",
        "spec": root / "speaker-truth-owner-spec.json",
        "policy": root / "capacity-policy.json",
        "audit": root / "capacity-policy-audit.json",
        "output": root / "speaker-truth-owner-output.private.json",
        "reference": root / "fixture-reference-v16-v172.private.json",
        "truth": root / "full-calibration-truth-v172.private.json",
        "patch": root / "speaker-truth-patch-audit.json",
    }
    values = {name: _load_json(path, f"v172 {name}") for name, path in paths.items()}
    terminal, score, spec, patch = (
        values["terminal"], values["score"], values["spec"], values["patch"]
    )
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v172_speaker_truth_owner_passed_reference_v16_frozen_full_confirmation_authorized"
        or terminal.get("reference_frozen") is not True
        or terminal.get("full_confirmation_authorized") is not True
        or terminal.get("selection_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 43811
        or terminal.get("cumulative_calibration_usage", {}).get("total_tokens") != 5371350
        or score.get("passed") is not True
        or score.get("metrics", {}).get("control_exact_count") != 4
        or score.get("metrics", {}).get("target_order_agreement") is not True
        or score.get("metrics", {}).get("target_adjudicated_status") != "incorrect"
        or patch.get("reference_change_count") != 1
        or patch.get("prior_expected_status") != "correct"
        or patch.get("adjudicated_expected_status") != "incorrect"
        or spec.get("retry_count_per_turn") != 0
    ):
        raise JudgeV5CalibrationV173Error("v172 reference contract drifted")
    attempts = _attempt_records(root)
    if len(attempts) != 2:
        raise JudgeV5CalibrationV173Error("v172 attempt coverage drifted")
    for attempt in attempts:
        for key in ("capacity", "sidecar", "output"):
            record = attempt.get(key)
            if not isinstance(record, Mapping):
                raise JudgeV5CalibrationV173Error(f"v172 {key} record is missing")
            _verify_record(record)
        _validate_usage(_load_json(Path(attempt["sidecar"]["path"]), "v172 sidecar"))
    for record in spec["runtime_files"]:
        _verify_record(record)
    for record in spec["frozen_inputs"]["turns"]:
        for key in ("input", "prompt", "schema"):
            _verify_record(record[key])
    for path in paths.values():
        if not path.is_file():
            raise JudgeV5CalibrationV173Error("v172 reference artifact disappeared")
    v171_source = v172._validate_v171_terminal()
    v170_source = v171_source["v170"]
    v171_attempts = _attempt_records(v171_source["root"])
    v171_sidecars = []
    v171_total_tokens = []
    for attempt in v171_attempts:
        sidecar = attempt.get("sidecar")
        if not isinstance(sidecar, Mapping):
            raise JudgeV5CalibrationV173Error("v171 measured sidecar record is missing")
        _verify_record(sidecar)
        usage = _validate_usage(_load_json(Path(sidecar["path"]), "v171 sidecar"))
        v171_sidecars.append(dict(sidecar))
        v171_total_tokens.append(usage["total_tokens"])
    if len(v171_sidecars) != 16:
        raise JudgeV5CalibrationV173Error("v171 measured sidecar coverage drifted")
    support_canary_path = v170.DEFAULT_OUTPUT_ROOT / "support-canary-output.private.json"
    final_canary_path = v170.DEFAULT_OUTPUT_ROOT / "alignment-final-canary.private.json"
    _load_json(support_canary_path, "v170 support canary")
    _load_json(final_canary_path, "v170 final canary")
    scoring_records = {
        "support_canary": _record(support_canary_path),
        "final_canary": _record(final_canary_path),
    }
    for record in scoring_records.values():
        _verify_record(record)
    cumulative = terminal["cumulative_calibration_usage"]
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "v170": v170_source,
        "v170_scoring_records": scoring_records,
        "v171_measured_sidecars": v171_sidecars,
        "v171_maximum_total_tokens": max(v171_total_tokens),
        "cumulative_usage": {field: int(cumulative[field]) for field in USAGE_FIELDS},
    }


def _field_input_map() -> dict[str, dict[str, Any]]:
    mapping = {}
    for name in v168.FIELD_PRIMARY_TURNS:
        path = v168.DEFAULT_OUTPUT_ROOT / "turns" / name.replace("_", "-") / "input.private.json"
        value = _load_json(path, f"v168 {name} input")
        tasks = value.get("tasks")
        if not isinstance(tasks, list) or len(tasks) != 1:
            raise JudgeV5CalibrationV173Error("v168 singleton field input drifted")
        task_id = str(tasks[0]["task_id"])
        mapping[task_id] = value
    if len(mapping) != 28:
        raise JudgeV5CalibrationV173Error("v168 field input coverage drifted")
    return mapping


def build_v173_selection(source: Mapping[str, Any]) -> list[str]:
    decisions = source["v170"]["values"]["fields"]["decisions"]
    selected = sorted(
        (str(row["task_id"]) for row in decisions if row["field_status"] == "incorrect"),
        key=lambda task_id: sha256_text(f"v173|negative-order|{task_id}"),
    )
    if len(selected) != 15 or len(set(selected)) != 15:
        raise JudgeV5CalibrationV173Error("v173 observable-negative selection drifted")
    return selected


def build_final_field_output(
    *, primary: Mapping[str, Any], confirmations: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    confirmed = {}
    for output in confirmations:
        decisions = output.get("decisions")
        if not isinstance(decisions, list) or len(decisions) != 1:
            raise JudgeV5CalibrationV173Error("v173 confirmation coverage drifted")
        row = decisions[0]
        task_id = str(row["task_id"])
        if task_id in confirmed:
            raise JudgeV5CalibrationV173Error("v173 confirmation identity is duplicated")
        confirmed[task_id] = deepcopy(row)
    original = {str(row["task_id"]): deepcopy(row) for row in primary["decisions"]}
    expected = {task_id for task_id, row in original.items() if row["field_status"] == "incorrect"}
    if set(confirmed) != expected:
        raise JudgeV5CalibrationV173Error("v173 confirmation does not cover every negative")
    original.update(confirmed)
    return {
        "schema_version": primary.get("schema_version"),
        "decisions": sorted(original.values(), key=lambda row: str(row["task_id"])),
    }


def _build_capacity_policy(root: Path, source: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    projected = math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    audit = {
        "schema_version": V173_CAPACITY_AUDIT_VERSION,
        "phase_id": V173_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v172_terminal": source["records"]["terminal"],
        "v172_reference": source["records"]["reference"],
        "v172_truth": source["records"]["truth"],
        "v171_measured_sidecars": source["v171_measured_sidecars"],
        "measured_basis": {
            "v171_gpt54_turn_count": 16,
            "v171_gpt54_maximum_total_tokens": source["v171_maximum_total_tokens"],
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
        "schema_version": V173_CAPACITY_POLICY_VERSION,
        "phase_id": V173_PHASE_ID,
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


def freeze_v173(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v173 terminal")}
    source = _validate_v172_reference()
    selected = build_v173_selection(source)
    field_inputs = _field_input_map()
    turns = []
    for turn_name, task_id in zip(TURN_NAMES, selected, strict=True):
        value = deepcopy(field_inputs[task_id])
        prompt = v149.field_prompt_v149(value)
        schema = field_output_schema(value)
        request_paths = _freeze_turn_request(
            root=root, turn_name=turn_name, input_value=value, prompt=prompt, schema=schema
        )
        turns.append(
            {
                "turn_name": turn_name,
                "task_id": task_id,
                "value": value,
                "prompt": prompt,
                "schema": schema,
                "paths": request_paths,
            }
        )
    selection_path = root / "full-negative-confirmation-selection-audit.json"
    _write_stable_time(
        selection_path,
        {
            "schema_version": V173_SPEC_VERSION,
            "created_at": now_iso(),
            "selected_task_count": len(selected),
            "selected_task_ids_sha256": sha256_text("\n".join(sorted(selected))),
            "selection_uses_source_text": False,
            "selection_uses_truth_labels": False,
            "selection_uses_only_observed_primary_field_status_and_opaque_ids": True,
            "every_observed_incorrect_primary_field_decision_selected": True,
            "v171_outputs_reused": False,
            "selection_authorized": False,
            "holdout_authorized": False,
        },
        "created_at",
    )
    capacity = _build_capacity_policy(root, source)
    v170_spec = source["v170"]["values"]["spec"]
    spec = {
        "schema_version": V173_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "phase_id": V173_PHASE_ID,
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "fresh_gpt54_confirmation_of_every_observable_negative_primary_field_decision",
        "selected_task_count": len(selected),
        "turn_plan": list(TURN_NAMES),
        "minimum_turn_count": len(TURN_NAMES),
        "maximum_turn_count": len(TURN_NAMES),
        "retry_count_per_turn": 0,
        "v171_diagnostic_outputs_reused": False,
        "all_confirmation_turns_fresh": True,
        "selection_uses_truth_labels": False,
        "quality_gates_unchanged": True,
        "development_judge_frozen": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "reference_truth_exposed_to_model": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": source["records"],
        "runtime_files": [_record(Path(__file__)), *source["values"]["spec"]["runtime_files"]],
        "frozen_instructions": {
            "support_sha256": sha256_text(v143.support_base_instructions_v143()),
            "primary_field_sha256": v170_spec["frozen_instructions"]["field_sha256"],
            "confirmation_field_sha256": sha256_text(v146.base_instructions_v146()),
            "alignment_sha256": v170_spec["frozen_instructions"]["alignment_sha256"],
        },
        "frozen_inputs": {
            "selection": _record(selection_path),
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
            "turns": [
                {
                    "turn_name": row["turn_name"],
                    "input": _record(row["paths"]["input"]),
                    "prompt": _record(row["paths"]["prompt"]),
                    "schema": _record(row["paths"]["schema"]),
                }
                for row in turns
            ],
        },
        "privacy": "private_source_event_output_truth_and_mapping_sanitized_terminal_only",
    }
    spec_path = root / "full-negative-confirmation-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "source": source,
    }


def _write_failure(
    root: Path, source: Mapping[str, Any], turn_name: Optional[str], error_class: str
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v173 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    cumulative = _sum_usage(source["cumulative_usage"], usage)
    failure = {
        "schema_version": V173_FAILURE_VERSION,
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
        "predecessor_cumulative_usage": source["cumulative_usage"],
        "cumulative_known_usage_lower_bound": cumulative,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V173_TERMINAL_VERSION,
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


async def run_v173(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v173 terminal")
    frozen = freeze_v173(output_dir=root, timeout_seconds=timeout_seconds)
    source = frozen["source"]
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
        v170_values = source["v170"]["values"]
        final_fields = build_final_field_output(
            primary=v170_values["fields"], confirmations=outputs
        )
        final_fields_path = root / "field-output-confirmed.private.json"
        _write_immutable(final_fields_path, final_fields)
        support_canary_record = frozen["spec"]["frozen_inputs"]["support_canary"]
        final_canary_record = frozen["spec"]["frozen_inputs"]["final_alignment_canary"]
        _verify_record(support_canary_record)
        _verify_record(final_canary_record)
        support_canary = _load_json(
            Path(support_canary_record["path"]), "v170 support canary"
        )
        final_canary = _load_json(
            Path(final_canary_record["path"]), "v170 final alignment canary"
        )
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
        score["schema_version"] = V173_SCORE_VERSION
        score["confirmed_negative_task_count"] = len(outputs)
        score_path = root / "full-negative-confirmation-score.json"
        _write_immutable(score_path, score)
        output_path = root / "full-negative-confirmation-output.private.json"
        _write_immutable(
            output_path,
            {
                "schema_version": V173_SPEC_VERSION,
                "turn_outputs": {
                    turn["turn_name"]: output
                    for turn, output in zip(frozen["turns"], outputs, strict=True)
                },
            },
        )
        passed = bool(score["passed"])
        protocol_path = root / "development-judge-protocol-v173.json"
        if passed:
            _write_immutable(
                protocol_path,
                {
                    "schema_version": V173_PROTOCOL_VERSION,
                    "frozen_at": now_iso(),
                    "support_model": v168.SUPPORT_MODEL,
                    "primary_field_model": v168.FIELD_MODEL,
                    "negative_field_confirmation_model": MODEL,
                    "alignment_model": v168.ALIGNMENT_MODEL,
                    "equivalent_pair_verifier_model": v168.VERIFIER_MODEL,
                    "reasoning_effort": EFFORT,
                    "support_instructions_sha256": sha256_text(
                        v143.support_base_instructions_v143()
                    ),
                    "primary_field_instructions_sha256": frozen["spec"]["frozen_instructions"]["primary_field_sha256"],
                    "negative_confirmation_instructions_sha256": sha256_text(
                        v146.base_instructions_v146()
                    ),
                    "alignment_instructions_sha256": frozen["spec"]["frozen_instructions"]["alignment_sha256"],
                    "negative_confirmation_route": "every_primary_field_status_incorrect",
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
        cumulative = _sum_usage(source["cumulative_usage"], accounting["usage"])
        terminal = {
            "schema_version": V173_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v173_full_development_calibration_passed_selection_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v173_full_development_calibration_passed"
                if passed
                else "v173_full_development_calibration_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "development_judge_frozen": passed,
            "selection_authorized": passed,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_retry_count": 0,
            "confirmed_negative_task_count": len(outputs),
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "score": _record(score_path),
            "protocol": _record(protocol_path) if passed else None,
            "reference": source["records"]["reference"],
            "truth": source["records"]["truth"],
            "final_fields": _record(final_fields_path),
            "predecessor_cumulative_usage": source["cumulative_usage"],
            "cumulative_calibration_usage": cumulative,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, source, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, source, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v173 full negative confirmation")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v173(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
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
