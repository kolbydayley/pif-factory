from __future__ import annotations

"""One capped side-free adjudication for the four v151 order disagreements."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v130_retained_alignment_owner as v130
from . import app_server_judge_v5_calibration_v150_fresh_alignment_diagnostic as v150
from . import app_server_judge_v5_calibration_v151_alignment_canary_recovery as v151
from . import app_server_judge_v5_calibration_v152_support_only_truth_repair as v152
from .app_server_judge_v5 import (
    adjudication_alignment_input,
    build_disagreement_adjudication_input,
    build_disagreement_adjudication_prompt,
    neutral_alignment_output_schema,
    normalize_neutral_alignment_output,
    reconcile_neutral_alignment,
    validate_neutral_alignment_output,
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
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V153_SPEC_VERSION = "pif_app_server_judge_v5_4_v153_spec_v1"
V153_AUDIT_VERSION = "pif_app_server_judge_v5_4_v153_reconciliation_audit_v1"
V153_PROTOCOL_VERSION = "pif_app_server_judge_v5_4_v153_alignment_protocol_v1"
V153_FAILURE_VERSION = "pif_app_server_judge_v5_4_v153_failure_v1"
V153_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v153_terminal_v1"
V153_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V153_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V153_PHASE_ID = "judge_v5_4_v153_capped_alignment_adjudication"

MODEL = "gpt-5.5"
EFFORT = "high"
TURN_NAME = "capped_alignment_disagreement_adjudication"
TIMEOUT_SECONDS = 900.0
MAX_PROMPT_BYTES = 512_000
MAX_SCHEMA_BYTES = 128_000
DEFAULT_OUTPUT_ROOT = (
    v152.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v153-capped-alignment-adjudication"
).resolve()


class JudgeV5CalibrationV153Error(RuntimeError):
    """The immutable v153 capped-adjudication contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v152() -> dict[str, Any]:
    root = v152.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "truth": root / "calibration-truth-v14-support-only.private.json",
        "audit": root / "support-only-truth-patch-audit.json",
        "score": root / "corrected-alignment-score.json",
        "plan": root / "capped-alignment-adjudication-plan.json",
    }
    values = {name: _load_json(path, f"v152 {name}") for name, path in paths.items()}
    terminal, audit, plan = values["terminal"], values["audit"], values["plan"]
    zero_usage = {field: 0 for field in USAGE_FIELDS}
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v152_support_only_truth_repaired_capped_adjudication_required"
        or terminal.get("reference_truth_repaired") is not True
        or terminal.get("reference_truth_frozen_for_next_attempt") is not True
        or terminal.get("capped_side_free_alignment_adjudication_authorized") is not True
        or terminal.get("fresh_full_development_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_attempt_started") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage") != zero_usage
        or terminal.get("truth") != _record(paths["truth"])
        or terminal.get("truth_patch_audit") != _record(paths["audit"])
        or terminal.get("corrected_score") != _record(paths["score"])
        or terminal.get("adjudication_plan") != _record(paths["plan"])
        or audit.get("patched_case_count") != 3
        or audit.get("semantic_model_decisions_changed") is not False
        or plan.get("observable_disagreement_case_count") != 4
        or plan.get("call_cap") != 1
        or plan.get("retry_count_per_turn") != 0
        or plan.get("majority_voting_used") is not False
    ):
        raise JudgeV5CalibrationV153Error("v152 authorization contract drifted")
    source = v152._validate_v151()
    rebuilt_truth, rebuilt_audit = v152.repair_support_only_truth(source)
    if (
        rebuilt_truth != values["truth"]
        or rebuilt_audit.get("changes") != audit.get("changes")
        or rebuilt_audit.get("patched_case_count") != audit.get("patched_case_count")
    ):
        raise JudgeV5CalibrationV153Error("v152 truth derivation drifted")
    rebuilt_plan = v152.build_v152_plan(
        primary=source["values"]["primary"],
        canary=source["values"]["canary"],
        truth=rebuilt_truth,
    )
    if rebuilt_plan.get("cases") != plan.get("cases"):
        raise JudgeV5CalibrationV153Error("v152 disagreement plan drifted")
    return {
        "root": root,
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "values": values,
        "truth": rebuilt_truth,
        "v151": source,
    }


def _merge_inputs(inputs: Sequence[Mapping[str, Any]], *, permutation: str) -> dict[str, Any]:
    if len(inputs) != 2:
        raise JudgeV5CalibrationV153Error("v153 input shard coverage drifted")
    merged = deepcopy(inputs[0])
    merged["cases"] = [deepcopy(case) for value in inputs for case in value["cases"]]
    merged["permutation"] = permutation
    if len(merged["cases"]) != 12 or len({case["case_id"] for case in merged["cases"]}) != 12:
        raise JudgeV5CalibrationV153Error("v153 merged input coverage drifted")
    return merged


def _merge_outputs(outputs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if len(outputs) != 2:
        raise JudgeV5CalibrationV153Error("v153 output shard coverage drifted")
    cases = [deepcopy(case) for output in outputs for case in output["cases"]]
    if len(cases) != 12 or len({case["case_id"] for case in cases}) != 12:
        raise JudgeV5CalibrationV153Error("v153 merged output coverage drifted")
    return {"cases": cases}


def _balance_anonymous_candidates(value: dict[str, Any]) -> dict[str, Any]:
    rows = sorted(
        value["cases"],
        key=lambda row: sha256_text(f"v153|candidate-order|{row['case_id']}"),
    )
    swap_ids = {str(row["case_id"]) for row in rows[: len(rows) // 2]}
    for row in value["cases"]:
        if str(row["case_id"]) in swap_ids:
            row["anonymous_candidate_1"], row["anonymous_candidate_2"] = (
                row["anonymous_candidate_2"],
                row["anonymous_candidate_1"],
            )
    value["anonymous_candidate_order_balanced"] = True
    value["anonymous_candidate_swap_count"] = len(swap_ids)
    value["anonymous_candidate_order_rule"] = "opaque_case_hash_balanced_half_swap"
    return value


def build_v153_input(source: Mapping[str, Any]) -> dict[str, Any]:
    v151_source = source["v151"]
    v150_source = v151_source["source"]
    primary_inputs = [v150_source["inputs"][name] for name in v150.PRIMARY_TURNS]
    primary_input = _merge_inputs(primary_inputs, permutation="base")
    projected_primary = _load_json(
        Path(v151_source["values"]["spec"]["frozen_inputs"]["projected_primary"]["path"]),
        "v151 projected primary",
    )
    primary_output = _merge_outputs(
        [v150_source["outputs"][v150.PRIMARY_TURNS[0]], projected_primary]
    )
    canary_inputs = [
        _load_json(
            Path(v150_source["frozen_turns"][name]["input"]["path"]),
            "v150 canary input",
        )
        for name in v150.CANARY_TURNS
    ]
    canary_input = _merge_inputs(canary_inputs, permutation="balanced_canary")
    canary_raw = v151_source["values"]["raw"]["turns"]
    canary_output = _merge_outputs([canary_raw[name] for name in v150.CANARY_TURNS])
    if validate_neutral_alignment_output(primary_output, primary_input):
        raise JudgeV5CalibrationV153Error("v153 primary merge did not validate")
    if validate_neutral_alignment_output(canary_output, canary_input):
        raise JudgeV5CalibrationV153Error("v153 canary merge did not validate")
    receipts = v150_source["values"]["receipts"]
    adjudication_packet = build_disagreement_adjudication_input(
        base_input=primary_input,
        base_output=primary_output,
        canary_input=canary_input,
        canary_output=canary_output,
        support_receipts=receipts,
    )
    adjudication_packet = _balance_anonymous_candidates(adjudication_packet)
    alignment_input = adjudication_alignment_input(
        base_input=primary_input,
        adjudication_input=adjudication_packet,
    )
    expected_ids = {row["case_id"] for row in source["values"]["plan"]["cases"]}
    actual_ids = {row["case_id"] for row in adjudication_packet["cases"]}
    if (
        adjudication_packet.get("adjudication_required") is not True
        or adjudication_packet.get("call_cap") != 1
        or len(actual_ids) != 4
        or actual_ids != expected_ids
        or adjudication_packet.get("anonymous_candidate_swap_count") != 2
        or len(alignment_input["cases"]) != 4
        or alignment_input.get("side_labels_present") is not False
        or alignment_input.get("system_identity_present") is not False
    ):
        raise JudgeV5CalibrationV153Error("v153 adjudication input drifted")
    return {
        "packet": adjudication_packet,
        "alignment_input": alignment_input,
        "primary_input": primary_input,
        "primary_output": primary_output,
        "canary_input": canary_input,
        "canary_output": canary_output,
        "support_receipts": receipts,
    }


def adjudication_instructions_v153() -> str:
    return v130.alignment_instructions_v130() + (
        " This is the sole capped side-free owner pass for four observable permutation "
        "disagreements. Anonymous candidate positions are balanced and have no vote meaning. "
        "Re-evaluate each source and every visible witness independently before considering "
        "the candidate projections. Account for every visible witness exactly once. Resolve "
        "one-to-one ownership before field checklists; for merge/split cases compare the full "
        "source-supported atomic proposition scope. Return abstain rather than guessing when "
        "a material decision is not source-resolvable."
    )


def adjudication_prompt_v153(value: Mapping[str, Any]) -> str:
    return build_disagreement_adjudication_prompt(
        adjudication_input=value["packet"],
        adjudication_alignment=value["alignment_input"],
    )


def reconcile_and_score_v153(
    *, source: Mapping[str, Any], value: Mapping[str, Any], output: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    errors = validate_neutral_alignment_output(output, value["alignment_input"])
    if errors:
        raise JudgeV5CalibrationV153Error("v153 adjudication output did not validate")
    normalized = normalize_neutral_alignment_output(output, value["alignment_input"])
    reconciled = reconcile_neutral_alignment(
        base_output=value["primary_output"],
        base_input=value["primary_input"],
        canary_output=value["canary_output"],
        canary_input=value["canary_input"],
        support_receipts=value["support_receipts"],
        adjudication_output=output,
        adjudication_input=value["alignment_input"],
    )
    if (
        reconciled.get("observable_disagreement_case_count") != 4
        or reconciled.get("adjudication_call_count") != 1
        or reconciled.get("adjudication_call_cap") != 1
        or reconciled.get("majority_voting_used") is not False
        or reconciled.get("unresolved_cases_abstained") != []
    ):
        raise JudgeV5CalibrationV153Error("v153 reconciliation contract drifted")
    score = v150.score_v150(primary=reconciled, canary=reconciled, truth=source["truth"])
    return normalized, reconciled, score


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V153_CAPACITY_AUDIT_VERSION,
        "phase_id": V153_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V153_CAPACITY_POLICY_VERSION,
        "phase_id": V153_PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": [TURN_NAME],
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


def freeze_v153(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v153 terminal")}
    source = _validate_v152()
    value = build_v153_input(source)
    prompt = adjudication_prompt_v153(value)
    schema = neutral_alignment_output_schema(value["alignment_input"])
    prompt_bytes = len(prompt.encode("utf-8"))
    schema_bytes = len(json.dumps(schema, sort_keys=True, separators=(",", ":")).encode("utf-8"))
    if prompt_bytes > MAX_PROMPT_BYTES or schema_bytes > MAX_SCHEMA_BYTES:
        raise JudgeV5CalibrationV153Error("v153 request size cap exceeded")
    paths = _freeze_turn_request(
        root=root,
        turn_name=TURN_NAME,
        input_value={"packet": value["packet"], "alignment_input": value["alignment_input"]},
        prompt=prompt,
        schema=schema,
    )
    predecessor = {
        **{f"v152_{name}": record for name, record in source["records"].items()},
        "v151_attempts": source["v151"]["attempts"],
        "v151_usage": source["v151"]["usage"],
        "v150_usage": source["v151"]["values"]["terminal"]["predecessor_v150_usage"],
        "v149_field_protocol": source["v151"]["source"]["source"]["records"]["protocol"],
        "v148_reference": source["v151"]["source"]["source"]["v148"]["records"]["reference"],
    }
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V153_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "one_capped_side_free_independent_adjudication_for_four_observable_permutation_disagreements",
        "turn_plan": [TURN_NAME],
        "adjudication_call_cap": 1,
        "retry_count_per_turn": 0,
        "anonymous_candidate_swap_count": value["packet"]["anonymous_candidate_swap_count"],
        "majority_voting_used": False,
        "fresh_full_development_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "prompt_bytes": prompt_bytes,
        "schema_bytes": schema_bytes,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v152_support_only_truth_repair.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v151_alignment_canary_recovery.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v150_fresh_alignment_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v130_retained_alignment_owner.py"),
            _record(runtime_dir / "app_server_judge_v5.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "truth": source["records"]["truth"],
            "truth_patch_audit": source["records"]["audit"],
            "adjudication_plan": source["records"]["plan"],
            "field_protocol": predecessor["v149_field_protocol"],
            "reference": predecessor["v148_reference"],
            "turn": {
                "turn_name": TURN_NAME,
                "input": _record(paths["input"]),
                "prompt": _record(paths["prompt"]),
                "schema": _record(paths["schema"]),
            },
        },
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "capped-alignment-adjudication-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "paths": paths,
        "source": source,
        "value": value,
        "prompt": prompt,
        "schema": schema,
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v153 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V153_FAILURE_VERSION,
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
        "schema_version": V153_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "alignment_protocol_frozen": False,
        "fresh_full_development_calibration_authorized": False,
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


async def run_v153(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v153 terminal")
    frozen = freeze_v153(output_dir=root, timeout_seconds=timeout_seconds)
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            output, sidecar, _ = await _get_or_run_turn(
                client=client,
                turn_name=TURN_NAME,
                paths=frozen["paths"],
                prompt=frozen["prompt"],
                schema=frozen["schema"],
                base_instructions=adjudication_instructions_v153(),
                model=MODEL,
                effort=EFFORT,
                timeout_seconds=timeout_seconds,
                batch_size=4,
                policy_path=frozen["capacity_policy"],
                output_validator=lambda candidate: validate_neutral_alignment_output(
                    candidate, frozen["value"]["alignment_input"]
                ),
            )
        normalized, reconciled, score = reconcile_and_score_v153(
            source=frozen["source"], value=frozen["value"], output=output
        )
        paths = {
            "raw": root / "capped-adjudication-output.private.json",
            "normalized": root / "capped-adjudication-normalized.private.json",
            "reconciled": root / "reconciled-alignment.private.json",
            "score": root / "reconciled-alignment-score.json",
            "audit": root / "reconciliation-audit.json",
            "protocol": root / "alignment-judge-protocol-v153.json",
        }
        _write_immutable(paths["raw"], output)
        _write_immutable(paths["normalized"], normalized)
        _write_immutable(paths["reconciled"], reconciled)
        _write_immutable(paths["score"], score)
        audit = {
            "schema_version": V153_AUDIT_VERSION,
            "created_at": now_iso(),
            "observable_disagreement_case_count": 4,
            "adjudication_call_count": 1,
            "adjudication_call_cap": 1,
            "anonymous_candidate_swap_count": frozen["value"]["packet"]["anonymous_candidate_swap_count"],
            "majority_voting_used": False,
            "pre_adjudication_score": frozen["source"]["records"]["score"],
            "corrected_truth": frozen["source"]["records"]["truth"],
            "post_adjudication_passed": score["passed"],
            "post_adjudication_failed_checks": score["failed_checks"],
            "post_adjudication_metrics": score["metrics"],
        }
        _write_stable_time(paths["audit"], audit, "created_at")
        passed = bool(score["passed"])
        if passed:
            protocol = {
                "schema_version": V153_PROTOCOL_VERSION,
                "frozen_at": now_iso(),
                "base_alignment_model": v150.MODEL,
                "base_alignment_effort": v150.EFFORT,
                "adjudication_model": MODEL,
                "adjudication_effort": EFFORT,
                "adjudication_instructions_sha256": sha256_text(adjudication_instructions_v153()),
                "adjudication_call_cap": 1,
                "retry_count_per_turn": 0,
                "anonymous_candidate_order_balanced": True,
                "majority_voting_used": False,
                "exact_span_projection_allowed": True,
                "corrected_truth": frozen["source"]["records"]["truth"],
                "truth_patch_audit": frozen["source"]["records"]["audit"],
                "field_protocol": frozen["spec"]["frozen_inputs"]["field_protocol"],
                "reference": frozen["spec"]["frozen_inputs"]["reference"],
                "reconciliation_audit": _record(paths["audit"]),
                "quality_gates_unchanged": True,
                "selection_authorized": False,
                "holdout_authorized": False,
                "production_mutation_allowed": False,
            }
            _write_immutable(paths["protocol"], protocol)
        accounting = _aggregate_usage([sidecar])
        terminal = {
            "schema_version": V153_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v153_alignment_protocol_frozen_full_development_calibration_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v153_capped_alignment_adjudication_passed"
                if passed
                else "v153_capped_alignment_adjudication_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "alignment_protocol_frozen": passed,
            "fresh_full_development_calibration_authorized": passed,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "adjudication_call_count": 1,
            "adjudication_call_cap": 1,
            "majority_voting_used": False,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "truth": frozen["source"]["records"]["truth"],
            "raw_output": _record(paths["raw"]),
            "normalized_output": _record(paths["normalized"]),
            "reconciled_output": _record(paths["reconciled"]),
            "score": _record(paths["score"]),
            "reconciliation_audit": _record(paths["audit"]),
            "protocol": _record(paths["protocol"]) if passed else None,
            "field_protocol": frozen["spec"]["frozen_inputs"]["field_protocol"],
            "reference": frozen["spec"]["frozen_inputs"]["reference"],
            "predecessor_v151_usage": frozen["source"]["v151"]["usage"],
            "predecessor_v150_usage": frozen["source"]["v151"]["values"]["terminal"]["predecessor_v150_usage"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, TURN_NAME, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v153 capped alignment adjudication")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v153(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "alignment_protocol_frozen": terminal.get("alignment_protocol_frozen", False),
                "fresh_full_development_calibration_authorized": terminal.get(
                    "fresh_full_development_calibration_authorized", False
                ),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
