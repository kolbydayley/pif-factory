from __future__ import annotations

"""One capped side-free adjudication for the sole v103 permutation disagreement."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

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
    _write_immutable,
)
from .app_server_judge_v5_calibration_v75_exact_span_remaining_shard import (
    project_exact_spans,
)
from .app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import (
    output_schema,
    validate_output,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record_matches,
    _verify_record,
    _write_stable_created,
)
from .app_server_judge_v5_calibration_v103_unsupported_inference_protocol import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V103_ROOT,
    field_rubric_v103,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _record,
    _validate_usage,
)
from .util import now_iso, sha256_text


V104_INPUT_VERSION = "pif_app_server_judge_v5_4_v104_adjudication_input_v1"
V104_TRUTH_VERSION = "pif_app_server_judge_v5_4_v104_adjudication_truth_v1"
V104_SPEC_VERSION = "pif_app_server_judge_v5_4_v104_adjudication_spec_v1"
V104_OUTPUT_VERSION = "pif_app_server_judge_v5_4_v104_adjudication_output_v1"
V104_SCORE_VERSION = "pif_app_server_judge_v5_4_v104_adjudication_score_v1"
V104_PROPOSAL_VERSION = "pif_app_server_judge_v5_4_v104_adjudication_proposal_v1"
V104_FAILURE_VERSION = "pif_app_server_judge_v5_4_v104_failure_v1"
V104_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v104_terminal_v1"
V104_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V104_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V104_PHASE_ID = "judge_v5_4_v104_side_free_adjudication"

MODEL = "gpt-5.6-sol"
EFFORT = "high"
TURN_NAME = "side_free_adjudication"
TURN_NAMES = (TURN_NAME,)
TIMEOUT_SECONDS = 600.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V103_ROOT.parent / "judge-calibration-v5_4-v104-side-free-adjudication"
).resolve()


class JudgeV5CalibrationV104Error(RuntimeError):
    """The v104 adjudication cannot preserve its frozen one-call contract."""


def _validate_predecessor() -> dict[str, Any]:
    paths = {
        "v103_terminal": DEFAULT_V103_ROOT / "terminal.json",
        "v103_spec": DEFAULT_V103_ROOT / "unsupported-protocol-spec.json",
        "v103_input": DEFAULT_V103_ROOT / "unsupported-protocol-input.private.json",
        "v103_truth": DEFAULT_V103_ROOT / "unsupported-protocol-truth.private.json",
        "v103_output": DEFAULT_V103_ROOT / "unsupported-protocol-output.private.json",
        "v103_canary": DEFAULT_V103_ROOT / "permutation-canary-output.private.json",
        "v103_score": DEFAULT_V103_ROOT / "unsupported-protocol-score.json",
        "v103_rubric": DEFAULT_V103_ROOT / "field-rubric.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    terminal, score, spec = (
        values["v103_terminal"],
        values["v103_score"],
        values["v103_spec"],
    )
    truth = {row["task_id"]: row for row in values["v103_truth"]["tasks"]}
    primary = {row["task_id"]: row for row in values["v103_output"]["decisions"]}
    canary = {row["task_id"]: row for row in values["v103_canary"]["decisions"]}
    disagreements = []
    for row in values["v103_truth"]["canary_map"]:
        left, right = primary[row["primary_task_id"]], canary[row["canary_task_id"]]
        if left["field_status"] != right["field_status"]:
            disagreements.append((row, truth[row["primary_task_id"]], left, right))
    if (
        terminal.get("state") != "inactive"
        or terminal.get("development_terminal_reason")
        != "v103_unsupported_protocol_quality_gate_not_passed"
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_retry_count") != 0
        or len(terminal.get("attempts") or []) != 3
        or not all(
            _verify_record(record)
            for attempt in terminal.get("attempts") or []
            for record in (
                attempt.get("capacity"),
                attempt.get("sidecar"),
                attempt.get("output"),
            )
        )
        or not _record_matches(terminal.get("score"), paths["v103_score"])
        or not _record_matches(terminal.get("output"), paths["v103_output"])
        or not _record_matches(terminal.get("canary_output"), paths["v103_canary"])
        or not _record_matches(spec.get("frozen_inputs", {}).get("input"), paths["v103_input"])
        or not _record_matches(spec.get("frozen_inputs", {}).get("truth"), paths["v103_truth"])
        or not _record_matches(spec.get("frozen_inputs", {}).get("rubric"), paths["v103_rubric"])
        or score.get("passed") is not False
        or score.get("metrics", {}).get("exact_count") != 6
        or score.get("metrics", {}).get("incorrect_sensitivity") != 1.0
        or score.get("metrics", {}).get("correct_specificity") != 1.0
        or score.get("metrics", {}).get("evidence_complete_count") != 8
        or score.get("metrics", {}).get("permutation_canary_exact_count") != 1
        or len(disagreements) != 1
        or disagreements[0][1].get("field") != "unsupported_inference"
        or disagreements[0][1].get("expected_status") != "incorrect"
        or disagreements[0][2].get("field_status") != "incorrect"
        or disagreements[0][3].get("field_status") != "correct"
    ):
        raise JudgeV5CalibrationV104Error("v103 disagreement contract drifted")
    return {
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "disagreement": disagreements[0],
    }


def _task_id(role: str) -> str:
    return "adj_" + sha256_text(f"v104|{role}")[:24]


def build_v104_input(
    *, v103_input: Mapping[str, Any], disagreement: tuple[Any, ...]
) -> tuple[dict[str, Any], dict[str, Any]]:
    _map_row, truth_row, _primary, _canary = disagreement
    source_task = next(
        row for row in v103_input["tasks"] if row["task_id"] == truth_row["task_id"]
    )
    contract = deepcopy(field_rubric_v103()["field_contracts"]["unsupported_inference"])
    dispute_id = _task_id("dispute")
    tasks = [
        {
            **deepcopy(source_task),
            "task_id": dispute_id,
            "field_contract": contract,
        },
        {
            "task_id": _task_id("supported_for"),
            "field": "unsupported_inference",
            "field_contract": deepcopy(contract),
            "requested_field_value": {
                "kind": "derived_support_check_not_stored_event_field",
                "output_semantics": contract["output_semantics"],
                "claim_text": "The assistant ranks applicants for recruiter review.",
                "submitted_evidence": "The assistant ranks applicants for recruiter review.",
            },
            "source_excerpt": "The assistant ranks applicants for recruiter review.",
            "structured_event": {
                "claim_text": "The assistant ranks applicants for recruiter review.",
                "evidence": "The assistant ranks applicants for recruiter review.",
            },
        },
        {
            "task_id": _task_id("supported_before"),
            "field": "unsupported_inference",
            "field_contract": deepcopy(contract),
            "requested_field_value": {
                "kind": "derived_support_check_not_stored_event_field",
                "output_semantics": contract["output_semantics"],
                "claim_text": "The assistant ranks applicants before recruiter review.",
                "submitted_evidence": "The assistant ranks applicants before recruiter review.",
            },
            "source_excerpt": "The assistant ranks applicants before recruiter review.",
            "structured_event": {
                "claim_text": "The assistant ranks applicants before recruiter review.",
                "evidence": "The assistant ranks applicants before recruiter review.",
            },
        },
        {
            "task_id": _task_id("unsupported_after"),
            "field": "unsupported_inference",
            "field_contract": deepcopy(contract),
            "requested_field_value": {
                "kind": "derived_support_check_not_stored_event_field",
                "output_semantics": contract["output_semantics"],
                "claim_text": "The assistant ranks applicants after recruiter review.",
                "submitted_evidence": "The assistant ranks applicants for recruiter review.",
            },
            "source_excerpt": "The assistant ranks applicants for recruiter review.",
            "structured_event": {
                "claim_text": "The assistant ranks applicants after recruiter review.",
                "evidence": "The assistant ranks applicants for recruiter review.",
            },
        },
    ]
    tasks.sort(key=lambda row: row["task_id"])
    truth = {
        "schema_version": V104_TRUTH_VERSION,
        "dispute_task_id": dispute_id,
        "dispute_case_id": truth_row["case_id"],
        "dispute_witness_id": truth_row["witness_id"],
        "prior_reference_status": truth_row["expected_status"],
        "control_truth": {
            _task_id("supported_for"): "correct",
            _task_id("supported_before"): "correct",
            _task_id("unsupported_after"): "incorrect",
        },
    }
    value = {
        "schema_version": V104_INPUT_VERSION,
        "task_count": 4,
        "tasks": tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
        "side_free_adjudication": True,
    }
    return value, truth


def _instructions() -> str:
    return (
        "You are the independent side-free owner for one unsupported-inference disagreement and three "
        "unlabeled controls. Judge every task independently. unsupported_inference is a derived check, not "
        "a stored boolean: correct means the event contains no unsupported material inference; incorrect "
        "means it contains at least one. Preserve material temporal relations exactly. Do not infer before "
        "or after solely from a purpose phrase unless that relation is entailed. Provide exact evidence and "
        "abstain only when genuinely indeterminate. Do not use prior labels, model outputs, identity, regex, "
        "keywords, embeddings, confidence, or voting."
    )


def _prompt(value: Mapping[str, Any]) -> str:
    return (
        "Return one field decision per opaque task_id. Do not compare tasks. Every evidence span must be an "
        "exact source_excerpt substring.\n\n"
        + json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    )


def score_v104(output: Mapping[str, Any], truth: Mapping[str, Any]) -> dict[str, Any]:
    observed = {row["task_id"]: row for row in output["decisions"]}
    controls = truth["control_truth"]
    if set(observed) != set(controls) | {truth["dispute_task_id"]}:
        raise JudgeV5CalibrationV104Error("v104 score coverage drifted")
    control_exact = sum(observed[key]["field_status"] == status for key, status in controls.items())
    evidence = sum(bool(row["source_evidence_spans"]) for row in observed.values())
    abstentions = sum(row["field_status"] == "abstain" for row in observed.values())
    checks = {
        "controls_exact": control_exact == 3,
        "evidence_complete": evidence == 4,
        "zero_abstentions": abstentions == 0,
        "dispute_decided": observed[truth["dispute_task_id"]]["field_status"] in {"correct", "incorrect"},
    }
    passed = all(checks.values())
    owner_status = observed[truth["dispute_task_id"]]["field_status"] if passed else None
    return {
        "schema_version": V104_SCORE_VERSION,
        "passed": passed,
        "metrics": {
            "control_exact_count": control_exact,
            "evidence_complete_count": evidence,
            "abstention_count": abstentions,
        },
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "owner_status": owner_status,
        "reference_change_proposed": (
            passed and owner_status != truth["prior_reference_status"]
        ),
        "reconciliation_authorized": passed,
        "fresh_full_development_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _capacity(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path, policy_path = root / "capacity-policy-audit.json", root / "capacity-policy.json"
    bound = MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V104_CAPACITY_AUDIT_VERSION,
        "phase_id": V104_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
        },
    }
    _write_stable_created(audit_path, audit, "v104 capacity audit")
    policy = {
        "schema_version": V104_CAPACITY_POLICY_VERSION,
        "phase_id": V104_PHASE_ID,
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
    _write_stable_created(policy_path, policy, "v104 capacity policy")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v104(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_predecessor()
    value, truth = build_v104_input(
        v103_input=predecessor["values"]["v103_input"],
        disagreement=predecessor["disagreement"],
    )
    files = {
        "input": root / "side-free-adjudication-input.private.json",
        "truth": root / "side-free-adjudication-truth.private.json",
    }
    _write_immutable(files["input"], value)
    _write_immutable(files["truth"], truth)
    prompt, schema = _prompt(value), output_schema(value)
    paths = _freeze_turn_request(
        root=root,
        turn_name=TURN_NAME,
        input_value=value,
        prompt=prompt,
        schema=schema,
    )
    capacity = _capacity(root, predecessor["records"])
    runtime_dir = Path(__file__).resolve().parent
    runtime_files = [
        Path(__file__),
        runtime_dir / "app_server_judge_v5_calibration_v103_unsupported_inference_protocol.py",
        runtime_dir / "app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic.py",
        runtime_dir / "app_server_judge_v5_calibration_v75_exact_span_remaining_shard.py",
        runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py",
        runtime_dir / "app_server_capacity_reserve.py",
        runtime_dir / "codex_app_server.py",
    ]
    spec = {
        "schema_version": V104_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "one_capped_independent_side_free_adjudication_with_three_minimal_controls",
        "task_count": 4,
        "turn_plan": list(TURN_NAMES),
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "three_controls_exact_all_evidence_zero_abstentions_then_owner_reconciliation",
        "reconciliation_authorized": False,
        "fresh_full_development_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor["records"],
        "runtime_files": [_record(path) for path in runtime_files],
        "frozen_inputs": {
            "input": _record(files["input"]),
            "truth": _record(files["truth"]),
            "turn": {
                "turn_name": TURN_NAME,
                "input": _record(paths["input"]),
                "prompt": _record(paths["prompt"]),
                "schema": _record(paths["schema"]),
            },
        },
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "side-free-adjudication-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v104 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV104Error("immutable v104 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "truth": truth,
        "turn": {"value": value, "prompt": prompt, "schema": schema, "paths": paths},
        "capacity_policy": capacity["policy"],
    }


def _real_attempts(root: Path) -> list[dict[str, Any]]:
    return [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]


def _failure(root: Path, error_class: str) -> dict[str, Any]:
    attempts = _real_attempts(root)
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            measured = _validate_usage(_load_json(Path(record["path"]), "v104 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V104_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": TURN_NAME,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "attempts": attempts,
        "accounting_complete": complete,
        "usage_status": "complete" if complete else "unknown",
        "usage": usage if complete else None,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V104_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "reconciliation_authorized": False,
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


async def run_v104(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v104 terminal")
    frozen = freeze_v104(output_dir=root, timeout_seconds=timeout_seconds)
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            output, sidecar, _adopted = await _get_or_run_turn(
                client=client,
                turn_name=TURN_NAME,
                paths=frozen["turn"]["paths"],
                prompt=frozen["turn"]["prompt"],
                schema=frozen["turn"]["schema"],
                base_instructions=_instructions(),
                model=MODEL,
                effort=EFFORT,
                timeout_seconds=timeout_seconds,
                batch_size=4,
                policy_path=frozen["capacity_policy"],
                output_validator=lambda candidate: validate_output(
                    project_exact_spans(candidate, frozen["turn"]["value"])[0],
                    frozen["turn"]["value"],
                ),
            )
        projected, _operations = project_exact_spans(output, frozen["turn"]["value"])
        if validate_output(projected, frozen["turn"]["value"]):
            raise JudgeV5CalibrationV104Error("projected v104 output is invalid")
        output_value = {"schema_version": V104_OUTPUT_VERSION, "decisions": projected["decisions"]}
        output_path = root / "side-free-adjudication-output.private.json"
        _write_immutable(output_path, output_value)
        score = score_v104(output_value, frozen["truth"])
        score_path = root / "side-free-adjudication-score.json"
        _write_immutable(score_path, score)
        proposal_path = root / "adjudication-proposal.json"
        if score["passed"]:
            proposal = {
                "schema_version": V104_PROPOSAL_VERSION,
                "created_at": now_iso(),
                "case_id": frozen["truth"]["dispute_case_id"],
                "witness_id": frozen["truth"]["dispute_witness_id"],
                "field": "unsupported_inference",
                "prior_reference_status": frozen["truth"]["prior_reference_status"],
                "owner_status": score["owner_status"],
                "reference_change_proposed": score["reference_change_proposed"],
                "reconciliation_authorized": True,
                "fresh_full_development_calibration_authorized": False,
            }
            _write_immutable(proposal_path, proposal)
        accounting = _aggregate_usage([sidecar])
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V104_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v104_side_free_adjudication_passed_reconciliation_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v104_side_free_adjudication_passed_reconciliation_authorized"
                if passed
                else "v104_side_free_adjudication_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "reconciliation_authorized": passed,
            "reference_change_proposed": score.get("reference_change_proposed", False),
            "fresh_full_development_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "output": _record(output_path),
            "proposal": _record(proposal_path) if passed else None,
            "attempts": _real_attempts(root),
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _failure(root, exc.error_class)
    except Exception as exc:
        return _failure(root, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v104 side-free adjudication")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v104(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "reconciliation_authorized": terminal.get("reconciliation_authorized", False),
                "reference_change_proposed": terminal.get("reference_change_proposed", False),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
