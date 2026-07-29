from __future__ import annotations

"""Blind source-only reference audit for the four persistent v69 disagreements."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .app_server_judge_v5 import CHECKLIST_FIELDS
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
from .app_server_judge_v5_calibration_v67_layered_residual import (
    project_root_contract,
    root_output_schema,
    validate_root_output,
)
from .app_server_judge_v5_calibration_v69_stable_recovery import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V69_ROOT,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _canonical_json,
    _record,
    _sha256_file,
    _validate_usage,
)
from .app_server_judge_v5_fixture import load_fixture_truth_audit
from .util import now_iso


V70_INPUT_VERSION = "pif_app_server_judge_v5_4_v70_blind_reference_input_v1"
V70_ROLE_VERSION = "pif_app_server_judge_v5_4_v70_blind_reference_roles_v1"
V70_SPEC_VERSION = "pif_app_server_judge_v5_4_v70_blind_reference_spec_v1"
V70_PROPOSAL_VERSION = "pif_app_server_judge_v5_4_v70_reference_proposal_v1"
V70_AUDIT_VERSION = "pif_app_server_judge_v5_4_v70_projection_audit_v1"
V70_FAILURE_VERSION = "pif_app_server_judge_v5_4_v70_failure_v1"
V70_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v70_terminal_v1"
V70_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V70_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V70_PHASE_ID = "judge_v5_4_v70_blind_reference_audit"
TURN_NAME = "blind_reference_audit"
MODEL = "gpt-5.6-luna"
EFFORT = "high"
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V69_ROOT.parent / "judge-calibration-v5_4-v70-blind-reference-audit"
).resolve()


class JudgeV5CalibrationV70Error(RuntimeError):
    """The v70 blind reference audit cannot preserve its frozen contract."""


def _record_matches(record: Any, path: Path) -> bool:
    return (
        isinstance(record, Mapping)
        and path.is_file()
        and record.get("sha256") == _sha256_file(path)
        and record.get("size_bytes") == path.stat().st_size
    )


def _verify_record(record: Any) -> bool:
    return (
        isinstance(record, Mapping)
        and isinstance(record.get("path"), str)
        and _record_matches(record, Path(record["path"]))
    )


def _validate_v69(v69_root: Path) -> dict[str, Any]:
    paths = {
        "v69_terminal": v69_root / "terminal.json",
        "v69_spec": v69_root / "stable-recovery-spec.json",
        "v69_score": v69_root / "layered-score.json",
        "v69_input": v69_root / "layered-input.private.json",
        "v69_truth": v69_root / "diagnostic-truth.private.json",
        "v69_roles": v69_root / "cohort-roles.json",
        "v69_output": v69_root / "layered-output.private.json",
        "v69_audit": v69_root / "root-projection-audit.json",
        "v69_capacity": v69_root / "turns/neutral-root-verification/capacity.json",
        "v69_sidecar": v69_root / "turns/neutral-root-verification/sidecar.json",
        "v69_raw_output": v69_root / "turns/neutral-root-verification/output.private.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    terminal = values["v69_terminal"]
    spec = values["v69_spec"]
    score = values["v69_score"]
    sidecar = values["v69_sidecar"]
    attempts = terminal.get("attempts") or []
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v69_layered_residual_quality_gate_not_passed"
        or terminal.get("layered_residual_diagnostic_passed") is not False
        or terminal.get("fresh_all_36_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or not _record_matches(terminal.get("score"), paths["v69_score"])
        or not _record_matches(terminal.get("output"), paths["v69_output"])
        or not _record_matches(terminal.get("root_projection_audit"), paths["v69_audit"])
        or score.get("passed") is not False
        or score.get("metrics", {}).get("exact_case_rate") != 0.666667
        or score.get("metrics", {}).get("semantic_residual_exact_rate") != 0.25
        or spec.get("retry_count_per_turn") != 0
        or not all(_verify_record(record) for record in spec.get("runtime_files") or [])
        or sidecar.get("model") != "gpt-5.6-sol"
        or sidecar.get("effort") != "high"
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("usage_status") != "measured"
        or _validate_usage(sidecar) != terminal.get("usage")
        or len(attempts) != 1
        or not _record_matches(attempts[0].get("capacity"), paths["v69_capacity"])
        or not _record_matches(attempts[0].get("sidecar"), paths["v69_sidecar"])
        or not _record_matches(attempts[0].get("output"), paths["v69_raw_output"])
    ):
        raise JudgeV5CalibrationV70Error("v69 predecessor is inadmissible")
    return {name: _record(path) for name, path in paths.items()}


def _root_fields(unit: Mapping[str, Any]) -> set[str]:
    return {
        row["field"] for row in unit.get("checklist") or [] if row.get("root_status") == "root"
    }


def build_v70_cohort(
    value: Mapping[str, Any],
    output: Mapping[str, Any],
    truth: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    sources = {(row["case_id"], row["witness_id"]): row for row in value["units"]}
    rows = {(row["case_id"], row["witness_id"]): row for row in output["units"]}
    expected = {
        (case_id, witness_id): set(fields)
        for case_id, case in truth["cases"].items()
        for witness_id, fields in case["field_issues"].items()
    }
    mismatches = sorted(key for key in expected if _root_fields(rows[key]) != expected[key])
    exact = sorted(key for key in expected if _root_fields(rows[key]) == expected[key])
    empty_controls = [key for key in exact if not expected[key]]
    nonempty_controls = [key for key in exact if expected[key]]
    if len(mismatches) != 4 or not empty_controls or not nonempty_controls:
        raise JudgeV5CalibrationV70Error("v70 audit cohort drifted")
    selected = mismatches + [empty_controls[0], nonempty_controls[0]]
    units = []
    roles = []
    for key in selected:
        source = sources[key]
        units.append(
            {
                "case_id": key[0],
                "witness_id": key[1],
                "source_excerpt": source["source_excerpt"],
                "structured_event": deepcopy(source["structured_event"]),
                "frozen_proposition_verdict": source["frozen_proposition_verdict"],
                "exact_evidence_receipt": source["exact_evidence_receipt"],
            }
        )
        roles.append(
            {
                "case_id": key[0],
                "witness_id": key[1],
                "role": (
                    "reference_disagreement"
                    if key in mismatches
                    else "empty_control"
                    if not expected[key]
                    else "nonempty_control"
                ),
            }
        )
    return (
        {
            "schema_version": V70_INPUT_VERSION,
            "units": units,
            "checklist_field_order": list(CHECKLIST_FIELDS),
            "prior_labels_present": False,
            "candidate_outputs_present": False,
            "system_identity_present": False,
            "reference_proposals_present": False,
        },
        {
            "schema_version": V70_ROLE_VERSION,
            "selection_rule": "four_v69_truth_disagreements_plus_first_exact_empty_and_nonempty_controls",
            "selection_uses_source_text": False,
            "units": roles,
            "role_counts": {
                "reference_disagreement": 4,
                "empty_control": 1,
                "nonempty_control": 1,
            },
        },
    )


def base_instructions() -> str:
    return (
        "You are an independent side-free structured-reference owner. No prior labels, candidate judge "
        "outputs, system identities, or reference proposals are shown. For each field, mark root only if "
        "its own populated value independently conflicts with the selected source proposition or a material "
        "required value is omitted after every other field is mentally corrected. Exact evidence is not a "
        "semantic mismatch. unsupported_inference follows the frozen proposition support receipt. Do not "
        "propagate actor or speaker errors into attribution, event_type, target, metric, evidence, or "
        "reported_actor. event_boundary is root only for a genuine merge or split, not because one field is "
        "wrong. Use exact source substrings or []. Do not infer origin, vote, use confidence, regex, keywords, "
        "overlap, or embeddings."
    )


def build_prompt(value: Mapping[str, Any]) -> str:
    fixture = load_fixture_truth_audit()
    packet = {
        "rubric": fixture["mismatch_checklist"],
        "mismatch_precedence": fixture["mismatch_precedence"],
        "root_rule": "minimal independent truth-conditional field errors only",
        "units": value["units"],
    }
    return (
        "Return every opaque unit once and all 15 fields in supplied order. This is a blind source audit, "
        "not a comparison. structured_field_verdict is incorrect iff any root exists, abstain iff no root "
        "and any abstain, otherwise correct. Every nonempty source span must be exact.\n\n"
        "# Blind reference units\n"
        + _canonical_json(packet)
        + "\n"
    )


def build_reference_proposal(
    output: Mapping[str, Any],
    truth: Mapping[str, Any],
    roles: Mapping[str, Any],
) -> dict[str, Any]:
    expected = {
        (case_id, witness_id): set(fields)
        for case_id, case in truth["cases"].items()
        for witness_id, fields in case["field_issues"].items()
    }
    role_map = {(row["case_id"], row["witness_id"]): row["role"] for row in roles["units"]}
    changes = []
    controls = 0
    controls_exact = 0
    abstentions = 0
    for unit in output["units"]:
        key = (unit["case_id"], unit["witness_id"])
        observed = _root_fields(unit)
        abstentions += sum(row["root_status"] == "abstain" for row in unit["checklist"])
        if role_map[key] != "reference_disagreement":
            controls += 1
            controls_exact += int(observed == expected[key])
        if observed != expected[key]:
            changes.append(
                {
                    "case_id": key[0],
                    "witness_id": key[1],
                    "old_root_fields": sorted(expected[key]),
                    "proposed_root_fields": sorted(observed),
                    "add_fields": sorted(observed - expected[key]),
                    "remove_fields": sorted(expected[key] - observed),
                    "role": role_map[key],
                }
            )
    valid = controls == 2 and controls_exact == 2 and abstentions == 0
    return {
        "schema_version": V70_PROPOSAL_VERSION,
        "created_at": now_iso(),
        "audited_witness_count": 6,
        "reference_disagreement_count": 4,
        "control_count": controls,
        "control_exact_count": controls_exact,
        "abstention_count": abstentions,
        "reference_audit_valid": valid,
        "change_count": len(changes),
        "changes": changes,
        "reference_patch_proposal_authorized": valid and bool(changes),
        "reference_confirmed_without_patch": valid and not changes,
        "fresh_diagnostic_required_after_any_freeze": True,
        "privacy": "opaque_ids_and_field_enums_only",
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    audit = {
        "schema_version": V70_CAPACITY_AUDIT_VERSION,
        "phase_id": V70_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": MAX_TOKENS_PER_TURN,
        },
    }
    if audit_path.exists():
        prior = _load_json(audit_path, "v70 capacity audit")
        candidate = deepcopy(audit)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV70Error("immutable v70 capacity audit drifted")
    else:
        _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V70_CAPACITY_POLICY_VERSION,
        "phase_id": V70_PHASE_ID,
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
        "phase_total_token_bound": MAX_TOKENS_PER_TURN,
        "projected_phase_quota_points": math.ceil(
            MAX_TOKENS_PER_TURN * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
        ),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    if policy_path.exists():
        prior = _load_json(policy_path, "v70 capacity policy")
        candidate = deepcopy(policy)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV70Error("immutable v70 capacity policy drifted")
    else:
        _write_immutable(policy_path, policy)
    return {"audit": audit_path, "policy": policy_path}


def freeze_v70(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v69_root: Path = DEFAULT_V69_ROOT,
    timeout_seconds: float = 1200.0,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_v69(v69_root.resolve())
    value, roles = build_v70_cohort(
        _load_json(v69_root / "layered-input.private.json", "v69 input"),
        _load_json(v69_root / "layered-output.private.json", "v69 output"),
        _load_json(v69_root / "diagnostic-truth.private.json", "v69 truth"),
    )
    truth = _load_json(v69_root / "diagnostic-truth.private.json", "v69 truth")
    input_path = root / "blind-reference-input.private.json"
    truth_path = root / "diagnostic-truth.private.json"
    roles_path = root / "cohort-roles.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_immutable(roles_path, roles)
    prompt = build_prompt(value)
    schema = root_output_schema(value)
    paths = _freeze_turn_request(
        root=root, turn_name=TURN_NAME, input_value=value, prompt=prompt, schema=schema
    )
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V70_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "blind_source_only_reference_owner_no_prior_labels_or_candidates",
        "witness_count": 6,
        "turn_plan": [TURN_NAME],
        "prior_labels_in_model_input": False,
        "candidate_outputs_in_model_input": False,
        "reference_proposals_in_model_input": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "valid_controls_and_no_abstentions_authorize_reference_freeze_decision_only",
        "fresh_all_36_authorized": False,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v69_stable_recovery.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v67_layered_residual.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "input": _record(input_path),
            "truth": _record(truth_path),
            "roles": _record(roles_path),
            "turn": {
                "input": _record(paths["input"]),
                "prompt": _record(paths["prompt"]),
                "schema": _record(paths["schema"]),
            },
        },
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "blind-reference-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v70 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV70Error("immutable v70 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "value": value,
        "truth": truth,
        "roles": roles,
        "prompt": prompt,
        "schema": schema,
        "paths": paths,
        "capacity_policy": capacity["policy"],
    }


def _write_failure(root: Path, error_class: str) -> dict[str, Any]:
    attempts = _attempt_records(root)
    known = {field: 0 for field in USAGE_FIELDS}
    complete = True
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            complete = False
            continue
        try:
            usage = _validate_usage(_load_json(Path(record["path"]), "v70 sidecar"))
        except Exception:
            complete = False
            continue
        for field in USAGE_FIELDS:
            known[field] += usage[field]
    failure = {
        "schema_version": V70_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": TURN_NAME,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "accounting_complete": complete,
        "usage_status": "complete" if complete else "unknown",
        "usage": known if complete else None,
        "attempts": attempts,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V70_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "reference_freeze_decision_authorized": False,
        "fresh_all_36_authorized": False,
        "full_calibration_authorized": False,
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


async def run_v70(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v69_root: Path = DEFAULT_V69_ROOT,
    timeout_seconds: float = 1200.0,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v70 terminal")
    frozen = freeze_v70(
        output_dir=root, v69_root=v69_root, timeout_seconds=timeout_seconds
    )
    factory = client_factory or _client_factory
    try:
        async with factory(frozen["capacity_policy"]) as client:
            output, sidecar, adopted = await _get_or_run_turn(
                client=client,
                turn_name=TURN_NAME,
                paths=frozen["paths"],
                prompt=frozen["prompt"],
                schema=frozen["schema"],
                base_instructions=base_instructions(),
                model=MODEL,
                effort=EFFORT,
                timeout_seconds=timeout_seconds,
                batch_size=6,
                policy_path=frozen["capacity_policy"],
                output_validator=lambda value: validate_root_output(
                    project_root_contract(value, frozen["value"])[0], frozen["value"]
                ),
            )
        projected, operations = project_root_contract(output, frozen["value"])
        errors = validate_root_output(projected, frozen["value"])
        if errors:
            raise JudgeV5CalibrationV70Error("projected v70 output is invalid")
        output_path = root / "blind-reference-output.private.json"
        _write_immutable(output_path, projected)
        audit = {
            "schema_version": V70_AUDIT_VERSION,
            "created_at": now_iso(),
            "operation_count": len(operations),
            "operations": operations,
            "semantic_field_source": MODEL,
            "new_semantic_decisions_from_deterministic_code": False,
            "privacy": "opaque_ids_enums_counts_and_span_hashes_only",
        }
        audit_path = root / "projection-audit.json"
        _write_immutable(audit_path, audit)
        proposal = build_reference_proposal(projected, frozen["truth"], frozen["roles"])
        proposal["projection_audit"] = _record(audit_path)
        proposal_path = root / "reference-proposal.json"
        _write_immutable(proposal_path, proposal)
        accounting = _aggregate_usage([sidecar])
        valid = bool(proposal["reference_audit_valid"])
        terminal = {
            "schema_version": V70_TERMINAL_VERSION,
            "state": "completed" if valid else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v70_blind_reference_audit_completed_freeze_decision_required"
                if valid
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v70_blind_reference_audit_completed_freeze_decision_required"
                if valid
                else "v70_blind_reference_audit_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "reference_audit_valid": valid,
            "reference_patch_proposal_authorized": proposal[
                "reference_patch_proposal_authorized"
            ],
            "reference_confirmed_without_patch": proposal[
                "reference_confirmed_without_patch"
            ],
            "reference_freeze_decision_authorized": valid,
            "fresh_all_36_authorized": False,
            "full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "reference_proposal": _record(proposal_path),
            "output": _record(output_path),
            "projection_audit": _record(audit_path),
            "attempts": _attempt_records(root),
            "completed_checkpoint_adoptions": {TURN_NAME: adopted},
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.error_class)
    except Exception as exc:
        return _write_failure(root, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v70 blind reference audit")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v69-root", default=str(DEFAULT_V69_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v70(
            output_dir=Path(args.output_dir),
            v69_root=Path(args.v69_root),
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "reference_audit_valid": terminal.get("reference_audit_valid", False),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
