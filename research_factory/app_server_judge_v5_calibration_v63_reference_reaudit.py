from __future__ import annotations

"""Side-free Sol-high reference-owner re-audit of the six v62 residuals."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

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
from .app_server_judge_v5_calibration_v62_root_status import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V62_ROOT,
    _validate_v61,
    project_v62_contract,
    v62_output_schema,
    validate_v62_output,
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


V63_INPUT_VERSION = "pif_app_server_judge_v5_4_v63_reference_reaudit_input_v1"
V63_SPEC_VERSION = "pif_app_server_judge_v5_4_v63_reference_reaudit_spec_v1"
V63_PROPOSAL_VERSION = "pif_app_server_judge_v5_4_v63_reference_patch_proposal_v1"
V63_AUDIT_VERSION = "pif_app_server_judge_v5_4_v63_contract_projection_audit_v1"
V63_FAILURE_VERSION = "pif_app_server_judge_v5_4_v63_reference_reaudit_failure_v1"
V63_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v63_reference_reaudit_terminal_v1"
V63_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V63_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V63_PHASE_ID = "judge_v5_4_v63_side_free_reference_owner_reaudit"
TURN_NAME = "side_free_reference_owner_reaudit"
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V62_ROOT.parent / "judge-calibration-v5_4-v63-reference-owner-reaudit"
).resolve()


class JudgeV5CalibrationV63ReferenceError(RuntimeError):
    """The bounded reference-owner re-audit cannot preserve its contract."""


def _record_matches(record: Any, path: Path) -> bool:
    return (
        isinstance(record, Mapping)
        and path.is_file()
        and record.get("sha256") == _sha256_file(path)
        and record.get("size_bytes") == path.stat().st_size
    )


def _validate_v62(v62_root: Path) -> dict[str, Any]:
    paths = {
        "v62_terminal": v62_root / "terminal.json",
        "v62_spec": v62_root / "root-status-spec.json",
        "v62_score": v62_root / "root-status-score.json",
        "v62_input": v62_root / "root-status-input.private.json",
        "v62_truth": v62_root / "diagnostic-truth.private.json",
        "v62_roles": v62_root / "cohort-roles.json",
        "v62_output": v62_root / "root-status-output.private.json",
        "v62_audit": v62_root / "contract-projection-audit.json",
        "v62_turn_input": v62_root / "turns/explicit-root-status-diagnostic/input.private.json",
        "v62_turn_output": v62_root / "turns/explicit-root-status-diagnostic/output.private.json",
        "v62_turn_capacity": v62_root / "turns/explicit-root-status-diagnostic/capacity.json",
        "v62_turn_sidecar": v62_root / "turns/explicit-root-status-diagnostic/sidecar.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    terminal = values["v62_terminal"]
    score = values["v62_score"]
    spec = values["v62_spec"]
    sidecar = values["v62_turn_sidecar"]
    capacity = values["v62_turn_capacity"]
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason") != "v62_root_status_quality_gate_not_passed"
        or terminal.get("root_status_diagnostic_passed") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("production_mutated") is not False
        or not _record_matches(terminal.get("score"), paths["v62_score"])
        or not _record_matches(terminal.get("output"), paths["v62_output"])
        or not _record_matches(terminal.get("contract_projection_audit"), paths["v62_audit"])
        or score.get("passed") is not False
        or spec.get("model") != "gpt-5.5"
        or sidecar.get("model") != "gpt-5.5"
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("state") != "completed"
        or sidecar.get("usage_status") != "measured"
        or capacity.get("cleared_for_semantic_turn") is not True
        or capacity.get("managed_chatgpt_auth_verified") is not True
        or capacity.get("rate_limit_reached_type") is not None
    ):
        raise JudgeV5CalibrationV63ReferenceError("v62 predecessor is inadmissible")
    if _validate_usage(sidecar) != terminal.get("usage"):
        raise JudgeV5CalibrationV63ReferenceError("v62 usage drifted")
    attempts = terminal.get("attempts") or []
    if (
        len(attempts) != 1
        or not _record_matches(attempts[0].get("sidecar"), paths["v62_turn_sidecar"])
        or not _record_matches(attempts[0].get("capacity"), paths["v62_turn_capacity"])
        or not _record_matches(attempts[0].get("output"), paths["v62_turn_output"])
    ):
        raise JudgeV5CalibrationV63ReferenceError("v62 attempt binding drifted")
    upstream = _validate_v61(
        Path(spec["predecessor"]["v61_terminal"]["path"]).parent
    )
    if spec.get("predecessor") != upstream:
        raise JudgeV5CalibrationV63ReferenceError("v62 upstream binding drifted")
    return {name: _record(path) for name, path in paths.items()}


def build_v63_input(
    value: Mapping[str, Any], output: Mapping[str, Any], truth: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    rows = {
        (row["case_id"], row["witness_id"]): row for row in output.get("units") or []
    }
    sources = {
        (row["case_id"], row["witness_id"]): row for row in value.get("units") or []
    }
    expected = {
        (case_id, witness_id): set(fields)
        for case_id, case in truth["cases"].items()
        for witness_id, fields in case["field_issues"].items()
    }
    residuals = []
    for key in sorted(rows):
        observed = {
            item["field"]
            for item in rows[key]["checklist"]
            if item["independent_root_status"] == "root"
        }
        if observed != expected[key]:
            residuals.append(key)
    if len(residuals) != 6:
        raise JudgeV5CalibrationV63ReferenceError("v63 residual cohort drifted")
    units = []
    old_truth = []
    for key in residuals:
        source = sources[key]
        units.append(
            {
                "case_id": key[0],
                "witness_id": key[1],
                "source_excerpt": source["source_excerpt"],
                "structured_event": deepcopy(source["structured_event"]),
                "frozen_proposition_verdict": source["frozen_proposition_verdict"],
            }
        )
        old_truth.append(
            {
                "case_id": key[0],
                "witness_id": key[1],
                "old_root_fields": sorted(expected[key]),
            }
        )
    return (
        {
            "schema_version": V63_INPUT_VERSION,
            "units": units,
            "checklist_field_order": value["checklist_field_order"],
            "prior_labels_present": False,
            "candidate_outputs_present": False,
            "system_identity_present": False,
        },
        {"units": old_truth},
    )


def v63_base_instructions() -> str:
    return (
        "You are the side-free structured-reference owner. No prior labels or candidate outputs are "
        "shown. For every field, independently judge value_relation, then mark root only when the "
        "material conflict persists after substituting source-supported values for every other field; "
        "mark derivative when it disappears and name the dependency fields. same requires not_different. "
        "For an added ungrounded assertion, report the specific material field or fields as root plus "
        "unsupported_inference. Exact evidence alone is not enough for an unsupported inference, while "
        "evidence is root only when the evidence field itself is inexact or independently unsupported. "
        "actor/speaker conflicts do not automatically make attribution, event_type, evidence, target, "
        "metric, certainty, or reported_actor root. attribution is only the reporting/source relation. "
        "event_boundary must be same/not_different. unsupported_inference follows the frozen proposition "
        "receipt exactly. Cite exact source spans or []. Do not infer system origin, use confidence, vote, "
        "regex, keywords, overlap, or embeddings."
    )


def build_v63_prompt(value: Mapping[str, Any]) -> str:
    fixture = load_fixture_truth_audit()
    packet = {
        "rubric": fixture["mismatch_checklist"],
        "mismatch_precedence": fixture["mismatch_precedence"],
        "units": value["units"],
    }
    return (
        "Return every opaque unit and all fields once. different requires root or derivative; derivative "
        "requires dependency_fields. same requires not_different. structured_field_verdict is incorrect "
        "iff any root exists, abstain iff no root and any abstain, otherwise correct. This is an independent "
        "reference audit, not a comparison. Every nonempty span must be an exact substring.\n\n"
        "# Side-free reference-owner units\n"
        + _canonical_json(packet)
        + "\n"
    )


def build_reference_proposal(
    output: Mapping[str, Any], old_truth: Mapping[str, Any]
) -> dict[str, Any]:
    old = {
        (row["case_id"], row["witness_id"]): set(row["old_root_fields"])
        for row in old_truth["units"]
    }
    changes = []
    exact = 0
    for row in output["units"]:
        key = (row["case_id"], row["witness_id"])
        proposed = {
            item["field"]
            for item in row["checklist"]
            if item["independent_root_status"] == "root"
        }
        if proposed == old[key]:
            exact += 1
        else:
            changes.append(
                {
                    "case_id": key[0],
                    "witness_id": key[1],
                    "old_root_fields": sorted(old[key]),
                    "proposed_root_fields": sorted(proposed),
                    "add_fields": sorted(proposed - old[key]),
                    "remove_fields": sorted(old[key] - proposed),
                }
            )
    return {
        "schema_version": V63_PROPOSAL_VERSION,
        "created_at": now_iso(),
        "audited_witness_count": 6,
        "exact_old_truth_count": exact,
        "change_count": len(changes),
        "reference_patch_proposed": bool(changes),
        "changes": changes,
        "reference_freeze_authorized": bool(changes),
        "fresh_diagnostic_required_after_freeze": True,
        "privacy": "opaque_ids_and_field_enums_only",
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    audit = {
        "schema_version": V63_CAPACITY_AUDIT_VERSION,
        "phase_id": V63_PHASE_ID,
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
        prior = _load_json(audit_path, "v63 capacity audit")
        candidate = deepcopy(audit)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV63ReferenceError("immutable v63 capacity audit drifted")
    else:
        _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V63_CAPACITY_POLICY_VERSION,
        "phase_id": V63_PHASE_ID,
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
        prior = _load_json(policy_path, "v63 capacity policy")
        candidate = deepcopy(policy)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV63ReferenceError("immutable v63 capacity policy drifted")
    else:
        _write_immutable(policy_path, policy)
    return {"audit": audit_path, "policy": policy_path}


def freeze_v63(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v62_root: Path = DEFAULT_V62_ROOT,
    model: str = "gpt-5.6-sol",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_v62(v62_root.resolve())
    source_value = _load_json(v62_root / "root-status-input.private.json", "v62 input")
    source_output = _load_json(v62_root / "root-status-output.private.json", "v62 output")
    source_truth = _load_json(v62_root / "diagnostic-truth.private.json", "v62 truth")
    value, old_truth = build_v63_input(source_value, source_output, source_truth)
    input_path = root / "reference-input.private.json"
    old_truth_path = root / "old-truth.private.json"
    _write_immutable(input_path, value)
    _write_immutable(old_truth_path, old_truth)
    prompt = build_v63_prompt(value)
    schema = v62_output_schema(value)
    paths = _freeze_turn_request(
        root=root, turn_name=TURN_NAME, input_value=value, prompt=prompt, schema=schema
    )
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V63_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": model,
        "reasoning_effort": reasoning_effort,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "side_free_reference_owner_reaudit_without_prior_labels_or_candidates",
        "audited_witness_count": 6,
        "prior_labels_in_model_input": False,
        "candidate_outputs_in_model_input": False,
        "retry_count_per_turn": 0,
        "reference_change_allowed_in_this_version": False,
        "fresh_diagnostic_required_after_any_reference_freeze": True,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v62_root_status.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v61_adjudication.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "input": _record(input_path),
            "old_truth": _record(old_truth_path),
            "turn_input": _record(paths["input"]),
            "prompt": _record(paths["prompt"]),
            "schema": _record(paths["schema"]),
        },
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "reference-reaudit-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v63 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV63ReferenceError("immutable v63 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "value": value,
        "old_truth": old_truth,
        "prompt": prompt,
        "schema": schema,
        "paths": paths,
        "capacity_policy": capacity["policy"],
    }


def _write_failure(root: Path, error_class: str) -> dict[str, Any]:
    attempts = _attempt_records(root)
    known = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            continue
        try:
            usage = _validate_usage(_load_json(Path(record["path"]), "v63 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            known[field] += usage[field]
    complete = unknown == 0
    failure = {
        "schema_version": V63_FAILURE_VERSION,
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
        "schema_version": V63_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "reference_frozen": False,
        "fresh_diagnostic_authorized": False,
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


async def run_v63(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v62_root: Path = DEFAULT_V62_ROOT,
    model: str = "gpt-5.6-sol",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v63 terminal")
    frozen = freeze_v63(
        output_dir=root,
        v62_root=v62_root,
        model=model,
        reasoning_effort=reasoning_effort,
        timeout_seconds=timeout_seconds,
    )
    factory = client_factory or _client_factory
    try:
        async with factory(frozen["capacity_policy"]) as client:
            output, sidecar, was_adopted = await _get_or_run_turn(
                client=client,
                turn_name=TURN_NAME,
                paths=frozen["paths"],
                prompt=frozen["prompt"],
                schema=frozen["schema"],
                base_instructions=v63_base_instructions(),
                model=model,
                effort=reasoning_effort,
                timeout_seconds=timeout_seconds,
                batch_size=6,
                policy_path=frozen["capacity_policy"],
                output_validator=lambda value: validate_v62_output(
                    project_v62_contract(value, frozen["value"])[0], frozen["value"]
                ),
            )
        projected, operations = project_v62_contract(output, frozen["value"])
        if validate_v62_output(projected, frozen["value"]):
            raise JudgeV5CalibrationV63ReferenceError("projected reference output remained invalid")
        output_path = root / "reference-output.private.json"
        _write_immutable(output_path, projected)
        audit = {
            "schema_version": V63_AUDIT_VERSION,
            "created_at": now_iso(),
            "operation_count": len(operations),
            "operations": operations,
            "reference_semantics_changed_by_deterministic_code": False,
            "privacy": "opaque_ids_enums_counts_and_span_hashes_only",
        }
        audit_path = root / "contract-projection-audit.json"
        _write_immutable(audit_path, audit)
        proposal = build_reference_proposal(projected, frozen["old_truth"])
        proposal["contract_projection_audit"] = _record(audit_path)
        proposal_path = root / "reference-patch-proposal.json"
        _write_immutable(proposal_path, proposal)
        accounting = _aggregate_usage([sidecar])
        changes = bool(proposal["reference_patch_proposed"])
        terminal = {
            "schema_version": V63_TERMINAL_VERSION,
            "state": "completed",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v63_reference_patch_proposal_completed_freeze_required"
                if changes
                else "v63_reference_reaffirmed_protocol_nonacceptance_required"
            ),
            "overall_evaluation_complete": False,
            "reference_patch_proposed": changes,
            "reference_freeze_authorized": changes,
            "reference_frozen": False,
            "fresh_diagnostic_required": True,
            "fresh_diagnostic_authorized": False,
            "full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "proposal": _record(proposal_path),
            "output": _record(output_path),
            "contract_projection_audit": _record(audit_path),
            "attempts": _attempt_records(root),
            "completed_checkpoint_adoptions": {TURN_NAME: was_adopted},
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.error_class)
    except Exception as exc:
        return _write_failure(root, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v63 side-free reference-owner re-audit")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v62-root", default=str(DEFAULT_V62_ROOT))
    parser.add_argument("--model", default="gpt-5.6-sol")
    parser.add_argument("--reasoning-effort", default="high")
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v63(
            output_dir=Path(args.output_dir),
            v62_root=Path(args.v62_root),
            model=args.model,
            reasoning_effort=args.reasoning_effort,
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "reference_patch_proposed": terminal.get("reference_patch_proposed", False),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
