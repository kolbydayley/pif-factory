from __future__ import annotations

"""Reconcile only the two observable v165 permutation disagreements."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v153_capped_alignment_adjudication as v153
from . import app_server_judge_v5_calibration_v157_exact_span_canary_recovery as v157
from . import app_server_judge_v5_calibration_v165_alignment_truth_owner as v165
from .app_server_judge_v5 import (
    POINTWISE_OUTPUT_VERSION,
    adjudication_alignment_input,
    build_disagreement_adjudication_input,
    neutral_alignment_output_schema,
    validate_neutral_alignment_output,
)
from .app_server_judge_v5_calibration_v25_diagnostic import QUOTA_POINTS_PER_MILLION_TOKENS
from .app_server_judge_v5_calibration_v26_diagnostic import (
    JudgeV5CalibrationV26DiagnosticAttemptFailed,
    _aggregate_usage,
    _client_factory,
    _freeze_turn_request,
    _get_or_run_turn,
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import _record, _verify_record
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V166_SPEC_VERSION = "pif_app_server_judge_v5_4_v166_spec_v1"
V166_SCORE_VERSION = "pif_app_server_judge_v5_4_v166_score_v1"
V166_PROTOCOL_VERSION = "pif_app_server_judge_v5_4_v166_alignment_protocol_v1"
V166_AUDIT_VERSION = "pif_app_server_judge_v5_4_v166_reconciliation_audit_v1"
V166_FAILURE_VERSION = "pif_app_server_judge_v5_4_v166_failure_v1"
V166_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v166_terminal_v1"
V166_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V166_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V166_PHASE_ID = "judge_v5_4_v166_capped_owner_reconciliation"

MODEL = "gpt-5.5"
EFFORT = "high"
TURN_NAME = "capped_owner_permutation_adjudication"
MAXIMUM_TOTAL_TOKENS_PER_TURN = 45_000
TIMEOUT_SECONDS = v165.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v165.DEFAULT_OUTPUT_ROOT.parent / "judge-calibration-v5_4-v166-capped-owner-reconciliation"
).resolve()


class JudgeV5CalibrationV166Error(RuntimeError):
    """The immutable v166 reconciliation contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v165() -> dict[str, Any]:
    root = v165.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "score": root / "alignment-truth-owner-score.json",
        "spec": root / "alignment-truth-owner-spec.json",
        "truth": root / "alignment-truth-owner-input.private.json",
        "primary": root / "owner-primary-projected.private.json",
        "canary": root / "owner-canary-projected.private.json",
        "primary_input": root / "turns" / "alignment-truth-owner-primary" / "input.private.json",
        "canary_input": root / "turns" / "alignment-truth-owner-canary" / "input.private.json",
        "primary_raw": root / "turns" / "alignment-truth-owner-primary" / "output.private.json",
        "canary_raw": root / "turns" / "alignment-truth-owner-canary" / "output.private.json",
    }
    values = {name: _load_json(path, f"v165 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    attempts = [
        row for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    sidecars = []
    for row in attempts:
        record = row.get("sidecar")
        if not isinstance(record, Mapping):
            raise JudgeV5CalibrationV166Error("v165 measured sidecar coverage drifted")
        _verify_record(record)
        _validate_usage(_load_json(Path(record["path"]), "v165 sidecar"))
        sidecars.append(record)
    metrics = score.get("metrics", {})
    retrospective = metrics.get("retrospective_v164_metrics") or {}
    if (
        terminal.get("state") != "inactive"
        or terminal.get("development_terminal_reason") != "v165_alignment_truth_owner_quality_gate_not_passed"
        or terminal.get("failed_quality_gates") != ["canary_controls_exact", "permutation_exact"]
        or terminal.get("reference_frozen") is not False
        or terminal.get("fresh_full_replacement_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 82055
        or terminal.get("cumulative_calibration_usage", {}).get("total_tokens") != 3186051
        or metrics.get("primary_control_exact_count") != 5
        or metrics.get("canary_control_exact_count") != 3
        or metrics.get("permutation_exact_count") != 4
        or metrics.get("target_owner_consistent") is not True
        or metrics.get("target_relation") != "equivalent"
        or metrics.get("target_mismatch_fields") != []
        or any(retrospective.get(key, 0) < 0.95 for key in (
            "alignment_f1", "relation_accuracy", "equivalent_sensitivity",
            "equivalent_specificity", "mismatch_field_f1",
            "equivalence_partition_exact_case_rate", "unpaired_exact_case_rate",
        ))
        or len(attempts) != 2
        or len(sidecars) != 2
        or spec.get("maximum_turn_count") != 2
        or spec.get("retry_count_per_turn") != 0
    ):
        raise JudgeV5CalibrationV166Error("v165 owner terminal drifted")
    for record in spec["runtime_files"]:
        _verify_record(record)
    v164_source = v165._validate_v164()
    data = v165.build_v165_inputs(v164_source)
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "sidecars": sidecars,
        "v164": v164_source,
        "data": data,
        "cumulative_usage": terminal["cumulative_calibration_usage"],
    }


def _support_receipts(base_input: Mapping[str, Any]) -> dict[str, Any]:
    units = []
    for case in base_input["cases"]:
        for witness in case["witnesses"]:
            units.append(
                {
                    "case_id": case["case_id"],
                    "witness_id": witness["witness_id"],
                    **deepcopy(witness["support_receipt"]),
                }
            )
    return {
        "schema_version": POINTWISE_OUTPUT_VERSION,
        "units": units,
        "side_free": True,
        "claim_support_and_field_correctness_separate": True,
    }


def build_v166_input(source: Mapping[str, Any]) -> dict[str, Any]:
    values = source["values"]
    receipts = _support_receipts(values["primary_input"])
    packet = build_disagreement_adjudication_input(
        base_input=values["primary_input"],
        base_output=values["primary_raw"],
        canary_input=values["canary_input"],
        canary_output=values["canary_raw"],
        support_receipts=receipts,
    )
    packet = v153._balance_anonymous_candidates(packet)
    alignment_input = adjudication_alignment_input(
        base_input=values["primary_input"], adjudication_input=packet
    )
    rows = {row["owner_case_id"]: row for row in source["data"]["rows"]}
    disagreement_ids = {str(row["case_id"]) for row in packet["cases"]}
    expected_ids = {
        key for key, row in rows.items()
        if source["values"]["primary"][key] != source["values"]["canary"][key]
    }
    if (
        packet.get("adjudication_required") is not True
        or packet.get("call_cap") != 1
        or packet.get("anonymous_candidate_swap_count") != 1
        or len(disagreement_ids) != 2
        or disagreement_ids != expected_ids
        or source["data"]["target_owner_case_id"] in disagreement_ids
        or any(rows[key]["role"] != "settled_control" for key in disagreement_ids)
        or any(rows[key]["expected"]["pairs"][0]["relation"] != "partial" for key in disagreement_ids)
        or len(alignment_input["cases"]) != 2
        or alignment_input.get("side_labels_present") is not False
        or alignment_input.get("system_identity_present") is not False
    ):
        raise JudgeV5CalibrationV166Error("v166 observable disagreement coverage drifted")
    return {
        "packet": packet,
        "alignment_input": alignment_input,
        "support_receipts": receipts,
        "disagreement_ids": sorted(disagreement_ids),
        "expected": {key: deepcopy(rows[key]["expected"]) for key in sorted(disagreement_ids)},
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    audit = {
        "schema_version": V166_CAPACITY_AUDIT_VERSION,
        "phase_id": V166_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V166_CAPACITY_POLICY_VERSION,
        "phase_id": V166_PHASE_ID,
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
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "phase_total_token_bound": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "projected_phase_quota_points": math.ceil(
            MAXIMUM_TOTAL_TOKENS_PER_TURN * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
        ),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v166(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v166 terminal")}
    source = _validate_v165()
    value = build_v166_input(source)
    prompt = v153.adjudication_prompt_v153(value)
    schema = neutral_alignment_output_schema(value["alignment_input"])
    paths = _freeze_turn_request(
        root=root, turn_name=TURN_NAME, input_value=value,
        prompt=prompt, schema=schema,
    )
    truth_path = root / "capped-reconciliation-truth.private.json"
    _write_immutable(
        truth_path,
        {
            "schema_version": V166_SCORE_VERSION,
            "disagreement_ids": value["disagreement_ids"],
            "expected": value["expected"],
            "target_owner_case_id": source["data"]["target_owner_case_id"],
            "target_relation_from_both_v165_orientations": "equivalent",
        },
    )
    predecessor = {
        **{f"v165_{name}": record for name, record in source["records"].items()},
        "v165_sidecars": source["sidecars"],
        "cumulative_usage": source["cumulative_usage"],
    }
    capacity = _build_capacity_policy(root, predecessor)
    spec = {
        "schema_version": V166_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "one_capped_side_free_owner_pass_only_for_observable_permutation_disagreements",
        "observable_disagreement_case_count": 2,
        "adjudication_call_cap": 1,
        "anonymous_candidate_swap_count": 1,
        "majority_voting_used": False,
        "turn_plan": [TURN_NAME],
        "minimum_turn_count": 1,
        "maximum_turn_count": 1,
        "retry_count_per_turn": 0,
        "truth_labels_exposed_to_model": False,
        "fresh_full_replacement_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [_record(Path(__file__)), *source["values"]["spec"]["runtime_files"]],
        "frozen_instructions": {"adjudication_sha256": sha256_text(v153.adjudication_instructions_v153())},
        "frozen_inputs": {
            "truth": _record(truth_path),
            "input": _record(paths["input"]),
            "prompt": _record(paths["prompt"]),
            "schema": _record(paths["schema"]),
        },
        "privacy": "private_source_event_candidates_output_truth_sanitized_terminal_only",
    }
    spec_path = root / "capped-owner-reconciliation-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "paths": paths,
        "prompt": prompt,
        "schema": schema,
        "value": value,
        "source": source,
    }


def score_v166(
    *, adjudicated: Mapping[str, Any], source: Mapping[str, Any], value: Mapping[str, Any]
) -> dict[str, Any]:
    if set(adjudicated) != set(value["expected"]):
        raise JudgeV5CalibrationV166Error("v166 adjudication coverage drifted")
    exact_count = sum(adjudicated[key] == value["expected"][key] for key in value["expected"])
    abstention_count = sum(
        pair["relation"] == "abstain"
        for case in adjudicated.values() for pair in case["pairs"]
    )
    reconciled = deepcopy(source["values"]["primary"])
    reconciled.update(deepcopy(dict(adjudicated)))
    repaired = v165.score_v165(
        primary=reconciled,
        canary=deepcopy(reconciled),
        data=source["data"],
        source=source["v164"],
    )
    checks = {
        "adjudicated_controls_exact": exact_count == 2,
        "adjudication_no_abstention": abstention_count == 0,
        "repaired_v165_all_gates": repaired["passed"] is True,
    }
    passed = all(checks.values())
    return {
        "schema_version": V166_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, result in checks.items() if not result),
        "metrics": {
            "observable_disagreement_case_count": 2,
            "adjudicated_control_exact_count": exact_count,
            "adjudication_abstention_count": abstention_count,
            "repaired_v165_metrics": repaired["metrics"],
        },
        "reference_patch_authorized": passed,
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v166 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    predecessor = _validate_v165()["cumulative_usage"]
    cumulative = _sum_usage(predecessor, usage)
    failure = {
        "schema_version": V166_FAILURE_VERSION,
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
        "schema_version": V166_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "reference_frozen": False,
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


async def run_v166(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v166 terminal")
    frozen = freeze_v166(output_dir=root, timeout_seconds=timeout_seconds)
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            output, sidecar, _ = await _get_or_run_turn(
                client=client,
                turn_name=TURN_NAME,
                paths=frozen["paths"],
                prompt=frozen["prompt"],
                schema=frozen["schema"],
                base_instructions=v153.adjudication_instructions_v153(),
                model=MODEL,
                effort=EFFORT,
                timeout_seconds=timeout_seconds,
                batch_size=2,
                policy_path=frozen["capacity_policy"],
                output_validator=lambda candidate: validate_neutral_alignment_output(
                    candidate, frozen["value"]["alignment_input"]
                ),
            )
        projected, projection_audit = v157.project_exact_spans_and_relation(
            output, frozen["value"]["alignment_input"]
        )
        adjudicated = v165._merged(
            [projected],
            [{"value": frozen["value"]["alignment_input"]}],
        )
        output_path = root / "capped-adjudication-projected.private.json"
        projection_path = root / "structural-projection-audit.json"
        _write_immutable(output_path, adjudicated)
        _write_immutable(projection_path, projection_audit)
        score = score_v166(adjudicated=adjudicated, source=frozen["source"], value=frozen["value"])
        score_path = root / "capped-owner-reconciliation-score.json"
        _write_immutable(score_path, score)
        passed = bool(score["passed"])
        corrected = v165._write_corrected_reference(
            root=root,
            data=frozen["source"]["data"],
            source=frozen["source"]["v164"],
            primary_path=frozen["source"]["paths"]["primary"],
            canary_path=frozen["source"]["paths"]["canary"],
        ) if passed else None
        audit_path = root / "capped-reconciliation-audit.json"
        protocol_path = root / "alignment-protocol-v166.json"
        if passed:
            _write_immutable(
                audit_path,
                {
                    "schema_version": V166_AUDIT_VERSION,
                    "observable_disagreement_case_count": 2,
                    "adjudication_call_count": 1,
                    "adjudication_call_cap": 1,
                    "anonymous_candidate_swap_count": 1,
                    "majority_voting_used": False,
                    "adjudication_model": MODEL,
                    "adjudication_output": _record(output_path),
                    "score": _record(score_path),
                    "v165_primary": frozen["source"]["records"]["primary"],
                    "v165_canary": frozen["source"]["records"]["canary"],
                    "reference_patch_audit": _record(corrected["audit"]),
                    "reference": _record(corrected["reference"]),
                    "truth": _record(corrected["truth"]),
                    "selection_authorized": False,
                    "holdout_authorized": False,
                    "production_mutated": False,
                },
            )
            _write_immutable(
                protocol_path,
                {
                    "schema_version": V166_PROTOCOL_VERSION,
                    "frozen_at": now_iso(),
                    "primary_alignment_model": "gpt-5.5",
                    "equivalent_pair_verifier_model": "gpt-5.6-sol",
                    "reference_owner_model": "gpt-5.4",
                    "disagreement_adjudicator_model": MODEL,
                    "reasoning_effort": EFFORT,
                    "observable_disagreement_only": True,
                    "adjudication_call_cap": 1,
                    "majority_voting_used": False,
                    "reference": _record(corrected["reference"]),
                    "truth": _record(corrected["truth"]),
                    "reconciliation_audit": _record(audit_path),
                    "quality_gates_unchanged": True,
                    "fresh_full_replacement_calibration_authorized": True,
                    "selection_authorized": False,
                    "holdout_authorized": False,
                    "production_mutation_allowed": False,
                },
            )
        accounting = _aggregate_usage([sidecar])
        predecessor = frozen["source"]["cumulative_usage"]
        cumulative = _sum_usage(predecessor, accounting["usage"])
        terminal = {
            "schema_version": V166_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": "v166_reference_reconciled_full_calibration_authorized" if passed else "inactive_incomplete_recovery_required",
            "development_terminal_reason": "v166_capped_owner_reconciliation_passed" if passed else "v166_capped_owner_reconciliation_quality_gate_not_passed",
            "overall_evaluation_complete": False,
            "reference_frozen": passed,
            "fresh_full_replacement_calibration_authorized": passed,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "score": _record(score_path),
            "reference": _record(corrected["reference"]) if corrected else None,
            "truth": _record(corrected["truth"]) if corrected else None,
            "reconciliation_audit": _record(audit_path) if passed else None,
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
        return _write_failure(root, TURN_NAME, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v166 capped owner reconciliation")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(run_v166(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds))
    print(json.dumps({
        "state": terminal["state"],
        "terminal_reason": terminal["terminal_reason"],
        "reference_frozen": terminal.get("reference_frozen", False),
        "fresh_full_replacement_calibration_authorized": terminal.get("fresh_full_replacement_calibration_authorized", False),
        "usage_status": terminal.get("usage_status"),
    }, sort_keys=True))
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
