from __future__ import annotations

"""Hash-bound pipeline-v5 reuse of immutable pipeline-v1 through v4 evidence."""

import argparse
import hashlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_judge_v5_fixture import FIXTURE_AUDIT_RECEIPT_VERSION
from .app_server_llm_judge import write_immutable_json
from .app_server_v2_reuse import verify_v4_reuse_contract
from .util import now_iso


PIPELINE_V5_REUSE_CONTRACT_VERSION = "pif_app_server_pipeline_v5_reuse_contract_v4"
EXPECTED_V4_TURNS = 22


class V5ReuseContractError(ValueError):
    """Pipeline-v5 predecessor evidence changed or permits an unsafe replay."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _inside(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


def _load_json(path: Path, *, purpose: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V5ReuseContractError(f"{purpose} is missing or invalid") from exc
    if not isinstance(value, dict):
        raise V5ReuseContractError(f"{purpose} is not an object")
    return value


def _file_record(path: Path, *, root: Optional[Path] = None) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file() or (root is not None and not _inside(resolved, root)):
        raise V5ReuseContractError("required immutable predecessor artifact is missing")
    return {
        "path": str(resolved),
        "sha256": _sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _verify_record(record: Mapping[str, Any], *, root: Optional[Path] = None) -> None:
    if set(record) != {"path", "sha256", "size_bytes"}:
        raise V5ReuseContractError("predecessor artifact record shape drifted")
    path = Path(str(record.get("path") or "")).expanduser().resolve()
    if (
        not path.is_file()
        or path.stat().st_size != record.get("size_bytes")
        or _sha256_file(path) != record.get("sha256")
        or (root is not None and not _inside(path, root))
    ):
        raise V5ReuseContractError("immutable predecessor artifact changed")


def _absence_record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if resolved.exists():
        raise V5ReuseContractError("forbidden predecessor continuation artifact exists")
    return {"path": str(resolved), "must_remain_absent": True}


def _verify_absence(record: Mapping[str, Any], *, root: Path) -> None:
    if set(record) != {"path", "must_remain_absent"} or record.get(
        "must_remain_absent"
    ) is not True:
        raise V5ReuseContractError("predecessor absence record drifted")
    path = Path(str(record.get("path") or "")).expanduser().resolve()
    if not _inside(path, root) or path.exists():
        raise V5ReuseContractError("predecessor absence invariant failed")


def _sum_usage(sidecars: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    fields = (
        "input_tokens",
        "cached_input_tokens",
        "output_tokens",
        "reasoning_output_tokens",
        "total_tokens",
    )
    total = {field: 0 for field in fields}
    for sidecar in sidecars:
        usage = sidecar.get("usage")
        if (
            sidecar.get("state") != "completed"
            or sidecar.get("status") != "completed"
            or sidecar.get("usage_complete") is not True
            or sidecar.get("usage_status") != "measured"
            or sidecar.get("recovery_reran_model") is not False
            or not isinstance(usage, dict)
        ):
            raise V5ReuseContractError("pipeline-v4 turn usage is incomplete")
        for field in fields:
            value = usage.get(field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise V5ReuseContractError("pipeline-v4 turn usage is malformed")
            total[field] += value
    if (
        total["cached_input_tokens"] > total["input_tokens"]
        or total["reasoning_output_tokens"] > total["output_tokens"]
        or total["total_tokens"] != total["input_tokens"] + total["output_tokens"]
    ):
        raise V5ReuseContractError("pipeline-v4 usage arithmetic drifted")
    return total


def build_v5_reuse_contract(
    *,
    repo_root: Path,
    output_path: Path,
    fixture_audit_receipt_path: Optional[Path] = None,
) -> dict[str, Any]:
    repo = repo_root.expanduser().resolve()
    output = output_path.expanduser().resolve()
    if output.exists():
        return verify_v5_reuse_contract(output)
    v4_root = repo / "work/app-server-development-v2/unattended-pipeline-v4"
    v4_contract_path = v4_root / "reuse-contract-v3.json"
    v4_contract = verify_v4_reuse_contract(v4_contract_path)
    predecessor_roots = {
        "pipeline_v1": Path(v4_contract["source_pipeline_v1_root"]).resolve(),
        "pipeline_v2": Path(v4_contract["source_pipeline_v2_root"]).resolve(),
        "pipeline_v3": Path(v4_contract["source_pipeline_v3_root"]).resolve(),
        "pipeline_v4": v4_root.resolve(),
    }
    if any(output == root or _inside(output, root) for root in predecessor_roots.values()):
        raise V5ReuseContractError("pipeline-v5 contract overlaps an immutable predecessor")
    audit_receipt_path = (
        fixture_audit_receipt_path
        or output.parent / "fixture-truth-audit-receipt-v1.json"
    ).expanduser().resolve()
    audit_receipt = _load_json(audit_receipt_path, purpose="fixture audit receipt")
    if (
        audit_receipt.get("schema_version") != FIXTURE_AUDIT_RECEIPT_VERSION
        or audit_receipt.get("status")
        != "verified_before_pipeline_v5_semantic_calls"
        or audit_receipt.get("legacy_joint_support_labels_admissible") is not False
        or audit_receipt.get("semantic_model_calls_performed") != 0
    ):
        raise V5ReuseContractError("fixture audit receipt is not admissible")

    selection_root = v4_root / "development-selection-sharded-v3"
    calibration_root = selection_root / "calibration"
    terminal_path = v4_root / "pipeline-terminal.json"
    selection_path = selection_root / "selection-result.json"
    report_path = calibration_root / "report.json"
    terminal = _load_json(terminal_path, purpose="pipeline-v4 terminal")
    selection = _load_json(selection_path, purpose="pipeline-v4 selection terminal")
    report = _load_json(report_path, purpose="pipeline-v4 calibration report")
    if (
        terminal.get("schema_version")
        != "pif_app_server_sharded_selection_pipeline_terminal_v4"
        or terminal.get("status") != "blocked"
        or terminal.get("terminal_phase")
        != "03_full_fresh_sharded_development_selection"
        or terminal.get("reason_code") != "judge_calibration_gate_not_passed"
        or terminal.get("production_mutation_performed") is not False
        or terminal.get("production_promotion_performed") is not False
        or selection.get("selection_status") != "blocked"
        or selection.get("terminal_classification")
        != "judge_calibration_gate_not_passed"
        or selection.get("winner_frozen") is not False
        or selection.get("holdout_preparation_authorized") is not False
        or selection.get("holdout_model_calls_authorized") is not False
        or report.get("state") != "completed"
        or report.get("calibrated") is not False
        or report.get("fail_closed_reason") != "fixture_gates_failed"
        or report.get("terminal_classification")
        != "judge_calibration_gate_not_passed"
        or report.get("required_turn_count") != EXPECTED_V4_TURNS
        or report.get("completed_turn_count") != EXPECTED_V4_TURNS
        or report.get("accounting_complete") is not True
        or report.get("usage_status") != "complete"
        or report.get("retry_count") != 0
    ):
        raise V5ReuseContractError("pipeline-v4 measured terminal contract drifted")

    attempts = []
    sidecar_payloads = []
    for shard_index in range(11):
        shard_root = calibration_root / ("shards/calibration-shard-%03d" % shard_index)
        judge_root = shard_root / "judge"
        for orientation in ("ab", "ba"):
            sidecar_path = judge_root / "sidecars" / (orientation + ".json")
            capacity_path = judge_root / "sidecars" / (orientation + ".capacity.json")
            output_turn_path = judge_root / ("output-%s.private.json" % orientation)
            prompt_path = judge_root / ("prompt-%s.private.md" % orientation)
            schema_path = judge_root / ("schema-%s.json" % orientation)
            sidecar = _load_json(sidecar_path, purpose="pipeline-v4 turn sidecar")
            capacity = _load_json(capacity_path, purpose="pipeline-v4 capacity checkpoint")
            if (
                capacity.get("managed_chatgpt_auth_verified") is not True
                or capacity.get("maximum_primary_used_percent") != 20
                or capacity.get("primary_used_percent", 101) > 20
                or capacity.get("rate_limit_reached_type") is not None
                or capacity.get("cleared_for_semantic_turn") is not True
                or capacity.get("retry_checkpoint_reuse_allowed") is not False
                or not output_turn_path.is_file()
            ):
                raise V5ReuseContractError("pipeline-v4 turn telemetry drifted")
            sidecar_payloads.append(sidecar)
            attempts.append(
                {
                    "shard_index": shard_index,
                    "orientation": orientation,
                    "sidecar": _file_record(sidecar_path, root=v4_root),
                    "capacity_checkpoint": _file_record(capacity_path, root=v4_root),
                    "output": _file_record(output_turn_path, root=v4_root),
                    "prompt": _file_record(prompt_path, root=v4_root),
                    "output_schema": _file_record(schema_path, root=v4_root),
                }
            )
    measured_usage = _sum_usage(sidecar_payloads)
    if measured_usage != report.get("usage"):
        raise V5ReuseContractError("pipeline-v4 aggregate usage drifted")

    control_root = repo / "work/app-server-development-v2"
    receipt_paths = (
        "unattended-control-v1/incident-snapshot-v1.json",
        "unattended-control-v1/launch-receipt.json",
        "unattended-control-v2/launch-receipt-v2.json",
        "unattended-control-v3/launch-receipt-v3.json",
        "unattended-control-v4/launch-receipt-v4.json",
        "unattended-control-v5/launch-receipt-v5.json",
        "unattended-control-v5/terminal-receipt-v5.json",
    )
    receipts = {
        Path(relative).stem: _file_record(control_root / relative, root=repo)
        for relative in receipt_paths
    }
    if len(receipts) != len(receipt_paths):
        raise V5ReuseContractError("predecessor receipt labels are not unique")
    runtime_locks = {
        "runtime_lock_v%d" % version: _file_record(
            control_root / ("unattended-runtime-lock-v%d.json" % version), root=repo
        )
        for version in range(1, 6)
    }
    terminals = {
        name: _file_record(root / "pipeline-terminal.json", root=root)
        for name, root in predecessor_roots.items()
    }
    v4_absent = [
        _absence_record(v4_root / "phase-markers/04_holdout_freeze.json"),
        _absence_record(v4_root / "holdout-v1/frozen-holdout.json"),
        _absence_record(
            v4_root / "holdout-execution-v1/pipeline-execution-terminal.json"
        ),
        _absence_record(v4_root / "holdout-judge-v1/gate-report.json"),
        _absence_record(v4_root / "prospective-epoch-v1/terminal.json"),
    ]
    payload = {
        "schema_version": PIPELINE_V5_REUSE_CONTRACT_VERSION,
        "created_at": now_iso(),
        "target_pipeline_root": str(output.parent),
        "source_pipeline_roots": {
            name: str(root) for name, root in predecessor_roots.items()
        },
        "policy": {
            "pipeline_v1_replay_allowed": False,
            "pipeline_v2_replay_allowed": False,
            "pipeline_v3_replay_allowed": False,
            "pipeline_v4_replay_allowed": False,
            "pipeline_v4_judge_outputs_admissible_as_passing_evidence": False,
            "extraction_model_calls_allowed": False,
            "batch_5_same_thread_retry_allowed": False,
            "source_artifact_mutation_allowed": False,
            "production_mutation_allowed": False,
            "full_v5_calibration_allowed_before_diagnostic_pass": False,
            "one_declared_attempt_per_semantic_turn": True,
            "reuse_is_hash_bound": True,
        },
        "prior_reuse_contract": _file_record(v4_contract_path, root=v4_root),
        "fixture_truth_audit_receipt": _file_record(audit_receipt_path),
        "predecessor_pipeline_terminals": terminals,
        "predecessor_receipts": receipts,
        "predecessor_runtime_locks": runtime_locks,
        "pipeline_v4_judge_calibration_incident": {
            "classification": "judge_calibration_gate_not_passed",
            "transport_failed": False,
            "judge_quality_measured": True,
            "extractor_quality_measured": False,
            "completed_turn_count": EXPECTED_V4_TURNS,
            "failed_turn_count": 0,
            "unknown_usage_turn_count": 0,
            "usage_status": "complete",
            "usage": measured_usage,
            "support_sensitivity": report["combined"]["metrics"][
                "support_sensitivity"
            ],
            "field_diagnostic_f1": report["combined"]["metrics"][
                "field_diagnostic_f1"
            ],
            "order_bias": report["combined"]["metrics"]["order_bias"],
            "retry_allowed": False,
        },
        "pipeline_v4_artifacts": {
            "pipeline_terminal": terminals["pipeline_v4"],
            "selection_result": _file_record(selection_path, root=v4_root),
            "calibration_report": _file_record(report_path, root=v4_root),
            "shard_plan": _file_record(calibration_root / "shard-plan.json", root=v4_root),
            "readiness_terminal": _file_record(
                v4_root / "provider-readiness-v1/readiness-terminal.json", root=v4_root
            ),
        },
        "pipeline_v4_attempts": attempts,
        "pipeline_v4_must_remain_absent": v4_absent,
        "clean_arms": deepcopy(v4_contract["clean_arms"]),
        "interrupted_batch_5_same_thread": deepcopy(
            v4_contract["interrupted_batch_5_same_thread"]
        ),
        "verified_provenance": deepcopy(v4_contract["verified_provenance"]),
        "selection_inputs": deepcopy(v4_contract["selection_inputs"]),
        "pipeline_v5_gate_contract": {
            "diagnostic_case_count_min": 12,
            "diagnostic_case_count_max": 18,
            "diagnostic_must_pass_before_full_calibration": True,
            "legacy_joint_support_labels_allowed": False,
            "support_and_structured_field_correctness_scored_separately": True,
            "support_pass_side_free_pointwise": True,
            "alignment_pass_origin_neutral": True,
            "full_support_ab_ba_allowed": False,
            "permutation_canary_only": True,
            "observable_disagreement_adjudication_cap": 1,
            "full_calibration_status": "not_authorized",
            "system_selection_status": "not_authorized",
            "holdout_status": "not_authorized",
        },
        "privacy": "paths_hashes_sizes_counts_metrics_and_failure_classes_no_prompt_output_or_source_text",
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    write_immutable_json(output, payload)
    return verify_v5_reuse_contract(output)


def verify_v5_reuse_contract(path: Path) -> dict[str, Any]:
    contract_path = path.expanduser().resolve()
    payload = _load_json(contract_path, purpose="pipeline-v5 reuse contract")
    if payload.get("schema_version") != PIPELINE_V5_REUSE_CONTRACT_VERSION:
        raise V5ReuseContractError("unsupported pipeline-v5 reuse contract")
    roots = {
        name: Path(str(value)).resolve()
        for name, value in (payload.get("source_pipeline_roots") or {}).items()
    }
    if set(roots) != {"pipeline_v1", "pipeline_v2", "pipeline_v3", "pipeline_v4"}:
        raise V5ReuseContractError("pipeline-v5 predecessor root coverage drifted")
    target = Path(str(payload.get("target_pipeline_root") or "")).resolve()
    if any(target == root or _inside(target, root) for root in roots.values()):
        raise V5ReuseContractError("pipeline-v5 target overlaps a predecessor")
    expected_policy = {
        "pipeline_v1_replay_allowed": False,
        "pipeline_v2_replay_allowed": False,
        "pipeline_v3_replay_allowed": False,
        "pipeline_v4_replay_allowed": False,
        "pipeline_v4_judge_outputs_admissible_as_passing_evidence": False,
        "extraction_model_calls_allowed": False,
        "batch_5_same_thread_retry_allowed": False,
        "source_artifact_mutation_allowed": False,
        "production_mutation_allowed": False,
        "full_v5_calibration_allowed_before_diagnostic_pass": False,
        "one_declared_attempt_per_semantic_turn": True,
        "reuse_is_hash_bound": True,
    }
    if payload.get("policy") != expected_policy:
        raise V5ReuseContractError("pipeline-v5 replay policy drifted")
    prior = payload.get("prior_reuse_contract") or {}
    _verify_record(prior, root=roots["pipeline_v4"])
    verify_v4_reuse_contract(Path(prior["path"]))
    for name, record in (payload.get("predecessor_pipeline_terminals") or {}).items():
        _verify_record(record, root=roots[name])
    for record in (payload.get("predecessor_receipts") or {}).values():
        _verify_record(record)
    for record in (payload.get("predecessor_runtime_locks") or {}).values():
        _verify_record(record)
    _verify_record(payload.get("fixture_truth_audit_receipt") or {})
    incident = payload.get("pipeline_v4_judge_calibration_incident") or {}
    if (
        incident.get("classification") != "judge_calibration_gate_not_passed"
        or incident.get("transport_failed") is not False
        or incident.get("judge_quality_measured") is not True
        or incident.get("extractor_quality_measured") is not False
        or incident.get("completed_turn_count") != EXPECTED_V4_TURNS
        or incident.get("failed_turn_count") != 0
        or incident.get("unknown_usage_turn_count") != 0
        or incident.get("usage_status") != "complete"
        or incident.get("usage", {}).get("total_tokens") != 684553
        or incident.get("support_sensitivity") != 0.869822
        or incident.get("field_diagnostic_f1") != 0.589744
        or incident.get("order_bias") != 0.19697
        or incident.get("retry_allowed") is not False
    ):
        raise V5ReuseContractError("pipeline-v4 incident classification drifted")
    for record in (payload.get("pipeline_v4_artifacts") or {}).values():
        _verify_record(record, root=roots["pipeline_v4"])
    attempts = payload.get("pipeline_v4_attempts")
    if not isinstance(attempts, list) or len(attempts) != EXPECTED_V4_TURNS:
        raise V5ReuseContractError("pipeline-v4 attempt coverage drifted")
    for attempt in attempts:
        for key in ("sidecar", "capacity_checkpoint", "output", "prompt", "output_schema"):
            _verify_record(attempt.get(key) or {}, root=roots["pipeline_v4"])
    absent = payload.get("pipeline_v4_must_remain_absent")
    if not isinstance(absent, list) or not absent:
        raise V5ReuseContractError("pipeline-v4 absence coverage drifted")
    for record in absent:
        _verify_absence(record, root=roots["pipeline_v4"])
    gate = payload.get("pipeline_v5_gate_contract") or {}
    if (
        gate.get("diagnostic_case_count_min") != 12
        or gate.get("diagnostic_case_count_max") != 18
        or gate.get("diagnostic_must_pass_before_full_calibration") is not True
        or gate.get("legacy_joint_support_labels_allowed") is not False
        or gate.get("support_and_structured_field_correctness_scored_separately")
        is not True
        or gate.get("full_support_ab_ba_allowed") is not False
        or gate.get("permutation_canary_only") is not True
        or gate.get("observable_disagreement_adjudication_cap") != 1
        or gate.get("full_calibration_status") != "not_authorized"
        or gate.get("system_selection_status") != "not_authorized"
        or gate.get("holdout_status") != "not_authorized"
    ):
        raise V5ReuseContractError("pipeline-v5 gate contract drifted")
    matrix_root = Path(str(payload.get("selection_inputs", {}).get("development_manifest", {}).get("path") or "")).parents[1]
    for arm in payload.get("clean_arms") or []:
        _verify_record(arm.get("report") or {})
    _verify_record(payload.get("interrupted_batch_5_same_thread") or {})
    for record in (payload.get("verified_provenance") or {}).values():
        _verify_record(record)
    for record in (payload.get("selection_inputs") or {}).values():
        _verify_record(record)
    return payload


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Freeze or verify pipeline-v5 reuse")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output", required=True)
    parser.add_argument("--fixture-audit-receipt")
    parser.add_argument("--verify-only", action="store_true")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    output = Path(args.output)
    payload = (
        verify_v5_reuse_contract(output)
        if args.verify_only
        else build_v5_reuse_contract(
            repo_root=Path(args.repo_root),
            output_path=output,
            fixture_audit_receipt_path=(
                Path(args.fixture_audit_receipt)
                if args.fixture_audit_receipt
                else None
            ),
        )
    )
    print(
        json.dumps(
            {
                "ok": True,
                "schema_version": payload["schema_version"],
                "output": str(output.expanduser().resolve()),
                "pipeline_v4_replay_allowed": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
