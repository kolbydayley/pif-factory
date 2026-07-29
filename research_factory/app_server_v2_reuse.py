from __future__ import annotations

"""Hash-bound reuse of immutable pipeline-v1 development evidence.

The v2 selection pipeline is not allowed to rerun extraction or the failed v1
calibration request.  This module turns that prohibition into an executable
contract: every reused artifact is content-addressed and every artifact that
must remain absent is checked on creation and on every later verification.
"""

import argparse
import hashlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_llm_judge import write_immutable_json
from .util import now_iso, sha256_text


REUSE_CONTRACT_VERSION = "pif_app_server_pipeline_v2_reuse_contract_v1"
PIPELINE_V3_REUSE_CONTRACT_VERSION = "pif_app_server_pipeline_v3_reuse_contract_v2"
PIPELINE_V4_REUSE_CONTRACT_VERSION = "pif_app_server_pipeline_v4_reuse_contract_v3"
V1_PIPELINE_TERMINAL_VERSION = "pif_unattended_app_server_pipeline_terminal_v1"
V1_FAILED_JUDGE_SIDECAR_VERSION = "pif_codex_app_server_turn_v2"
RECOVERED_MATRIX_VERSION = "pif_app_server_recovered_development_matrix_v1"
INTERRUPTED_ARM_VERSION = "pif_app_server_arm_intent_to_treat_interruption_v1"

EXPECTED_V1_PROMPT_BYTES = 190_840
EXPECTED_V1_OUTPUT_SCHEMA_BYTES = 38_080
EXPECTED_V1_WALL_SECONDS = 3.574
EXPECTED_CLEAN_ARM_COUNT = 5
EXPECTED_V3_COMPLETED_MEASURED_TURNS = 5
EXPECTED_V3_FAILED_UNKNOWN_USAGE_TURNS = 1
EXPECTED_V4_CALIBRATION_SHARD_SIZE = 6
EXPECTED_V4_CALIBRATION_SHARD_COUNT = 11


class ReuseContractError(ValueError):
    """A v1 input drifted or the v2 boundary would permit replay."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path, *, purpose: str) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    try:
        value = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReuseContractError("%s is missing or invalid JSON" % purpose) from exc
    if not isinstance(value, dict):
        raise ReuseContractError("%s is not a JSON object" % purpose)
    return value


def _inside(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


def _file_record(path: Path, *, root: Optional[Path] = None) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise ReuseContractError("required immutable reuse artifact is missing")
    if root is not None and not _inside(resolved, root):
        raise ReuseContractError("reuse artifact escaped its declared root")
    return {
        "path": str(resolved),
        "sha256": _sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _verify_record(record: Mapping[str, Any], *, root: Optional[Path] = None) -> None:
    if set(record) != {"path", "sha256", "size_bytes"}:
        raise ReuseContractError("immutable reuse record shape changed")
    path = Path(str(record.get("path") or "")).expanduser().resolve()
    expected_size = record.get("size_bytes")
    expected_sha = record.get("sha256")
    if (
        not path.is_file()
        or isinstance(expected_size, bool)
        or not isinstance(expected_size, int)
        or path.stat().st_size != expected_size
        or not isinstance(expected_sha, str)
        or _sha256_file(path) != expected_sha
    ):
        raise ReuseContractError("immutable reuse artifact changed")
    if root is not None and not _inside(path, root):
        raise ReuseContractError("immutable reuse artifact moved outside its root")


def _absence_record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if resolved.exists():
        raise ReuseContractError("forbidden v1 replay artifact exists")
    return {"path": str(resolved), "must_remain_absent": True}


def _verify_absence(record: Mapping[str, Any], *, v1_root: Path) -> None:
    if set(record) != {"path", "must_remain_absent"} or record.get(
        "must_remain_absent"
    ) is not True:
        raise ReuseContractError("v1 absence record is malformed")
    path = Path(str(record.get("path") or "")).expanduser().resolve()
    if not _inside(path, v1_root) or path.exists():
        raise ReuseContractError("v1 replay/output absence invariant failed")


def _validate_v1_failed_attempt(
    *, v1_root: Path
) -> tuple[dict[str, Any], dict[str, dict[str, Any]], list[dict[str, Any]]]:
    selection_root = v1_root / "development-selection"
    judge_root = selection_root / "calibration" / "judge"
    terminal_path = v1_root / "pipeline-terminal.json"
    selection_path = selection_root / "selection-result.json"
    marker_path = v1_root / "phase-markers" / "03_development_selection.json"
    sidecar_path = judge_root / "sidecars" / "ab.json"
    prompt_path = judge_root / "prompt-ab.private.md"
    schema_path = judge_root / "schema-ab.json"
    judge_spec_path = judge_root / "judge-spec.json"

    terminal = _load_json(terminal_path, purpose="pipeline-v1 terminal report")
    selection = _load_json(selection_path, purpose="pipeline-v1 selection result")
    sidecar = _load_json(sidecar_path, purpose="pipeline-v1 failed AB sidecar")
    judge_spec = _load_json(judge_spec_path, purpose="pipeline-v1 judge specification")
    schema = _load_json(schema_path, purpose="pipeline-v1 AB output schema")

    if (
        terminal.get("schema_version") != V1_PIPELINE_TERMINAL_VERSION
        or terminal.get("status") != "blocked"
        or terminal.get("terminal_phase") != "03_development_selection"
        or terminal.get("production_mutation_performed") is not False
        or terminal.get("production_promotion_performed") is not False
        or selection.get("selection_status") != "blocked"
        or selection.get("winner_frozen") is not False
        or selection.get("holdout_preparation_authorized") is not False
        or selection.get("holdout_model_calls_authorized") is not False
    ):
        raise ReuseContractError("pipeline-v1 is not the expected blocked ITT run")
    if (
        sidecar.get("schema_version") != V1_FAILED_JUDGE_SIDECAR_VERSION
        or sidecar.get("state") != "failed"
        or sidecar.get("status") != "failed"
        or sidecar.get("error_class") != "turn_failed"
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("transport") != "stdio"
        or sidecar.get("usage_complete") is not False
        or sidecar.get("usage_status") != "unknown"
        or sidecar.get("usage") is not None
        or sidecar.get("output_sha256") is not None
        or sidecar.get("prompt_bytes") != EXPECTED_V1_PROMPT_BYTES
        or sidecar.get("output_schema_bytes") != EXPECTED_V1_OUTPUT_SCHEMA_BYTES
        or sidecar.get("wall_elapsed_seconds") != EXPECTED_V1_WALL_SECONDS
        or sidecar.get("batch_size") != 66
        or sidecar.get("recovery_reran_model") is not False
    ):
        raise ReuseContractError("pipeline-v1 AB failure evidence drifted")
    if prompt_path.stat().st_size != EXPECTED_V1_PROMPT_BYTES:
        raise ReuseContractError("pipeline-v1 failed prompt size drifted")
    schema_canonical = _canonical_json(schema)
    if (
        len(schema_canonical.encode("utf-8")) != EXPECTED_V1_OUTPUT_SCHEMA_BYTES
        or sha256_text(schema_canonical) != sidecar.get("output_schema_sha256")
        or _sha256_file(prompt_path) != sidecar.get("prompt_sha256")
        or judge_spec.get("case_ids") is None
        or len(judge_spec["case_ids"]) != 66
    ):
        raise ReuseContractError("pipeline-v1 failed request envelope drifted")

    artifacts = {
        "pipeline_terminal": _file_record(terminal_path, root=v1_root),
        "development_selection_marker": _file_record(marker_path, root=v1_root),
        "development_selection_result": _file_record(selection_path, root=v1_root),
        "development_selection_spec": _file_record(
            selection_root / "selection-spec.json", root=v1_root
        ),
        "failed_ab_sidecar": _file_record(sidecar_path, root=v1_root),
        "failed_ab_prompt": _file_record(prompt_path, root=v1_root),
        "failed_ab_output_schema": _file_record(schema_path, root=v1_root),
        "failed_judge_spec": _file_record(judge_spec_path, root=v1_root),
    }
    absent = [
        _absence_record(judge_root / "output-ab.private.json"),
        _absence_record(judge_root / "sidecars" / "ba.json"),
        _absence_record(judge_root / "output-ba.private.json"),
        _absence_record(judge_root / "report.json"),
        _absence_record(judge_root / "consensus.private.json"),
        _absence_record(selection_root / "calibration" / "report.json"),
    ]
    incident = {
        "classification": "infrastructure_or_judge_attempt_failed",
        "quality_measured": False,
        "cost_measured": False,
        "attempted_variant": "ab",
        "ab_status": "failed",
        "ba_status": "not_started",
        "prompt_bytes": EXPECTED_V1_PROMPT_BYTES,
        "output_schema_bytes": EXPECTED_V1_OUTPUT_SCHEMA_BYTES,
        "wall_elapsed_seconds": EXPECTED_V1_WALL_SECONDS,
        "output_present": False,
        "usage_status": "unknown",
        "outer_v1_reason_retained_as_historical_evidence": terminal.get("reason_code"),
    }
    return incident, artifacts, absent


def build_v2_reuse_contract(
    *,
    repo_root: Path,
    output_path: Path,
    v1_root: Optional[Path] = None,
    matrix_root: Optional[Path] = None,
) -> dict[str, Any]:
    repo = repo_root.expanduser().resolve()
    v1 = (v1_root or repo / "work/app-server-development-v2/unattended-pipeline-v1").resolve()
    matrix = (matrix_root or repo / "work/app-server-development-v2/matrix-v1").resolve()
    output = output_path.expanduser().resolve()
    if not _inside(v1, repo) or not _inside(matrix, repo):
        raise ReuseContractError("v1 or matrix root escaped the repository")
    if _inside(output, v1) or output == v1:
        raise ReuseContractError("v2 reuse contract cannot be written inside pipeline-v1")
    if output.exists():
        return verify_v2_reuse_contract(output)

    incident, v1_artifacts, absent = _validate_v1_failed_attempt(v1_root=v1)
    recovered_path = matrix / "recovered-matrix-report.json"
    recovered = _load_json(recovered_path, purpose="recovered five-arm matrix")
    clean_arms = recovered.get("clean_arms")
    if (
        recovered.get("schema_version") != RECOVERED_MATRIX_VERSION
        or recovered.get("clean_arm_count") != EXPECTED_CLEAN_ARM_COUNT
        or recovered.get("five_clean_arm_selection_eligible") is not True
        or recovered.get("selection_eligible") is not True
        or recovered.get("clean_six_arm_matrix_achieved") is not False
        or not isinstance(clean_arms, list)
        or len(clean_arms) != EXPECTED_CLEAN_ARM_COUNT
    ):
        raise ReuseContractError("recovered matrix is not the frozen five-arm design")
    arm_records = []
    identities = set()
    for arm in clean_arms:
        if not isinstance(arm, dict):
            raise ReuseContractError("recovered matrix arm record is malformed")
        identity = (arm.get("batch_size"), arm.get("thread_mode"))
        if identity in identities or arm.get("selection_eligible") is not True:
            raise ReuseContractError("recovered matrix arm identity is invalid")
        identities.add(identity)
        report_path = Path(str(arm.get("report_path") or "")).expanduser().resolve()
        record = _file_record(report_path, root=matrix)
        if record["sha256"] != arm.get("report_sha256"):
            raise ReuseContractError("recovered arm report hash drifted")
        arm_records.append(
            {
                "batch_size": identity[0],
                "thread_mode": identity[1],
                "selection_eligible": True,
                "report": record,
            }
        )
    expected_identities = {(3, "new_thread"), (3, "same_thread"), (5, "new_thread"), (8, "new_thread"), (8, "same_thread")}
    if identities != expected_identities:
        raise ReuseContractError("recovered matrix does not contain the exact five clean arms")

    itt_path = matrix / "batch-5/same_thread/intent-to-treat-interruption-v1.json"
    itt = _load_json(itt_path, purpose="batch-5 same-thread ITT provenance")
    if (
        itt.get("schema_version") != INTERRUPTED_ARM_VERSION
        or itt.get("automatic_retry_prohibited") is not True
        or itt.get("selection_eligible") is not False
        or itt.get("usage_status") != "partial_unknown"
        or itt.get("usage") is not None
        or itt.get("completed_measured_calls") != 4
        or itt.get("cancelled_unknown_usage_calls") != 1
        or itt.get("not_started_calls") != 3
    ):
        raise ReuseContractError("batch-5 same-thread ITT invariant drifted")
    recovered_declared_itt = (recovered.get("interrupted_arm_intent_to_treat_provenance") or {}).get("sha256")
    itt_record = _file_record(itt_path, root=matrix)
    if itt_record["sha256"] != recovered_declared_itt:
        raise ReuseContractError("recovered matrix ITT hash drifted")

    provenance_root = v1 / "provenance"
    provenance = {
        "reconstructed_exclusions": _file_record(
            provenance_root / "reconstructed-exclusions-v1.json", root=v1
        ),
        "reference_transform_noise": _file_record(
            provenance_root / "reference-transform-noise-v1.json", root=v1
        ),
        "instruction_provenance": _file_record(
            provenance_root / "instruction-provenance-v1.json", root=v1
        ),
    }
    exclusion = _load_json(
        Path(provenance["reconstructed_exclusions"]["path"]),
        purpose="reconstructed exclusion provenance",
    )
    transform = _load_json(
        Path(provenance["reference_transform_noise"]["path"]),
        purpose="reference transform provenance",
    )
    instruction = _load_json(
        Path(provenance["instruction_provenance"]["path"]),
        purpose="instruction provenance",
    )
    if (
        exclusion.get("complete") is not True
        or (transform.get("summary") or {}).get("all_declared_raw_artifacts_verified") is not True
        or instruction.get("complete") is not True
    ):
        raise ReuseContractError("verified v1 provenance is no longer complete")

    selection_spec = _load_json(
        v1 / "development-selection/selection-spec.json",
        purpose="pipeline-v1 selection specification",
    )
    manifest = repo / "work/app-server-development-v2/manifest.json"
    context_usage = repo / "work/windowed-acceptance-v1/paired-run-v2/evaluator-v2/context-usage-recovery-report.json"
    run_spec = repo / "work/app-server-development-v2/run-spec-v2.json"
    inputs = {
        "development_manifest": _file_record(manifest, root=repo),
        "context_usage_recovery": _file_record(context_usage, root=repo),
        "extraction_run_spec": _file_record(run_spec, root=repo),
        "preassembled_witness_pool": _file_record(
            v1 / "development-selection/witness-pool/shared-witness-pool.private.json",
            root=v1,
        ),
        "preassembled_private_mapping": _file_record(
            v1 / "development-selection/witness-pool/private-mapping.json", root=v1
        ),
        "preassembled_membership_index": _file_record(
            v1 / "development-selection/witness-pool/membership-index.private.json",
            root=v1,
        ),
        "preassembled_assembly_report": _file_record(
            v1 / "development-selection/witness-pool/assembly-report.json", root=v1
        ),
    }
    expected_input_hashes = {
        "development_manifest": selection_spec.get("manifest_sha256"),
        "context_usage_recovery": selection_spec.get("context_usage_recovery_sha256"),
        "extraction_run_spec": selection_spec.get("run_spec_sha256"),
        "preassembled_witness_pool": selection_spec.get("pool_sha256"),
        "preassembled_assembly_report": selection_spec.get("assembly_report_sha256"),
    }
    for name, expected_sha in expected_input_hashes.items():
        if inputs[name]["sha256"] != expected_sha:
            raise ReuseContractError("pipeline-v1 selection input hash drifted: %s" % name)
    assembly = _load_json(
        Path(inputs["preassembled_assembly_report"]["path"]),
        purpose="pipeline-v1 witness assembly report",
    )
    if (
        assembly.get("pool_sha256") != inputs["preassembled_witness_pool"]["sha256"]
        or assembly.get("private_mapping_sha256")
        != inputs["preassembled_private_mapping"]["sha256"]
        or assembly.get("membership_index_sha256")
        != inputs["preassembled_membership_index"]["sha256"]
        or assembly.get("manifest_sha256") != inputs["development_manifest"]["sha256"]
        or assembly.get("matrix_mode") != "five_clean_plus_terminal_interrupted"
        or assembly.get("arm_count") != EXPECTED_CLEAN_ARM_COUNT
    ):
        raise ReuseContractError("pipeline-v1 preassembled witness artifacts drifted")

    payload = {
        "schema_version": REUSE_CONTRACT_VERSION,
        "created_at": now_iso(),
        "source_pipeline_root": str(v1),
        "source_matrix_root": str(matrix),
        "target_pipeline_root": str(output.parent),
        "policy": {
            "pipeline_v1_replay_allowed": False,
            "extraction_model_calls_allowed": False,
            "failed_ab_retry_allowed": False,
            "batch_5_same_thread_retry_allowed": False,
            "v1_artifact_mutation_allowed": False,
            "reuse_is_hash_bound": True,
            "production_mutation_allowed": False,
        },
        "v1_failed_calibration_incident": incident,
        "v1_artifacts": v1_artifacts,
        "must_remain_absent": absent,
        "recovered_matrix_report": _file_record(recovered_path, root=matrix),
        "clean_arms": sorted(
            arm_records, key=lambda item: (item["batch_size"], item["thread_mode"])
        ),
        "interrupted_batch_5_same_thread": itt_record,
        "verified_provenance": provenance,
        "selection_inputs": inputs,
        "privacy": "paths_hashes_sizes_statuses_and_failure_classes_no_prompt_output_or_transcript_text",
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    write_immutable_json(output, payload)
    return verify_v2_reuse_contract(output)


def verify_v2_reuse_contract(path: Path) -> dict[str, Any]:
    contract_path = path.expanduser().resolve()
    payload = _load_json(contract_path, purpose="pipeline-v2 reuse contract")
    if payload.get("schema_version") != REUSE_CONTRACT_VERSION:
        raise ReuseContractError("unsupported pipeline-v2 reuse contract")
    v1_root = Path(str(payload.get("source_pipeline_root") or "")).expanduser().resolve()
    matrix_root = Path(str(payload.get("source_matrix_root") or "")).expanduser().resolve()
    target_root = Path(str(payload.get("target_pipeline_root") or "")).expanduser().resolve()
    if _inside(contract_path, v1_root) or target_root == v1_root or _inside(target_root, v1_root):
        raise ReuseContractError("pipeline-v2 reuse contract is not version isolated")
    policy = payload.get("policy") or {}
    if policy != {
        "pipeline_v1_replay_allowed": False,
        "extraction_model_calls_allowed": False,
        "failed_ab_retry_allowed": False,
        "batch_5_same_thread_retry_allowed": False,
        "v1_artifact_mutation_allowed": False,
        "reuse_is_hash_bound": True,
        "production_mutation_allowed": False,
    }:
        raise ReuseContractError("pipeline-v2 replay policy drifted")
    incident = payload.get("v1_failed_calibration_incident") or {}
    if (
        incident.get("classification") != "infrastructure_or_judge_attempt_failed"
        or incident.get("quality_measured") is not False
        or incident.get("cost_measured") is not False
        or incident.get("ab_status") != "failed"
        or incident.get("ba_status") != "not_started"
        or incident.get("usage_status") != "unknown"
    ):
        raise ReuseContractError("pipeline-v1 incident classification drifted")
    for record in (payload.get("v1_artifacts") or {}).values():
        _verify_record(record, root=v1_root)
    absent = payload.get("must_remain_absent")
    if not isinstance(absent, list) or not absent:
        raise ReuseContractError("pipeline-v1 absence contract is missing")
    for record in absent:
        _verify_absence(record, v1_root=v1_root)
    _verify_record(payload.get("recovered_matrix_report") or {}, root=matrix_root)
    arms = payload.get("clean_arms")
    if not isinstance(arms, list) or len(arms) != EXPECTED_CLEAN_ARM_COUNT:
        raise ReuseContractError("pipeline-v2 clean arm contract changed")
    identities = set()
    for arm in arms:
        if not isinstance(arm, dict) or arm.get("selection_eligible") is not True:
            raise ReuseContractError("pipeline-v2 clean arm record is malformed")
        identities.add((arm.get("batch_size"), arm.get("thread_mode")))
        _verify_record(arm.get("report") or {}, root=matrix_root)
    if identities != {(3, "new_thread"), (3, "same_thread"), (5, "new_thread"), (8, "new_thread"), (8, "same_thread")}:
        raise ReuseContractError("pipeline-v2 clean arm identity set changed")
    _verify_record(payload.get("interrupted_batch_5_same_thread") or {}, root=matrix_root)
    for record in (payload.get("verified_provenance") or {}).values():
        _verify_record(record, root=v1_root)
    for record in (payload.get("selection_inputs") or {}).values():
        _verify_record(record)
    return payload


def build_v3_reuse_contract(
    *,
    repo_root: Path,
    output_path: Path,
    v1_contract_path: Optional[Path] = None,
    v2_root: Optional[Path] = None,
) -> dict[str, Any]:
    """Supersede failed pipeline-v2 without mutating or retrying its shard."""

    repo = repo_root.expanduser().resolve()
    v2 = (
        v2_root or repo / "work/app-server-development-v2/unattended-pipeline-v2"
    ).resolve()
    v1_contract_file = (
        v1_contract_path or v2 / "reuse-contract-v1.json"
    ).expanduser().resolve()
    output = output_path.expanduser().resolve()
    if output.exists():
        return verify_v3_reuse_contract(output)
    if _inside(output, v2) or output == v2:
        raise ReuseContractError("pipeline-v3 reuse contract cannot be written inside pipeline-v2")
    v1_contract = verify_v2_reuse_contract(v1_contract_file)
    v1_root = Path(v1_contract["source_pipeline_root"]).resolve()
    if _inside(output, v1_root) or output == v1_root:
        raise ReuseContractError("pipeline-v3 reuse contract cannot overlap pipeline-v1")

    selection_root = v2 / "development-selection-sharded-v1"
    calibration_root = selection_root / "calibration"
    shard_root = calibration_root / "shards/calibration-shard-000"
    judge_root = shard_root / "judge"
    terminal_path = v2 / "pipeline-terminal.json"
    selection_path = selection_root / "selection-result.json"
    calibration_path = calibration_root / "report.json"
    failure_path = shard_root / "failure.json"
    sidecar_path = judge_root / "sidecars/ab.json"
    capacity_path = judge_root / "sidecars/ab.capacity.json"
    terminal = _load_json(terminal_path, purpose="pipeline-v2 terminal report")
    selection = _load_json(selection_path, purpose="pipeline-v2 selection result")
    calibration = _load_json(calibration_path, purpose="pipeline-v2 calibration report")
    failure = _load_json(failure_path, purpose="pipeline-v2 failed shard")
    sidecar = _load_json(sidecar_path, purpose="pipeline-v2 failed AB sidecar")
    capacity = _load_json(capacity_path, purpose="pipeline-v2 AB capacity checkpoint")
    if (
        terminal.get("schema_version")
        != "pif_app_server_sharded_selection_pipeline_terminal_v2"
        or terminal.get("status") != "blocked"
        or terminal.get("terminal_phase") != "02_sharded_development_selection"
        or terminal.get("reason_code") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("production_mutation_performed") is not False
        or terminal.get("production_promotion_performed") is not False
        or selection.get("selection_status") != "blocked"
        or selection.get("terminal_classification")
        != "infrastructure_or_judge_attempt_failed"
        or selection.get("winner_frozen") is not False
        or selection.get("holdout_preparation_authorized") is not False
        or selection.get("holdout_model_calls_authorized") is not False
    ):
        raise ReuseContractError("pipeline-v2 terminal classification drifted")
    if (
        calibration.get("schema_version")
        != "pif_app_server_sharded_judge_calibration_v1"
        or calibration.get("state") != "blocked"
        or calibration.get("calibrated") is not False
        or calibration.get("fail_closed_reason")
        != "infrastructure_or_judge_attempt_failed"
        or calibration.get("failed_shard_id") != "calibration-shard-000"
        or calibration.get("failed_variant") != "ab"
        or calibration.get("completed_shard_count") != 0
        or calibration.get("aggregate_authorized") is not False
        or calibration.get("accounting_complete") is not False
        or calibration.get("usage_status") != "unknown"
        or calibration.get("usage") is not None
        or calibration.get("retry_count") != 0
        or failure.get("schema_version")
        != "pif_app_server_calibration_shard_failure_v1"
        or failure.get("shard_id") != "calibration-shard-000"
        or failure.get("failed_variant") != "ab"
        or failure.get("retry_allowed") is not False
        or failure.get("aggregate_authorized") is not False
    ):
        raise ReuseContractError("pipeline-v2 shard failure contract drifted")
    if (
        sidecar.get("state") != "failed"
        or sidecar.get("status") != "failed"
        or sidecar.get("error_class") != "turn_failed"
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("usage_complete") is not False
        or sidecar.get("usage_status") != "unknown"
        or sidecar.get("usage") is not None
        or sidecar.get("output_sha256") is not None
        or sidecar.get("prompt_bytes") != 22_599
        or sidecar.get("output_schema_bytes") != 6_218
        or sidecar.get("batch_size") != 8
        or sidecar.get("recovery_reran_model") is not False
        or capacity.get("schema_version")
        != "pif_app_server_capacity_checkpoint_v1"
        or capacity.get("managed_chatgpt_auth_verified") is not True
        or capacity.get("maximum_primary_used_percent") != 20
        or not isinstance(capacity.get("primary_used_percent"), int)
        or capacity["primary_used_percent"] > 20
        or capacity.get("rate_limit_reached_type") is not None
        or capacity.get("cleared_for_semantic_turn") is not True
        or capacity.get("turn_started") is not False
        or capacity.get("retry_checkpoint_reuse_allowed") is not False
    ):
        raise ReuseContractError("pipeline-v2 failed AB telemetry drifted")

    absent = [
        _absence_record(judge_root / "output-ab.private.json"),
        _absence_record(judge_root / "sidecars/ba.capacity.json"),
        _absence_record(judge_root / "sidecars/ba.json"),
        _absence_record(judge_root / "output-ba.private.json"),
        _absence_record(judge_root / "report.json"),
        _absence_record(calibration_root / "merged-output-ab.private.json"),
        _absence_record(calibration_root / "merged-output-ba.private.json"),
        _absence_record(selection_root / "full-judge/report.json"),
    ]
    for index in range(1, 9):
        later_judge = calibration_root / (
            "shards/calibration-shard-%03d/judge" % index
        )
        absent.extend(
            [
                _absence_record(later_judge / "sidecars/ab.capacity.json"),
                _absence_record(later_judge / "sidecars/ab.json"),
                _absence_record(later_judge / "sidecars/ba.capacity.json"),
                _absence_record(later_judge / "sidecars/ba.json"),
                _absence_record(later_judge / "report.json"),
            ]
        )
    artifacts = {
        "pipeline_terminal": _file_record(terminal_path, root=v2),
        "development_selection_marker": _file_record(
            v2 / "phase-markers/02_sharded_development_selection.json", root=v2
        ),
        "development_selection_result": _file_record(selection_path, root=v2),
        "development_selection_spec": _file_record(
            selection_root / "selection-spec.json", root=v2
        ),
        "calibration_report": _file_record(calibration_path, root=v2),
        "shard_plan": _file_record(calibration_root / "shard-plan.json", root=v2),
        "failed_shard": _file_record(failure_path, root=v2),
        "failed_ab_sidecar": _file_record(sidecar_path, root=v2),
        "failed_ab_capacity_checkpoint": _file_record(capacity_path, root=v2),
        "launch_receipt": _file_record(
            repo / "work/app-server-development-v2/unattended-control-v3/launch-receipt-v3.json",
            root=repo,
        ),
        "runtime_lock": _file_record(
            repo / "work/app-server-development-v2/unattended-runtime-lock-v3.json",
            root=repo,
        ),
    }
    payload = {
        "schema_version": PIPELINE_V3_REUSE_CONTRACT_VERSION,
        "created_at": now_iso(),
        "source_pipeline_v1_root": str(v1_root),
        "source_pipeline_v2_root": str(v2),
        "target_pipeline_root": str(output.parent),
        "policy": {
            "pipeline_v1_replay_allowed": False,
            "pipeline_v2_replay_allowed": False,
            "extraction_model_calls_allowed": False,
            "failed_v1_ab_retry_allowed": False,
            "failed_v2_shard_retry_allowed": False,
            "batch_5_same_thread_retry_allowed": False,
            "source_artifact_mutation_allowed": False,
            "reuse_is_hash_bound": True,
            "production_mutation_allowed": False,
        },
        "v1_reuse_contract": _file_record(v1_contract_file, root=v2),
        "v2_failed_sharded_calibration_incident": {
            "classification": "infrastructure_or_judge_attempt_failed",
            "quality_measured": False,
            "cost_measured": False,
            "failed_shard_id": "calibration-shard-000",
            "failed_variant": "ab",
            "prompt_bytes": sidecar["prompt_bytes"],
            "output_schema_bytes": sidecar["output_schema_bytes"],
            "usage_status": "unknown",
            "ba_status": "not_started",
            "later_shards_started": 0,
            "retry_allowed": False,
            "suspected_failure_class": "unsupported_structured_output_schema_keyword",
        },
        "v2_artifacts": artifacts,
        "v2_must_remain_absent": absent,
        "source_matrix_root": v1_contract["source_matrix_root"],
        "clean_arms": deepcopy(v1_contract["clean_arms"]),
        "interrupted_batch_5_same_thread": deepcopy(
            v1_contract["interrupted_batch_5_same_thread"]
        ),
        "verified_provenance": deepcopy(v1_contract["verified_provenance"]),
        "selection_inputs": deepcopy(v1_contract["selection_inputs"]),
        "privacy": "paths_hashes_sizes_statuses_and_failure_classes_no_prompt_output_or_transcript_text",
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    write_immutable_json(output, payload)
    return verify_v3_reuse_contract(output)


def verify_v3_reuse_contract(path: Path) -> dict[str, Any]:
    contract_path = path.expanduser().resolve()
    payload = _load_json(contract_path, purpose="pipeline-v3 reuse contract")
    if payload.get("schema_version") != PIPELINE_V3_REUSE_CONTRACT_VERSION:
        raise ReuseContractError("unsupported pipeline-v3 reuse contract")
    v1_root = Path(str(payload.get("source_pipeline_v1_root") or "")).resolve()
    v2_root = Path(str(payload.get("source_pipeline_v2_root") or "")).resolve()
    target_root = Path(str(payload.get("target_pipeline_root") or "")).resolve()
    if (
        target_root in {v1_root, v2_root}
        or _inside(target_root, v1_root)
        or _inside(target_root, v2_root)
        or _inside(contract_path, v1_root)
        or _inside(contract_path, v2_root)
    ):
        raise ReuseContractError("pipeline-v3 reuse contract is not version isolated")
    expected_policy = {
        "pipeline_v1_replay_allowed": False,
        "pipeline_v2_replay_allowed": False,
        "extraction_model_calls_allowed": False,
        "failed_v1_ab_retry_allowed": False,
        "failed_v2_shard_retry_allowed": False,
        "batch_5_same_thread_retry_allowed": False,
        "source_artifact_mutation_allowed": False,
        "reuse_is_hash_bound": True,
        "production_mutation_allowed": False,
    }
    if payload.get("policy") != expected_policy:
        raise ReuseContractError("pipeline-v3 replay policy drifted")
    incident = payload.get("v2_failed_sharded_calibration_incident") or {}
    if (
        incident.get("classification") != "infrastructure_or_judge_attempt_failed"
        or incident.get("quality_measured") is not False
        or incident.get("cost_measured") is not False
        or incident.get("failed_shard_id") != "calibration-shard-000"
        or incident.get("failed_variant") != "ab"
        or incident.get("usage_status") != "unknown"
        or incident.get("ba_status") != "not_started"
        or incident.get("later_shards_started") != 0
        or incident.get("retry_allowed") is not False
    ):
        raise ReuseContractError("pipeline-v2 incident classification drifted")
    _verify_record(payload.get("v1_reuse_contract") or {}, root=v2_root)
    verify_v2_reuse_contract(Path(payload["v1_reuse_contract"]["path"]))
    for record in (payload.get("v2_artifacts") or {}).values():
        _verify_record(record)
    absent = payload.get("v2_must_remain_absent")
    if not isinstance(absent, list) or not absent:
        raise ReuseContractError("pipeline-v2 absence contract is missing")
    for record in absent:
        _verify_absence(record, v1_root=v2_root)
    matrix_root = Path(str(payload.get("source_matrix_root") or "")).resolve()
    arms = payload.get("clean_arms")
    if not isinstance(arms, list) or len(arms) != EXPECTED_CLEAN_ARM_COUNT:
        raise ReuseContractError("pipeline-v3 clean arm contract changed")
    for arm in arms:
        _verify_record(arm.get("report") or {}, root=matrix_root)
    _verify_record(payload.get("interrupted_batch_5_same_thread") or {}, root=matrix_root)
    for record in (payload.get("verified_provenance") or {}).values():
        _verify_record(record, root=v1_root)
    for record in (payload.get("selection_inputs") or {}).values():
        _verify_record(record)
    return payload


def _schema_has_forbidden_structured_output_keyword(value: Any) -> bool:
    if isinstance(value, dict):
        if "$schema" in value or "uniqueItems" in value:
            return True
        return any(
            _schema_has_forbidden_structured_output_keyword(item)
            for item in value.values()
        )
    if isinstance(value, list):
        return any(_schema_has_forbidden_structured_output_keyword(item) for item in value)
    return False


def _sum_complete_sidecar_usage(sidecars: Sequence[Mapping[str, Any]]) -> dict[str, int]:
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
            or sidecar.get("usage_status") not in {"measured", "complete"}
            or not isinstance(usage, dict)
        ):
            raise ReuseContractError("pipeline-v3 completed usage sidecar is incomplete")
        for field in fields:
            value = usage.get(field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ReuseContractError("pipeline-v3 completed usage is malformed")
            total[field] += value
    if (
        total["cached_input_tokens"] > total["input_tokens"]
        or total["reasoning_output_tokens"] > total["output_tokens"]
        or total["total_tokens"] != total["input_tokens"] + total["output_tokens"]
    ):
        raise ReuseContractError("pipeline-v3 completed usage accounting is inconsistent")
    return total


def build_v4_reuse_contract(
    *,
    repo_root: Path,
    output_path: Path,
    v3_root: Optional[Path] = None,
    v3_contract_path: Optional[Path] = None,
) -> dict[str, Any]:
    """Freeze all predecessor ITT evidence before a full fresh v4 calibration."""

    repo = repo_root.expanduser().resolve()
    v3 = (
        v3_root or repo / "work/app-server-development-v2/unattended-pipeline-v3"
    ).resolve()
    prior_contract_path = (
        v3_contract_path or v3 / "reuse-contract-v2.json"
    ).expanduser().resolve()
    output = output_path.expanduser().resolve()
    if output.exists():
        return verify_v4_reuse_contract(output)
    prior_contract = verify_v3_reuse_contract(prior_contract_path)
    predecessor_roots = {
        "pipeline_v1": Path(prior_contract["source_pipeline_v1_root"]).resolve(),
        "pipeline_v2": Path(prior_contract["source_pipeline_v2_root"]).resolve(),
        "pipeline_v3": v3,
    }
    if any(output == root or _inside(output, root) for root in predecessor_roots.values()):
        raise ReuseContractError("pipeline-v4 contract cannot overlap an immutable predecessor")

    selection_root = v3 / "development-selection-sharded-v2"
    calibration_root = selection_root / "calibration"
    terminal_path = v3 / "pipeline-terminal.json"
    state_path = v3 / "state.json"
    marker_path = v3 / "phase-markers/02_sharded_development_selection.json"
    selection_path = selection_root / "selection-result.json"
    selection_spec_path = selection_root / "selection-spec.json"
    calibration_path = calibration_root / "report.json"
    shard_plan_path = calibration_root / "shard-plan.json"
    failed_shard_root = calibration_root / "shards/calibration-shard-002"
    failure_path = failed_shard_root / "failure.json"
    failed_sidecar_path = failed_shard_root / "judge/sidecars/ba.json"
    failed_capacity_path = failed_shard_root / "judge/sidecars/ba.capacity.json"

    terminal = _load_json(terminal_path, purpose="pipeline-v3 terminal report")
    state = _load_json(state_path, purpose="pipeline-v3 state")
    selection = _load_json(selection_path, purpose="pipeline-v3 selection result")
    calibration = _load_json(calibration_path, purpose="pipeline-v3 calibration report")
    shard_plan = _load_json(shard_plan_path, purpose="pipeline-v3 shard plan")
    failure = _load_json(failure_path, purpose="pipeline-v3 failed shard")
    failed_sidecar = _load_json(failed_sidecar_path, purpose="pipeline-v3 failed BA sidecar")
    failed_capacity = _load_json(
        failed_capacity_path, purpose="pipeline-v3 failed BA capacity checkpoint"
    )
    if (
        terminal.get("schema_version")
        != "pif_app_server_sharded_selection_pipeline_terminal_v3"
        or terminal.get("status") != "blocked"
        or terminal.get("terminal_phase") != "02_sharded_development_selection"
        or terminal.get("reason_code") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("production_mutation_performed") is not False
        or terminal.get("production_promotion_performed") is not False
        or state.get("status") != "blocked"
        or state.get("reason_code") != "infrastructure_or_judge_attempt_failed"
        or selection.get("selection_status") != "blocked"
        or selection.get("terminal_classification")
        != "infrastructure_or_judge_attempt_failed"
        or selection.get("winner_frozen") is not False
        or selection.get("holdout_preparation_authorized") is not False
        or selection.get("holdout_model_calls_authorized") is not False
    ):
        raise ReuseContractError("pipeline-v3 terminal classification drifted")
    if (
        calibration.get("schema_version")
        != "pif_app_server_sharded_judge_calibration_v2"
        or calibration.get("state") != "blocked"
        or calibration.get("calibrated") is not False
        or calibration.get("fail_closed_reason")
        != "infrastructure_or_judge_attempt_failed"
        or calibration.get("failed_shard_id") != "calibration-shard-002"
        or calibration.get("failed_variant") != "ba"
        or calibration.get("completed_shard_count") != 2
        or calibration.get("aggregate_authorized") is not False
        or calibration.get("accounting_complete") is not False
        or calibration.get("usage_status") != "unknown"
        or calibration.get("usage") is not None
        or calibration.get("retry_count") != 0
        or failure.get("schema_version")
        != "pif_app_server_calibration_shard_failure_v2"
        or failure.get("failed_variant") != "ba"
        or failure.get("retry_allowed") is not False
        or failure.get("aggregate_authorized") is not False
        or failure.get("usage_status") != "unknown"
        or failure.get("usage") is not None
    ):
        raise ReuseContractError("pipeline-v3 calibration failure contract drifted")
    turn_error = failed_sidecar.get("turn_error") or {}
    if (
        failed_sidecar.get("state") != "failed"
        or failed_sidecar.get("status") != "failed"
        or failed_sidecar.get("error_class") != "turn_failed"
        or failed_sidecar.get("auth_type") != "chatgpt"
        or failed_sidecar.get("usage_complete") is not False
        or failed_sidecar.get("usage_status") != "unknown"
        or failed_sidecar.get("usage") is not None
        or failed_sidecar.get("output_sha256") is not None
        or failed_sidecar.get("recovery_reran_model") is not False
        or turn_error.get("codex_error_info") != "serverOverloaded"
        or failed_capacity.get("schema_version")
        != "pif_app_server_capacity_checkpoint_v1"
        or failed_capacity.get("managed_chatgpt_auth_verified") is not True
        or failed_capacity.get("maximum_primary_used_percent") != 20
        or failed_capacity.get("primary_used_percent", 101) > 20
        or failed_capacity.get("rate_limit_reached_type") is not None
        or failed_capacity.get("cleared_for_semantic_turn") is not True
        or failed_capacity.get("turn_started") is not False
        or failed_capacity.get("retry_checkpoint_reuse_allowed") is not False
    ):
        raise ReuseContractError("pipeline-v3 overload telemetry drifted")
    if (
        shard_plan.get("schema_version") != "pif_app_server_calibration_shard_plan_v2"
        or shard_plan.get("case_count") != 66
        or shard_plan.get("shard_count") != 9
        or shard_plan.get("ab_ba_membership_and_order_identical") is not True
        or shard_plan.get("zero_retry_policy")
        != "zero_retries_immutable_terminal_attempts"
    ):
        raise ReuseContractError("pipeline-v3 shard plan drifted")

    completed_attempts = []
    completed_sidecar_payloads = []
    attempted_members = (
        (0, "ab"),
        (0, "ba"),
        (1, "ab"),
        (1, "ba"),
        (2, "ab"),
    )
    for shard_index, orientation in attempted_members:
        judge_root = calibration_root / (
            "shards/calibration-shard-%03d/judge" % shard_index
        )
        sidecar_path = judge_root / "sidecars" / (orientation + ".json")
        capacity_path = judge_root / "sidecars" / (orientation + ".capacity.json")
        output_path_for_attempt = judge_root / ("output-%s.private.json" % orientation)
        sidecar = _load_json(sidecar_path, purpose="completed pipeline-v3 sidecar")
        capacity = _load_json(capacity_path, purpose="completed pipeline-v3 capacity")
        if (
            sidecar.get("recovery_reran_model") is not False
            or not output_path_for_attempt.is_file()
            or capacity.get("managed_chatgpt_auth_verified") is not True
            or capacity.get("maximum_primary_used_percent") != 20
            or capacity.get("primary_used_percent", 101) > 20
            or capacity.get("cleared_for_semantic_turn") is not True
            or capacity.get("retry_checkpoint_reuse_allowed") is not False
        ):
            raise ReuseContractError("pipeline-v3 completed attempt telemetry drifted")
        completed_sidecar_payloads.append(sidecar)
        completed_attempts.append(
            {
                "shard_index": shard_index,
                "orientation": orientation,
                "sidecar": _file_record(sidecar_path, root=v3),
                "capacity_checkpoint": _file_record(capacity_path, root=v3),
                "output": _file_record(output_path_for_attempt, root=v3),
            }
        )
    measured_partial_usage = _sum_complete_sidecar_usage(completed_sidecar_payloads)

    request_envelopes = []
    for shard_index in range(9):
        shard_root = calibration_root / (
            "shards/calibration-shard-%03d" % shard_index
        )
        request = {
            "shard_index": shard_index,
            "shard_spec": _file_record(shard_root / "shard-spec.json", root=v3),
            "pool": _file_record(shard_root / "pool.private.json", root=v3),
            "mapping": _file_record(shard_root / "private-mapping.json", root=v3),
            "expected": _file_record(shard_root / "expected.private.json", root=v3),
            "orientations": {},
        }
        for orientation in ("ab", "ba"):
            prompt_path = shard_root / "judge" / ("prompt-%s.private.md" % orientation)
            schema_path = shard_root / "judge" / ("schema-%s.json" % orientation)
            schema = _load_json(schema_path, purpose="pipeline-v3 structured output schema")
            if _schema_has_forbidden_structured_output_keyword(schema):
                raise ReuseContractError("pipeline-v3 corrected output schema regressed")
            request["orientations"][orientation] = {
                "prompt": _file_record(prompt_path, root=v3),
                "output_schema": _file_record(schema_path, root=v3),
                "structured_outputs_compatible": True,
            }
        request_envelopes.append(request)

    v3_absent = [
        _absence_record(failed_shard_root / "judge/output-ba.private.json"),
        _absence_record(failed_shard_root / "judge/report.json"),
        _absence_record(failed_shard_root / "judge/consensus.private.json"),
        _absence_record(calibration_root / "merged-output-ab.private.json"),
        _absence_record(calibration_root / "merged-output-ba.private.json"),
        _absence_record(selection_root / "full-judge/report.json"),
        _absence_record(v3 / "phase-markers/03_holdout_freeze.json"),
        _absence_record(v3 / "holdout-v1/frozen-holdout.json"),
        _absence_record(v3 / "holdout-execution-v1/pipeline-execution-terminal.json"),
        _absence_record(v3 / "holdout-judge-v1/gate-report.json"),
        _absence_record(v3 / "prospective-epoch-v1/terminal.json"),
    ]
    for shard_index in range(3, 9):
        judge_root = calibration_root / (
            "shards/calibration-shard-%03d/judge" % shard_index
        )
        for relative in (
            "sidecars/ab.capacity.json",
            "sidecars/ab.json",
            "output-ab.private.json",
            "sidecars/ba.capacity.json",
            "sidecars/ba.json",
            "output-ba.private.json",
            "report.json",
            "consensus.private.json",
        ):
            v3_absent.append(_absence_record(judge_root / relative))

    launch_receipt_path = (
        repo
        / "work/app-server-development-v2/unattended-control-v4/launch-receipt-v4.json"
    )
    runtime_lock_path = repo / "work/app-server-development-v2/unattended-runtime-lock-v4.json"
    launch_receipt = _load_json(launch_receipt_path, purpose="pipeline-v3 launch receipt")
    progress = launch_receipt.get("calibration_progress") or {}
    if (
        launch_receipt.get("schema_version")
        != "pif_app_server_unattended_launch_receipt_v4"
        or launch_receipt.get("pipeline_v3_retry_allowed") is not False
        or progress.get("completed_measured_turns")
        != EXPECTED_V3_COMPLETED_MEASURED_TURNS
        or progress.get("failed_unknown_usage_turns")
        != EXPECTED_V3_FAILED_UNKNOWN_USAGE_TURNS
        or progress.get("aggregate_authorized") is not False
        or progress.get("measured_partial_usage") != measured_partial_usage
        or progress.get("whole_version_usage") is not None
        or progress.get("whole_version_usage_status") != "unknown"
    ):
        raise ReuseContractError("pipeline-v3 launch/usage receipt drifted")

    predecessor_terminals = {
        "pipeline_v1": _file_record(
            predecessor_roots["pipeline_v1"] / "pipeline-terminal.json",
            root=predecessor_roots["pipeline_v1"],
        ),
        "pipeline_v2": _file_record(
            predecessor_roots["pipeline_v2"] / "pipeline-terminal.json",
            root=predecessor_roots["pipeline_v2"],
        ),
        "pipeline_v3": _file_record(terminal_path, root=v3),
    }
    v1_sidecar = prior_contract["v1_reuse_contract"]
    v1_contract = verify_v2_reuse_contract(Path(v1_sidecar["path"]))
    predecessor_usage_sidecars = {
        "pipeline_v1_failed_ab": deepcopy(v1_contract["v1_artifacts"]["failed_ab_sidecar"]),
        "pipeline_v2_failed_ab": deepcopy(
            prior_contract["v2_artifacts"]["failed_ab_sidecar"]
        ),
        "pipeline_v3_failed_ba": _file_record(failed_sidecar_path, root=v3),
    }
    payload = {
        "schema_version": PIPELINE_V4_REUSE_CONTRACT_VERSION,
        "created_at": now_iso(),
        "source_pipeline_v1_root": str(predecessor_roots["pipeline_v1"]),
        "source_pipeline_v2_root": str(predecessor_roots["pipeline_v2"]),
        "source_pipeline_v3_root": str(v3),
        "target_pipeline_root": str(output.parent),
        "policy": {
            "pipeline_v1_replay_allowed": False,
            "pipeline_v2_replay_allowed": False,
            "pipeline_v3_replay_allowed": False,
            "pipeline_v3_partial_calibration_scoring_allowed": False,
            "full_fresh_v4_calibration_required": True,
            "extraction_model_calls_allowed": False,
            "failed_v1_ab_retry_allowed": False,
            "failed_v2_shard_retry_allowed": False,
            "failed_v3_shard_retry_allowed": False,
            "batch_5_same_thread_retry_allowed": False,
            "source_artifact_mutation_allowed": False,
            "reuse_is_hash_bound": True,
            "production_mutation_allowed": False,
            "one_attempt_per_ab_or_ba_turn_within_version": True,
        },
        "prior_reuse_contract": _file_record(prior_contract_path, root=v3),
        "predecessor_pipeline_terminals": predecessor_terminals,
        "predecessor_usage_sidecars": predecessor_usage_sidecars,
        "v3_failed_sharded_calibration_incident": {
            "classification": "infrastructure_or_judge_attempt_failed",
            "failure_origin": "external_provider",
            "provider_error_code": "serverOverloaded",
            "quality_measured": False,
            "cost_measured": False,
            "failed_shard_id": "calibration-shard-002",
            "failed_variant": "ba",
            "finished_at": failed_sidecar["finished_at"],
            "prompt_bytes": failed_sidecar["prompt_bytes"],
            "output_schema_bytes": failed_sidecar["output_schema_bytes"],
            "wall_elapsed_seconds": failed_sidecar["wall_elapsed_seconds"],
            "completed_measured_turns": EXPECTED_V3_COMPLETED_MEASURED_TURNS,
            "failed_unknown_usage_turns": EXPECTED_V3_FAILED_UNKNOWN_USAGE_TURNS,
            "measured_partial_usage": measured_partial_usage,
            "whole_version_usage": None,
            "usage_status": "unknown",
            "aggregate_authorized": False,
            "retry_allowed": False,
            "later_shards_started": 0,
        },
        "v3_artifacts": {
            "pipeline_state": _file_record(state_path, root=v3),
            "development_selection_marker": _file_record(marker_path, root=v3),
            "development_selection_result": _file_record(selection_path, root=v3),
            "development_selection_spec": _file_record(selection_spec_path, root=v3),
            "calibration_report": _file_record(calibration_path, root=v3),
            "shard_plan": _file_record(shard_plan_path, root=v3),
            "failed_shard": _file_record(failure_path, root=v3),
            "failed_ba_sidecar": _file_record(failed_sidecar_path, root=v3),
            "failed_ba_capacity_checkpoint": _file_record(failed_capacity_path, root=v3),
            "launch_receipt": _file_record(launch_receipt_path, root=repo),
            "runtime_lock": _file_record(runtime_lock_path, root=repo),
        },
        "v3_completed_attempts": completed_attempts,
        "v3_request_envelopes": request_envelopes,
        "v3_must_remain_absent": v3_absent,
        "v4_calibration_contract": {
            "fixture_case_count": 66,
            "case_count_per_shard": EXPECTED_V4_CALIBRATION_SHARD_SIZE,
            "required_shard_count": EXPECTED_V4_CALIBRATION_SHARD_COUNT,
            "required_ab_ba_turn_count": EXPECTED_V4_CALIBRATION_SHARD_COUNT * 2,
            "ab_ba_membership_and_order_identical": True,
            "aggregate_requires_every_required_turn": True,
            "zero_retry_policy": "zero_retries_immutable_terminal_attempts",
            "corrected_structured_output_schema_required": True,
        },
        "source_matrix_root": prior_contract["source_matrix_root"],
        "clean_arms": deepcopy(prior_contract["clean_arms"]),
        "interrupted_batch_5_same_thread": deepcopy(
            prior_contract["interrupted_batch_5_same_thread"]
        ),
        "verified_provenance": deepcopy(prior_contract["verified_provenance"]),
        "selection_inputs": deepcopy(prior_contract["selection_inputs"]),
        "privacy": "paths_hashes_sizes_statuses_usage_counts_and_failure_codes_no_prompt_output_or_transcript_text",
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    write_immutable_json(output, payload)
    return verify_v4_reuse_contract(output)


def verify_v4_reuse_contract(path: Path) -> dict[str, Any]:
    contract_path = path.expanduser().resolve()
    payload = _load_json(contract_path, purpose="pipeline-v4 reuse contract")
    if payload.get("schema_version") != PIPELINE_V4_REUSE_CONTRACT_VERSION:
        raise ReuseContractError("unsupported pipeline-v4 reuse contract")
    roots = {
        key: Path(str(payload.get(key) or "")).resolve()
        for key in (
            "source_pipeline_v1_root",
            "source_pipeline_v2_root",
            "source_pipeline_v3_root",
        )
    }
    target_root = Path(str(payload.get("target_pipeline_root") or "")).resolve()
    if any(
        target_root == root
        or _inside(target_root, root)
        or _inside(contract_path, root)
        for root in roots.values()
    ):
        raise ReuseContractError("pipeline-v4 reuse contract is not version isolated")
    expected_policy = {
        "pipeline_v1_replay_allowed": False,
        "pipeline_v2_replay_allowed": False,
        "pipeline_v3_replay_allowed": False,
        "pipeline_v3_partial_calibration_scoring_allowed": False,
        "full_fresh_v4_calibration_required": True,
        "extraction_model_calls_allowed": False,
        "failed_v1_ab_retry_allowed": False,
        "failed_v2_shard_retry_allowed": False,
        "failed_v3_shard_retry_allowed": False,
        "batch_5_same_thread_retry_allowed": False,
        "source_artifact_mutation_allowed": False,
        "reuse_is_hash_bound": True,
        "production_mutation_allowed": False,
        "one_attempt_per_ab_or_ba_turn_within_version": True,
    }
    if payload.get("policy") != expected_policy:
        raise ReuseContractError("pipeline-v4 replay policy drifted")
    prior_record = payload.get("prior_reuse_contract") or {}
    _verify_record(prior_record, root=roots["source_pipeline_v3_root"])
    verify_v3_reuse_contract(Path(prior_record["path"]))
    for name, record in (payload.get("predecessor_pipeline_terminals") or {}).items():
        root = roots.get("source_" + name + "_root")
        _verify_record(record, root=root)
    if set(payload.get("predecessor_pipeline_terminals") or {}) != {
        "pipeline_v1",
        "pipeline_v2",
        "pipeline_v3",
    }:
        raise ReuseContractError("pipeline-v4 predecessor terminal coverage drifted")
    for record in (payload.get("predecessor_usage_sidecars") or {}).values():
        _verify_record(record)
    incident = payload.get("v3_failed_sharded_calibration_incident") or {}
    if (
        incident.get("classification") != "infrastructure_or_judge_attempt_failed"
        or incident.get("failure_origin") != "external_provider"
        or incident.get("provider_error_code") != "serverOverloaded"
        or incident.get("quality_measured") is not False
        or incident.get("cost_measured") is not False
        or incident.get("failed_shard_id") != "calibration-shard-002"
        or incident.get("failed_variant") != "ba"
        or incident.get("completed_measured_turns")
        != EXPECTED_V3_COMPLETED_MEASURED_TURNS
        or incident.get("failed_unknown_usage_turns")
        != EXPECTED_V3_FAILED_UNKNOWN_USAGE_TURNS
        or incident.get("whole_version_usage") is not None
        or incident.get("usage_status") != "unknown"
        or incident.get("aggregate_authorized") is not False
        or incident.get("retry_allowed") is not False
        or incident.get("later_shards_started") != 0
    ):
        raise ReuseContractError("pipeline-v3 overload incident classification drifted")
    for record in (payload.get("v3_artifacts") or {}).values():
        _verify_record(record)
    attempts = payload.get("v3_completed_attempts")
    if not isinstance(attempts, list) or len(attempts) != EXPECTED_V3_COMPLETED_MEASURED_TURNS:
        raise ReuseContractError("pipeline-v3 completed attempt coverage drifted")
    for attempt in attempts:
        for key in ("sidecar", "capacity_checkpoint", "output"):
            _verify_record(attempt.get(key) or {}, root=roots["source_pipeline_v3_root"])
    envelopes = payload.get("v3_request_envelopes")
    if not isinstance(envelopes, list) or len(envelopes) != 9:
        raise ReuseContractError("pipeline-v3 request envelope coverage drifted")
    for envelope in envelopes:
        _verify_record(envelope.get("shard_spec") or {}, root=roots["source_pipeline_v3_root"])
        _verify_record(envelope.get("pool") or {}, root=roots["source_pipeline_v3_root"])
        _verify_record(envelope.get("mapping") or {}, root=roots["source_pipeline_v3_root"])
        _verify_record(envelope.get("expected") or {}, root=roots["source_pipeline_v3_root"])
        orientations = envelope.get("orientations") or {}
        if set(orientations) != {"ab", "ba"}:
            raise ReuseContractError("pipeline-v3 request orientation coverage drifted")
        for request in orientations.values():
            if request.get("structured_outputs_compatible") is not True:
                raise ReuseContractError("pipeline-v3 corrected schema contract drifted")
            _verify_record(request.get("prompt") or {}, root=roots["source_pipeline_v3_root"])
            schema_record = request.get("output_schema") or {}
            _verify_record(schema_record, root=roots["source_pipeline_v3_root"])
            schema = _load_json(Path(schema_record["path"]), purpose="bound output schema")
            if _schema_has_forbidden_structured_output_keyword(schema):
                raise ReuseContractError("pipeline-v3 bound output schema is unsupported")
    absent = payload.get("v3_must_remain_absent")
    if not isinstance(absent, list) or not absent:
        raise ReuseContractError("pipeline-v3 absence contract is missing")
    for record in absent:
        _verify_absence(record, v1_root=roots["source_pipeline_v3_root"])
    v4 = payload.get("v4_calibration_contract") or {}
    if (
        v4.get("fixture_case_count") != 66
        or v4.get("case_count_per_shard") != EXPECTED_V4_CALIBRATION_SHARD_SIZE
        or v4.get("required_shard_count") != EXPECTED_V4_CALIBRATION_SHARD_COUNT
        or v4.get("required_ab_ba_turn_count") != 22
        or v4.get("ab_ba_membership_and_order_identical") is not True
        or v4.get("aggregate_requires_every_required_turn") is not True
        or v4.get("zero_retry_policy")
        != "zero_retries_immutable_terminal_attempts"
        or v4.get("corrected_structured_output_schema_required") is not True
    ):
        raise ReuseContractError("pipeline-v4 calibration contract drifted")
    matrix_root = Path(str(payload.get("source_matrix_root") or "")).resolve()
    arms = payload.get("clean_arms")
    if not isinstance(arms, list) or len(arms) != EXPECTED_CLEAN_ARM_COUNT:
        raise ReuseContractError("pipeline-v4 clean-arm reuse drifted")
    for arm in arms:
        _verify_record(arm.get("report") or {}, root=matrix_root)
    _verify_record(payload.get("interrupted_batch_5_same_thread") or {}, root=matrix_root)
    for record in (payload.get("verified_provenance") or {}).values():
        _verify_record(record, root=roots["source_pipeline_v1_root"])
    for record in (payload.get("selection_inputs") or {}).values():
        _verify_record(record)
    return payload


def verify_selection_reuse_contract(path: Path) -> dict[str, Any]:
    payload = _load_json(path.expanduser().resolve(), purpose="selection reuse contract")
    version = payload.get("schema_version")
    if version == REUSE_CONTRACT_VERSION:
        return verify_v2_reuse_contract(path)
    if version == PIPELINE_V3_REUSE_CONTRACT_VERSION:
        return verify_v3_reuse_contract(path)
    if version == PIPELINE_V4_REUSE_CONTRACT_VERSION:
        return verify_v4_reuse_contract(path)
    raise ReuseContractError("selection reuse contract version is unsupported")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Freeze or verify pipeline-v2 reuse evidence")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output", required=True)
    parser.add_argument("--v1-root")
    parser.add_argument("--v2-root")
    parser.add_argument("--matrix-root")
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--v3-root")
    parser.add_argument(
        "--contract-version", choices=("v2", "v3", "v4"), default="v2"
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    output = Path(args.output)
    if args.verify_only:
        payload = verify_selection_reuse_contract(output)
    elif args.contract_version == "v4":
        payload = build_v4_reuse_contract(
            repo_root=Path(args.repo_root),
            output_path=output,
            v3_root=Path(args.v3_root) if args.v3_root else None,
        )
    elif args.contract_version == "v3":
        payload = build_v3_reuse_contract(
            repo_root=Path(args.repo_root),
            output_path=output,
            v2_root=Path(args.v2_root) if args.v2_root else None,
        )
    else:
        payload = build_v2_reuse_contract(
            repo_root=Path(args.repo_root),
            output_path=output,
            v1_root=Path(args.v1_root) if args.v1_root else None,
            matrix_root=Path(args.matrix_root) if args.matrix_root else None,
        )
    print(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
