from __future__ import annotations

"""Run fresh judge calibration against the adopted-turn v21 reference."""

import argparse
import asyncio
import hashlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_runner as calibration_runner
from . import app_server_judge_v5_diagnostic as diagnostic
from . import app_server_judge_v5_fresh_calibration as fresh
from .app_server_capacity_reserve import (
    ReserveCapacityGatedCodexAppServerClient,
    load_reserve_capacity_policy,
)
from .app_server_capacity_policy_v21 import audit_v20_checkpoint_validator_failure
from .app_server_judge_v5_reference_adjudication_v21 import (
    ADOPTION_RECEIPT_VERSION,
    ADOPTED_TURN_NAME,
    build_reserve_capacity_checkpoint_validator,
)


REFERENCE_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/"
    "fixture-reference-adjudication-luna-v6-capacity-v21"
).resolve()
REFERENCE_POLICY = Path(
    "work/app-server-development-v2/unattended-control-v21/capacity-policy-v21.json"
).resolve()
DEFAULT_OUTPUT_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/"
    "judge-calibration-v5_4-reference-v2-capacity-v22"
).resolve()
CALIBRATION_BINDING_VERSION = "pif_app_server_fresh_calibration_binding_v22"


class FreshCalibrationV22Error(fresh.FreshCalibrationError):
    """The v21 reference or v22 calibration binding is unsafe."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load(path: Path, purpose: str) -> dict[str, Any]:
    try:
        value = json.loads(path.expanduser().resolve().read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FreshCalibrationV22Error(f"{purpose} is missing or invalid") from exc
    if not isinstance(value, dict):
        raise FreshCalibrationV22Error(f"{purpose} is not an object")
    return value


def _record(path: Path) -> dict[str, Any]:
    target = path.expanduser().resolve()
    if not target.is_file():
        raise FreshCalibrationV22Error("required v22 lineage artifact is missing")
    return {
        "path": str(target),
        "sha256": _sha256_file(target),
        "size_bytes": target.stat().st_size,
    }


def _verify_record(
    record: Any, *, purpose: str, expected_path: Optional[Path] = None
) -> Path:
    if not isinstance(record, Mapping):
        raise FreshCalibrationV22Error(f"{purpose} record is missing")
    path = Path(str(record.get("path") or "")).expanduser().resolve()
    if expected_path is not None and path != expected_path.expanduser().resolve():
        raise FreshCalibrationV22Error(f"{purpose} path drifted")
    if (
        not path.is_file()
        or record.get("sha256") != _sha256_file(path)
        or record.get("size_bytes") != path.stat().st_size
    ):
        raise FreshCalibrationV22Error(f"{purpose} record drifted")
    return path


def _verify_adopted_turn(root: Path, terminal: Mapping[str, Any]) -> dict[str, int]:
    records = terminal.get("completed_checkpoint_adoptions")
    adoption_path = root / "completed-turn-adoption.json"
    if not isinstance(records, list) or len(records) != 1:
        raise FreshCalibrationV22Error("v21 adoption coverage drifted")
    _verify_record(
        records[0], purpose="v21 adoption", expected_path=adoption_path
    )
    adoption = _load(adoption_path, "v21 adoption receipt")
    if (
        adoption.get("schema_version") != ADOPTION_RECEIPT_VERSION
        or adoption.get("turn_name") != ADOPTED_TURN_NAME
        or adoption.get("source_phase_id") != "fixture_reference_adjudication_v20"
        or adoption.get("request_identity_verified") is not True
        or adoption.get("semantic_output_validated") is not True
        or adoption.get("capacity_checkpoint_validated") is not True
        or adoption.get("semantic_call_performed_by_v21") is not False
        or adoption.get("adoption_is_not_retry") is not True
        or adoption.get("production_mutated") is not False
    ):
        raise FreshCalibrationV22Error("v21 adoption contract drifted")
    source_records = adoption.get("source_artifacts")
    request_records = adoption.get("v21_request_artifacts")
    if not isinstance(source_records, Mapping) or not isinstance(
        request_records, Mapping
    ):
        raise FreshCalibrationV22Error("v21 adoption records are missing")
    source_paths = {}
    for kind, record in source_records.items():
        source_paths[kind] = _verify_record(
            record, purpose=f"adopted source {kind}"
        )
    turn_root = root / "turns" / ADOPTED_TURN_NAME.replace("_", "-")
    for kind in ("input", "prompt", "schema"):
        current = _verify_record(
            request_records.get(kind),
            purpose=f"v21 adopted request {kind}",
            expected_path=turn_root / {
                "input": "input.private.json",
                "prompt": "prompt.private.md",
                "schema": "schema.json",
            }[kind],
        )
        if (
            current.stat().st_size != source_paths[kind].stat().st_size
            or _sha256_file(current) != _sha256_file(source_paths[kind])
        ):
            raise FreshCalibrationV22Error("adopted request identity drifted")
    forensic = audit_v20_checkpoint_validator_failure()
    usage = adoption.get("usage")
    if usage != forensic["usage"]:
        raise FreshCalibrationV22Error("adopted usage drifted")
    diagnostic._validate_usage(
        _load(source_paths["sidecar"], "adopted source sidecar")
    )
    return dict(usage)


def _validate_v21_attempts(
    root: Path, terminal: Mapping[str, Any]
) -> dict[str, int]:
    attempts = terminal.get("attempts")
    if not isinstance(attempts, list) or len(attempts) != 12:
        raise FreshCalibrationV22Error("v21 reference attempt coverage drifted")
    expected_names = {
        *(f"reference_pointwise_shard_{index:02d}" for index in range(9)),
        *(f"reference_alignment_shard_{index:02d}" for index in range(3)),
    }
    validator = build_reserve_capacity_checkpoint_validator(REFERENCE_POLICY)
    usage = _verify_adopted_turn(root, terminal)
    names = []
    for attempt in attempts:
        if not isinstance(attempt, Mapping):
            raise FreshCalibrationV22Error("v21 reference attempt is malformed")
        name = str(attempt.get("turn_name") or "")
        names.append(name)
        if name == ADOPTED_TURN_NAME:
            if any(attempt.get(key) is not None for key in ("capacity", "sidecar", "output")):
                raise FreshCalibrationV22Error("adopted turn was replayed in v21")
            continue
        if (
            attempt.get("state") != "completed"
            or attempt.get("status") != "completed"
            or attempt.get("usage_status") != "measured"
            or attempt.get("error_class") is not None
        ):
            raise FreshCalibrationV22Error("v21 reference attempt is incomplete")
        turn_root = root / "turns" / name.replace("_", "-")
        capacity_path = _verify_record(
            attempt.get("capacity"),
            purpose="v21 capacity",
            expected_path=turn_root / "capacity.json",
        )
        sidecar_path = _verify_record(
            attempt.get("sidecar"),
            purpose="v21 sidecar",
            expected_path=turn_root / "sidecar.json",
        )
        _verify_record(
            attempt.get("output"),
            purpose="v21 output",
            expected_path=turn_root / "output.private.json",
        )
        checkpoint = validator(capacity_path)
        if checkpoint.get("turn_name") != name:
            raise FreshCalibrationV22Error("v21 capacity turn identity drifted")
        sidecar = _load(sidecar_path, "v21 sidecar")
        if (
            sidecar.get("state") != "completed"
            or sidecar.get("status") != "completed"
            or sidecar.get("usage_status") != "measured"
            or sidecar.get("auth_type") != "chatgpt"
            or sidecar.get("transport") != "stdio"
            or sidecar.get("error_class") is not None
            or sidecar.get("recovery_reran_model") is not False
        ):
            raise FreshCalibrationV22Error("v21 sidecar boundary drifted")
        measured = diagnostic._validate_usage(sidecar)
        for field, value in measured.items():
            usage[field] += value
    if set(names) != expected_names or len(names) != len(set(names)):
        raise FreshCalibrationV22Error("v21 reference attempt identities drifted")
    return usage


def load_frozen_fixture_reference_v21(
    reference_root: Path = REFERENCE_ROOT,
) -> dict[str, Any]:
    root = reference_root.expanduser().resolve()
    if root != REFERENCE_ROOT:
        raise FreshCalibrationV22Error("v22 reference root is not the frozen v21 root")
    terminal_path = root / "terminal.json"
    terminal = _load(terminal_path, "v21 fixture-reference terminal")
    if (
        terminal.get("schema_version")
        != fresh.REFERENCE_ADJUDICATION_TERMINAL_VERSION
        or terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "fixture_reference_v2_frozen_fresh_calibration_authorized"
        or terminal.get("reference_frozen") is not True
        or terminal.get("fresh_calibration_authorized") is not True
        or terminal.get("selection_authorized") is not False
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("turn_count") != 12
        or terminal.get("new_semantic_turn_count") != 11
        or terminal.get("production_mutated") is not False
    ):
        raise FreshCalibrationV22Error("v21 reference terminal is not admissible")
    measured_usage = _validate_v21_attempts(root, terminal)
    receipt_path = _verify_record(
        terminal.get("receipt"),
        purpose="v21 reference receipt",
        expected_path=root / "fixture-reference-v2-receipt.json",
    )
    receipt = _load(receipt_path, "v21 reference receipt")
    if (
        receipt.get("schema_version") != fresh.REFERENCE_RECEIPT_VERSION
        or receipt.get("status") != "fixture_reference_v2_frozen"
        or receipt.get("reference_frozen") is not True
        or receipt.get("fresh_calibration_authorized") is not True
        or receipt.get("selection_authorized") is not False
        or receipt.get("semantic_turn_count") != 12
        or receipt.get("new_semantic_turn_count") != 11
        or receipt.get("semantic_retry_count") != 0
        or receipt.get("usage_status") != "complete"
        or receipt.get("candidate_labels_exposed_to_adjudicator") is not False
        or receipt.get("model_identities_exposed_to_adjudicator") is not False
        or receipt.get("majority_voting_used") is not False
        or receipt.get("production_mutation_performed") is not False
        or receipt.get("usage") != measured_usage
        or terminal.get("usage") != measured_usage
        or receipt.get("completed_checkpoint_adoptions")
        != terminal.get("completed_checkpoint_adoptions")
    ):
        raise FreshCalibrationV22Error("v21 reference receipt is not admissible")
    reference_path = _verify_record(
        receipt.get("reference"), purpose="fixture reference"
    )
    unresolved_path = _verify_record(
        receipt.get("unresolved"), purpose="reference unresolved"
    )
    manifest_path = _verify_record(
        receipt.get("disagreement_manifest"), purpose="disagreement manifest"
    )
    change_path = _verify_record(
        receipt.get("change_summary"), purpose="reference change summary"
    )
    if terminal.get("unresolved") != receipt.get("unresolved"):
        raise FreshCalibrationV22Error("v21 terminal/receipt unresolved drifted")
    fresh._validate_unresolved(_load(unresolved_path, "reference unresolved"))
    manifest = _load(manifest_path, "reference disagreement manifest")
    if (
        manifest.get("pointwise_disputed_witness_count") != 102
        or manifest.get("pointwise_disputed_case_count") != 49
        or manifest.get("alignment_disputed_case_count") != 18
        or manifest.get("candidate_labels_exposed_to_adjudicator") is not False
        or manifest.get("model_identities_exposed_to_adjudicator") is not False
        or manifest.get("majority_voting_allowed") is not False
    ):
        raise FreshCalibrationV22Error("v21 disagreement manifest drifted")
    reference = _load(reference_path, "fixture reference")
    pool, mapping, expected = fresh._reference_as_calibration_truth(reference)
    spec_path = root / "reference-adjudication-spec.json"
    spec = _load(spec_path, "v21 reference specification")
    capacity = spec.get("semantic_capacity_policy")
    if (
        spec.get("schema_version") != fresh.REFERENCE_ADJUDICATION_SPEC_VERSION
        or spec.get("total_turn_count") != 12
        or spec.get("new_semantic_turn_count") != 11
        or spec.get("retry_count_per_turn") != 0
        or spec.get("managed_chatgpt_auth_only") is not True
        or spec.get("semantic_turn_maximum_primary_used_percent") is not None
        or not isinstance(capacity, Mapping)
        or capacity.get("phase_id") != "fixture_reference_adjudication_v21"
        or capacity.get("minimum_remaining_reserve_percent") != 20
        or capacity.get("phase_total_token_bound") != 1122000
        or capacity.get("projected_phase_quota_points") != 20
        or spec.get("reference_freeze_requires_zero_abstentions") is not True
        or spec.get("selection_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise FreshCalibrationV22Error("v21 reference specification drifted")
    _verify_record(
        spec.get("completed_checkpoint_adoption"),
        purpose="spec adoption",
        expected_path=root / "completed-turn-adoption.json",
    )
    for request in spec.get("pointwise_requests") or []:
        if not isinstance(request, Mapping):
            raise FreshCalibrationV22Error("v21 pointwise request record is malformed")
        for kind in ("input", "prompt", "schema"):
            _verify_record(request.get(kind), purpose=f"v21 request {kind}")
    return {
        "root": root,
        "pool": pool,
        "mapping": mapping,
        "expected": expected,
        "terminal": terminal,
        "receipt": receipt,
        "records": {
            "terminal": _record(terminal_path),
            "receipt": _record(receipt_path),
            "reference": _record(reference_path),
            "unresolved": _record(unresolved_path),
            "disagreement_manifest": _record(manifest_path),
            "change_summary": _record(change_path),
            "reference_spec": _record(spec_path),
            "completed_turn_adoption": _record(
                root / "completed-turn-adoption.json"
            ),
        },
    }


def _policy_summary(policy: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "phase_id": policy["phase_id"],
        "minimum_remaining_reserve_percent": policy[
            "minimum_remaining_reserve_percent"
        ],
        "maximum_total_tokens_per_turn": policy["maximum_total_tokens_per_turn"],
        "phase_total_token_bound": policy["phase_total_token_bound"],
        "projected_phase_quota_points": policy["projected_phase_quota_points"],
        "reprobe_before_every_semantic_turn": True,
        "retry_count_per_turn": 0,
        "selection_authorized_before_calibration": False,
        "holdout_authorized": False,
    }


async def run_v22_calibration(
    *, policy_path: Path, output_dir: Path, reference_root: Path = REFERENCE_ROOT
) -> dict[str, Any]:
    policy_file = policy_path.expanduser().resolve()
    policy = load_reserve_capacity_policy(policy_file)
    root = output_dir.expanduser().resolve()
    if (
        policy.get("phase_id") != "fresh_judge_v5_4_calibration_v22"
        or Path(str(policy.get("phase_output_root") or "")).resolve() != root
        or Path(policy["semantic_output_root"]).resolve() != root / "fresh-attempt"
    ):
        raise FreshCalibrationV22Error("v22 calibration policy/root binding drifted")

    def client_factory() -> ReserveCapacityGatedCodexAppServerClient:
        return ReserveCapacityGatedCodexAppServerClient(policy_path=policy_file)

    original_loader = fresh.load_frozen_fixture_reference
    original_fresh_writer = fresh._write_immutable_json
    original_runner_writer = calibration_runner._write_immutable_json
    original_validator = diagnostic._validate_capacity_checkpoint
    summary = _policy_summary(policy)
    validator = build_reserve_capacity_checkpoint_validator(policy_file)

    def bind_value(path: Path, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        bound = deepcopy(value)
        if path.name in {"fresh-calibration-spec.json", "calibration-spec.json"}:
            bound["semantic_capacity_policy"] = deepcopy(summary)
            bound["capacity_policy"] = _record(policy_file)
            bound["capacity_adapter_source"] = _record(Path(__file__).resolve())
        elif path.name == "terminal.json":
            bound["semantic_capacity_policy"] = deepcopy(summary)
            bound["capacity_policy"] = _record(policy_file)
        return bound

    def fresh_writer(path: Path, value: Any) -> None:
        original_fresh_writer(path, bind_value(path, value))

    def runner_writer(path: Path, value: Any) -> None:
        original_runner_writer(path, bind_value(path, value))

    fresh.load_frozen_fixture_reference = load_frozen_fixture_reference_v21
    fresh._write_immutable_json = fresh_writer
    calibration_runner._write_immutable_json = runner_writer
    diagnostic._validate_capacity_checkpoint = validator
    try:
        return await fresh.run_fresh_reference_calibration(
            reference_root=reference_root,
            output_dir=root,
            model="gpt-5.6-sol",
            reasoning_effort="high",
            timeout_seconds=1200.0,
            client_factory=client_factory,
        )
    finally:
        diagnostic._validate_capacity_checkpoint = original_validator
        calibration_runner._write_immutable_json = original_runner_writer
        fresh._write_immutable_json = original_fresh_writer
        fresh.load_frozen_fixture_reference = original_loader


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v22 fresh reference calibration")
    parser.add_argument("--policy", required=True)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--reference-root", default=str(REFERENCE_ROOT))
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v22_calibration(
            policy_path=Path(args.policy),
            output_dir=Path(args.output_dir),
            reference_root=Path(args.reference_root),
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "calibration_passed": terminal.get("calibration_passed", False),
                "selection_authorized": terminal.get("selection_authorized", False),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal.get("state") == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
