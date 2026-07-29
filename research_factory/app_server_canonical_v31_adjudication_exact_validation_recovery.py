from __future__ import annotations

"""Zero-call terminal recovery for the completed invalid epoch-29 output."""

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_tagged_metric_token_id_adjudication_runtime as epoch29
from . import app_server_llm_judge as judge
from .util import now_iso


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT / "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
EPOCH29_ROOT = epoch29.DEFAULT_ROOT
DEFAULT_ROOT = (
    PIPELINE_ROOT / "canonical-v31-epoch30-adjudication-exact-validation-recovery-v1"
).resolve()
DIRECTIVE_PATH = (
    PROJECT_ROOT
    / "automation/pif-evaluation-epoch30-adjudication-exact-validation-recovery-v30.json"
).resolve()
PLAN_PATH = (
    PROJECT_ROOT / "automation/pif-evaluation-semantic-plan-v30.json"
).resolve()
SCHEMA_VERSION = "pif_canonical_v31_adjudication_exact_validation_recovery_v30"
LOCK_VERSION = "pif_canonical_v31_adjudication_exact_validation_recovery_lock_v1"
RECEIPT_VERSION = "pif_semantic_plan_step_receipt_v1"
THREAD_ID = epoch29.THREAD_ID
PLAN_EPOCH = 30
STEP_ID = "canonical_v31_epoch30_adjudication_exact_validation_recovery_v30"
EXPECTED_ERRORS = (
    "case_0_support_2_evidence_not_exact",
    "case_0_support_12_evidence_not_exact",
)
EXPECTED_ERRORS_SHA256 = "c508fe0ff1263a578df14d88b563ae86c0abc34168b2aa0561735cbfa6e6fb48"
EXPECTED_USAGE = {
    "input_tokens": 36_377,
    "cached_input_tokens": 1_408,
    "output_tokens": 13_027,
    "reasoning_output_tokens": 8_166,
    "total_tokens": 49_404,
}
EXPECTED_ARTIFACT_HASHES = {
    "runtime_lock": "320db92fbf2adb00aa4c7f5c812d563ea289128b3f1ca87620fd7598c1e0abd8",
    "launch": "c51c6ba92d6916419729fef7a525f870f00fba0d180142202734729807030cfc",
    "capacity": "a84154b3f9a73cad3d97d8f742b719e15c8b06408ce84c74a35e02b843c07bd2",
    "sidecar": "5ba220e29daf08708ac3ecc369c85b8e9b38b35b8de584dd58281ca3e768e778",
    "output": "3cd298aeba02eb01578f550420e405c14cf03ad3bf2f27afda17c9b43482e89d",
}


class Epoch30RecoveryError(RuntimeError):
    """The completed epoch-29 failure cannot be recovered exactly."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve(strict=True)
    return {
        "path": str(resolved),
        "sha256": _sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _verify_record(record: Mapping[str, Any]) -> bool:
    try:
        return _record(Path(str(record["path"]))) == dict(record)
    except (KeyError, OSError, TypeError, ValueError):
        return False


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise Epoch30RecoveryError(f"cannot read {label}") from exc


def _write_immutable_json(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise Epoch30RecoveryError(f"frozen {path.name} drifted")
        return
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(payload, encoding="utf-8")
    temporary.replace(path)


def _artifact_paths() -> dict[str, Path]:
    turn = EPOCH29_ROOT / "turns/epoch29-quality-adjudication"
    return {
        "runtime_lock": EPOCH29_ROOT / "runtime-lock.json",
        "launch": EPOCH29_ROOT / "launch-receipt.json",
        "capacity": turn / "capacity.json",
        "sidecar": turn / "sidecar.json",
        "output": turn / "output.private.json",
    }


def validate_completed_failure() -> dict[str, Any]:
    if (EPOCH29_ROOT / "plan-step-receipt.json").exists() or (
        EPOCH29_ROOT / "terminal.json"
    ).exists():
        raise Epoch30RecoveryError("epoch-29 unexpectedly has a terminal receipt")
    epoch29.verify_runtime_lock(EPOCH29_ROOT / "runtime-lock.json")
    epoch29._validate_launch_receipt(EPOCH29_ROOT)  # noqa: SLF001
    paths = _artifact_paths()
    for role, path in paths.items():
        if _sha256_file(path) != EXPECTED_ARTIFACT_HASHES[role]:
            raise Epoch30RecoveryError(f"epoch-29 {role} drifted")
    variant, base, prompt, schema = epoch29._request_material()  # noqa: SLF001
    try:
        output, sidecar = judge._validate_completed_checkpoint(  # noqa: SLF001
            raw_output_path=paths["output"],
            sidecar_path=paths["sidecar"],
            prompt=prompt,
            schema=schema,
            base_instructions=base,
            model=epoch29.MODEL,
            reasoning_effort=epoch29.EFFORT,
        )
    except (ValueError, judge.JudgeArtifactError) as exc:
        raise Epoch30RecoveryError("epoch-29 completed checkpoint drifted") from exc
    epoch29._validate_partial_sidecar(EPOCH29_ROOT, sidecar)  # noqa: SLF001
    epoch29._validate_capacity_checkpoint(EPOCH29_ROOT)  # noqa: SLF001
    usage = epoch29._valid_usage(sidecar.get("usage"))  # noqa: SLF001
    errors = tuple(judge.validate_judge_output(output, variant))
    if (
        sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("thread_total_usage") != usage
        or usage != EXPECTED_USAGE
        or errors != EXPECTED_ERRORS
        or _sha256_json(list(errors)) != EXPECTED_ERRORS_SHA256
    ):
        raise Epoch30RecoveryError("epoch-29 exact-validation failure drifted")
    return {
        "artifact_records": {role: _record(path) for role, path in paths.items()},
        "thread_id": sidecar["thread_id"],
        "turn_id": sidecar["turn_id"],
        "wall_elapsed_seconds": sidecar.get("wall_elapsed_seconds"),
        "usage": usage,
        "validation_error_count": len(errors),
        "validation_errors_sha256": _sha256_json(list(errors)),
    }


def load_contract() -> dict[str, Any]:
    plan = _load_json(PLAN_PATH, "epoch-30 semantic plan")
    directive = _load_json(DIRECTIVE_PATH, "epoch-30 directive")
    step = plan.get("step") if isinstance(plan, Mapping) else None
    recovery = directive.get("recovery_contract") if isinstance(directive, Mapping) else None
    if (
        set(plan) != {"schema_version", "thread_id", "plan_epoch", "state", "step"}
        or not isinstance(step, Mapping)
        or plan.get("schema_version") != "pif_evaluation_semantic_plan_v1"
        or plan.get("thread_id") != THREAD_ID
        or plan.get("plan_epoch") != PLAN_EPOCH
        or plan.get("state") != "executable"
        or step.get("step_id") != STEP_ID
        or step.get("state") != "executable"
        or step.get("max_model_calls") != 0
        or step.get("max_total_tokens") != 0
        or step.get("accepted_receipt_states") != ["rejected"]
        or step.get("directive_path") != str(DIRECTIVE_PATH)
        or step.get("directive_sha256") != _sha256_file(DIRECTIVE_PATH)
        or step.get("expected_receipt_path")
        != str(DEFAULT_ROOT / "plan-step-receipt.json")
        or directive.get("schema_version")
        != "pif_evaluation_epoch30_adjudication_exact_validation_recovery_directive_v1"
        or directive.get("thread_id") != THREAD_ID
        or directive.get("plan_epoch") != PLAN_EPOCH
        or directive.get("step_id") != STEP_ID
        or directive.get("authority") != "direct_user_instruction"
        or directive.get("authorized_by") != "kolby"
        or directive.get("authorization_statement")
        != "You have my full permission to continue. No need to seek out my approval anymore."
        or directive.get("state") != "authorized_for_zero_call_terminal_recovery"
        or directive.get("expected_receipt_path")
        != str(DEFAULT_ROOT / "plan-step-receipt.json")
        or not isinstance(recovery, Mapping)
        or recovery.get("new_semantic_model_call_cap") != 0
        or recovery.get("predecessor_model_call_count") != 1
        or recovery.get("predecessor_retry_count") != 0
        or recovery.get("predecessor_total_tokens") != EXPECTED_USAGE["total_tokens"]
        or recovery.get("expected_exact_validation_error_count") != len(EXPECTED_ERRORS)
        or recovery.get("expected_exact_validation_errors_sha256")
        != EXPECTED_ERRORS_SHA256
        or recovery.get("semantic_output_repair_allowed") is not False
        or recovery.get("semantic_replay_allowed") is not False
    ):
        raise Epoch30RecoveryError("epoch-30 recovery contract drifted")
    return {"plan": plan, "directive": directive}


def freeze_run(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    output_root = root.expanduser().resolve()
    lock_path = output_root / "runtime-lock.json"
    if lock_path.exists():
        return verify_runtime_lock(lock_path)
    if output_root.exists() and any(output_root.iterdir()):
        raise Epoch30RecoveryError("unfrozen epoch-30 root is not empty")
    load_contract()
    failure = validate_completed_failure()
    records = [
        _record(Path(__file__).resolve()),
        _record(Path(epoch29.__file__).resolve()),
        _record(Path(judge.__file__).resolve()),
        _record(PLAN_PATH),
        _record(DIRECTIVE_PATH),
        *failure["artifact_records"].values(),
    ]
    lock = {
        "schema_version": LOCK_VERSION,
        "thread_id": THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "records": records,
        "records_sha256": _sha256_json(records),
        "new_semantic_model_call_cap": 0,
        "semantic_replay_allowed": False,
        "semantic_output_repair_allowed": False,
        "production_mutated": False,
        "holdout_authorized": False,
        "winner_frozen": False,
    }
    _write_immutable_json(lock_path, lock)
    return verify_runtime_lock(lock_path)


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path.expanduser().resolve(), "epoch-30 runtime lock")
    failure = validate_completed_failure()
    expected_records = [
        _record(Path(__file__).resolve()),
        _record(Path(epoch29.__file__).resolve()),
        _record(Path(judge.__file__).resolve()),
        _record(PLAN_PATH),
        _record(DIRECTIVE_PATH),
        *failure["artifact_records"].values(),
    ]
    if (
        lock.get("schema_version") != LOCK_VERSION
        or lock.get("thread_id") != THREAD_ID
        or lock.get("plan_epoch") != PLAN_EPOCH
        or lock.get("step_id") != STEP_ID
        or lock.get("records") != expected_records
        or lock.get("records_sha256") != _sha256_json(expected_records)
        or lock.get("new_semantic_model_call_cap") != 0
        or lock.get("semantic_replay_allowed") is not False
        or lock.get("semantic_output_repair_allowed") is not False
        or lock.get("production_mutated") is not False
        or lock.get("holdout_authorized") is not False
        or lock.get("winner_frozen") is not False
        or any(not _verify_record(row) for row in expected_records)
    ):
        raise Epoch30RecoveryError("epoch-30 runtime lock drifted")
    load_contract()
    return lock


def _receipt(root: Path) -> dict[str, Any]:
    failure = validate_completed_failure()
    return {
        "schema_version": RECEIPT_VERSION,
        "thread_id": THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "state": "rejected",
        "terminal_reason": "epoch29_adjudication_exact_evidence_validation_rejected",
        "created_at": now_iso(),
        "output_root": str(root),
        "semantic_model_call_count": 0,
        "measured_model_call_count": 0,
        "unknown_usage_turn_count": 0,
        "semantic_retry_count": 0,
        "usage_status": "zero_call_recovery",
        "accounting_complete": True,
        "usage": {field: 0 for field in epoch29.USAGE_FIELDS},
        "recovered_predecessor_semantic_model_call_count": 1,
        "recovered_predecessor_usage_status": "measured",
        "recovered_predecessor_usage": failure["usage"],
        "recovered_predecessor_thread_id": failure["thread_id"],
        "recovered_predecessor_turn_id": failure["turn_id"],
        "recovered_predecessor_wall_elapsed_seconds": failure["wall_elapsed_seconds"],
        "exact_validation_error_count": failure["validation_error_count"],
        "exact_validation_errors_sha256": failure["validation_errors_sha256"],
        "aggregate_architecture_semantic_model_call_count": 8,
        "aggregate_architecture_unknown_usage_turn_count": 2,
        "aggregate_architecture_measured_total_tokens": 265_792,
        "quality_measured_by_this_step": False,
        "quality_passed": False,
        "next_authorized_action": (
            "materially_distinct_opaque_source_unit_adjudication_without_prior_replay"
        ),
        "semantic_output_repair_applied": False,
        "winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "runtime_lock": _record(root / "runtime-lock.json"),
        "recovered_artifact_records": failure["artifact_records"],
    }


def verify_receipt(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    output_root = root.expanduser().resolve()
    verify_runtime_lock(output_root / "runtime-lock.json")
    receipt_path = output_root / "plan-step-receipt.json"
    terminal_path = output_root / "terminal.json"
    present = [path for path in (receipt_path, terminal_path) if path.is_file()]
    if not present:
        raise Epoch30RecoveryError("epoch-30 terminal receipt is absent")
    if len(present) == 1:
        existing = _load_json(present[0], "epoch-30 terminal mirror")
        expected = _receipt(output_root)
        expected["created_at"] = existing.get("created_at")
        if (
            not isinstance(existing.get("created_at"), str)
            or not existing.get("created_at")
            or existing != expected
        ):
            raise Epoch30RecoveryError("epoch-30 terminal mirror drifted")
        missing = terminal_path if present[0] == receipt_path else receipt_path
        _write_immutable_json(missing, existing)
    receipt = _load_json(receipt_path, "epoch-30 receipt")
    terminal = _load_json(terminal_path, "epoch-30 terminal")
    if receipt != terminal:
        raise Epoch30RecoveryError("epoch-30 terminal mirrors differ")
    expected = _receipt(output_root)
    expected["created_at"] = receipt.get("created_at")
    if (
        not isinstance(receipt.get("created_at"), str)
        or not receipt.get("created_at")
        or receipt != expected
    ):
        raise Epoch30RecoveryError("epoch-30 receipt drifted")
    return receipt


def run(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    output_root = root.expanduser().resolve()
    if (output_root / "plan-step-receipt.json").exists() or (
        output_root / "terminal.json"
    ).exists():
        return verify_receipt(output_root)
    freeze_run(output_root)
    receipt = _receipt(output_root)
    _write_immutable_json(output_root / "plan-step-receipt.json", receipt)
    _write_immutable_json(output_root / "terminal.json", receipt)
    return verify_receipt(output_root)


def status(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    output_root = root.expanduser().resolve()
    if (output_root / "plan-step-receipt.json").exists() or (
        output_root / "terminal.json"
    ).exists():
        receipt = verify_receipt(output_root)
        return {"state": receipt["state"], "terminal_reason": receipt["terminal_reason"]}
    if (output_root / "runtime-lock.json").exists():
        verify_runtime_lock(output_root / "runtime-lock.json")
        return {"state": "frozen_zero_call"}
    return {"state": "absent"}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("freeze", "verify-runtime", "run", "verify-receipt", "status"):
        commands.add_parser(command)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            result = freeze_run(args.root)
        elif args.command == "verify-runtime":
            result = verify_runtime_lock(args.root / "runtime-lock.json")
        elif args.command == "run":
            result = run(args.root)
        elif args.command == "verify-receipt":
            result = verify_receipt(args.root)
        else:
            result = status(args.root)
    except Epoch30RecoveryError as exc:
        print(json.dumps({"state": "invalid", "error": str(exc)}, sort_keys=True))
        return 1
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
