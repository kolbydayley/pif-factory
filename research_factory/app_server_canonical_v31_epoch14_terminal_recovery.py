from __future__ import annotations

"""Zero-call terminal recovery for the completed epoch-14 semantic attempt."""

import argparse
import copy
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_literal_pointer_canary_runtime as epoch14
from . import app_server_one_turn_canary_runtime as one_turn


ROOT = epoch14.DEFAULT_ROOT
RECOVERY_FILENAME = "terminal-accounting-recovery.json"
RECEIPT_FILENAME = "plan-step-receipt.json"
TERMINAL_FILENAME = "terminal.json"
REJECTION_FILENAME = "semantic-rejection.json"
RECOVERY_VERSION = "pif_epoch14_terminal_accounting_recovery_v1"
REJECTION_VERSION = "pif_epoch14_terminal_accounting_rejection_v1"
RECEIPT_VERSION = "pif_semantic_plan_step_receipt_v1"
EXPECTED_RUNTIME_LOCK_SHA256 = (
    "c5e61187701c5a8dbe9e2d6d9f32f2f7e70eda0bb6a05eb09616ca7e77d291f7"
)
EXPECTED_SIDECAR_SHA256 = (
    "3e7531861ac7901b7220befd9b9b6e5e3ab6227c1f555705c80845e3a9673815"
)
EXPECTED_OUTPUT_SHA256 = (
    "8a18fdcc778eb2f32a66be62dac09dd6acdfb1cabba9bed9db535f1837dfa6e9"
)
EXPECTED_THREAD_ID = "019f7c06-d56d-78f0-b096-5c2856947758"
EXPECTED_TURN_ID = "019f7c06-d7a4-78c2-b790-4ec611f1798f"
EXPECTED_PROJECTION_ERROR = "unit coverage receipt counts do not reconcile"


class Epoch14TerminalRecoveryError(RuntimeError):
    pass


def _record(path: Path) -> dict[str, Any]:
    return one_turn._record(path)  # noqa: SLF001


def _load(path: Path, label: str) -> dict[str, Any]:
    return one_turn._load(path, label)  # noqa: SLF001


def _write(path: Path, value: Any) -> dict[str, Any]:
    return one_turn._write_json(path, value)  # noqa: SLF001


def _usage(value: Any, label: str) -> dict[str, int]:
    return epoch14.adapter.base._usage_values(value, label)  # noqa: SLF001


def _validate_completed_accounting(
    sidecar: Mapping[str, Any],
    *,
    request: Mapping[str, Any],
    thread: Any,
    sidecar_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    try:
        epoch14.adapter.validate_turn_sidecar(
            request,
            sidecar_path,
            output_path=output_path,
            expected_thread=thread,
        )
    except epoch14.adapter.CanonicalV31TelemetryError as exc:
        if str(exc) != "new-thread total usage differs from turn usage":
            raise Epoch14TerminalRecoveryError(
                "epoch-14 sidecar failed outside the isolated accounting invariant"
            ) from exc
    else:
        raise Epoch14TerminalRecoveryError(
            "epoch-14 legacy sidecar validator unexpectedly passed"
        )
    turn_usage = _usage(sidecar.get("usage"), "turn usage")
    thread_total_usage = _usage(sidecar.get("thread_total_usage"), "thread total usage")
    if (
        sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("error_class") is not None
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("thread_id") != EXPECTED_THREAD_ID
        or sidecar.get("turn_id") != EXPECTED_TURN_ID
        or any(
            thread_total_usage[field] < turn_usage[field]
            for field in epoch14.adapter.USAGE_FIELDS
        )
        or thread_total_usage == turn_usage
        or not isinstance(sidecar.get("wall_elapsed_seconds"), (int, float))
        or isinstance(sidecar.get("wall_elapsed_seconds"), bool)
        or float(sidecar["wall_elapsed_seconds"]) < 0
        or not output_path.is_file()
        or sidecar.get("output_sha256")
        != epoch14.adapter.base._output_message_hash(output_path)  # noqa: SLF001
        or Path(str(sidecar.get("output_path") or "")).expanduser().resolve()
        != output_path.resolve()
    ):
        raise Epoch14TerminalRecoveryError(
            "epoch-14 cumulative accounting or output lineage drifted"
        )
    return {
        "turn_usage": turn_usage,
        "thread_total_usage": thread_total_usage,
        "wall_elapsed_seconds": float(sidecar["wall_elapsed_seconds"]),
    }


def audit_completed_attempt(root: Path = ROOT) -> dict[str, Any]:
    output_root = root.expanduser().resolve()
    paths = one_turn._paths(output_root)  # noqa: SLF001
    epoch14.verify_runtime(output_root)
    authorization = epoch14.verify_authorization(
        output_root,
        expected_authorization_id="kolby-epoch14-literal-pointer-20260719",
        require_current=False,
    )
    contract = _load(paths["contract"], "epoch-14 runtime contract")
    request = _load(paths["request"], "epoch-14 request")
    if (
        _record(paths["lock"])["sha256"] != EXPECTED_RUNTIME_LOCK_SHA256
        or _record(paths["sidecar"])["sha256"] != EXPECTED_SIDECAR_SHA256
        or _record(paths["output"])["sha256"] != EXPECTED_OUTPUT_SHA256
        or _load(paths["attempt"], "epoch-14 attempt")
        != one_turn._attempt_payload(  # noqa: SLF001
            epoch14.SPEC, output_root, authorization
        )
    ):
        raise Epoch14TerminalRecoveryError("epoch-14 immutable attempt lineage drifted")
    one_turn._verify_capacity_bundles(  # noqa: SLF001
        epoch14.SPEC,
        output_root,
        contract,
        authorization,
        historical=True,
    )
    thread_payload = _load(paths["thread"], "epoch-14 thread")
    thread = one_turn._thread_from_payload(  # noqa: SLF001
        epoch14.SPEC, thread_payload, request
    )
    dispatch = _load(paths["dispatch"], "epoch-14 dispatch")
    if (
        thread.thread_id != EXPECTED_THREAD_ID
        or thread_payload.get("runtime_contract") != _record(paths["contract"])
        or thread_payload.get("runtime_lock") != _record(paths["lock"])
        or thread_payload.get("operator_authorization")
        != _record(paths["authorization"])
        or thread_payload.get("turn_manifest") != _record(paths["manifest"])
        or dispatch.get("schema_version") != one_turn.DISPATCH_VERSION
        or dispatch.get("state") != "semantic_turn_dispatch_committed"
        or dispatch.get("turn_name") != epoch14.TURN_NAME
        or dispatch.get("request") != _record(paths["request"])
        or dispatch.get("runtime_lock") != _record(paths["lock"])
        or dispatch.get("operator_authorization") != _record(paths["authorization"])
        or dispatch.get("thread") != _record(paths["thread"])
        or dispatch.get("initial_capacity")
        != one_turn._capacity_records(paths, "initial_capacity")  # noqa: SLF001
        or dispatch.get("preturn_capacity")
        != one_turn._capacity_records(paths, "preturn_capacity")  # noqa: SLF001
        or dispatch.get("semantic_retry_count") != 0
    ):
        raise Epoch14TerminalRecoveryError("epoch-14 dispatch lineage drifted")
    sidecar = _load(paths["sidecar"], "epoch-14 sidecar")
    accounting = _validate_completed_accounting(
        sidecar,
        request=request,
        thread=thread,
        sidecar_path=paths["sidecar"],
        output_path=paths["output"],
    )
    raw_output = _load(paths["output"], "epoch-14 raw output")
    try:
        epoch14.adapter.validate_and_project_output(request, raw_output)
    except epoch14.adapter.CanonicalV31OutputError as exc:
        projection_error = str(exc)
    else:
        raise Epoch14TerminalRecoveryError(
            "epoch-14 raw output unexpectedly passed canonical projection"
        )
    if (
        projection_error != EXPECTED_PROJECTION_ERROR
        or accounting["thread_total_usage"]["total_tokens"] != 443_532
        or accounting["turn_usage"]["total_tokens"] != 78_693
        or accounting["thread_total_usage"]["total_tokens"]
        <= epoch14.MAXIMUM_TOTAL_TOKENS
    ):
        raise Epoch14TerminalRecoveryError("epoch-14 measured rejection evidence drifted")
    return {
        "runtime_contract": _record(paths["contract"]),
        "runtime_lock": _record(paths["lock"]),
        "authorization": _record(paths["authorization"]),
        "attempt": _record(paths["attempt"]),
        "thread": _record(paths["thread"]),
        "dispatch": _record(paths["dispatch"]),
        "sidecar": _record(paths["sidecar"]),
        "raw_output": _record(paths["output"]),
        "initial_capacity": one_turn._capacity_records(  # noqa: SLF001
            paths, "initial_capacity"
        ),
        "preturn_capacity": one_turn._capacity_records(  # noqa: SLF001
            paths, "preturn_capacity"
        ),
        "turn_usage": accounting["turn_usage"],
        "thread_total_usage": accounting["thread_total_usage"],
        "wall_elapsed_seconds": accounting["wall_elapsed_seconds"],
        "thread_id": EXPECTED_THREAD_ID,
        "turn_id": EXPECTED_TURN_ID,
        "projection_error": projection_error,
    }


def _rejection_payload(audit: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": REJECTION_VERSION,
        "turn_name": epoch14.TURN_NAME,
        "failed_checks": [
            "measured_total_token_acceptance_ceiling",
            "semantic_output_validity",
        ],
        "semantic_output_error": audit["projection_error"],
        "semantic_output_error_sha256": one_turn._sha256_bytes(  # noqa: SLF001
            str(audit["projection_error"]).encode("utf-8")
        ),
        "measured_total_tokens": audit["thread_total_usage"]["total_tokens"],
        "measured_total_token_acceptance_ceiling": epoch14.MAXIMUM_TOTAL_TOKENS,
        "legacy_validator_error": "new-thread total usage differs from turn usage",
        "correct_accounting_source": "thread_total_usage",
    }


def _recovery_payload(
    audit: Mapping[str, Any], *, created_at: str
) -> dict[str, Any]:
    return {
        "schema_version": RECOVERY_VERSION,
        "state": "deterministic_terminal_recovery_complete",
        "created_at": created_at,
        "recovery_semantic_model_call_count": 0,
        "recovery_semantic_retry_count": 0,
        "adopted_original_semantic_model_call_count": 1,
        "legacy_validator_error": "new-thread total usage differs from turn usage",
        "protocol_accounting_interpretation": (
            "last_usage_is_the_last_sampling_cycle_and_thread_total_usage_is_the_"
            "complete_cumulative_single_turn_accounting"
        ),
        "turn_usage": copy.deepcopy(audit["turn_usage"]),
        "thread_total_usage": copy.deepcopy(audit["thread_total_usage"]),
        "semantic_output_error": audit["projection_error"],
        "runtime_lock": copy.deepcopy(audit["runtime_lock"]),
        "sidecar": copy.deepcopy(audit["sidecar"]),
        "raw_output": copy.deepcopy(audit["raw_output"]),
        "recovery_runtime": _record(Path(__file__).resolve()),
        "production_mutated": False,
        "holdout_authorized": False,
    }


def _artifact_records(root: Path) -> dict[str, Any]:
    paths = one_turn._paths(root)  # noqa: SLF001
    records = one_turn._artifact_records(root)  # noqa: SLF001
    records["terminal_accounting_recovery"] = _record(root / RECOVERY_FILENAME)
    records["recovery_runtime"] = _record(Path(__file__).resolve())
    if paths["rejection"].is_file():
        records["rejection"] = _record(paths["rejection"])
    return records


def _receipt_payload(
    root: Path,
    audit: Mapping[str, Any],
    *,
    created_at: str,
) -> dict[str, Any]:
    paths = one_turn._paths(root)  # noqa: SLF001
    return {
        "schema_version": RECEIPT_VERSION,
        "thread_id": epoch14.THREAD_ID,
        "plan_epoch": epoch14.PLAN_EPOCH,
        "step_id": epoch14.STEP_ID,
        "state": "rejected",
        "terminal_reason": (
            "epoch14_literal_pointer_canary_cost_and_semantic_output_rejected_"
            "after_accounting_recovery"
        ),
        "created_at": created_at,
        "output_root": str(root.resolve()),
        "new_semantic_model_call_count": 1,
        "semantic_retry_count": 0,
        "new_unknown_usage_turn_count": 0,
        "new_measured_usage": copy.deepcopy(audit["thread_total_usage"]),
        "last_sampling_cycle_usage": copy.deepcopy(audit["turn_usage"]),
        "new_wall_elapsed_seconds": audit["wall_elapsed_seconds"],
        "failed_checks": [
            "measured_total_token_acceptance_ceiling",
            "semantic_output_validity",
        ],
        "diagnostic": {
            "error_class": "CanonicalV31OutputError",
            "diagnostic_path": audit["projection_error"],
            "legacy_accounting_validator_defect": (
                "new_thread_total_usage_was_incorrectly_required_to_equal_last_usage"
            ),
        },
        "thread_ids": [audit["thread_id"]],
        "semantic_turn_ids": [audit["turn_id"]],
        "runtime_contract": copy.deepcopy(audit["runtime_contract"]),
        "runtime_lock": copy.deepcopy(audit["runtime_lock"]),
        "operator_authorization": copy.deepcopy(audit["authorization"]),
        "directive": _record(epoch14.DIRECTIVE_PATH),
        "semantic_plan": _record(epoch14.PLAN_PATH),
        "artifact_records": _artifact_records(root),
        "architecture_class": (
            "bounded_evidence_with_llm_selected_exact_metric_literal_token_indices"
        ),
        "literal_tokenization_version": epoch14.adapter.LITERAL_TOKENIZATION_VERSION,
        "literal_token_count": epoch14.EXPECTED_LITERAL_TOKEN_COUNT,
        "measured_total_token_acceptance_ceiling": epoch14.MAXIMUM_TOTAL_TOKENS,
        "recovery_semantic_model_call_count": 0,
        "winner_frozen": False,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "next_authorized_action": (
            "reject_literal_pointer_architecture_and_predeclare_next_distinct_"
            "bounded_two_pass_llm_architecture"
        ),
    }


def recover(root: Path = ROOT) -> dict[str, Any]:
    output_root = root.expanduser().resolve()
    receipt_path = output_root / RECEIPT_FILENAME
    terminal_path = output_root / TERMINAL_FILENAME
    if receipt_path.is_file() or terminal_path.is_file():
        return verify(root)
    audit = audit_completed_attempt(output_root)
    rejection = _rejection_payload(audit)
    rejection_path = output_root / REJECTION_FILENAME
    if rejection_path.exists():
        if _load(rejection_path, "epoch-14 semantic rejection") != rejection:
            raise Epoch14TerminalRecoveryError("semantic rejection artifact drifted")
    else:
        _write(rejection_path, rejection)
    created_at = one_turn._now().isoformat()  # noqa: SLF001
    recovery_payload = _recovery_payload(audit, created_at=created_at)
    _write(output_root / RECOVERY_FILENAME, recovery_payload)
    payload = _receipt_payload(output_root, audit, created_at=created_at)
    _write(receipt_path, payload)
    _write(terminal_path, payload)
    return verify(output_root)


def verify(root: Path = ROOT) -> dict[str, Any]:
    output_root = root.expanduser().resolve()
    receipt_path = output_root / RECEIPT_FILENAME
    terminal_path = output_root / TERMINAL_FILENAME
    if not receipt_path.is_file() or not terminal_path.is_file():
        raise Epoch14TerminalRecoveryError("epoch-14 terminal mirrors are incomplete")
    receipt = _load(receipt_path, "epoch-14 recovered receipt")
    terminal = _load(terminal_path, "epoch-14 recovered terminal")
    if receipt != terminal:
        raise Epoch14TerminalRecoveryError("epoch-14 terminal mirrors differ")
    created_at = receipt.get("created_at")
    one_turn._parse_timestamp(created_at, "recovery created_at")  # noqa: SLF001
    audit = audit_completed_attempt(output_root)
    recovery_path = output_root / RECOVERY_FILENAME
    recovery = _load(recovery_path, "epoch-14 accounting recovery")
    if recovery != _recovery_payload(audit, created_at=created_at):
        raise Epoch14TerminalRecoveryError("epoch-14 accounting recovery drifted")
    if _load(
        output_root / REJECTION_FILENAME, "epoch-14 semantic rejection"
    ) != _rejection_payload(audit):
        raise Epoch14TerminalRecoveryError("epoch-14 semantic rejection drifted")
    expected = _receipt_payload(output_root, audit, created_at=created_at)
    if receipt != expected:
        raise Epoch14TerminalRecoveryError("epoch-14 recovered receipt drifted")
    return receipt


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("command", choices=("audit", "recover", "verify"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    value = (
        audit_completed_attempt(args.root)
        if args.command == "audit"
        else recover(args.root)
        if args.command == "recover"
        else verify(args.root)
    )
    print(json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n", end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__: Sequence[str] = (
    "Epoch14TerminalRecoveryError",
    "ROOT",
    "audit_completed_attempt",
    "recover",
    "verify",
)
