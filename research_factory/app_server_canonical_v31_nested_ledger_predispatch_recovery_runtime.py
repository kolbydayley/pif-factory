from __future__ import annotations

"""Epoch-34 zero-replay recovery for the epoch-33 predispatch wait."""

import argparse
import asyncio
import copy
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_nested_proposition_event_ledger_canary_runtime as epoch33
from . import app_server_one_turn_canary_runtime as one_turn


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROOT = (
    PROJECT_ROOT
    / "work/app-server-development-v2/unattended-pipeline-v5"
    / "canonical-v31-epoch34-nested-ledger-predispatch-recovery-v1"
)
EPOCH33_ROOT = epoch33.DEFAULT_ROOT
DIRECTIVE_PATH = (
    PROJECT_ROOT
    / "automation/pif-evaluation-epoch34-nested-ledger-predispatch-recovery-v34.json"
)
PLAN_PATH = PROJECT_ROOT / "automation/pif-evaluation-semantic-plan-v34.json"
DIRECTIVE_SHA256 = "cf0fa9239517dc77ddb6dc0903d09ccd18b2e911ca620a53858b66d30ac0560d"
THREAD_ID = epoch33.THREAD_ID
PLAN_EPOCH = 34
STEP_ID = "canonical_v31_epoch34_nested_ledger_predispatch_recovery_v34"
TURN_NAME = "epoch34_nested_ledger_predispatch_recovery"
AUTHORIZATION_STATEMENT = epoch33.AUTHORIZATION_STATEMENT
AUTHORIZATION_ID_DEFAULT = "kolby-epoch34-nested-ledger-predispatch-recovery-20260720"
SELECTED_SEGMENT_IDS = epoch33.SELECTED_SEGMENT_IDS
MAXIMUM_TOTAL_TOKENS = epoch33.MAXIMUM_TOTAL_TOKENS
MAXIMUM_WALL_SECONDS = epoch33.MAXIMUM_WALL_SECONDS
MINIMUM_REMAINING_RESERVE_PERCENT = epoch33.MINIMUM_REMAINING_RESERVE_PERCENT
CAPACITY_SAFETY_MARGIN_PERCENT = epoch33.CAPACITY_SAFETY_MARGIN_PERCENT
QUOTA_POINTS_PER_MILLION_TOKENS = epoch33.QUOTA_POINTS_PER_MILLION_TOKENS
AUTHORIZATION_WINDOW_SECONDS = epoch33.AUTHORIZATION_WINDOW_SECONDS
EXPECTED_SHAPE = epoch33.EXPECTED_SHAPE
EXPECTED_REQUEST_SHA256 = epoch33.EXPECTED_REQUEST_SHA256
EXPECTED_PROMPT_SHA256 = epoch33.EXPECTED_PROMPT_SHA256
EXPECTED_BASE_SHA256 = epoch33.EXPECTED_BASE_SHA256
EXPECTED_SCHEMA_SHA256 = epoch33.EXPECTED_SCHEMA_SHA256
CANARY_VISIBLE_REQUEST_BYTES = epoch33.CANARY_VISIBLE_REQUEST_BYTES
FULL_VISIBLE_REQUEST_BYTES = epoch33.FULL_VISIBLE_REQUEST_BYTES
FULL_CASE_SCALE = epoch33.FULL_CASE_SCALE
FULL_PRODUCTION_TOKEN_CEILING = epoch33.FULL_PRODUCTION_TOKEN_CEILING
adapter = epoch33.adapter

NestedLedgerRecoveryError = one_turn.OneTurnCanaryError
NestedLedgerRecoveryWaiting = one_turn.OneTurnCanaryWaiting


def _load(path: Path, label: str) -> dict[str, Any]:
    return one_turn._load(path, label)  # noqa: SLF001


def _record(path: Path) -> dict[str, Any]:
    return one_turn._record(path)  # noqa: SLF001


def _request_shape(request: Mapping[str, Any]) -> dict[str, int]:
    return epoch33._request_shape(request)  # noqa: SLF001


def _request_sha256(request: Mapping[str, Any]) -> str:
    return epoch33._request_sha256(request)  # noqa: SLF001


def _build_request() -> dict[str, Any]:
    request = epoch33._build_request()  # noqa: SLF001
    if (
        _request_shape(request) != EXPECTED_SHAPE
        or _request_sha256(request) != EXPECTED_REQUEST_SHA256
        or request.get("prompt_sha256") != EXPECTED_PROMPT_SHA256
        or request.get("base_instructions_sha256") != EXPECTED_BASE_SHA256
        or request.get("output_schema_sha256") != EXPECTED_SCHEMA_SHA256
        or request.get("model") != "gpt-5.6-sol"
        or request.get("effort") != "medium"
        or request.get("retry_count") != 0
    ):
        raise NestedLedgerRecoveryError("epoch-34 request drifted from epoch 33")
    return request


_PREDECESSOR_FILES: tuple[tuple[str, Path], ...] = (
    ("epoch33_receipt", EPOCH33_ROOT / "plan-step-receipt.json"),
    ("epoch33_terminal", EPOCH33_ROOT / "terminal.json"),
    ("epoch33_runtime_lock", EPOCH33_ROOT / "runtime-lock.json"),
    ("epoch33_runtime_contract", EPOCH33_ROOT / "runtime-contract.json"),
    ("epoch33_authorization", EPOCH33_ROOT / "operator-authorization.json"),
    ("epoch33_attempt", EPOCH33_ROOT / "turn/semantic-attempt.json"),
    ("epoch33_capacity_request", EPOCH33_ROOT / "turn/capacity/initial/request.json"),
    (
        "epoch33_capacity_measurement",
        EPOCH33_ROOT / "turn/capacity/initial/measurement.json",
    ),
    (
        "epoch33_capacity_provider_response",
        EPOCH33_ROOT / "turn/capacity/initial/provider-response.private.json",
    ),
    ("epoch33_request", EPOCH33_ROOT / "prepared-turn/request.private.json"),
)

_PREDECESSOR_HASHES = {
    "epoch33_receipt": "7e68e2b313d0b026ddc8e7705e76ec76c52cd5db05b89841374f2d248a152b04",
    "epoch33_terminal": "7e68e2b313d0b026ddc8e7705e76ec76c52cd5db05b89841374f2d248a152b04",
    "epoch33_runtime_lock": "23624bd395c2ca5717bf0cebff5032cd7134e060a729833beed1e74385c3a264",
    "epoch33_runtime_contract": "b41ba742e49d61034dc41a24064384ae4b464114940adc03cfdf76e84162ba0a",
    "epoch33_authorization": "d6e474dc31826513ee6f925493015635d289ad873c2c438f88ab509f6b299248",
    "epoch33_attempt": "1c533669349275331cd9403a27d52a7a18c05860f2963ce692f8a1baf34d0248",
    "epoch33_capacity_request": "9147d4678b1a269fcf7af0137ba495a9bf9174e56e5fc45a32495efd3ca8227c",
    "epoch33_capacity_measurement": "3ca2e6c8a643cd3ccb71d5f502ff9208b7e2bfb0ad2c262812818e7139c08aa7",
    "epoch33_capacity_provider_response": "ce413ed2f318a532a0976a7e9c1d1b361076de491c32c0322fa7085c37472c7c",
    "epoch33_request": "549104c9624d10d4f6b3d53700ac79b9855219ecc0b0f239af549fff8db24107",
}


def _frozen_predecessor_evidence() -> list[dict[str, Any]]:
    return [{"role": role, **_record(path)} for role, path in _PREDECESSOR_FILES]


def _predecessor_records() -> Mapping[str, Any]:
    return {
        "epoch33_predispatch_waiting_zero_calls": {
            role.removeprefix("epoch33_"): _record(path)
            for role, path in _PREDECESSOR_FILES
        }
    }


def _intention_to_treat_accounting() -> dict[str, Any]:
    return {
        "predecessor_semantic_model_call_count": 10,
        "predecessor_unknown_usage_turn_count": 2,
        "predecessor_measured_total_tokens": 347_709,
        "maximum_semantic_model_call_count_after_epoch34_dispatch": 11,
        "predecessor_replay_allowed": False,
    }


def _validate_predecessor(full_verify: bool) -> None:
    del full_verify
    if any(
        _record(path)["sha256"] != _PREDECESSOR_HASHES[role]
        for role, path in _PREDECESSOR_FILES
    ):
        raise NestedLedgerRecoveryError("epoch-33 predecessor record drifted")
    receipt = _load(EPOCH33_ROOT / "plan-step-receipt.json", "epoch-33 receipt")
    terminal = _load(EPOCH33_ROOT / "terminal.json", "epoch-33 terminal")
    attempt = _load(EPOCH33_ROOT / "turn/semantic-attempt.json", "epoch-33 attempt")
    measurement = _load(
        EPOCH33_ROOT / "turn/capacity/initial/measurement.json",
        "epoch-33 capacity measurement",
    )
    forbidden = (
        "turn/thread.json",
        "turn/semantic-dispatch.json",
        "turn/capacity/preturn",
        "turn/sidecar.json",
        "turn/output.private.json",
        "turn/semantic-fidelity.json",
    )
    if (
        receipt != terminal
        or receipt.get("state") != "waiting"
        or receipt.get("terminal_reason")
        != "epoch33_capacity_or_predispatch_waiting_zero_calls"
        or receipt.get("new_semantic_model_call_count") != 0
        or receipt.get("semantic_retry_count") != 0
        or receipt.get("new_unknown_usage_turn_count") != 0
        or receipt.get("new_measured_usage", {}).get("total_tokens") != 0
        or receipt.get("thread_ids") != []
        or receipt.get("semantic_turn_ids") != []
        or attempt.get("state") != "declared_before_capacity_thread_or_turn"
        or measurement.get("state") != "cleared_before_semantic_boundary"
        or measurement.get("capacity_available") is not True
        or measurement.get("capacity_unknown") is not False
        or measurement.get("minimum_applicable_remaining_percent") != 75
        or measurement.get("minimum_remaining_reserve_percent") != 20
        or measurement.get("rate_limit_reached_type") is not None
        or measurement.get("semantic_thread_started") is not False
        or measurement.get("semantic_turn_started") is not False
        or any((EPOCH33_ROOT / value).exists() for value in forbidden)
        or _intention_to_treat_accounting()
        != json.loads(DIRECTIVE_PATH.read_text(encoding="utf-8"))[
            "intention_to_treat_accounting"
        ]
    ):
        raise NestedLedgerRecoveryError("epoch-33 zero-call waiting evidence drifted")


def _runtime_module_paths() -> Sequence[Path]:
    return (Path(__file__), *epoch33._runtime_module_paths())  # noqa: SLF001


def _validate_directive(value: Mapping[str, Any]) -> None:
    expected = json.loads(DIRECTIVE_PATH.read_text(encoding="utf-8"))
    recovery = expected.get("recovery_contract", {})
    if dict(value) != expected:
        raise NestedLedgerRecoveryError("epoch-34 directive contract drifted")
    if (
        expected.get("schema_version")
        != "pif_evaluation_epoch34_nested_ledger_predispatch_recovery_directive_v1"
        or expected.get("thread_id") != THREAD_ID
        or expected.get("plan_epoch") != PLAN_EPOCH
        or expected.get("step_id") != STEP_ID
        or expected.get("authorized_by") != "kolby"
        or expected.get("authority") != "direct_user_instruction"
        or expected.get("authorization_statement") != AUTHORIZATION_STATEMENT
        or expected.get("expected_receipt_path")
        != str((DEFAULT_ROOT / "plan-step-receipt.json").resolve())
        or expected.get("frozen_predecessor_evidence") != _frozen_predecessor_evidence()
        or expected.get("intention_to_treat_accounting")
        != _intention_to_treat_accounting()
        or recovery.get("exact_epoch33_semantic_request_reused") is not True
        or recovery.get("semantic_request_change_allowed") is not False
        or recovery.get("epoch33_semantic_model_call_count") != 0
        or recovery.get("epoch33_capacity_available") is not True
        or recovery.get("semantic_model_call_cap") != 1
        or recovery.get("semantic_retry_cap") != 0
        or recovery.get("measured_total_token_ceiling") != MAXIMUM_TOTAL_TOKENS
        or recovery.get("minimum_remaining_reserve_percent")
        != MINIMUM_REMAINING_RESERVE_PERCENT
        or expected.get("transport_contract", {}).get("transport")
        != "official_codex_app_server_stdio_managed_chatgpt_auth"
        or expected.get("promotion_contract", {}).get("quality_threshold") != 0.97
        or expected.get("promotion_contract", {}).get("holdout_authorized") is not False
        or expected.get("promotion_contract", {}).get("production_mutation_allowed")
        is not False
    ):
        raise NestedLedgerRecoveryError("epoch-34 directive invariants drifted")


def _receipt_metadata(outcome: Mapping[str, Any]) -> Mapping[str, Any]:
    usage = outcome["usage"]
    total = int(usage["total_tokens"])
    fidelity_path = DEFAULT_ROOT / "turn/semantic-fidelity.json"
    fidelity: Mapping[str, Any] = {}
    if fidelity_path.is_file():
        fidelity = _load(fidelity_path, "epoch-34 semantic fidelity")
    measurable = outcome["state"] != "waiting"
    input_projection = (
        math.ceil(
            int(usage["input_tokens"])
            * FULL_VISIBLE_REQUEST_BYTES
            / CANARY_VISIBLE_REQUEST_BYTES
        )
        if measurable
        else None
    )
    output_projection = int(usage["output_tokens"]) * FULL_CASE_SCALE if measurable else None
    full_projection = (
        input_projection + output_projection
        if input_projection is not None and output_projection is not None
        else None
    )
    cost_pass = (
        full_projection <= FULL_PRODUCTION_TOKEN_CEILING
        if full_projection is not None
        else False
    )
    current_calls = int(outcome["semantic_model_call_count"])
    current_unknown = int(outcome["unknown_usage_turn_count"])
    return {
        "diagnostic_segment_ids": list(SELECTED_SEGMENT_IDS),
        "diagnostic_source_unit_count": EXPECTED_SHAPE["source_unit_count"],
        "diagnostic_ledger_item_count": fidelity.get("ledger_item_count"),
        "diagnostic_event_count": fidelity.get("emitted_event_count"),
        "diagnostic_concept_count": fidelity.get("emitted_concept_count"),
        "ledger_event_cardinality_exact": (
            fidelity.get("ledger_item_count") == fidelity.get("emitted_event_count")
            if fidelity
            else None
        ),
        "measured_canary_total_tokens": total,
        "full_six_case_input_token_projection": input_projection,
        "full_six_case_output_token_projection_by_case_count": output_projection,
        "full_six_case_total_token_projection_by_case_count": full_projection,
        "full_six_case_cost_projection_pass": cost_pass,
        "structural_and_cost_pass": outcome["state"] == "passed" and cost_pass,
        "production_token_ceiling_for_future_full_run": FULL_PRODUCTION_TOKEN_CEILING,
        "quality_measured_by_this_step": False,
        "fresh_full_event_ab_ba_quality_required": True,
        "epoch33_zero_call_waiting_preserved": True,
        "epoch33_capacity_available": True,
        "epoch33_minimum_applicable_remaining_percent": 75,
        "aggregate_architecture_semantic_model_call_count": 10 + current_calls,
        "aggregate_architecture_unknown_usage_turn_count": 2 + current_unknown,
        "aggregate_architecture_measured_total_tokens": 347_709 + total,
        "predecessor_accounting_reconciled_additively": True,
        "epoch27_replayed": False,
        "epoch31_replayed": False,
        "epoch32_replayed": False,
        "epoch33_replayed": False,
        "prior_semantic_output_reuse_count": 0,
        "deterministic_semantic_pruning": False,
        "deterministic_support_filtering": False,
        "deterministic_deduplication": False,
        "deterministic_relabeling": False,
    }


SPEC = one_turn.OneTurnCanarySpec(
    project_root=PROJECT_ROOT,
    default_root=DEFAULT_ROOT,
    directive_path=DIRECTIVE_PATH,
    plan_path=PLAN_PATH,
    directive_sha256=DIRECTIVE_SHA256,
    thread_id=THREAD_ID,
    plan_epoch=PLAN_EPOCH,
    step_id=STEP_ID,
    turn_name=TURN_NAME,
    stage="nested_ledger_predispatch_recovery",
    architecture_class="one_turn_nested_proposition_event_ledger_full_canonical_v1",
    authorization_statement=AUTHORIZATION_STATEMENT,
    authorization_state="authorized_for_exactly_one_nested_ledger_predispatch_recovery",
    model=adapter.MODEL,
    effort=adapter.EFFORT,
    maximum_total_tokens=MAXIMUM_TOTAL_TOKENS,
    maximum_wall_seconds=MAXIMUM_WALL_SECONDS,
    minimum_remaining_reserve_percent=MINIMUM_REMAINING_RESERVE_PERCENT,
    capacity_safety_margin_percent=CAPACITY_SAFETY_MARGIN_PERCENT,
    quota_points_per_million_tokens=QUOTA_POINTS_PER_MILLION_TOKENS,
    authorization_window_seconds=AUTHORIZATION_WINDOW_SECONDS,
    adapter=adapter,
    validate_directive=_validate_directive,
    validate_predecessor=_validate_predecessor,
    build_request=_build_request,
    predecessor_records=_predecessor_records,
    runtime_module_paths=_runtime_module_paths,
    receipt_metadata=_receipt_metadata,
    pass_next_action="run_fresh_full_event_ab_ba_quality_only_if_structural_and_cost_pass_true",
    reject_next_action="reject_nested_ledger_architecture_without_field_patch",
    waiting_next_action="no_replay_external_predispatch_blocker_or_bounded_diagnostic",
)


def _runtime_status(status: Mapping[str, Any], root: Path) -> dict[str, Any]:
    request = _load(root.resolve() / "prepared-turn/request.private.json", "epoch-34 request")
    shape = _request_shape(request)
    if shape != EXPECTED_SHAPE or _request_sha256(request) != EXPECTED_REQUEST_SHA256:
        raise NestedLedgerRecoveryError("epoch-34 runtime request drifted")
    return {**copy.deepcopy(dict(status)), **shape}


def prepare_canary(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    return _runtime_status(one_turn.prepare(SPEC, root), root)


def verify_runtime(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    return _runtime_status(one_turn.verify_runtime(SPEC, root), root)


def authorize_canary(
    *, root: Path = DEFAULT_ROOT, operator_authorization_id: str, now: Any = None
) -> dict[str, Any]:
    return one_turn.authorize(
        SPEC, root=root, operator_authorization_id=operator_authorization_id, now=now
    )


async def execute_canary(
    *, root: Path = DEFAULT_ROOT, operator_authorization_id: str
) -> dict[str, Any]:
    return await one_turn.execute(
        SPEC, root=root, operator_authorization_id=operator_authorization_id
    )


def verify_receipt(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    return one_turn.verify_receipt(SPEC, root)


def status_canary(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    return one_turn.status(SPEC, root)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("prepare")
    authorize_parser = sub.add_parser("authorize")
    authorize_parser.add_argument("--operator-authorization-id", required=True)
    execute_parser = sub.add_parser("execute")
    execute_parser.add_argument("--operator-authorization-id", required=True)
    sub.add_parser("verify-runtime")
    sub.add_parser("verify-receipt")
    sub.add_parser("status")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "prepare":
        value = prepare_canary(args.root)
    elif args.command == "authorize":
        value = authorize_canary(
            root=args.root, operator_authorization_id=args.operator_authorization_id
        )
    elif args.command == "execute":
        value = asyncio.run(
            execute_canary(
                root=args.root,
                operator_authorization_id=args.operator_authorization_id,
            )
        )
    elif args.command == "verify-runtime":
        value = verify_runtime(args.root)
    elif args.command == "verify-receipt":
        value = verify_receipt(args.root)
    else:
        value = status_canary(args.root)
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
