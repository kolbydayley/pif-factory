from __future__ import annotations

"""Epoch-24 provider-schema recovery for the unit-owned canonical canary."""

import argparse
import asyncio
import copy
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_epoch22_quality_adoption as epoch22
from . import app_server_canonical_v31_unit_owned_ordinal_episode_batch as adapter
from . import app_server_canonical_v31_unit_owned_positional_canary_runtime as epoch23
from . import app_server_one_turn_canary_runtime as one_turn


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROOT = (
    PROJECT_ROOT
    / "work/app-server-development-v2/unattended-pipeline-v5"
    / "canonical-v31-epoch24-unit-owned-ordinal-schema-recovery-v1"
)
SOURCE_ROOT = epoch23.SOURCE_ROOT
PREDECESSOR_ROOT = epoch23.DEFAULT_ROOT
QUALITY_PREDECESSOR_ROOT = epoch22.DEFAULT_ROOT
DIRECTIVE_PATH = (
    PROJECT_ROOT
    / "automation/pif-evaluation-epoch24-unit-owned-ordinal-schema-recovery-v24.json"
)
PLAN_PATH = PROJECT_ROOT / "automation/pif-evaluation-semantic-plan-v24.json"
DIRECTIVE_SHA256 = "f508f8a9dd08a2a540c5621198437e4bb826cc552b340e13c795d33d8dfc8191"
THREAD_ID = "019f4cf1-c46e-7db3-acd2-bf03c4459a10"
PLAN_EPOCH = 24
STEP_ID = "canonical_v31_epoch24_unit_owned_ordinal_schema_recovery_v24"
TURN_NAME = "epoch24_unit_owned_ordinal_two_case_schema_recovery"
AUTHORIZATION_STATEMENT = "You have my full permission to continue. No need to seek out my approval anymore."
AUTHORIZATION_ID_DEFAULT = "kolby-epoch24-unit-owned-ordinal-20260719"
SELECTED_SEGMENT_IDS = epoch23.SELECTED_SEGMENT_IDS
MAXIMUM_TOTAL_TOKENS = 32_000
MAXIMUM_WALL_SECONDS = 1_200
MINIMUM_REMAINING_RESERVE_PERCENT = 10
CAPACITY_SAFETY_MARGIN_PERCENT = 1
QUOTA_POINTS_PER_MILLION_TOKENS = 17
AUTHORIZATION_WINDOW_SECONDS = 14_400
EXPECTED_SHAPE = {
    "prompt_bytes": 14_277,
    "base_bytes": 22_773,
    "schema_bytes": 5_420,
    "request_bytes": 310_984,
    "source_unit_count": 20,
    "evidence_span_count": 38,
    "literal_token_count": 2_020,
}
FULL_VISIBLE_REQUEST_BYTES = 70_407
CANARY_VISIBLE_REQUEST_BYTES = 42_470
FULL_SHARED_REFERENCE_UNIT_TARGET = 148
PREDECESSOR_RUNTIME_LOCK_SHA256 = "03c3d0f2fe81fe8b93f9855b2fd45bdc4f6f34810f1243c4eb1d31bb43f05df5"
PREDECESSOR_RECEIPT_SHA256 = "d4ab5d93e68d7767cd73c95693caf287112591e2ca6bcb543b465ef1aaeb90bc"
PREDECESSOR_SIDECAR_SHA256 = "0e16d394060a1188328b1f63e4fad3e26cd19a45d5f58225dd8ee7a9846bf78b"
QUALITY_RUNTIME_LOCK_SHA256 = "ca65364267bbac099ce097f5e84153166aa7648eb3d69da5ea14087ca877a620"
QUALITY_RECEIPT_SHA256 = "5125266047c804cc771856f75531972ae03426d8100ab29804c276fdea5b9927"

UnitOwnedOrdinalCanaryError = one_turn.OneTurnCanaryError
UnitOwnedOrdinalCanaryWaiting = one_turn.OneTurnCanaryWaiting


def _load(path: Path, label: str) -> dict[str, Any]:
    return one_turn._load(path, label)  # noqa: SLF001


def _record(path: Path) -> dict[str, Any]:
    return one_turn._record(path)  # noqa: SLF001


def _request_shape(request: Mapping[str, Any]) -> dict[str, int]:
    return {
        "prompt_bytes": len(request["prompt"].encode("utf-8")),
        "base_bytes": len(request["base_instructions"].encode("utf-8")),
        "schema_bytes": len(
            one_turn._canonical_json(request["output_schema"]).encode("utf-8")  # noqa: SLF001
        ),
        "request_bytes": len(one_turn._canonical_json(request).encode("utf-8")),  # noqa: SLF001
        "source_unit_count": sum(
            len(segment["units"])
            for segment in request["private_input"]["segments"]
        ),
        "evidence_span_count": sum(
            len(segment["evidence_spans"])
            for segment in request["private_input"]["segments"]
        ),
        "literal_token_count": sum(
            len(unit["literal_tokens"])
            for segment in request["private_input"]["segments"]
            for unit in segment["units"]
        ),
    }


def _validate_provider_schema(value: Any, path: str = "$") -> None:
    if isinstance(value, Mapping):
        if "prefixItems" in value or isinstance(value.get("items"), bool):
            raise UnitOwnedOrdinalCanaryError(
                f"epoch-24 provider-incompatible schema at {path}"
            )
        value_type = value.get("type")
        types = set(value_type) if isinstance(value_type, list) else {value_type}
        if "object" in types:
            properties = value.get("properties")
            if (
                not isinstance(properties, Mapping)
                or value.get("additionalProperties") is not False
                or set(value.get("required", ())) != set(properties)
            ):
                raise UnitOwnedOrdinalCanaryError(
                    f"epoch-24 object schema is not closed at {path}"
                )
        if "array" in types and not isinstance(value.get("items"), Mapping):
            raise UnitOwnedOrdinalCanaryError(
                f"epoch-24 array items schema missing at {path}"
            )
        for key, child in value.items():
            _validate_provider_schema(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _validate_provider_schema(child, f"{path}[{index}]")


def _build_request() -> dict[str, Any]:
    requests = adapter.prepare_episode_batches(
        epoch23._source_episode(),  # noqa: SLF001
        batch_size=3,
        thread_mode="new_thread",
    )
    if len(requests) != 1:
        raise UnitOwnedOrdinalCanaryError("epoch-24 request count drifted")
    request = requests[0]
    if (
        tuple(request.get("segment_ids", ())) != SELECTED_SEGMENT_IDS
        or request.get("effective_batch_size") != 2
        or request.get("batch_size_ceiling") != 3
        or request.get("candidate_system_id") != adapter.CANDIDATE_SYSTEM_ID
        or request.get("semantic_integrity", {}).get("ordinal_protocol_version")
        != adapter.ORDINAL_PROTOCOL_VERSION
        or "positional_protocol_version" in request.get("semantic_integrity", {})
        or _request_shape(request) != EXPECTED_SHAPE
    ):
        raise UnitOwnedOrdinalCanaryError("epoch-24 frozen request shape drifted")
    _validate_provider_schema(request["output_schema"])
    adapter.validate_prepared_request(request)
    return request


_PREDECESSOR_FILES: tuple[tuple[str, Path], ...] = (
    ("epoch23_runtime_lock", PREDECESSOR_ROOT / "runtime-lock.json"),
    ("epoch23_receipt", PREDECESSOR_ROOT / "plan-step-receipt.json"),
    ("epoch23_terminal", PREDECESSOR_ROOT / "terminal.json"),
    ("epoch23_authorization", PREDECESSOR_ROOT / "operator-authorization.json"),
    ("epoch23_attempt", PREDECESSOR_ROOT / "turn/semantic-attempt.json"),
    ("epoch23_dispatch", PREDECESSOR_ROOT / "turn/semantic-dispatch.json"),
    ("epoch23_thread_start", PREDECESSOR_ROOT / "turn/thread-start.json"),
    (
        "epoch23_initial_capacity_request",
        PREDECESSOR_ROOT / "turn/capacity/initial/request.json",
    ),
    (
        "epoch23_initial_capacity_measurement",
        PREDECESSOR_ROOT / "turn/capacity/initial/measurement.json",
    ),
    (
        "epoch23_initial_capacity_provider_response",
        PREDECESSOR_ROOT / "turn/capacity/initial/provider-response.private.json",
    ),
    (
        "epoch23_preturn_capacity_request",
        PREDECESSOR_ROOT / "turn/capacity/preturn/request.json",
    ),
    (
        "epoch23_preturn_capacity_measurement",
        PREDECESSOR_ROOT / "turn/capacity/preturn/measurement.json",
    ),
    (
        "epoch23_preturn_capacity_provider_response",
        PREDECESSOR_ROOT / "turn/capacity/preturn/provider-response.private.json",
    ),
    ("epoch23_sidecar", PREDECESSOR_ROOT / "turn/sidecar.json"),
    ("epoch23_request", PREDECESSOR_ROOT / "prepared-turn/request.private.json"),
    ("epoch22_runtime_lock", QUALITY_PREDECESSOR_ROOT / "runtime-lock.json"),
    ("epoch22_receipt", QUALITY_PREDECESSOR_ROOT / "plan-step-receipt.json"),
    ("epoch22_terminal", QUALITY_PREDECESSOR_ROOT / "terminal.json"),
)


def _frozen_predecessor_evidence() -> list[dict[str, Any]]:
    return [{"role": role, **_record(path)} for role, path in _PREDECESSOR_FILES]


def _predecessor_records() -> Mapping[str, Any]:
    epoch23_records = {
        role.removeprefix("epoch23_"): _record(path)
        for role, path in _PREDECESSOR_FILES
        if role.startswith("epoch23_")
    }
    epoch22_records = {
        role.removeprefix("epoch22_"): _record(path)
        for role, path in _PREDECESSOR_FILES
        if role.startswith("epoch22_")
    }
    return {
        "epoch23_interrupted_unknown_usage_attempt": epoch23_records,
        "epoch22_quality_rejection": epoch22_records,
    }


def _validate_predecessor(full_verify: bool) -> None:
    predecessor = (
        epoch23.verify_receipt(PREDECESSOR_ROOT)
        if full_verify
        else _load(PREDECESSOR_ROOT / "plan-step-receipt.json", "epoch-23 receipt")
    )
    quality = _load(
        QUALITY_PREDECESSOR_ROOT / "plan-step-receipt.json", "epoch-22 receipt"
    )
    sidecar = _load(PREDECESSOR_ROOT / "turn/sidecar.json", "epoch-23 sidecar")
    if (
        _record(PREDECESSOR_ROOT / "runtime-lock.json")["sha256"]
        != PREDECESSOR_RUNTIME_LOCK_SHA256
        or _record(PREDECESSOR_ROOT / "plan-step-receipt.json")["sha256"]
        != PREDECESSOR_RECEIPT_SHA256
        or _record(PREDECESSOR_ROOT / "terminal.json")["sha256"]
        != PREDECESSOR_RECEIPT_SHA256
        or _record(PREDECESSOR_ROOT / "turn/sidecar.json")["sha256"]
        != PREDECESSOR_SIDECAR_SHA256
        or predecessor.get("state") != "waiting"
        or predecessor.get("terminal_reason")
        != "epoch23_interrupted_semantic_attempt_preserved_no_replay"
        or predecessor.get("new_semantic_model_call_count") != 1
        or predecessor.get("new_unknown_usage_turn_count") != 1
        or predecessor.get("semantic_retry_count") != 0
        or predecessor.get("new_measured_usage", {}).get("total_tokens") != 0
        or predecessor.get("production_mutated") is not False
        or predecessor.get("holdout_authorized") is not False
        or sidecar.get("schema_version") != "pif_codex_app_server_turn_v2"
        or sidecar.get("state") != "failed"
        or sidecar.get("status") != "failed"
        or sidecar.get("error_class") != "turn_failed"
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("plan_type") != "pro"
        or sidecar.get("usage_status") != "unknown"
        or sidecar.get("usage_complete") is not False
        or sidecar.get("usage") is not None
        or sidecar.get("output_sha256") is not None
        or sidecar.get("recovery_reran_model") is not False
        or (PREDECESSOR_ROOT / "turn/output.private.json").exists()
        or _record(QUALITY_PREDECESSOR_ROOT / "runtime-lock.json")["sha256"]
        != QUALITY_RUNTIME_LOCK_SHA256
        or _record(QUALITY_PREDECESSOR_ROOT / "plan-step-receipt.json")["sha256"]
        != QUALITY_RECEIPT_SHA256
        or _record(QUALITY_PREDECESSOR_ROOT / "terminal.json")["sha256"]
        != QUALITY_RECEIPT_SHA256
        or quality.get("state") != "rejected"
        or quality.get("candidate_strict_full_field_macro_f1") != 0.344287
        or quality.get("production_mutated") is not False
        or quality.get("holdout_authorized") is not False
    ):
        raise UnitOwnedOrdinalCanaryError("epoch-24 predecessor evidence drifted")
    _build_request()


def _runtime_module_paths() -> Sequence[Path]:
    return (
        Path(__file__),
        Path(adapter.__file__),
        Path(adapter.positional.__file__),
        Path(adapter.parent.__file__),
        Path(adapter.parent.compact.__file__),
        Path(adapter.parent.compact.local.__file__),
        Path(adapter.parent.compact.local.bounded_span.__file__),
        Path(adapter.parent.compact.local.pointer.__file__),
        Path(adapter.base.__file__),
        Path(one_turn.__file__),
        Path(epoch23.__file__),
        Path(epoch22.__file__),
    )


def _validate_directive(value: Mapping[str, Any]) -> None:
    expected = json.loads(DIRECTIVE_PATH.read_text(encoding="utf-8"))
    recovery = expected.get("recovery_contract", {})
    constraints = recovery.get("provider_schema_constraints", {})
    if dict(value) != expected:
        raise UnitOwnedOrdinalCanaryError("epoch-24 directive contract drifted")
    if (
        expected.get("schema_version")
        != "pif_evaluation_epoch24_unit_owned_ordinal_schema_recovery_directive_v1"
        or expected.get("thread_id") != THREAD_ID
        or expected.get("plan_epoch") != PLAN_EPOCH
        or expected.get("step_id") != STEP_ID
        or expected.get("authorized_by") != "kolby"
        or expected.get("authority") != "direct_user_instruction"
        or expected.get("expected_receipt_path")
        != str((DEFAULT_ROOT / "plan-step-receipt.json").resolve())
        or expected.get("frozen_predecessor_evidence")
        != _frozen_predecessor_evidence()
        or recovery.get("selected_segment_ids") != list(SELECTED_SEGMENT_IDS)
        or recovery.get("semantic_model_call_cap") != 1
        or recovery.get("semantic_retry_cap") != 0
        or recovery.get("measured_total_token_ceiling") != MAXIMUM_TOTAL_TOKENS
        or recovery.get("epoch23_replay_allowed") is not False
        or recovery.get("semantic_delta_from_epoch23") != "none"
        or constraints
        != {
            "all_array_items_are_schemas": True,
            "all_objects_closed_and_fully_required": True,
            "boolean_items_allowed": False,
            "prefix_items_allowed": False,
        }
        or expected.get("transport_contract", {}).get("transport")
        != "official_codex_app_server_stdio_managed_chatgpt_auth"
        or expected.get("promotion_contract", {}).get("holdout_authorized") is not False
        or expected.get("promotion_contract", {}).get("production_mutation_allowed")
        is not False
    ):
        raise UnitOwnedOrdinalCanaryError("epoch-24 directive invariants drifted")


def _receipt_metadata(outcome: Mapping[str, Any]) -> Mapping[str, Any]:
    usage = outcome["usage"]
    total = int(usage["total_tokens"])
    fidelity_path = DEFAULT_ROOT / "turn/semantic-fidelity.json"
    event_count = None
    if fidelity_path.is_file():
        fidelity = _load(fidelity_path, "epoch-24 semantic fidelity")
        event_count = fidelity.get("emitted_event_count")
    input_projection = (
        math.ceil(
            int(usage["input_tokens"])
            * FULL_VISIBLE_REQUEST_BYTES
            / CANARY_VISIBLE_REQUEST_BYTES
        )
        if outcome["state"] != "waiting"
        else None
    )
    output_projection = None
    full_total_projection = None
    if isinstance(event_count, int) and event_count > 0:
        output_projection = math.ceil(
            int(usage["output_tokens"])
            * FULL_SHARED_REFERENCE_UNIT_TARGET
            / event_count
        )
        full_total_projection = input_projection + output_projection
    return {
        "diagnostic_segment_ids": list(SELECTED_SEGMENT_IDS),
        "diagnostic_source_unit_count": EXPECTED_SHAPE["source_unit_count"],
        "diagnostic_event_count": event_count,
        "measured_canary_total_tokens": total,
        "full_six_case_input_token_projection": input_projection,
        "full_six_case_output_token_projection_at_148_units": output_projection,
        "full_six_case_total_token_projection_at_148_units": full_total_projection,
        "production_token_ceiling_for_future_full_run": 73_926,
        "quality_measured_by_this_step": False,
        "judge_required_before_full_run_promotion": True,
        "epoch23_predecessor_semantic_model_call_count": 1,
        "epoch23_predecessor_unknown_usage_turn_count": 1,
        "epoch23_predecessor_measured_total_tokens": 0,
        "epoch23_replayed": False,
        "provider_schema_recovery_only": True,
        "prior_semantic_output_reuse_count": 0,
        "deterministic_semantic_pruning": False,
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
    stage="unit_owned_ordinal_two_case_provider_schema_recovery",
    architecture_class="one_turn_unit_owned_ordinal_full_canonical_schema_v1",
    authorization_statement=AUTHORIZATION_STATEMENT,
    authorization_state="authorized_for_exactly_one_two_case_provider_schema_recovery",
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
    pass_next_action="freeze_two_case_full_event_ab_ba_quality_and_cost_projection",
    reject_next_action="freeze_provider_schema_recovery_nonacceptance_without_replay",
)


def _runtime_status(status: Mapping[str, Any], root: Path) -> dict[str, Any]:
    request = _load(root.resolve() / "prepared-turn/request.private.json", "epoch-24 request")
    shape = _request_shape(request)
    if shape != EXPECTED_SHAPE:
        raise UnitOwnedOrdinalCanaryError("epoch-24 runtime request shape drifted")
    _validate_provider_schema(request["output_schema"])
    return {**copy.deepcopy(dict(status)), **shape}


def prepare_canary(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    return _runtime_status(one_turn.prepare(SPEC, root), root)


def verify_runtime(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    return _runtime_status(one_turn.verify_runtime(SPEC, root), root)


def authorize_canary(
    *, root: Path = DEFAULT_ROOT, operator_authorization_id: str, now: Any = None
) -> dict[str, Any]:
    return one_turn.authorize(
        SPEC,
        root=root,
        operator_authorization_id=operator_authorization_id,
        now=now,
    )


async def execute_canary(
    *, root: Path = DEFAULT_ROOT, operator_authorization_id: str
) -> dict[str, Any]:
    return await one_turn.execute(
        SPEC,
        root=root,
        operator_authorization_id=operator_authorization_id,
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
            root=args.root,
            operator_authorization_id=args.operator_authorization_id,
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
