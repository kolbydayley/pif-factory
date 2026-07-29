from __future__ import annotations

"""Epoch-17 one-turn canary for unit-local canonical metric pointers."""

import argparse
import asyncio
import copy
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_bounded_span_canary_runtime as epoch13
from . import app_server_canonical_v31_metric_compiler_canary_runtime as epoch15
from . import app_server_canonical_v31_source_unit_owner_window_canary_runtime as epoch16
from . import app_server_canonical_v31_unit_local_pointer_episode_batch as adapter
from . import app_server_one_turn_canary_runtime as one_turn


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROOT = (
    PROJECT_ROOT
    / "work/app-server-development-v2/unattended-pipeline-v5/"
    "canonical-v31-epoch17-unit-local-pointer-canary-v1"
)
SOURCE_ROOT = epoch13.DEFAULT_ROOT
EXACT_LABELS_ROOT = epoch15.DEFAULT_ROOT
PREDECESSOR_ROOT = epoch16.DEFAULT_ROOT
DIRECTIVE_PATH = (
    PROJECT_ROOT / "automation/pif-evaluation-epoch17-unit-local-pointer-canary-v17.json"
)
PLAN_PATH = PROJECT_ROOT / "automation/pif-evaluation-semantic-plan-v17.json"
DIRECTIVE_SHA256 = "768c9aebe3cdb14d30493cf9845b33b670f08f40032f735573ce933095d96bac"
THREAD_ID = "019f4cf1-c46e-7db3-acd2-bf03c4459a10"
PLAN_EPOCH = 17
STEP_ID = "canonical_v31_epoch17_unit_local_pointer_canary_v17"
TURN_NAME = "epoch17_unit_local_pointer_768c9aebe3cdb14d30493cf9"
AUTHORIZATION_STATEMENT = (
    "You have my full permission to continue. No need to seek out my approval anymore."
)
AUTHORIZATION_ID_DEFAULT = "kolby-epoch17-unit-local-pointer-20260719"
MAXIMUM_TOTAL_TOKENS = 73_000
MAXIMUM_TOKENS_BELOW_RATIO_GATE = 73_926
MAXIMUM_WALL_SECONDS = 1_200
MINIMUM_REMAINING_RESERVE_PERCENT = 10
CAPACITY_SAFETY_MARGIN_PERCENT = 1
QUOTA_POINTS_PER_MILLION_TOKENS = 17
EXPECTED_PROMPT_BYTES = 46_826
EXPECTED_BASE_BYTES = 20_398
EXPECTED_SCHEMA_BYTES = 20_087
EXPECTED_REQUEST_BYTES = 850_817
EXPECTED_SOURCE_UNIT_COUNT = 60
EXPECTED_EVIDENCE_SPAN_COUNT = 114
EXPECTED_LOCAL_TOKEN_COUNT = 6_078
EXPECTED_SEGMENT_COUNT = 6
EXPECTED_PRIOR_EVENT_COUNT = 56
EXPECTED_EXACT_METRIC_LITERAL_COUNT = 77
EXPECTED_SEGMENT_IDS = (
    "seg_155ba31ee473823b6e7e9504",
    "seg_a845b306d731d9728a5b0fa0",
    "seg_80fe585badf8f3d3597d0f96",
    "seg_6aa065273c73b7078583dcb6",
    "seg_7c027e01ed7e813b528c5c7f",
    "seg_24034ad57133895b52bdc068",
)
SOURCE_RECEIPT_SHA256 = (
    "26c7f4870adc029719b097e82306c80f7b052b01faa4467fcd396816d43b7c60"
)
SOURCE_REQUEST_SHA256 = (
    "7f2e23b294164e0a530435f4ef00719f0e9f63754d3fb054460dc412cca53f6f"
)
SOURCE_OUTPUT_SHA256 = (
    "202c0cd1d9de66b7b999e49f3ac5d094886dcb32bf8845a22deda0f8f9a8f418"
)
EXACT_LABELS_SHA256 = (
    "fa0f9c0c8a9edb569f98c3a1f19f817465632a645a60a3f2ae195f0784fb282c"
)
EPOCH15_RECEIPT_SHA256 = (
    "4fe9baaa16410d4021ae6f7c43b242c3469a7977bf2802169b0c673889cf5ebb"
)
PREDECESSOR_RECEIPT_SHA256 = (
    "e57fbf633cb8804694b0c2e95cce095962be54473e5380345bb862a2801efd50"
)
PREDECESSOR_RUNTIME_LOCK_SHA256 = (
    "6d70a3fa0c533825cb42690b8f134d226142fd1684a97424d804c8dc51716f0b"
)
PREDECESSOR_OUTPUT_SHA256 = (
    "c7d5c234fb63e5fd1d458a593a3dfa2620318c0b4d82c3c840448ac2b3c06bcd"
)
PREDECESSOR_SIDECAR_SHA256 = (
    "b6d76504199ce322501a66b446806274e08078432dc68938708768c61c1b83c3"
)
PRODUCTION_FIXED_TOKENS = 600_538
PRODUCTION_MULTIPLIER = 30
PRODUCTION_DENOMINATOR_TOKENS = 10_065_426
PRODUCTION_RATIO_GATE = 0.28

UnitLocalPointerCanaryError = one_turn.OneTurnCanaryError
UnitLocalPointerCanaryWaiting = one_turn.OneTurnCanaryWaiting


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
        "unit_local_token_count": sum(
            len(unit["literal_tokens"])
            for segment in request["private_input"]["segments"]
            for unit in segment["units"]
        ),
    }


def _build_request() -> dict[str, Any]:
    source = _load(
        SOURCE_ROOT / "prepared-turn/request.private.json",
        "epoch-13 source request",
    )
    episode = copy.deepcopy(source["episode_context"])
    episode["segments"] = [
        {
            key: copy.deepcopy(segment[key])
            for key in (
                "segment_id",
                "segment_text",
                "segment_quality",
                "density_stratum",
                "boundaries",
            )
        }
        for segment in source["private_input"]["segments"]
    ]
    requests = adapter.prepare_episode_batches(
        episode, batch_size=8, thread_mode="new_thread"
    )
    if len(requests) != 1:
        raise UnitLocalPointerCanaryError("epoch-17 request count drifted")
    request = requests[0]
    expected_shape = {
        "prompt_bytes": EXPECTED_PROMPT_BYTES,
        "base_bytes": EXPECTED_BASE_BYTES,
        "schema_bytes": EXPECTED_SCHEMA_BYTES,
        "request_bytes": EXPECTED_REQUEST_BYTES,
        "source_unit_count": EXPECTED_SOURCE_UNIT_COUNT,
        "evidence_span_count": EXPECTED_EVIDENCE_SPAN_COUNT,
        "unit_local_token_count": EXPECTED_LOCAL_TOKEN_COUNT,
    }
    if (
        tuple(request.get("segment_ids", ())) != EXPECTED_SEGMENT_IDS
        or request.get("batch_size_ceiling") != 8
        or request.get("effective_batch_size") != EXPECTED_SEGMENT_COUNT
        or _request_shape(request) != expected_shape
    ):
        raise UnitLocalPointerCanaryError("epoch-17 frozen request shape drifted")
    adapter.validate_prepared_request(request)
    return request


def _literal_is_representable(
    source: Mapping[str, Any], span: Mapping[str, Any], literal: str
) -> bool:
    starts = {
        token["start_char"]
        for unit in source["units"]
        for token in unit["literal_tokens"]
    }
    ends = {
        token["end_char"]
        for unit in source["units"]
        for token in unit["literal_tokens"]
    }
    offset = source["segment_text"].find(
        literal, span["start_char"], span["end_char"]
    )
    while offset >= 0 and offset + len(literal) <= span["end_char"]:
        if offset in starts and offset + len(literal) in ends:
            return True
        offset = source["segment_text"].find(
            literal, offset + 1, span["end_char"]
        )
    return False


def _validate_representability(request: Mapping[str, Any]) -> None:
    source_output = _load(
        SOURCE_ROOT / "turn/output.private.json", "epoch-13 raw output"
    )
    exact_labels = one_turn._load_json(  # noqa: SLF001
        EXACT_LABELS_ROOT / "turn/canonical-labels.private.json",
        "epoch-15 exact labels",
    )
    if not isinstance(exact_labels, list) or len(exact_labels) != EXPECTED_SEGMENT_COUNT:
        raise UnitLocalPointerCanaryError("epoch-17 exact labels drifted")
    event_count = 0
    literal_count = 0
    for raw_segment, exact_label, source in zip(
        source_output["segments"], exact_labels, request["private_input"]["segments"]
    ):
        if (
            raw_segment["segment_id"] != exact_label["segment_id"]
            or exact_label["segment_id"] != source["segment_id"]
        ):
            raise UnitLocalPointerCanaryError("epoch-17 representability order drifted")
        spans = {row["evidence_span_id"]: row for row in source["evidence_spans"]}
        if len(raw_segment["discourse_events"]) != len(exact_label["discourse_events"]):
            raise UnitLocalPointerCanaryError("epoch-17 event cardinality drifted")
        for raw_event, exact_event in zip(
            raw_segment["discourse_events"], exact_label["discourse_events"]
        ):
            event_count += 1
            span = spans[raw_event["evidence_span_id"]]
            for field in adapter.METRIC_LITERAL_FIELDS:
                literal = exact_event["metric"][field]
                if literal is None:
                    continue
                literal_count += 1
                if not _literal_is_representable(source, span, literal):
                    raise UnitLocalPointerCanaryError(
                        "epoch-17 exact metric literal is not unit-local representable"
                    )
    if (
        event_count != EXPECTED_PRIOR_EVENT_COUNT
        or literal_count != EXPECTED_EXACT_METRIC_LITERAL_COUNT
    ):
        raise UnitLocalPointerCanaryError("epoch-17 representability counts drifted")


def _validate_predecessor(full_verify: bool) -> None:
    source_receipt_path = SOURCE_ROOT / "plan-step-receipt.json"
    exact_receipt_path = EXACT_LABELS_ROOT / "plan-step-receipt.json"
    predecessor_receipt_path = PREDECESSOR_ROOT / "plan-step-receipt.json"
    source_receipt = (
        epoch13.verify_receipt(SOURCE_ROOT)
        if full_verify
        else _load(source_receipt_path, "epoch-13 source receipt")
    )
    exact_receipt = (
        epoch15.verify_receipt(EXACT_LABELS_ROOT)
        if full_verify
        else _load(exact_receipt_path, "epoch-15 receipt")
    )
    predecessor = (
        epoch16.verify_receipt(PREDECESSOR_ROOT)
        if full_verify
        else _load(predecessor_receipt_path, "epoch-16 receipt")
    )
    if (
        _record(source_receipt_path)["sha256"] != SOURCE_RECEIPT_SHA256
        or _record(SOURCE_ROOT / "terminal.json")["sha256"] != SOURCE_RECEIPT_SHA256
        or _record(SOURCE_ROOT / "prepared-turn/request.private.json")["sha256"]
        != SOURCE_REQUEST_SHA256
        or _record(SOURCE_ROOT / "turn/output.private.json")["sha256"]
        != SOURCE_OUTPUT_SHA256
        or source_receipt.get("state") != "rejected"
        or source_receipt.get("new_semantic_model_call_count") != 1
        or source_receipt.get("new_measured_usage", {}).get("total_tokens") != 61_072
        or source_receipt.get("production_mutated") is not False
        or _record(exact_receipt_path)["sha256"] != EPOCH15_RECEIPT_SHA256
        or _record(EXACT_LABELS_ROOT / "turn/canonical-labels.private.json")["sha256"]
        != EXACT_LABELS_SHA256
        or exact_receipt.get("state") != "rejected"
        or exact_receipt.get("compiler_measured_total_tokens") != 17_453
        or exact_receipt.get("production_mutated") is not False
        or _record(predecessor_receipt_path)["sha256"]
        != PREDECESSOR_RECEIPT_SHA256
        or _record(PREDECESSOR_ROOT / "terminal.json")["sha256"]
        != PREDECESSOR_RECEIPT_SHA256
        or _record(PREDECESSOR_ROOT / "runtime-lock.json")["sha256"]
        != PREDECESSOR_RUNTIME_LOCK_SHA256
        or _record(PREDECESSOR_ROOT / "turn/output.private.json")["sha256"]
        != PREDECESSOR_OUTPUT_SHA256
        or _record(PREDECESSOR_ROOT / "turn/sidecar.json")["sha256"]
        != PREDECESSOR_SIDECAR_SHA256
        or predecessor.get("state") != "rejected"
        or predecessor.get("terminal_reason")
        != "epoch16_one_turn_canary_semantic_output_rejected"
        or predecessor.get("new_semantic_model_call_count") != 1
        or predecessor.get("semantic_retry_count") != 0
        or predecessor.get("new_unknown_usage_turn_count") != 0
        or predecessor.get("new_measured_usage", {}).get("total_tokens") != 43_683
        or predecessor.get("failed_checks")
        != ["measured_total_token_acceptance_ceiling", "semantic_output_validity"]
        or predecessor.get("diagnostic", {}).get("diagnostic_path")
        != "metric value is not an exact substring of its selected source-unit span and evidence"
        or predecessor.get("production_mutated") is not False
        or predecessor.get("holdout_authorized") is not False
    ):
        raise UnitLocalPointerCanaryError("epoch-17 predecessor evidence drifted")
    request = _build_request()
    _validate_representability(request)


def _predecessor_records() -> Mapping[str, Any]:
    return {
        "source_epoch13": {
            "receipt": _record(SOURCE_ROOT / "plan-step-receipt.json"),
            "terminal": _record(SOURCE_ROOT / "terminal.json"),
            "runtime_lock": _record(SOURCE_ROOT / "runtime-lock.json"),
            "prepared_request": _record(
                SOURCE_ROOT / "prepared-turn/request.private.json"
            ),
            "raw_output": _record(SOURCE_ROOT / "turn/output.private.json"),
        },
        "exact_metric_evidence_epoch15": {
            "receipt": _record(EXACT_LABELS_ROOT / "plan-step-receipt.json"),
            "terminal": _record(EXACT_LABELS_ROOT / "terminal.json"),
            "canonical_labels": _record(
                EXACT_LABELS_ROOT / "turn/canonical-labels.private.json"
            ),
        },
        "rejected_epoch16": {
            "receipt": _record(PREDECESSOR_ROOT / "plan-step-receipt.json"),
            "terminal": _record(PREDECESSOR_ROOT / "terminal.json"),
            "runtime_lock": _record(PREDECESSOR_ROOT / "runtime-lock.json"),
            "raw_output": _record(PREDECESSOR_ROOT / "turn/output.private.json"),
            "sidecar": _record(PREDECESSOR_ROOT / "turn/sidecar.json"),
        },
    }


def _runtime_module_paths() -> Sequence[Path]:
    return (
        Path(__file__),
        Path(adapter.__file__),
        Path(adapter.bounded_span.__file__),
        Path(adapter.pointer.__file__),
        Path(adapter.base.__file__),
        Path(epoch13.__file__),
        Path(epoch15.__file__),
        Path(epoch16.__file__),
    )


def _expected_directive() -> dict[str, Any]:
    return json.loads(DIRECTIVE_PATH.read_text(encoding="utf-8"))


def _validate_directive(value: Mapping[str, Any]) -> None:
    expected = _expected_directive()
    if dict(value) != expected:
        raise UnitLocalPointerCanaryError("epoch-17 directive contract drifted")
    if (
        expected.get("schema_version")
        != "pif_evaluation_epoch17_unit_local_pointer_canary_directive_v1"
        or expected.get("thread_id") != THREAD_ID
        or expected.get("plan_epoch") != PLAN_EPOCH
        or expected.get("step_id") != STEP_ID
        or expected.get("expected_receipt_path")
        != str((DEFAULT_ROOT / "plan-step-receipt.json").resolve())
        or expected["canary_contract"]["measured_total_token_acceptance_ceiling"]
        != MAXIMUM_TOTAL_TOKENS
        or expected["transport_contract"]["transport"]
        != "official_codex_app_server_stdio_managed_chatgpt_auth"
        or expected["promotion_contract"]["production_mutation_allowed"] is not False
    ):
        raise UnitLocalPointerCanaryError("epoch-17 directive invariants drifted")


def _production_ratio(tokens: int) -> float:
    return (
        PRODUCTION_FIXED_TOKENS + tokens * PRODUCTION_MULTIPLIER
    ) / PRODUCTION_DENOMINATOR_TOKENS


def _receipt_metadata(outcome: Mapping[str, Any]) -> Mapping[str, Any]:
    total_tokens = int(outcome["usage"]["total_tokens"])
    usage_complete = (
        outcome["semantic_model_call_count"] == 1
        and outcome["unknown_usage_turn_count"] == 0
    )
    ratio = _production_ratio(total_tokens) if usage_complete else None
    gate = {
        "managed_usage_complete": usage_complete,
        "measured_total_tokens_at_most_73000": (
            usage_complete and total_tokens <= MAXIMUM_TOTAL_TOKENS
        ),
        "production_amortized_ratio_below_0_28": (
            ratio is not None and ratio < PRODUCTION_RATIO_GATE
        ),
    }
    if outcome["state"] == "passed" and not all(gate.values()):
        raise UnitLocalPointerCanaryError("epoch-17 pass violates production cost gate")
    return {
        "measured_extraction_total_tokens": total_tokens,
        "production_amortized_ratio": ratio,
        "production_cost_gate": gate,
        "production_amortized_ratio_formula": (
            "(600538 + measured_total_tokens * 30) / 10065426"
        ),
        "source_segment_count": EXPECTED_SEGMENT_COUNT,
        "source_unit_count": EXPECTED_SOURCE_UNIT_COUNT,
        "evidence_span_count": EXPECTED_EVIDENCE_SPAN_COUNT,
        "unit_local_token_count": EXPECTED_LOCAL_TOKEN_COUNT,
        "offline_representable_metric_literal_count": (
            EXPECTED_EXACT_METRIC_LITERAL_COUNT
        ),
        "prior_semantic_output_reuse_count": 0,
        "unit_local_metric_pointer_contract": True,
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
    stage="unit_local_metric_pointer_full_episode",
    architecture_class="one_turn_full_canonical_unit_local_metric_pointer_extraction_v1",
    authorization_statement=AUTHORIZATION_STATEMENT,
    authorization_state="authorized_for_exactly_one_unit_local_pointer_canary_turn",
    model=adapter.MODEL,
    effort=adapter.EFFORT,
    maximum_total_tokens=MAXIMUM_TOTAL_TOKENS,
    maximum_wall_seconds=MAXIMUM_WALL_SECONDS,
    minimum_remaining_reserve_percent=MINIMUM_REMAINING_RESERVE_PERCENT,
    capacity_safety_margin_percent=CAPACITY_SAFETY_MARGIN_PERCENT,
    quota_points_per_million_tokens=QUOTA_POINTS_PER_MILLION_TOKENS,
    authorization_window_seconds=14_400,
    adapter=adapter,
    validate_directive=_validate_directive,
    validate_predecessor=_validate_predecessor,
    build_request=_build_request,
    predecessor_records=_predecessor_records,
    runtime_module_paths=_runtime_module_paths,
    receipt_metadata=_receipt_metadata,
    pass_next_action="freeze_full_event_ab_ba_shared_reference_quality_evaluation",
    reject_next_action="reject_unit_local_pointer_architecture_without_field_patch",
)


def _runtime_status(status: Mapping[str, Any], root: Path) -> dict[str, Any]:
    request = _load(
        root.expanduser().resolve() / "prepared-turn/request.private.json",
        "epoch-17 prepared request",
    )
    shape = _request_shape(request)
    if shape != {
        "prompt_bytes": EXPECTED_PROMPT_BYTES,
        "base_bytes": EXPECTED_BASE_BYTES,
        "schema_bytes": EXPECTED_SCHEMA_BYTES,
        "request_bytes": EXPECTED_REQUEST_BYTES,
        "source_unit_count": EXPECTED_SOURCE_UNIT_COUNT,
        "evidence_span_count": EXPECTED_EVIDENCE_SPAN_COUNT,
        "unit_local_token_count": EXPECTED_LOCAL_TOKEN_COUNT,
    }:
        raise UnitLocalPointerCanaryError("epoch-17 runtime status shape drifted")
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


def verify_authorization(
    root: Path = DEFAULT_ROOT,
    *,
    expected_authorization_id: str | None,
    require_current: bool,
    now: Any = None,
) -> dict[str, Any]:
    return one_turn.verify_authorization(
        SPEC,
        root=root,
        expected_authorization_id=expected_authorization_id,
        require_current=require_current,
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
    sub.add_parser("verify")
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
    elif args.command == "verify":
        value = verify_receipt(args.root)
    else:
        value = status_canary(args.root)
    print(json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n", end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__: Sequence[str] = (
    "AUTHORIZATION_ID_DEFAULT",
    "DEFAULT_ROOT",
    "UnitLocalPointerCanaryError",
    "authorize_canary",
    "execute_canary",
    "prepare_canary",
    "status_canary",
    "verify_authorization",
    "verify_receipt",
    "verify_runtime",
)
