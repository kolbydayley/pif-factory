from __future__ import annotations

"""Epoch-18 one-turn canary for compact positional unit-local pointers."""

import argparse
import asyncio
import copy
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_bounded_span_canary_runtime as epoch13
from . import app_server_canonical_v31_compact_unit_pointer_episode_batch as adapter
from . import app_server_canonical_v31_metric_compiler_canary_runtime as epoch15
from . import app_server_canonical_v31_unit_local_pointer_canary_runtime as epoch17
from . import app_server_one_turn_canary_runtime as one_turn


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROOT = (
    PROJECT_ROOT
    / "work/app-server-development-v2/unattended-pipeline-v5/"
    "canonical-v31-epoch18-compact-unit-pointer-canary-v1"
)
SOURCE_ROOT = epoch13.DEFAULT_ROOT
EXACT_LABELS_ROOT = epoch15.DEFAULT_ROOT
PREDECESSOR_ROOT = epoch17.DEFAULT_ROOT
DIRECTIVE_PATH = (
    PROJECT_ROOT / "automation/pif-evaluation-epoch18-compact-unit-pointer-canary-v18.json"
)
PLAN_PATH = PROJECT_ROOT / "automation/pif-evaluation-semantic-plan-v18.json"
DIRECTIVE_SHA256 = "569cdad6f3b29b2014167bcb458433d5aaa8e66cad1d0af921e9365053e48520"
THREAD_ID = "019f4cf1-c46e-7db3-acd2-bf03c4459a10"
PLAN_EPOCH = 18
STEP_ID = "canonical_v31_epoch18_compact_unit_pointer_canary_v18"
TURN_NAME = "epoch18_compact_unit_pointer_569cdad6f3b29b2014167bc"
AUTHORIZATION_STATEMENT = (
    "You have my full permission to continue. No need to seek out my approval anymore."
)
AUTHORIZATION_ID_DEFAULT = "kolby-epoch18-compact-unit-pointer-20260719"
MAXIMUM_TOTAL_TOKENS = 73_000
MAXIMUM_TOKENS_BELOW_RATIO_GATE = 73_926
MAXIMUM_WALL_SECONDS = 1_200
MINIMUM_REMAINING_RESERVE_PERCENT = 10
CAPACITY_SAFETY_MARGIN_PERCENT = 1
QUOTA_POINTS_PER_MILLION_TOKENS = 17
EXPECTED_PROMPT_BYTES = 48_141
EXPECTED_BASE_BYTES = 20_439
EXPECTED_SCHEMA_BYTES = 12_787
EXPECTED_REQUEST_BYTES = 845_132
EXPECTED_SOURCE_UNIT_COUNT = 60
EXPECTED_EVIDENCE_SPAN_COUNT = 114
EXPECTED_LOCAL_TOKEN_COUNT = 6_078
EXPECTED_SEGMENT_COUNT = 6
EXPECTED_EXACT_METRIC_LITERAL_COUNT = 77
EXPECTED_SEGMENT_IDS = epoch17.EXPECTED_SEGMENT_IDS
SOURCE_RECEIPT_SHA256 = epoch17.SOURCE_RECEIPT_SHA256
SOURCE_REQUEST_SHA256 = epoch17.SOURCE_REQUEST_SHA256
SOURCE_OUTPUT_SHA256 = epoch17.SOURCE_OUTPUT_SHA256
EXACT_LABELS_SHA256 = epoch17.EXACT_LABELS_SHA256
EPOCH15_RECEIPT_SHA256 = epoch17.EPOCH15_RECEIPT_SHA256
PREDECESSOR_RECEIPT_SHA256 = (
    "e5aa1bf1316150a187bc07b5e322fa15ec25f594e503d592c1028994f639ca58"
)
PREDECESSOR_RUNTIME_LOCK_SHA256 = (
    "f853c7cb9a56a4ff5466b4bf774809ee708b08089285a8ae18abdcbb5a083734"
)
PREDECESSOR_OUTPUT_SHA256 = (
    "d4540e96c6bd67ae9a5723d18728630d9b0c531a169f432d6b606a52ef6bf46e"
)
PREDECESSOR_SIDECAR_SHA256 = (
    "f71e0038b112cfea0829f6e54812cd31b04a7718b22bcc630c0450d6d92d77a2"
)
PREDECESSOR_LABELS_SHA256 = (
    "f0f856f4810773f1b76296eebee65d6fa42394fe7ece280a66f88fef50e31b33"
)
PRODUCTION_FIXED_TOKENS = 600_538
PRODUCTION_MULTIPLIER = 30
PRODUCTION_DENOMINATOR_TOKENS = 10_065_426
PRODUCTION_RATIO_GATE = 0.28

CompactUnitPointerCanaryError = one_turn.OneTurnCanaryError
CompactUnitPointerCanaryWaiting = one_turn.OneTurnCanaryWaiting


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


def _expected_shape() -> dict[str, int]:
    return {
        "prompt_bytes": EXPECTED_PROMPT_BYTES,
        "base_bytes": EXPECTED_BASE_BYTES,
        "schema_bytes": EXPECTED_SCHEMA_BYTES,
        "request_bytes": EXPECTED_REQUEST_BYTES,
        "source_unit_count": EXPECTED_SOURCE_UNIT_COUNT,
        "evidence_span_count": EXPECTED_EVIDENCE_SPAN_COUNT,
        "unit_local_token_count": EXPECTED_LOCAL_TOKEN_COUNT,
    }


def _build_request() -> dict[str, Any]:
    source = _load(
        SOURCE_ROOT / "prepared-turn/request.private.json", "epoch-13 source request"
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
        raise CompactUnitPointerCanaryError("epoch-18 request count drifted")
    request = requests[0]
    if (
        tuple(request.get("segment_ids", ())) != EXPECTED_SEGMENT_IDS
        or request.get("batch_size_ceiling") != 8
        or request.get("effective_batch_size") != EXPECTED_SEGMENT_COUNT
        or _request_shape(request) != _expected_shape()
    ):
        raise CompactUnitPointerCanaryError("epoch-18 frozen request shape drifted")
    adapter.validate_prepared_request(request)
    return request


def _validate_predecessor(full_verify: bool) -> None:
    source_receipt_path = SOURCE_ROOT / "plan-step-receipt.json"
    exact_receipt_path = EXACT_LABELS_ROOT / "plan-step-receipt.json"
    predecessor_receipt_path = PREDECESSOR_ROOT / "plan-step-receipt.json"
    source_receipt = _load(source_receipt_path, "epoch-13 source receipt")
    exact_receipt = _load(exact_receipt_path, "epoch-15 receipt")
    predecessor = (
        epoch17.verify_receipt(PREDECESSOR_ROOT)
        if full_verify
        else _load(predecessor_receipt_path, "epoch-17 receipt")
    )
    predecessor_sidecar = _load(
        PREDECESSOR_ROOT / "turn/sidecar.json", "epoch-17 sidecar"
    )
    if (
        _record(source_receipt_path)["sha256"] != SOURCE_RECEIPT_SHA256
        or _record(SOURCE_ROOT / "terminal.json")["sha256"] != SOURCE_RECEIPT_SHA256
        or _record(SOURCE_ROOT / "prepared-turn/request.private.json")["sha256"]
        != SOURCE_REQUEST_SHA256
        or _record(SOURCE_ROOT / "turn/output.private.json")["sha256"]
        != SOURCE_OUTPUT_SHA256
        or source_receipt.get("state") != "rejected"
        or source_receipt.get("production_mutated") is not False
        or _record(exact_receipt_path)["sha256"] != EPOCH15_RECEIPT_SHA256
        or _record(EXACT_LABELS_ROOT / "turn/canonical-labels.private.json")["sha256"]
        != EXACT_LABELS_SHA256
        or exact_receipt.get("state") != "rejected"
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
        or _record(PREDECESSOR_ROOT / "turn/canonical-labels.private.json")["sha256"]
        != PREDECESSOR_LABELS_SHA256
        or predecessor.get("state") != "rejected"
        or predecessor.get("terminal_reason") != "epoch17_one_turn_canary_cost_rejected"
        or predecessor.get("new_semantic_model_call_count") != 1
        or predecessor.get("semantic_retry_count") != 0
        or predecessor.get("new_unknown_usage_turn_count") != 0
        or predecessor.get("new_measured_usage", {}).get("total_tokens") != 93_868
        or predecessor.get("failed_checks")
        != ["measured_total_token_acceptance_ceiling"]
        or predecessor.get("diagnostic") is not None
        or predecessor.get("production_amortized_ratio") != 0.33943699948715533
        or predecessor.get("production_mutated") is not False
        or predecessor.get("holdout_authorized") is not False
        or predecessor_sidecar.get("usage", {}).get("total_tokens") != 62_381
        or predecessor_sidecar.get("thread_total_usage", {}).get("total_tokens")
        != 93_868
    ):
        raise CompactUnitPointerCanaryError("epoch-18 predecessor evidence drifted")
    request = _build_request()
    epoch17._validate_representability(request)  # noqa: SLF001


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
        "rejected_epoch17": {
            "receipt": _record(PREDECESSOR_ROOT / "plan-step-receipt.json"),
            "terminal": _record(PREDECESSOR_ROOT / "terminal.json"),
            "runtime_lock": _record(PREDECESSOR_ROOT / "runtime-lock.json"),
            "canonical_labels": _record(
                PREDECESSOR_ROOT / "turn/canonical-labels.private.json"
            ),
            "raw_output": _record(PREDECESSOR_ROOT / "turn/output.private.json"),
            "sidecar": _record(PREDECESSOR_ROOT / "turn/sidecar.json"),
        },
    }


def _runtime_module_paths() -> Sequence[Path]:
    return (
        Path(__file__),
        Path(adapter.__file__),
        Path(adapter.local.__file__),
        Path(adapter.local.bounded_span.__file__),
        Path(adapter.local.pointer.__file__),
        Path(adapter.base.__file__),
        Path(epoch13.__file__),
        Path(epoch15.__file__),
        Path(epoch17.__file__),
    )


def _validate_directive(value: Mapping[str, Any]) -> None:
    expected = json.loads(DIRECTIVE_PATH.read_text(encoding="utf-8"))
    if dict(value) != expected:
        raise CompactUnitPointerCanaryError("epoch-18 directive contract drifted")
    if (
        expected.get("schema_version")
        != "pif_evaluation_epoch18_compact_unit_pointer_canary_directive_v1"
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
        raise CompactUnitPointerCanaryError("epoch-18 directive invariants drifted")


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
        raise CompactUnitPointerCanaryError("epoch-18 pass violates production cost gate")
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
        "compact_unit_pointer_contract": True,
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
    stage="compact_unit_pointer_full_episode",
    architecture_class=(
        "one_turn_full_canonical_compact_positional_unit_pointer_extraction_v1"
    ),
    authorization_statement=AUTHORIZATION_STATEMENT,
    authorization_state="authorized_for_exactly_one_compact_unit_pointer_canary_turn",
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
    reject_next_action="reject_compact_unit_pointer_architecture_without_field_patch",
)


def _runtime_status(status: Mapping[str, Any], root: Path) -> dict[str, Any]:
    request = _load(
        root.expanduser().resolve() / "prepared-turn/request.private.json",
        "epoch-18 prepared request",
    )
    shape = _request_shape(request)
    if shape != _expected_shape():
        raise CompactUnitPointerCanaryError("epoch-18 runtime status shape drifted")
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
    "CompactUnitPointerCanaryError",
    "authorize_canary",
    "execute_canary",
    "prepare_canary",
    "status_canary",
    "verify_authorization",
    "verify_receipt",
    "verify_runtime",
)
