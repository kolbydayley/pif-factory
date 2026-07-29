from __future__ import annotations

"""Epoch-16 first-window canary for source-unit-owned canonical metrics."""

import argparse
import asyncio
import copy
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_bounded_span_canary_runtime as epoch13
from . import app_server_canonical_v31_metric_compiler_canary_runtime as epoch15
from . import app_server_canonical_v31_source_unit_owner_episode_batch as adapter
from . import app_server_one_turn_canary_runtime as one_turn


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROOT = (
    PROJECT_ROOT
    / "work/app-server-development-v2/unattended-pipeline-v5/"
    "canonical-v31-epoch16-source-unit-owner-window0-canary-v1"
)
SOURCE_ROOT = epoch13.DEFAULT_ROOT
PREDECESSOR_ROOT = epoch15.DEFAULT_ROOT
DIRECTIVE_PATH = (
    PROJECT_ROOT
    / "automation/pif-evaluation-epoch16-source-unit-owner-window-canary-v16.json"
)
PLAN_PATH = PROJECT_ROOT / "automation/pif-evaluation-semantic-plan-v16.json"
DIRECTIVE_SHA256 = "78fb3c03bdfb45e2ec940a42a777817a72e8d20b15157fb90c3f24889806c381"
THREAD_ID = "019f4cf1-c46e-7db3-acd2-bf03c4459a10"
PLAN_EPOCH = 16
STEP_ID = "canonical_v31_epoch16_source_unit_owner_window0_canary_v16"
TURN_NAME = "epoch16_source_unit_owner_window0_78fb3c03bdfb45e2ec940a42"
AUTHORIZATION_STATEMENT = (
    "You have my full permission to continue. No need to seek out my approval anymore."
)
AUTHORIZATION_ID_DEFAULT = "kolby-epoch16-source-unit-owner-window0-20260719"
MAXIMUM_WINDOW_TOTAL_TOKENS = 36_000
PROJECTED_TWO_WINDOW_TOTAL_TOKENS = 72_000
MAXIMUM_TWO_WINDOW_TOTAL_TOKENS = 73_926
MAXIMUM_WALL_SECONDS = 900
MINIMUM_REMAINING_RESERVE_PERCENT = 20
CAPACITY_SAFETY_MARGIN_PERCENT = 1
QUOTA_POINTS_PER_MILLION_TOKENS = 17
EXPECTED_PROMPT_BYTES = 22_737
EXPECTED_BASE_BYTES = 20_257
EXPECTED_SCHEMA_BYTES = 14_845
EXPECTED_SOURCE_UNIT_COUNT = 30
EXPECTED_EVIDENCE_SPAN_COUNT = 57
WINDOW0_SEGMENT_IDS = (
    "seg_155ba31ee473823b6e7e9504",
    "seg_a845b306d731d9728a5b0fa0",
    "seg_80fe585badf8f3d3597d0f96",
)
WINDOW1_SEGMENT_IDS = (
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
PREDECESSOR_RECEIPT_SHA256 = (
    "4fe9baaa16410d4021ae6f7c43b242c3469a7977bf2802169b0c673889cf5ebb"
)
PRODUCTION_FIXED_TOKENS = 600_538
PRODUCTION_MULTIPLIER = 30
PRODUCTION_DENOMINATOR_TOKENS = 10_065_426
PRODUCTION_RATIO_GATE = 0.28

SourceUnitOwnerCanaryError = one_turn.OneTurnCanaryError
SourceUnitOwnerCanaryWaiting = one_turn.OneTurnCanaryWaiting


def _load(path: Path, label: str) -> dict[str, Any]:
    return one_turn._load(path, label)  # noqa: SLF001


def _record(path: Path) -> dict[str, Any]:
    return one_turn._record(path)  # noqa: SLF001


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
        for segment in source["private_input"]["segments"][:3]
    ]
    requests = adapter.prepare_episode_batches(
        episode, batch_size=3, thread_mode="new_thread"
    )
    if len(requests) != 1:
        raise SourceUnitOwnerCanaryError("epoch-16 request count drifted")
    request = requests[0]
    unit_count = sum(
        len(segment["units"]) for segment in request["private_input"]["segments"]
    )
    span_count = sum(
        len(segment["evidence_spans"])
        for segment in request["private_input"]["segments"]
    )
    schema_bytes = len(
        one_turn._canonical_json(request["output_schema"]).encode("utf-8")  # noqa: SLF001
    )
    if (
        tuple(request.get("segment_ids", ())) != WINDOW0_SEGMENT_IDS
        or request.get("effective_batch_size") != 3
        or len(request["prompt"].encode("utf-8")) != EXPECTED_PROMPT_BYTES
        or len(request["base_instructions"].encode("utf-8")) != EXPECTED_BASE_BYTES
        or schema_bytes != EXPECTED_SCHEMA_BYTES
        or unit_count != EXPECTED_SOURCE_UNIT_COUNT
        or span_count != EXPECTED_EVIDENCE_SPAN_COUNT
    ):
        raise SourceUnitOwnerCanaryError("epoch-16 frozen window shape drifted")
    adapter.validate_prepared_request(request)
    return request


def _validate_predecessor(full_verify: bool) -> None:
    source_receipt_path = SOURCE_ROOT / "plan-step-receipt.json"
    predecessor_receipt_path = PREDECESSOR_ROOT / "plan-step-receipt.json"
    source_receipt = (
        epoch13.verify_receipt(SOURCE_ROOT)
        if full_verify
        else _load(source_receipt_path, "epoch-13 source receipt")
    )
    predecessor = (
        epoch15.verify_receipt(PREDECESSOR_ROOT)
        if full_verify
        else _load(predecessor_receipt_path, "epoch-15 receipt")
    )
    if (
        _record(source_receipt_path)["sha256"] != SOURCE_RECEIPT_SHA256
        or _record(SOURCE_ROOT / "terminal.json")["sha256"]
        != SOURCE_RECEIPT_SHA256
        or _record(SOURCE_ROOT / "prepared-turn/request.private.json")["sha256"]
        != SOURCE_REQUEST_SHA256
        or source_receipt.get("state") != "rejected"
        or source_receipt.get("new_measured_usage", {}).get("total_tokens") != 61_072
        or source_receipt.get("production_mutated") is not False
        or _record(predecessor_receipt_path)["sha256"]
        != PREDECESSOR_RECEIPT_SHA256
        or _record(PREDECESSOR_ROOT / "terminal.json")["sha256"]
        != PREDECESSOR_RECEIPT_SHA256
        or predecessor.get("state") != "rejected"
        or predecessor.get("terminal_reason") != "epoch15_one_turn_canary_cost_rejected"
        or predecessor.get("new_semantic_model_call_count") != 1
        or predecessor.get("semantic_retry_count") != 0
        or predecessor.get("new_unknown_usage_turn_count") != 0
        or predecessor.get("compiler_measured_total_tokens") != 17_453
        or predecessor.get("combined_extraction_and_compiler_total_tokens") != 78_525
        or predecessor.get("production_amortized_ratio")
        != 0.29370719133000434
        or predecessor.get("failed_checks")
        != ["measured_total_token_acceptance_ceiling"]
        or predecessor.get("diagnostic") is not None
        or predecessor.get("production_mutated") is not False
        or predecessor.get("holdout_authorized") is not False
    ):
        raise SourceUnitOwnerCanaryError("epoch-16 predecessor evidence drifted")
    _build_request()


def _predecessor_records() -> Mapping[str, Any]:
    return {
        "source_authority": {
            "receipt": _record(SOURCE_ROOT / "plan-step-receipt.json"),
            "terminal": _record(SOURCE_ROOT / "terminal.json"),
            "runtime_lock": _record(SOURCE_ROOT / "runtime-lock.json"),
            "prepared_request": _record(
                SOURCE_ROOT / "prepared-turn/request.private.json"
            ),
        },
        "rejected_epoch15": {
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
        Path(adapter.base.__file__),
        Path(epoch13.__file__),
        Path(epoch15.__file__),
    )


def _validate_directive(value: Mapping[str, Any]) -> None:
    authorization = value.get("authorization_contract")
    predecessor = value.get("predecessor_contract")
    architecture = value.get("architecture_contract")
    canary = value.get("canary_contract")
    promotion = value.get("promotion_contract")
    transport = value.get("transport_contract")
    forbidden = (
        "deterministic_semantic_pruning",
        "deterministic_support_filtering",
        "deterministic_deduplication",
        "deterministic_relabeling",
        "semantic_regex_or_keyword_rules",
    )
    if (
        value.get("schema_version")
        != "pif_evaluation_epoch16_source_unit_owner_window_canary_directive_v1"
        or value.get("thread_id") != THREAD_ID
        or value.get("plan_epoch") != PLAN_EPOCH
        or value.get("step_id") != STEP_ID
        or value.get("state") != "ready_for_direct_user_authorized_execute"
        or value.get("expected_receipt_path")
        != str((DEFAULT_ROOT / "plan-step-receipt.json").resolve())
        or not isinstance(authorization, Mapping)
        or authorization.get("authority") != "direct_user_instruction"
        or authorization.get("authorized_by") != "kolby"
        or authorization.get("operator_authorization_statement")
        != AUTHORIZATION_STATEMENT
        or authorization.get("additional_interactive_approval_required") is not False
        or not isinstance(predecessor, Mapping)
        or predecessor.get("epoch15_receipt_sha256") != PREDECESSOR_RECEIPT_SHA256
        or predecessor.get("epoch15_state") != "rejected"
        or predecessor.get("epoch15_compiler_total_tokens") != 17_453
        or predecessor.get("epoch15_combined_total_tokens") != 78_525
        or predecessor.get("epoch15_production_amortized_ratio")
        != 0.29370719133000434
        or predecessor.get("epoch15_semantic_projection_passed") is not True
        or predecessor.get("epoch15_completed_turn_replay_allowed") is not False
        or predecessor.get("source_request_sha256") != SOURCE_REQUEST_SHA256
        or predecessor.get("prior_semantic_output_reuse_allowed") is not False
        or not isinstance(architecture, Mapping)
        or architecture.get("architecture_class")
        != "two_window_full_canonical_source_unit_owner_extraction_v1"
        or architecture.get("materially_distinct_from_posthoc_metric_compiler")
        is not True
        or architecture.get("exact_window_count") != 2
        or architecture.get("exact_segments_per_window") != 3
        or architecture.get("window0_segment_ids") != list(WINDOW0_SEGMENT_IDS)
        or architecture.get("window1_segment_ids") != list(WINDOW1_SEGMENT_IDS)
        or architecture.get("window0_is_smallest_decision_changing_canary") is not True
        or architecture.get("complete_canonical_ai_discourse_v3_1_in_each_turn")
        is not True
        or architecture.get(
            "model_selects_start_and_end_source_unit_for_every_nonnull_metric_literal"
        )
        is not True
        or architecture.get(
            "model_copies_metric_literals_exactly_from_selected_source_unit_span"
        )
        is not True
        or architecture.get("deterministic_exact_substring_and_offset_projection_only")
        is not True
        or architecture.get("deterministic_coverage_owner_count_reconciliation_only")
        is not True
        or architecture.get("structural_concatenation_only_after_two_windows") is not True
        or any(architecture.get(field) is not False for field in forbidden)
        or not isinstance(canary, Mapping)
        or canary.get("model") != adapter.MODEL
        or canary.get("effort") != adapter.EFFORT
        or canary.get("thread_mode") != "new_thread"
        or canary.get("configured_batch_size") != 3
        or canary.get("effective_batch_size") != 3
        or canary.get("new_model_call_cap") != 1
        or canary.get("semantic_retry_count") != 0
        or canary.get("measured_window0_total_token_acceptance_ceiling")
        != MAXIMUM_WINDOW_TOTAL_TOKENS
        or canary.get("projected_two_window_total_token_ceiling")
        != PROJECTED_TWO_WINDOW_TOTAL_TOKENS
        or canary.get("maximum_full_two_window_tokens_below_ratio_gate")
        != MAXIMUM_TWO_WINDOW_TOTAL_TOKENS
        or canary.get("maximum_wall_seconds") != MAXIMUM_WALL_SECONDS
        or canary.get("minimum_remaining_reserve_percent")
        != MINIMUM_REMAINING_RESERVE_PERCENT
        or canary.get("capacity_safety_margin_percent")
        != CAPACITY_SAFETY_MARGIN_PERCENT
        or canary.get("quota_points_per_million_tokens")
        != QUOTA_POINTS_PER_MILLION_TOKENS
        or canary.get("frozen_prompt_bytes") != EXPECTED_PROMPT_BYTES
        or canary.get("frozen_base_instruction_bytes") != EXPECTED_BASE_BYTES
        or canary.get("frozen_output_schema_bytes") != EXPECTED_SCHEMA_BYTES
        or canary.get("frozen_source_unit_count") != EXPECTED_SOURCE_UNIT_COUNT
        or canary.get("frozen_evidence_span_count") != EXPECTED_EVIDENCE_SPAN_COUNT
        or canary.get("production_amortized_ratio_gate") != PRODUCTION_RATIO_GATE
        or not isinstance(promotion, Mapping)
        or promotion.get("pass_requires_complete_canonical_projection") is not True
        or promotion.get("pass_requires_exact_evidence_rate") != 1.0
        or promotion.get("pass_requires_all_metric_literals_exactly_owned_and_projected")
        is not True
        or promotion.get("pass_requires_complete_managed_auth_usage") is not True
        or promotion.get("pass_requires_window0_total_tokens_at_most_36000") is not True
        or promotion.get("pass_authorizes_only_frozen_window1_with_remaining_token_cap")
        is not True
        or promotion.get("window1_remaining_cap_formula")
        != "73926 - measured_window0_total_tokens"
        or promotion.get(
            "failure_rejects_source_unit_owner_window_architecture_without_field_patch"
        )
        is not True
        or promotion.get("winner_frozen") is not False
        or promotion.get("quality_authorized") is not False
        or promotion.get("holdout_authorized") is not False
        or promotion.get("production_mutation_allowed") is not False
        or not isinstance(transport, Mapping)
        or transport.get("transport")
        != "official_codex_app_server_stdio_managed_chatgpt_auth"
        or transport.get("managed_chatgpt_plan_type") != "pro"
        or transport.get("pinned_codex_cli_version") != "0.144.1"
        or transport.get("persistent_app_server_process_count") != 1
        or transport.get("api_key_billing_allowed") is not False
        or transport.get("raw_session_token_access_or_replay_allowed") is not False
        or transport.get("codex_exec_semantic_work_allowed") is not False
    ):
        raise SourceUnitOwnerCanaryError("epoch-16 directive contract drifted")


def _production_ratio(tokens: int) -> float:
    return (
        PRODUCTION_FIXED_TOKENS + tokens * PRODUCTION_MULTIPLIER
    ) / PRODUCTION_DENOMINATOR_TOKENS


def _receipt_metadata(outcome: Mapping[str, Any]) -> Mapping[str, Any]:
    window_tokens = int(outcome["usage"]["total_tokens"])
    complete = (
        outcome["semantic_model_call_count"] == 1
        and outcome["unknown_usage_turn_count"] == 0
    )
    projected = window_tokens * 2 if complete else None
    ratio = _production_ratio(projected) if projected is not None else None
    gate = {
        "window0_usage_complete": complete,
        "window0_total_tokens_at_most_36000": (
            complete and window_tokens <= MAXIMUM_WINDOW_TOTAL_TOKENS
        ),
        "projected_two_window_total_tokens_at_most_72000": (
            projected is not None and projected <= PROJECTED_TWO_WINDOW_TOTAL_TOKENS
        ),
        "projected_production_amortized_ratio_below_0_28": (
            ratio is not None and ratio < PRODUCTION_RATIO_GATE
        ),
    }
    if outcome["state"] == "passed" and not all(gate.values()):
        raise SourceUnitOwnerCanaryError("epoch-16 pass violates projected cost gate")
    return {
        "window_index": 0,
        "window_segment_ids": list(WINDOW0_SEGMENT_IDS),
        "measured_window0_total_tokens": window_tokens,
        "projected_two_window_total_tokens": projected,
        "projected_production_amortized_ratio": ratio,
        "projected_cost_gate": gate,
        "maximum_window1_total_tokens_after_pass": (
            MAXIMUM_TWO_WINDOW_TOTAL_TOKENS - window_tokens if complete else None
        ),
        "prior_semantic_output_reuse_count": 0,
        "metric_source_unit_span_owner_contract": True,
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
    stage="source_unit_owner_window0",
    architecture_class="two_window_full_canonical_source_unit_owner_extraction_v1",
    authorization_statement=AUTHORIZATION_STATEMENT,
    authorization_state="authorized_for_exactly_one_source_unit_owner_window0_turn",
    model=adapter.MODEL,
    effort=adapter.EFFORT,
    maximum_total_tokens=MAXIMUM_WINDOW_TOTAL_TOKENS,
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
    pass_next_action="freeze_source_unit_owner_window1_with_remaining_token_cap",
    reject_next_action="reject_source_unit_owner_window_architecture_without_field_patch",
)


def prepare_canary(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    return one_turn.prepare(SPEC, root)


def verify_runtime(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    return one_turn.verify_runtime(SPEC, root)


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
    "SourceUnitOwnerCanaryError",
    "authorize_canary",
    "execute_canary",
    "prepare_canary",
    "status_canary",
    "verify_authorization",
    "verify_receipt",
    "verify_runtime",
)
