from __future__ import annotations

"""Epoch-15 one-turn canary for the bounded LLM metric compiler."""

import argparse
import asyncio
import copy
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_bounded_span_canary_runtime as epoch13
from . import app_server_canonical_v31_epoch14_terminal_recovery as epoch14_recovery
from . import app_server_canonical_v31_metric_compiler as adapter
from . import app_server_one_turn_canary_runtime as one_turn


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROOT = (
    PROJECT_ROOT
    / "work/app-server-development-v2/unattended-pipeline-v5/"
    "canonical-v31-epoch15-metric-compiler-canary-v1"
)
SOURCE_ROOT = epoch13.DEFAULT_ROOT
EPOCH14_ROOT = epoch14_recovery.ROOT
DIRECTIVE_PATH = (
    PROJECT_ROOT / "automation/pif-evaluation-epoch15-metric-compiler-canary-v15.json"
)
PLAN_PATH = PROJECT_ROOT / "automation/pif-evaluation-semantic-plan-v15.json"
DIRECTIVE_SHA256 = "f784ddf0d904efded912601a6a5d58e030c46765132a59f7ab2bff05116fe101"
THREAD_ID = "019f4cf1-c46e-7db3-acd2-bf03c4459a10"
PLAN_EPOCH = 15
STEP_ID = "canonical_v31_epoch15_metric_compiler_canary_v15"
TURN_NAME = "epoch15_metric_compiler_canary_f784ddf0d904efded912601a"
AUTHORIZATION_STATEMENT = (
    "You have my full permission to continue. No need to seek out my approval anymore."
)
AUTHORIZATION_ID_DEFAULT = "kolby-epoch15-metric-compiler-20260719"
MAXIMUM_COMPILER_TOTAL_TOKENS = 12_000
SOURCE_EXTRACTION_TOTAL_TOKENS = 61_072
MAXIMUM_COMBINED_TOTAL_TOKENS = 73_072
MAXIMUM_WALL_SECONDS = 600
MINIMUM_REMAINING_RESERVE_PERCENT = 20
CAPACITY_SAFETY_MARGIN_PERCENT = 1
QUOTA_POINTS_PER_MILLION_TOKENS = 17
EXPECTED_PROMPT_BYTES = 22_389
EXPECTED_SCHEMA_BYTES = 1_415
EXPECTED_LITERAL_TOKEN_COUNT = 2_423
EXPECTED_APPLICABLE_CASE_COUNT = 28
EXPECTED_EXACT_PRESERVED_CASE_COUNT = 12
EXPECTED_DISPUTED_CASE_COUNT = 16
PRODUCTION_FIXED_TOKENS = 600_538
PRODUCTION_MULTIPLIER = 30
PRODUCTION_DENOMINATOR_TOKENS = 10_065_426
PRODUCTION_RATIO_GATE = 0.28
PRODUCTION_RATIO_AT_COMBINED_CEILING = (
    PRODUCTION_FIXED_TOKENS
    + MAXIMUM_COMBINED_TOTAL_TOKENS * PRODUCTION_MULTIPLIER
) / PRODUCTION_DENOMINATOR_TOKENS
SOURCE_RECEIPT_SHA256 = (
    "26c7f4870adc029719b097e82306c80f7b052b01faa4467fcd396816d43b7c60"
)
SOURCE_REQUEST_SHA256 = (
    "7f2e23b294164e0a530435f4ef00719f0e9f63754d3fb054460dc412cca53f6f"
)
SOURCE_OUTPUT_SHA256 = (
    "202c0cd1d9de66b7b999e49f3ac5d094886dcb32bf8845a22deda0f8f9a8f418"
)
SOURCE_RUNTIME_LOCK_SHA256 = (
    "06187d8e69262f87d38049ba459b2f229eaaae939d80b3bf41a073a8db8867de"
)
EPOCH14_RECEIPT_SHA256 = (
    "4d273a216aaa932c7e37b228e7997c549795c4cbb0f56a73dc47857712c6d1ac"
)
EPOCH14_RECOVERY_SHA256 = (
    "927da8b4f9c41005f22aa78a9bf3dfbcb699982d284746d9328c21ee350eeabd"
)
EPOCH14_REJECTION_SHA256 = (
    "0fa377ce652f16f154a95fd962a3cbbc3bd71d3ce1b2bffb8a1ab44e0e39ede7"
)

MetricCompilerCanaryError = one_turn.OneTurnCanaryError
MetricCompilerCanaryWaiting = one_turn.OneTurnCanaryWaiting


def _load(path: Path, label: str) -> dict[str, Any]:
    return one_turn._load(path, label)  # noqa: SLF001


def _record(path: Path) -> dict[str, Any]:
    return one_turn._record(path)  # noqa: SLF001


def _build_request() -> dict[str, Any]:
    source_request = _load(
        SOURCE_ROOT / "prepared-turn/request.private.json",
        "epoch-13 prepared request",
    )
    source_output = _load(
        SOURCE_ROOT / "turn/output.private.json",
        "epoch-13 raw output",
    )
    request = adapter.prepare_request(source_request, source_output)
    literal_token_count = sum(
        len(case["literal_tokens"])
        for case in request["private_input"]["metric_cases"]
    )
    integrity = request["semantic_integrity"]
    if (
        request.get("effective_batch_size") != EXPECTED_DISPUTED_CASE_COUNT
        or request.get("segment_ids") != source_request.get("segment_ids")
        or len(request["prompt"].encode("utf-8")) != EXPECTED_PROMPT_BYTES
        or len(
            one_turn._canonical_json(request["output_schema"]).encode("utf-8")  # noqa: SLF001
        )
        != EXPECTED_SCHEMA_BYTES
        or literal_token_count != EXPECTED_LITERAL_TOKEN_COUNT
        or integrity.get("applicable_metric_case_count")
        != EXPECTED_APPLICABLE_CASE_COUNT
        or integrity.get("exact_metric_case_count_preserved")
        != EXPECTED_EXACT_PRESERVED_CASE_COUNT
        or integrity.get("llm_reauthors_every_disputed_metric_case") is not True
    ):
        raise MetricCompilerCanaryError("epoch-15 frozen compiler sample drifted")
    adapter.validate_prepared_request(request)
    return request


def _validate_predecessor(full_verify: bool) -> None:
    source_receipt_path = SOURCE_ROOT / "plan-step-receipt.json"
    epoch14_receipt_path = EPOCH14_ROOT / "plan-step-receipt.json"
    source_receipt = (
        epoch13.verify_receipt(SOURCE_ROOT)
        if full_verify
        else _load(source_receipt_path, "epoch-13 receipt")
    )
    epoch14_receipt = (
        epoch14_recovery.verify(EPOCH14_ROOT)
        if full_verify
        else _load(epoch14_receipt_path, "epoch-14 recovered receipt")
    )
    if (
        _record(source_receipt_path)["sha256"] != SOURCE_RECEIPT_SHA256
        or _record(SOURCE_ROOT / "terminal.json")["sha256"]
        != SOURCE_RECEIPT_SHA256
        or _record(SOURCE_ROOT / "prepared-turn/request.private.json")["sha256"]
        != SOURCE_REQUEST_SHA256
        or _record(SOURCE_ROOT / "turn/output.private.json")["sha256"]
        != SOURCE_OUTPUT_SHA256
        or _record(SOURCE_ROOT / "runtime-lock.json")["sha256"]
        != SOURCE_RUNTIME_LOCK_SHA256
        or source_receipt.get("state") != "rejected"
        or source_receipt.get("terminal_reason")
        != "epoch13_bounded_span_canary_semantic_output_rejected"
        or source_receipt.get("new_semantic_model_call_count") != 1
        or source_receipt.get("semantic_retry_count") != 0
        or source_receipt.get("new_unknown_usage_turn_count") != 0
        or source_receipt.get("new_measured_usage", {}).get("total_tokens")
        != SOURCE_EXTRACTION_TOTAL_TOKENS
        or source_receipt.get("failed_checks") != ["semantic_output_validity"]
        or source_receipt.get("production_mutated") is not False
        or source_receipt.get("holdout_authorized") is not False
        or _record(epoch14_receipt_path)["sha256"] != EPOCH14_RECEIPT_SHA256
        or _record(EPOCH14_ROOT / "terminal.json")["sha256"]
        != EPOCH14_RECEIPT_SHA256
        or _record(EPOCH14_ROOT / "terminal-accounting-recovery.json")["sha256"]
        != EPOCH14_RECOVERY_SHA256
        or _record(EPOCH14_ROOT / "semantic-rejection.json")["sha256"]
        != EPOCH14_REJECTION_SHA256
        or epoch14_receipt.get("state") != "rejected"
        or epoch14_receipt.get("terminal_reason")
        != (
            "epoch14_literal_pointer_canary_cost_and_semantic_output_rejected_"
            "after_accounting_recovery"
        )
        or epoch14_receipt.get("new_semantic_model_call_count") != 1
        or epoch14_receipt.get("recovery_semantic_model_call_count") != 0
        or epoch14_receipt.get("semantic_retry_count") != 0
        or epoch14_receipt.get("new_unknown_usage_turn_count") != 0
        or epoch14_receipt.get("new_measured_usage", {}).get("total_tokens")
        != 443_532
        or epoch14_receipt.get("failed_checks")
        != ["measured_total_token_acceptance_ceiling", "semantic_output_validity"]
        or epoch14_receipt.get("diagnostic", {}).get("diagnostic_path")
        != "unit coverage receipt counts do not reconcile"
        or epoch14_receipt.get("production_mutated") is not False
        or epoch14_receipt.get("holdout_authorized") is not False
    ):
        raise MetricCompilerCanaryError("epoch-15 predecessor evidence drifted")
    _build_request()


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
            "sidecar": _record(SOURCE_ROOT / "turn/sidecar.json"),
        },
        "rejected_epoch14": {
            "receipt": _record(EPOCH14_ROOT / "plan-step-receipt.json"),
            "terminal": _record(EPOCH14_ROOT / "terminal.json"),
            "runtime_lock": _record(EPOCH14_ROOT / "runtime-lock.json"),
            "terminal_accounting_recovery": _record(
                EPOCH14_ROOT / "terminal-accounting-recovery.json"
            ),
            "semantic_rejection": _record(EPOCH14_ROOT / "semantic-rejection.json"),
            "raw_output": _record(EPOCH14_ROOT / "turn/output.private.json"),
            "sidecar": _record(EPOCH14_ROOT / "turn/sidecar.json"),
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
        Path(epoch14_recovery.__file__),
    )


def _validate_directive(value: Mapping[str, Any]) -> None:
    authorization = value.get("authorization_contract")
    predecessor = value.get("predecessor_contract")
    architecture = value.get("architecture_contract")
    canary = value.get("canary_contract")
    promotion = value.get("promotion_contract")
    transport = value.get("transport_contract")
    forbidden_false = (
        "deterministic_semantic_pruning",
        "deterministic_support_filtering",
        "deterministic_deduplication",
        "deterministic_relabeling",
        "semantic_regex_or_keyword_rules",
    )
    if (
        value.get("schema_version")
        != "pif_evaluation_epoch15_metric_compiler_canary_directive_v1"
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
        or predecessor.get("epoch14_receipt_sha256") != EPOCH14_RECEIPT_SHA256
        or predecessor.get("epoch14_state") != "rejected"
        or predecessor.get("epoch14_total_tokens") != 443_532
        or predecessor.get("epoch14_semantic_output_error")
        != "unit coverage receipt counts do not reconcile"
        or predecessor.get("epoch14_completed_turn_replay_allowed") is not False
        or predecessor.get("source_extraction_total_tokens")
        != SOURCE_EXTRACTION_TOTAL_TOKENS
        or predecessor.get("source_extraction_replay_allowed") is not False
        or not isinstance(architecture, Mapping)
        or architecture.get("architecture_class") != adapter.COMPILER_ARCHITECTURE
        or architecture.get("materially_distinct_from_epoch14") is not True
        or architecture.get("source_extraction_reused_without_replay") is not True
        or architecture.get("applicable_metric_case_count")
        != EXPECTED_APPLICABLE_CASE_COUNT
        or architecture.get("exact_metric_case_count_preserved")
        != EXPECTED_EXACT_PRESERVED_CASE_COUNT
        or architecture.get("llm_reauthored_disputed_metric_case_count")
        != EXPECTED_DISPUTED_CASE_COUNT
        or architecture.get("model_selects_local_exact_metric_literal_token_ranges")
        is not True
        or architecture.get("deterministic_exact_token_projection_only") is not True
        or architecture.get("deterministic_coverage_owner_count_reconciliation_only")
        is not True
        or any(architecture.get(field) is not False for field in forbidden_false)
        or not isinstance(canary, Mapping)
        or canary.get("model") != adapter.MODEL
        or canary.get("effort") != adapter.EFFORT
        or canary.get("thread_mode") != "new_thread"
        or canary.get("effective_batch_size") != EXPECTED_DISPUTED_CASE_COUNT
        or canary.get("new_model_call_cap") != 1
        or canary.get("semantic_retry_count") != 0
        or canary.get("measured_compiler_total_token_acceptance_ceiling")
        != MAXIMUM_COMPILER_TOTAL_TOKENS
        or canary.get("source_extraction_total_tokens")
        != SOURCE_EXTRACTION_TOTAL_TOKENS
        or canary.get("combined_extraction_and_compiler_token_ceiling")
        != MAXIMUM_COMBINED_TOTAL_TOKENS
        or canary.get("maximum_wall_seconds") != MAXIMUM_WALL_SECONDS
        or canary.get("minimum_remaining_reserve_percent")
        != MINIMUM_REMAINING_RESERVE_PERCENT
        or canary.get("capacity_safety_margin_percent")
        != CAPACITY_SAFETY_MARGIN_PERCENT
        or canary.get("quota_points_per_million_tokens")
        != QUOTA_POINTS_PER_MILLION_TOKENS
        or canary.get("frozen_prompt_bytes") != EXPECTED_PROMPT_BYTES
        or canary.get("frozen_literal_token_count") != EXPECTED_LITERAL_TOKEN_COUNT
        or canary.get("production_amortized_ratio_formula")
        != "(600538 + combined_tokens * 30) / 10065426"
        or canary.get("production_amortized_ratio_at_combined_ceiling")
        != 0.277455
        or canary.get("production_amortized_ratio_gate") != PRODUCTION_RATIO_GATE
        or not isinstance(promotion, Mapping)
        or promotion.get("pass_requires_complete_canonical_projection") is not True
        or promotion.get("pass_requires_exact_evidence_rate") != 1.0
        or promotion.get("pass_requires_all_compiled_metric_literals_exactly_projected")
        is not True
        or promotion.get("pass_requires_complete_managed_auth_usage") is not True
        or promotion.get("pass_requires_compiler_total_tokens_at_most_12000") is not True
        or promotion.get("pass_requires_combined_total_tokens_at_most_73072") is not True
        or promotion.get("pass_requires_production_amortized_ratio_below_0_28")
        is not True
        or promotion.get(
            "pass_authorizes_only_fresh_six_arm_two_pass_development_matrix_freeze"
        )
        is not True
        or promotion.get("failure_rejects_two_pass_compiler_without_isolated_field_patch")
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
        raise MetricCompilerCanaryError("epoch-15 directive contract drifted")


def _production_ratio(combined_tokens: int) -> float:
    return (
        PRODUCTION_FIXED_TOKENS + combined_tokens * PRODUCTION_MULTIPLIER
    ) / PRODUCTION_DENOMINATOR_TOKENS


def _receipt_metadata(outcome: Mapping[str, Any]) -> Mapping[str, Any]:
    compiler_tokens = int(outcome["usage"]["total_tokens"])
    usage_complete = (
        outcome["semantic_model_call_count"] == 1
        and outcome["unknown_usage_turn_count"] == 0
    )
    combined_tokens = (
        SOURCE_EXTRACTION_TOTAL_TOKENS + compiler_tokens if usage_complete else None
    )
    ratio = _production_ratio(combined_tokens) if combined_tokens is not None else None
    cost_gate = {
        "compiler_usage_complete": usage_complete,
        "compiler_total_tokens_at_most_12000": (
            usage_complete and compiler_tokens <= MAXIMUM_COMPILER_TOTAL_TOKENS
        ),
        "combined_total_tokens_at_most_73072": (
            combined_tokens is not None
            and combined_tokens <= MAXIMUM_COMBINED_TOTAL_TOKENS
        ),
        "production_amortized_ratio_below_0_28": (
            ratio is not None and ratio < PRODUCTION_RATIO_GATE
        ),
    }
    if outcome["state"] == "passed" and not all(cost_gate.values()):
        raise MetricCompilerCanaryError("epoch-15 pass violates combined cost gate")
    return {
        "source_extraction_total_tokens": SOURCE_EXTRACTION_TOTAL_TOKENS,
        "source_extraction_replay_count": 0,
        "compiler_measured_total_tokens": compiler_tokens,
        "combined_extraction_and_compiler_total_tokens": combined_tokens,
        "production_amortized_ratio": ratio,
        "production_cost_gate": cost_gate,
        "production_amortized_ratio_formula": (
            "(600538 + combined_tokens * 30) / 10065426"
        ),
        "compiler_metric_case_counts": {
            "applicable": EXPECTED_APPLICABLE_CASE_COUNT,
            "exact_preserved": EXPECTED_EXACT_PRESERVED_CASE_COUNT,
            "llm_reauthored": EXPECTED_DISPUTED_CASE_COUNT,
        },
        "compiler_prompt_bytes": EXPECTED_PROMPT_BYTES,
        "compiler_literal_token_count": EXPECTED_LITERAL_TOKEN_COUNT,
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
    stage="bounded_metric_compiler",
    architecture_class=adapter.COMPILER_ARCHITECTURE,
    authorization_statement=AUTHORIZATION_STATEMENT,
    authorization_state="authorized_for_exactly_one_metric_compiler_canary_turn",
    model=adapter.MODEL,
    effort=adapter.EFFORT,
    maximum_total_tokens=MAXIMUM_COMPILER_TOTAL_TOKENS,
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
    pass_next_action="freeze_fresh_six_arm_two_pass_development_matrix",
    reject_next_action="reject_two_pass_metric_compiler_without_field_patch",
)


def prepare_canary(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    return _compiler_runtime_status(one_turn.prepare(SPEC, root), root)


def verify_runtime(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    return _compiler_runtime_status(one_turn.verify_runtime(SPEC, root), root)


def _compiler_runtime_status(
    status: Mapping[str, Any], root: Path
) -> dict[str, Any]:
    request = _load(
        root.expanduser().resolve() / "prepared-turn/request.private.json",
        "epoch-15 prepared request",
    )
    literal_token_count = sum(
        len(case["literal_tokens"])
        for case in request["private_input"]["metric_cases"]
    )
    canonical_schema_bytes = len(
        one_turn._canonical_json(request["output_schema"]).encode("utf-8")  # noqa: SLF001
    )
    if (
        literal_token_count != EXPECTED_LITERAL_TOKEN_COUNT
        or canonical_schema_bytes != EXPECTED_SCHEMA_BYTES
    ):
        raise MetricCompilerCanaryError("epoch-15 compiler status shape drifted")
    return {
        **copy.deepcopy(dict(status)),
        "literal_token_count": literal_token_count,
        "compiler_literal_token_count": literal_token_count,
        "canonical_schema_bytes": canonical_schema_bytes,
    }


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
    "MetricCompilerCanaryError",
    "authorize_canary",
    "execute_canary",
    "prepare_canary",
    "status_canary",
    "verify_authorization",
    "verify_receipt",
    "verify_runtime",
)
