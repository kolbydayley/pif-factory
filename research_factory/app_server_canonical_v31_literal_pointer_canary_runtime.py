from __future__ import annotations

"""Epoch-14 one-turn canary for exact metric-literal token pointers."""

import argparse
import asyncio
import copy
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_bounded_span_canary_runtime as epoch13
from . import app_server_canonical_v31_literal_pointer_episode_batch as adapter
from . import app_server_one_turn_canary_runtime as one_turn


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROOT = (
    PROJECT_ROOT
    / "work/app-server-development-v2/unattended-pipeline-v5/"
    "canonical-v31-epoch14-literal-pointer-canary-v1"
)
PREDECESSOR_ROOT = epoch13.DEFAULT_ROOT
DIRECTIVE_PATH = (
    PROJECT_ROOT / "automation/pif-evaluation-epoch14-literal-pointer-canary-v14.json"
)
PLAN_PATH = PROJECT_ROOT / "automation/pif-evaluation-semantic-plan-v14.json"
DIRECTIVE_SHA256 = "540dddc13b03a4a4273c636b3ff8c48465482cd98f7a83916cbe3739c717abc0"
THREAD_ID = "019f4cf1-c46e-7db3-acd2-bf03c4459a10"
PLAN_EPOCH = 14
STEP_ID = "canonical_v31_epoch14_literal_pointer_canary_v14"
TURN_NAME = "epoch14_literal_pointer_canary_6f0779f9233160dc06d14b36"
AUTHORIZATION_STATEMENT = (
    "You have my full permission to continue. No need to seek out my approval anymore."
)
AUTHORIZATION_ID_DEFAULT = "kolby-epoch14-literal-pointer-20260719"
MAXIMUM_TOTAL_TOKENS = 90_000
MAXIMUM_WALL_SECONDS = 900
MINIMUM_REMAINING_RESERVE_PERCENT = 20
CAPACITY_SAFETY_MARGIN_PERCENT = 1
QUOTA_POINTS_PER_MILLION_TOKENS = 17
EXPECTED_PROMPT_BYTES = 84_740
EXPECTED_LITERAL_TOKEN_COUNT = 6_044
PREDECESSOR_RECEIPT_SHA256 = (
    "26c7f4870adc029719b097e82306c80f7b052b01faa4467fcd396816d43b7c60"
)
PREDECESSOR_OUTPUT_SHA256 = (
    "202c0cd1d9de66b7b999e49f3ac5d094886dcb32bf8845a22deda0f8f9a8f418"
)
EXPECTED_TAXONOMY = {
    "raw_text": 16,
    "value": 3,
    "unit": 0,
    "comparator": 1,
}

LiteralPointerCanaryError = one_turn.OneTurnCanaryError
LiteralPointerCanaryWaiting = one_turn.OneTurnCanaryWaiting


def _load(path: Path, label: str) -> dict[str, Any]:
    return one_turn._load(path, label)  # noqa: SLF001


def _record(path: Path) -> dict[str, Any]:
    return one_turn._record(path)  # noqa: SLF001


def _metric_literal_taxonomy() -> dict[str, int]:
    request = _load(
        PREDECESSOR_ROOT / "prepared-turn/request.private.json",
        "epoch-13 prepared request",
    )
    output = _load(
        PREDECESSOR_ROOT / "turn/output.private.json",
        "epoch-13 raw output",
    )
    failures = {field: 0 for field in ("raw_text", "value", "unit", "comparator")}
    if len(output.get("segments", [])) != len(request["private_input"]["segments"]):
        raise LiteralPointerCanaryError("epoch-13 taxonomy segment cardinality drifted")
    for raw_segment, source in zip(
        output["segments"], request["private_input"]["segments"]
    ):
        if raw_segment.get("segment_id") != source.get("segment_id"):
            raise LiteralPointerCanaryError("epoch-13 taxonomy segment order drifted")
        spans = {span["evidence_span_id"]: span for span in source["evidence_spans"]}
        for event in raw_segment.get("discourse_events", []):
            span = spans.get(event.get("evidence_span_id"))
            if span is None:
                raise LiteralPointerCanaryError("epoch-13 evidence span lineage drifted")
            evidence = source["segment_text"][span["start_char"] : span["end_char"]]
            metric = event.get("metric")
            if not isinstance(metric, Mapping):
                raise LiteralPointerCanaryError("epoch-13 metric taxonomy input drifted")
            for field in failures:
                value = metric.get(field)
                if value not in (None, "") and value not in evidence:
                    failures[field] += 1
    return failures


def _validate_predecessor(full_verify: bool) -> None:
    receipt_path = PREDECESSOR_ROOT / "plan-step-receipt.json"
    receipt = (
        epoch13.verify_receipt(PREDECESSOR_ROOT)
        if full_verify
        else _load(receipt_path, "epoch-13 receipt")
    )
    if (
        _record(receipt_path)["sha256"] != PREDECESSOR_RECEIPT_SHA256
        or _record(PREDECESSOR_ROOT / "terminal.json")["sha256"]
        != PREDECESSOR_RECEIPT_SHA256
        or _record(PREDECESSOR_ROOT / "turn/output.private.json")["sha256"]
        != PREDECESSOR_OUTPUT_SHA256
        or receipt.get("state") != "rejected"
        or receipt.get("terminal_reason")
        != "epoch13_bounded_span_canary_semantic_output_rejected"
        or receipt.get("new_semantic_model_call_count") != 1
        or receipt.get("semantic_retry_count") != 0
        or receipt.get("new_unknown_usage_turn_count") != 0
        or receipt.get("new_measured_usage", {}).get("total_tokens") != 61_072
        or receipt.get("failed_checks") != ["semantic_output_validity"]
        or receipt.get("diagnostic", {}).get("diagnostic_path")
        != "segment seg_155ba31ee473823b6e7e9504 failed canonical v3.1 validation"
        or receipt.get("production_mutated") is not False
        or receipt.get("holdout_authorized") is not False
        or _metric_literal_taxonomy() != EXPECTED_TAXONOMY
    ):
        raise LiteralPointerCanaryError("epoch-13 predecessor evidence drifted")


def _build_request() -> dict[str, Any]:
    predecessor = _load(
        PREDECESSOR_ROOT / "prepared-turn/request.private.json",
        "epoch-13 prepared request",
    )
    episode = copy.deepcopy(predecessor["episode_context"])
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
        for segment in predecessor["private_input"]["segments"]
    ]
    requests = adapter.prepare_episode_batches(
        episode, batch_size=8, thread_mode="new_thread"
    )
    if len(requests) != 1:
        raise LiteralPointerCanaryError("epoch-14 request count drifted")
    request = requests[0]
    token_count = sum(
        len(segment["literal_tokens"])
        for segment in request["private_input"]["segments"]
    )
    if (
        request.get("effective_batch_size") != 6
        or request.get("segment_ids") != predecessor.get("segment_ids")
        or len(request["prompt"].encode("utf-8")) != EXPECTED_PROMPT_BYTES
        or token_count != EXPECTED_LITERAL_TOKEN_COUNT
    ):
        raise LiteralPointerCanaryError("epoch-14 frozen sample shape drifted")
    adapter.validate_prepared_request(request)
    return request


def _predecessor_records() -> Mapping[str, Any]:
    return {
        "receipt": _record(PREDECESSOR_ROOT / "plan-step-receipt.json"),
        "terminal": _record(PREDECESSOR_ROOT / "terminal.json"),
        "runtime_lock": _record(PREDECESSOR_ROOT / "runtime-lock.json"),
        "semantic_rejection": _record(PREDECESSOR_ROOT / "semantic-rejection.json"),
        "prepared_request": _record(
            PREDECESSOR_ROOT / "prepared-turn/request.private.json"
        ),
        "raw_output": _record(PREDECESSOR_ROOT / "turn/output.private.json"),
        "sidecar": _record(PREDECESSOR_ROOT / "turn/sidecar.json"),
    }


def _runtime_module_paths() -> Sequence[Path]:
    return (
        Path(__file__),
        Path(adapter.__file__),
        Path(adapter.bounded_span.__file__),
        Path(adapter.bounded_span.audited.__file__),
        Path(adapter.base.__file__),
        Path(epoch13.__file__),
    )


def _validate_directive(value: Mapping[str, Any]) -> None:
    authorization = value.get("authorization_contract")
    predecessor = value.get("predecessor_contract")
    ranking = value.get("architecture_ranking")
    architecture = value.get("architecture_contract")
    canary = value.get("canary_contract")
    promotion = value.get("promotion_contract")
    transport = value.get("transport_contract")
    if (
        value.get("schema_version")
        != "pif_evaluation_epoch14_literal_pointer_canary_directive_v1"
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
        or predecessor.get("epoch13_receipt_sha256") != PREDECESSOR_RECEIPT_SHA256
        or predecessor.get("epoch13_state") != "rejected"
        or predecessor.get("epoch13_semantic_model_call_count") != 1
        or predecessor.get("epoch13_total_tokens") != 61_072
        or predecessor.get("epoch13_completed_turn_replay_allowed") is not False
        or predecessor.get("offline_metric_literal_drift_taxonomy")
        != {
            "raw_text_not_exact_evidence_substring": 16,
            "value_not_exact_evidence_substring": 3,
            "unit_not_exact_evidence_substring": 0,
            "comparator_not_exact_evidence_substring": 1,
        }
        or not isinstance(ranking, list)
        or len(ranking) != 3
        or [row.get("rank") for row in ranking if isinstance(row, Mapping)]
        != [1, 2, 3]
        or ranking[0].get("selected") is not True
        or any(row.get("selected") is not False for row in ranking[1:])
        or not isinstance(architecture, Mapping)
        or architecture.get("source_unit_max_characters") != 450
        or architecture.get("selectable_evidence_span_max_characters") != 1000
        or architecture.get("literal_tokenization_version")
        != adapter.LITERAL_TOKENIZATION_VERSION
        or architecture.get(
            "model_selects_metric_value_unit_comparator_and_raw_text_token_ranges"
        )
        is not True
        or architecture.get(
            "deterministic_exact_token_range_to_source_substring_projection_only"
        )
        is not True
        or architecture.get("model_authored_free_form_metric_literals") is not False
        or any(
            architecture.get(field) is not False
            for field in (
                "deterministic_semantic_pruning",
                "deterministic_support_filtering",
                "deterministic_deduplication",
                "deterministic_relabeling",
                "semantic_regex_or_keyword_rules",
            )
        )
        or not isinstance(canary, Mapping)
        or canary.get("model") != adapter.MODEL
        or canary.get("effort") != adapter.EFFORT
        or canary.get("thread_mode") != "new_thread"
        or canary.get("configured_batch_size") != 8
        or canary.get("effective_batch_size") != 6
        or canary.get("new_model_call_cap") != 1
        or canary.get("semantic_retry_count") != 0
        or canary.get("measured_total_token_acceptance_ceiling")
        != MAXIMUM_TOTAL_TOKENS
        or canary.get("maximum_wall_seconds") != MAXIMUM_WALL_SECONDS
        or canary.get("minimum_remaining_reserve_percent")
        != MINIMUM_REMAINING_RESERVE_PERCENT
        or canary.get("capacity_safety_margin_percent")
        != CAPACITY_SAFETY_MARGIN_PERCENT
        or canary.get("quota_points_per_million_tokens")
        != QUOTA_POINTS_PER_MILLION_TOKENS
        or canary.get("frozen_prompt_bytes") != EXPECTED_PROMPT_BYTES
        or canary.get("frozen_literal_token_count") != EXPECTED_LITERAL_TOKEN_COUNT
        or canary.get("production_amortized_ratio_gate") != 0.28
        or not isinstance(promotion, Mapping)
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
        raise LiteralPointerCanaryError("epoch-14 directive contract drifted")


def _receipt_metadata(outcome: Mapping[str, Any]) -> Mapping[str, Any]:
    del outcome
    return {
        "source_unit_max_characters": adapter.SOURCE_UNIT_MAX_CHARS,
        "selected_span_bound_characters": adapter.CANONICAL_EVIDENCE_MAX_CHARS,
        "literal_tokenization_version": adapter.LITERAL_TOKENIZATION_VERSION,
        "literal_token_count": EXPECTED_LITERAL_TOKEN_COUNT,
        "metric_literal_fields": list(adapter.METRIC_LITERAL_FIELDS),
        "epoch13_metric_literal_drift_taxonomy": copy.deepcopy(EXPECTED_TAXONOMY),
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
    stage="literal_pointer_canonical_labels",
    architecture_class=(
        "bounded_evidence_with_llm_selected_exact_metric_literal_token_indices"
    ),
    authorization_statement=AUTHORIZATION_STATEMENT,
    authorization_state="authorized_for_exactly_one_literal_pointer_canary_turn",
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
    pass_next_action="freeze_fresh_six_arm_literal_pointer_development_matrix",
    reject_next_action="reject_literal_pointer_architecture_without_field_patch",
)


def prepare_canary(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    return one_turn.prepare(SPEC, root)


def verify_runtime(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    return one_turn.verify_runtime(SPEC, root)


def authorize_canary(
    *,
    root: Path = DEFAULT_ROOT,
    operator_authorization_id: str,
    now: Any = None,
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
    "LiteralPointerCanaryError",
    "authorize_canary",
    "execute_canary",
    "prepare_canary",
    "status_canary",
    "verify_authorization",
    "verify_receipt",
    "verify_runtime",
)
