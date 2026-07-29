from __future__ import annotations

"""Epoch-26 low-effort recovery for the exact-token-ID canonical canary."""

import argparse
import asyncio
import copy
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_unit_owned_token_id_canary_runtime as epoch25
from . import app_server_canonical_v31_unit_owned_token_id_low_episode_batch as adapter
from . import app_server_one_turn_canary_runtime as one_turn


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROOT = (
    PROJECT_ROOT
    / "work/app-server-development-v2/unattended-pipeline-v5"
    / "canonical-v31-epoch26-unit-owned-token-id-low-recovery-v1"
)
PREDECESSOR_ROOT = epoch25.DEFAULT_ROOT
EPOCH24_ROOT = epoch25.PREDECESSOR_ROOT
EPOCH23_ROOT = epoch25.EPOCH23_ROOT
DIRECTIVE_PATH = (
    PROJECT_ROOT
    / "automation/pif-evaluation-epoch26-unit-owned-token-id-low-recovery-v26.json"
)
PLAN_PATH = PROJECT_ROOT / "automation/pif-evaluation-semantic-plan-v26.json"
DIRECTIVE_SHA256 = "b5d0dc3c0737925a32de1eb6b6cf01ad86f6a3e626908161a7bddbb9465b373d"
THREAD_ID = "019f4cf1-c46e-7db3-acd2-bf03c4459a10"
PLAN_EPOCH = 26
STEP_ID = "canonical_v31_epoch26_unit_owned_token_id_low_recovery_v26"
TURN_NAME = "epoch26_unit_owned_token_id_low_two_case_recovery"
AUTHORIZATION_STATEMENT = (
    "You have my full permission to continue. No need to seek out my approval anymore."
)
AUTHORIZATION_ID_DEFAULT = "kolby-epoch26-unit-owned-token-id-low-20260719"
SELECTED_SEGMENT_IDS = epoch25.SELECTED_SEGMENT_IDS
MAXIMUM_TOTAL_TOKENS = 30_000
MAXIMUM_WALL_SECONDS = 1_200
MINIMUM_REMAINING_RESERVE_PERCENT = 10
CAPACITY_SAFETY_MARGIN_PERCENT = 1
QUOTA_POINTS_PER_MILLION_TOKENS = 17
AUTHORIZATION_WINDOW_SECONDS = 14_400
EXPECTED_SHAPE = {
    "prompt_bytes": 14_277,
    "base_bytes": 23_614,
    "schema_bytes": 5_841,
    "request_bytes": 312_751,
    "source_unit_count": 20,
    "evidence_span_count": 38,
    "literal_token_count": 2_020,
}
EXPECTED_REQUEST_SHA256 = "dc6a897b2fead84fc9e517a2f66b385e9bb7845bb3b3de987ff5bb5a78d60e06"
PARENT_PROMPT_SHA256 = "36af4c581e70487d72887f225ed18430d4b5c35b02370fed0a0257f34bc3a1c1"
PARENT_BASE_SHA256 = "db76cef744f62dd9ee81dd16db0bb56731acbd12febdc5161840c6f1965e1bab"
PARENT_SCHEMA_SHA256 = "d355cf3833e08a4276ee41628611e689bddab9bf19320fa1844f35b709a2ca07"
FULL_VISIBLE_REQUEST_BYTES = epoch25.FULL_VISIBLE_REQUEST_BYTES
CANARY_VISIBLE_REQUEST_BYTES = epoch25.CANARY_VISIBLE_REQUEST_BYTES
FULL_CASE_SCALE = epoch25.FULL_CASE_SCALE
FULL_PRODUCTION_TOKEN_CEILING = epoch25.FULL_PRODUCTION_TOKEN_CEILING

PREDECESSOR_RUNTIME_LOCK_SHA256 = (
    "637f45af761f8cb1a880d6fee77e7da0010f78c8db6c71da255abc7bc40a1fde"
)
PREDECESSOR_RECEIPT_SHA256 = (
    "1809661881d4b43d951f978317eedebcbbcaeb81184486c7b4c4fc8f5e5fe500"
)
PREDECESSOR_AUTHORIZATION_SHA256 = (
    "7ceb0bb7b4367a93a776358b296add1829aa18eb425d597a5159bb4857419d63"
)
PREDECESSOR_ATTEMPT_SHA256 = (
    "f1895919759546c695172212fcb0c470d0d73216763a082c394627f3396d929d"
)
PREDECESSOR_SIDECAR_SHA256 = (
    "55dddae421eae9b7a0b199cb88adf7200ad1a1233eb12870da016ac4af81e185"
)
PREDECESSOR_REQUEST_SHA256 = (
    "fa7714198950716dd0dedefbf88516b5197fb363343d99b8175c638ce6e3c6a9"
)
EPOCH24_RECEIPT_SHA256 = epoch25.PREDECESSOR_RECEIPT_SHA256
EPOCH24_SIDECAR_SHA256 = epoch25.PREDECESSOR_SIDECAR_SHA256
EPOCH24_OUTPUT_SHA256 = epoch25.PREDECESSOR_OUTPUT_SHA256
EPOCH23_RECEIPT_SHA256 = epoch25.EPOCH23_RECEIPT_SHA256
EPOCH23_SIDECAR_SHA256 = epoch25.EPOCH23_SIDECAR_SHA256

UnitOwnedTokenIdLowCanaryError = one_turn.OneTurnCanaryError
UnitOwnedTokenIdLowCanaryWaiting = one_turn.OneTurnCanaryWaiting


def _load(path: Path, label: str) -> dict[str, Any]:
    return one_turn._load(path, label)  # noqa: SLF001


def _record(path: Path) -> dict[str, Any]:
    return one_turn._record(path)  # noqa: SLF001


def _request_shape(request: Mapping[str, Any]) -> dict[str, int]:
    return epoch25._request_shape(request)  # noqa: SLF001


def _request_sha256(request: Mapping[str, Any]) -> str:
    return one_turn._sha256_bytes(  # noqa: SLF001
        one_turn._canonical_json(request).encode("utf-8")  # noqa: SLF001
    )


def _build_request() -> dict[str, Any]:
    requests = adapter.prepare_episode_batches(
        epoch25.epoch24.epoch23._source_episode(),  # noqa: SLF001
        batch_size=3,
        thread_mode="new_thread",
    )
    if len(requests) != 1:
        raise UnitOwnedTokenIdLowCanaryError("epoch-26 request count drifted")
    request = requests[0]
    parent = adapter._parent_request(request)  # noqa: SLF001
    if (
        tuple(request.get("segment_ids", ())) != SELECTED_SEGMENT_IDS
        or request.get("effective_batch_size") != 2
        or request.get("batch_size_ceiling") != 3
        or request.get("candidate_system_id") != adapter.CANDIDATE_SYSTEM_ID
        or request.get("model") != adapter.MODEL
        or request.get("effort") != "low"
        or request.get("retry_count") != 0
        or request.get("semantic_integrity", {}).get("semantic_system_variant")
        != "exact_token_id_low_effort_v1"
        or request.get("semantic_integrity", {}).get("parent_prompt_unchanged") is not True
        or request.get("semantic_integrity", {}).get("parent_output_schema_unchanged")
        is not True
        or request["prompt"] != parent["prompt"]
        or request["base_instructions"] != parent["base_instructions"]
        or request["output_schema"] != parent["output_schema"]
        or request["prompt_sha256"] != PARENT_PROMPT_SHA256
        or request["base_instructions_sha256"] != PARENT_BASE_SHA256
        or request["output_schema_sha256"] != PARENT_SCHEMA_SHA256
        or _request_shape(request) != EXPECTED_SHAPE
        or _request_sha256(request) != EXPECTED_REQUEST_SHA256
    ):
        raise UnitOwnedTokenIdLowCanaryError("epoch-26 frozen request contract drifted")
    epoch25.epoch24._validate_provider_schema(request["output_schema"])  # noqa: SLF001
    adapter.validate_prepared_request(request)
    return request


_PREDECESSOR_FILES: tuple[tuple[str, Path], ...] = (
    ("epoch25_runtime_lock", PREDECESSOR_ROOT / "runtime-lock.json"),
    ("epoch25_receipt", PREDECESSOR_ROOT / "plan-step-receipt.json"),
    ("epoch25_terminal", PREDECESSOR_ROOT / "terminal.json"),
    ("epoch25_authorization", PREDECESSOR_ROOT / "operator-authorization.json"),
    ("epoch25_attempt", PREDECESSOR_ROOT / "turn/semantic-attempt.json"),
    ("epoch25_sidecar", PREDECESSOR_ROOT / "turn/sidecar.json"),
    ("epoch25_request", PREDECESSOR_ROOT / "prepared-turn/request.private.json"),
    ("epoch24_receipt", EPOCH24_ROOT / "plan-step-receipt.json"),
    ("epoch24_sidecar", EPOCH24_ROOT / "turn/sidecar.json"),
    ("epoch24_output", EPOCH24_ROOT / "turn/output.private.json"),
    ("epoch23_receipt", EPOCH23_ROOT / "plan-step-receipt.json"),
    ("epoch23_sidecar", EPOCH23_ROOT / "turn/sidecar.json"),
)


def _frozen_predecessor_evidence() -> list[dict[str, Any]]:
    return [{"role": role, **_record(path)} for role, path in _PREDECESSOR_FILES]


def _predecessor_records() -> Mapping[str, Any]:
    grouped: dict[str, dict[str, Any]] = {
        "epoch25_interrupted_unknown_usage_attempt": {},
        "epoch24_measured_structural_rejection": {},
        "epoch23_interrupted_unknown_usage_attempt": {},
    }
    for role, path in _PREDECESSOR_FILES:
        epoch, name = role.split("_", 1)
        key = {
            "epoch25": "epoch25_interrupted_unknown_usage_attempt",
            "epoch24": "epoch24_measured_structural_rejection",
            "epoch23": "epoch23_interrupted_unknown_usage_attempt",
        }[epoch]
        grouped[key][name] = _record(path)
    return grouped


def _intention_to_treat_accounting() -> dict[str, Any]:
    return {
        "predecessor_semantic_model_call_count": 3,
        "predecessor_measured_call_count": 1,
        "predecessor_unknown_usage_turn_count": 2,
        "predecessor_measured_total_tokens": 29_079,
        "epoch23": {"semantic_model_call_count": 1, "usage_status": "unknown"},
        "epoch24": {
            "semantic_model_call_count": 1,
            "usage_status": "measured",
            "total_tokens": 29_079,
        },
        "epoch25": {"semantic_model_call_count": 1, "usage_status": "unknown"},
        "epoch25_aggregate_metadata_unknown_count": 1,
        "reconciled_unknown_count_uses_direct_epoch_receipt_fields": True,
        "maximum_semantic_model_call_count_after_epoch26_dispatch": 4,
        "predecessor_replay_allowed": False,
    }


def _validate_predecessor(full_verify: bool) -> None:
    predecessor = (
        epoch25.verify_receipt(PREDECESSOR_ROOT)
        if full_verify
        else _load(PREDECESSOR_ROOT / "plan-step-receipt.json", "epoch-25 receipt")
    )
    epoch25_sidecar = _load(PREDECESSOR_ROOT / "turn/sidecar.json", "epoch-25 sidecar")
    epoch24_receipt = _load(EPOCH24_ROOT / "plan-step-receipt.json", "epoch-24 receipt")
    epoch23_receipt = _load(EPOCH23_ROOT / "plan-step-receipt.json", "epoch-23 receipt")
    expected_hashes = {
        PREDECESSOR_ROOT / "runtime-lock.json": PREDECESSOR_RUNTIME_LOCK_SHA256,
        PREDECESSOR_ROOT / "plan-step-receipt.json": PREDECESSOR_RECEIPT_SHA256,
        PREDECESSOR_ROOT / "terminal.json": PREDECESSOR_RECEIPT_SHA256,
        PREDECESSOR_ROOT / "operator-authorization.json": PREDECESSOR_AUTHORIZATION_SHA256,
        PREDECESSOR_ROOT / "turn/semantic-attempt.json": PREDECESSOR_ATTEMPT_SHA256,
        PREDECESSOR_ROOT / "turn/sidecar.json": PREDECESSOR_SIDECAR_SHA256,
        PREDECESSOR_ROOT / "prepared-turn/request.private.json": PREDECESSOR_REQUEST_SHA256,
        EPOCH24_ROOT / "plan-step-receipt.json": EPOCH24_RECEIPT_SHA256,
        EPOCH24_ROOT / "turn/sidecar.json": EPOCH24_SIDECAR_SHA256,
        EPOCH24_ROOT / "turn/output.private.json": EPOCH24_OUTPUT_SHA256,
        EPOCH23_ROOT / "plan-step-receipt.json": EPOCH23_RECEIPT_SHA256,
        EPOCH23_ROOT / "turn/sidecar.json": EPOCH23_SIDECAR_SHA256,
    }
    if any(_record(path)["sha256"] != digest for path, digest in expected_hashes.items()):
        raise UnitOwnedTokenIdLowCanaryError("epoch-26 predecessor record drifted")
    if (
        (PREDECESSOR_ROOT / "turn/output.private.json").exists()
        or predecessor.get("state") != "waiting"
        or predecessor.get("terminal_reason")
        != "epoch25_interrupted_semantic_attempt_preserved_no_replay"
        or predecessor.get("new_semantic_model_call_count") != 1
        or predecessor.get("new_unknown_usage_turn_count") != 1
        or predecessor.get("semantic_retry_count") != 0
        or predecessor.get("new_measured_usage", {}).get("total_tokens") != 0
        or predecessor.get("new_wall_elapsed_seconds") != 1_200.009
        or predecessor.get("aggregate_architecture_unknown_usage_turn_count") != 1
        or predecessor.get("production_mutated") is not False
        or predecessor.get("holdout_authorized") is not False
        or epoch25_sidecar.get("state") != "interrupted"
        or epoch25_sidecar.get("status") != "timeout"
        or epoch25_sidecar.get("error_class") != "turn_timeout"
        or epoch25_sidecar.get("usage_status") != "unknown"
        or epoch25_sidecar.get("usage_complete") is not False
        or epoch25_sidecar.get("usage") is not None
        or epoch25_sidecar.get("thread_total_usage") is not None
        or epoch25_sidecar.get("auth_type") != "chatgpt"
        or epoch25_sidecar.get("plan_type") != "pro"
        or epoch25_sidecar.get("model") != "gpt-5.6-sol"
        or epoch25_sidecar.get("effort") != "high"
        or epoch24_receipt.get("state") != "rejected"
        or epoch24_receipt.get("new_semantic_model_call_count") != 1
        or epoch24_receipt.get("new_unknown_usage_turn_count") != 0
        or epoch24_receipt.get("new_measured_usage", {}).get("total_tokens") != 29_079
        or epoch23_receipt.get("state") != "waiting"
        or epoch23_receipt.get("new_semantic_model_call_count") != 1
        or epoch23_receipt.get("new_unknown_usage_turn_count") != 1
        or _intention_to_treat_accounting()
        != json.loads(DIRECTIVE_PATH.read_text(encoding="utf-8"))[
            "intention_to_treat_accounting"
        ]
    ):
        raise UnitOwnedTokenIdLowCanaryError("epoch-26 predecessor evidence drifted")
    _build_request()


def _runtime_module_paths() -> Sequence[Path]:
    return (
        Path(__file__),
        Path(adapter.__file__),
        Path(epoch25.__file__),
        *epoch25._runtime_module_paths(),  # noqa: SLF001
    )


def _validate_directive(value: Mapping[str, Any]) -> None:
    expected = json.loads(DIRECTIVE_PATH.read_text(encoding="utf-8"))
    canary = expected.get("canary_contract", {})
    ranking = expected.get("architecture_ranking", [])
    if dict(value) != expected:
        raise UnitOwnedTokenIdLowCanaryError("epoch-26 directive contract drifted")
    if (
        expected.get("schema_version")
        != "pif_evaluation_epoch26_unit_owned_token_id_low_recovery_directive_v1"
        or expected.get("thread_id") != THREAD_ID
        or expected.get("plan_epoch") != PLAN_EPOCH
        or expected.get("step_id") != STEP_ID
        or expected.get("authorized_by") != "kolby"
        or expected.get("authority") != "direct_user_instruction"
        or expected.get("authorization_statement") != AUTHORIZATION_STATEMENT
        or expected.get("expected_receipt_path")
        != str((DEFAULT_ROOT / "plan-step-receipt.json").resolve())
        or expected.get("frozen_predecessor_evidence")
        != _frozen_predecessor_evidence()
        or expected.get("intention_to_treat_accounting")
        != _intention_to_treat_accounting()
        or [row.get("rank") for row in ranking] != [1, 2, 3]
        or sum(row.get("selected") is True for row in ranking) != 1
        or canary.get("selected_segment_ids") != list(SELECTED_SEGMENT_IDS)
        or canary.get("candidate_model") != "gpt-5.6-sol"
        or canary.get("candidate_reasoning_effort") != "low"
        or canary.get("semantic_model_call_cap") != 1
        or canary.get("semantic_retry_cap") != 0
        or canary.get("measured_total_token_ceiling") != MAXIMUM_TOTAL_TOKENS
        or canary.get("epoch25_replay_allowed") is not False
        or canary.get("prompt_schema_source_semantics_unchanged_from_epoch25") is not True
        or canary.get("semantic_system_variant_is_new") is not True
        or expected.get("transport_contract", {}).get("transport")
        != "official_codex_app_server_stdio_managed_chatgpt_auth"
        or expected.get("promotion_contract", {}).get("holdout_authorized") is not False
        or expected.get("promotion_contract", {}).get("production_mutation_allowed")
        is not False
    ):
        raise UnitOwnedTokenIdLowCanaryError("epoch-26 directive invariants drifted")


def _receipt_metadata(outcome: Mapping[str, Any]) -> Mapping[str, Any]:
    usage = outcome["usage"]
    total = int(usage["total_tokens"])
    fidelity_path = DEFAULT_ROOT / "turn/semantic-fidelity.json"
    fidelity: Mapping[str, Any] = {}
    if fidelity_path.is_file():
        fidelity = _load(fidelity_path, "epoch-26 semantic fidelity")
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
    current_calls = int(outcome["semantic_model_call_count"])
    current_unknown = int(outcome["unknown_usage_turn_count"])
    return {
        "diagnostic_segment_ids": list(SELECTED_SEGMENT_IDS),
        "diagnostic_source_unit_count": EXPECTED_SHAPE["source_unit_count"],
        "diagnostic_event_count": fidelity.get("emitted_event_count"),
        "diagnostic_concept_count": fidelity.get("emitted_concept_count"),
        "event_owner_container_normalization_count": fidelity.get(
            "event_owner_container_normalization_count"
        ),
        "concept_owner_container_normalization_count": fidelity.get(
            "concept_owner_container_normalization_count"
        ),
        "exact_metric_token_pair_count": fidelity.get("exact_metric_token_pair_count"),
        "measured_canary_total_tokens": total,
        "full_six_case_input_token_projection": input_projection,
        "full_six_case_output_token_projection_by_case_count": output_projection,
        "full_six_case_total_token_projection_by_case_count": full_projection,
        "full_six_case_cost_projection_pass": (
            full_projection <= FULL_PRODUCTION_TOKEN_CEILING
            if full_projection is not None
            else False
        ),
        "production_token_ceiling_for_future_full_run": FULL_PRODUCTION_TOKEN_CEILING,
        "quality_measured_by_this_step": False,
        "judge_required_before_full_run_promotion": True,
        "epoch23_predecessor_semantic_model_call_count": 1,
        "epoch23_predecessor_unknown_usage_turn_count": 1,
        "epoch24_predecessor_semantic_model_call_count": 1,
        "epoch24_predecessor_measured_total_tokens": 29_079,
        "epoch25_predecessor_semantic_model_call_count": 1,
        "epoch25_predecessor_unknown_usage_turn_count": 1,
        "aggregate_architecture_semantic_model_call_count": 3 + current_calls,
        "aggregate_architecture_unknown_usage_turn_count": 2 + current_unknown,
        "aggregate_architecture_measured_total_tokens": 29_079 + total,
        "predecessor_accounting_reconciled_additively": True,
        "epoch23_replayed": False,
        "epoch24_replayed": False,
        "epoch25_replayed": False,
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
    stage="unit_owned_exact_token_id_low_effort_two_case_extraction_recovery",
    architecture_class="one_turn_unit_owned_exact_token_id_low_effort_full_canonical_schema_v1",
    authorization_statement=AUTHORIZATION_STATEMENT,
    authorization_state="authorized_for_exactly_one_low_effort_two_case_token_id_recovery",
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
    pass_next_action="freeze_two_case_full_event_ab_ba_quality_if_cost_projection_passes",
    reject_next_action="reject_low_effort_exact_token_id_variant_without_field_repair",
    waiting_next_action="no_replay_materially_distinct_architecture_only",
)


def _runtime_status(status: Mapping[str, Any], root: Path) -> dict[str, Any]:
    request = _load(root.resolve() / "prepared-turn/request.private.json", "epoch-26 request")
    shape = _request_shape(request)
    if shape != EXPECTED_SHAPE or _request_sha256(request) != EXPECTED_REQUEST_SHA256:
        raise UnitOwnedTokenIdLowCanaryError("epoch-26 runtime request drifted")
    epoch25.epoch24._validate_provider_schema(request["output_schema"])  # noqa: SLF001
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
