from __future__ import annotations

"""Epoch-27 tagged-metric medium-effort exact-token-ID canary."""

import argparse
import asyncio
import copy
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_tagged_metric_token_id_episode_batch as adapter
from . import app_server_canonical_v31_unit_owned_token_id_low_canary_runtime as epoch26
from . import app_server_one_turn_canary_runtime as one_turn


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROOT = (
    PROJECT_ROOT
    / "work/app-server-development-v2/unattended-pipeline-v5"
    / "canonical-v31-epoch27-tagged-metric-token-id-canary-v1"
)
PREDECESSOR_ROOT = epoch26.DEFAULT_ROOT
EPOCH25_ROOT = epoch26.PREDECESSOR_ROOT
EPOCH24_ROOT = epoch26.EPOCH24_ROOT
EPOCH23_ROOT = epoch26.EPOCH23_ROOT
DIRECTIVE_PATH = (
    PROJECT_ROOT / "automation/pif-evaluation-epoch27-tagged-metric-token-id-canary-v27.json"
)
PLAN_PATH = PROJECT_ROOT / "automation/pif-evaluation-semantic-plan-v27.json"
DIRECTIVE_SHA256 = "82ae5850469bbc72dadee62e5feda031f58fad04541fea606a60acfecf0a75aa"
THREAD_ID = epoch26.THREAD_ID
PLAN_EPOCH = 27
STEP_ID = "canonical_v31_epoch27_tagged_metric_token_id_canary_v27"
TURN_NAME = "epoch27_tagged_metric_token_id_two_case_canary"
AUTHORIZATION_STATEMENT = epoch26.AUTHORIZATION_STATEMENT
AUTHORIZATION_ID_DEFAULT = "kolby-epoch27-tagged-metric-token-id-20260719"
SELECTED_SEGMENT_IDS = epoch26.SELECTED_SEGMENT_IDS
MAXIMUM_TOTAL_TOKENS = 32_000
MAXIMUM_WALL_SECONDS = 1_200
MINIMUM_REMAINING_RESERVE_PERCENT = 10
CAPACITY_SAFETY_MARGIN_PERCENT = 1
QUOTA_POINTS_PER_MILLION_TOKENS = 17
AUTHORIZATION_WINDOW_SECONDS = 14_400
EXPECTED_SHAPE = {
    "prompt_bytes": 14_277,
    "base_bytes": 24_269,
    "schema_bytes": 5_939,
    "request_bytes": 313_695,
    "source_unit_count": 20,
    "evidence_span_count": 38,
    "literal_token_count": 2_020,
}
EXPECTED_REQUEST_SHA256 = "35247344979f121ddf6739ad56ba7e4bab3a5d6502274c74e36097c364bd2553"
EXPECTED_PROMPT_SHA256 = epoch26.PARENT_PROMPT_SHA256
EXPECTED_BASE_SHA256 = "d8563985e8b1e0baa7c26b3015567e68ce795f31253887ae96341ffb776904f8"
EXPECTED_SCHEMA_SHA256 = "d9b5ea0852bab79f5505974d2138bb96e2ad949743b208f718d8531eb094221d"
FULL_VISIBLE_REQUEST_BYTES = epoch26.FULL_VISIBLE_REQUEST_BYTES
CANARY_VISIBLE_REQUEST_BYTES = epoch26.CANARY_VISIBLE_REQUEST_BYTES
FULL_CASE_SCALE = epoch26.FULL_CASE_SCALE
FULL_PRODUCTION_TOKEN_CEILING = epoch26.FULL_PRODUCTION_TOKEN_CEILING

PREDECESSOR_RUNTIME_LOCK_SHA256 = (
    "e78215e2a418ca893ef80d0351f10b06fb62ab9858f01d369173c637cb18b05b"
)
PREDECESSOR_RECEIPT_SHA256 = (
    "4e73614d8006a579cff1e9260f2ff57838133d521306bb4c4f26c2e2952475be"
)
PREDECESSOR_AUTHORIZATION_SHA256 = (
    "5486b53042d44059b593d0bfd837765cc8fe01e781afac652fdade510d4b211f"
)
PREDECESSOR_ATTEMPT_SHA256 = (
    "e0e3174488ecb3ba03d9750fb5856194c8aded28ea826e105efc3e611a0d09f1"
)
PREDECESSOR_SIDECAR_SHA256 = (
    "b608705f938ae131a335d6ee36de4be9674b1e5716f251e656f4b0b285bec6be"
)
PREDECESSOR_OUTPUT_SHA256 = (
    "b56d0038da8bd5f7fc87263eab7f0ebd8bbecc2344d301021623548cb1b4b910"
)
PREDECESSOR_REJECTION_SHA256 = (
    "ea0ff8453c3eb551fe790e410c921d5de5da6ad00086b4533770cc24bd937754"
)
PREDECESSOR_REQUEST_SHA256 = (
    "067f2f42844f45c8e8e57f84ac1d4f562de8bfdc8356ee905649cb6234d7a155"
)
EPOCH25_RECEIPT_SHA256 = epoch26.PREDECESSOR_RECEIPT_SHA256
EPOCH25_SIDECAR_SHA256 = epoch26.PREDECESSOR_SIDECAR_SHA256
EPOCH24_RECEIPT_SHA256 = epoch26.EPOCH24_RECEIPT_SHA256
EPOCH24_SIDECAR_SHA256 = epoch26.EPOCH24_SIDECAR_SHA256
EPOCH24_OUTPUT_SHA256 = epoch26.EPOCH24_OUTPUT_SHA256
EPOCH23_RECEIPT_SHA256 = epoch26.EPOCH23_RECEIPT_SHA256
EPOCH23_SIDECAR_SHA256 = epoch26.EPOCH23_SIDECAR_SHA256

TaggedMetricTokenIdCanaryError = one_turn.OneTurnCanaryError
TaggedMetricTokenIdCanaryWaiting = one_turn.OneTurnCanaryWaiting


def _load(path: Path, label: str) -> dict[str, Any]:
    return one_turn._load(path, label)  # noqa: SLF001


def _record(path: Path) -> dict[str, Any]:
    return one_turn._record(path)  # noqa: SLF001


def _request_shape(request: Mapping[str, Any]) -> dict[str, int]:
    return epoch26._request_shape(request)  # noqa: SLF001


def _request_sha256(request: Mapping[str, Any]) -> str:
    return one_turn._sha256_bytes(  # noqa: SLF001
        one_turn._canonical_json(request).encode("utf-8")  # noqa: SLF001
    )


def _build_request() -> dict[str, Any]:
    values = adapter.prepare_episode_batches(
        epoch26.epoch25.epoch24.epoch23._source_episode(),  # noqa: SLF001
        batch_size=3,
        thread_mode="new_thread",
    )
    if len(values) != 1:
        raise TaggedMetricTokenIdCanaryError("epoch-27 request count drifted")
    request = values[0]
    if (
        tuple(request.get("segment_ids", ())) != SELECTED_SEGMENT_IDS
        or request.get("effective_batch_size") != 2
        or request.get("batch_size_ceiling") != 3
        or request.get("candidate_system_id") != adapter.CANDIDATE_SYSTEM_ID
        or request.get("model") != "gpt-5.6-sol"
        or request.get("effort") != "medium"
        or request.get("retry_count") != 0
        or request.get("semantic_integrity", {}).get("tagged_metric_protocol_version")
        != adapter.TAGGED_METRIC_PROTOCOL_VERSION
        or request.get("semantic_integrity", {}).get(
            "model_authors_metric_applicability_by_bundle_cardinality"
        )
        is not True
        or request["prompt_sha256"] != EXPECTED_PROMPT_SHA256
        or request["base_instructions_sha256"] != EXPECTED_BASE_SHA256
        or request["output_schema_sha256"] != EXPECTED_SCHEMA_SHA256
        or _request_shape(request) != EXPECTED_SHAPE
        or _request_sha256(request) != EXPECTED_REQUEST_SHA256
    ):
        raise TaggedMetricTokenIdCanaryError("epoch-27 frozen request contract drifted")
    epoch26.epoch25.epoch24._validate_provider_schema(request["output_schema"])  # noqa: SLF001
    adapter.validate_prepared_request(request)
    return request


_PREDECESSOR_FILES: tuple[tuple[str, Path], ...] = (
    ("epoch26_runtime_lock", PREDECESSOR_ROOT / "runtime-lock.json"),
    ("epoch26_receipt", PREDECESSOR_ROOT / "plan-step-receipt.json"),
    ("epoch26_terminal", PREDECESSOR_ROOT / "terminal.json"),
    ("epoch26_authorization", PREDECESSOR_ROOT / "operator-authorization.json"),
    ("epoch26_attempt", PREDECESSOR_ROOT / "turn/semantic-attempt.json"),
    ("epoch26_sidecar", PREDECESSOR_ROOT / "turn/sidecar.json"),
    ("epoch26_output", PREDECESSOR_ROOT / "turn/output.private.json"),
    ("epoch26_rejection", PREDECESSOR_ROOT / "semantic-rejection.json"),
    ("epoch26_request", PREDECESSOR_ROOT / "prepared-turn/request.private.json"),
    ("epoch25_receipt", EPOCH25_ROOT / "plan-step-receipt.json"),
    ("epoch25_sidecar", EPOCH25_ROOT / "turn/sidecar.json"),
    ("epoch24_receipt", EPOCH24_ROOT / "plan-step-receipt.json"),
    ("epoch24_sidecar", EPOCH24_ROOT / "turn/sidecar.json"),
    ("epoch24_output", EPOCH24_ROOT / "turn/output.private.json"),
    ("epoch23_receipt", EPOCH23_ROOT / "plan-step-receipt.json"),
    ("epoch23_sidecar", EPOCH23_ROOT / "turn/sidecar.json"),
)


def _frozen_predecessor_evidence() -> list[dict[str, Any]]:
    return [{"role": role, **_record(path)} for role, path in _PREDECESSOR_FILES]


def _predecessor_records() -> Mapping[str, Any]:
    result: dict[str, dict[str, Any]] = {}
    labels = {
        "epoch26": "epoch26_measured_semantic_rejection",
        "epoch25": "epoch25_interrupted_unknown_usage_attempt",
        "epoch24": "epoch24_measured_structural_rejection",
        "epoch23": "epoch23_interrupted_unknown_usage_attempt",
    }
    for role, path in _PREDECESSOR_FILES:
        epoch, name = role.split("_", 1)
        result.setdefault(labels[epoch], {})[name] = _record(path)
    return result


def _intention_to_treat_accounting() -> dict[str, Any]:
    return {
        "predecessor_semantic_model_call_count": 4,
        "predecessor_measured_call_count": 2,
        "predecessor_unknown_usage_turn_count": 2,
        "predecessor_measured_total_tokens": 49_384,
        "epoch23": {"semantic_model_call_count": 1, "usage_status": "unknown"},
        "epoch24": {
            "semantic_model_call_count": 1,
            "usage_status": "measured",
            "total_tokens": 29_079,
        },
        "epoch25": {"semantic_model_call_count": 1, "usage_status": "unknown"},
        "epoch26": {
            "semantic_model_call_count": 1,
            "usage_status": "measured",
            "total_tokens": 20_305,
        },
        "maximum_semantic_model_call_count_after_epoch27_dispatch": 5,
        "predecessor_replay_allowed": False,
    }


def _validate_predecessor(full_verify: bool) -> None:
    predecessor = (
        epoch26.verify_receipt(PREDECESSOR_ROOT)
        if full_verify
        else _load(PREDECESSOR_ROOT / "plan-step-receipt.json", "epoch-26 receipt")
    )
    expected_hashes = {
        PREDECESSOR_ROOT / "runtime-lock.json": PREDECESSOR_RUNTIME_LOCK_SHA256,
        PREDECESSOR_ROOT / "plan-step-receipt.json": PREDECESSOR_RECEIPT_SHA256,
        PREDECESSOR_ROOT / "terminal.json": PREDECESSOR_RECEIPT_SHA256,
        PREDECESSOR_ROOT / "operator-authorization.json": PREDECESSOR_AUTHORIZATION_SHA256,
        PREDECESSOR_ROOT / "turn/semantic-attempt.json": PREDECESSOR_ATTEMPT_SHA256,
        PREDECESSOR_ROOT / "turn/sidecar.json": PREDECESSOR_SIDECAR_SHA256,
        PREDECESSOR_ROOT / "turn/output.private.json": PREDECESSOR_OUTPUT_SHA256,
        PREDECESSOR_ROOT / "semantic-rejection.json": PREDECESSOR_REJECTION_SHA256,
        PREDECESSOR_ROOT / "prepared-turn/request.private.json": PREDECESSOR_REQUEST_SHA256,
        EPOCH25_ROOT / "plan-step-receipt.json": EPOCH25_RECEIPT_SHA256,
        EPOCH25_ROOT / "turn/sidecar.json": EPOCH25_SIDECAR_SHA256,
        EPOCH24_ROOT / "plan-step-receipt.json": EPOCH24_RECEIPT_SHA256,
        EPOCH24_ROOT / "turn/sidecar.json": EPOCH24_SIDECAR_SHA256,
        EPOCH24_ROOT / "turn/output.private.json": EPOCH24_OUTPUT_SHA256,
        EPOCH23_ROOT / "plan-step-receipt.json": EPOCH23_RECEIPT_SHA256,
        EPOCH23_ROOT / "turn/sidecar.json": EPOCH23_SIDECAR_SHA256,
    }
    if any(_record(path)["sha256"] != digest for path, digest in expected_hashes.items()):
        raise TaggedMetricTokenIdCanaryError("epoch-27 predecessor record drifted")
    epoch25_receipt = _load(EPOCH25_ROOT / "plan-step-receipt.json", "epoch-25 receipt")
    epoch24_receipt = _load(EPOCH24_ROOT / "plan-step-receipt.json", "epoch-24 receipt")
    epoch23_receipt = _load(EPOCH23_ROOT / "plan-step-receipt.json", "epoch-23 receipt")
    if (
        predecessor.get("state") != "rejected"
        or predecessor.get("terminal_reason")
        != "epoch26_one_turn_canary_semantic_output_rejected"
        or predecessor.get("new_semantic_model_call_count") != 1
        or predecessor.get("new_unknown_usage_turn_count") != 0
        or predecessor.get("semantic_retry_count") != 0
        or predecessor.get("new_measured_usage", {}).get("total_tokens") != 20_305
        or predecessor.get("new_wall_elapsed_seconds") != 134.241
        or predecessor.get("diagnostic", {}).get("diagnostic_path")
        != "metric without pointers must use direction=not_applicable"
        or predecessor.get("full_six_case_total_token_projection_by_case_count") != 41_037
        or predecessor.get("production_mutated") is not False
        or predecessor.get("holdout_authorized") is not False
        or epoch25_receipt.get("state") != "waiting"
        or epoch25_receipt.get("new_unknown_usage_turn_count") != 1
        or epoch24_receipt.get("state") != "rejected"
        or epoch24_receipt.get("new_measured_usage", {}).get("total_tokens") != 29_079
        or epoch23_receipt.get("state") != "waiting"
        or epoch23_receipt.get("new_unknown_usage_turn_count") != 1
        or _intention_to_treat_accounting()
        != json.loads(DIRECTIVE_PATH.read_text(encoding="utf-8"))[
            "intention_to_treat_accounting"
        ]
    ):
        raise TaggedMetricTokenIdCanaryError("epoch-27 predecessor evidence drifted")
    _build_request()


def _runtime_module_paths() -> Sequence[Path]:
    return (
        Path(__file__),
        Path(adapter.__file__),
        Path(epoch26.__file__),
        *epoch26._runtime_module_paths(),  # noqa: SLF001
    )


def _validate_directive(value: Mapping[str, Any]) -> None:
    expected = json.loads(DIRECTIVE_PATH.read_text(encoding="utf-8"))
    ranking = expected.get("architecture_ranking", [])
    canary = expected.get("canary_contract", {})
    if dict(value) != expected:
        raise TaggedMetricTokenIdCanaryError("epoch-27 directive contract drifted")
    if (
        expected.get("schema_version")
        != "pif_evaluation_epoch27_tagged_metric_token_id_canary_directive_v1"
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
        or canary.get("candidate_reasoning_effort") != "medium"
        or canary.get("semantic_model_call_cap") != 1
        or canary.get("semantic_retry_cap") != 0
        or canary.get("measured_total_token_ceiling") != MAXIMUM_TOTAL_TOKENS
        or canary.get("epoch26_replay_allowed") is not False
        or canary.get("explicit_metric_bundle_cardinality_is_model_authored") is not True
        or canary.get("deterministic_semantic_repair_allowed") is not False
        or expected.get("transport_contract", {}).get("transport")
        != "official_codex_app_server_stdio_managed_chatgpt_auth"
        or expected.get("promotion_contract", {}).get("holdout_authorized") is not False
        or expected.get("promotion_contract", {}).get("production_mutation_allowed")
        is not False
    ):
        raise TaggedMetricTokenIdCanaryError("epoch-27 directive invariants drifted")


def _receipt_metadata(outcome: Mapping[str, Any]) -> Mapping[str, Any]:
    usage = outcome["usage"]
    total = int(usage["total_tokens"])
    fidelity_path = DEFAULT_ROOT / "turn/semantic-fidelity.json"
    fidelity: Mapping[str, Any] = {}
    if fidelity_path.is_file():
        fidelity = _load(fidelity_path, "epoch-27 semantic fidelity")
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
        "explicit_not_applicable_metric_bundle_count": fidelity.get(
            "explicit_not_applicable_metric_bundle_count"
        ),
        "explicit_applicable_metric_bundle_count": fidelity.get(
            "explicit_applicable_metric_bundle_count"
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
        "epoch23_predecessor_unknown_usage_turn_count": 1,
        "epoch24_predecessor_measured_total_tokens": 29_079,
        "epoch25_predecessor_unknown_usage_turn_count": 1,
        "epoch26_predecessor_measured_total_tokens": 20_305,
        "aggregate_architecture_semantic_model_call_count": 4 + current_calls,
        "aggregate_architecture_unknown_usage_turn_count": 2 + current_unknown,
        "aggregate_architecture_measured_total_tokens": 49_384 + total,
        "predecessor_accounting_reconciled_additively": True,
        "epoch23_replayed": False,
        "epoch24_replayed": False,
        "epoch25_replayed": False,
        "epoch26_replayed": False,
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
    stage="tagged_metric_exact_token_id_medium_effort_two_case_extraction_canary",
    architecture_class="one_turn_tagged_metric_exact_token_id_medium_effort_full_canonical_v1",
    authorization_statement=AUTHORIZATION_STATEMENT,
    authorization_state="authorized_for_exactly_one_tagged_metric_two_case_canary",
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
    reject_next_action="reject_tagged_metric_medium_effort_architecture_without_field_repair",
    waiting_next_action="no_replay_materially_distinct_architecture_only",
)


def _runtime_status(status: Mapping[str, Any], root: Path) -> dict[str, Any]:
    request = _load(root.resolve() / "prepared-turn/request.private.json", "epoch-27 request")
    shape = _request_shape(request)
    if shape != EXPECTED_SHAPE or _request_sha256(request) != EXPECTED_REQUEST_SHA256:
        raise TaggedMetricTokenIdCanaryError("epoch-27 runtime request drifted")
    epoch26.epoch25.epoch24._validate_provider_schema(request["output_schema"])  # noqa: SLF001
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
