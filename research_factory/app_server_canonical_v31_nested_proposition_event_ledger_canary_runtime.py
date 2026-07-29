from __future__ import annotations

"""Epoch-33 nested proposition-event ledger two-segment canary."""

import argparse
import asyncio
import copy
import json
import math
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_inventory_first_canary_runtime as epoch32
from . import app_server_canonical_v31_nested_proposition_event_ledger_episode_batch as adapter
from . import app_server_one_turn_canary_runtime as one_turn


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROOT = (
    PROJECT_ROOT
    / "work/app-server-development-v2/unattended-pipeline-v5"
    / "canonical-v31-epoch33-nested-proposition-event-ledger-canary-v1"
)
EPOCH32_ROOT = epoch32.DEFAULT_ROOT
EPOCH27_ROOT = epoch32.EPOCH27_ROOT
DIRECTIVE_PATH = (
    PROJECT_ROOT
    / "automation/pif-evaluation-epoch33-nested-proposition-event-ledger-canary-v33.json"
)
PLAN_PATH = PROJECT_ROOT / "automation/pif-evaluation-semantic-plan-v33.json"
DIRECTIVE_SHA256 = "03423a2ca8d36cf662ba116560669410369da0bd6447ab50d8e423d28e56f70d"
THREAD_ID = epoch32.THREAD_ID
PLAN_EPOCH = 33
STEP_ID = "canonical_v31_epoch33_nested_proposition_event_ledger_canary_v33"
TURN_NAME = "epoch33_nested_proposition_event_ledger_two_segment_canary"
AUTHORIZATION_STATEMENT = epoch32.AUTHORIZATION_STATEMENT
AUTHORIZATION_ID_DEFAULT = "kolby-epoch33-nested-proposition-event-ledger-20260720"
SELECTED_SEGMENT_IDS = epoch32.SELECTED_SEGMENT_IDS
MAXIMUM_TOTAL_TOKENS = 36_000
MAXIMUM_WALL_SECONDS = 1_200
MINIMUM_REMAINING_RESERVE_PERCENT = 20
CAPACITY_SAFETY_MARGIN_PERCENT = 1
QUOTA_POINTS_PER_MILLION_TOKENS = 17
AUTHORIZATION_WINDOW_SECONDS = 14_400
EXPECTED_SHAPE = {
    "prompt_bytes": 14_277,
    "base_bytes": 25_377,
    "schema_bytes": 6_086,
    "request_bytes": 315_267,
    "source_unit_count": 20,
    "evidence_span_count": 38,
    "literal_token_count": 2_020,
}
EXPECTED_REQUEST_SHA256 = "63077992a4ff5062b4c25dee4fecec0bd7fdc899a9b9922069c166a2e37f1f4f"
EXPECTED_PROMPT_SHA256 = epoch32.EXPECTED_PROMPT_SHA256
EXPECTED_BASE_SHA256 = "0e2c94c98ec1d3b7d1e4e125a50fc16edabc43e3e588f0c4c96f5b6b1061bf18"
EXPECTED_SCHEMA_SHA256 = "b31e1743b1a187affb9db68fdd1885664cc759277484056237af79f012cae6db"
CANARY_VISIBLE_REQUEST_BYTES = 45_740
FULL_VISIBLE_REQUEST_BYTES = 73_677
FULL_CASE_SCALE = 3
FULL_PRODUCTION_TOKEN_CEILING = 73_926

EPOCH32_RECEIPT_SHA256 = "873edef6d6de28e55d88a00e189b2a137dbcea944e8b19ee4ca49b3ff312530a"
EPOCH32_RUNTIME_LOCK_SHA256 = "900a0c1ebb0c04b6405ee0c29272c719e2b45438da98ef78c55f2e1b2b2c7ef6"
EPOCH32_AUTHORIZATION_SHA256 = "f8f91c7b469bf74239a20eff6edc23db85dc97ac45b2e8b7bf75ae7c1a5b3c04"
EPOCH32_ATTEMPT_SHA256 = "541eff8bdf162c17f3edc6e41adf752028ab1fd500c51746a747bb3dda6f106c"
EPOCH32_SIDECAR_SHA256 = "161d73e863ff37d9865ea41ae07ae2d89355d2b58754a1f196ade50cbc34aaa7"
EPOCH32_OUTPUT_SHA256 = "ef3dae9479eda7ddb95978dcd4925e2658634845628c8f3188c72835ff8d79fb"
EPOCH32_REJECTION_SHA256 = "4abca3f0c967db9500ef5bbce0adf2a33da87e209ad3101dfb5e984f2e120861"
EPOCH32_REQUEST_SHA256 = "279916f353d580537aaea303a6f61bd944733d71e4cc1c1268df9c3f175e6812"
EPOCH27_REQUEST_SHA256 = epoch32.EPOCH27_REQUEST_SHA256

NestedLedgerCanaryError = one_turn.OneTurnCanaryError
NestedLedgerCanaryWaiting = one_turn.OneTurnCanaryWaiting


def _load(path: Path, label: str) -> dict[str, Any]:
    return one_turn._load(path, label)  # noqa: SLF001


def _record(path: Path) -> dict[str, Any]:
    return one_turn._record(path)  # noqa: SLF001


def _request_shape(request: Mapping[str, Any]) -> dict[str, int]:
    return epoch32._request_shape(request)  # noqa: SLF001


def _request_sha256(request: Mapping[str, Any]) -> str:
    return epoch32._request_sha256(request)  # noqa: SLF001


def _source_request() -> dict[str, Any]:
    path = EPOCH27_ROOT / "prepared-turn/request.private.json"
    if _record(path)["sha256"] != EPOCH27_REQUEST_SHA256:
        raise NestedLedgerCanaryError("epoch-27 frozen source request drifted")
    return _load(path, "epoch-27 frozen source request")


def _build_request_uncached() -> dict[str, Any]:
    source = _source_request()
    values = adapter.prepare_episode_batches(
        adapter._episode_from_request(source),  # noqa: SLF001
        batch_size=3,
        thread_mode="new_thread",
    )
    if len(values) != 1:
        raise NestedLedgerCanaryError("epoch-33 request count drifted")
    request = values[0]
    if (
        tuple(request.get("segment_ids", ())) != SELECTED_SEGMENT_IDS
        or request.get("effective_batch_size") != 2
        or request.get("batch_size_ceiling") != 3
        or request.get("candidate_system_id") != adapter.CANDIDATE_SYSTEM_ID
        or request.get("model") != "gpt-5.6-sol"
        or request.get("effort") != "medium"
        or request.get("retry_count") != 0
        or request.get("semantic_integrity", {}).get("ledger_protocol_version")
        != adapter.LEDGER_PROTOCOL_VERSION
        or request.get("semantic_integrity", {}).get(
            "model_authors_one_nested_full_canonical_event_per_proposition"
        )
        is not True
        or request.get("semantic_integrity", {}).get("deterministic_ledger_unwrap_only")
        is not True
        or request["prompt_sha256"] != EXPECTED_PROMPT_SHA256
        or request["base_instructions_sha256"] != EXPECTED_BASE_SHA256
        or request["output_schema_sha256"] != EXPECTED_SCHEMA_SHA256
        or _request_shape(request) != EXPECTED_SHAPE
        or _request_sha256(request) != EXPECTED_REQUEST_SHA256
    ):
        raise NestedLedgerCanaryError("epoch-33 frozen request contract drifted")
    adapter.validate_prepared_request(request)
    return request


@lru_cache(maxsize=1)
def _frozen_request_json() -> str:
    return one_turn._canonical_json(_build_request_uncached())  # noqa: SLF001


def _build_request() -> dict[str, Any]:
    return json.loads(_frozen_request_json())


_PREDECESSOR_FILES: tuple[tuple[str, Path], ...] = (
    ("epoch32_receipt", EPOCH32_ROOT / "plan-step-receipt.json"),
    ("epoch32_terminal", EPOCH32_ROOT / "terminal.json"),
    ("epoch32_runtime_lock", EPOCH32_ROOT / "runtime-lock.json"),
    ("epoch32_authorization", EPOCH32_ROOT / "operator-authorization.json"),
    ("epoch32_attempt", EPOCH32_ROOT / "turn/semantic-attempt.json"),
    ("epoch32_sidecar", EPOCH32_ROOT / "turn/sidecar.json"),
    ("epoch32_output", EPOCH32_ROOT / "turn/output.private.json"),
    ("epoch32_rejection", EPOCH32_ROOT / "semantic-rejection.json"),
    ("epoch32_request", EPOCH32_ROOT / "prepared-turn/request.private.json"),
    ("epoch27_request", EPOCH27_ROOT / "prepared-turn/request.private.json"),
)


def _frozen_predecessor_evidence() -> list[dict[str, Any]]:
    return [{"role": role, **_record(path)} for role, path in _PREDECESSOR_FILES]


def _predecessor_records() -> Mapping[str, Any]:
    return {
        "epoch32_rejected_inventory_first_attempt": {
            role.removeprefix("epoch32_"): _record(path)
            for role, path in _PREDECESSOR_FILES
            if role.startswith("epoch32_")
        },
        "epoch27_frozen_source_request": _record(
            EPOCH27_ROOT / "prepared-turn/request.private.json"
        ),
    }


def _intention_to_treat_accounting() -> dict[str, Any]:
    return {
        "predecessor_semantic_model_call_count": 10,
        "predecessor_unknown_usage_turn_count": 2,
        "predecessor_measured_total_tokens": 347_709,
        "maximum_semantic_model_call_count_after_epoch33_dispatch": 11,
        "predecessor_replay_allowed": False,
    }


def _epoch32_output_shape() -> dict[str, int]:
    output = _load(EPOCH32_ROOT / "turn/output.private.json", "epoch-32 raw output")
    inventory = events = concepts = mismatches = 0
    for segment in output.get("s", []):
        for row in segment.get("2", []):
            inventory += len(row.get("0", []))
            events += len(row.get("1", []))
            concepts += len(row.get("2", []))
            for item, event in zip(row.get("0", []), row.get("1", [])):
                if item.get("1") != event.get("26"):
                    mismatches += 1
    return {
        "inventory_item_count": inventory,
        "event_count": events,
        "concept_count": concepts,
        "pointer_mismatch_count": mismatches,
    }


def _validate_predecessor(full_verify: bool) -> None:
    del full_verify
    expected_hashes = {
        EPOCH32_ROOT / "plan-step-receipt.json": EPOCH32_RECEIPT_SHA256,
        EPOCH32_ROOT / "terminal.json": EPOCH32_RECEIPT_SHA256,
        EPOCH32_ROOT / "runtime-lock.json": EPOCH32_RUNTIME_LOCK_SHA256,
        EPOCH32_ROOT / "operator-authorization.json": EPOCH32_AUTHORIZATION_SHA256,
        EPOCH32_ROOT / "turn/semantic-attempt.json": EPOCH32_ATTEMPT_SHA256,
        EPOCH32_ROOT / "turn/sidecar.json": EPOCH32_SIDECAR_SHA256,
        EPOCH32_ROOT / "turn/output.private.json": EPOCH32_OUTPUT_SHA256,
        EPOCH32_ROOT / "semantic-rejection.json": EPOCH32_REJECTION_SHA256,
        EPOCH32_ROOT / "prepared-turn/request.private.json": EPOCH32_REQUEST_SHA256,
        EPOCH27_ROOT / "prepared-turn/request.private.json": EPOCH27_REQUEST_SHA256,
    }
    if any(_record(path)["sha256"] != digest for path, digest in expected_hashes.items()):
        raise NestedLedgerCanaryError("epoch-33 predecessor record drifted")
    receipt = _load(EPOCH32_ROOT / "plan-step-receipt.json", "epoch-32 receipt")
    terminal = _load(EPOCH32_ROOT / "terminal.json", "epoch-32 terminal")
    if (
        receipt != terminal
        or receipt.get("state") != "rejected"
        or receipt.get("terminal_reason") != "epoch32_one_turn_canary_semantic_output_rejected"
        or receipt.get("new_semantic_model_call_count") != 1
        or receipt.get("semantic_retry_count") != 0
        or receipt.get("new_unknown_usage_turn_count") != 0
        or receipt.get("new_measured_usage", {}).get("total_tokens") != 34_115
        or receipt.get("new_wall_elapsed_seconds") != 441.177
        or receipt.get("full_six_case_total_token_projection_by_case_count") != 81_504
        or receipt.get("full_six_case_cost_projection_pass") is not False
        or receipt.get("diagnostic", {}).get("diagnostic_path")
        != "inventory/event evidence pointer drifted"
        or receipt.get("aggregate_architecture_semantic_model_call_count") != 10
        or receipt.get("aggregate_architecture_unknown_usage_turn_count") != 2
        or receipt.get("aggregate_architecture_measured_total_tokens") != 347_709
        or receipt.get("production_mutated") is not False
        or receipt.get("holdout_authorized") is not False
        or receipt.get("winner_frozen") is not False
        or _epoch32_output_shape()
        != {
            "inventory_item_count": 40,
            "event_count": 40,
            "concept_count": 22,
            "pointer_mismatch_count": 2,
        }
        or _intention_to_treat_accounting()
        != json.loads(DIRECTIVE_PATH.read_text(encoding="utf-8"))[
            "intention_to_treat_accounting"
        ]
    ):
        raise NestedLedgerCanaryError("epoch-33 predecessor evidence drifted")


def _runtime_module_paths() -> Sequence[Path]:
    names = (
        "app_server_canonical_v31_nested_proposition_event_ledger_episode_batch.py",
        "app_server_canonical_v31_tagged_metric_token_id_episode_batch.py",
        "app_server_canonical_v31_unit_owned_token_id_episode_batch.py",
        "app_server_canonical_v31_unit_owned_ordinal_episode_batch.py",
        "app_server_canonical_v31_unit_owned_positional_episode_batch.py",
        "app_server_canonical_v31_single_message_compact_pointer_episode_batch.py",
        "app_server_canonical_v31_compact_unit_pointer_episode_batch.py",
        "app_server_canonical_v31_unit_local_pointer_episode_batch.py",
        "app_server_canonical_v31_bounded_span_episode_batch.py",
        "app_server_canonical_v31_literal_pointer_episode_batch.py",
        "app_server_canonical_v31_episode_batch.py",
        "app_server_canonical_v31_inventory_first_canary_runtime.py",
    )
    return (Path(__file__), *(PROJECT_ROOT / "research_factory" / name for name in names))


def _validate_directive(value: Mapping[str, Any]) -> None:
    expected = json.loads(DIRECTIVE_PATH.read_text(encoding="utf-8"))
    ranking = expected.get("architecture_ranking", [])
    canary = expected.get("canary_contract", {})
    if dict(value) != expected:
        raise NestedLedgerCanaryError("epoch-33 directive contract drifted")
    if (
        expected.get("schema_version")
        != "pif_evaluation_epoch33_nested_proposition_event_ledger_canary_directive_v1"
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
        or [row.get("rank") for row in ranking] != [1, 2, 3]
        or sum(row.get("selected") is True for row in ranking) != 1
        or canary.get("selected_segment_ids") != list(SELECTED_SEGMENT_IDS)
        or canary.get("candidate_model") != adapter.MODEL
        or canary.get("candidate_reasoning_effort") != adapter.EFFORT
        or canary.get("semantic_model_call_cap") != 1
        or canary.get("semantic_retry_cap") != 0
        or canary.get("measured_total_token_ceiling") != MAXIMUM_TOTAL_TOKENS
        or canary.get("full_six_case_total_token_ceiling")
        != FULL_PRODUCTION_TOKEN_CEILING
        or canary.get("model_authors_one_nested_full_canonical_event_per_proposition")
        is not True
        or canary.get("no_duplicate_inventory_event_evidence_transport") is not True
        or canary.get("deterministic_ledger_unwrap_only") is not True
        or canary.get("deterministic_semantic_repair_allowed") is not False
        or any(
            canary.get(name) is not False
            for name in (
                "epoch27_extraction_replay_allowed",
                "epoch31_quality_replay_allowed",
                "epoch32_extraction_replay_allowed",
            )
        )
        or expected.get("transport_contract", {}).get("transport")
        != "official_codex_app_server_stdio_managed_chatgpt_auth"
        or expected.get("promotion_contract", {}).get("quality_threshold") != 0.97
        or expected.get("promotion_contract", {}).get("holdout_authorized") is not False
        or expected.get("promotion_contract", {}).get("production_mutation_allowed")
        is not False
    ):
        raise NestedLedgerCanaryError("epoch-33 directive invariants drifted")


def _receipt_metadata(outcome: Mapping[str, Any]) -> Mapping[str, Any]:
    usage = outcome["usage"]
    total = int(usage["total_tokens"])
    fidelity_path = DEFAULT_ROOT / "turn/semantic-fidelity.json"
    fidelity: Mapping[str, Any] = {}
    if fidelity_path.is_file():
        fidelity = _load(fidelity_path, "epoch-33 semantic fidelity")
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
        "epoch32_diagnostic_inventory_item_count": 40,
        "epoch32_diagnostic_event_count": 40,
        "epoch32_diagnostic_pointer_mismatch_count": 2,
        "epoch32_six_case_token_projection": 81_504,
        "aggregate_architecture_semantic_model_call_count": 10 + current_calls,
        "aggregate_architecture_unknown_usage_turn_count": 2 + current_unknown,
        "aggregate_architecture_measured_total_tokens": 347_709 + total,
        "predecessor_accounting_reconciled_additively": True,
        "epoch27_replayed": False,
        "epoch31_replayed": False,
        "epoch32_replayed": False,
        "epoch32_rejected_output_adopted": False,
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
    stage="nested_proposition_event_ledger_two_segment_extraction_canary",
    architecture_class="one_turn_nested_proposition_event_ledger_full_canonical_v1",
    authorization_statement=AUTHORIZATION_STATEMENT,
    authorization_state="authorized_for_exactly_one_nested_ledger_two_segment_canary",
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
    waiting_next_action="no_replay_bounded_recovery_only",
)


def _runtime_status(status: Mapping[str, Any], root: Path) -> dict[str, Any]:
    request = _load(root.resolve() / "prepared-turn/request.private.json", "epoch-33 request")
    shape = _request_shape(request)
    if shape != EXPECTED_SHAPE or _request_sha256(request) != EXPECTED_REQUEST_SHA256:
        raise NestedLedgerCanaryError("epoch-33 runtime request drifted")
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
