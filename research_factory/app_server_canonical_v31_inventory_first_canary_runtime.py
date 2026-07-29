from __future__ import annotations

"""Epoch-32 inventory-first full-canonical two-segment canary."""

import argparse
import asyncio
import copy
import json
import math
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import (
    app_server_canonical_v31_inventory_first_tagged_metric_token_id_episode_batch as adapter,
)
from . import app_server_one_turn_canary_runtime as one_turn


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROOT = (
    PROJECT_ROOT
    / "work/app-server-development-v2/unattended-pipeline-v5"
    / "canonical-v31-epoch32-inventory-first-canary-v1"
)
EPOCH31_ROOT = (
    PROJECT_ROOT
    / "work/app-server-development-v2/unattended-pipeline-v5"
    / "canonical-v31-epoch31-opaque-source-unit-adjudication-v1"
)
EPOCH27_ROOT = (
    PROJECT_ROOT
    / "work/app-server-development-v2/unattended-pipeline-v5"
    / "canonical-v31-epoch27-tagged-metric-token-id-canary-v1"
)
DIRECTIVE_PATH = (
    PROJECT_ROOT
    / "automation/pif-evaluation-epoch32-inventory-first-canonical-canary-v32.json"
)
PLAN_PATH = PROJECT_ROOT / "automation/pif-evaluation-semantic-plan-v32.json"
DIRECTIVE_SHA256 = "18fbf78901e2d5c9ab241104fd9032a2505e4c706d401190dc5c7bc8b8881998"
THREAD_ID = "019f4cf1-c46e-7db3-acd2-bf03c4459a10"
PLAN_EPOCH = 32
STEP_ID = "canonical_v31_epoch32_inventory_first_canary_v32"
TURN_NAME = "epoch32_inventory_first_two_segment_canary"
AUTHORIZATION_STATEMENT = "You have my full permission to continue. No need to seek out my approval anymore."
AUTHORIZATION_ID_DEFAULT = "kolby-epoch32-inventory-first-canonical-20260720"
SELECTED_SEGMENT_IDS = (
    "seg_80fe585badf8f3d3597d0f96",
    "seg_7c027e01ed7e813b528c5c7f",
)
MAXIMUM_TOTAL_TOKENS = 40_000
MAXIMUM_WALL_SECONDS = 1_200
MINIMUM_REMAINING_RESERVE_PERCENT = 20
CAPACITY_SAFETY_MARGIN_PERCENT = 1
QUOTA_POINTS_PER_MILLION_TOKENS = 17
AUTHORIZATION_WINDOW_SECONDS = 14_400
EXPECTED_SHAPE = {
    "prompt_bytes": 14_277,
    "base_bytes": 25_453,
    "schema_bytes": 6_172,
    "request_bytes": 315_529,
    "source_unit_count": 20,
    "evidence_span_count": 38,
    "literal_token_count": 2_020,
}
EXPECTED_REQUEST_SHA256 = "9cb575848d4d62775ddc92fba0739ad68ebb8baf9344b92ecbf03992d35efd24"
EXPECTED_PROMPT_SHA256 = "36af4c581e70487d72887f225ed18430d4b5c35b02370fed0a0257f34bc3a1c1"
EXPECTED_BASE_SHA256 = "297e354a3f6770c68124f1af0ac5574d71a74f12385244518cf2e0cbef4db61c"
EXPECTED_SCHEMA_SHA256 = "1d8c8336598a9bd6dbd3fc910aead884fbce4a3a6385d3e92efaa6bce525cb20"
CANARY_VISIBLE_REQUEST_BYTES = 45_902
FULL_VISIBLE_REQUEST_BYTES = 73_839
FULL_CASE_SCALE = 3
FULL_PRODUCTION_TOKEN_CEILING = 73_926

EPOCH31_RECEIPT_SHA256 = "630222afad2cf7477796ff4d99fe13f9566fec245f187a7faa45b817f64dafb4"
EPOCH31_RUNTIME_LOCK_SHA256 = "c6f6e215a3b506e0f41e88a2131e9af27924e6617030f3ee5a7f42ab659a73bb"
EPOCH31_SCORE_SHA256 = "bb7e6269de5093952a13e069d32823eaae604df6a8e16f7445420de7fc91414d"
EPOCH31_SIDECAR_SHA256 = "ccbb9213448cd6c0a1abb99b5e4a2eeebe7d31fef2ba5150fc37c5b0a0f7d66d"
EPOCH31_OUTPUT_SHA256 = "f083e9ac6000ed5816fb54fec942de2d20e812af0f45f985d624e46173b668a3"
EPOCH27_RECEIPT_SHA256 = "47d068f553d776d22986312c31f2746b2616a2983f4fb9379901aca31deccb16"
EPOCH27_REQUEST_SHA256 = "f94df0a9f66f3de82a67f6442e37e5f6f0e563794e6722e5a6e56cc1e02513b6"
EPOCH27_LABELS_SHA256 = "8e2666750b3fb67658d7786b318d5e337558743bb8e142b0fcf88a667ca437a6"
EPOCH27_PROVENANCE_SHA256 = "f753eb545cf1c56624b53b7aefec80e473c3e286bb8243d461231cff0817a543"
EPOCH27_FIDELITY_SHA256 = "94c7132270da9afc51d9dd39756dd6bbcb14403d8889338c46aa81df893db87d"

InventoryFirstCanaryError = one_turn.OneTurnCanaryError
InventoryFirstCanaryWaiting = one_turn.OneTurnCanaryWaiting


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
        "request_bytes": len(
            one_turn._canonical_json(request).encode("utf-8")  # noqa: SLF001
        ),
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


def _request_sha256(request: Mapping[str, Any]) -> str:
    return one_turn._sha256_bytes(  # noqa: SLF001
        one_turn._canonical_json(request).encode("utf-8")  # noqa: SLF001
    )


def _source_request() -> dict[str, Any]:
    path = EPOCH27_ROOT / "prepared-turn/request.private.json"
    if _record(path)["sha256"] != EPOCH27_REQUEST_SHA256:
        raise InventoryFirstCanaryError("epoch-27 frozen source request drifted")
    return _load(path, "epoch-27 frozen source request")


def _build_request_uncached() -> dict[str, Any]:
    source = _source_request()
    values = adapter.prepare_episode_batches(
        adapter._episode_from_request(source),  # noqa: SLF001
        batch_size=3,
        thread_mode="new_thread",
    )
    if len(values) != 1:
        raise InventoryFirstCanaryError("epoch-32 request count drifted")
    request = values[0]
    if (
        tuple(request.get("segment_ids", ())) != SELECTED_SEGMENT_IDS
        or request.get("effective_batch_size") != 2
        or request.get("batch_size_ceiling") != 3
        or request.get("candidate_system_id") != adapter.CANDIDATE_SYSTEM_ID
        or request.get("model") != "gpt-5.6-sol"
        or request.get("effort") != "high"
        or request.get("retry_count") != 0
        or request.get("semantic_integrity", {}).get("inventory_protocol_version")
        != adapter.INVENTORY_PROTOCOL_VERSION
        or request.get("semantic_integrity", {}).get(
            "one_inventory_item_per_canonical_event"
        )
        is not True
        or request.get("semantic_integrity", {}).get(
            "deterministic_inventory_removal_only"
        )
        is not True
        or request["prompt_sha256"] != EXPECTED_PROMPT_SHA256
        or request["base_instructions_sha256"] != EXPECTED_BASE_SHA256
        or request["output_schema_sha256"] != EXPECTED_SCHEMA_SHA256
        or _request_shape(request) != EXPECTED_SHAPE
        or _request_sha256(request) != EXPECTED_REQUEST_SHA256
    ):
        raise InventoryFirstCanaryError("epoch-32 frozen request contract drifted")
    adapter.validate_prepared_request(request)
    return request


@lru_cache(maxsize=1)
def _frozen_request_json() -> str:
    return one_turn._canonical_json(_build_request_uncached())  # noqa: SLF001


def _build_request() -> dict[str, Any]:
    return json.loads(_frozen_request_json())


_PREDECESSOR_FILES: tuple[tuple[str, Path], ...] = (
    ("epoch31_receipt", EPOCH31_ROOT / "plan-step-receipt.json"),
    ("epoch31_terminal", EPOCH31_ROOT / "terminal.json"),
    ("epoch31_runtime_lock", EPOCH31_ROOT / "runtime-lock.json"),
    ("epoch31_score", EPOCH31_ROOT / "shared-reference-score.json"),
    (
        "epoch31_sidecar",
        EPOCH31_ROOT / "turns/epoch31-quality-adjudication/sidecar.json",
    ),
    (
        "epoch31_output",
        EPOCH31_ROOT / "turns/epoch31-quality-adjudication/output.private.json",
    ),
    ("epoch27_receipt", EPOCH27_ROOT / "plan-step-receipt.json"),
    ("epoch27_request", EPOCH27_ROOT / "prepared-turn/request.private.json"),
    ("epoch27_labels", EPOCH27_ROOT / "turn/canonical-labels.private.json"),
    (
        "epoch27_provenance",
        EPOCH27_ROOT / "turn/evidence-provenance.private.json",
    ),
    ("epoch27_fidelity", EPOCH27_ROOT / "turn/semantic-fidelity.json"),
)


def _frozen_predecessor_evidence() -> list[dict[str, Any]]:
    return [{"role": role, **_record(path)} for role, path in _PREDECESSOR_FILES]


def _predecessor_records() -> Mapping[str, Any]:
    records = dict(_PREDECESSOR_FILES)
    return {
        "epoch31_quality_rejection": {
            role.removeprefix("epoch31_"): _record(path)
            for role, path in _PREDECESSOR_FILES
            if role.startswith("epoch31_")
        },
        "epoch27_frozen_source_and_candidate": {
            role.removeprefix("epoch27_"): _record(path)
            for role, path in _PREDECESSOR_FILES
            if role.startswith("epoch27_")
        },
        "source_request": _record(records["epoch27_request"]),
    }


def _intention_to_treat_accounting() -> dict[str, Any]:
    return {
        "predecessor_semantic_model_call_count": 9,
        "predecessor_unknown_usage_turn_count": 2,
        "predecessor_measured_total_tokens": 313_594,
        "maximum_semantic_model_call_count_after_epoch32_dispatch": 10,
        "predecessor_replay_allowed": False,
    }


def _validate_predecessor(full_verify: bool) -> None:
    del full_verify
    expected_hashes = {
        EPOCH31_ROOT / "plan-step-receipt.json": EPOCH31_RECEIPT_SHA256,
        EPOCH31_ROOT / "terminal.json": EPOCH31_RECEIPT_SHA256,
        EPOCH31_ROOT / "runtime-lock.json": EPOCH31_RUNTIME_LOCK_SHA256,
        EPOCH31_ROOT / "shared-reference-score.json": EPOCH31_SCORE_SHA256,
        EPOCH31_ROOT / "turns/epoch31-quality-adjudication/sidecar.json": EPOCH31_SIDECAR_SHA256,
        EPOCH31_ROOT / "turns/epoch31-quality-adjudication/output.private.json": EPOCH31_OUTPUT_SHA256,
        EPOCH27_ROOT / "plan-step-receipt.json": EPOCH27_RECEIPT_SHA256,
        EPOCH27_ROOT / "prepared-turn/request.private.json": EPOCH27_REQUEST_SHA256,
        EPOCH27_ROOT / "turn/canonical-labels.private.json": EPOCH27_LABELS_SHA256,
        EPOCH27_ROOT / "turn/evidence-provenance.private.json": EPOCH27_PROVENANCE_SHA256,
        EPOCH27_ROOT / "turn/semantic-fidelity.json": EPOCH27_FIDELITY_SHA256,
    }
    if any(_record(path)["sha256"] != digest for path, digest in expected_hashes.items()):
        raise InventoryFirstCanaryError("epoch-32 predecessor record drifted")
    epoch31 = _load(EPOCH31_ROOT / "plan-step-receipt.json", "epoch-31 receipt")
    terminal = _load(EPOCH31_ROOT / "terminal.json", "epoch-31 terminal")
    score = _load(EPOCH31_ROOT / "shared-reference-score.json", "epoch-31 score")
    epoch27 = _load(EPOCH27_ROOT / "plan-step-receipt.json", "epoch-27 receipt")
    fidelity = _load(EPOCH27_ROOT / "turn/semantic-fidelity.json", "epoch-27 fidelity")
    candidate = score.get("systems", {}).get(
        "canonical_v31_epoch27_tagged_metric_token_id_medium", {}
    )
    if (
        epoch31 != terminal
        or epoch31.get("state") != "rejected"
        or epoch31.get("terminal_reason")
        != "epoch31_source_unit_adjudicated_quality_gate_rejected"
        or epoch31.get("semantic_model_call_count") != 1
        or epoch31.get("semantic_retry_count") != 0
        or epoch31.get("usage", {}).get("total_tokens") != 47_802
        or epoch31.get("aggregate_architecture_semantic_model_call_count") != 9
        or epoch31.get("aggregate_architecture_unknown_usage_turn_count") != 2
        or epoch31.get("aggregate_architecture_measured_total_tokens") != 313_594
        or score.get("candidate_strict_full_field_macro_f1") != 0.666667
        or score.get("baseline_strict_full_field_macro_f1") != 0.903361
        or score.get("exact_evidence_rate") != 1.0
        or candidate.get("strict_reference_units_covered") != 25
        or candidate.get("submitted_unit_count") != 25
        or score.get("shared_reference_unit_count") != 50
        or epoch31.get("production_mutated") is not False
        or epoch31.get("holdout_authorized") is not False
        or epoch31.get("winner_frozen") is not False
        or epoch27.get("state") != "passed"
        or epoch27.get("diagnostic_event_count") != 25
        or epoch27.get("full_six_case_total_token_projection_by_case_count") != 54_770
        or fidelity.get("emitted_event_count") != 25
        or fidelity.get("deterministic_semantic_pruning") is not False
        or _intention_to_treat_accounting()
        != json.loads(DIRECTIVE_PATH.read_text(encoding="utf-8"))[
            "intention_to_treat_accounting"
        ]
    ):
        raise InventoryFirstCanaryError("epoch-32 predecessor evidence drifted")


def _runtime_module_paths() -> Sequence[Path]:
    names = (
        "app_server_canonical_v31_inventory_first_tagged_metric_token_id_episode_batch.py",
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
    )
    return (Path(__file__), *(PROJECT_ROOT / "research_factory" / name for name in names))


def _validate_directive(value: Mapping[str, Any]) -> None:
    expected = json.loads(DIRECTIVE_PATH.read_text(encoding="utf-8"))
    ranking = expected.get("architecture_ranking", [])
    canary = expected.get("canary_contract", {})
    if dict(value) != expected:
        raise InventoryFirstCanaryError("epoch-32 directive contract drifted")
    if (
        expected.get("schema_version")
        != "pif_evaluation_epoch32_inventory_first_canonical_canary_directive_v1"
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
        or canary.get("candidate_model") != adapter.MODEL
        or canary.get("candidate_reasoning_effort") != adapter.EFFORT
        or canary.get("semantic_model_call_cap") != 1
        or canary.get("semantic_retry_cap") != 0
        or canary.get("measured_total_token_ceiling") != MAXIMUM_TOTAL_TOKENS
        or canary.get("full_six_case_total_token_ceiling")
        != FULL_PRODUCTION_TOKEN_CEILING
        or canary.get("one_inventory_item_per_canonical_event") is not True
        or canary.get("inventory_and_event_evidence_pointer_identity_required")
        is not True
        or canary.get("deterministic_inventory_removal_only") is not True
        or canary.get("deterministic_semantic_repair_allowed") is not False
        or any(
            canary.get(name) is not False
            for name in (
                "epoch27_extraction_replay_allowed",
                "epoch28_ab_ba_replay_allowed",
                "epoch29_adjudication_replay_allowed",
                "epoch31_adjudication_replay_allowed",
            )
        )
        or expected.get("transport_contract", {}).get("transport")
        != "official_codex_app_server_stdio_managed_chatgpt_auth"
        or expected.get("promotion_contract", {}).get("quality_threshold") != 0.97
        or expected.get("promotion_contract", {}).get("holdout_authorized") is not False
        or expected.get("promotion_contract", {}).get("production_mutation_allowed")
        is not False
    ):
        raise InventoryFirstCanaryError("epoch-32 directive invariants drifted")


def _receipt_metadata(outcome: Mapping[str, Any]) -> Mapping[str, Any]:
    usage = outcome["usage"]
    total = int(usage["total_tokens"])
    fidelity_path = DEFAULT_ROOT / "turn/semantic-fidelity.json"
    fidelity: Mapping[str, Any] = {}
    if fidelity_path.is_file():
        fidelity = _load(fidelity_path, "epoch-32 semantic fidelity")
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
        "diagnostic_inventory_item_count": fidelity.get("inventory_item_count"),
        "diagnostic_event_count": fidelity.get("emitted_event_count"),
        "diagnostic_concept_count": fidelity.get("emitted_concept_count"),
        "inventory_event_cardinality_exact": (
            fidelity.get("inventory_item_count") == fidelity.get("emitted_event_count")
            if fidelity
            else None
        ),
        "inventory_evidence_identity_count": fidelity.get(
            "inventory_evidence_identity_count"
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
        "predecessor_quality_candidate_strict_macro_f1": 0.666667,
        "predecessor_quality_candidate_reference_units_covered": 25,
        "predecessor_quality_shared_reference_unit_count": 50,
        "aggregate_architecture_semantic_model_call_count": 9 + current_calls,
        "aggregate_architecture_unknown_usage_turn_count": 2 + current_unknown,
        "aggregate_architecture_measured_total_tokens": 313_594 + total,
        "predecessor_accounting_reconciled_additively": True,
        "epoch27_replayed": False,
        "epoch28_replayed": False,
        "epoch29_replayed": False,
        "epoch31_replayed": False,
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
    stage="inventory_first_full_canonical_two_segment_extraction_canary",
    architecture_class="one_turn_inventory_first_one_to_one_full_canonical_v1",
    authorization_statement=AUTHORIZATION_STATEMENT,
    authorization_state="authorized_for_exactly_one_inventory_first_two_segment_canary",
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
    reject_next_action="reject_inventory_first_architecture_without_field_patch",
    waiting_next_action="no_replay_bounded_recovery_only",
)


def _runtime_status(status: Mapping[str, Any], root: Path) -> dict[str, Any]:
    request = _load(root.resolve() / "prepared-turn/request.private.json", "epoch-32 request")
    shape = _request_shape(request)
    if shape != EXPECTED_SHAPE or _request_sha256(request) != EXPECTED_REQUEST_SHA256:
        raise InventoryFirstCanaryError("epoch-32 runtime request drifted")
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
