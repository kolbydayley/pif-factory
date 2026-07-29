from __future__ import annotations

"""Epoch-37 dual-pass omission-audit full-canonical extraction canary."""

import argparse
import asyncio
import copy
import json
import math
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_dual_pass_omission_audit_ledger_episode_batch as adapter
from . import app_server_one_turn_canary_runtime as one_turn


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT / "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
DEFAULT_ROOT = (
    PIPELINE_ROOT / "canonical-v31-epoch37-dual-pass-omission-audit-ledger-canary-v1"
).resolve()
EPOCH27_ROOT = (
    PIPELINE_ROOT / "canonical-v31-epoch27-tagged-metric-token-id-canary-v1"
).resolve()
EPOCH36_ROOT = (
    PIPELINE_ROOT / "canonical-v31-epoch36-nested-ledger-source-unit-adjudication-v1"
).resolve()
DIRECTIVE_PATH = (
    PROJECT_ROOT
    / "automation/pif-evaluation-epoch37-dual-pass-omission-audit-ledger-canary-v37.json"
).resolve()
PLAN_PATH = (
    PROJECT_ROOT / "automation/pif-evaluation-semantic-plan-v37.json"
).resolve()
DIRECTIVE_SHA256 = "36a653c0b6eada7d950adc9ef00abc2d334779c03f4b696763c1fd94410f3bd7"
THREAD_ID = "019f4cf1-c46e-7db3-acd2-bf03c4459a10"
PLAN_EPOCH = 37
STEP_ID = "canonical_v31_epoch37_dual_pass_omission_audit_ledger_canary_v37"
TURN_NAME = "epoch37_dual_pass_omission_audit_two_segment_canary"
AUTHORIZATION_STATEMENT = (
    "You have my full permission to continue. No need to seek out my approval anymore."
)
AUTHORIZATION_ID_DEFAULT = "kolby-epoch37-dual-pass-omission-audit-20260720"
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
    "base_bytes": 26_292,
    "schema_bytes": 10_337,
    "request_bytes": 320_705,
    "source_unit_count": 20,
    "evidence_span_count": 38,
    "literal_token_count": 2_020,
}
EXPECTED_REQUEST_SHA256 = "f97b01cea16e5508f991a336ad448a81d28ee029f588336f4dc50e6c429d2514"
EXPECTED_PROMPT_SHA256 = "36af4c581e70487d72887f225ed18430d4b5c35b02370fed0a0257f34bc3a1c1"
EXPECTED_BASE_SHA256 = "a080beaec99ccabdf3d7338950a932cbdbc89a265876257599762ed5ce2d00c7"
EXPECTED_SCHEMA_SHA256 = "9c79102a601e44d2d41b91e2e0800488784876a08719c13410d427a68bd85413"
CANARY_VISIBLE_REQUEST_BYTES = 50_906
FULL_VISIBLE_REQUEST_BYTES = 78_843
FULL_CASE_SCALE = 3
FULL_PRODUCTION_TOKEN_CEILING = 73_926
EPOCH27_REQUEST_SHA256 = "f94df0a9f66f3de82a67f6442e37e5f6f0e563794e6722e5a6e56cc1e02513b6"
EPOCH36_HASHES = {
    "receipt": "55e051261fcdc21d525e843cd49eee8f9175208aa84426bb48866cb7f6b5fad6",
    "terminal": "55e051261fcdc21d525e843cd49eee8f9175208aa84426bb48866cb7f6b5fad6",
    "runtime_lock": "1256ee5ca2b70714c2816afe32ae6125ffd024ce2413cf628ffa91cfa65de3b4",
    "launch": "3423e666b61e31fba16bba8b9c7cdce281a0b7dc0e4e304707e1326491bcd81f",
    "sidecar": "3cec5f8ae430adc7a281b28f6e935f659517b9c80726accf61c3108ad064f69d",
    "output": "61c204eff9d79febd77584a22e7e334532c054d7956e2f72705be2c07f2ee1f3",
    "score": "9f4e9454d9eb63d35205a2c27498f2152ce0b94468e851cec3e0db8a5107e0aa",
}
PREDECESSOR_MODEL_CALLS = 14
PREDECESSOR_UNKNOWN_USAGE = 2
PREDECESSOR_MEASURED_TOKENS = 596_289

DualPassCanaryError = one_turn.OneTurnCanaryError
DualPassCanaryWaiting = one_turn.OneTurnCanaryWaiting


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
        raise DualPassCanaryError("epoch-27 frozen source request drifted")
    return _load(path, "epoch-27 frozen source request")


def _build_request_uncached() -> dict[str, Any]:
    values = adapter.prepare_episode_batches(
        adapter._episode_from_request(_source_request()),  # noqa: SLF001
        batch_size=3,
        thread_mode="new_thread",
    )
    if len(values) != 1:
        raise DualPassCanaryError("epoch-37 request count drifted")
    request = values[0]
    integrity = request.get("semantic_integrity", {})
    if (
        tuple(request.get("segment_ids", ())) != SELECTED_SEGMENT_IDS
        or request.get("effective_batch_size") != 2
        or request.get("batch_size_ceiling") != 3
        or request.get("candidate_system_id") != adapter.CANDIDATE_SYSTEM_ID
        or request.get("model") != "gpt-5.6-sol"
        or request.get("effort") != "medium"
        or request.get("retry_count") != 0
        or integrity.get("dual_pass_protocol_version")
        != adapter.DUAL_PASS_PROTOCOL_VERSION
        or integrity.get(
            "model_authors_primary_and_omission_audit_full_canonical_events"
        )
        is not True
        or integrity.get("deterministic_dual_ledger_concatenation_only") is not True
        or integrity.get("audit_duplicates_are_preserved_for_llm_quality_scoring")
        is not True
        or request["prompt_sha256"] != EXPECTED_PROMPT_SHA256
        or request["base_instructions_sha256"] != EXPECTED_BASE_SHA256
        or request["output_schema_sha256"] != EXPECTED_SCHEMA_SHA256
        or _request_shape(request) != EXPECTED_SHAPE
        or _request_sha256(request) != EXPECTED_REQUEST_SHA256
    ):
        raise DualPassCanaryError("epoch-37 frozen request contract drifted")
    adapter.validate_prepared_request(request)
    return request


@lru_cache(maxsize=1)
def _frozen_request_json() -> str:
    return one_turn._canonical_json(_build_request_uncached())  # noqa: SLF001


def _build_request() -> dict[str, Any]:
    return json.loads(_frozen_request_json())


_PREDECESSOR_FILES: tuple[tuple[str, Path], ...] = (
    ("receipt", EPOCH36_ROOT / "plan-step-receipt.json"),
    ("terminal", EPOCH36_ROOT / "terminal.json"),
    ("runtime_lock", EPOCH36_ROOT / "runtime-lock.json"),
    ("launch", EPOCH36_ROOT / "launch-receipt.json"),
    ("sidecar", EPOCH36_ROOT / "turns/epoch36-quality-adjudication/sidecar.json"),
    ("output", EPOCH36_ROOT / "turns/epoch36-quality-adjudication/output.private.json"),
    ("score", EPOCH36_ROOT / "shared-reference-score.json"),
    ("epoch27_request", EPOCH27_ROOT / "prepared-turn/request.private.json"),
)


def _predecessor_records() -> Mapping[str, Any]:
    return {
        "epoch36_quality_rejection": {
            role: _record(path)
            for role, path in _PREDECESSOR_FILES
            if role != "epoch27_request"
        },
        "epoch27_frozen_source_request": _record(
            EPOCH27_ROOT / "prepared-turn/request.private.json"
        ),
    }


def _validate_predecessor(full_verify: bool) -> None:
    del full_verify
    expected = {
        EPOCH36_ROOT / "plan-step-receipt.json": EPOCH36_HASHES["receipt"],
        EPOCH36_ROOT / "terminal.json": EPOCH36_HASHES["terminal"],
        EPOCH36_ROOT / "runtime-lock.json": EPOCH36_HASHES["runtime_lock"],
        EPOCH36_ROOT / "launch-receipt.json": EPOCH36_HASHES["launch"],
        EPOCH36_ROOT / "turns/epoch36-quality-adjudication/sidecar.json": EPOCH36_HASHES[
            "sidecar"
        ],
        EPOCH36_ROOT / "turns/epoch36-quality-adjudication/output.private.json": EPOCH36_HASHES[
            "output"
        ],
        EPOCH36_ROOT / "shared-reference-score.json": EPOCH36_HASHES["score"],
        EPOCH27_ROOT / "prepared-turn/request.private.json": EPOCH27_REQUEST_SHA256,
    }
    if any(_record(path)["sha256"] != digest for path, digest in expected.items()):
        raise DualPassCanaryError("epoch-37 predecessor record drifted")
    receipt = _load(EPOCH36_ROOT / "plan-step-receipt.json", "epoch-36 receipt")
    terminal = _load(EPOCH36_ROOT / "terminal.json", "epoch-36 terminal")
    score = receipt.get("score", {})
    if (
        receipt != terminal
        or receipt.get("state") != "rejected"
        or receipt.get("terminal_reason")
        != "epoch36_source_unit_adjudicated_quality_gate_rejected"
        or receipt.get("semantic_model_call_count") != 1
        or receipt.get("measured_model_call_count") != 1
        or receipt.get("unknown_usage_turn_count") != 0
        or receipt.get("semantic_retry_count") != 0
        or receipt.get("usage", {}).get("total_tokens") != 73_079
        or receipt.get("aggregate_architecture_semantic_model_call_count")
        != PREDECESSOR_MODEL_CALLS
        or receipt.get("aggregate_architecture_unknown_usage_turn_count")
        != PREDECESSOR_UNKNOWN_USAGE
        or receipt.get("aggregate_architecture_measured_total_tokens")
        != PREDECESSOR_MEASURED_TOKENS
        or score.get("candidate_strict_full_field_macro_f1") != 0.689746
        or score.get("baseline_strict_full_field_macro_f1") != 0.848039
        or score.get("shared_reference_unit_count") != 57
        or score.get("systems", {})
        .get("canonical_v31_epoch34_nested_proposition_event_ledger_medium", {})
        .get("supported_unit_count")
        != 30
        or score.get("exact_evidence_rate") != 1.0
        or receipt.get("winner_frozen") is not False
        or receipt.get("holdout_authorized") is not False
        or receipt.get("production_mutated") is not False
    ):
        raise DualPassCanaryError("epoch-36 quality rejection contract drifted")


def _runtime_module_paths() -> Sequence[Path]:
    names = (
        "app_server_canonical_v31_dual_pass_omission_audit_ledger_episode_batch.py",
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
    )
    return (Path(__file__), *(PROJECT_ROOT / "research_factory" / name for name in names))


def _validate_directive(value: Mapping[str, Any]) -> None:
    expected = json.loads(DIRECTIVE_PATH.read_text(encoding="utf-8"))
    canary = expected.get("canary_contract", {})
    ranking = expected.get("architecture_ranking", [])
    evidence = expected.get("architecture_evidence", {})
    if dict(value) != expected:
        raise DualPassCanaryError("epoch-37 directive contract drifted")
    if (
        expected.get("schema_version")
        != "pif_evaluation_epoch37_dual_pass_omission_audit_ledger_canary_directive_v1"
        or expected.get("thread_id") != THREAD_ID
        or expected.get("plan_epoch") != PLAN_EPOCH
        or expected.get("step_id") != STEP_ID
        or expected.get("authority") != "direct_user_instruction"
        or expected.get("authorized_by") != "kolby"
        or expected.get("authorization_statement") != AUTHORIZATION_STATEMENT
        or expected.get("state")
        != "authorized_for_exactly_one_dual_pass_omission_audit_two_segment_canary"
        or expected.get("expected_receipt_path")
        != str(DEFAULT_ROOT / "plan-step-receipt.json")
        or [row.get("rank") for row in ranking] != [1, 2, 3]
        or sum(row.get("selected") is True for row in ranking) != 1
        or evidence.get("epoch36_candidate_supported_unit_count") != 30
        or evidence.get("epoch36_shared_reference_unit_count") != 57
        or evidence.get("semantic_field_repair_authorized") is not False
        or canary.get("selected_segment_ids") != list(SELECTED_SEGMENT_IDS)
        or canary.get("candidate_model") != adapter.MODEL
        or canary.get("candidate_reasoning_effort") != adapter.EFFORT
        or canary.get("semantic_model_call_cap") != 1
        or canary.get("semantic_retry_cap") != 0
        or canary.get("measured_total_token_ceiling") != MAXIMUM_TOTAL_TOKENS
        or canary.get("full_six_case_total_token_ceiling")
        != FULL_PRODUCTION_TOKEN_CEILING
        or canary.get("expected_request_shape") != EXPECTED_SHAPE
        or canary.get("expected_request_sha256") != EXPECTED_REQUEST_SHA256
        or canary.get("model_authors_complete_canonical_events_in_both_lanes")
        is not True
        or canary.get("deterministic_dual_ledger_concatenation_only") is not True
        or canary.get("audit_duplicate_rejection_allowed") is not False
        or canary.get("event_count_is_diagnostic_only") is not True
        or canary.get("predecessor_semantic_replay_allowed") is not False
        or any(
            canary.get(name) is not False
            for name in (
                "deterministic_semantic_deduplication_allowed",
                "deterministic_semantic_pruning_allowed",
                "deterministic_semantic_relabeling_allowed",
                "deterministic_support_filtering_allowed",
            )
        )
        or expected.get("intention_to_treat_accounting")
        != {
            "maximum_semantic_model_call_count_after_epoch37_dispatch": 15,
            "predecessor_measured_total_tokens": PREDECESSOR_MEASURED_TOKENS,
            "predecessor_replay_allowed": False,
            "predecessor_semantic_model_call_count": PREDECESSOR_MODEL_CALLS,
            "predecessor_unknown_usage_turn_count": PREDECESSOR_UNKNOWN_USAGE,
        }
        or expected.get("promotion_contract", {}).get("quality_threshold") != 0.97
        or expected.get("promotion_contract", {}).get("winner_frozen") is not False
        or expected.get("promotion_contract", {}).get("holdout_authorized") is not False
        or expected.get("promotion_contract", {}).get("production_mutation_allowed")
        is not False
        or expected.get("transport_contract", {}).get("transport")
        != "official_codex_app_server_stdio_managed_chatgpt_auth"
        or expected.get("transport_contract", {}).get("managed_chatgpt_pro_auth_required")
        is not True
    ):
        raise DualPassCanaryError("epoch-37 directive invariants drifted")


def _receipt_metadata(outcome: Mapping[str, Any]) -> Mapping[str, Any]:
    usage = outcome["usage"]
    total = int(usage["total_tokens"])
    fidelity_path = DEFAULT_ROOT / "turn/semantic-fidelity.json"
    fidelity: Mapping[str, Any] = {}
    if fidelity_path.is_file():
        fidelity = _load(fidelity_path, "epoch-37 semantic fidelity")
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
    structural_and_cost_pass = outcome["state"] == "passed" and cost_pass
    current_calls = int(outcome["semantic_model_call_count"])
    current_unknown = int(outcome["unknown_usage_turn_count"])
    metadata: dict[str, Any] = {
        "diagnostic_segment_ids": list(SELECTED_SEGMENT_IDS),
        "diagnostic_source_unit_count": EXPECTED_SHAPE["source_unit_count"],
        "primary_ledger_item_count": fidelity.get("primary_ledger_item_count"),
        "omission_audit_ledger_item_count": fidelity.get(
            "omission_audit_ledger_item_count"
        ),
        "dual_ledger_item_count": fidelity.get("dual_ledger_item_count"),
        "diagnostic_event_count": fidelity.get("emitted_event_count"),
        "diagnostic_concept_count": fidelity.get("emitted_concept_count"),
        "event_count_is_diagnostic_only": True,
        "measured_canary_total_tokens": total,
        "full_six_case_input_token_projection": input_projection,
        "full_six_case_output_token_projection_by_case_count": output_projection,
        "full_six_case_total_token_projection_by_case_count": full_projection,
        "full_six_case_cost_projection_pass": cost_pass,
        "structural_and_cost_pass": structural_and_cost_pass,
        "production_token_ceiling_for_future_full_run": FULL_PRODUCTION_TOKEN_CEILING,
        "quality_measured_by_this_step": False,
        "fresh_full_event_ab_ba_quality_required": structural_and_cost_pass,
        "aggregate_architecture_semantic_model_call_count": (
            PREDECESSOR_MODEL_CALLS + current_calls
        ),
        "aggregate_architecture_unknown_usage_turn_count": (
            PREDECESSOR_UNKNOWN_USAGE + current_unknown
        ),
        "aggregate_architecture_measured_total_tokens": (
            PREDECESSOR_MEASURED_TOKENS + total
        ),
        "predecessor_accounting_reconciled_additively": True,
        "epoch36_replayed": False,
        "prior_semantic_output_reuse_count": 0,
        "deterministic_semantic_pruning": False,
        "deterministic_support_filtering": False,
        "deterministic_deduplication": False,
        "deterministic_relabeling": False,
    }
    if outcome["state"] == "passed" and not cost_pass:
        metadata.update(
            {
                "state": "rejected",
                "terminal_reason": "epoch37_full_six_case_cost_projection_rejected",
                "next_authorized_action": (
                    "reject_dual_pass_architecture_without_semantic_field_patch"
                ),
            }
        )
    return metadata


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
    stage="dual_pass_omission_audit_full_canonical_two_segment_canary",
    architecture_class="one_turn_primary_plus_omission_audit_full_canonical_v1",
    authorization_statement=AUTHORIZATION_STATEMENT,
    authorization_state="authorized_for_exactly_one_dual_pass_omission_audit_two_segment_canary",
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
    reject_next_action="reject_dual_pass_architecture_without_semantic_field_patch",
    waiting_next_action="no_replay_bounded_recovery_only",
)


def _runtime_status(status: Mapping[str, Any], root: Path) -> dict[str, Any]:
    request = _load(root.resolve() / "prepared-turn/request.private.json", "epoch-37 request")
    shape = _request_shape(request)
    if shape != EXPECTED_SHAPE or _request_sha256(request) != EXPECTED_REQUEST_SHA256:
        raise DualPassCanaryError("epoch-37 runtime request drifted")
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
